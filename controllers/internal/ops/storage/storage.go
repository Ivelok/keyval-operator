package storage

import (
	"context"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

var nowFunc = time.Now

const (
	// ReasonResize marks storage-driven restarts in the rolling update plan.
	ReasonResize = "storage:resize"

	defaultRequeue        = 5 * time.Second
	resizeFilesystemGrace = 2 * time.Minute
	resizeCapacityGrace   = 15 * time.Minute
	resizeThrottleBackoff = 5 * time.Minute
	cleanupBatchDefault   = 3
)

// ResizeResult conveys PVC resize state back to the reconciler.
type ResizeResult struct {
	PodReasons      map[string][]string
	PendingPVCs     []string
	Waiting         bool
	RequeueAfter    time.Duration
	MinRequeueAfter time.Duration
}

// CleanupResult reports deletion progress for PVC cleanup during finalization.
type CleanupResult struct {
	Pending []string
	Deleted []string
}

// EnsureResize aligns PVC capacity with spec.storage.size and prepares pod restarts when needed.
func EnsureResize(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod, recorder record.EventRecorder, logger logging.Logger) (ResizeResult, error) {
	res := ResizeResult{PodReasons: map[string][]string{}}
	if cr == nil || c == nil {
		return res, nil
	}
	if !core.HasPersistentData(cr) {
		return res, nil
	}
	if logger.IsZero() {
		logger = logging.New(nil)
	}

	desired := core.DesiredStorageQuantity(cr)
	desiredStr := desired.String()

	podSet := make(map[string]struct{}, len(pods))
	for i := range pods {
		podSet[pods[i].Name] = struct{}{}
	}

	pvcs, err := listClusterPVCs(ctx, c, cr)
	if err != nil {
		return res, err
	}

	now := nowFunc().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	for i := range pvcs {
		pvc := pvcs[i]
		if pvc.DeletionTimestamp != nil {
			markWaiting(&res, pvc.Name, defaultRequeue)
			continue
		}

		podName := pvc.Labels["statefulset.kubernetes.io/pod-name"]
		if podName == "" {
			podName = core.PodNameFromPVC(cr, pvc.Name)
		}
		if podName == "" {
			podName = pvc.Name
		}

		current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if pvc.Spec.Resources.Requests == nil {
			current = resource.Quantity{}
		}
		fromStr := current.String()

		// Handle grow requests
		if current.Cmp(desired) < 0 {
			desiredPVC := pvc.DeepCopy()
			if desiredPVC.Spec.Resources.Requests == nil {
				desiredPVC.Spec.Resources.Requests = corev1.ResourceList{}
			}
			desiredPVC.Spec.Resources.Requests[corev1.ResourceStorage] = desired
			if desiredPVC.Annotations == nil {
				desiredPVC.Annotations = map[string]string{}
			}
			desiredPVC.Annotations[core.AnnotationPVCResizeTarget] = desiredStr
			desiredPVC.Annotations[core.AnnotationPVCResizeRequestedAt] = nowStr
			if err := c.Patch(ctx, desiredPVC, client.MergeFrom(&pvc)); err != nil {
				if apierrors.IsConflict(err) {
					markWaiting(&res, pvc.Name, defaultRequeue)
					continue
				}
				if backoff, throttled := resizeThrottle(err); throttled {
					applyThrottle(&res, pvc.Name, backoff)
					if recorder != nil {
						observability.EventStorageResizeBackoff(recorder, cr, pvc.Name, backoff)
					}
					logger.V(1).Info("pvc resize throttled", "pvc", pvc.Name, "backoff", backoff, "error", err)
					continue
				}
				return res, err
			}
			pvc = *desiredPVC
			if recorder != nil {
				observability.EventStorageResizeRequested(recorder, cr, pvc.Name, fromStr, desiredStr)
			}
		} else if current.Cmp(desired) > 0 {
			if recorder != nil {
				observability.EventStorageResizeIgnored(recorder, cr, pvc.Name, fromStr, desiredStr)
			}
		}

		pendingFS := hasFileSystemResizePending(&pvc)
		capacity := pvc.Status.Capacity[corev1.ResourceStorage]
		if pvc.Status.Capacity == nil {
			capacity = resource.Quantity{}
		}

		targetMatches := pvc.Annotations[core.AnnotationPVCResizeTarget] == desiredStr
		requestedAt, hasRequested := parseResizeTimestamp(pvc.Annotations[core.AnnotationPVCResizeRequestedAt])
		if !hasRequested {
			requestedAt = now
		}
		age := now.Sub(requestedAt)
		if age < 0 {
			age = 0
		}

		waiting := false
		needsRestart := false

		if targetMatches {
			if pendingFS {
				waiting = true
				if age >= resizeFilesystemGrace {
					needsRestart = true
				}
			} else if capacity.Cmp(desired) < 0 {
				waiting = true
				if age >= resizeCapacityGrace {
					needsRestart = true
				}
			}
		}

		if waiting {
			markWaiting(&res, pvc.Name, defaultRequeue)
			if needsRestart {
				if _, ok := podSet[podName]; ok {
					appendReason(&res, podName, ReasonResize)
					if recorder != nil {
						observability.EventStorageResizeRestartQueued(recorder, cr, pvc.Name, podName, age)
					}
					logger.V(1).Info("resize requires pod restart", "pod", podName, "pvc", pvc.Name, "age", age.String(), "capacity", capacity.String())
				}
			}
			continue
		}

		if targetMatches && !pendingFS && capacity.Cmp(desired) >= 0 {
			if pvc.Annotations[core.AnnotationPVCResizeCompletedAt] == "" && recorder != nil {
				observability.EventStorageResizeCompleted(recorder, cr, pvc.Name, desiredStr)
			}
			if pvc.Annotations[core.AnnotationPVCResizeCompletedAt] == "" || pvc.Annotations[core.AnnotationPVCResizeRequestedAt] != "" {
				patched := pvc.DeepCopy()
				if patched.Annotations == nil {
					patched.Annotations = map[string]string{}
				}
				patched.Annotations[core.AnnotationPVCResizeCompletedAt] = nowStr
				delete(patched.Annotations, core.AnnotationPVCResizeRequestedAt)
				if err := c.Patch(ctx, patched, client.MergeFrom(&pvc)); err != nil && !apierrors.IsConflict(err) {
					logger.V(1).Info("patch pvc resize completion failed", "pvc", pvc.Name, "error", err)
				}
			}
		}
	}

	if len(res.PendingPVCs) > 1 {
		sort.Strings(res.PendingPVCs)
		res.PendingPVCs = uniqueStrings(res.PendingPVCs)
	}

	return res, nil
}

// CleanupPVCs removes PVCs owned by the cluster when cleanupOnDelete=true.
// The caller may cap the number of deletions attempted per invocation via limit.
func CleanupPVCs(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, limit int, recorder record.EventRecorder, logger logging.Logger) (CleanupResult, error) {
	result := CleanupResult{}
	if cr == nil || c == nil {
		return result, nil
	}
	if !core.HasPersistentData(cr) {
		return result, nil
	}
	if logger.IsZero() {
		logger = logging.New(nil)
	}

	pvcs, err := listClusterPVCs(ctx, c, cr)
	if err != nil {
		return result, err
	}

	if limit <= 0 {
		limit = cleanupBatchDefault
	}

	deletions := 0
	for i := range pvcs {
		pvc := pvcs[i]
		result.Pending = append(result.Pending, pvc.Name)
		if pvc.DeletionTimestamp != nil {
			continue
		}
		if deletions >= limit {
			continue
		}
		err := c.Delete(ctx, pvc.DeepCopy())
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return result, err
		}
		logger.V(1).Info("removing pvc for cluster cleanup", "pvc", pvc.Name)
		result.Deleted = append(result.Deleted, pvc.Name)
		deletions++
		if recorder != nil {
			observability.EventStoragePVCDeletionRequested(recorder, cr, pvc.Name)
		}
	}

	sort.Strings(result.Pending)
	sort.Strings(result.Deleted)
	return result, nil
}

func listClusterPVCs(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster) ([]corev1.PersistentVolumeClaim, error) {
	var list corev1.PersistentVolumeClaimList
	if err := c.List(ctx, &list, client.InNamespace(cr.Namespace)); err != nil {
		return nil, err
	}
	out := make([]corev1.PersistentVolumeClaim, 0, len(list.Items))
	for i := range list.Items {
		pvc := list.Items[i]
		if core.PVCBelongsToCluster(cr, &pvc) {
			out = append(out, pvc)
		}
	}
	return out, nil
}

func hasFileSystemResizePending(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc == nil {
		return false
	}
	for _, cond := range pvc.Status.Conditions {
		if cond.Type == corev1.PersistentVolumeClaimFileSystemResizePending && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func appendReason(res *ResizeResult, podName, reason string) {
	if res == nil || podName == "" || reason == "" {
		return
	}
	if res.PodReasons == nil {
		res.PodReasons = map[string][]string{}
	}
	reasons := res.PodReasons[podName]
	for _, existing := range reasons {
		if existing == reason {
			return
		}
	}
	res.PodReasons[podName] = append(reasons, reason)
}

func minDuration(cur, candidate time.Duration) time.Duration {
	if cur == 0 {
		return candidate
	}
	if candidate == 0 {
		return cur
	}
	if candidate < cur {
		return candidate
	}
	return cur
}

func uniqueStrings(in []string) []string {
	if len(in) < 2 {
		return in
	}
	out := make([]string, 0, len(in))
	prev := ""
	for i, s := range in {
		if i == 0 || s != prev {
			out = append(out, s)
			prev = s
		}
	}
	return out
}

func markWaiting(res *ResizeResult, pvcName string, candidate time.Duration) {
	if res == nil {
		return
	}
	res.Waiting = true
	if pvcName != "" {
		res.PendingPVCs = append(res.PendingPVCs, pvcName)
	}
	if candidate <= 0 {
		candidate = defaultRequeue
	}
	if res.MinRequeueAfter > 0 && candidate < res.MinRequeueAfter {
		candidate = res.MinRequeueAfter
	}
	res.RequeueAfter = minDuration(res.RequeueAfter, candidate)
	if res.MinRequeueAfter > 0 && res.RequeueAfter < res.MinRequeueAfter {
		res.RequeueAfter = res.MinRequeueAfter
	}
}

func applyThrottle(res *ResizeResult, pvcName string, backoff time.Duration) {
	if res == nil {
		return
	}
	if backoff <= 0 {
		backoff = resizeThrottleBackoff
	}
	if backoff > res.MinRequeueAfter {
		res.MinRequeueAfter = backoff
	}
	if backoff > res.RequeueAfter {
		res.RequeueAfter = backoff
	}
	markWaiting(res, pvcName, backoff)
}

func parseResizeTimestamp(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	if ts, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return ts, true
	}
	if ts, err := time.Parse(time.RFC3339, value); err == nil {
		return ts, true
	}
	return time.Time{}, false
}

func resizeThrottle(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	if apierrors.IsTooManyRequests(err) {
		return resizeThrottleBackoff, true
	}
	msg := err.Error()
	if msg == "" {
		return 0, false
	}
	throttleSignals := []string{
		"RequestLimitExceeded",
		"VolumeModifyRateExceeded",
		"Throttling",
		"Rate exceeded",
	}
	for _, signal := range throttleSignals {
		if strings.Contains(msg, signal) {
			return resizeThrottleBackoff, true
		}
	}
	return 0, false
}
