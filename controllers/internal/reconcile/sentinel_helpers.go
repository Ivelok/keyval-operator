package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	clientspkg "github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
)

// SentinelResetTracker tracks sentinel quorum loss windows and cooldowns across reconciles.
type SentinelResetTracker struct {
	LossSince     time.Time
	Attempts      int
	CooldownUntil time.Time
}

// ReadSentinelResetTracker extracts tracker data from cluster annotations.
func ReadSentinelResetTracker(cr *keyvalv1alpha1.KeyValCluster) SentinelResetTracker {
	var tracker SentinelResetTracker
	if cr == nil || cr.Annotations == nil {
		return tracker
	}
	if raw := cr.Annotations[core.AnnotationSentinelQuorumLossSince]; raw != "" {
		if ts, err := time.Parse(time.RFC3339, raw); err == nil {
			tracker.LossSince = ts
		}
	}
	if raw := cr.Annotations[core.AnnotationSentinelClusterResetAttempts]; raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			tracker.Attempts = n
		}
	}
	if raw := cr.Annotations[core.AnnotationSentinelClusterResetCooldown]; raw != "" {
		if ts, err := time.Parse(time.RFC3339, raw); err == nil {
			tracker.CooldownUntil = ts
		}
	}
	return tracker
}

// WriteSentinelResetTracker persists tracker annotations when they change.
func WriteSentinelResetTracker(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, tracker SentinelResetTracker) error {
	current := ReadSentinelResetTracker(cr)
	if sentinelResetTrackersEqual(current, tracker) {
		return nil
	}
	base := cr.DeepCopy()
	if !tracker.LossSince.IsZero() {
		tracker.LossSince = NormalizeTimestamp(tracker.LossSince)
	}
	if !tracker.CooldownUntil.IsZero() {
		tracker.CooldownUntil = NormalizeTimestamp(tracker.CooldownUntil)
	}
	if tracker.Attempts < 0 {
		tracker.Attempts = 0
	}
	if tracker.LossSince.IsZero() {
		delete(cr.Annotations, core.AnnotationSentinelQuorumLossSince)
	} else {
		ensureAnnotations(cr)
		cr.Annotations[core.AnnotationSentinelQuorumLossSince] = tracker.LossSince.Format(time.RFC3339)
	}
	if tracker.Attempts > 0 {
		ensureAnnotations(cr)
		cr.Annotations[core.AnnotationSentinelClusterResetAttempts] = strconv.Itoa(tracker.Attempts)
	} else {
		delete(cr.Annotations, core.AnnotationSentinelClusterResetAttempts)
	}
	if !tracker.CooldownUntil.IsZero() {
		ensureAnnotations(cr)
		cr.Annotations[core.AnnotationSentinelClusterResetCooldown] = tracker.CooldownUntil.Format(time.RFC3339)
	} else {
		delete(cr.Annotations, core.AnnotationSentinelClusterResetCooldown)
	}
	if len(cr.Annotations) == 0 {
		cr.Annotations = nil
	}
	if err := c.Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		cr.Annotations = base.Annotations
		return err
	}
	return nil
}

func sentinelResetTrackersEqual(a, b SentinelResetTracker) bool {
	return timesEqual(a.LossSince, b.LossSince) && timesEqual(a.CooldownUntil, b.CooldownUntil) && a.Attempts == b.Attempts
}

func timesEqual(a, b time.Time) bool {
	if a.IsZero() && b.IsZero() {
		return true
	}
	return a.Equal(b)
}

// NormalizeTimestamp truncates timestamps to second precision for annotation stability.
func NormalizeTimestamp(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t.UTC().Truncate(time.Second)
}

func ensureAnnotations(cr *keyvalv1alpha1.KeyValCluster) {
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
}

// SentinelQuorumReport captures the outcome of querying sentinel quorum.
type SentinelQuorumReport struct {
	Healthy  bool
	Detail   string
	Ready    int
	OK       int
	NoQuorum int
}

// HasSentinelQuorum checks sentinel quorum health by probing ready sentinel pods.
func HasSentinelQuorum(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, sentinelPods []corev1.Pod, sf clientspkg.SentinelFactory, quorumTarget int, opts clientspkg.SentinelOptions) (SentinelQuorumReport, error) {
	report := SentinelQuorumReport{}
	if sf == nil {
		report.Detail = "sentinel client not configured"
		return report, errors.New("nil sentinel factory")
	}
	if len(sentinelPods) == 0 {
		report.Detail = "no sentinel pods available"
		return report, nil
	}
	ordered := runtimepkg.SortedPodsByName(sentinelPods)
	var errs []error
	for _, pod := range ordered {
		if !runtimepkg.IsPodReady(&pod) {
			continue
		}
		report.Ready++
		cli, err := sf.ForPod(ctx, pod, opts)
		if err != nil {
			if strings.Contains(strings.ToUpper(err.Error()), "NOQUORUM") {
				report.NoQuorum++
				if report.Detail == "" {
					report.Detail = fmt.Sprintf("%s: %v", pod.Name, err)
				}
				continue
			}
			errs = append(errs, fmt.Errorf("%s: %w", pod.Name, err))
			if report.Detail == "" {
				report.Detail = fmt.Sprintf("%s: %v", pod.Name, err)
			}
			continue
		}
		opCtx, cancel := runtimepkg.WithTimeout(ctx, 3*time.Second)
		ok, err := cli.CheckQuorum(opCtx, cr.Name)
		cancel()
		if err != nil {
			if strings.Contains(strings.ToUpper(err.Error()), "NOQUORUM") {
				report.NoQuorum++
				if report.Detail == "" {
					report.Detail = fmt.Sprintf("%s: %v", pod.Name, err)
				}
				continue
			}
			errs = append(errs, fmt.Errorf("%s: %w", pod.Name, err))
			if report.Detail == "" {
				report.Detail = fmt.Sprintf("%s: %v", pod.Name, err)
			}
			continue
		}
		if ok {
			report.OK++
			continue
		}
		if report.Detail == "" {
			report.Detail = fmt.Sprintf("sentinel %s reported quorum=0", pod.Name)
		}
	}
	if report.Ready == 0 {
		report.Detail = "no ready sentinel pods"
		return report, nil
	}
	if quorumTarget <= 0 {
		quorumTarget = 1
	}
	if report.OK >= quorumTarget {
		report.Healthy = true
		report.Detail = ""
	} else if report.Detail == "" {
		report.Detail = fmt.Sprintf("sentinel quorum ok=%d ready=%d target=%d", report.OK, report.Ready, quorumTarget)
	}
	if len(errs) > 0 {
		return report, errors.Join(errs...)
	}
	return report, nil
}

// IssueSentinelReset triggers a sentinel FAILOVER reset against ready sentinel pods.
func IssueSentinelReset(ctx context.Context, sf clientspkg.SentinelFactory, sentinelPods []corev1.Pod, cluster string, opts clientspkg.SentinelOptions) (string, error) {
	ordered := runtimepkg.SortedPodsByName(sentinelPods)
	var errs []error
	for _, pod := range ordered {
		if !runtimepkg.IsPodReady(&pod) {
			continue
		}
		cli, err := sf.ForPod(ctx, pod, opts)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", pod.Name, err))
			continue
		}
		opCtx, cancel := runtimepkg.WithTimeout(ctx, 5*time.Second)
		err = cli.Reset(opCtx, cluster)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", pod.Name, err))
			continue
		}
		return pod.Name, nil
	}
	if len(errs) == 0 {
		return "", errors.New("no ready sentinel pod available for reset")
	}
	return "", controllererrors.WrapTransient(fmt.Errorf("sentinel reset failed: %w", errors.Join(errs...)))
}
