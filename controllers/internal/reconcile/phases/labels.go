package phases

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
)

// Labels reconciles pod role labels, maintaining demotion-first ordering and retry semantics.
func Labels(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	pods := state.RedisPods
	if len(pods) == 0 {
		return nil
	}

	deps := state.Dependencies
	if deps.Client == nil {
		return controllererrors.WrapTransient(fmt.Errorf("label phase missing client"))
	}
	cr := state.Cluster
	logger := state.Logger
	if logger.IsZero() {
		logger = deps.BaseLogger
	}
	roleLogger := logger.WithValues("component", "replication")

	master := state.Runtime.Master
	desiredPods := opreplication.UpdatePodRoleLabels(pods, master)

	var demoteIdxs, otherIdxs []int
	for i := range pods {
		cur := &pods[i]
		want := &desiredPods[i]
		curRole := cur.Labels[core.RoleLabelKey]
		wantRole := want.Labels[core.RoleLabelKey]
		if curRole == wantRole {
			continue
		}
		if curRole == string(keyvalv1alpha1.PodRoleMaster) && wantRole != string(keyvalv1alpha1.PodRoleMaster) {
			demoteIdxs = append(demoteIdxs, i)
		} else {
			otherIdxs = append(otherIdxs, i)
		}
	}

	changes := 0
	recorder := deps.Recorder

	applyAt := func(i int) error {
		for attempt := 1; attempt <= reconcile.PodLabelPatchMaxAttempts; attempt++ {
			cur := &pods[i]
			want := &desiredPods[i]
			if cur.Labels == nil {
				cur.Labels = map[string]string{}
			}
			fromRole := cur.Labels[core.RoleLabelKey]
			toRole := want.Labels[core.RoleLabelKey]
			if fromRole == toRole {
				return nil
			}

			patchLogger := roleLogger.WithValues(
				"pod", cur.Name,
				"fromRole", fromRole,
				"toRole", toRole,
			)

			base := cur.DeepCopy()
			cur.Labels[core.RoleLabelKey] = toRole
			patch := client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
			if err := deps.Client.Patch(ctx, cur, patch); err != nil {
				if !apierrors.IsConflict(err) {
					patchLogger.Error(err, "role label patch failed", "attempt", attempt, "resourceVersion", base.ResourceVersion)
					return controllererrors.WrapTransient(fmt.Errorf("patch role label %s: %w", cur.Name, err))
				}
				*cur = *base
				opobs.IncPodLabelPatchConflict(cr, cur.Name)
				patchLogger.Info("role label patch conflict", "attempt", attempt, "resourceVersion", base.ResourceVersion)
				state.RequeueMin(reconcile.RoleLabelConflictBackoff(attempt))
				if attempt == reconcile.PodLabelPatchMaxAttempts {
					patchLogger.Info("role label patch retries exhausted", "attempt", attempt, "resourceVersion", base.ResourceVersion)
					return nil
				}
				var refreshed corev1.Pod
				if getErr := deps.Client.Get(ctx, client.ObjectKey{Namespace: cur.Namespace, Name: cur.Name}, &refreshed); getErr != nil {
					if apierrors.IsNotFound(getErr) {
						patchLogger.Info("pod disappeared while retrying role label patch", "attempt", attempt, "resourceVersion", base.ResourceVersion)
						return nil
					}
					return controllererrors.WrapTransient(fmt.Errorf("refresh pod %s after conflict: %w", cur.Name, getErr))
				}
				pods[i] = refreshed
				continue
			}

			changes++
			if attempt > 1 {
				opobs.AddPodLabelPatchRetry(cr, cur.Name, attempt-1)
				patchLogger.Info("role label patch applied after retries", "attempt", attempt, "resourceVersion", cur.ResourceVersion)
			}
			return nil
		}
		return nil
	}

	for _, i := range demoteIdxs {
		if err := applyAt(i); err != nil {
			return err
		}
	}
	for _, i := range otherIdxs {
		if err := applyAt(i); err != nil {
			return err
		}
	}

	if changes > 0 {
		opobs.IncLabelCorrectionsBy(cr, changes)
		opobs.EventRoleCorrected(recorder, cr, master)
	}

	state.RedisPods = pods
	return nil
}
