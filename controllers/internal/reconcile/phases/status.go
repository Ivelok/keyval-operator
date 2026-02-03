package phases

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
)

// Status computes cluster conditions/events and persists status updates.
func Status(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, state *reconcile.State) error {
	if state == nil || cr == nil {
		return nil
	}

	pods := state.RedisPods
	sentinelPods := state.SentinelPods
	runtimeState := state.Runtime
	conditionOverrides := state.ConditionOverrides
	if conditionOverrides == nil {
		conditionOverrides = make(map[keyvalv1alpha1.ConditionType]*opstatus.ConditionState)
		state.ConditionOverrides = conditionOverrides
	}

	roles := runtimeState.Roles
	if roles == nil {
		roles = make(map[string]keyvalv1alpha1.PodRole)
	}
	healthStates := runtimeState.Health
	if healthStates == nil {
		healthStates = make(map[string]keyvalv1alpha1.PodHealth)
	}
	lagSeconds := runtimeState.LagSeconds
	if lagSeconds == nil {
		lagSeconds = make(map[string]*int32)
	}

	clusterState := opstatus.ClusterState{
		Master:         runtimeState.Master,
		Pods:           pods,
		Roles:          roles,
		SentinelPods:   sentinelPods,
		Health:         healthStates,
		LagSeconds:     lagSeconds,
		RolesSource:    reconcile.EnsureSourceToRolesSource(runtimeState.Topology.Source),
		RolesReason:    runtimeState.Topology.Reason,
		RuntimeConfig:  runtimeState.RuntimeCondition,
		Conditions:     conditionOverrides,
		ExternalImport: state.Import.Status,
	}

	wantStatus := opstatus.ComputeStatus(cr, clusterState)

	// Sentinel quorum events
	prevSentinelCond := reconcile.ConditionStatus(cr.Status.Conditions, keyvalv1alpha1.ConditionSentinelQuorum)
	newSentinelCond := reconcile.ConditionStatus(wantStatus.Conditions, keyvalv1alpha1.ConditionSentinelQuorum)
	if newSentinelCond != nil {
		if (prevSentinelCond == nil || prevSentinelCond.Status != metav1.ConditionFalse) && newSentinelCond.Status == metav1.ConditionFalse {
			opobs.EventSentinelQuorumLost(state.Dependencies.Recorder, cr, newSentinelCond.Message)
		}
		if prevSentinelCond != nil && prevSentinelCond.Status == metav1.ConditionFalse && newSentinelCond.Status == metav1.ConditionTrue {
			opobs.EventSentinelQuorumRestored(state.Dependencies.Recorder, cr, newSentinelCond.Message)
		}
	}

	// Bootstrap events
	prevBootstrap := reconcile.ConditionStatus(cr.Status.Conditions, keyvalv1alpha1.ConditionBootstrapInProgress)
	newBootstrap := reconcile.ConditionStatus(wantStatus.Conditions, keyvalv1alpha1.ConditionBootstrapInProgress)
	if newBootstrap != nil && newBootstrap.Status == metav1.ConditionTrue && (prevBootstrap == nil || prevBootstrap.Status != metav1.ConditionTrue) {
		detail := newBootstrap.Message
		if detail == "" {
			detail = "bootstrap sequence started"
		}
		opobs.EventBootstrapStart(state.Dependencies.Recorder, cr, detail)
		mode := opobs.BootstrapModeStandalone
		if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
			mode = opobs.BootstrapModeSentinel
		}
		opobs.IncBootstrapAttempt(cr, mode)
	}
	if prevBootstrap != nil && prevBootstrap.Status == metav1.ConditionTrue && (newBootstrap == nil || newBootstrap.Status != metav1.ConditionTrue) {
		detail := "bootstrap complete"
		if wantStatus.MasterPod != "" {
			detail = fmt.Sprintf("bootstrap complete, master=%s", wantStatus.MasterPod)
		}
		opobs.EventBootstrapFinish(state.Dependencies.Recorder, cr, detail)
	}

	allowDisruptions := wantStatus.HealthGate != nil && wantStatus.HealthGate.AllowDisruptions
	allowSentinel := reconcile.AllowSentinelDisruptions(&wantStatus)
	state.Disruption.AllowDisruptions = allowDisruptions
	state.Disruption.AllowSentinel = allowSentinel

	state.Status = reconcile.StatusState{
		ClusterState: clusterState,
		Computed:     wantStatus,
	}

	if err := opstatus.UpdateStatus(ctx, state.Dependencies.Client, cr, clusterState); err != nil {
		return controllererrors.WrapTransient(controllererrors.WrapKubeAPI(fmt.Errorf("update status: %w", err)))
	}

	return nil
}
