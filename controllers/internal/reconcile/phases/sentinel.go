package phases

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
)

const (
	sentinelResetObservationWindow = 30 * time.Second
	sentinelResetCooldownSuccess   = 2 * time.Minute
	sentinelResetCooldownFailure   = 45 * time.Second
	sentinelResetMaxAttempts       = 3
)

type sentinelResetMode int

const (
	sentinelResetModeNone sentinelResetMode = iota
	sentinelResetModeQuorumLoss
	sentinelResetModeTopology
)

// Sentinel evaluates sentinel quorum health, manages reset cooldowns, and updates sentinel conditions.
func Sentinel(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	cr := state.Cluster
	logger := state.Logger
	deps := state.Dependencies
	overrides := state.EnsureConditionOverrides()

	sentinelState := reconcile.SentinelState{}
	sentinelState.Managed = cr.Spec.Mode == keyvalv1alpha1.ModeSentinel

	if !sentinelState.Managed {
		opobs.SetSentinelQuorum(cr, true)
		opobs.ObserveSentinelReady(cr, 0)
		opobs.ObserveSentinelQuorumTarget(cr, 0)
		opobs.ObserveSentinelNoQuorum(cr, 0)
		opobs.ObserveSentinelQuorumLoss(cr, nil)
		sentinelState.QuorumOK = true
		state.Sentinel = sentinelState
		return nil
	}

	sentinelPods := state.SentinelPods

	expected := int32(3)
	if cr.Spec.SentinelCount != nil && *cr.Spec.SentinelCount > 0 {
		expected = *cr.Spec.SentinelCount
	}

	readySentinels := 0
	for i := range sentinelPods {
		if runtimepkg.IsPodReady(&sentinelPods[i]) {
			readySentinels++
		}
	}
	sentinelState.Ready = readySentinels

	quorumTarget := int(expected/2) + 1
	if quorumTarget < 1 {
		quorumTarget = 1
	}
	sentinelState.QuorumTarget = quorumTarget

	opobs.ObserveSentinelReady(cr, readySentinels)
	opobs.ObserveSentinelQuorumTarget(cr, quorumTarget)

	if len(sentinelPods) < int(expected) {
		state.RequeueAfter(2 * time.Second)
		logger.Info("Sentinel pods not all created yet, requeueing", "statefulset", fmt.Sprintf("%s-sentinel", cr.Name), "current", len(sentinelPods), "expected", expected)
	}

	resetTimestamp := reconcile.NormalizeTimestamp(time.Now())
	tracker := reconcile.ReadSentinelResetTracker(cr)
	newTracker := tracker
	if !newTracker.CooldownUntil.IsZero() && !resetTimestamp.Before(newTracker.CooldownUntil) {
		newTracker.CooldownUntil = time.Time{}
	}

	sentinelQuorumOK := false
	sentinelQuorumReason := "SentinelQuorumUnknown"
	sentinelQuorumDetail := "sentinel quorum not evaluated"
	report := reconcile.SentinelQuorumReport{}

	if deps.SentinelFactory == nil {
		sentinelQuorumReason = "SentinelQuorumCheckFailed"
		sentinelQuorumDetail = "sentinel client factory not configured"
		logger.Error(errors.New("sentinel factory not configured"), "unable to check sentinel quorum")
	} else {
		var err error
		report, err = reconcile.HasSentinelQuorum(ctx, cr, sentinelPods, deps.SentinelFactory, quorumTarget, state.Security.SentinelClientOptions)
		sentinelQuorumOK = report.Healthy
		sentinelQuorumDetail = report.Detail
		if err != nil {
			sentinelQuorumReason = "SentinelQuorumCheckFailed"
			sentinelQuorumOK = false
			if sentinelQuorumDetail == "" {
				sentinelQuorumDetail = err.Error()
			}
			prev := reconcile.ConditionStatus(cr.Status.Conditions, keyvalv1alpha1.ConditionSentinelQuorum)
			if prev != nil && prev.Status == metav1.ConditionFalse && prev.Reason == sentinelQuorumReason && prev.Message == sentinelQuorumDetail {
				logger.V(1).Info("sentinel quorum check still failing", "detail", sentinelQuorumDetail)
			} else {
				logger.Error(err, "sentinel quorum check failed")
			}
		} else if sentinelQuorumOK {
			sentinelQuorumReason = "SentinelQuorumAchieved"
			if sentinelQuorumDetail == "" {
				sentinelQuorumDetail = fmt.Sprintf("sentinel quorum ok=%d target=%d ready=%d", report.OK, quorumTarget, readySentinels)
			}
		} else {
			sentinelQuorumReason = "SentinelQuorumLost"
			if sentinelQuorumDetail == "" {
				sentinelQuorumDetail = fmt.Sprintf("sentinel quorum ok=%d noquorum=%d ready=%d target=%d", report.OK, report.NoQuorum, readySentinels, quorumTarget)
			}
		}
	}

	if !sentinelQuorumOK && quorumTarget > 0 && readySentinels < quorumTarget && sentinelQuorumDetail == "" {
		sentinelQuorumDetail = fmt.Sprintf("ready sentinel pods %d/%d", readySentinels, quorumTarget)
	}

	opobs.SetSentinelQuorum(cr, sentinelQuorumOK)

	condStatus := metav1.ConditionTrue
	if !sentinelQuorumOK {
		condStatus = metav1.ConditionFalse
	}
	overrides[keyvalv1alpha1.ConditionSentinelQuorum] = &opstatus.ConditionState{
		Status:  condStatus,
		Reason:  sentinelQuorumReason,
		Message: sentinelQuorumDetail,
	}

	runtimeState := state.Runtime
	bootstrapCandidate := runtimeState.BootstrapCandidate

	if !sentinelQuorumOK {
		if !newTracker.CooldownUntil.IsZero() {
			logger.V(1).Info("clearing sentinel reset cooldown after quorum loss", "previous", newTracker.CooldownUntil)
			newTracker.CooldownUntil = time.Time{}
		}
		if _, ok := overrides[keyvalv1alpha1.ConditionDisruptionsPaused]; !ok {
			overrides[keyvalv1alpha1.ConditionDisruptionsPaused] = &opstatus.ConditionState{
				Status:  metav1.ConditionTrue,
				Reason:  "SentinelQuorum",
				Message: "disruptions paused until sentinel quorum recovers",
			}
		}
		if _, ok := overrides[keyvalv1alpha1.ConditionBootstrapInProgress]; !ok {
			overrides[keyvalv1alpha1.ConditionBootstrapInProgress] = &opstatus.ConditionState{
				Status:  metav1.ConditionTrue,
				Reason:  "SentinelQuorum",
				Message: "waiting for sentinel quorum",
			}
		}
		if bootstrapCandidate.Name != "" {
			if cond := overrides[keyvalv1alpha1.ConditionBootstrapInProgress]; cond != nil {
				cond.Message = fmt.Sprintf("%s; candidate %s via %s", cond.Message, bootstrapCandidate.Name, bootstrapCandidate.Source)
			}
		}
		state.RequeueAfter(5 * time.Second)
	}

	resetMode := sentinelResetModeNone
	if sentinelQuorumOK {
		if !tracker.LossSince.IsZero() {
			logger.Info("sentinel quorum restored", "lossSince", tracker.LossSince, "ready", readySentinels, "ok", report.OK, "target", quorumTarget)
			runtimeState.NeedSentinelReset = true
		}
		newTracker.LossSince = time.Time{}
		newTracker.Attempts = 0
	} else {
		resetMode = sentinelResetModeQuorumLoss
		if newTracker.LossSince.IsZero() {
			newTracker.LossSince = resetTimestamp
			logger.Info("sentinel quorum loss detected", "since", newTracker.LossSince, "detail", sentinelQuorumDetail)
		}
		if quorumTarget > 0 && readySentinels >= quorumTarget {
			runtimeState.NeedSentinelReset = true
		}
	}

	if runtimeState.NeedSentinelReset && resetMode == sentinelResetModeNone {
		resetMode = sentinelResetModeTopology
	}

	if resetMode != sentinelResetModeNone && deps.SentinelFactory != nil && len(sentinelPods) > 0 && quorumTarget > 0 {
		failoverActive := false
		if cond := overrides[keyvalv1alpha1.ConditionFailoverInProgress]; cond != nil && cond.Status == metav1.ConditionTrue {
			failoverActive = true
		}
		if !failoverActive {
			if cond := reconcile.ConditionStatus(cr.Status.Conditions, keyvalv1alpha1.ConditionFailoverInProgress); cond != nil && cond.Status == metav1.ConditionTrue {
				failoverActive = true
			}
		}
		if failoverActive {
			logger.V(1).Info("skip sentinel reset while failover in progress", "cluster", cr.Name)
			state.RequeueAfter(5 * time.Second)
		} else {
			now := reconcile.NormalizeTimestamp(time.Now())
			cooldownActive := !newTracker.CooldownUntil.IsZero() && newTracker.CooldownUntil.After(now)
			if cooldownActive {
				delay := time.Until(newTracker.CooldownUntil)
				if delay <= 0 {
					delay = 2 * time.Second
				}
				state.RequeueAfter(delay)
				logger.V(1).Info("sentinel reset cooldown active", "until", newTracker.CooldownUntil)
			} else if resetMode == sentinelResetModeQuorumLoss {
				if readySentinels < quorumTarget {
					logger.V(1).Info("waiting for ready sentinels before quorum-loss reset", "ready", readySentinels, "target", quorumTarget)
					state.RequeueAfter(5 * time.Second)
				} else {
					if newTracker.LossSince.IsZero() {
						newTracker.LossSince = now
					}
					elapsed := now.Sub(newTracker.LossSince)
					if elapsed < sentinelResetObservationWindow {
						wait := sentinelResetObservationWindow - elapsed
						if wait <= 0 {
							wait = 2 * time.Second
						}
						state.RequeueAfter(wait)
					} else if newTracker.Attempts >= sentinelResetMaxAttempts {
						newTracker.CooldownUntil = reconcile.NormalizeTimestamp(now.Add(sentinelResetCooldownFailure))
						delay := time.Until(newTracker.CooldownUntil)
						if delay <= 0 {
							delay = 2 * time.Second
						}
						state.RequeueAfter(delay)
						logger.Info("sentinel reset attempts exhausted; entering cooldown", "cooldownUntil", newTracker.CooldownUntil)
					} else {
						podName, err := reconcile.IssueSentinelReset(ctx, deps.SentinelFactory, sentinelPods, cr.Name, state.Security.SentinelClientOptions)
						newTracker.Attempts++
						newTracker.LossSince = now
						if err != nil {
							newTracker.CooldownUntil = reconcile.NormalizeTimestamp(now.Add(sentinelResetCooldownFailure))
							logger.Error(err, "sentinel reset failed")
							delay := time.Until(newTracker.CooldownUntil)
							if delay <= 0 {
								delay = 2 * time.Second
							}
							state.RequeueAfter(delay)
						} else {
							newTracker.CooldownUntil = reconcile.NormalizeTimestamp(now.Add(sentinelResetCooldownSuccess))
							logger.Info("sentinel reset issued", "pod", podName, "cooldownUntil", newTracker.CooldownUntil)
							opobs.EventSentinelResetDone(deps.Recorder, cr, podName)
							opobs.IncSentinelReset(cr)
							state.RequeueAfter(5 * time.Second)
						}
					}
				}
			} else if resetMode == sentinelResetModeTopology {
				if readySentinels < quorumTarget {
					logger.V(1).Info("deferring sentinel reset until enough sentinels ready", "ready", readySentinels, "target", quorumTarget)
					state.RequeueAfter(5 * time.Second)
				} else {
					podName, err := reconcile.IssueSentinelReset(ctx, deps.SentinelFactory, sentinelPods, cr.Name, state.Security.SentinelClientOptions)
					if err != nil {
						newTracker.CooldownUntil = reconcile.NormalizeTimestamp(now.Add(sentinelResetCooldownFailure))
						logger.Error(err, "sentinel reset (topology) failed")
						delay := time.Until(newTracker.CooldownUntil)
						if delay <= 0 {
							delay = 2 * time.Second
						}
						state.RequeueAfter(delay)
					} else {
						newTracker.Attempts = 0
						newTracker.CooldownUntil = reconcile.NormalizeTimestamp(now.Add(sentinelResetCooldownSuccess))
						logger.Info("sentinel reset issued after topology change", "pod", podName, "cooldownUntil", newTracker.CooldownUntil)
						opobs.EventSentinelResetDone(deps.Recorder, cr, podName)
						opobs.IncSentinelReset(cr)
						state.RequeueAfter(5 * time.Second)
					}
				}
			}
		}
	}

	if err := reconcile.WriteSentinelResetTracker(ctx, deps.Client, cr, newTracker); err != nil {
		logger.Error(err, "update sentinel reset tracker failed")
	}

	opobs.ObserveSentinelNoQuorum(cr, report.NoQuorum)
	if newTracker.LossSince.IsZero() {
		opobs.ObserveSentinelQuorumLoss(cr, nil)
	} else {
		loss := newTracker.LossSince
		opobs.ObserveSentinelQuorumLoss(cr, &loss)
	}

	state.Runtime = runtimeState

	sentinelState.QuorumOK = sentinelQuorumOK
	sentinelState.Reason = sentinelQuorumReason
	sentinelState.Detail = sentinelQuorumDetail
	state.Sentinel = sentinelState

	return nil
}
