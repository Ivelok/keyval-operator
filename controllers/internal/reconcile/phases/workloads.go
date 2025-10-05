package phases

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	opupdate "github.com/ivelok/keyval-operator/controllers/internal/ops/update"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
	"github.com/ivelok/keyval-operator/controllers/internal/ssa"
)

// Workloads reconciles Redis/Sentinel StatefulSets and records observed pods for downstream phases.
func Workloads(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	deps := state.Dependencies
	if deps.Client == nil {
		return controllererrors.WrapTransient(fmt.Errorf("workloads phase missing client"))
	}
	if deps.Scheme == nil {
		return controllererrors.WrapTransient(fmt.Errorf("workloads phase missing scheme"))
	}

	cr := state.Cluster
	logger := state.Logger
	secSettings := state.Security.Settings
	tlsHash := state.Security.TLSHash

	hash := state.Config.ConfigHash
	if hash == "" {
		hash = resources.ConfigHash(cr, &secSettings)
		state.Config.ConfigHash = hash
	}

	overrides := state.EnsureConditionOverrides()

	// Ensure Redis StatefulSet via SSA.
	ssDesired := resources.StatefulSet(cr, hash, tlsHash, &secSettings)
	var existingSS appsv1.StatefulSet
	if err := deps.Client.Get(ctx, client.ObjectKey{Namespace: ssDesired.Namespace, Name: ssDesired.Name}, &existingSS); err == nil {
		if len(existingSS.Spec.VolumeClaimTemplates) > 0 && len(ssDesired.Spec.VolumeClaimTemplates) > 0 {
			ssDesired.Spec.VolumeClaimTemplates[0].Spec = existingSS.Spec.VolumeClaimTemplates[0].Spec
		}
	} else if !apierrors.IsNotFound(err) {
		return controllererrors.WrapTransient(fmt.Errorf("get statefulset: %w", err))
	}

	prevTLSHash := ""
	if existingSS.Spec.Template.Annotations != nil {
		prevTLSHash = existingSS.Spec.Template.Annotations[resources.TLSSecretHashAnnotationKey]
	}
	if tlsHash != "" && prevTLSHash != tlsHash && secSettings.TLS.SecretName != "" {
		opobs.EventTLSSecretRotated(deps.Recorder, cr, opupdate.ComponentRedis, secSettings.TLS.SecretName, prevTLSHash, tlsHash)
		opobs.IncTLSSecretRotation(cr, opupdate.ComponentRedis)
	}

	ssApply := ssa.StatefulSet(ssDesired)
	if err := controllerutil.SetOwnerReference(cr, ssApply, deps.Scheme); err != nil {
		return controllererrors.WrapTransient(fmt.Errorf("set owner on statefulset: %w", err))
	}
	redisLogger := logger.WithValues("component", opupdate.ComponentRedis, "statefulset", ssDesired.Name)
	if err := deps.Client.Patch(ctx, ssApply, client.Apply, client.FieldOwner(core.FieldOwner)); err != nil {
		if conflict := detectStatefulSetApplyConflict(err); conflict != nil {
			message := formatApplyConflictMessage(ssDesired.Name, conflict)
			redisLogger.Error(err, "apply redis statefulset conflict", "manager", conflict.Manager, "field", conflict.Field)
			opobs.EventRedisApplyConflict(deps.Recorder, cr, conflict.Manager, conflict.Field, conflict.Message)
			opobs.IncRedisApplyFailure(cr, opobs.RedisApplyFailureReasonConflict)
			overrides[keyvalv1alpha1.ConditionReconciled] = &opstatus.ConditionState{
				Status:  metav1.ConditionFalse,
				Reason:  conflictConditionReason,
				Message: message,
			}
			state.AbortWithError(controllererrors.WrapFatal(fmt.Errorf("apply redis statefulset conflict: %w", err)))
			return nil
		}
		redisLogger.Error(err, "apply redis statefulset failed")
		opobs.EventRedisApplyFailed(deps.Recorder, cr, err)
		opobs.IncRedisApplyFailure(cr, opobs.RedisApplyFailureReasonError)
		return controllererrors.WrapTransient(fmt.Errorf("apply statefulset: %w", err))
	}
	state.RedisStatefulSet = ssApply.DeepCopy()

	pods, err := runtimepkg.ListStatefulSetPods(ctx, deps.Client, ssApply)
	if err != nil {
		return controllererrors.WrapTransient(fmt.Errorf("list pods: %w", err))
	}
	if deps.ObserveGracefulShutdown != nil {
		deps.ObserveGracefulShutdown(cr, pods)
	}
	state.RedisPods = pods

	if len(pods) == 0 && cr.Spec.RedisReplicas > 0 {
		delay := 2 * time.Second
		if state.ApplyBackoff != nil {
			if backoff := state.ApplyBackoff(delay); backoff > 0 {
				delay = backoff
			}
		}
		state.RequeueAfter(delay)
		// No further work possible until pods exist; downstream phases will no-op with empty pod list.
	}

	if cr.Spec.Mode != keyvalv1alpha1.ModeSentinel {
		return nil
	}

	ssSent := resources.SentinelStatefulSet(cr, hash, tlsHash, &secSettings)
	ssSentApply := ssa.StatefulSet(ssSent)
	if err := controllerutil.SetOwnerReference(cr, ssSentApply, deps.Scheme); err != nil {
		return controllererrors.WrapTransient(fmt.Errorf("set owner on sentinel statefulset: %w", err))
	}

	sentinelLogger := logger.WithValues("component", opupdate.ComponentSentinel, "statefulset", ssSent.Name)
	if err := deps.Client.Patch(ctx, ssSentApply, client.Apply, client.FieldOwner(core.FieldOwner)); err != nil {
		if conflict := detectStatefulSetApplyConflict(err); conflict != nil {
			message := formatApplyConflictMessage(ssSent.Name, conflict)
			overrides[keyvalv1alpha1.ConditionReconciled] = &opstatus.ConditionState{
				Status:  metav1.ConditionFalse,
				Reason:  conflictConditionReason,
				Message: message,
			}
			sentinelLogger.Error(err, "apply sentinel statefulset conflict", "manager", conflict.Manager, "field", conflict.Field)
			opobs.EventSentinelApplyConflict(deps.Recorder, cr, conflict.Manager, conflict.Field, conflict.Message)
			opobs.IncSentinelApplyFailure(cr, opobs.SentinelApplyFailureReasonConflict)
			state.AbortWithError(controllererrors.WrapFatal(fmt.Errorf("apply sentinel statefulset conflict: %w", err)))
			return nil
		}
		if cause := detectSentinelImmutableCause(err); cause != nil {
			field := cause.Field
			detail := cause.Message
			sentinelLogger.Error(nil, "sentinel statefulset apply rejected", "field", field, "detail", detail)
			overrides[keyvalv1alpha1.ConditionReconciled] = &opstatus.ConditionState{
				Status:  metav1.ConditionFalse,
				Reason:  "ImmutableField",
				Message: formatSentinelImmutableMessage(field, detail),
			}
			opobs.EventSentinelApplyImmutableField(deps.Recorder, cr, field, detail)
			opobs.IncSentinelApplyFailure(cr, opobs.SentinelApplyFailureReasonImmutable)
		} else {
			sentinelLogger.Error(err, "apply sentinel statefulset failed", "field", "")
			opobs.EventSentinelApplyFailed(deps.Recorder, cr, err)
			opobs.IncSentinelApplyFailure(cr, opobs.SentinelApplyFailureReasonError)
			return controllererrors.WrapTransient(fmt.Errorf("apply sentinel statefulset: %w", err))
		}
	}

	sentinelPods, err := runtimepkg.ListStatefulSetPods(ctx, deps.Client, ssSentApply)
	if err != nil {
		return controllererrors.WrapTransient(fmt.Errorf("list sentinel pods: %w", err))
	}
	if deps.ObserveGracefulShutdown != nil {
		deps.ObserveGracefulShutdown(cr, sentinelPods)
	}
	state.SentinelPods = sentinelPods
	state.SentinelStatefulSet = ssSentApply.DeepCopy()

	expectedSentinelCount := reconcile.SentinelDesiredCount(cr)
	if len(sentinelPods) < int(expectedSentinelCount) {
		logger.Info("Sentinel pods not all created yet, requeueing", "statefulset", ssSent.Name, "current", len(sentinelPods), "expected", expectedSentinelCount)
		state.RequeueAfter(2 * time.Second)
	}

	return nil
}

type sentinelImmutableCause struct {
	Field   string
	Message string
}

func detectSentinelImmutableCause(err error) *sentinelImmutableCause {
	if err == nil || !apierrors.IsInvalid(err) {
		return nil
	}
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		return nil
	}
	details := statusErr.Status().Details
	if details != nil {
		for i := range details.Causes {
			cause := details.Causes[i]
			if isSentinelImmutableCause(cause) {
				field := cause.Field
				if field == "" {
					field = "spec"
				}
				msg := cause.Message
				if msg == "" {
					msg = statusErr.Status().Message
				}
				return &sentinelImmutableCause{Field: field, Message: msg}
			}
		}
	}
	message := statusErr.Status().Message
	if containsImmutableHint(message) {
		return &sentinelImmutableCause{Field: "spec", Message: message}
	}
	return nil
}

func containsImmutableHint(message string) bool {
	lower := strings.ToLower(message)
	return strings.Contains(lower, "immutable") || strings.Contains(lower, "updates to statefulset spec")
}

func isSentinelImmutableCause(cause metav1.StatusCause) bool {
	field := cause.Field
	for _, prefix := range []string{"spec.serviceName", "spec.selector", "spec.volumeClaimTemplates"} {
		if strings.HasPrefix(field, prefix) {
			return true
		}
	}
	return containsImmutableHint(cause.Message)
}

func formatSentinelImmutableMessage(field, detail string) string {
	if field == "" {
		field = "spec"
	}
	base := fmt.Sprintf("Sentinel StatefulSet field %s is immutable", field)
	detail = strings.TrimSpace(detail)
	if detail == "" {
		return base
	}
	return fmt.Sprintf("%s: %s", base, detail)
}

const conflictConditionReason = "ApplyConflict"

type statefulSetApplyConflict struct {
	Manager string
	Field   string
	Message string
}

var conflictRegex = regexp.MustCompile(`conflict with "([^"]+)":\s*(.+)`) // best-effort extract manager and field

func detectStatefulSetApplyConflict(err error) *statefulSetApplyConflict {
	if err == nil || !apierrors.IsConflict(err) {
		return nil
	}
	conflict := &statefulSetApplyConflict{}
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) {
		status := statusErr.Status()
		if details := status.Details; details != nil {
			for i := range details.Causes {
				cause := details.Causes[i]
				if cause.Message != "" && conflict.Message == "" {
					conflict.Message = strings.TrimSpace(cause.Message)
				}
				if cause.Field != "" && conflict.Field == "" {
					conflict.Field = cause.Field
				}
			}
		}
		if conflict.Message == "" {
			conflict.Message = strings.TrimSpace(status.Message)
		}
	}
	if conflict.Message == "" {
		conflict.Message = strings.TrimSpace(err.Error())
	}
	if matches := conflictRegex.FindStringSubmatch(conflict.Message); len(matches) == 3 {
		conflict.Manager = matches[1]
		if conflict.Field == "" {
			conflict.Field = matches[2]
		}
	}
	conflict.Field = strings.TrimSpace(conflict.Field)
	conflict.Manager = strings.TrimSpace(conflict.Manager)
	if conflict.Message == "" {
		return nil
	}
	return conflict
}

func formatApplyConflictMessage(statefulSetName string, conflict *statefulSetApplyConflict) string {
	base := fmt.Sprintf("StatefulSet %s apply conflict", statefulSetName)
	if conflict == nil {
		return base
	}
	parts := []string{base}
	if conflict.Field != "" {
		parts = append(parts, fmt.Sprintf("field %s", conflict.Field))
	}
	if conflict.Manager != "" {
		parts = append(parts, fmt.Sprintf("owned by field manager %q", conflict.Manager))
	}
	if conflict.Message != "" {
		parts = append(parts, conflict.Message)
	}
	parts = append(parts, "release the conflicting manager or reapply with --force-conflicts")
	return strings.Join(parts, ": ")
}
