package reconcile

import (
	"time"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	ophealthy "github.com/ivelok/keyval-operator/controllers/internal/ops/health"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const PodLabelPatchMaxAttempts = 3

func RoleLabelConflictBackoff(attempt int) time.Duration {
	switch {
	case attempt <= 1:
		return 2 * time.Second
	case attempt == 2:
		return 4 * time.Second
	default:
		return 8 * time.Second
	}
}

// RequeueAccumulator collects requeue hints and emits the tightest next delay while
// respecting caller-provided minimums.
type RequeueAccumulator struct {
	min  time.Duration
	next time.Duration
}

// Set records a candidate requeue delay.
func (a *RequeueAccumulator) Set(d time.Duration) {
	if d <= 0 {
		return
	}
	if a.next == 0 || d < a.next {
		a.next = d
	}
	if a.min > 0 && a.next < a.min {
		a.next = a.min
	}
}

// SetMin raises the minimum allowed delay for subsequent Result calls.
func (a *RequeueAccumulator) SetMin(d time.Duration) {
	if d <= 0 {
		return
	}
	if a.min == 0 || d > a.min {
		a.min = d
	}
	if a.next > 0 && a.next < a.min {
		a.next = a.min
	}
}

// Result returns the effective delay.
func (a *RequeueAccumulator) Result() time.Duration {
	if a.next == 0 {
		return a.min
	}
	if a.min > 0 && a.next < a.min {
		return a.min
	}
	return a.next
}

// ConditionTrue reports whether the specified condition is True on the status object.
func ConditionTrue(status *keyvalv1alpha1.KeyValClusterStatus, typ keyvalv1alpha1.ConditionType) bool {
	if status == nil {
		return false
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == string(typ) && status.Conditions[i].Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// ConditionStatus returns the latest copy of the requested condition from the slice, if present.
func ConditionStatus(conds []metav1.Condition, typ keyvalv1alpha1.ConditionType) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == string(typ) {
			cond := conds[i]
			return &cond
		}
	}
	return nil
}

// EnsureSourceToRolesSource converts an EnsureSource to the CRD-facing RolesSource.
func EnsureSourceToRolesSource(source opreplication.EnsureSource) keyvalv1alpha1.RolesSource {
	switch source {
	case opreplication.EnsureSourceSentinel:
		return keyvalv1alpha1.RolesSourceSentinel
	case opreplication.EnsureSourceCache:
		return keyvalv1alpha1.RolesSourceSentinel
	case opreplication.EnsureSourceProbe:
		return keyvalv1alpha1.RolesSourceProbe
	case opreplication.EnsureSourceForced:
		return keyvalv1alpha1.RolesSourceForced
	default:
		return ""
	}
}

var sentinelQuorumRecoverableReasons = map[string]struct{}{
	"SentinelQuorumNotReady":    {},
	"InsufficientSentinels":     {},
	"SentinelQuorumLost":        {},
	"SentinelQuorumUnknown":     {},
	"SentinelQuorumCheckFailed": {},
	"SentinelQuorum":            {},
}

// AllowSentinelDisruptions decides whether voluntary disruptions may proceed given
// the current status snapshot.
func AllowSentinelDisruptions(status *keyvalv1alpha1.KeyValClusterStatus) bool {
	if status == nil {
		return false
	}
	if status.HealthGate != nil && status.HealthGate.AllowDisruptions {
		return true
	}
	sentinelCond := ConditionStatus(status.Conditions, keyvalv1alpha1.ConditionSentinelQuorum)
	paused := ConditionStatus(status.Conditions, keyvalv1alpha1.ConditionDisruptionsPaused)
	if sentinelCond != nil && sentinelCond.Status == metav1.ConditionFalse {
		if _, ok := sentinelQuorumRecoverableReasons[sentinelCond.Reason]; ok {
			if paused != nil && paused.Status == metav1.ConditionTrue && paused.Reason != "SentinelQuorum" {
				return false
			}
			return true
		}
	}
	if paused != nil && paused.Status == metav1.ConditionTrue {
		return false
	}
	return false
}

// PDBPreserveMinAnnotation is the annotation that signals the operator to keep an existing
// MinAvailable value when it is higher than the computed target.
const PDBPreserveMinAnnotation = "keyval.ivelok.io/preserve-min-available"

// ComputeRedisPDBMinAvailable calculates minAvailable for the Redis PDB given current settings.
func ComputeRedisPDBMinAvailable(cr *keyvalv1alpha1.KeyValCluster, allowDisruptions bool, settings ophealthy.Settings) int32 {
	desired := cr.Spec.RedisReplicas
	if desired < 0 {
		desired = 0
	}
	if !allowDisruptions {
		return desired
	}
	limit := desired - 1
	if limit < 0 {
		limit = 0
	}
	safety := settings.MinReplicasForSafety
	if safety < 0 {
		safety = 0
	}
	if safety > limit {
		safety = limit
	}
	if cr.Spec.Mode == keyvalv1alpha1.ModeStandalone {
		if desired <= 1 {
			return 0
		}
		if safety < 1 {
			safety = 1
		}
	}
	return safety
}

// SetMinAvailable mutates the PDB spec to enforce the provided minAvailable and clears MaxUnavailable.
func SetMinAvailable(pdb *policyv1.PodDisruptionBudget, min int32) {
	if pdb == nil {
		return
	}
	if min < 0 {
		min = 0
	}
	value := intstr.FromInt(int(min))
	pdb.Spec.MinAvailable = &value
	pdb.Spec.MaxUnavailable = nil
}

// ComputeSentinelPDBMinAvailable returns the minAvailable for Sentinel PDBs based on quorum rules.
func ComputeSentinelPDBMinAvailable(cr *keyvalv1alpha1.KeyValCluster, allowDisruptions bool) int32 {
	if cr.Spec.Mode != keyvalv1alpha1.ModeSentinel {
		return 0
	}
	desired := SentinelDesiredCount(cr)
	if desired <= 0 {
		return 0
	}
	if !allowDisruptions {
		return desired
	}
	quorum := desired/2 + 1
	if quorum > desired {
		quorum = desired
	}
	if quorum < 1 {
		quorum = 1
	}
	return quorum
}

// SentinelDesiredCount returns the desired sentinel replicas for the cluster.
func SentinelDesiredCount(cr *keyvalv1alpha1.KeyValCluster) int32 {
	if cr.Spec.SentinelCount != nil && *cr.Spec.SentinelCount > 0 {
		return *cr.Spec.SentinelCount
	}
	return 3
}

// MinAvailableValue extracts the integral minAvailable from a PDB spec.
func MinAvailableValue(spec policyv1.PodDisruptionBudgetSpec) (int32, bool) {
	if spec.MinAvailable == nil {
		return 0, false
	}
	min := spec.MinAvailable
	if min.Type != intstr.Int {
		return 0, false
	}
	return int32(min.IntValue()), true
}

// ResolvePDBMinAvailable decides whether to reuse the previous minAvailable when the
// preserve annotation is set and the previous value is larger.
func ResolvePDBMinAvailable(prevMin int32, prevMinOK bool, computedMin int32, annotations map[string]string) (int32, bool) {
	if AnnotationBoolTrue(annotations, PDBPreserveMinAnnotation) && prevMinOK && prevMin > computedMin {
		return prevMin, true
	}
	return computedMin, false
}

// AnnotationBoolTrue evaluates a boolean-style annotation.
func AnnotationBoolTrue(annotations map[string]string, key string) bool {
	if len(annotations) == 0 {
		return false
	}
	val, ok := annotations[key]
	if !ok {
		return false
	}
	switch val {
	case "true", "True", "TRUE", "1", "yes", "on":
		return true
	default:
		return false
	}
}
