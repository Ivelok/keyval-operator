package health

import (
	"math"
	"strings"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	clientspkg "github.com/ivelok/keyval-operator/controllers/internal/clients"
)

const (
	defaultLagThresholdSeconds    int32 = 5
	defaultFailoverTimeoutSeconds int32 = 20
	defaultMinReplicasForSafety   int32 = 2

	DefaultLagThresholdSeconds int32 = defaultLagThresholdSeconds
)

// Settings represents resolved health thresholds for a cluster.
type Settings struct {
	LagThresholdSeconds    int32
	FailoverTimeoutSeconds int32
	MinReplicasForSafety   int32
}

// SettingsFor resolves health settings using spec.health with backward compatibility for spec.replicationHealth.
func SettingsFor(cr *keyvalv1alpha1.KeyValCluster) Settings {
	settings := Settings{
		LagThresholdSeconds:    defaultLagThresholdSeconds,
		FailoverTimeoutSeconds: defaultFailoverTimeoutSeconds,
		MinReplicasForSafety:   defaultMinReplicasForSafety,
	}
	if hs := cr.Spec.Health; hs != nil {
		if hs.ReplicationLagSecondsMax >= 0 {
			settings.LagThresholdSeconds = hs.ReplicationLagSecondsMax
		}
		if hs.FailoverTimeoutSeconds > 0 {
			settings.FailoverTimeoutSeconds = hs.FailoverTimeoutSeconds
		}
		if hs.MinReplicasForSafety > 0 {
			settings.MinReplicasForSafety = hs.MinReplicasForSafety
		}
	}
	if legacy := cr.Spec.ReplicationHealth; legacy != nil && cr.Spec.Health == nil {
		if legacy.LagThresholdSeconds > 0 {
			settings.LagThresholdSeconds = legacy.LagThresholdSeconds
		}
	}
	if settings.MinReplicasForSafety < 1 {
		settings.MinReplicasForSafety = 1
	}
	return settings
}

func LagThresholdSeconds(cr *keyvalv1alpha1.KeyValCluster) int32 {
	return SettingsFor(cr).LagThresholdSeconds
}

func EvaluatePodHealth(role keyvalv1alpha1.PodRole, info clientspkg.ReplicationInfo, ready bool, threshold int32) (keyvalv1alpha1.PodHealth, *int32) {
	lagPtr := lagPointer(info)

	switch role {
	case keyvalv1alpha1.PodRoleSentinel:
		if ready {
			return keyvalv1alpha1.PodHealthHealthy, nil
		}
		return keyvalv1alpha1.PodHealthOffline, nil
	case keyvalv1alpha1.PodRoleMaster:
		if info.Role == "" && !ready {
			return keyvalv1alpha1.PodHealthOffline, nil
		}
		return keyvalv1alpha1.PodHealthHealthy, lagPtr
	case keyvalv1alpha1.PodRoleReplica:
		if info.Role == "" {
			return keyvalv1alpha1.PodHealthOffline, lagPtr
		}
		status := keyvalv1alpha1.PodHealthHealthy
		link := info.MasterLinkStatus
		if link != "" && !stringsEqualFoldAny(link, "up", "ok") {
			return keyvalv1alpha1.PodHealthDesynced, lagPtr
		}
		if info.MasterSyncInProgress {
			status = keyvalv1alpha1.PodHealthLagging
		}
		if info.MasterReplOffset > 0 && info.ReplicaReplOffset > 0 {
			diffLag := computeLagSeconds(info.MasterReplOffset, info.ReplicaReplOffset)
			lagPtr = mergeLagPointers(lagPtr, diffLag)
			if diffLag != nil && *diffLag > 0 && status == keyvalv1alpha1.PodHealthHealthy {
				status = keyvalv1alpha1.PodHealthLagging
			}
		}
		return status, lagPtr
	default:
		return keyvalv1alpha1.PodHealthOffline, lagPtr
	}
}

func stringsEqualFoldAny(s string, values ...string) bool {
	for _, v := range values {
		if strings.EqualFold(s, v) {
			return true
		}
	}
	return false
}

func computeLagSeconds(masterOffset, replicaOffset int64) *int32 {
	if masterOffset <= 0 || replicaOffset <= 0 {
		return nil
	}
	diff := masterOffset - replicaOffset
	if diff <= 0 {
		return nil
	}
	if diff > math.MaxInt32 {
		diff = math.MaxInt32
	}
	lag := int32(diff)
	return &lag
}

func lagPointer(info clientspkg.ReplicationInfo) *int32 {
	var lagCandidates []*int32
	if info.MasterLastIOSecondsAgo > 0 {
		lag := int32(info.MasterLastIOSecondsAgo)
		lagCandidates = append(lagCandidates, &lag)
	}
	if off := computeLagSeconds(info.MasterReplOffset, info.ReplicaReplOffset); off != nil {
		lagCandidates = append(lagCandidates, off)
	}
	return mergeLagPointers(lagCandidates...)
}

func mergeLagPointers(ptrs ...*int32) *int32 {
	var result *int32
	max := int32(0)
	for _, p := range ptrs {
		if p == nil {
			continue
		}
		if result == nil || *p > max {
			v := *p
			result = &v
			max = v
		}
	}
	return result
}
