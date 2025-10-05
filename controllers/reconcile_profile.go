package controllers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// reconcileProfileEnvVar controls the debug-only manual override for the reconcile profile.
const reconcileProfileEnvVar = "KEYVAL_OPERATOR_RECONCILE_PROFILE"

// ReconcileProfileName identifies a tuning profile.
type ReconcileProfileName string

const (
	ReconcileProfileSmall  ReconcileProfileName = "small"
	ReconcileProfileMedium ReconcileProfileName = "medium"
	ReconcileProfileLarge  ReconcileProfileName = "large"
)

// ReconcileProfile contains concurrency, Kubernetes client rate limits, and cache tuning knobs.
type ReconcileProfile struct {
	Name                    string
	MaxConcurrentReconciles int
	ClientQPS               float32
	ClientBurst             int
	CacheTTL                time.Duration
}

var reconcileProfiles = map[ReconcileProfileName]ReconcileProfile{
	ReconcileProfileSmall: {
		Name:                    string(ReconcileProfileSmall),
		MaxConcurrentReconciles: 2,
		ClientQPS:               15,
		ClientBurst:             30,
		CacheTTL:                200 * time.Millisecond,
	},
	ReconcileProfileMedium: {
		Name:                    string(ReconcileProfileMedium),
		MaxConcurrentReconciles: 4,
		ClientQPS:               30,
		ClientBurst:             60,
		CacheTTL:                200 * time.Millisecond,
	},
	ReconcileProfileLarge: {
		Name:                    string(ReconcileProfileLarge),
		MaxConcurrentReconciles: 8,
		ClientQPS:               60,
		ClientBurst:             120,
		CacheTTL:                200 * time.Millisecond,
	},
}

// ReconcileProfileSnapshot summarises the observed cluster workload when selecting a profile.
type ReconcileProfileSnapshot struct {
	ClusterCount       int
	SentinelClusters   int
	RedisReplicas      int
	SentinelMembers    int
	StandaloneClusters int
}

// WorkloadUnits approximates the total number of pods managed by the operator.
func (s ReconcileProfileSnapshot) WorkloadUnits() int {
	un := s.RedisReplicas + s.SentinelMembers
	if un == 0 {
		return s.ClusterCount
	}
	return un
}

// ReconcileProfileSelection captures the resolved profile and inputs used to compute it.
type ReconcileProfileSelection struct {
	Profile  ReconcileProfile
	Name     ReconcileProfileName
	Source   string
	Override bool
	Snapshot ReconcileProfileSnapshot
}

// CacheCapacityHint returns a bounded capacity recommendation for the master-address cache.
func (s ReconcileProfileSelection) CacheCapacityHint() int {
	units := s.Snapshot.WorkloadUnits()
	if units <= 0 {
		units = s.Profile.MaxConcurrentReconciles * 4
	}
	cap := units * 2
	if cap < 64 {
		cap = 64
	}
	if cap > 2048 {
		cap = 2048
	}
	return cap
}

// ResolveReconcileProfile determines the reconcile profile, optionally using the live cluster state.
func ResolveReconcileProfile(ctx context.Context, cfg *rest.Config, scheme *runtime.Scheme) (ReconcileProfileSelection, error) {
	selection := ReconcileProfileSelection{
		Name:    ReconcileProfileSmall,
		Profile: reconcileProfiles[ReconcileProfileSmall],
		Source:  "default",
	}

	override := strings.TrimSpace(os.Getenv(reconcileProfileEnvVar))
	if override != "" {
		name := ReconcileProfileName(strings.ToLower(override))
		profile, ok := reconcileProfiles[name]
		if !ok {
			return selection, fmt.Errorf("unknown reconcile profile override %q", override)
		}
		selection.Name = name
		selection.Profile = profile
		selection.Source = "override"
		selection.Override = true
		return selection, nil
	}

	if cfg == nil {
		return selection, errors.New("kubernetes rest config is nil")
	}
	if scheme == nil {
		return selection, errors.New("runtime scheme is nil")
	}

	cli, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return selection, fmt.Errorf("build kube client: %w", err)
	}

	var list keyvalv1alpha1.KeyValClusterList
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cli.List(ctx, &list); err != nil {
		return selection, fmt.Errorf("list KeyValClusters: %w", err)
	}

	snapshot := snapshotForClusters(list.Items)
	selection.Snapshot = snapshot
	selection.Name = chooseProfile(snapshot)
	selection.Profile = reconcileProfiles[selection.Name]
	selection.Source = "auto"
	return selection, nil
}

func snapshotForClusters(clusters []keyvalv1alpha1.KeyValCluster) ReconcileProfileSnapshot {
	s := ReconcileProfileSnapshot{}
	for i := range clusters {
		cr := clusters[i]
		s.ClusterCount++
		redis := cr.Spec.RedisReplicas
		if redis < 0 {
			redis = 0
		}
		s.RedisReplicas += int(redis)
		if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
			s.SentinelClusters++
			members := int32(0)
			if cr.Spec.SentinelCount != nil {
				members = *cr.Spec.SentinelCount
			}
			if members < 0 {
				members = 0
			}
			s.SentinelMembers += int(members)
		} else {
			s.StandaloneClusters++
		}
	}
	return s
}

func chooseProfile(snapshot ReconcileProfileSnapshot) ReconcileProfileName {
	workload := snapshot.WorkloadUnits()
	switch {
	case snapshot.ClusterCount == 0:
		return ReconcileProfileSmall
	case workload <= 60 && snapshot.ClusterCount <= 20:
		return ReconcileProfileSmall
	case workload <= 180 && snapshot.ClusterCount <= 70:
		return ReconcileProfileMedium
	default:
		return ReconcileProfileLarge
	}
}
