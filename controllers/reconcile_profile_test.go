package controllers

import (
	"testing"
	"time"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

func TestChooseProfile(t *testing.T) {
	cases := []struct {
		name     string
		snapshot ReconcileProfileSnapshot
		expect   ReconcileProfileName
	}{
		{name: "empty", snapshot: ReconcileProfileSnapshot{}, expect: ReconcileProfileSmall},
		{name: "small-workload", snapshot: ReconcileProfileSnapshot{ClusterCount: 12, RedisReplicas: 36}, expect: ReconcileProfileSmall},
		{name: "medium-workload", snapshot: ReconcileProfileSnapshot{ClusterCount: 30, RedisReplicas: 90, SentinelMembers: 30}, expect: ReconcileProfileMedium},
		{name: "large-workload", snapshot: ReconcileProfileSnapshot{ClusterCount: 90, RedisReplicas: 210, SentinelMembers: 90}, expect: ReconcileProfileLarge},
	}

	for _, tc := range cases {
		if got := chooseProfile(tc.snapshot); got != tc.expect {
			t.Fatalf("%s: expected %s, got %s", tc.name, tc.expect, got)
		}
	}
}

func TestSnapshotForClusters(t *testing.T) {
	clusters := []keyvalv1alpha1.KeyValCluster{
		{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, RedisReplicas: 1}},
		{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, RedisReplicas: 3, SentinelCount: int32Ptr(5)}},
	}
	s := snapshotForClusters(clusters)
	if s.ClusterCount != 2 {
		t.Fatalf("expected 2 clusters, got %d", s.ClusterCount)
	}
	if s.RedisReplicas != 4 {
		t.Fatalf("expected 4 redis replicas, got %d", s.RedisReplicas)
	}
	if s.SentinelMembers != 5 {
		t.Fatalf("expected 5 sentinel members, got %d", s.SentinelMembers)
	}
	if s.SentinelClusters != 1 {
		t.Fatalf("expected 1 sentinel cluster, got %d", s.SentinelClusters)
	}
	if s.StandaloneClusters != 1 {
		t.Fatalf("expected 1 standalone cluster, got %d", s.StandaloneClusters)
	}
}

func TestCacheCapacityHint(t *testing.T) {
	selection := ReconcileProfileSelection{
		Profile:  ReconcileProfile{Name: "small", MaxConcurrentReconciles: 2, CacheTTL: 200 * time.Millisecond},
		Snapshot: ReconcileProfileSnapshot{ClusterCount: 5, RedisReplicas: 10, SentinelMembers: 0},
	}
	cap := selection.CacheCapacityHint()
	if cap < 64 {
		t.Fatalf("expected capacity >= 64, got %d", cap)
	}
	selection.Snapshot = ReconcileProfileSnapshot{ClusterCount: 0}
	cap = selection.CacheCapacityHint()
	if cap < 64 {
		t.Fatalf("expected fallback capacity >= 64, got %d", cap)
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}
