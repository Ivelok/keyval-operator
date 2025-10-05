package health

import (
	"testing"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	clientspkg "github.com/ivelok/keyval-operator/controllers/internal/clients"
)

func TestEvaluatePodHealth_ReplicaLagging(t *testing.T) {
	t.Parallel()
	info := clientspkg.ReplicationInfo{
		Role:                   "slave",
		MasterLinkStatus:       "up",
		MasterLastIOSecondsAgo: 8,
		MasterReplOffset:       100,
		ReplicaReplOffset:      90,
	}
	h, lag := EvaluatePodHealth(keyvalv1alpha1.PodRoleReplica, info, true, 5)
	if h != keyvalv1alpha1.PodHealthLagging {
		t.Fatalf("expected lagging, got %s", h)
	}
	if lag == nil || *lag != 10 {
		t.Fatalf("expected lag pointer 10, got %v", lag)
	}
}

func TestEvaluatePodHealth_IdleReplicaHealthy(t *testing.T) {
	t.Parallel()
	info := clientspkg.ReplicationInfo{
		Role:                   "slave",
		MasterLinkStatus:       "up",
		MasterLastIOSecondsAgo: 12,
		MasterReplOffset:       100,
		ReplicaReplOffset:      100,
	}
	h, lag := EvaluatePodHealth(keyvalv1alpha1.PodRoleReplica, info, true, 5)
	if h != keyvalv1alpha1.PodHealthHealthy {
		t.Fatalf("expected healthy for idle replica, got %s", h)
	}
	if lag == nil || *lag <= 0 {
		t.Fatalf("expected lag pointer with idle age, got %v", lag)
	}
}

func TestEvaluatePodHealth_ReplicaDesynced(t *testing.T) {
	t.Parallel()
	info := clientspkg.ReplicationInfo{Role: "replica", MasterLinkStatus: "down"}
	h, _ := EvaluatePodHealth(keyvalv1alpha1.PodRoleReplica, info, true, 5)
	if h != keyvalv1alpha1.PodHealthDesynced {
		t.Fatalf("expected desynced, got %s", h)
	}
}

func TestLagThresholdDefault(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{}
	if v := LagThresholdSeconds(cr); v != defaultLagThresholdSeconds {
		t.Fatalf("expected default threshold %d, got %d", defaultLagThresholdSeconds, v)
	}
	cr.Spec.ReplicationHealth = &keyvalv1alpha1.ReplicationHealthSpec{LagThresholdSeconds: 12}
	if v := LagThresholdSeconds(cr); v != 12 {
		t.Fatalf("expected legacy override 12, got %d", v)
	}
	cr.Spec.Health = &keyvalv1alpha1.HealthSpec{ReplicationLagSecondsMax: 9}
	if v := LagThresholdSeconds(cr); v != 9 {
		t.Fatalf("expected health override 9, got %d", v)
	}
}

func TestSettingsFor_DefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{}
	settings := SettingsFor(cr)
	if settings.LagThresholdSeconds != defaultLagThresholdSeconds {
		t.Fatalf("expected default lag %d, got %d", defaultLagThresholdSeconds, settings.LagThresholdSeconds)
	}
	if settings.FailoverTimeoutSeconds != defaultFailoverTimeoutSeconds {
		t.Fatalf("expected default failover %d, got %d", defaultFailoverTimeoutSeconds, settings.FailoverTimeoutSeconds)
	}
	if settings.MinReplicasForSafety != defaultMinReplicasForSafety {
		t.Fatalf("expected default safety %d, got %d", defaultMinReplicasForSafety, settings.MinReplicasForSafety)
	}
	cr.Spec.Health = &keyvalv1alpha1.HealthSpec{
		ReplicationLagSecondsMax: 3,
		FailoverTimeoutSeconds:   45,
		MinReplicasForSafety:     4,
	}
	settings = SettingsFor(cr)
	if settings.LagThresholdSeconds != 3 {
		t.Fatalf("expected lag override 3, got %d", settings.LagThresholdSeconds)
	}
	if settings.FailoverTimeoutSeconds != 45 {
		t.Fatalf("expected failover override 45, got %d", settings.FailoverTimeoutSeconds)
	}
	if settings.MinReplicasForSafety != 4 {
		t.Fatalf("expected safety override 4, got %d", settings.MinReplicasForSafety)
	}
	cr.Spec.ReplicationHealth = &keyvalv1alpha1.ReplicationHealthSpec{LagThresholdSeconds: 7}
	settings = SettingsFor(cr)
	if settings.LagThresholdSeconds != 3 {
		t.Fatalf("expected health spec to take precedence (3), got %d", settings.LagThresholdSeconds)
	}
}
