package reconcile

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	ophealthy "github.com/ivelok/keyval-operator/controllers/internal/ops/health"
)

func TestRequeueAccumulator(t *testing.T) {
	var acc RequeueAccumulator
	if acc.Result() != 0 {
		t.Fatalf("expected zero result for empty accumulator")
	}
	acc.Set(5 * time.Second)
	if got := acc.Result(); got != 5*time.Second {
		t.Fatalf("expected 5s result, got %s", got)
	}
	acc.Set(2 * time.Second)
	if got := acc.Result(); got != 2*time.Second {
		t.Fatalf("expected tighter 2s result, got %s", got)
	}
	acc.SetMin(4 * time.Second)
	if got := acc.Result(); got != 4*time.Second {
		t.Fatalf("expected min floor to raise result to 4s, got %s", got)
	}
}

func TestAllowSentinelDisruptions(t *testing.T) {
	status := &keyvalv1alpha1.KeyValClusterStatus{
		HealthGate: &keyvalv1alpha1.HealthGateStatus{AllowDisruptions: true},
	}
	if !AllowSentinelDisruptions(status) {
		t.Fatalf("expected health gate to allow disruptions")
	}

	status = &keyvalv1alpha1.KeyValClusterStatus{
		Conditions: []metav1.Condition{
			{Type: string(keyvalv1alpha1.ConditionSentinelQuorum), Status: metav1.ConditionFalse, Reason: "SentinelQuorumNotReady"},
		},
	}
	if !AllowSentinelDisruptions(status) {
		t.Fatalf("expected recoverable sentinel reason to allow disruptions")
	}

	status = &keyvalv1alpha1.KeyValClusterStatus{
		Conditions: []metav1.Condition{{Type: string(keyvalv1alpha1.ConditionDisruptionsPaused), Status: metav1.ConditionTrue}},
	}
	if AllowSentinelDisruptions(status) {
		t.Fatalf("expected disruptions pause to block disruptions")
	}
}

func TestComputeRedisPDBMinAvailable(t *testing.T) {
	t.Parallel()

	makeCluster := func(mode keyvalv1alpha1.Mode, replicas int32) *keyvalv1alpha1.KeyValCluster {
		cr := &keyvalv1alpha1.KeyValCluster{}
		cr.Spec.Mode = mode
		cr.Spec.RedisReplicas = replicas
		return cr
	}

	settings := ophealthy.Settings{MinReplicasForSafety: 2}

	t.Run("no disruptions keeps all replicas", func(t *testing.T) {
		cr := makeCluster(keyvalv1alpha1.ModeStandalone, 3)
		if got := ComputeRedisPDBMinAvailable(cr, false, settings); got != 3 {
			t.Fatalf("want 3 minAvailable, got %d", got)
		}
	})

	t.Run("single replica standalone allows zero when disruptions enabled", func(t *testing.T) {
		cr := makeCluster(keyvalv1alpha1.ModeStandalone, 1)
		if got := ComputeRedisPDBMinAvailable(cr, true, settings); got != 0 {
			t.Fatalf("want 0 minAvailable, got %d", got)
		}
	})

	t.Run("multi replica standalone respects safety floor", func(t *testing.T) {
		cr := makeCluster(keyvalv1alpha1.ModeStandalone, 3)
		if got := ComputeRedisPDBMinAvailable(cr, true, settings); got != 2 {
			t.Fatalf("want 2 minAvailable, got %d", got)
		}
	})

	t.Run("standalone clamps safety to at least one when replicas > 1", func(t *testing.T) {
		cr := makeCluster(keyvalv1alpha1.ModeStandalone, 4)
		lowSafety := ophealthy.Settings{MinReplicasForSafety: 0}
		if got := ComputeRedisPDBMinAvailable(cr, true, lowSafety); got != 1 {
			t.Fatalf("want 1 minAvailable, got %d", got)
		}
	})

	t.Run("sentinel uses safety within limit", func(t *testing.T) {
		cr := makeCluster(keyvalv1alpha1.ModeSentinel, 3)
		if got := ComputeRedisPDBMinAvailable(cr, true, settings); got != 2 {
			t.Fatalf("want 2 minAvailable, got %d", got)
		}
	})

	t.Run("sentinel clamps safety to limit", func(t *testing.T) {
		cr := makeCluster(keyvalv1alpha1.ModeSentinel, 3)
		highSafety := ophealthy.Settings{MinReplicasForSafety: 5}
		if got := ComputeRedisPDBMinAvailable(cr, true, highSafety); got != 2 {
			t.Fatalf("want 2 minAvailable, got %d", got)
		}
	})
}

func TestComputeSentinelPDBMinAvailable(t *testing.T) {
	t.Parallel()
	sentinelCluster := func(count int32) *keyvalv1alpha1.KeyValCluster {
		return &keyvalv1alpha1.KeyValCluster{
			Spec: keyvalv1alpha1.KeyValClusterSpec{
				Mode:          keyvalv1alpha1.ModeSentinel,
				SentinelCount: int32Ptr(count),
			},
		}
	}

	cr := sentinelCluster(3)
	if got := ComputeSentinelPDBMinAvailable(cr, true); got != 2 {
		t.Fatalf("expected quorum 2, got %d", got)
	}
	if got := ComputeSentinelPDBMinAvailable(cr, false); got != 3 {
		t.Fatalf("expected hard mode 3, got %d", got)
	}
	cr.Spec.SentinelCount = int32Ptr(5)
	if got := ComputeSentinelPDBMinAvailable(cr, true); got != 3 {
		t.Fatalf("expected quorum 3, got %d", got)
	}
	cr.Spec.SentinelCount = int32Ptr(1)
	if got := ComputeSentinelPDBMinAvailable(cr, true); got != 1 {
		t.Fatalf("expected quorum floor 1, got %d", got)
	}
	cr.Spec.Mode = keyvalv1alpha1.ModeStandalone
	if got := ComputeSentinelPDBMinAvailable(cr, true); got != 0 {
		t.Fatalf("non-sentinel mode should return 0, got %d", got)
	}
}

func int32Ptr(v int32) *int32 { return &v }

func TestResolvePDBMinAvailable(t *testing.T) {
	t.Parallel()

	t.Run("drops to computed when no preserve annotation", func(t *testing.T) {
		min, keep := ResolvePDBMinAvailable(3, true, 2, nil)
		if min != 2 || keep {
			t.Fatalf("expected min=2 keep=false, got min=%d keep=%v", min, keep)
		}
	})

	t.Run("retains previous when preserve annotation set", func(t *testing.T) {
		annotations := map[string]string{PDBPreserveMinAnnotation: "true"}
		min, keep := ResolvePDBMinAvailable(3, true, 2, annotations)
		if min != 3 || !keep {
			t.Fatalf("expected min=3 keep=true, got min=%d keep=%v", min, keep)
		}
	})

	t.Run("ignores preserve when previous not larger", func(t *testing.T) {
		annotations := map[string]string{PDBPreserveMinAnnotation: "true"}
		min, keep := ResolvePDBMinAvailable(2, true, 3, annotations)
		if min != 3 || keep {
			t.Fatalf("expected min=3 keep=false, got min=%d keep=%v", min, keep)
		}
	})

	t.Run("ignores preserve when previous unknown", func(t *testing.T) {
		annotations := map[string]string{PDBPreserveMinAnnotation: "true"}
		min, keep := ResolvePDBMinAvailable(0, false, 2, annotations)
		if min != 2 || keep {
			t.Fatalf("expected min=2 keep=false, got min=%d keep=%v", min, keep)
		}
	})
}
