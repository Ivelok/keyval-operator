package phases

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
)

func TestRuntimeFallbackWithoutFactory(t *testing.T) {
	scheme := newScheme(t)

	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 2,
		},
	}
	cluster.Status.ReadyReplicas = 1

	readyPod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-redis-0", Namespace: "default"}}
	readyPod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	pendingPod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-redis-1", Namespace: "default"}}

	state := &reconcile.State{
		Cluster:      cluster.DeepCopy(),
		Logger:       newLogger(),
		RedisPods:    []corev1.Pod{readyPod, pendingPod},
		SentinelPods: nil,
		Dependencies: reconcile.Dependencies{
			Client:   fake.NewClientBuilder().WithScheme(scheme).Build(),
			Recorder: record.NewFakeRecorder(5),
			Scheme:   scheme,
		},
		Accumulator:        &reconcile.RequeueAccumulator{},
		ConditionOverrides: make(map[keyvalv1alpha1.ConditionType]*status.ConditionState),
	}

	if err := Runtime(context.Background(), state); err != nil {
		t.Fatalf("runtime phase: %v", err)
	}

	runtimeState := state.Runtime
	if runtimeState.Master != "demo-redis-0" {
		t.Fatalf("expected master demo-redis-0, got %s", runtimeState.Master)
	}
	if runtimeState.ReadyCount != 1 {
		t.Fatalf("expected ReadyCount=1, got %d", runtimeState.ReadyCount)
	}
	if !runtimeState.AnyNotReady {
		t.Fatalf("expected AnyNotReady to be true")
	}
	if delay := state.Accumulator.Result(); delay != 4*time.Second {
		t.Fatalf("expected requeue after 4s, got %v", delay)
	}
	if len(runtimeState.RuntimeResults) == 0 {
		t.Fatalf("expected runtime results recorded")
	}
}
