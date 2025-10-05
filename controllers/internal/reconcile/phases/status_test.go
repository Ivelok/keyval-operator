package phases

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"k8s.io/client-go/tools/record"
)

func TestStatusPhaseUpdatesStatusAndDisruption(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: "keyval.ivelok.io/v1alpha1", Kind: "KeyValCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 1,
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy()).
		WithStatusSubresource(cluster).
		Build()

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-0",
			Namespace: cluster.Namespace,
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}

	state := &reconcile.State{
		Logger: newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   client,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		ConditionOverrides: make(map[keyvalv1alpha1.ConditionType]*status.ConditionState),
		RedisPods:          []corev1.Pod{pod},
		Runtime: reconcile.RuntimeState{
			Master: "demo-0",
			Roles:  map[string]keyvalv1alpha1.PodRole{"demo-0": keyvalv1alpha1.PodRoleMaster},
			Health: map[string]keyvalv1alpha1.PodHealth{"demo-0": keyvalv1alpha1.PodHealthHealthy},
		},
	}

	clusterCopy := cluster.DeepCopy()
	if err := Status(context.Background(), clusterCopy, state); err != nil {
		t.Fatalf("status phase failed: %v", err)
	}

	if !state.Disruption.AllowDisruptions {
		t.Fatalf("expected disruptions to be allowed")
	}
	if got := state.Status.Computed.MasterPod; got != "demo-0" {
		t.Fatalf("expected master pod demo-0, got %s", got)
	}

	var updated keyvalv1alpha1.KeyValCluster
	if err := client.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, &updated); err != nil {
		t.Fatalf("get updated cluster: %v", err)
	}
	if updated.Status.MasterPod != "demo-0" {
		t.Fatalf("expected persisted master pod demo-0, got %s", updated.Status.MasterPod)
	}
}
