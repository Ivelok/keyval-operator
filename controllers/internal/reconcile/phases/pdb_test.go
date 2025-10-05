package phases

import (
	"context"
	"strings"
	"testing"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"k8s.io/client-go/tools/record"
)

func TestPDBRedisCreates(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 3,
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy()).
		Build()

	recorder := record.NewFakeRecorder(10)
	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   client,
			Recorder: recorder,
			Scheme:   scheme,
		},
		Disruption: reconcile.DisruptionState{
			AllowDisruptions: true,
		},
	}

	if err := PDB(context.Background(), state); err != nil {
		t.Fatalf("pdb phase failed: %v", err)
	}

	var pdb policyv1.PodDisruptionBudget
	if err := client.Get(context.Background(), ctrlclient.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-redis"}, &pdb); err != nil {
		t.Fatalf("get redis pdb: %v", err)
	}
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntValue() != 2 {
		t.Fatalf("expected redis pdb minAvailable=2, got %v", pdb.Spec.MinAvailable)
	}
	if state.Disruption.RedisMin != 2 {
		t.Fatalf("expected disruption state redis min 2, got %d", state.Disruption.RedisMin)
	}

	select {
	case evt := <-recorder.Events:
		if evt == "" || !strings.Contains(evt, "PDBModeSwitched") {
			t.Fatalf("expected PDBModeSwitched event, got %q", evt)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected redis PDB event")
	}
}

func TestPDBPreservesMinAnnotation(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 2,
			Health: &keyvalv1alpha1.HealthSpec{
				MinReplicasForSafety: 1,
			},
		},
	}

	existing := resources.RedisPDB(cluster, 2)
	existing.Spec.MinAvailable = intOrStringPtr(2)
	existing.Annotations = map[string]string{reconcile.PDBPreserveMinAnnotation: "true"}

	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy(), existing).
		Build()

	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   client,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Disruption: reconcile.DisruptionState{
			AllowDisruptions: true,
		},
	}

	if err := PDB(context.Background(), state); err != nil {
		t.Fatalf("pdb phase failed: %v", err)
	}

	var updated policyv1.PodDisruptionBudget
	if err := client.Get(context.Background(), ctrlclient.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-redis"}, &updated); err != nil {
		t.Fatalf("get redis pdb: %v", err)
	}
	if updated.Spec.MinAvailable == nil || updated.Spec.MinAvailable.IntValue() != 2 {
		t.Fatalf("expected preserved minAvailable=2, got %v", updated.Spec.MinAvailable)
	}
	if updated.Annotations[reconcile.PDBPreserveMinAnnotation] != "true" {
		t.Fatalf("expected preserve annotation to remain true, annotations=%v", updated.Annotations)
	}
}

func TestPDBSentinelCreates(t *testing.T) {
	scheme := newScheme(t)
	sentinelCount := int32(3)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 3,
			SentinelCount: &sentinelCount,
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy()).
		Build()

	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   client,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Disruption: reconcile.DisruptionState{
			AllowDisruptions: true,
			AllowSentinel:    false,
		},
	}

	if err := PDB(context.Background(), state); err != nil {
		t.Fatalf("pdb phase failed: %v", err)
	}

	var sentinel policyv1.PodDisruptionBudget
	if err := client.Get(context.Background(), ctrlclient.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-sentinel"}, &sentinel); err != nil {
		t.Fatalf("get sentinel pdb: %v", err)
	}
	if sentinel.Spec.MinAvailable == nil || sentinel.Spec.MinAvailable.IntValue() != 3 {
		t.Fatalf("expected sentinel minAvailable=3 when disruptions disallowed, got %v", sentinel.Spec.MinAvailable)
	}
	if state.Disruption.SentinelMin != 3 {
		t.Fatalf("expected disruption sentinel min 3, got %d", state.Disruption.SentinelMin)
	}
}

func intOrStringPtr(v int32) *intstr.IntOrString {
	value := intstr.FromInt(int(v))
	return &value
}
