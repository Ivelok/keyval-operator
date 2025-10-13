package phases

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
)

func TestPodMetadataRemovesStaleLabel(t *testing.T) {
	scheme := newScheme(t)
	cluster := baseCluster()
	cr := cluster.DeepCopy()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-redis-0",
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				core.LabelAppKey:                        core.AppLabel(cluster),
				core.LabelClusterKey:                    cluster.Name,
				"prometheus.deckhouse.io/custom-target": "redis",
			},
			ResourceVersion: "5",
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cr.DeepCopy(), pod.DeepCopy()).
		Build()

	rc := &recordingClient{Client: k8sClient}

	state := &reconcile.State{
		Cluster: cr,
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   rc,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		RedisPods:   copyPods([]*corev1.Pod{pod}),
		RedisTemplateMetadata: reconcile.PodTemplateMetadata{
			PreviousLabels: map[string]string{
				core.LabelAppKey:                        core.AppLabel(cluster),
				core.LabelClusterKey:                    cluster.Name,
				"prometheus.deckhouse.io/custom-target": "redis",
			},
			DesiredLabels: map[string]string{
				core.LabelAppKey:     core.AppLabel(cluster),
				core.LabelClusterKey: cluster.Name,
			},
		},
	}

	if err := PodMetadata(context.Background(), state); err != nil {
		t.Fatalf("pod metadata phase failed: %v", err)
	}

	if len(rc.patches) != 1 {
		t.Fatalf("expected 1 patch, got %d", len(rc.patches))
	}

	var updated corev1.Pod
	if err := rc.Get(context.Background(), ctrlclient.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}, &updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if _, ok := updated.Labels["prometheus.deckhouse.io/custom-target"]; ok {
		t.Fatalf("expected custom target label removed, got %v", updated.Labels)
	}
	if updated.Labels[core.LabelAppKey] != core.AppLabel(cluster) {
		t.Fatalf("app label drifted: %v", updated.Labels)
	}
	if updated.Labels[core.LabelClusterKey] != cluster.Name {
		t.Fatalf("cluster label drifted: %v", updated.Labels)
	}
}

func TestPodMetadataAddsNewLabel(t *testing.T) {
	scheme := newScheme(t)
	cluster := baseCluster()
	cr := cluster.DeepCopy()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-redis-1",
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				core.LabelAppKey:     core.AppLabel(cluster),
				core.LabelClusterKey: cluster.Name,
			},
			ResourceVersion: "7",
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cr.DeepCopy(), pod.DeepCopy()).
		Build()

	rc := &recordingClient{Client: k8sClient}

	state := &reconcile.State{
		Cluster: cr,
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   rc,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		RedisPods:   copyPods([]*corev1.Pod{pod}),
		RedisTemplateMetadata: reconcile.PodTemplateMetadata{
			PreviousLabels: map[string]string{
				core.LabelAppKey:     core.AppLabel(cluster),
				core.LabelClusterKey: cluster.Name,
			},
			DesiredLabels: map[string]string{
				core.LabelAppKey:     core.AppLabel(cluster),
				core.LabelClusterKey: cluster.Name,
				"tier":               "cache",
			},
		},
	}

	if err := PodMetadata(context.Background(), state); err != nil {
		t.Fatalf("pod metadata phase failed: %v", err)
	}

	if len(rc.patches) != 1 {
		t.Fatalf("expected 1 patch, got %d", len(rc.patches))
	}

	var updated corev1.Pod
	if err := rc.Get(context.Background(), ctrlclient.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}, &updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if updated.Labels["tier"] != "cache" {
		t.Fatalf("expected tier label applied, got %v", updated.Labels)
	}
}
