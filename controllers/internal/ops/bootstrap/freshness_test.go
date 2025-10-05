package bootstrap

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	clientspkg "github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func buildScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add api scheme: %v", err)
	}
	return scheme
}

func TestSelectCandidateHonorsForceMaster(t *testing.T) {
	t.Parallel()
	scheme := buildScheme(t)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, Image: "valkey", RedisReplicas: 3},
	}
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-0",
				Namespace: "ns",
				Labels:    core.LabelsFor(cr),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "demo-1",
				Namespace:   "ns",
				Labels:      core.LabelsFor(cr),
				Annotations: map[string]string{core.AnnotationForceMaster: "true"},
			},
		},
	}
	pcs := []*corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "data-demo-0", Namespace: "ns"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "data-demo-1",
				Namespace:   "ns",
				Annotations: map[string]string{core.AnnotationPVCReplicationOffset: "42", core.AnnotationPVCOffsetTimestamp: time.Now().Format(time.RFC3339Nano)},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cr, pcs[0], pcs[1]).Build()
	cand := SelectCandidate(context.Background(), client, cr, pods)
	if cand.Name != "demo-1" {
		t.Fatalf("expected force-master demo-1, got %s", cand.Name)
	}
	if cand.Source != candidateSourceForced {
		t.Fatalf("expected source force-master, got %s", cand.Source)
	}
}

func TestSelectCandidateUsesFreshestOffset(t *testing.T) {
	t.Parallel()
	scheme := buildScheme(t)
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, Image: "valkey", RedisReplicas: 3}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns", Labels: core.LabelsFor(cr)}, Status: corev1.PodStatus{}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "ns", Labels: core.LabelsFor(cr)}, Status: corev1.PodStatus{}},
	}
	now := time.Now()
	pcs := []*corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-demo-0",
				Namespace: "ns",
				Annotations: map[string]string{
					core.AnnotationPVCReplicationOffset: "128",
					core.AnnotationPVCOffsetTimestamp:   now.Add(-time.Minute).Format(time.RFC3339Nano),
					core.AnnotationPVCReplicationRole:   "replica",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-demo-1",
				Namespace: "ns",
				Annotations: map[string]string{
					core.AnnotationPVCReplicationOffset: "256",
					core.AnnotationPVCOffsetTimestamp:   now.Format(time.RFC3339Nano),
					core.AnnotationPVCReplicationRole:   "master",
				},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cr, pcs[0], pcs[1]).Build()
	cand := SelectCandidate(context.Background(), client, cr, pods)
	if cand.Name != "demo-1" {
		t.Fatalf("expected demo-1 as freshest, got %s", cand.Name)
	}
	if cand.Source != candidateSourceFreshest {
		t.Fatalf("expected source freshness, got %s", cand.Source)
	}
}

func TestSelectCandidateDefaultsToSeed(t *testing.T) {
	t.Parallel()
	scheme := buildScheme(t)
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey", RedisReplicas: 1}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns", Labels: core.LabelsFor(cr)}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "ns", Labels: core.LabelsFor(cr)}},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cr).Build()
	cand := SelectCandidate(context.Background(), client, cr, pods)
	if cand.Name != "demo-0" {
		t.Fatalf("expected seed demo-0, got %s", cand.Name)
	}
	if cand.Source != candidateSourceSeed {
		t.Fatalf("expected seed source, got %s", cand.Source)
	}
}

func TestUpdatePVCFreshnessPersistsOffsets(t *testing.T) {
	t.Parallel()
	scheme := buildScheme(t)
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, Image: "valkey", RedisReplicas: 3}}
	info := map[string]clientspkg.ReplicationInfo{
		"demo-0": {Role: "master", MasterReplOffset: 512},
	}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "ns", Labels: core.LabelsFor(cr)}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-demo-0", Namespace: "ns"}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cr, pvc).Build()
	UpdatePVCFreshness(context.Background(), client, cr, []corev1.Pod{pod}, info)
	var updated corev1.PersistentVolumeClaim
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "data-demo-0"}, &updated); err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if updated.Annotations[core.AnnotationPVCReplicationOffset] != "512" {
		t.Fatalf("expected offset 512, got %s", updated.Annotations[core.AnnotationPVCReplicationOffset])
	}
	if updated.Annotations[core.AnnotationPVCReplicationRole] != "master" {
		t.Fatalf("expected role master, got %s", updated.Annotations[core.AnnotationPVCReplicationRole])
	}
}
