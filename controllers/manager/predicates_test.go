package manager

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestPodLifecyclePredicateReadyTransition(t *testing.T) {
	pred := PodLifecyclePredicate()
	if pred == nil {
		t.Fatalf("predicate is nil")
	}
	readyCond := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Labels: map[string]string{core.LabelClusterKey: "demo", core.LabelAppKey: core.AppLabelForName("demo")}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{}}}
	newPod := oldPod.DeepCopy()
	newPod.Status.Conditions = []corev1.PodCondition{readyCond}
	update := event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}
	if !pred.Update(update) {
		t.Fatalf("expected predicate to trigger on ready transition")
	}
}

func TestPodToClusterMapper(t *testing.T) {
	mapper := PodToClusterMapper()
	if mapper == nil {
		t.Fatalf("mapper is nil")
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Labels: map[string]string{core.LabelClusterKey: "demo", core.LabelAppKey: core.AppLabelForName("demo")}}}
	reqs := mapper(context.Background(), pod)
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	if reqs[0].NamespacedName.Name != "demo" || reqs[0].NamespacedName.Namespace != "default" {
		t.Fatalf("unexpected request: %+v", reqs[0])
	}
}

func TestTLSSecretMapper(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme core: %v", err)
	}
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme api: %v", err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "redis-tls", Namespace: "default"}}
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Security: &keyvalv1alpha1.SecuritySpec{
				TLS: &keyvalv1alpha1.TLSSpec{Enabled: true, SecretName: secret.Name},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster, secret).Build()
	mapper := TLSSecretMapper(client)
	reqs := mapper(context.Background(), secret)
	if len(reqs) != 1 {
		t.Fatalf("expected one request, got %d", len(reqs))
	}
	if reqs[0].NamespacedName != ctrlclient.ObjectKeyFromObject(cluster) {
		t.Fatalf("unexpected request: %+v", reqs[0])
	}
}

func TestGenerationChangedPredicate(t *testing.T) {
	pred := GenerationChangedPredicate()
	if pred == nil {
		t.Fatalf("generation predicate is nil")
	}
}
