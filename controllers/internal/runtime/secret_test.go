package runtime

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

func TestTLSSecretToClusterRequests(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Security: &keyvalv1alpha1.SecuritySpec{TLS: &keyvalv1alpha1.TLSSpec{Enabled: true, SecretName: "tls"}},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls", Namespace: "default"}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()

	reqs := TLSSecretToClusterRequests(context.Background(), secret, client)
	if len(reqs) != 1 {
		t.Fatalf("expected single reconcile request, got %d", len(reqs))
	}
	expected := reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}
	if reqs[0] != expected {
		t.Fatalf("unexpected request: %#v", reqs[0])
	}
}
