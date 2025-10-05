package security

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

func TestTLSDigestStable(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tls", Namespace: "default", ResourceVersion: "1"},
		Data: map[string][]byte{
			"tls.crt": []byte("cert-data"),
			"tls.key": []byte("key-data"),
			"ca.crt":  []byte("ca-data"),
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(secret).Build()

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Security: &keyvalv1alpha1.SecuritySpec{
				TLS: &keyvalv1alpha1.TLSSpec{Enabled: true, SecretName: secret.Name},
			},
		},
	}

	hash1, ok1 := TLSDigest(context.Background(), k8sClient, cr)
	if !ok1 || hash1 == "" {
		t.Fatalf("expected digest, got %q", hash1)
	}

	var stored corev1.Secret
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "tls"}, &stored); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	reordered := stored.DeepCopy()
	reordered.Data = map[string][]byte{
		"tls.key": []byte("key-data"),
		"ca.crt":  []byte("ca-data"),
		"tls.crt": []byte("cert-data"),
	}
	if err := k8sClient.Patch(context.Background(), reordered, ctrlclient.MergeFrom(&stored)); err != nil {
		t.Fatalf("patch secret reorder: %v", err)
	}

	hash2, ok2 := TLSDigest(context.Background(), k8sClient, cr)
	if !ok2 || hash2 == "" {
		t.Fatalf("expected digest after reorder")
	}
	if hash1 != hash2 {
		t.Fatalf("digest should be stable across key order, %q vs %q", hash1, hash2)
	}

	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "tls"}, &stored); err != nil {
		t.Fatalf("get secret for update: %v", err)
	}
	updated := stored.DeepCopy()
	updated.Data["tls.crt"] = []byte("cert-data-new")
	if err := k8sClient.Patch(context.Background(), updated, ctrlclient.MergeFrom(&stored)); err != nil {
		t.Fatalf("patch secret data: %v", err)
	}

	hash3, ok3 := TLSDigest(context.Background(), k8sClient, cr)
	if !ok3 || hash3 == "" {
		t.Fatalf("expected digest after update")
	}
	if hash3 == hash1 {
		t.Fatalf("expected different digest after secret change")
	}
}
