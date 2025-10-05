package runtime

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPreserveClusterIP_DualStack(t *testing.T) {
	t.Parallel()
	policy := corev1.IPFamilyPolicyPreferDualStack
	existing := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			ClusterIP:      "10.0.0.10",
			ClusterIPs:     []string{"10.0.0.10", "fd00::10"},
			IPFamilies:     []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol},
			IPFamilyPolicy: &policy,
		},
	}
	c := fake.NewClientBuilder().WithObjects(existing).Build()
	desired := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"}}
	if err := PreserveClusterIP(context.Background(), c, desired); err != nil {
		t.Fatalf("PreserveClusterIP err: %v", err)
	}
	if desired.Spec.ClusterIP != "10.0.0.10" {
		t.Fatalf("expected ClusterIP preserved, got %s", desired.Spec.ClusterIP)
	}
	if len(desired.Spec.ClusterIPs) != 2 || desired.Spec.ClusterIPs[1] != "fd00::10" {
		t.Fatalf("expected ClusterIPs preserved, got %v", desired.Spec.ClusterIPs)
	}
	if len(desired.Spec.IPFamilies) != 2 || desired.Spec.IPFamilies[1] != corev1.IPv6Protocol {
		t.Fatalf("expected IPFamilies preserved, got %v", desired.Spec.IPFamilies)
	}
	if desired.Spec.IPFamilyPolicy == nil || *desired.Spec.IPFamilyPolicy != policy {
		t.Fatalf("expected IPFamilyPolicy preserved, got %v", desired.Spec.IPFamilyPolicy)
	}
}
