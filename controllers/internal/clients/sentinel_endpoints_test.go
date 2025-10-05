package clients

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

func TestSentinelServiceEndpoints_Defaults(t *testing.T) {
	t.Parallel()
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, SentinelCount: &v},
	}
	got := sentinelServiceEndpoints(cr, "default")
	want := []string{
		"demo-sentinel.default.svc:26379",
		"demo-sentinel-0.demo-sentinel-headless.default.svc:26379",
		"demo-sentinel-1.demo-sentinel-headless.default.svc:26379",
		"demo-sentinel-2.demo-sentinel-headless.default.svc:26379",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d endpoints, got %d: %v", len(want), len(got), got)
	}
	for i, ep := range want {
		if got[i] != ep {
			t.Fatalf("endpoint %d mismatch: got %s, want %s", i, got[i], ep)
		}
	}
}

func TestSentinelServiceEndpoints_CustomNamespaceAndPort(t *testing.T) {
	t.Parallel()
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "demo-ns"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:            keyvalv1alpha1.ModeSentinel,
			SentinelCount:   &v,
			SentinelConfig:  map[string]string{"port": "36379"},
			SentinelService: &keyvalv1alpha1.ServiceSpec{Ports: &keyvalv1alpha1.ServicePorts{Sentinel: 36380}},
		},
	}
	got := sentinelServiceEndpoints(cr, "demo-ns")
	if len(got) == 0 {
		t.Fatalf("expected endpoints, got none")
	}
	if got[0] != "demo-sentinel.demo-ns.svc:36380" {
		t.Fatalf("expected custom service endpoint, got %s", got[0])
	}
	if got[1] != "demo-sentinel-0.demo-sentinel-headless.demo-ns.svc:36380" {
		t.Fatalf("expected headless endpoint with port override, got %s", got[1])
	}
}
