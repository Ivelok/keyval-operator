package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
)

func TestGetPorts_DefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, Image: "valkey/valkey:7.2", RedisReplicas: 3, SentinelCount: func() *int32 { v := int32(3); return &v }()}}
	// defaults
	if rp, _ := core.RedisPort(cr); rp != 6379 {
		t.Fatalf("redis default port: %d", rp)
	}
	if sp, _ := core.SentinelPort(cr); sp != 26379 {
		t.Fatalf("sentinel default port: %d", sp)
	}
	// overrides
	cr.Spec.RedisConfig = map[string]string{"port": "6380"}
	if rp, _ := core.RedisPort(cr); rp != 6380 {
		t.Fatalf("redis override port: %d", rp)
	}
	cr.Spec.SentinelService = &keyvalv1alpha1.ServiceSpec{Ports: &keyvalv1alpha1.ServicePorts{Sentinel: 36379}}
	if sp, _ := core.SentinelPort(cr); sp != 36379 {
		t.Fatalf("sentinel override port: %d", sp)
	}
}

func TestServices_UseOverriddenPorts(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, Image: "valkey/valkey:7.2", RedisReplicas: 3, SentinelCount: func() *int32 { v := int32(3); return &v }()}}
	cr.Spec.RedisConfig = map[string]string{"port": "6381"}
	cr.Spec.SentinelService = &keyvalv1alpha1.ServiceSpec{Ports: &keyvalv1alpha1.ServicePorts{Sentinel: 36380}}
	ms := resources.MasterService(cr)
	if ms.Spec.Ports[0].Port != 6381 {
		t.Fatalf("master service port = %d", ms.Spec.Ports[0].Port)
	}
	rs := resources.ReplicasService(cr)
	if rs.Spec.Ports[0].Port != 6381 {
		t.Fatalf("replicas service port = %d", rs.Spec.Ports[0].Port)
	}
	hs := resources.HeadlessService(cr)
	if hs.Spec.Ports[0].Port != 6381 || hs.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("headless service port/clusterIP = %d/%s", hs.Spec.Ports[0].Port, hs.Spec.ClusterIP)
	}
	ss := resources.SentinelService(cr)
	if ss.Spec.Ports[0].Port != 36380 {
		t.Fatalf("sentinel service port = %d", ss.Spec.Ports[0].Port)
	}
	sh := resources.SentinelHeadlessService(cr)
	if sh.Spec.Ports[0].Port != 36380 || sh.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("sentinel headless port/clusterIP = %d/%s", sh.Spec.Ports[0].Port, sh.Spec.ClusterIP)
	}
}

func TestPodFQDN(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}}
	f := core.PodFQDN(cr, 0)
	if f != "demo-0.demo-headless.ns.svc" {
		t.Fatalf("core.PodFQDN: %s", f)
	}
}
