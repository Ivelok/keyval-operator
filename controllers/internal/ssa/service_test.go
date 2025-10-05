package ssa

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestServiceTrim(t *testing.T) {
	source := &corev1.Service{}
	source.Name = "svc"
	source.Namespace = "ns"
	source.Labels = map[string]string{"managed": "true", "user": "keep"}
	source.Annotations = map[string]string{"service.beta.kubernetes.io/aws-load-balancer-type": "nlb"}
	source.Spec.Type = corev1.ServiceTypeClusterIP
	source.Spec.ClusterIP = "10.0.0.15"
	source.Spec.ClusterIPs = []string{"10.0.0.15"}
	source.Spec.Selector = map[string]string{"app": "redis", "role": "master"}
	source.Spec.SessionAffinity = corev1.ServiceAffinityClientIP
	source.Spec.Ports = []corev1.ServicePort{{Name: "redis", Port: 6379}}
	source.Status.LoadBalancer.Ingress = append(source.Status.LoadBalancer.Ingress, corev1.LoadBalancerIngress{IP: "1.1.1.1"})

	result := Service(source)

	if result.APIVersion != "v1" || result.Kind != "Service" {
		t.Fatalf("unexpected typemeta: %s %s", result.APIVersion, result.Kind)
	}
	if result.Spec.ClusterIP != "10.0.0.15" {
		t.Fatalf("clusterIP lost: %#v", result.Spec.ClusterIP)
	}
	if len(result.Spec.ClusterIPs) != 1 || result.Spec.ClusterIPs[0] != "10.0.0.15" {
		t.Fatalf("clusterIPs lost: %#v", result.Spec.ClusterIPs)
	}
	if len(result.Spec.Ports) != 1 || result.Spec.Ports[0].Port != 6379 {
		t.Fatalf("ports mismatch: %#v", result.Spec.Ports)
	}
	if len(result.Spec.Selector) != len(source.Spec.Selector) {
		t.Fatalf("selector mismatch: %#v", result.Spec.Selector)
	}
	result.Spec.Selector["new"] = "value"
	if _, ok := source.Spec.Selector["new"]; ok {
		t.Fatalf("selector map should be deep copied")
	}
	if len(result.Status.LoadBalancer.Ingress) != 0 {
		t.Fatalf("status should not be copied")
	}
}

func TestServiceHeadlessClusterIPNone(t *testing.T) {
	source := &corev1.Service{}
	source.Name = "headless"
	source.Namespace = "ns"
	source.Spec.Type = corev1.ServiceTypeClusterIP
	source.Spec.ClusterIP = corev1.ClusterIPNone

	result := Service(source)
	if result.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("expected clusterIP None, got %q", result.Spec.ClusterIP)
	}
}
