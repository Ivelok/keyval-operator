package clients

import (
	"fmt"
	"net"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	corev1 "k8s.io/api/core/v1"
)

func sentinelServiceEnabled(cr *keyvalv1alpha1.KeyValCluster) bool {
	if cr.Spec.SentinelService == nil || cr.Spec.SentinelService.Create == nil {
		return true
	}
	return *cr.Spec.SentinelService.Create
}

func sentinelPorts(cr *keyvalv1alpha1.KeyValCluster) (int, string) {
	port, portStr := core.SentinelPort(cr)
	return port, portStr
}

func sentinelServiceEndpoints(cr *keyvalv1alpha1.KeyValCluster, namespace string) []string {
	port, portStr := sentinelPorts(cr)
	if port <= 0 {
		return nil
	}
	var endpoints []string
	added := map[string]struct{}{}
	add := func(host string) {
		addr := fmt.Sprintf("%s:%s", host, portStr)
		if _, ok := added[addr]; ok {
			return
		}
		added[addr] = struct{}{}
		endpoints = append(endpoints, addr)
	}
	if sentinelServiceEnabled(cr) {
		add(fmt.Sprintf("%s.%s.svc", core.SentinelServiceName(cr), namespace))
	}
	count := 0
	if cr.Spec.SentinelCount != nil && *cr.Spec.SentinelCount > 0 {
		count = int(*cr.Spec.SentinelCount)
	}
	if count == 0 {
		count = 3
	}
	headless := fmt.Sprintf("%s.%s.svc", core.SentinelHeadlessName(cr), namespace)
	for i := 0; i < count; i++ {
		add(fmt.Sprintf("%s-sentinel-%d.%s", cr.Name, i, headless))
	}
	return endpoints
}

func sentinelPodAddress(cr *keyvalv1alpha1.KeyValCluster, pod corev1.Pod) string {
	if !tlsEnabled(cr) {
		if ip := net.ParseIP(pod.Status.PodIP); ip != nil {
			return fmt.Sprintf("%s:%s", pod.Status.PodIP, sentinelPortString(cr))
		}
	}
	return fmt.Sprintf("%s.%s.%s.svc:%s", pod.Name, core.SentinelHeadlessName(cr), pod.Namespace, sentinelPortString(cr))
}

func sentinelPortString(cr *keyvalv1alpha1.KeyValCluster) string {
	_, s := sentinelPorts(cr)
	return s
}

func tlsEnabled(cr *keyvalv1alpha1.KeyValCluster) bool {
	if cr == nil || cr.Spec.Security == nil || cr.Spec.Security.TLS == nil {
		return false
	}
	return cr.Spec.Security.TLS.Enabled
}
