package resources

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestMasterService_Golden(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	svc := MasterService(cr)
	if svc.Spec.Selector[core.LabelAppKey] != "demo-redis" || svc.Spec.Selector[core.RoleLabelKey] != string(keyvalv1alpha1.PodRoleMaster) {
		t.Fatalf("unexpected selector: %+v", svc.Spec.Selector)
	}
}

func TestMasterService_Overrides(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	cr.Spec.Service = &keyvalv1alpha1.ServiceSpec{
		Type:        corev1.ServiceTypeNodePort,
		Labels:      map[string]string{"tier": "cache", core.LabelAppKey: "override"},
		Annotations: map[string]string{"team": "platform"},
		Ports:       &keyvalv1alpha1.ServicePorts{Redis: 6380},
	}
	svc := MasterService(cr)
	if svc.Spec.Type != corev1.ServiceTypeNodePort {
		t.Fatalf("expected NodePort, got %s", svc.Spec.Type)
	}
	if svc.Spec.Ports[0].Port != 6380 {
		t.Fatalf("expected port override, got %d", svc.Spec.Ports[0].Port)
	}
	if svc.Labels[core.LabelAppKey] != "demo-redis" {
		t.Fatalf("operator label must be preserved, got %s", svc.Labels[core.LabelAppKey])
	}
	if svc.Labels["tier"] != "cache" {
		t.Fatalf("expected merged tier label, labels=%v", svc.Labels)
	}
	if svc.Annotations["team"] != "platform" {
		t.Fatalf("expected annotation merge, got %v", svc.Annotations)
	}
}

func TestReplicasService(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	svc := ReplicasService(cr)
	if svc.Name != "demo-replicas" || svc.Namespace != "default" {
		t.Fatalf("unexpected name/namespace: %s/%s", svc.Namespace, svc.Name)
	}
	if svc.Spec.Selector[core.LabelAppKey] != "demo-redis" || svc.Spec.Selector[core.RoleLabelKey] != string(keyvalv1alpha1.PodRoleReplica) {
		t.Fatalf("unexpected selector: %+v", svc.Spec.Selector)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 6379 {
		t.Fatalf("unexpected ports: %+v", svc.Spec.Ports)
	}
}

func TestReplicasService_Overrides(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	cr.Spec.ReplicasService = &keyvalv1alpha1.ServiceSpec{
		Type:        corev1.ServiceTypeLoadBalancer,
		Labels:      map[string]string{"role": "read"},
		Annotations: map[string]string{"service.beta.kubernetes.io/aws-load-balancer-type": "nlb"},
		Ports:       &keyvalv1alpha1.ServicePorts{Redis: 6381},
	}
	svc := ReplicasService(cr)
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Fatalf("expected LoadBalancer, got %s", svc.Spec.Type)
	}
	if svc.Spec.Ports[0].Port != 6381 {
		t.Fatalf("expected port override, got %d", svc.Spec.Ports[0].Port)
	}
	if svc.Labels["role"] != "read" {
		t.Fatalf("expected merged label, labels=%v", svc.Labels)
	}
	if svc.Annotations["service.beta.kubernetes.io/aws-load-balancer-type"] != "nlb" {
		t.Fatalf("expected annotation merge, annotations=%v", svc.Annotations)
	}
}

func TestSentinelService_Golden(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeSentinel)
	svc := SentinelService(cr)
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 26379 {
		t.Fatalf("unexpected ports: %+v", svc.Spec.Ports)
	}
	if svc.Spec.Selector[core.LabelAppKey] != "demo-sentinel" || len(svc.Spec.Selector) != 1 {
		t.Fatalf("unexpected selector: %+v", svc.Spec.Selector)
	}
}

func TestSentinelService_Overrides(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeSentinel)
	cr.Spec.SentinelService = &keyvalv1alpha1.ServiceSpec{
		Type:        corev1.ServiceTypeNodePort,
		Labels:      map[string]string{"component": "sentinel"},
		Annotations: map[string]string{"monitor": "true"},
		Ports:       &keyvalv1alpha1.ServicePorts{Sentinel: 36379},
	}
	svc := SentinelService(cr)
	if svc.Spec.Type != corev1.ServiceTypeNodePort {
		t.Fatalf("expected NodePort, got %s", svc.Spec.Type)
	}
	if svc.Spec.Ports[0].Port != 36379 {
		t.Fatalf("expected sentinel port override, got %d", svc.Spec.Ports[0].Port)
	}
	if svc.Labels["component"] != "sentinel" {
		t.Fatalf("expected merged label, labels=%v", svc.Labels)
	}
	if svc.Annotations["monitor"] != "true" {
		t.Fatalf("expected merged annotation, annotations=%v", svc.Annotations)
	}
}

func TestSentinelHeadlessService_Overrides(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeSentinel)
	f := false
	cr.Spec.SentinelService = &keyvalv1alpha1.ServiceSpec{
		PublishNotReadyAddresses: &f,
		Labels:                   map[string]string{"component": "sentinel"},
		Annotations:              map[string]string{"state": "ready"},
	}
	svc := SentinelHeadlessService(cr)
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("expected headless clusterIP, got %s", svc.Spec.ClusterIP)
	}
	if svc.Spec.PublishNotReadyAddresses {
		t.Fatalf("expected publishNotReady=false after override")
	}
	if svc.Labels["component"] != "sentinel" {
		t.Fatalf("expected merged label, labels=%v", svc.Labels)
	}
	if svc.Annotations["state"] != "ready" {
		t.Fatalf("expected merged annotation, annotations=%v", svc.Annotations)
	}
}

func TestHeadlessService_Defaults(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	svc := HeadlessService(cr)
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("expected headless clusterIP, got %s", svc.Spec.ClusterIP)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Fatalf("expected publishNotReadyAddresses=true by default")
	}
	if len(svc.Spec.Ports) != 2 {
		t.Fatalf("expected redis and metrics ports, got %v", svc.Spec.Ports)
	}
	if !hasServicePort(svc.Spec.Ports, "redis", 6379) {
		t.Fatalf("expected redis port 6379, ports=%v", svc.Spec.Ports)
	}
	if !hasServicePort(svc.Spec.Ports, "http-metrics", int(defaultMetricsPort)) {
		t.Fatalf("expected metrics port %d, ports=%v", defaultMetricsPort, svc.Spec.Ports)
	}
}

func TestHeadlessService_DisableMetrics(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	cr.Spec.Metrics = &keyvalv1alpha1.MetricsSpec{Enabled: ptr.To(false)}
	svc := HeadlessService(cr)
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Name != "redis" {
		t.Fatalf("expected only redis port when metrics disabled, ports=%v", svc.Spec.Ports)
	}
}

func TestHeadlessService_CustomMetricsPort(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	cr.Spec.Metrics = &keyvalv1alpha1.MetricsSpec{Port: 12345}
	svc := HeadlessService(cr)
	if !hasServicePort(svc.Spec.Ports, "http-metrics", 12345) {
		t.Fatalf("expected metrics port override, ports=%v", svc.Spec.Ports)
	}
}

func hasServicePort(ports []corev1.ServicePort, name string, port int) bool {
	for _, p := range ports {
		if p.Name == name && int(p.Port) == port {
			return true
		}
	}
	return false
}
