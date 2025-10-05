package controllers

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func TestDesiredSentinelStatefulSet_Basic(t *testing.T) {
	t.Parallel()
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:           keyvalv1alpha1.ModeSentinel,
			Image:          "valkey/valkey:7.2",
			RedisReplicas:  3,
			SentinelCount:  &v,
			SentinelConfig: map[string]string{},
			Storage:        &keyvalv1alpha1.StorageSpec{Type: "Ephemeral"},
		},
	}
	ss := resources.SentinelStatefulSet(cr, "", "", &security.Settings{})
	if ss.Name != "demo-sentinel" || ss.Namespace != "default" {
		t.Fatalf("unexpected name/namespace: %s/%s", ss.Namespace, ss.Name)
	}
	if ss.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
		t.Fatalf("expected OnDelete strategy")
	}
	if ss.Spec.ServiceName != core.SentinelHeadlessName(cr) {
		t.Fatalf("unexpected serviceName: %s", ss.Spec.ServiceName)
	}
	if ss.Spec.Replicas == nil || *ss.Spec.Replicas != 3 {
		t.Fatalf("unexpected replicas: %v", derefInt32(ss.Spec.Replicas))
	}
	// Labels
	if ss.Labels[core.LabelAppKey] != core.SentinelAppLabel(cr) || ss.Labels[core.LabelClusterKey] != cr.Name {
		t.Fatalf("unexpected ss labels: %+v", ss.Labels)
	}
	podLabels := ss.Spec.Template.Labels
	if podLabels[core.LabelAppKey] != core.SentinelAppLabel(cr) || podLabels[core.LabelClusterKey] != cr.Name || podLabels[core.RoleLabelKey] != string(keyvalv1alpha1.PodRoleSentinel) {
		t.Fatalf("unexpected pod labels: %+v", podLabels)
	}
	// Container/ports
	if len(ss.Spec.Template.Spec.Containers) != 1 || ss.Spec.Template.Spec.Containers[0].Name != core.SentinelContainerName {
		t.Fatalf("expected single sentinel container: %+v", ss.Spec.Template.Spec.Containers)
	}
	gotPorts := ss.Spec.Template.Spec.Containers[0].Ports
	if len(gotPorts) != 1 || gotPorts[0].ContainerPort != 26379 {
		t.Fatalf("unexpected sentinel port list: %+v", gotPorts)
	}
	if len(ss.Spec.Template.Spec.Containers[0].Command) == 0 || ss.Spec.Template.Spec.Containers[0].Command[0] != "valkey-sentinel" {
		t.Fatalf("expected valkey sentinel command, got %v", ss.Spec.Template.Spec.Containers[0].Command)
	}
	if envValue(ss.Spec.Template.Spec.Containers[0].Env, "REDIS_PORT") != "26379" {
		t.Fatalf("expected sentinel container to expose REDIS_PORT env")
	}
	if lifecycle := ss.Spec.Template.Spec.Containers[0].Lifecycle; lifecycle == nil || lifecycle.PreStop == nil || lifecycle.PreStop.Exec == nil {
		t.Fatalf("expected sentinel container to have preStop lifecycle")
	} else if cmd := lifecycle.PreStop.Exec.Command; len(cmd) != 3 || cmd[0] != "sh" || cmd[1] != "-c" || !strings.Contains(cmd[2], "redis-cli") {
		t.Fatalf("unexpected sentinel preStop command: %#v", cmd)
	}
	if spec := ss.Spec.Template.Spec; spec.TerminationGracePeriodSeconds == nil || *spec.TerminationGracePeriodSeconds != 25 {
		t.Fatalf("expected terminationGracePeriodSeconds=25, got %v", spec.TerminationGracePeriodSeconds)
	}
	// Volumes: config-src (ConfigMap), config (EmptyDir), data (EmptyDir)
	vols := ss.Spec.Template.Spec.Volumes
	var hasConfigSrc, hasConfig, hasData bool
	for _, v := range vols {
		if v.Name == "config-src" && v.ConfigMap != nil {
			hasConfigSrc = true
		}
		if v.Name == "config" && v.EmptyDir != nil {
			hasConfig = true
		}
		if v.Name == core.DataVolumeName && v.EmptyDir != nil {
			hasData = true
		}
	}
	if !hasConfigSrc || !hasConfig || !hasData {
		t.Fatalf("expected volumes config-src(cm), config(ed), data(ed): %+v", vols)
	}
	// Init container exists
	if len(ss.Spec.Template.Spec.InitContainers) == 0 {
		t.Fatalf("expected init container to copy config")
	}
}

func TestDesiredSentinelStatefulSet_TopologyDefaults(t *testing.T) {
	t.Parallel()
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.4",
			RedisReplicas: 3,
			SentinelCount: &v,
		},
	}
	ss := resources.SentinelStatefulSet(cr, "cfg", "", &security.Settings{})
	constraints := ss.Spec.Template.Spec.TopologySpreadConstraints
	if len(constraints) != 2 {
		t.Fatalf("expected 2 topology spread constraints, got %d", len(constraints))
	}
	if constraints[0].TopologyKey != "topology.kubernetes.io/zone" || constraints[1].TopologyKey != corev1.LabelHostname {
		t.Fatalf("unexpected topology keys: %+v", constraints)
	}
	for _, c := range constraints {
		if c.MaxSkew != 1 {
			t.Fatalf("expected maxSkew=1, got %d", c.MaxSkew)
		}
		if c.WhenUnsatisfiable != corev1.ScheduleAnyway {
			t.Fatalf("expected ScheduleAnyway action, got %s", c.WhenUnsatisfiable)
		}
		if c.LabelSelector == nil || c.LabelSelector.MatchLabels[core.LabelAppKey] != core.SentinelAppLabel(cr) {
			t.Fatalf("expected selector to match sentinel app label")
		}
	}
	aff := ss.Spec.Template.Spec.Affinity
	if aff == nil || aff.PodAntiAffinity == nil {
		t.Fatalf("expected pod anti-affinity configured")
	}
	if len(aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("expected single preferred anti-affinity term")
	}
}

func TestDesiredSentinelStatefulSet_TLSHashAnnotation(t *testing.T) {
	t.Parallel()
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: &v,
			Storage:       &keyvalv1alpha1.StorageSpec{Type: "Ephemeral"},
		},
	}
	sec := &security.Settings{TLS: security.TLSSettings{Enabled: true}}
	ss := resources.SentinelStatefulSet(cr, "cfg", "tls123", sec)
	if ss.Spec.Template.Annotations[resources.ConfigHashAnnotationKey] != "cfg" {
		t.Fatalf("expected config hash annotation")
	}
	if ss.Spec.Template.Annotations[resources.TLSSecretHashAnnotationKey] != "tls123" {
		t.Fatalf("expected tls hash annotation")
	}
}

func TestDesiredSentinelStatefulSet_PreservesUserAntiAffinity(t *testing.T) {
	t.Parallel()
	v := int32(3)
	customSel := &metav1.LabelSelector{MatchLabels: map[string]string{"custom": "true"}}
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.4",
			RedisReplicas: 3,
			SentinelCount: &v,
			SentinelPod: &keyvalv1alpha1.SentinelPodSpec{
				Affinity: &corev1.Affinity{
					PodAntiAffinity: &corev1.PodAntiAffinity{
						PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
							{
								Weight: 50,
								PodAffinityTerm: corev1.PodAffinityTerm{
									TopologyKey:   "topology.kubernetes.io/zone",
									LabelSelector: customSel,
								},
							},
						},
					},
				},
			},
		},
	}

	ss := resources.SentinelStatefulSet(cr, "cfg", "", &security.Settings{})
	paa := ss.Spec.Template.Spec.Affinity.PodAntiAffinity
	if paa == nil {
		t.Fatalf("expected pod anti-affinity present")
	}
	if !hasPreferredAntiAffinity(paa, corev1.LabelHostname, map[string]string{core.LabelClusterKey: cr.Name, core.LabelAppKey: core.SentinelAppLabel(cr)}) {
		t.Fatalf("expected default preferred anti-affinity with hostname selector")
	}
	if !hasPreferredAntiAffinity(paa, "topology.kubernetes.io/zone", customSel.MatchLabels) {
		t.Fatalf("expected user preferred anti-affinity to be preserved")
	}
}

func TestDesiredSentinelStatefulSet_AntiAffinityDisablePreservesUser(t *testing.T) {
	t.Parallel()
	v := int32(3)
	customSel := &metav1.LabelSelector{MatchLabels: map[string]string{"custom": "true"}}
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.4",
			RedisReplicas: 3,
			SentinelCount: &v,
			SentinelPod: &keyvalv1alpha1.SentinelPodSpec{
				Affinity: &corev1.Affinity{
					PodAntiAffinity: &corev1.PodAntiAffinity{
						PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
							{
								Weight: 25,
								PodAffinityTerm: corev1.PodAffinityTerm{
									TopologyKey:   "failure-domain.example/zone",
									LabelSelector: customSel,
								},
							},
						},
					},
				},
			},
			Topology: &keyvalv1alpha1.TopologySpec{
				AntiAffinity: &keyvalv1alpha1.TopologyAntiAffinitySpec{Disabled: true},
			},
		},
	}

	ss := resources.SentinelStatefulSet(cr, "cfg", "", &security.Settings{})
	paa := ss.Spec.Template.Spec.Affinity.PodAntiAffinity
	if paa == nil {
		t.Fatalf("expected user anti-affinity to remain")
	}
	if hasPreferredAntiAffinity(paa, corev1.LabelHostname, map[string]string{core.LabelClusterKey: cr.Name, core.LabelAppKey: core.SentinelAppLabel(cr)}) {
		t.Fatalf("expected default anti-affinity to be removed when disabled")
	}
	if !hasPreferredAntiAffinity(paa, "failure-domain.example/zone", customSel.MatchLabels) {
		t.Fatalf("expected user anti-affinity term to be preserved")
	}
}

func derefInt32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

func TestDesiredSentinelStatefulSet_CommandHonorsImageOverrides(t *testing.T) {
	t.Parallel()
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Engine:        keyvalv1alpha1.EngineValkey,
			Image:         "valkey/valkey:7.2",
			SentinelImage: func() *string { s := "redis:7.2"; return &s }(),
			RedisReplicas: 3,
			SentinelCount: &v,
		},
	}
	ss := resources.SentinelStatefulSet(cr, "", "", &security.Settings{})
	cmd := ss.Spec.Template.Spec.Containers[0].Command
	if len(cmd) == 0 || cmd[0] != "redis-sentinel" {
		t.Fatalf("expected redis-sentinel command when sentinel image is redis, got %v", cmd)
	}
}

func TestDesiredSentinelStatefulSet_UsesDedicatedDefaultsWhenUnset(t *testing.T) {
	t.Parallel()
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: &v,
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
		},
	}
	ss := resources.SentinelStatefulSet(cr, "", "", &security.Settings{})
	want := resources.DesiredSentinelResources(cr)
	got := sentinelContainerResources(t, ss)
	expectedCPU := resource.MustParse("50m")
	cpu := want.Requests[corev1.ResourceCPU]
	if cpu.Cmp(expectedCPU) != 0 {
		t.Fatalf("expected default cpu request of 50m, got %v", cpu)
	}
	requireResourceRequirementsEqual(t, got, want)
}

func TestDesiredSentinelStatefulSet_HonorsExplicitResources(t *testing.T) {
	t.Parallel()
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: &v,
			SentinelResources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("150m"),
					corev1.ResourceMemory: resource.MustParse("256Mi"),
				},
			},
		},
	}
	ss := resources.SentinelStatefulSet(cr, "", "", &security.Settings{})
	want := resources.DesiredSentinelResources(cr)
	requireResourceRequirementsEqual(t, sentinelContainerResources(t, ss), want)
}

func sentinelContainerResources(t *testing.T, ss *appsv1.StatefulSet) corev1.ResourceRequirements {
	t.Helper()
	if len(ss.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("sentinel container missing")
	}
	return ss.Spec.Template.Spec.Containers[0].Resources
}

func hasPreferredAntiAffinity(paa *corev1.PodAntiAffinity, topologyKey string, matchLabels map[string]string) bool {
	if paa == nil {
		return false
	}
	for _, term := range paa.PreferredDuringSchedulingIgnoredDuringExecution {
		if term.PodAffinityTerm.TopologyKey != topologyKey {
			continue
		}
		if labelSelectorEqual(term.PodAffinityTerm.LabelSelector, matchLabels) {
			return true
		}
	}
	return false
}

func labelSelectorEqual(sel *metav1.LabelSelector, matchLabels map[string]string) bool {
	if sel == nil {
		return false
	}
	if len(sel.MatchExpressions) > 0 {
		return false
	}
	if len(sel.MatchLabels) != len(matchLabels) {
		return false
	}
	for k, v := range sel.MatchLabels {
		if matchLabels[k] != v {
			return false
		}
	}
	for k := range matchLabels {
		if _, ok := sel.MatchLabels[k]; !ok {
			return false
		}
	}
	return true
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func requireResourceRequirementsEqual(t *testing.T, got, want corev1.ResourceRequirements) {
	t.Helper()
	if !resourceRequirementsEqual(got, want) {
		t.Fatalf("unexpected resources: got %+v want %+v", got, want)
	}
}

func resourceRequirementsEqual(a, b corev1.ResourceRequirements) bool {
	return resourceListEqual(a.Limits, b.Limits) && resourceListEqual(a.Requests, b.Requests)
}

func resourceListEqual(x, y corev1.ResourceList) bool {
	if len(x) == 0 && len(y) == 0 {
		return true
	}
	for k, qx := range x {
		qy, ok := y[k]
		if !ok {
			if qx.IsZero() {
				continue
			}
			return false
		}
		if qx.Cmp(qy) != 0 {
			return false
		}
	}
	for k, qy := range y {
		if _, ok := x[k]; !ok {
			if qy.IsZero() {
				continue
			}
			return false
		}
	}
	return true
}
