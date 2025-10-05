package resources

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func TestStatefulSetIncludesBootstrapInitContainer(t *testing.T) {
	t.Parallel()
	var replicas int32 = 3
	var sentinels int32 = 3
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: replicas,
			SentinelCount: &sentinels,
		},
	}

	ss := StatefulSet(cr, "hash", "", &security.Settings{})

	var initContainer *corev1.Container
	for i := range ss.Spec.Template.Spec.InitContainers {
		if ss.Spec.Template.Spec.InitContainers[i].Name == core.BootstrapInitContainerName {
			initContainer = &ss.Spec.Template.Spec.InitContainers[i]
			break
		}
	}

	if initContainer == nil {
		t.Fatalf("expected init container %s", core.BootstrapInitContainerName)
	}

	if len(initContainer.Command) != 2 || initContainer.Command[0] != "sh" || initContainer.Command[1] != BootstrapScriptPath {
		t.Fatalf("unexpected command for bootstrap init container: %#v", initContainer.Command)
	}

	if envVal(initContainer.Env, "CLUSTER_MODE") != string(keyvalv1alpha1.ModeSentinel) {
		t.Fatalf("expected CLUSTER_MODE env to be %s", keyvalv1alpha1.ModeSentinel)
	}
	if envVal(initContainer.Env, "SENTINEL_SVC") == "" {
		t.Fatalf("expected SENTINEL_SVC env to be populated")
	}

	if !hasMount(initContainer.VolumeMounts, core.RuntimeConfigVolumeName, "/runtime-conf") {
		t.Fatalf("expected init container to mount runtime-conf volume")
	}
	if !hasMount(initContainer.VolumeMounts, "config", "/conf") {
		t.Fatalf("expected init container to mount config volume")
	}

	if !hasVolume(ss.Spec.Template.Spec.Volumes, core.RuntimeConfigVolumeName) {
		t.Fatalf("expected runtime-conf volume on pod spec")
	}

	var redisContainer *corev1.Container
	for i := range ss.Spec.Template.Spec.Containers {
		if ss.Spec.Template.Spec.Containers[i].Name == core.RedisContainerName {
			redisContainer = &ss.Spec.Template.Spec.Containers[i]
			break
		}
	}
	if redisContainer == nil {
		t.Fatalf("expected redis container present")
	}
	if probe := redisContainer.LivenessProbe; probe == nil || probe.Exec == nil {
		t.Fatalf("expected redis container to have exec liveness probe")
	} else if cmd := probe.Exec.Command; len(cmd) != 2 || cmd[0] != "sh" || cmd[1] != LivenessScriptPath {
		t.Fatalf("unexpected liveness probe command: %#v", probe.Exec.Command)
	}
	if probe := redisContainer.ReadinessProbe; probe == nil || probe.Exec == nil {
		t.Fatalf("expected redis container to have exec readiness probe")
	} else if cmd := probe.Exec.Command; len(cmd) != 2 || cmd[0] != "sh" || cmd[1] != ReadinessScriptPath {
		t.Fatalf("unexpected readiness probe command: %#v", probe.Exec.Command)
	}
	if !hasMount(redisContainer.VolumeMounts, core.RuntimeConfigVolumeName, "/runtime-conf") {
		t.Fatalf("expected redis container to mount runtime-conf volume")
	}
	if val := envVal(redisContainer.Env, "REDIS_PORT"); val != "6379" {
		t.Fatalf("expected REDIS_PORT env to be 6379, got %q", val)
	}
	if lifecycle := redisContainer.Lifecycle; lifecycle == nil || lifecycle.PreStop == nil || lifecycle.PreStop.Exec == nil {
		t.Fatalf("expected redis container to have preStop lifecycle")
	} else if cmd := lifecycle.PreStop.Exec.Command; len(cmd) != 3 || cmd[0] != "sh" || cmd[1] != "-c" || cmd[2] != redisPreStopScript {
		t.Fatalf("unexpected preStop command: %#v", lifecycle.PreStop.Exec.Command)
	}
	if spec := ss.Spec.Template.Spec; spec.TerminationGracePeriodSeconds == nil || *spec.TerminationGracePeriodSeconds != 25 {
		t.Fatalf("expected terminationGracePeriodSeconds to be 25, got %v", spec.TerminationGracePeriodSeconds)
	}
}

func envVal(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func hasMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, m := range mounts {
		if m.Name == name && m.MountPath == path {
			return true
		}
	}
	return false
}

func hasVolume(vols []corev1.Volume, name string) bool {
	for _, v := range vols {
		if v.Name == name {
			return true
		}
	}
	return false
}

func TestStatefulSetTopologyDefaults(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.4",
			RedisReplicas: 3,
			SentinelCount: ptr.To(int32(3)),
		},
	}

	ss := StatefulSet(cr, "cfg", "", &security.Settings{})
	constraints := ss.Spec.Template.Spec.TopologySpreadConstraints
	if len(constraints) != 2 {
		t.Fatalf("expected 2 topology spread constraints, got %d", len(constraints))
	}
	if constraints[0].TopologyKey != "topology.kubernetes.io/zone" {
		t.Fatalf("expected zone constraint first, got %s", constraints[0].TopologyKey)
	}
	if constraints[1].TopologyKey != corev1.LabelHostname {
		t.Fatalf("expected hostname constraint second, got %s", constraints[1].TopologyKey)
	}
	for _, c := range constraints {
		if c.MaxSkew != 1 {
			t.Fatalf("expected maxSkew=1, got %d", c.MaxSkew)
		}
		if c.WhenUnsatisfiable != corev1.ScheduleAnyway {
			t.Fatalf("expected WhenUnsatisfiable=ScheduleAnyway, got %s", c.WhenUnsatisfiable)
		}
		if c.LabelSelector == nil || c.LabelSelector.MatchLabels[core.LabelClusterKey] != cr.Name {
			t.Fatalf("expected selector to include cluster label")
		}
		if c.LabelSelector.MatchLabels[core.LabelAppKey] != core.AppLabel(cr) {
			t.Fatalf("expected selector to include app label")
		}
	}

	aff := ss.Spec.Template.Spec.Affinity
	if aff == nil || aff.PodAntiAffinity == nil {
		t.Fatalf("expected pod anti-affinity to be configured")
	}
	pref := aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(pref) != 1 || pref[0].Weight != 100 {
		t.Fatalf("expected single preferred anti-affinity term with weight 100")
	}
	if pref[0].PodAffinityTerm.TopologyKey != corev1.LabelHostname {
		t.Fatalf("expected preferred topology key hostname, got %s", pref[0].PodAffinityTerm.TopologyKey)
	}
	if pref[0].PodAffinityTerm.LabelSelector == nil || pref[0].PodAffinityTerm.LabelSelector.MatchLabels[core.LabelAppKey] != core.AppLabel(cr) {
		t.Fatalf("expected preferred selector to match app label")
	}
}

func TestStatefulSetTopologyCanBeDisabled(t *testing.T) {
	t.Parallel()
	var replicas int32 = 3
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: replicas,
			SentinelCount: ptr.To(int32(3)),
			Topology: &keyvalv1alpha1.TopologySpec{
				Spread:       &keyvalv1alpha1.TopologySpreadSpec{Disabled: true},
				AntiAffinity: &keyvalv1alpha1.TopologyAntiAffinitySpec{Disabled: true},
			},
		},
	}

	ss := StatefulSet(cr, "hash", "", &security.Settings{})
	if len(ss.Spec.Template.Spec.TopologySpreadConstraints) != 0 {
		t.Fatalf("expected topology spread constraints to be disabled")
	}
	if ss.Spec.Template.Spec.Affinity != nil && ss.Spec.Template.Spec.Affinity.PodAntiAffinity != nil {
		t.Fatalf("expected pod anti-affinity to be disabled")
	}
}

func TestStatefulSetTopologyOverrides(t *testing.T) {
	t.Parallel()
	maxSkew := int32(2)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: ptr.To(int32(3)),
			Topology: &keyvalv1alpha1.TopologySpec{
				Spread: &keyvalv1alpha1.TopologySpreadSpec{
					TopologyKeys:      []string{"topology.kubernetes.io/zone"},
					MaxSkew:           &maxSkew,
					WhenUnsatisfiable: corev1.DoNotSchedule,
				},
				AntiAffinity: &keyvalv1alpha1.TopologyAntiAffinitySpec{Required: true},
			},
		},
	}

	ss := StatefulSet(cr, "hash", "", &security.Settings{})
	constraints := ss.Spec.Template.Spec.TopologySpreadConstraints
	if len(constraints) != 1 {
		t.Fatalf("expected exactly one constraint, got %d", len(constraints))
	}
	if constraints[0].MaxSkew != maxSkew {
		t.Fatalf("expected maxSkew override to be %d, got %d", maxSkew, constraints[0].MaxSkew)
	}
	if constraints[0].WhenUnsatisfiable != corev1.DoNotSchedule {
		t.Fatalf("expected whenUnsatisfiable=DoNotSchedule override, got %s", constraints[0].WhenUnsatisfiable)
	}
	if constraints[0].TopologyKey != "topology.kubernetes.io/zone" {
		t.Fatalf("unexpected topology key %s", constraints[0].TopologyKey)
	}

	aff := ss.Spec.Template.Spec.Affinity
	if aff == nil || aff.PodAntiAffinity == nil {
		t.Fatalf("expected pod anti-affinity to be configured")
	}
	if len(aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("expected required anti-affinity to be configured")
	}
	if aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey != corev1.LabelHostname {
		t.Fatalf("expected required topology key hostname")
	}
}
