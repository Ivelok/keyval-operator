package resources

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	if len(ss.Spec.Template.Spec.Containers) != 2 {
		t.Fatalf("expected redis + metrics containers, got %d", len(ss.Spec.Template.Spec.Containers))
	}

	var redisContainer *corev1.Container
	var metricsContainer *corev1.Container
	for i := range ss.Spec.Template.Spec.Containers {
		c := &ss.Spec.Template.Spec.Containers[i]
		switch c.Name {
		case core.RedisContainerName:
			redisContainer = c
		case core.MetricsContainerName:
			metricsContainer = c
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
	if metricsContainer == nil {
		t.Fatalf("expected metrics container present")
	}
	if metricsContainer.Image != defaultMetricsImage {
		t.Fatalf("expected default metrics image %q, got %q", defaultMetricsImage, metricsContainer.Image)
	}
	if len(metricsContainer.Ports) != 1 || metricsContainer.Ports[0].Name != "metrics" || metricsContainer.Ports[0].ContainerPort != defaultMetricsPort {
		t.Fatalf("unexpected metrics container ports: %+v", metricsContainer.Ports)
	}
	if metricsContainer.ReadinessProbe == nil || metricsContainer.ReadinessProbe.HTTPGet == nil {
		t.Fatalf("expected metrics readiness HTTP probe")
	}
	if metricsContainer.LivenessProbe == nil || metricsContainer.LivenessProbe.HTTPGet == nil {
		t.Fatalf("expected metrics liveness HTTP probe")
	}
	if addr := envVal(metricsContainer.Env, "REDIS_ADDR"); addr != "redis://127.0.0.1:6379" {
		t.Fatalf("expected REDIS_ADDR redis://127.0.0.1:6379, got %q", addr)
	}
	if envVal(metricsContainer.Env, "REDIS_PASSWORD") != "" {
		t.Fatalf("did not expect REDIS_PASSWORD when auth disabled")
	}
	if !hasArg(metricsContainer.Args, fmt.Sprintf("--web.listen-address=:%d", defaultMetricsPort)) {
		t.Fatalf("expected metrics args to include listen address, args=%v", metricsContainer.Args)
	}
	if !hasArg(metricsContainer.Args, "--redis.addr=redis://127.0.0.1:6379") {
		t.Fatalf("expected metrics args to include redis addr, args=%v", metricsContainer.Args)
	}
	if cpu := metricsContainer.Resources.Requests[corev1.ResourceCPU]; !cpu.Equal(resource.MustParse("20m")) {
		t.Fatalf("expected metrics cpu request 20m, got %s", cpu.String())
	}
	if mem := metricsContainer.Resources.Requests[corev1.ResourceMemory]; !mem.Equal(resource.MustParse("64Mi")) {
		t.Fatalf("expected metrics memory request 64Mi, got %s", mem.String())
	}
	if metricsContainer.SecurityContext == nil || metricsContainer.SecurityContext.ReadOnlyRootFilesystem == nil || !*metricsContainer.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("expected metrics container to enforce read-only root filesystem")
	}
}

func TestStatefulSet_DisableMetricsExporter(t *testing.T) {
	t.Parallel()
	var replicas int32 = 1
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:             keyvalv1alpha1.ModeStandalone,
			Image:            "valkey/valkey:7.2",
			RedisReplicas:    replicas,
			Metrics:          &keyvalv1alpha1.MetricsSpec{Enabled: ptr.To(false)},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "reg-cred"}},
		},
	}

	ss := StatefulSet(cr, "hash", "", &security.Settings{})
	if len(ss.Spec.Template.Spec.ImagePullSecrets) != 1 || ss.Spec.Template.Spec.ImagePullSecrets[0].Name != "reg-cred" {
		t.Fatalf("expected imagePullSecrets to propagate, got %v", ss.Spec.Template.Spec.ImagePullSecrets)
	}
	for _, c := range ss.Spec.Template.Spec.Containers {
		if c.Name == core.MetricsContainerName {
			t.Fatalf("expected metrics container disabled")
		}
	}
	if len(ss.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected single redis container when metrics disabled, got %d", len(ss.Spec.Template.Spec.Containers))
	}
}

func TestMetricsContainer_ConfiguresTLSAndAuth(t *testing.T) {
	t.Parallel()
	cr := crBase("secure", keyvalv1alpha1.ModeStandalone)
	cr.Spec.Metrics = &keyvalv1alpha1.MetricsSpec{}
	sec := &security.Settings{
		Auth: security.AuthSettings{
			Enabled:    true,
			Username:   "ops",
			SecretName: "redis-auth",
			SecretKey:  "password",
		},
		TLS: security.TLSSettings{
			Enabled:           true,
			SecretName:        "redis-tls",
			CACertKey:         "ca.crt",
			CertKey:           "tls.crt",
			KeyKey:            "tls.key",
			RequireClientAuth: true,
		},
	}

	ss := StatefulSet(cr, "hash", "tls", sec)
	var metricsContainer *corev1.Container
	for i := range ss.Spec.Template.Spec.Containers {
		c := &ss.Spec.Template.Spec.Containers[i]
		if c.Name == core.MetricsContainerName {
			metricsContainer = c
			break
		}
	}
	if metricsContainer == nil {
		t.Fatalf("expected metrics container present")
	}
	if addr := envVal(metricsContainer.Env, "REDIS_ADDR"); addr != "rediss://127.0.0.1:6379" {
		t.Fatalf("expected REDIS_ADDR rediss scheme, got %q", addr)
	}
	if !hasArg(metricsContainer.Args, "--redis.addr=rediss://127.0.0.1:6379") {
		t.Fatalf("expected metrics args to include rediss addr, args=%v", metricsContainer.Args)
	}
	passEnv := envVar(metricsContainer.Env, "REDIS_PASSWORD")
	if passEnv == nil || passEnv.ValueFrom == nil || passEnv.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("expected REDIS_PASSWORD sourced from secret")
	}
	if ref := passEnv.ValueFrom.SecretKeyRef; ref.Name != "redis-auth" || ref.Key != "password" {
		t.Fatalf("unexpected secret ref for REDIS_PASSWORD: %+v", ref)
	}
	if envVal(metricsContainer.Env, "REDIS_USER") != "ops" {
		t.Fatalf("expected REDIS_USER env")
	}
	if ca := envVal(metricsContainer.Env, "REDIS_EXPORTER_TLS_CA_CERT_FILE"); ca != TLSMountPath+"/ca.crt" {
		t.Fatalf("expected CA env path, got %q", ca)
	}
	if cert := envVal(metricsContainer.Env, "REDIS_EXPORTER_TLS_CLIENT_CERT_FILE"); cert != TLSMountPath+"/tls.crt" {
		t.Fatalf("expected client cert env, got %q", cert)
	}
	if key := envVal(metricsContainer.Env, "REDIS_EXPORTER_TLS_CLIENT_KEY_FILE"); key != TLSMountPath+"/tls.key" {
		t.Fatalf("expected client key env, got %q", key)
	}
	if !hasMount(metricsContainer.VolumeMounts, TLSVolumeName, TLSMountPath) {
		t.Fatalf("expected tls volume mount on metrics container")
	}
}

func TestMetricsContainer_CustomPort(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	cr.Spec.Metrics = &keyvalv1alpha1.MetricsSpec{Port: 10001}
	ss := StatefulSet(cr, "cfg", "", &security.Settings{})
	var metricsContainer *corev1.Container
	for i := range ss.Spec.Template.Spec.Containers {
		if ss.Spec.Template.Spec.Containers[i].Name == core.MetricsContainerName {
			metricsContainer = &ss.Spec.Template.Spec.Containers[i]
			break
		}
	}
	if metricsContainer == nil {
		t.Fatalf("expected metrics container present")
	}
	if len(metricsContainer.Ports) != 1 || metricsContainer.Ports[0].ContainerPort != 10001 {
		t.Fatalf("expected container port override, ports=%v", metricsContainer.Ports)
	}
	if !hasArg(metricsContainer.Args, "--web.listen-address=:10001") {
		t.Fatalf("expected listen address override, args=%v", metricsContainer.Args)
	}
}

func TestMetricsContainer_CustomImage(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	cr.Spec.Metrics = &keyvalv1alpha1.MetricsSpec{Image: "registry.local/redis_exporter:dev"}

	ss := StatefulSet(cr, "cfg", "", &security.Settings{})
	var metricsContainer *corev1.Container
	for i := range ss.Spec.Template.Spec.Containers {
		if ss.Spec.Template.Spec.Containers[i].Name == core.MetricsContainerName {
			metricsContainer = &ss.Spec.Template.Spec.Containers[i]
			break
		}
	}
	if metricsContainer == nil {
		t.Fatalf("expected metrics container present")
	}
	if metricsContainer.Image != "registry.local/redis_exporter:dev" {
		t.Fatalf("expected custom metrics image, got %q", metricsContainer.Image)
	}
}

func TestMetricsContainer_UsesRedisConfigAuth(t *testing.T) {
	t.Parallel()
	cr := crBase("legacy", keyvalv1alpha1.ModeStandalone)
	cr.Spec.RedisConfig = map[string]string{"requirepass": "legacy-pass"}
	ss := StatefulSet(cr, "cfg", "", &security.Settings{})
	var metricsContainer *corev1.Container
	for i := range ss.Spec.Template.Spec.Containers {
		if ss.Spec.Template.Spec.Containers[i].Name == core.MetricsContainerName {
			metricsContainer = &ss.Spec.Template.Spec.Containers[i]
			break
		}
	}
	if metricsContainer == nil {
		t.Fatalf("expected metrics container present")
	}
	passEnv := envVar(metricsContainer.Env, "REDIS_PASSWORD")
	if passEnv == nil || passEnv.Value != "legacy-pass" {
		t.Fatalf("expected REDIS_PASSWORD inline value from redisConfig, got %#v", passEnv)
	}
	if passEnv.ValueFrom != nil {
		t.Fatalf("expected inline password value, not secret ref")
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

func hasArg(args []string, expected string) bool {
	for _, a := range args {
		if a == expected {
			return true
		}
	}
	return false
}

func envVar(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
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
