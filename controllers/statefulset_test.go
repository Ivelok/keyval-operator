package controllers

import (
	"os"
	"sort"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func newCR(name string, mode keyvalv1alpha1.Mode) *keyvalv1alpha1.KeyValCluster {
	var replicas int32 = 1
	if mode == keyvalv1alpha1.ModeSentinel {
		replicas = 3
	}
	return &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          mode,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: replicas,
			Storage:       &keyvalv1alpha1.StorageSpec{Type: "Ephemeral"},
			SentinelCount: func() *int32 {
				if mode == keyvalv1alpha1.ModeSentinel {
					v := int32(3)
					return &v
				}
				return nil
			}(),
		},
	}
}

func TestDesiredHeadlessService_Golden(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeStandalone)
	svc := resources.HeadlessService(cr)
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("expected headless service, got ClusterIP=%q", svc.Spec.ClusterIP)
	}
	y := mustYAML(t, toServiceSnapshot(svc))
	want := loadGolden(t, "testdata/standalone_headless_service.yaml")
	if string(y) != string(want) {
		t.Fatalf("golden mismatch:\nGot:\n%s\nWant:\n%s", string(y), string(want))
	}
}

func TestDesiredHeadlessService_PublishNotReady_DefaultTrue(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeStandalone)
	svc := resources.HeadlessService(cr)
	if !svc.Spec.PublishNotReadyAddresses {
		t.Fatalf("expected publishNotReadyAddresses=true by default")
	}
}

func TestDesiredHeadlessService_OverridesMetadata(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeStandalone)
	f := false
	cr.Spec.Service = &keyvalv1alpha1.ServiceSpec{
		PublishNotReadyAddresses: &f,
		Labels:                   map[string]string{"tier": "cache"},
		Annotations:              map[string]string{"team": "platform"},
		Ports:                    &keyvalv1alpha1.ServicePorts{Redis: 6385},
	}
	svc := resources.HeadlessService(cr)
	if svc.Spec.PublishNotReadyAddresses {
		t.Fatalf("expected publishNotReady=false override")
	}
	if svc.Spec.Ports[0].Port != 6385 {
		t.Fatalf("expected port override, got %d", svc.Spec.Ports[0].Port)
	}
	if svc.Labels["tier"] != "cache" {
		t.Fatalf("expected merged label, got %v", svc.Labels)
	}
	if svc.Annotations["team"] != "platform" {
		t.Fatalf("expected merged annotation, got %v", svc.Annotations)
	}
}

func TestDesiredStatefulSet_Golden_Standalone(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeStandalone)
	ss := resources.StatefulSet(cr, "", "", &security.Settings{})
	if ss.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
		t.Fatalf("expected OnDelete update strategy")
	}
	y := mustYAML(t, toSSSnapshot(ss))
	want := loadGolden(t, "testdata/standalone_statefulset.yaml")
	if string(y) != string(want) {
		t.Fatalf("golden mismatch for standalone ss:\nGot:\n%s\nWant:\n%s", string(y), string(want))
	}
}

func TestDesiredStatefulSet_Golden_Sentinel(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeSentinel)
	ss := resources.StatefulSet(cr, "", "", &security.Settings{})
	if ss.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
		t.Fatalf("expected OnDelete update strategy")
	}
	if len(ss.Spec.Template.Spec.Containers) != 2 {
		t.Fatalf("expected redis + metrics containers, got: %v", ss.Spec.Template.Spec.Containers)
	}
	var hasRedis, hasMetrics bool
	for _, c := range ss.Spec.Template.Spec.Containers {
		if c.Name == core.RedisContainerName {
			hasRedis = true
		}
		if c.Name == core.MetricsContainerName {
			hasMetrics = true
		}
	}
	if !hasRedis || !hasMetrics {
		t.Fatalf("expected redis and metrics containers, got: %v", ss.Spec.Template.Spec.Containers)
	}
	y := mustYAML(t, toSSSnapshot(ss))
	want := loadGolden(t, "testdata/sentinel_statefulset.yaml")
	if string(y) != string(want) {
		t.Fatalf("golden mismatch for sentinel ss:\nGot:\n%s\nWant:\n%s", string(y), string(want))
	}
}

func TestStatefulSet_ConfigMountsAndArgs_Standalone(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeStandalone)
	ss := resources.StatefulSet(cr, "", "", &security.Settings{})
	// config volume present
	foundVol := false
	for _, v := range ss.Spec.Template.Spec.Volumes {
		if v.Name == "config" && v.ConfigMap != nil {
			foundVol = true
			break
		}
	}
	if !foundVol {
		t.Fatalf("config volume not found")
	}
	// redis container has command/args and mount
	var rc *corev1.Container
	for i := range ss.Spec.Template.Spec.Containers {
		if ss.Spec.Template.Spec.Containers[i].Name == core.RedisContainerName {
			rc = &ss.Spec.Template.Spec.Containers[i]
			break
		}
	}
	if rc == nil {
		t.Fatalf("redis container not found")
	}
	if rc.Command == nil || rc.Command[0] != "valkey-server" {
		t.Fatalf("unexpected redis command: %v", rc.Command)
	}
	if rc.SecurityContext == nil || rc.SecurityContext.ReadOnlyRootFilesystem == nil || !*rc.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("redis container must run with readOnlyRootFilesystem")
	}
	if len(rc.Args) == 0 || rc.Args[0] != "/runtime-conf/redis.conf" {
		t.Fatalf("unexpected redis args: %v", rc.Args)
	}
	hasConfMount := false
	for _, m := range rc.VolumeMounts {
		if m.Name == "config" && m.MountPath == "/conf" {
			hasConfMount = true
		}
	}
	if !hasConfMount {
		t.Fatalf("redis container missing config mount")
	}
	if ss.Spec.Template.Spec.SecurityContext == nil || ss.Spec.Template.Spec.SecurityContext.RunAsNonRoot == nil || !*ss.Spec.Template.Spec.SecurityContext.RunAsNonRoot {
		t.Fatalf("pod security context must enforce runAsNonRoot")
	}
}

func TestStatefulSet_NoSentinelSidecar_InSentinelMode(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeSentinel)
	ss := resources.StatefulSet(cr, "", "", &security.Settings{})
	for _, c := range ss.Spec.Template.Spec.Containers {
		if c.Name == core.SentinelContainerName {
			t.Fatalf("unexpected sentinel sidecar in redis pods")
		}
	}
}

func TestStatefulSet_TLSHashAnnotation(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeStandalone)
	sec := &security.Settings{TLS: security.TLSSettings{Enabled: true}}
	ss := resources.StatefulSet(cr, "cfg", "tls123", sec)
	if ss.Spec.Template.Annotations[resources.ConfigHashAnnotationKey] != "cfg" {
		t.Fatalf("expected config hash annotation")
	}
	if ss.Spec.Template.Annotations[resources.TLSSecretHashAnnotationKey] != "tls123" {
		t.Fatalf("expected tls hash annotation")
	}
}

func TestDesiredStatefulSet_CommandFollowsImageHints(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeStandalone)
	cr.Spec.Engine = keyvalv1alpha1.EngineValkey
	cr.Spec.Image = "redis:7.2"
	ss := resources.StatefulSet(cr, "", "", &security.Settings{})
	var rc *corev1.Container
	for i := range ss.Spec.Template.Spec.Containers {
		if ss.Spec.Template.Spec.Containers[i].Name == core.RedisContainerName {
			rc = &ss.Spec.Template.Spec.Containers[i]
			break
		}
	}
	if rc == nil {
		t.Fatalf("redis container not found")
	}
	if rc.Command == nil || rc.Command[0] != "redis-server" {
		t.Fatalf("expected redis command when image is redis: %v", rc.Command)
	}
	if rc.SecurityContext == nil || rc.SecurityContext.AllowPrivilegeEscalation == nil || *rc.SecurityContext.AllowPrivilegeEscalation {
		t.Fatalf("redis container should disable privilege escalation")
	}

	// switching image back to valkey should restore valkey command even if engine set to Redis
	cr.Spec.Engine = keyvalv1alpha1.EngineRedis
	cr.Spec.Image = "valkey/valkey:7.2"
	ss = resources.StatefulSet(cr, "", "", &security.Settings{})
	rc = nil
	for i := range ss.Spec.Template.Spec.Containers {
		if ss.Spec.Template.Spec.Containers[i].Name == core.RedisContainerName {
			rc = &ss.Spec.Template.Spec.Containers[i]
			break
		}
	}
	if rc == nil {
		t.Fatalf("redis container not found on second render")
	}
	if len(rc.Command) == 0 || rc.Command[0] != "valkey-server" {
		t.Fatalf("expected valkey command when image is valkey: %v", rc.Command)
	}
}

func TestDesiredStatefulSet_MergesPodMetadata(t *testing.T) {
	t.Parallel()
	cr := newCR("demo", keyvalv1alpha1.ModeStandalone)
	cr.Spec.PodLabels = map[string]string{"tier": "cache", core.LabelAppKey: "override"}
	cr.Spec.PodAnnotations = map[string]string{"team": "platform"}
	ss := resources.StatefulSet(cr, "", "", &security.Settings{})
	if ss.Spec.Template.Labels[core.LabelAppKey] != "demo-redis" {
		t.Fatalf("operator label must be preserved, got %s", ss.Spec.Template.Labels[core.LabelAppKey])
	}
	if ss.Spec.Template.Labels["tier"] != "cache" {
		t.Fatalf("expected custom label tier=cache, labels=%v", ss.Spec.Template.Labels)
	}
	if ss.Spec.Template.Annotations["team"] != "platform" {
		t.Fatalf("expected custom annotation, got %v", ss.Spec.Template.Annotations)
	}
}

func mustYAML(t *testing.T, obj interface{}) []byte {
	t.Helper()
	y, err := yaml.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal yaml: %v", err)
	}
	return y
}

func loadGolden(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	return b
}

// toSSSnapshot reduces the StatefulSet to a stable, minimal snapshot for golden testing.
func toSSSnapshot(ss *appsv1.StatefulSet) interface{} {
	// collect containers
	type C struct {
		Name  string  `json:"name"`
		Ports []int32 `json:"ports"`
	}
	var cs []C
	for _, c := range ss.Spec.Template.Spec.Containers {
		var ports []int32
		for _, p := range c.Ports {
			ports = append(ports, p.ContainerPort)
		}
		cs = append(cs, C{Name: c.Name, Ports: ports})
	}
	var initContainers []C
	for _, c := range ss.Spec.Template.Spec.InitContainers {
		var ports []int32
		for _, p := range c.Ports {
			ports = append(ports, p.ContainerPort)
		}
		initContainers = append(initContainers, C{Name: c.Name, Ports: ports})
	}
	// collect pvc templates
	type PVC struct {
		Name        string
		AccessModes []corev1.PersistentVolumeAccessMode
	}
	var pvcs []PVC
	for _, p := range ss.Spec.VolumeClaimTemplates {
		pvcs = append(pvcs, PVC{Name: p.Name, AccessModes: p.Spec.AccessModes})
	}
	// collect volumes (only names)
	type V struct {
		Name     string `json:"name"`
		EmptyDir bool   `json:"emptyDir"`
	}
	var vs []V
	for _, v := range ss.Spec.Template.Spec.Volumes {
		vs = append(vs, V{Name: v.Name, EmptyDir: v.EmptyDir != nil})
	}
	// sort selector for stability
	sel := make([]string, 0, len(ss.Spec.Selector.MatchLabels))
	for k, v := range ss.Spec.Selector.MatchLabels {
		sel = append(sel, k+"="+v)
	}
	sort.Strings(sel)

	return struct {
		Metadata struct {
			Name      string            `json:"name"`
			Namespace string            `json:"namespace"`
			Labels    map[string]string `json:"labels"`
		} `json:"metadata"`
		Spec struct {
			Replicas       *int32   `json:"replicas"`
			ServiceName    string   `json:"serviceName"`
			Selector       []string `json:"selector"`
			UpdateStrategy string   `json:"updateStrategy"`
			InitContainers []C      `json:"initContainers"`
			Containers     []C      `json:"containers"`
			PVCs           []PVC    `json:"volumeClaimTemplates"`
			Volumes        []V      `json:"volumes"`
		} `json:"spec"`
	}{
		Metadata: struct {
			Name      string            `json:"name"`
			Namespace string            `json:"namespace"`
			Labels    map[string]string `json:"labels"`
		}{Name: ss.Name, Namespace: ss.Namespace, Labels: ss.Labels},
		Spec: struct {
			Replicas       *int32   `json:"replicas"`
			ServiceName    string   `json:"serviceName"`
			Selector       []string `json:"selector"`
			UpdateStrategy string   `json:"updateStrategy"`
			InitContainers []C      `json:"initContainers"`
			Containers     []C      `json:"containers"`
			PVCs           []PVC    `json:"volumeClaimTemplates"`
			Volumes        []V      `json:"volumes"`
		}{
			Replicas:    ss.Spec.Replicas,
			ServiceName: ss.Spec.ServiceName,
			Selector:    sel,
			UpdateStrategy: func() string {
				if ss.Spec.UpdateStrategy.Type == "" {
					return ""
				}
				return string(ss.Spec.UpdateStrategy.Type)
			}(),
			InitContainers: initContainers,
			Containers:     cs,
			PVCs:           pvcs,
			Volumes:        vs,
		},
	}
}

// toServiceSnapshot reduces the Service to a minimal stable snapshot.
func toServiceSnapshot(svc *corev1.Service) interface{} {
	type P struct {
		Name string `json:"name"`
		Port int32  `json:"port"`
	}
	var ports []P
	for _, p := range svc.Spec.Ports {
		ports = append(ports, P{Name: p.Name, Port: p.Port})
	}
	// sort selector for stability
	sel := make([]string, 0, len(svc.Spec.Selector))
	for k, v := range svc.Spec.Selector {
		sel = append(sel, k+"="+v)
	}
	sort.Strings(sel)

	return struct {
		Metadata struct {
			Name      string            `json:"name"`
			Namespace string            `json:"namespace"`
			Labels    map[string]string `json:"labels"`
		} `json:"metadata"`
		Spec struct {
			ClusterIP string   `json:"clusterIP"`
			Selector  []string `json:"selector"`
			Ports     []P      `json:"ports"`
		} `json:"spec"`
	}{
		Metadata: struct {
			Name      string            `json:"name"`
			Namespace string            `json:"namespace"`
			Labels    map[string]string `json:"labels"`
		}{Name: svc.Name, Namespace: svc.Namespace, Labels: svc.Labels},
		Spec: struct {
			ClusterIP string   `json:"clusterIP"`
			Selector  []string `json:"selector"`
			Ports     []P      `json:"ports"`
		}{ClusterIP: svc.Spec.ClusterIP, Selector: sel, Ports: ports},
	}
}
