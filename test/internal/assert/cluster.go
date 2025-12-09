//go:build e2e || chaos

package assert

import (
	"bufio"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/harness"
)

// MasterService ensures the master Service exists and targets a single ready pod.
func MasterService(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(h.Context(), timeout)
	defer cancel()

	svc := h.WaitForService(ctx, MasterServiceName(cluster.Name), timeout)
	if svc.Spec.Selector == nil {
		t.Fatalf("master service %s has no selector", svc.Name)
	}
	if role := svc.Spec.Selector["role"]; role != string(keyvalv1alpha1.PodRoleMaster) {
		t.Fatalf("master service selector mismatch: %q", role)
	}

	pods := &corev1.PodList{}
	selector := labels.Set{
		"keyvalcluster": cluster.Name,
		"role":          string(keyvalv1alpha1.PodRoleMaster),
	}
	if err := h.Client().List(ctx, pods, client.InNamespace(h.Namespace()), client.MatchingLabels(selector)); err != nil {
		t.Fatalf("list master pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected single master pod, got %d", len(pods.Items))
	}
	master := pods.Items[0]
	if master.Name != cluster.Status.MasterPod {
		t.Fatalf("status masterPod=%s, but selector returns %s", cluster.Status.MasterPod, master.Name)
	}
	if !isPodReady(&master) {
		t.Fatalf("master pod %s is not ready", master.Name)
	}
}

func isPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for i := range pod.Status.Conditions {
		cond := pod.Status.Conditions[i]
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func MasterServiceName(cluster string) string {
	return cluster + "-master"
}

// RedisPodOrdinals asserts that redis pods match the expected count and ordinals.
func RedisPodOrdinals(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, expected int) []corev1.Pod {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.Context(), 5*time.Minute)
	defer cancel()

	selector := labels.Set{"app": fmt.Sprintf("%s-redis", cluster.Name)}
	var (
		pods  corev1.PodList
		ready []corev1.Pod
	)

	err := wait.PollUntilContextTimeout(ctx, time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := h.Client().List(ctx, &pods, client.InNamespace(h.Namespace()), client.MatchingLabels(selector)); err != nil {
			return false, err
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
		items := append([]corev1.Pod(nil), pods.Items...)
		sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
		if len(items) != expected {
			return false, nil
		}
		for i := range items {
			name := items[i].Name
			if ord := ordinal(name); ord != i {
				return false, nil
			}
			if !isPodReady(&items[i]) {
				return false, nil
			}
		}
		ready = items
		return true, nil
	})
	if err != nil {
		t.Fatalf("wait for redis pod ordinals: %v", err)
	}
	return ready
}

// RedisPodsImage waits until all Redis pods use the expected image.
func RedisPodsImage(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, expectedImage string, timeout time.Duration) {
	t.Helper()
	if expectedImage == "" {
		t.Fatalf("expected image must be provided")
	}

	ctx, cancel := context.WithTimeout(h.Context(), timeout)
	defer cancel()

	selector := labels.Set{"app": fmt.Sprintf("%s-redis", cluster.Name)}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var pods corev1.PodList
		if err := h.Client().List(ctx, &pods, client.InNamespace(h.Namespace()), client.MatchingLabels(selector)); err != nil {
			return false, err
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
		for i := range pods.Items {
			containers := pods.Items[i].Spec.Containers
			if len(containers) == 0 {
				return false, nil
			}
			if containers[0].Image != expectedImage {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait for redis pods to use image %s: %v", expectedImage, err)
	}
}

// MetricsExporter validates that every redis pod exposes the metrics sidecar on the expected port.
func MetricsExporter(t *testing.T, pods []corev1.Pod, expectedPort int32) {
	t.Helper()
	if expectedPort <= 0 {
		t.Fatalf("expectedPort must be positive")
	}
	wantArg := fmt.Sprintf("--web.listen-address=:%d", expectedPort)
	for i := range pods {
		pod := pods[i]
		var metrics *corev1.Container
		for j := range pod.Spec.Containers {
			c := &pod.Spec.Containers[j]
			if c.Name == "metrics" {
				metrics = c
				break
			}
		}
		if metrics == nil {
			t.Fatalf("pod %s is missing metrics container", pod.Name)
		}
		if metrics.Image == "" {
			t.Fatalf("pod %s metrics container has empty image", pod.Name)
		}
		if !hasPort(metrics.Ports, expectedPort) {
			t.Fatalf("pod %s metrics container missing port %d", pod.Name, expectedPort)
		}
		if !containsArg(metrics.Args, wantArg) {
			t.Fatalf("pod %s metrics args %v do not include %q", pod.Name, metrics.Args, wantArg)
		}
		if !hasRedisAddr(metrics.Args) {
			t.Fatalf("pod %s metrics args %v missing --redis.addr", pod.Name, metrics.Args)
		}
	}
}

func hasPort(ports []corev1.ContainerPort, port int32) bool {
	for _, p := range ports {
		if p.Name == "metrics" && p.ContainerPort == port {
			return true
		}
	}
	return false
}

func containsArg(args []string, expected string) bool {
	for _, a := range args {
		if a == expected {
			return true
		}
	}
	return false
}

func hasRedisAddr(args []string) bool {
	for _, a := range args {
		if strings.HasPrefix(a, "--redis.addr=") {
			return true
		}
	}
	return false
}

// LogPodDistribution prints the pod-to-node mapping and aggregate placement.
func LogPodDistribution(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, timeout time.Duration) {
	t.Helper()
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	ctx, cancel := context.WithTimeout(h.Context(), timeout)
	defer cancel()

	selector := labels.Set{"keyvalcluster": cluster.Name}
	var pods corev1.PodList
	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		if err := h.Client().List(ctx, &pods, client.InNamespace(h.Namespace()), client.MatchingLabels(selector)); err != nil {
			return false, err
		}
		return len(pods.Items) > 0, nil
	}); err != nil {
		t.Logf("pod distribution: list failed: %v", err)
		return
	}

	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	counts := map[string]int{}
	for i := range pods.Items {
		pod := pods.Items[i]
		node := pod.Spec.NodeName
		if node == "" {
			node = "<unscheduled>"
		}
		counts[node]++
		state := "NotReady"
		if isPodReady(&pod) {
			state = "Ready"
		}
		t.Logf("pod %s -> node=%s (%s)", pod.Name, node, state)
	}
	if len(counts) == 0 {
		return
	}
	summary := make([]string, 0, len(counts))
	for node, count := range counts {
		summary = append(summary, fmt.Sprintf("%s=%d", node, count))
	}
	sort.Strings(summary)
	t.Logf("pod distribution summary: %s", strings.Join(summary, ", "))
}

// SentinelService verifies the sentinel Service endpoints match the expected quorum.
func SentinelService(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, timeout time.Duration) {
	t.Helper()
	if cluster.Spec.Mode != keyvalv1alpha1.ModeSentinel {
		t.Fatalf("sentinel service assertion requires sentinel mode")
	}

	ctx, cancel := context.WithTimeout(h.Context(), timeout)
	defer cancel()

	name := fmt.Sprintf("%s-sentinel", cluster.Name)
	key := types.NamespacedName{Namespace: h.Namespace(), Name: name}

	var svc corev1.Service
	if err := h.Client().Get(ctx, key, &svc); err != nil {
		t.Fatalf("get sentinel service: %v", err)
	}
	expectedPort := SentinelPort(cluster)
	port := findServicePort(&svc, "sentinel")
	if port == nil {
		t.Fatalf("sentinel service missing port definition")
	}
	if port.Port != int32(expectedPort) {
		t.Fatalf("sentinel service port=%d, expected %d", port.Port, expectedPort)
	}

	expected := int(ptr.Deref(cluster.Spec.SentinelCount, int32(0)))
	if expected <= 0 {
		t.Fatalf("invalid sentinel count %d", expected)
	}

	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var eps corev1.Endpoints
		if err := h.Client().Get(ctx, key, &eps); err != nil {
			return false, err
		}
		ready := 0
		for _, subset := range eps.Subsets {
			ready += len(subset.Addresses)
		}
		return ready == expected, nil
	}); err != nil {
		t.Fatalf("wait for sentinel endpoints: %v", err)
	}
}

// SentinelStatefulSet ensures the sentinel StatefulSet is ready and uses the desired image.
func SentinelStatefulSet(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, expectedImage string, timeout time.Duration) {
	t.Helper()
	if cluster.Spec.Mode != keyvalv1alpha1.ModeSentinel {
		t.Fatalf("sentinel statefulset assertion requires sentinel mode")
	}

	ctx, cancel := context.WithTimeout(h.Context(), timeout)
	defer cancel()

	name := fmt.Sprintf("%s-sentinel", cluster.Name)
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: name}
	var sts appsv1.StatefulSet
	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		if err := h.Client().Get(ctx, key, &sts); err != nil {
			return false, err
		}
		expected := ptr.Deref(sts.Spec.Replicas, int32(0))
		return sts.Status.ReadyReplicas == expected && sts.Status.ReadyReplicas > 0, nil
	}); err != nil {
		t.Fatalf("wait for sentinel statefulset ready: %v", err)
	}

	if len(sts.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("sentinel statefulset has no containers")
	}
	if image := sts.Spec.Template.Spec.Containers[0].Image; image != expectedImage {
		t.Fatalf("sentinel statefulset image=%s, expected %s", image, expectedImage)
	}
}

func findServicePort(svc *corev1.Service, name string) *corev1.ServicePort {
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Name == name {
			return &svc.Spec.Ports[i]
		}
	}
	return nil
}

func SentinelPort(cr *keyvalv1alpha1.KeyValCluster) int {
	if cr.Spec.SentinelService != nil && cr.Spec.SentinelService.Ports != nil && cr.Spec.SentinelService.Ports.Sentinel != 0 {
		return int(cr.Spec.SentinelService.Ports.Sentinel)
	}
	if cr.Spec.SentinelConfig != nil {
		if value, ok := cr.Spec.SentinelConfig["port"]; ok && value != "" {
			if port, err := strconv.Atoi(value); err == nil && port > 0 && port <= 65535 {
				return port
			}
		}
	}
	return 26379
}

// ReplicationHealthy asserts master/replica alignment using redis INFO replication.
func ReplicationHealthy(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, timeout time.Duration) {
	t.Helper()
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(h.Context(), timeout)
	defer cancel()

	selector := labels.Set{"keyvalcluster": cluster.Name}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var podList corev1.PodList
		if err := h.Client().List(ctx, &podList, client.InNamespace(h.Namespace()), client.MatchingLabels(selector)); err != nil {
			return false, err
		}
		var masterPod *corev1.Pod
		var replicaPods []corev1.Pod
		for i := range podList.Items {
			pod := podList.Items[i]
			role := pod.Labels["role"]
			switch role {
			case string(keyvalv1alpha1.PodRoleSentinel):
				continue
			case string(keyvalv1alpha1.PodRoleMaster):
				copy := pod
				masterPod = &copy
			case string(keyvalv1alpha1.PodRoleReplica):
				replicaPods = append(replicaPods, pod)
			}
		}
		if masterPod == nil {
			return false, nil
		}
		info, err := redisInfo(ctx, h, masterPod.Name, redisPort(cluster))
		if err != nil {
			return false, nil
		}
		if role := info["role"]; role != "master" {
			return false, nil
		}
		wantReplicas := len(replicaPods)
		if value := info["connected_slaves"]; value != "" {
			if n, err := strconv.Atoi(value); err == nil {
				if n != wantReplicas {
					return false, nil
				}
			}
		}
		for i := range replicaPods {
			info, err := redisInfo(ctx, h, replicaPods[i].Name, redisPort(cluster))
			if err != nil {
				return false, nil
			}
			role := info["role"]
			if role != "slave" && role != "replica" {
				return false, nil
			}
			if status := info["master_link_status"]; status != "up" {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait replication healthy: %v", err)
	}
}

func redisInfo(ctx context.Context, h *harness.Harness, podName string, port int) (map[string]string, error) {
	args := []string{"redis-cli", "--no-auth-warning", "-p", strconv.Itoa(port), "INFO", "replication"}
	stdout, stderr, err := h.Exec(ctx, podName, "redis", args...)
	if err != nil {
		return nil, fmt.Errorf("exec redis-cli stderr=%s: %w", strings.TrimSpace(stderr), err)
	}
	return parseInfo(stdout), nil
}

func parseInfo(body string) map[string]string {
	values := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		values[parts[0]] = parts[1]
	}
	return values
}

func redisPort(cr *keyvalv1alpha1.KeyValCluster) int {
	if cr.Spec.RedisConfig != nil {
		if value, ok := cr.Spec.RedisConfig["port"]; ok && value != "" {
			if port, err := strconv.Atoi(value); err == nil && port > 0 && port <= 65535 {
				return port
			}
		}
	}
	return 6379
}

func ordinal(name string) int {
	idx := strings.LastIndexByte(name, '-')
	if idx < 0 || idx+1 >= len(name) {
		return -1
	}
	value, err := strconv.Atoi(name[idx+1:])
	if err != nil {
		return -1
	}
	return value
}

// ReadySentinelPod returns a ready sentinel pod for the cluster.
func ReadySentinelPod(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster) *corev1.Pod {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.Context(), 2*time.Minute)
	defer cancel()

	selector := labels.Set{"app": fmt.Sprintf("%s-sentinel", cluster.Name)}
	var pods corev1.PodList
	if err := h.Client().List(ctx, &pods, client.InNamespace(h.Namespace()), client.MatchingLabels(selector)); err != nil {
		t.Fatalf("list sentinel pods: %v", err)
	}
	items := pods.Items
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	for i := range items {
		if isPodReady(&items[i]) {
			return &items[i]
		}
	}
	t.Fatalf("no ready sentinel pods found for cluster %s", cluster.Name)
	return nil
}

// WaitForMasterChange waits until master pod differs from previous.
func WaitForMasterChange(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, previous string, previousUID types.UID, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.Context(), timeout)
	defer cancel()

	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var latest keyvalv1alpha1.KeyValCluster
		if err := h.Client().Get(ctx, key, &latest); err != nil {
			return false, err
		}
		var podList corev1.PodList
		if err := h.Client().List(ctx, &podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(labels.Set{"keyvalcluster": cluster.Name})); err != nil {
			return false, err
		}
		var masterPod *corev1.Pod
		for i := range podList.Items {
			pod := podList.Items[i]
			if pod.Labels["role"] == string(keyvalv1alpha1.PodRoleMaster) && isPodReady(&pod) {
				copy := pod
				masterPod = &copy
				break
			}
		}
		if masterPod == nil {
			return false, nil
		}
		name := masterPod.Name
		switched := false
		if name != previous {
			switched = true
		} else if masterPod.UID != previousUID {
			switched = true
		}
		if !switched {
			return false, nil
		}
		if latest.Status.MasterPod != name {
			return false, nil
		}
		*cluster = latest
		return true, nil
	}); err != nil {
		t.Fatalf("wait for master change: %v", err)
	}
}

// MasterPod returns the current master Pod object.
func MasterPod(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster) *corev1.Pod {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.Context(), 30*time.Second)
	defer cancel()

	var latest keyvalv1alpha1.KeyValCluster
	if err := h.Client().Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, &latest); err != nil {
		t.Fatalf("refresh cluster status: %v", err)
	}
	*cluster = latest
	name := cluster.Status.MasterPod
	if name == "" {
		t.Fatalf("cluster %s has empty masterPod", cluster.Name)
	}
	var pod corev1.Pod
	if err := h.Client().Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, &pod); err != nil {
		t.Fatalf("get master pod %s: %v", name, err)
	}
	return &pod
}
