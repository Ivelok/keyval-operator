//go:build e2e

package update

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/harness"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

type masterMonitor struct {
	mu   sync.Mutex
	err  error
	done chan struct{}
}

func startMasterMonitor(ctx context.Context, h *harness.Harness, cluster string) *masterMonitor {
	monitor := &masterMonitor{done: make(chan struct{})}
	go func() {
		defer close(monitor.done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		selector := labels.Set{
			"keyvalcluster": cluster,
			"role":          string(keyvalv1alpha1.PodRoleMaster),
		}
		for {
			select {
			case <-ctx.Done():
				monitor.setErr(nil)
				return
			case <-ticker.C:
				var pods corev1.PodList
				if err := h.Client().List(ctx, &pods, client.InNamespace(h.Namespace()), client.MatchingLabels(selector)); err != nil {
					if ctx.Err() != nil {
						monitor.setErr(nil)
						return
					}
					monitor.setErr(fmt.Errorf("list master pods: %w", err))
					return
				}
				if len(pods.Items) > 1 {
					names := make([]string, len(pods.Items))
					for i := range pods.Items {
						names[i] = pods.Items[i].Name
					}
					monitor.setErr(fmt.Errorf("multiple master pods detected: %v", names))
					return
				}
			}
		}
	}()
	return monitor
}

func (m *masterMonitor) setErr(err error) {
	m.mu.Lock()
	if m.err == nil {
		m.err = err
	}
	m.mu.Unlock()
}

func (m *masterMonitor) Wait() error {
	<-m.done
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err
}

func TestGracefulShutdownReplica(t *testing.T) {
	t.Parallel()

	s := suite.New(t)
	manager := cluster.NewManager(s.Harness)

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName("graceful-shutdown").
		WithLabels(map[string]string{"suite": "update", "feature": "graceful-shutdown"}).
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage()
	cr := builder.Build()

	var (
		masterBefore    string
		replicaToDelete string
		monitorCancel   context.CancelFunc
		monitor         *masterMonitor
	)

	s.Step("create-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply cluster: %v", err)
		}
	})

	s.Step("wait-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 4*time.Minute)
		*cr = *updated
		assert.MasterService(t, s.Harness, cr, 1*time.Minute)
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
		assert.MetricsExporter(t, pods, 9121)
		masterBefore = cr.Status.MasterPod
		for _, pod := range pods {
			if pod.Name != masterBefore {
				replicaToDelete = pod.Name
				break
			}
		}
		if replicaToDelete == "" {
			t.Fatalf("failed to identify replica pod for deletion")
		}
		assert.LogPodDistribution(t, s.Harness, cr, 30*time.Second)
	})

	s.Step("start-monitor", func(context.Context) {
		watchCtx, cancel := context.WithCancel(s.Context())
		monitorCancel = cancel
		monitor = startMasterMonitor(watchCtx, s.Harness, cr.Name)
	})

	s.Step("delete-replica", func(ctx context.Context) {
		var pod corev1.Pod
		key := types.NamespacedName{Namespace: cr.Namespace, Name: replicaToDelete}
		if err := s.Harness.Client().Get(ctx, key, &pod); err != nil {
			t.Fatalf("get replica pod: %v", err)
		}
		if err := s.Harness.Client().Delete(ctx, &pod); err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("delete replica pod: %v", err)
		}
	})

	s.Step("wait-replicas-stable", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 4*time.Minute)
		*cr = *updated
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
		if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			var pod corev1.Pod
			key := types.NamespacedName{Namespace: cr.Namespace, Name: replicaToDelete}
			if err := s.Harness.Client().Get(ctx, key, &pod); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return true, nil
				}
			}
			return false, nil
		}); err != nil {
			t.Fatalf("replica %s did not become ready: %v", replicaToDelete, err)
		}
		assert.LogPodDistribution(t, s.Harness, cr, 30*time.Second)
		if cr.Status.MasterPod != masterBefore {
			t.Fatalf("expected master pod to remain %s, got %s", masterBefore, cr.Status.MasterPod)
		}
	})

	s.Step("stop-monitor", func(context.Context) {
		if monitorCancel != nil {
			monitorCancel()
		}
		if monitor != nil {
			if err := monitor.Wait(); err != nil {
				t.Fatalf("master monitor error: %v", err)
			}
		}
	})
}

func TestRollingUpgradeSentinelRedis(t *testing.T) {
	t.Parallel()

	const (
		initialImage  = "redis:7.2"
		upgradedImage = "redis:7.2-alpine"
		configKey     = "maxmemory-policy"
		configValue   = "volatile-lru"
		metricsPortV2 = int32(10081)
	)

	s := suite.New(t)
	manager := cluster.NewManager(s.Harness)

	builder := cluster.NewBuilder(s.Harness.Namespace(), initialImage).
		WithName("upgrade-redis-sentinel").
		WithLabels(map[string]string{"suite": "update", "engine": "redis", "mode": "sentinel"}).
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithEngine(keyvalv1alpha1.EngineRedis).
		WithImage(initialImage).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage()
	cr := builder.Build()

	var (
		monitorCancel context.CancelFunc
		monitor       *masterMonitor
		lastUIDs      map[string]string
	)

	s.Step("create-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply cluster: %v", err)
		}
	})

	s.Step("wait-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 4*time.Minute)
		*cr = *updated
		assert.MasterService(t, s.Harness, cr, 1*time.Minute)
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
		assert.RedisPodsImage(t, s.Harness, cr, initialImage, 1*time.Minute)
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
		assert.MetricsExporter(t, pods, 9121)
		lastUIDs = podUIDMap(pods)
	})

	s.Step("start-master-monitor", func(context.Context) {
		watchCtx, cancel := context.WithCancel(s.Context())
		monitorCancel = cancel
		monitor = startMasterMonitor(watchCtx, s.Harness, cr.Name)
	})

	s.Step("upgrade-image", func(ctx context.Context) {
		patched := cr.DeepCopy()
		patched.Spec.Image = upgradedImage
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
			t.Fatalf("patch image: %v", err)
		}
		*cr = *patched
	})

	s.Step("wait-image-rolling-complete", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 6*time.Minute)
		*cr = *updated
		if err := manager.Refresh(ctx, cr); err != nil {
			t.Fatalf("refresh cluster after image upgrade: %v", err)
		}
		assert.RedisPodsImage(t, s.Harness, cr, upgradedImage, 5*time.Minute)
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
		newUIDs := podUIDMap(pods)
		if changes := countUIDChanges(lastUIDs, newUIDs); changes == 0 {
			t.Fatalf("expected redis pods to restart during image upgrade")
		}
		lastUIDs = newUIDs
	})

	s.Step("patch-redis-config", func(ctx context.Context) {
		patched := cr.DeepCopy()
		if patched.Spec.RedisConfig == nil {
			patched.Spec.RedisConfig = map[string]string{}
		}
		patched.Spec.RedisConfig[configKey] = configValue
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
			t.Fatalf("patch redis config: %v", err)
		}
		*cr = *patched
	})

	s.Step("wait-config-rolling-complete", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 6*time.Minute)
		*cr = *updated
		if err := manager.Refresh(ctx, cr); err != nil {
			t.Fatalf("refresh cluster after config update: %v", err)
		}
		assert.RedisPodsImage(t, s.Harness, cr, upgradedImage, 5*time.Minute)
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
		newUIDs := podUIDMap(pods)
		if changes := countUIDChanges(lastUIDs, newUIDs); changes == 0 {
			t.Logf("config update applied without pod restarts; relying on on-line CONFIG SET")
		} else {
			t.Logf("config update rotated %d pods", changes)
		}
		lastUIDs = newUIDs
	})

	s.Step("patch-metrics-port", func(ctx context.Context) {
		patched := cr.DeepCopy()
		if patched.Spec.Metrics == nil {
			patched.Spec.Metrics = &keyvalv1alpha1.MetricsSpec{}
		}
		patched.Spec.Metrics.Port = metricsPortV2
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
			t.Fatalf("patch metrics port: %v", err)
		}
		*cr = *patched
	})

	s.Step("wait-metrics-rolling-complete", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 6*time.Minute)
		*cr = *updated
		deadline := time.Now().Add(6 * time.Minute)
		for {
			if err := manager.Refresh(ctx, cr); err != nil {
				t.Fatalf("refresh cluster after metrics update: %v", err)
			}
			pods, err := collectReadyRedisPods(ctx, s, cr)
			if err == nil && metricsPortMatches(pods, metricsPortV2) {
				newUIDs := podUIDMap(pods)
				if changes := countUIDChanges(lastUIDs, newUIDs); changes == len(pods) {
					assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
					assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
					assert.MetricsExporter(t, pods, metricsPortV2)
					lastUIDs = newUIDs
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("metrics port change did not converge within deadline")
			}
			select {
			case <-ctx.Done():
				t.Fatalf("context cancelled while waiting for metrics port update: %v", ctx.Err())
			case <-time.After(2 * time.Second):
			}
		}
	})

	s.Step("verify-config-applied", func(ctx context.Context) {
		master := cr.Status.MasterPod
		if master == "" {
			t.Fatalf("masterPod status is empty")
		}
		if err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			stdout, stderr, err := s.Harness.Exec(ctx, master, "redis", "redis-cli", "CONFIG", "GET", configKey)
			if err != nil {
				return false, fmt.Errorf("exec redis config get: %s (%w)", strings.TrimSpace(stderr), err)
			}
			if strings.Contains(stdout, configValue) {
				return true, nil
			}
			t.Logf("config %s still %q; waiting for %s", configKey, strings.TrimSpace(stdout), configValue)
			return false, nil
		}); err != nil {
			t.Fatalf("wait for config %s=%s: %v", configKey, configValue, err)
		}
		waitMasterAlignment(t, s, cr, 1*time.Minute)
		assert.MasterService(t, s.Harness, cr, 1*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
	})

	s.Step("stop-master-monitor", func(context.Context) {
		if monitorCancel != nil {
			monitorCancel()
		}
		if monitor != nil {
			if err := monitor.Wait(); err != nil {
				t.Fatalf("master monitor detected inconsistency: %v", err)
			}
		}
	})
}

func podUIDMap(pods []corev1.Pod) map[string]string {
	out := make(map[string]string, len(pods))
	for i := range pods {
		out[pods[i].Name] = string(pods[i].UID)
	}
	return out
}

func countUIDChanges(previous, current map[string]string) int {
	if len(previous) == 0 || len(current) == 0 {
		return 0
	}
	changes := 0
	for name, uid := range previous {
		if next, ok := current[name]; ok && next != uid {
			changes++
		}
	}
	return changes
}

func waitMasterAlignment(t *testing.T, s *suite.Suite, cluster *keyvalv1alpha1.KeyValCluster, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Context(), timeout)
	defer cancel()

	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	selector := labels.Set{
		"keyvalcluster": cluster.Name,
		"role":          string(keyvalv1alpha1.PodRoleMaster),
	}

	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		if err := s.Harness.Client().Get(ctx, key, cluster); err != nil {
			return false, err
		}
		var pods corev1.PodList
		if err := s.Harness.Client().List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(selector)); err != nil {
			return false, err
		}
		if len(pods.Items) != 1 {
			return false, nil
		}
		return pods.Items[0].Name == cluster.Status.MasterPod, nil
	}); err != nil {
		t.Fatalf("wait for master alignment: %v", err)
	}
}

func collectReadyRedisPods(ctx context.Context, s *suite.Suite, cluster *keyvalv1alpha1.KeyValCluster) ([]corev1.Pod, error) {
	var pods corev1.PodList
	selector := labels.Set{"app": fmt.Sprintf("%s-redis", cluster.Name)}
	if err := s.Harness.Client().List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(selector)); err != nil {
		return nil, err
	}
	if len(pods.Items) != int(cluster.Spec.RedisReplicas) {
		return nil, fmt.Errorf("expected %d redis pods, got %d", cluster.Spec.RedisReplicas, len(pods.Items))
	}
	items := append([]corev1.Pod(nil), pods.Items...)
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	for i := range items {
		if !podReady(&items[i]) {
			return nil, fmt.Errorf("pod %s is not ready", items[i].Name)
		}
	}
	return items, nil
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func metricsPortMatches(pods []corev1.Pod, port int32) bool {
	if port <= 0 {
		return false
	}
	arg := fmt.Sprintf("--web.listen-address=:%d", port)
	for i := range pods {
		var metrics *corev1.Container
		for j := range pods[i].Spec.Containers {
			c := &pods[i].Spec.Containers[j]
			if c.Name == "metrics" {
				metrics = c
				break
			}
		}
		if metrics == nil {
			return false
		}
		if !containerPortEquals(metrics.Ports, port) {
			return false
		}
		if !argPresent(metrics.Args, arg) {
			return false
		}
	}
	return true
}

func containerPortEquals(ports []corev1.ContainerPort, want int32) bool {
	for _, p := range ports {
		if p.Name == "http-metrics" && p.ContainerPort == want {
			return true
		}
	}
	return false
}

func argPresent(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}
