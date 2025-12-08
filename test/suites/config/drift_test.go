//go:build e2e

package config

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

func TestRedisConfigDriftStandalone(t *testing.T) {
	s := suite.New(t)

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName("config-standalone-valkey").
		WithLabels(map[string]string{"suite": "config", "engine": "valkey", "mode": "standalone"}).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithImage(s.Harness.RedisImage()).
		WithEphemeralStorage()
	cr := builder.Build()

	manager := cluster.NewManager(s.Harness)

	s.Step("create-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply cluster: %v", err)
		}
	})

	s.Step("wait-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 2*time.Minute)
		*cr = *updated
		assert.MasterService(t, s.Harness, cr, 45*time.Second)
		assert.ReplicationHealthy(t, s.Harness, cr, 1*time.Minute)
	})

	// Change redisConfig
	s.Step("patch-redis-config", func(ctx context.Context) {
		patched := cr.DeepCopy()
		if patched.Spec.RedisConfig == nil {
			patched.Spec.RedisConfig = map[string]string{}
		}
		patched.Spec.RedisConfig["maxmemory-policy"] = "allkeys-lru"
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
			t.Fatalf("patch redis config: %v", err)
		}
		*cr = *patched
	})

	s.Step("wait-config-applied", func(ctx context.Context) {
		waitForConfig(t, s, cr, "maxmemory-policy", "allkeys-lru", readyTimeout)
		updated := manager.WaitReady(ctx, cr, readyTimeout)
		*cr = *updated
	})

	s.Step("verify-config", func(ctx context.Context) {
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, 1)
		stdout, stderr, err := s.Harness.Exec(ctx, pods[0].Name, "redis", "redis-cli", "CONFIG", "GET", "maxmemory-policy")
		if err != nil {
			t.Fatalf("exec redis config get: %s (%v)", strings.TrimSpace(stderr), err)
		}
		if !strings.Contains(stdout, "allkeys-lru") {
			t.Fatalf("expected config to contain allkeys-lru, got %q", stdout)
		}
	})
}

func TestServiceToggleSentinel(t *testing.T) {
	s := suite.New(t)

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName("config-sentinel-valkey").
		WithLabels(map[string]string{"suite": "config", "engine": "valkey", "mode": "sentinel"}).
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithImage(s.Harness.RedisImage()).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage()
	cr := builder.Build()

	manager := cluster.NewManager(s.Harness)

	s.Step("create-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply cluster: %v", err)
		}
	})

	s.Step("wait-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, readyTimeout)
		*cr = *updated
		assert.MasterService(t, s.Harness, cr, 45*time.Second)
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 1*time.Minute)
	})

	s.Step("disable-services", func(ctx context.Context) {
		patched := cr.DeepCopy()
		patched.Spec.Service = &keyvalv1alpha1.ServiceSpec{Create: pointerBool(false)}
		patched.Spec.ReplicasService = &keyvalv1alpha1.ServiceSpec{Create: pointerBool(false)}
		patched.Spec.SentinelService = &keyvalv1alpha1.ServiceSpec{Create: pointerBool(false)}
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
			t.Fatalf("disable services patch: %v", err)
		}
		*cr = *patched
	})

	s.Step("wait-services-removed", func(ctx context.Context) {
		waitNoService(t, s, fmt.Sprintf("%s-master", cr.Name), 2*time.Minute)
		waitNoService(t, s, fmt.Sprintf("%s-replicas", cr.Name), 2*time.Minute)
		waitNoService(t, s, fmt.Sprintf("%s-sentinel", cr.Name), 2*time.Minute)
	})

	s.Step("enable-services", func(ctx context.Context) {
		patched := cr.DeepCopy()
		patched.Spec.Service.Create = pointerBool(true)
		patched.Spec.ReplicasService.Create = pointerBool(true)
		patched.Spec.SentinelService.Create = pointerBool(true)
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
			t.Fatalf("enable services patch: %v", err)
		}
		*cr = *patched
	})

	s.Step("wait-services-restored", func(ctx context.Context) {
		waitServiceExists(t, s, fmt.Sprintf("%s-master", cr.Name), 2*time.Minute)
		waitServiceExists(t, s, fmt.Sprintf("%s-replicas", cr.Name), 2*time.Minute)
		waitServiceExists(t, s, fmt.Sprintf("%s-sentinel", cr.Name), 2*time.Minute)
		manager.Refresh(ctx, cr)
		assert.MasterService(t, s.Harness, cr, 45*time.Second)
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 1*time.Minute)
	})
}

func waitForPodAnnotation(t *testing.T, s *suite.Suite, clusterName, key string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Context(), timeout)
	defer cancel()

	selector := labels.Set{"app": fmt.Sprintf("%s-redis", clusterName)}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var pods corev1.PodList
		if err := s.Harness.Client().List(ctx, &pods, client.InNamespace(s.Harness.Namespace()), client.MatchingLabels(selector)); err != nil {
			return false, err
		}
		for _, pod := range pods.Items {
			if value := pod.Annotations[key]; value != "" {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("wait for rolling annotation %s: %v", key, err)
	}
}

func waitNoService(t *testing.T, s *suite.Suite, name string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Context(), timeout)
	defer cancel()

	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var svc corev1.Service
		err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: s.Harness.Namespace(), Name: name}, &svc)
		if err == nil {
			return false, nil
		}
		return client.IgnoreNotFound(err) == nil, nil
	}); err != nil {
		t.Fatalf("wait for service %s removal: %v", name, err)
	}
}

func waitServiceExists(t *testing.T, s *suite.Suite, name string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Context(), timeout)
	defer cancel()

	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var svc corev1.Service
		if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: s.Harness.Namespace(), Name: name}, &svc); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait for service %s: %v", name, err)
	}
}

func pointerBool(v bool) *bool {
	return &v
}

func waitForConfig(t *testing.T, s *suite.Suite, cr *keyvalv1alpha1.KeyValCluster, key, want string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Context(), timeout)
	defer cancel()

	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, 1)
		stdout, _, err := s.Harness.Exec(ctx, pods[0].Name, "redis", "redis-cli", "CONFIG", "GET", key)
		if err != nil {
			return false, nil
		}
		return strings.Contains(stdout, want), nil
	}); err != nil {
		t.Fatalf("wait for config %s=%s: %v", key, want, err)
	}
}
