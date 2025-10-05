//go:build e2e

package config

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

func TestServiceSSAAllowsUserAnnotations(t *testing.T) {
	s := suite.New(t)

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName("ssa-service-annotations").
		WithLabels(map[string]string{"suite": "config", "focus": "ssa"}).
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
		updated := manager.WaitReady(ctx, cr, 3*time.Minute)
		*cr = *updated
		assert.MasterService(t, s.Harness, cr, 45*time.Second)
	})

	s.Step("inject-user-annotation", func(ctx context.Context) {
		var svc corev1.Service
		key := types.NamespacedName{Namespace: cr.Namespace, Name: masterServiceName(cr.Name)}
		if err := s.Harness.Client().Get(ctx, key, &svc); err != nil {
			t.Fatalf("get master service: %v", err)
		}
		patched := svc.DeepCopy()
		if patched.Annotations == nil {
			patched.Annotations = map[string]string{}
		}
		patched.Annotations["external.example.com/custom"] = "preserve"
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(&svc)); err != nil {
			t.Fatalf("patch service annotation: %v", err)
		}
	})

	s.Step("trigger-reconcile", func(ctx context.Context) {
		patched := cr.DeepCopy()
		if patched.Spec.RedisConfig == nil {
			patched.Spec.RedisConfig = map[string]string{}
		}
		patched.Spec.RedisConfig["maxmemory-policy"] = "volatile-lru"
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
			t.Fatalf("patch cluster redis config: %v", err)
		}
		*cr = *patched
		updated := manager.WaitReady(ctx, cr, 3*time.Minute)
		*cr = *updated
	})

	s.Step("verify-annotation", func(ctx context.Context) {
		var svc corev1.Service
		key := types.NamespacedName{Namespace: cr.Namespace, Name: masterServiceName(cr.Name)}
		if err := s.Harness.Client().Get(ctx, key, &svc); err != nil {
			t.Fatalf("get master service: %v", err)
		}
		value := svc.Annotations["external.example.com/custom"]
		if value != "preserve" {
			t.Fatalf("expected user annotation to persist, got %q", value)
		}
	})
}

func masterServiceName(cluster string) string {
	return cluster + "-master"
}
