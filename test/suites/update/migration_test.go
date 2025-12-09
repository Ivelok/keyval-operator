//go:build e2e

package update

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

func TestMigrationStandaloneToSentinelAndBack(t *testing.T) {
	t.Parallel()

	s := suite.New(t)
	manager := cluster.NewManager(s.Harness)

	// Start as Standalone
	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName("mode-migration").
		WithLabels(map[string]string{"suite": "update", "test": "migration"}).
		WithMode(keyvalv1alpha1.ModeStandalone).
		WithReplicas(1).
		WithEphemeralStorage()
	cr := builder.Build()

	s.Step("create-standalone", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply standalone cluster: %v", err)
		}
	})

	s.Step("wait-standalone-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 4*time.Minute)
		*cr = *updated
		
		// Verify Standalone state
		assert.MasterService(t, s.Harness, cr, 1*time.Minute)
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, 1)
		if len(pods) != 1 {
			t.Fatalf("expected 1 pod in standalone, got %d", len(pods))
		}
		
		// Ensure NO sentinel resources
		assertNoSentinelResources(t, s, cr)
	})

	s.Step("migrate-to-sentinel", func(ctx context.Context) {
		sentinelCount := int32(3)
		patched := cr.DeepCopy()
		patched.Spec.Mode = keyvalv1alpha1.ModeSentinel
		patched.Spec.RedisReplicas = 3
		patched.Spec.SentinelCount = &sentinelCount
		
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
			t.Fatalf("patch to sentinel mode: %v", err)
		}
		*cr = *patched
	})

	s.Step("wait-sentinel-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 5*time.Minute)
		*cr = *updated
		
		// Verify Sentinel state
		assert.MasterService(t, s.Harness, cr, 1*time.Minute)
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
		
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, 3)
		if len(pods) != 3 {
			t.Fatalf("expected 3 pods in sentinel mode, got %d", len(pods))
		}
		
		// Verify Sentinel StatefulSet exists
		key := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name + "-sentinel"}
		var ss appsv1.StatefulSet
		if err := s.Harness.Client().Get(ctx, key, &ss); err != nil {
			t.Fatalf("expected sentinel statefulset to exist: %v", err)
		}
	})

	s.Step("downgrade-to-standalone", func(ctx context.Context) {
		patched := cr.DeepCopy()
		patched.Spec.Mode = keyvalv1alpha1.ModeStandalone
		patched.Spec.RedisReplicas = 1
		patched.Spec.SentinelCount = nil
		
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
			t.Fatalf("patch to standalone mode: %v", err)
		}
		*cr = *patched
	})

	s.Step("wait-downgrade-ready", func(ctx context.Context) {
		// Wait for CR to report 1 replica and ready
		updated := manager.WaitReady(ctx, cr, 4*time.Minute)
		*cr = *updated
		
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, 1)
		if len(pods) != 1 {
			t.Fatalf("expected 1 pod after downgrade, got %d", len(pods))
		}
		
		// Wait for Sentinel resources to be deleted
		// We poll because deletion might be async relative to CR status ready
		if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			key := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name + "-sentinel"}
			var ss appsv1.StatefulSet
			err := s.Harness.Client().Get(ctx, key, &ss)
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err != nil {
				return false, err
			}
			// If it exists, check if it's deleting
			if ss.DeletionTimestamp != nil {
				return false, nil // Still deleting
			}
			return false, nil // Exists and not deleting (bad)
		}); err != nil {
			t.Fatalf("sentinel statefulset was not deleted: %v", err)
		}
		
		assertNoSentinelResources(t, s, cr)
	})
}

func assertNoSentinelResources(t *testing.T, s *suite.Suite, cr *keyvalv1alpha1.KeyValCluster) {
	ctx := s.Context()
	name := cr.Name
	ns := cr.Namespace

	// Check StatefulSet
	key := types.NamespacedName{Namespace: ns, Name: name + "-sentinel"}
	var ss appsv1.StatefulSet
	if err := s.Harness.Client().Get(ctx, key, &ss); !apierrors.IsNotFound(err) {
		t.Fatalf("expected no sentinel statefulset, but found one (err=%v)", err)
	}

	// Check Service
	keySvc := types.NamespacedName{Namespace: ns, Name: name + "-sentinel"}
	var svc corev1.Service
	if err := s.Harness.Client().Get(ctx, keySvc, &svc); !apierrors.IsNotFound(err) {
		t.Fatalf("expected no sentinel service, but found one (err=%v)", err)
	}
	
	// Check Headless Service
	keyHeadless := types.NamespacedName{Namespace: ns, Name: name + "-sentinel-headless"}
	if err := s.Harness.Client().Get(ctx, keyHeadless, &svc); !apierrors.IsNotFound(err) {
		t.Fatalf("expected no sentinel headless service, but found one (err=%v)", err)
	}
}
