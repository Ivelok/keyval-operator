//go:build e2e

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestPersistentStorageResizeSentinel(t *testing.T) {
	s := suite.New(t)

	name := fmt.Sprintf("storage-sentinel-%d", time.Now().UnixNano())
	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName(name).
		WithLabels(map[string]string{"suite": "storage", "engine": "valkey", "mode": "sentinel"}).
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithImage(s.Harness.RedisImage()).
		WithReplicas(3).
		WithSentinelCount(3)
	cr := builder.Build()
	cr.Spec.Storage = &keyvalv1alpha1.StorageSpec{Type: "Persistent", CleanupOnDelete: true}

	manager := cluster.NewManager(s.Harness)

	s.Step("create-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply cluster: %v", err)
		}
	})

	s.Step("wait-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 4*time.Minute)
		*cr = *updated
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
	})

	s.Step("check-expansion-support", func(ctx context.Context) {
		if !supportsExpansion(t, s.Harness, cr.Name) {
			t.Skip("storage class does not allow expansion; skipping resize test")
		}
	})

	pvcRequests := capturePVCRequests(t, s.Harness, cr.Name)

	s.Step("resize-storage", func(ctx context.Context) {
		wantSize := "2Gi"
		patch := []byte(fmt.Sprintf(`{"spec":{"storage":{"size":"%s"}}}`, wantSize))
		if err := s.Harness.Client().Patch(ctx, cr, client.RawPatch(types.MergePatchType, patch)); err != nil {
			t.Fatalf("patch storage size: %v", err)
		}
		waitForPVCRequest(t, s.Harness, cr.Name, resource.MustParse(wantSize), 5*time.Minute)
		updated := manager.WaitReady(ctx, cr, 4*time.Minute)
		*cr = *updated
	})

	s.Step("verify", func(ctx context.Context) {
		newRequests := capturePVCRequests(t, s.Harness, cr.Name)
		for name, old := range pvcRequests {
			if size := newRequests[name]; size == old {
				t.Fatalf("pvc %s request did not change (still %s)", name, size)
			}
		}
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
		assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
	})
}

func capturePVCRequests(t *testing.T, h *harness.Harness, cluster string) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.Context(), 30*time.Second)
	defer cancel()

	var list corev1.PersistentVolumeClaimList
	if err := h.Client().List(ctx, &list, client.InNamespace(h.Namespace()), client.MatchingLabels(labels.Set{"keyvalcluster": cluster})); err != nil {
		t.Fatalf("list pvcs: %v", err)
	}
	out := make(map[string]string, len(list.Items))
	for i := range list.Items {
		pvc := list.Items[i]
		qty := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		out[pvc.Name] = qty.String()
	}
	if len(out) == 0 {
		t.Fatalf("no pvcs found for cluster %s", cluster)
	}
	return out
}

func waitForPVCRequest(t *testing.T, h *harness.Harness, cluster string, want resource.Quantity, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.Context(), timeout)
	defer cancel()

	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var list corev1.PersistentVolumeClaimList
		if err := h.Client().List(ctx, &list, client.InNamespace(h.Namespace()), client.MatchingLabels(labels.Set{"keyvalcluster": cluster})); err != nil {
			return false, err
		}
		if len(list.Items) == 0 {
			return false, nil
		}
		for i := range list.Items {
			size := list.Items[i].Spec.Resources.Requests[corev1.ResourceStorage]
			if size.IsZero() {
				return false, nil
			}
			if size.Cmp(want) < 0 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait for pvc resize: %v", err)
	}
}

func supportsExpansion(t *testing.T, h *harness.Harness, cluster string) bool {
	ctx, cancel := context.WithTimeout(h.Context(), 30*time.Second)
	defer cancel()

	var list corev1.PersistentVolumeClaimList
	if err := h.Client().List(ctx, &list, client.InNamespace(h.Namespace()), client.MatchingLabels(labels.Set{"keyvalcluster": cluster})); err != nil {
		t.Fatalf("list pvcs: %v", err)
	}
	if len(list.Items) == 0 {
		t.Fatalf("no pvcs found for cluster %s", cluster)
	}
	scName := list.Items[0].Spec.StorageClassName
	if scName == nil || *scName == "" {
		return false
	}
	sc, err := h.Kube().StorageV1().StorageClasses().Get(ctx, *scName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get storageclass %s: %v", *scName, err)
	}
	if sc.AllowVolumeExpansion == nil {
		return false
	}
	return *sc.AllowVolumeExpansion
}
