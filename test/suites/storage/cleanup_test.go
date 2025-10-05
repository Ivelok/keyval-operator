//go:build e2e

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/harness"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

func TestStorageCleanupPolicy(t *testing.T) {
	cases := []struct {
		name           string
		cleanupEnabled bool
	}{
		{name: "retain-pvcs", cleanupEnabled: false},
		{name: "delete-pvcs", cleanupEnabled: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := suite.New(t)
			manager := cluster.NewManager(s.Harness)

			clusterName := fmt.Sprintf("storage-cleanup-%d", time.Now().UnixNano())
			builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
				WithName(clusterName).
				WithLabels(map[string]string{"suite": "storage", "scenario": tc.name})

			cr := builder.Build()
			cr.Spec.Storage = &keyvalv1alpha1.StorageSpec{CleanupOnDelete: tc.cleanupEnabled}

			var pvcUIDs []types.NamespacedName

			s.Step("create-cluster", func(ctx context.Context) {
				if err := manager.Apply(ctx, cr); err != nil {
					t.Fatalf("apply cluster: %v", err)
				}
			})

			s.Step("wait-ready", func(ctx context.Context) {
				updated := manager.WaitReady(ctx, cr, 4*time.Minute)
				*cr = *updated
			})

			s.Step("assert-policy-condition", func(ctx context.Context) {
				if err := manager.Refresh(ctx, cr); err != nil {
					t.Fatalf("refresh cluster: %v", err)
				}
				expectedReason := "RetentionPolicy"
				if tc.cleanupEnabled {
					expectedReason = "CleanupEnabled"
				}
				cond := findCondition(cr.Status.Conditions, keyvalv1alpha1.ConditionStorageCleanup)
				if cond == nil {
					t.Fatalf("storage cleanup condition not found")
				}
				if cond.Status != metav1.ConditionTrue {
					t.Fatalf("expected StorageCleanup status True, got %s", cond.Status)
				}
				if cond.Reason != expectedReason {
					t.Fatalf("expected reason %s, got %s", expectedReason, cond.Reason)
				}
			})

			s.Step("list-pvcs", func(ctx context.Context) {
				var list corev1.PersistentVolumeClaimList
				selector := labels.Set{"keyvalcluster": cr.Name}
				if err := s.Harness.Client().List(ctx, &list, client.InNamespace(s.Harness.Namespace()), client.MatchingLabels(selector)); err != nil {
					t.Fatalf("list pvcs: %v", err)
				}
				if len(list.Items) == 0 {
					t.Fatalf("expected pvcs for cluster %s", cr.Name)
				}
				pvcUIDs = pvcUIDs[:0]
				for i := range list.Items {
					pvc := list.Items[i]
					pvcUIDs = append(pvcUIDs, types.NamespacedName{Namespace: pvc.Namespace, Name: pvc.Name})
				}
			})

			s.Step("delete-cluster", func(ctx context.Context) {
				if err := s.Harness.Client().Delete(ctx, cr); err != nil {
					t.Fatalf("delete cluster: %v", err)
				}
			})

			s.Step("wait-removed", func(ctx context.Context) {
				key := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
				pollCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
				defer cancel()
				if err := wait.PollUntilContextCancel(pollCtx, time.Second, true, func(ctx context.Context) (bool, error) {
					var latest keyvalv1alpha1.KeyValCluster
					err := s.Harness.Client().Get(ctx, key, &latest)
					if err == nil {
						return false, nil
					}
					if client.IgnoreNotFound(err) == nil {
						return true, nil
					}
					return false, err
				}); err != nil {
					t.Fatalf("wait for cluster deletion: %v", err)
				}
			})

			if tc.cleanupEnabled {
				s.Step("verify-pvcs-deleted", func(ctx context.Context) {
					for _, key := range pvcUIDs {
						if err := waitPVCGone(ctx, s.Harness, key); err != nil {
							t.Fatalf("wait for pvc %s deletion: %v", key.Name, err)
						}
					}
				})
			} else {
				s.Step("verify-pvcs-retained", func(ctx context.Context) {
					for _, key := range pvcUIDs {
						var pvc corev1.PersistentVolumeClaim
						if err := s.Harness.Client().Get(ctx, key, &pvc); err != nil {
							t.Fatalf("expected pvc %s to remain: %v", key.Name, err)
						}
					}
				})
				s.Step("cleanup-pvcs", func(ctx context.Context) {
					for _, key := range pvcUIDs {
						var pvc corev1.PersistentVolumeClaim
						if err := s.Harness.Client().Get(ctx, key, &pvc); err != nil {
							if client.IgnoreNotFound(err) == nil {
								continue
							}
							t.Fatalf("get pvc %s: %v", key.Name, err)
						}
						if err := s.Harness.Client().Delete(ctx, &pvc); err != nil {
							t.Fatalf("delete pvc %s: %v", key.Name, err)
						}
						if err := waitPVCGone(ctx, s.Harness, key); err != nil {
							t.Fatalf("wait pvc %s removal: %v", key.Name, err)
						}
					}
				})
			}
		})
	}
}

func waitPVCGone(ctx context.Context, h *harness.Harness, key types.NamespacedName) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		var pvc corev1.PersistentVolumeClaim
		err := h.Client().Get(ctx, key, &pvc)
		if client.IgnoreNotFound(err) == nil {
			return true, nil
		}
		return false, err
	})
}

func findCondition(conds []metav1.Condition, typ keyvalv1alpha1.ConditionType) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == string(typ) {
			return &conds[i]
		}
	}
	return nil
}
