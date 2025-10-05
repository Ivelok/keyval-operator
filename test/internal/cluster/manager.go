//go:build e2e || chaos

package cluster

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/harness"
)

// Manager orchestrates lifecycle operations for KeyValCluster objects.
type Manager struct {
	h *harness.Harness
}

// NewManager binds a Manager to the provided harness.
func NewManager(h *harness.Harness) *Manager {
	return &Manager{h: h}
}

// Apply creates the cluster and registers it for cleanup.
func (m *Manager) Apply(ctx context.Context, cluster *keyvalv1alpha1.KeyValCluster) error {
	if err := m.h.Client().Create(ctx, cluster); err != nil {
		return err
	}
	m.h.TrackCluster(cluster)
	return nil
}

// Refresh fetches the latest cluster status into the provided reference.
func (m *Manager) Refresh(ctx context.Context, cluster *keyvalv1alpha1.KeyValCluster) error {
	if cluster == nil {
		return nil
	}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	return m.h.Client().Get(ctx, key, cluster)
}

// WaitReady waits for ConditionReconciled=True and readyReplicas==desired.
func (m *Manager) WaitReady(ctx context.Context, cluster *keyvalv1alpha1.KeyValCluster, timeout time.Duration) *keyvalv1alpha1.KeyValCluster {
	if cluster == nil {
		return nil
	}
	name := cluster.Name
	m.h.WaitForCondition(ctx, name, keyvalv1alpha1.ConditionReconciled, metav1.ConditionTrue, timeout)
	replicas := cluster.Spec.RedisReplicas
	return m.h.WaitForReadyReplicas(ctx, name, replicas, timeout)
}

// WaitForRoles polls until the status exposes the requested roles mapping.
func (m *Manager) WaitForRoles(ctx context.Context, cluster *keyvalv1alpha1.KeyValCluster, expected map[string]keyvalv1alpha1.PodRole, timeout time.Duration) error {
	if len(expected) == 0 || cluster == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	return wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var latest keyvalv1alpha1.KeyValCluster
		if err := m.h.Client().Get(ctx, key, &latest); err != nil {
			return false, err
		}
		if !rolesMatch(latest.Status.Roles, expected) {
			return false, nil
		}
		*cluster = latest
		return true, nil
	})
}

func rolesMatch(roles []keyvalv1alpha1.PodRoleStatus, want map[string]keyvalv1alpha1.PodRole) bool {
	if len(roles) < len(want) {
		return false
	}
	for name, role := range want {
		found := false
		for i := range roles {
			if roles[i].Name == name {
				if roles[i].Role != role {
					return false
				}
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
