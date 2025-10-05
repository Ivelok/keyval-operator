//go:build e2e || chaos

package cluster

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/harness"
)

// Scaler drives replica count changes on KeyValCluster objects.
type Scaler struct {
	h *harness.Harness
}

// NewScaler returns a new scaler bound to the given harness.
func NewScaler(h *harness.Harness) *Scaler {
	return &Scaler{h: h}
}

// Scale patches spec.redisReplicas to the requested value and waits for
// readyReplicas to match. The returned cluster snapshot reflects the latest
// status after scaling.
func (s *Scaler) Scale(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, replicas int32, timeout time.Duration) (*keyvalv1alpha1.KeyValCluster, error) {
	if cr == nil {
		return nil, fmt.Errorf("scale requires non-nil cluster reference")
	}
	if replicas <= 0 {
		replicas = 1
	}
	if timeout <= 0 {
		timeout = 3 * time.Minute
	}

	patched := cr.DeepCopy()
	patched.Spec.RedisReplicas = replicas
	if err := s.h.Client().Patch(ctx, patched, client.MergeFrom(cr)); err != nil {
		return nil, fmt.Errorf("patch redisReplicas: %w", err)
	}

	ready := s.h.WaitForReadyReplicas(ctx, patched.Name, replicas, timeout)
	return ready, nil
}

// Refresh fetches the latest cluster state into the provided reference.
func (s *Scaler) Refresh(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster) error {
	if cr == nil {
		return nil
	}
	return s.h.Client().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, cr)
}
