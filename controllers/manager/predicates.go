package manager

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ivelok/keyval-operator/controllers/internal/runtime"
)

// GenerationChangedPredicate returns the default generation predicate used for KeyValCluster resources.
func GenerationChangedPredicate() predicate.Predicate {
	return predicate.GenerationChangedPredicate{}
}

// PodLifecyclePredicate proxies the internal runtime predicate to decouple controller setup from runtime internals.
func PodLifecyclePredicate() predicate.Predicate {
	return runtime.PodLifecyclePredicate()
}

// PodToClusterMapper maps pods managed by the operator back to the owning KeyValCluster.
func PodToClusterMapper() handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		return runtime.PodToClusterRequests(ctx, obj)
	}
}

// TLSSecretMapper returns a handler that enqueues clusters referencing TLS secrets.
func TLSSecretMapper(c client.Client) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		return runtime.TLSSecretToClusterRequests(ctx, obj, c)
	}
}
