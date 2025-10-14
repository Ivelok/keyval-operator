package phases

import (
	"context"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/importer"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
)

// ExternalImport orchestrates optional data import from an external Redis/Valkey source before normal reconciliation proceeds.
func ExternalImport(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}
	cr := state.Cluster
	overrides := state.EnsureConditionOverrides()

	spec := cr.Spec.Bootstrap
	externalSpec := (*keyvalv1alpha1.ExternalSourceSpec)(nil)
	if spec != nil {
		externalSpec = spec.ExternalSource
	}

	if state.AbortDirective != nil {
		return nil
	}

	res, err := importer.Ensure(ctx, importer.Options{
		Cluster:       cr,
		Spec:          externalSpec,
		CurrentStatus: cr.Status.ExternalImport,
		Pods:          state.RedisPods,
		SentinelPods:  state.SentinelPods,
		ClientFactory: state.Dependencies.ClientFactory,
		ClientOptions: state.Security.RedisClientOptions,
		KubeClient:    state.Dependencies.Client,
		Recorder:      state.Dependencies.Recorder,
		Logger:        state.Logger,
	})

	state.Import.Status = res.Status
	overrides[keyvalv1alpha1.ConditionExternalImport] = &res.Condition

	if res.RequeueAfter > 0 {
		state.RequeueAfter(res.RequeueAfter)
	}

	if res.Abort {
		state.AbortDirective = &reconcile.AbortDirective{Err: err}
	}

	return err
}
