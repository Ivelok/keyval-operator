package phases

import (
	"context"
	"fmt"

	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/ssa"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Config ensures the redis/sentinel configuration ConfigMap exists and records the hash for later phases.
func Config(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	if state.Dependencies.Client == nil {
		return controllererrors.WrapTransient(fmt.Errorf("config phase missing client"))
	}
	if state.Dependencies.Scheme == nil {
		return controllererrors.WrapTransient(fmt.Errorf("config phase missing scheme"))
	}

	cfg := resources.ConfigMap(state.Cluster, &state.Security.Settings)
	applyCfg := ssa.ConfigMap(cfg)
	if err := controllerutil.SetOwnerReference(state.Cluster, applyCfg, state.Dependencies.Scheme); err != nil {
		return controllererrors.WrapTransient(fmt.Errorf("set owner on configmap: %w", err))
	}
	if err := state.Dependencies.Client.Patch(ctx, applyCfg, client.Apply, client.FieldOwner(core.FieldOwner)); err != nil {
		return controllererrors.WrapTransient(fmt.Errorf("apply configmap: %w", err))
	}

	state.Config = reconcile.ConfigState{
		ConfigHash: resources.ConfigHash(state.Cluster, &state.Security.Settings),
	}

	return nil
}
