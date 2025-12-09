package phases

import (
	"context"
	"fmt"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
)

// Services reconciles all Services associated with the cluster (headless, master, replicas, sentinel).
func Services(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	deps := ServiceDependencies{
		Client:   state.Dependencies.Client,
		Recorder: state.Dependencies.Recorder,
		Scheme:   state.Dependencies.Scheme,
	}
	if deps.Client == nil {
		return controllererrors.WrapTransient(fmt.Errorf("service phase missing client"))
	}
	if deps.Scheme == nil {
		return controllererrors.WrapTransient(fmt.Errorf("service phase missing scheme"))
	}

	cr := state.Cluster
	logger := state.Logger

	serviceCreate := true
	if cr.Spec.Service != nil && cr.Spec.Service.Create != nil {
		serviceCreate = *cr.Spec.Service.Create
	}

	headless := resources.HeadlessService(cr)
	if err := ReconcileServiceLifecycle(ctx, deps, cr, headless, ServiceLifecycleOptions{
		Enabled:           serviceCreate,
		PreserveClusterIP: false,
		ServiceType:       "headless",
	}, logger); err != nil {
		return err
	}

	masterSvc := resources.MasterService(cr)
	if err := ReconcileServiceLifecycle(ctx, deps, cr, masterSvc, ServiceLifecycleOptions{
		Enabled:           serviceCreate,
		PreserveClusterIP: true,
		ServiceType:       "master",
	}, logger); err != nil {
		return err
	}

	sentinelMode := cr.Spec.Mode == keyvalv1alpha1.ModeSentinel
	sentinelEnabled := false
	if sentinelMode {
		sentinelEnabled = true
		if cr.Spec.SentinelService != nil && cr.Spec.SentinelService.Create != nil {
			sentinelEnabled = *cr.Spec.SentinelService.Create
		}
	}
	sentinelSvc := resources.SentinelService(cr)
	if err := ReconcileServiceLifecycle(ctx, deps, cr, sentinelSvc, ServiceLifecycleOptions{
		Enabled:           sentinelEnabled,
		PreserveClusterIP: true,
		ServiceType:       "sentinel",
	}, logger); err != nil {
		return err
	}

	sentinelHeadlessEnabled := sentinelMode
	sentinelHeadless := resources.SentinelHeadlessService(cr)
	if err := ReconcileServiceLifecycle(ctx, deps, cr, sentinelHeadless, ServiceLifecycleOptions{
		Enabled:           sentinelHeadlessEnabled,
		PreserveClusterIP: false,
		ServiceType:       "sentinel-headless",
	}, logger); err != nil {
		return err
	}

	createReplicas := true
	if cr.Spec.ReplicasService != nil && cr.Spec.ReplicasService.Create != nil {
		createReplicas = *cr.Spec.ReplicasService.Create
	}
	replicasSvc := resources.ReplicasService(cr)
	if err := ReconcileServiceLifecycle(ctx, deps, cr, replicasSvc, ServiceLifecycleOptions{
		Enabled:           createReplicas,
		PreserveClusterIP: true,
		ServiceType:       "replicas",
	}, logger); err != nil {
		return err
	}

	return nil
}
