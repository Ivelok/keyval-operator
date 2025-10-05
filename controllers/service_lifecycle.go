package controllers

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile/phases"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

type serviceLifecycleOptions = phases.ServiceLifecycleOptions

func (r *KeyValClusterReconciler) reconcileServiceLifecycle(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, desired *corev1.Service, opts serviceLifecycleOptions, logger logging.Logger) error {
	deps := phases.ServiceDependencies{
		Client:   r.Client,
		Recorder: r.Recorder,
		Scheme:   r.Scheme,
	}
	return phases.ReconcileServiceLifecycle(ctx, deps, cr, desired, phases.ServiceLifecycleOptions(opts), logger)
}
