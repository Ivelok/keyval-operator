package controllers

import (
	"context"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/finalizer"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

// finalizeCluster performs best-effort cleanup before CR deletion by delegating to the
// internal finalizer pipeline.
func (r *KeyValClusterReconciler) finalizeCluster(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster) error {
	logger := logging.FromContext(ctx)
	if logger.IsZero() {
		logger = r.baseLogger
	}
	deps := finalizer.Dependencies{
		Client:            r.Client,
		APIReader:         r.APIReader,
		Recorder:          r.Recorder,
		Logger:            logger,
		ClientFactory:     r.ClientFactory,
		CleanupTimeout:    0,
		OperationTimeout:  0,
		CleanupBatchLimit: 0,
	}
	pipe := finalizer.NewPipeline(deps, finalizer.HandlerFunc(finalizer.Execute))
	return pipe.Run(ctx, cr)
}
