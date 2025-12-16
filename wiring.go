package main

import (
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/ivelok/keyval-operator/controllers"
	controllerslogging "github.com/ivelok/keyval-operator/controllers/logging"
)

func setupControllers(
	mgr manager.Manager,
	sel controllers.ReconcileProfileSelection,
	clientCfg controllers.ClientFactoryConfig,
	cache *controllers.MasterAddressCache,
) error {
	// Setup controller with Redis/Sentinel client factories
	baseLogger := controllerslogging.FromLogr(ctrl.Log.WithName("keyval-controller"))
	redisFactory := controllers.NewGoRedisFactoryWithConfig(clientCfg)
	sentinelFactory := controllers.NewGoRedisSentinelFactoryWithConfig(mgr.GetClient(), clientCfg)

	reconciler := controllers.NewKeyValClusterReconciler(controllers.ReconcilerDependencies{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		Scheme:          mgr.GetScheme(),
		ClientFactory:   redisFactory,
		SentinelFactory: sentinelFactory,
		Logger:          baseLogger,
		Profile:         sel.Profile,
		MasterCache:     cache,
		ControllerName:  "keyvalcluster",
	})
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "KeyValCluster")
		return err
	}
	return nil
}
