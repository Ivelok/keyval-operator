package main

import (
	"context"

	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/ivelok/keyval-operator/controllers"
)

// Run owns controller-runtime setup and manager startup.
func Run(ctx context.Context, cfg AppConfig) error {
	ctrl.SetLogger(zap.New(
		zap.UseDevMode(cfg.ZapDev),
		zap.StacktraceLevel(zapcore.PanicLevel),
	))

	restCfg := ctrl.GetConfigOrDie()
	selection, selErr := controllers.ResolveReconcileProfile(ctx, restCfg, scheme)
	if selErr != nil {
		setupLog.Error(selErr, "resolve reconcile profile; using defaults")
	}
	if selection.Override {
		setupLog.Info("reconcile profile override in effect; using debug-only configuration", "profile", selection.Name)
	}
	setupLog.Info("reconcile profile resolved",
		"profile", selection.Name,
		"source", selection.Source,
		"override", selection.Override,
		"clusters", selection.Snapshot.ClusterCount,
		"sentinelClusters", selection.Snapshot.SentinelClusters,
		"redisReplicas", selection.Snapshot.RedisReplicas,
		"sentinelMembers", selection.Snapshot.SentinelMembers,
		"maxConcurrentReconciles", selection.Profile.MaxConcurrentReconciles,
		"clientQPS", selection.Profile.ClientQPS,
		"clientBurst", selection.Profile.ClientBurst,
		"cacheTTL", selection.Profile.CacheTTL,
	)
	cacheCapacity := selection.CacheCapacityHint()
	setupLog.Info("master-address cache configured", "ttl", selection.Profile.CacheTTL, "capacity", cacheCapacity)
	restCfg.QPS = selection.Profile.ClientQPS
	restCfg.Burst = selection.Profile.ClientBurst
	masterCache := controllers.NewMasterAddressCache(selection.Profile.CacheTTL, cacheCapacity)

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: cfg.MetricsAddr},
		HealthProbeBindAddress: cfg.ProbeAddr,
		LeaderElection:         cfg.LeaderElection,
		LeaderElectionID:       "keyval-operator.ivelok.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		return err
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		return err
	}

	if err := setupControllers(mgr, selection, cfg.Client, masterCache); err != nil {
		return err
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		return err
	}
	return nil
}
