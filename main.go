package main

import (
	"context"
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers"
	controllerslogging "github.com/ivelok/keyval-operator/controllers/logging"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(keyvalv1alpha1.AddToScheme(scheme))
}

func main() {
	cfg := DefaultAppConfig()
	cfg.ApplyEnv()
	cfg.BindFlags(flag.CommandLine)
	flag.Parse()
	ctx := context.Background()
	// Configure controller-runtime logger (zap)
	ctrl.SetLogger(zap.New(zap.UseDevMode(cfg.ZapDev)))

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
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// Setup controller with Redis/Sentinel client factories
	baseLogger := controllerslogging.FromLogr(ctrl.Log.WithName("keyval-controller"))
	redisFactory := controllers.NewGoRedisFactoryWithConfig(cfg.Client)
	sentinelFactory := controllers.NewGoRedisSentinelFactoryWithConfig(mgr.GetClient(), cfg.Client)

	reconciler := controllers.NewKeyValClusterReconciler(controllers.ReconcilerDependencies{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		Scheme:          mgr.GetScheme(),
		ClientFactory:   redisFactory,
		SentinelFactory: sentinelFactory,
		Logger:          baseLogger,
		Profile:         selection.Profile,
		MasterCache:     masterCache,
		ControllerName:  "keyvalcluster",
	})
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "KeyValCluster")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
