package main

import (
	"context"
	"flag"
	"os"
	"strconv"
	"time"

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
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var zapDev bool

	defaultClientCfg := controllers.ClientFactoryConfig{
		DialTimeout:              3 * time.Second,
		ReadTimeout:              2 * time.Second,
		WriteTimeout:             2 * time.Second,
		PoolTimeout:              2 * time.Second,
		OperationTimeout:         2 * time.Second,
		SentinelOperationTimeout: 5 * time.Second,
		MaxRetries:               2,
		RetryInitialBackoff:      200 * time.Millisecond,
		RetryMaxBackoff:          time.Second,
		RetryBackoffFactor:       2.0,
		RetryJitter:              0.1,
		MinIdleConns:             1,
	}
	applyClientConfigEnvOverrides(&defaultClientCfg)

	redisDialTimeout := defaultClientCfg.DialTimeout
	redisReadTimeout := defaultClientCfg.ReadTimeout
	redisWriteTimeout := defaultClientCfg.WriteTimeout
	redisPoolTimeout := defaultClientCfg.PoolTimeout
	redisOpTimeout := defaultClientCfg.OperationTimeout
	sentinelOpTimeout := defaultClientCfg.SentinelOperationTimeout
	redisRetryInitialBackoff := defaultClientCfg.RetryInitialBackoff
	redisRetryMaxBackoff := defaultClientCfg.RetryMaxBackoff
	redisRetryBackoffFactor := defaultClientCfg.RetryBackoffFactor
	redisRetryJitter := defaultClientCfg.RetryJitter
	redisMaxRetries := defaultClientCfg.MaxRetries
	redisMinIdleConns := defaultClientCfg.MinIdleConns
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true, "Enable leader election for controller manager.")
	flag.BoolVar(&zapDev, "zap-devel", true, "Enable development logging (human-friendly)")
	flag.DurationVar(&redisDialTimeout, "redis-dial-timeout", redisDialTimeout, "Dial timeout for Redis/Sentinel clients.")
	flag.DurationVar(&redisReadTimeout, "redis-read-timeout", redisReadTimeout, "Socket read timeout for Redis/Sentinel clients.")
	flag.DurationVar(&redisWriteTimeout, "redis-write-timeout", redisWriteTimeout, "Socket write timeout for Redis/Sentinel clients.")
	flag.DurationVar(&redisPoolTimeout, "redis-pool-timeout", redisPoolTimeout, "Connection pool wait timeout for Redis/Sentinel clients.")
	flag.DurationVar(&redisOpTimeout, "redis-operation-timeout", redisOpTimeout, "Per-command timeout for Redis operations.")
	flag.DurationVar(&sentinelOpTimeout, "sentinel-operation-timeout", sentinelOpTimeout, "Per-command timeout for Sentinel operations.")
	flag.DurationVar(&redisRetryInitialBackoff, "redis-retry-backoff-initial", redisRetryInitialBackoff, "Initial backoff between Redis client retries.")
	flag.DurationVar(&redisRetryMaxBackoff, "redis-retry-backoff-max", redisRetryMaxBackoff, "Maximum backoff between Redis client retries.")
	flag.Float64Var(&redisRetryBackoffFactor, "redis-retry-backoff-factor", redisRetryBackoffFactor, "Multiplicative factor for Redis client retry backoff.")
	flag.Float64Var(&redisRetryJitter, "redis-retry-jitter", redisRetryJitter, "Jitter fraction (0-1) applied to Redis client retry backoff.")
	flag.IntVar(&redisMaxRetries, "redis-max-retries", redisMaxRetries, "Maximum number of retries per Redis/Sentinel command.")
	flag.IntVar(&redisMinIdleConns, "redis-min-idle-conns", redisMinIdleConns, "Minimum number of idle connections to maintain in the Redis/Sentinel pool.")
	flag.Parse()
	ctx := context.Background()

	clientCfg := controllers.ClientFactoryConfig{
		DialTimeout:              redisDialTimeout,
		ReadTimeout:              redisReadTimeout,
		WriteTimeout:             redisWriteTimeout,
		PoolTimeout:              redisPoolTimeout,
		OperationTimeout:         redisOpTimeout,
		SentinelOperationTimeout: sentinelOpTimeout,
		MaxRetries:               redisMaxRetries,
		RetryInitialBackoff:      redisRetryInitialBackoff,
		RetryMaxBackoff:          redisRetryMaxBackoff,
		RetryBackoffFactor:       redisRetryBackoffFactor,
		RetryJitter:              redisRetryJitter,
		MinIdleConns:             redisMinIdleConns,
	}
	// Configure controller-runtime logger (zap)
	ctrl.SetLogger(zap.New(zap.UseDevMode(zapDev)))

	cfg := ctrl.GetConfigOrDie()
	selection, selErr := controllers.ResolveReconcileProfile(ctx, cfg, scheme)
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
	cfg.QPS = selection.Profile.ClientQPS
	cfg.Burst = selection.Profile.ClientBurst
	masterCache := controllers.NewMasterAddressCache(selection.Profile.CacheTTL, cacheCapacity)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
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
	redisFactory := controllers.NewGoRedisFactoryWithConfig(clientCfg)
	sentinelFactory := controllers.NewGoRedisSentinelFactoryWithConfig(mgr.GetClient(), clientCfg)

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

func applyClientConfigEnvOverrides(cfg *controllers.ClientFactoryConfig) {
	if cfg == nil {
		return
	}
	overrideDurationFromEnv("KEYVAL_REDIS_DIAL_TIMEOUT", &cfg.DialTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_READ_TIMEOUT", &cfg.ReadTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_WRITE_TIMEOUT", &cfg.WriteTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_POOL_TIMEOUT", &cfg.PoolTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_OPERATION_TIMEOUT", &cfg.OperationTimeout)
	overrideDurationFromEnv("KEYVAL_SENTINEL_OPERATION_TIMEOUT", &cfg.SentinelOperationTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_RETRY_INITIAL_BACKOFF", &cfg.RetryInitialBackoff)
	overrideDurationFromEnv("KEYVAL_REDIS_RETRY_MAX_BACKOFF", &cfg.RetryMaxBackoff)
	overrideFloatFromEnv("KEYVAL_REDIS_RETRY_BACKOFF_FACTOR", &cfg.RetryBackoffFactor)
	overrideFloatFromEnv("KEYVAL_REDIS_RETRY_JITTER", &cfg.RetryJitter)
	overrideIntFromEnv("KEYVAL_REDIS_MAX_RETRIES", &cfg.MaxRetries)
	overrideIntFromEnv("KEYVAL_REDIS_MIN_IDLE_CONNS", &cfg.MinIdleConns)
}

func overrideDurationFromEnv(key string, target *time.Duration) {
	if target == nil {
		return
	}
	if val, ok := os.LookupEnv(key); ok {
		parsed, err := time.ParseDuration(val)
		if err != nil {
			setupLog.Info("invalid duration override", "key", key, "value", val, "error", err)
			return
		}
		*target = parsed
	}
}

func overrideIntFromEnv(key string, target *int) {
	if target == nil {
		return
	}
	if val, ok := os.LookupEnv(key); ok {
		parsed, err := strconv.Atoi(val)
		if err != nil {
			setupLog.Info("invalid integer override", "key", key, "value", val, "error", err)
			return
		}
		*target = parsed
	}
}

func overrideFloatFromEnv(key string, target *float64) {
	if target == nil {
		return
	}
	if val, ok := os.LookupEnv(key); ok {
		parsed, err := strconv.ParseFloat(val, 64)
		if err != nil {
			setupLog.Info("invalid float override", "key", key, "value", val, "error", err)
			return
		}
		*target = parsed
	}
}
