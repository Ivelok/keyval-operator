package controllers

import (
	"time"

	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	opeviction "github.com/ivelok/keyval-operator/controllers/internal/ops/eviction"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

// ReconcilerDependencies bundles the objects required to construct a KeyValClusterReconciler.
type ReconcilerDependencies struct {
	Client           client.Client
	APIReader        client.Reader
	Scheme           *runtime.Scheme
	ClientFactory    ClientFactory
	SentinelFactory  SentinelFactory
	Recorder         record.EventRecorder
	RateLimiter      workqueue.TypedRateLimiter[reconcile.Request]
	Logger           logging.Logger
	EvictionSettings opeviction.Settings
	Profile          ReconcileProfile
	MasterCache      *opreplication.MasterAddressCache
	ControllerName   string
}

// NewKeyValClusterReconciler builds a reconciler with the provided dependencies.
func NewKeyValClusterReconciler(deps ReconcilerDependencies) *KeyValClusterReconciler {
	baseLogger := deps.Logger
	if baseLogger.IsZero() {
		baseLogger = logging.New(nil)
	}
	r := &KeyValClusterReconciler{
		Client:           deps.Client,
		APIReader:        deps.APIReader,
		Scheme:           deps.Scheme,
		ClientFactory:    deps.ClientFactory,
		SentinelFactory:  deps.SentinelFactory,
		Recorder:         deps.Recorder,
		shutdownObserved: map[string]struct{}{},
	}
	r.rateLimiter = deps.RateLimiter
	r.baseLogger = baseLogger
	r.evictionSettings = deps.EvictionSettings
	r.backoffs = newBackoffTracker()
	r.controllerName = deps.ControllerName
	if deps.Profile.MaxConcurrentReconciles > 0 {
		r.maxConcurrentReconciles = deps.Profile.MaxConcurrentReconciles
	}
	if deps.MasterCache != nil {
		r.masterCache = deps.MasterCache
	}
	return r
}

func defaultRateLimiter() workqueue.TypedRateLimiter[reconcile.Request] {
	return workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](500*time.Millisecond, 5*time.Minute),
		&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(5), 50)},
	)
}
