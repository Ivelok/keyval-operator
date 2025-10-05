package manager

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

// SetupConfig contains the knobs required to register the controller with the manager.
type SetupConfig struct {
	Manager                 manager.Manager
	Reconciler              reconcile.Reconciler
	ControllerName          string
	RateLimiter             workqueue.TypedRateLimiter[reconcile.Request]
	MaxConcurrentReconciles int
	SetQueueDepth           func(provider func() int)
	SecretMap               handler.MapFunc
	PodMap                  handler.MapFunc
	PodPredicate            predicate.Predicate
}

// SetupWithManager wires the controller into the provided manager according to the configuration.
func SetupWithManager(cfg SetupConfig) error {
	if cfg.Manager == nil {
		return fmt.Errorf("manager is nil")
	}
	if cfg.Reconciler == nil {
		return fmt.Errorf("reconciler is nil")
	}

	controllerName := cfg.ControllerName
	if controllerName == "" {
		controllerName = "keyvalcluster"
	}

	options := controller.Options{RateLimiter: cfg.RateLimiter}
	if cfg.MaxConcurrentReconciles > 0 {
		options.MaxConcurrentReconciles = cfg.MaxConcurrentReconciles
	}
	options.NewQueue = func(name string, rl workqueue.TypedRateLimiter[reconcile.Request]) workqueue.TypedRateLimitingInterface[reconcile.Request] {
		queueName := name
		if queueName == "" {
			queueName = controllerName
		}
		q := workqueue.NewTypedRateLimitingQueueWithConfig(rl, workqueue.TypedRateLimitingQueueConfig[reconcile.Request]{Name: queueName})
		if cfg.SetQueueDepth != nil {
			cfg.SetQueueDepth(func() int { return q.Len() })
		}
		return q
	}

	b := ctrl.NewControllerManagedBy(cfg.Manager).
		WithOptions(options).
		For(&keyvalv1alpha1.KeyValCluster{}, builder.WithPredicates(GenerationChangedPredicate())).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{})

	if cfg.SecretMap != nil {
		b = b.Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(cfg.SecretMap))
	}

	if cfg.PodMap != nil {
		if cfg.PodPredicate != nil {
			b = b.Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(cfg.PodMap), builder.WithPredicates(cfg.PodPredicate))
		} else {
			b = b.Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(cfg.PodMap))
		}
	}

	return b.Complete(cfg.Reconciler)
}
