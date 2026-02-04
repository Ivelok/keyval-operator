//go:build envtest
// +build envtest

package controllers

import (
	"context"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

// startEnvTest starts an API server if KUBEBUILDER_ASSETS is set, otherwise skips.
func startEnvTest(t *testing.T) (*envtest.Environment, *runtime.Scheme, *restConfig) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; skipping envtest")
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../config/crd/bases"}}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("failed to start envtest: %v", err)
	}
	return env, scheme, &restConfig{cfg}
}

type restConfig struct{ *rest.Config }

func TestReconcileStandalone_ResourcesCreated(t *testing.T) {
	env, scheme, cfg := startEnvTest(t)
	defer func() { _ = env.Stop() }()

	mgr, err := ctrl.NewManager(cfg.Config, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	r := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	})
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("setup: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mgr.Start(ctx) }()

	// Ensure default namespace exists
	_ = mgr.GetClient().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})

	// Create CR
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1}}
	if err := mgr.GetClient().Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	// Wait for resources
	mustEventually(t, 10*time.Second, func() bool {
		var cm corev1.ConfigMap
		return mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-config"}, &cm) == nil
	})
	mustEventually(t, 10*time.Second, func() bool {
		var ss appsv1.StatefulSet
		return mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo"}, &ss) == nil
	})
	mustEventually(t, 10*time.Second, func() bool {
		var svc corev1.Service
		return mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-headless"}, &svc) == nil
	})
	mustEventually(t, 10*time.Second, func() bool {
		var svc corev1.Service
		return mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-master"}, &svc) == nil
	})
}

func TestReconcileSentinel_SentinelServiceCreated(t *testing.T) {
	env, scheme, cfg := startEnvTest(t)
	defer func() { _ = env.Stop() }()
	mgr, err := ctrl.NewManager(cfg.Config, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	r := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	})
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("setup: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mgr.Start(ctx) }()
	_ = mgr.GetClient().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, Image: "valkey/valkey:7.2", RedisReplicas: 3, SentinelCount: &v}}
	if err := mgr.GetClient().Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	mustEventually(t, 15*time.Second, func() bool {
		var svc corev1.Service
		return mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-sentinel"}, &svc) == nil
	})
}

func TestReconcileStandalone_Idempotent(t *testing.T) {
	env, scheme, cfg := startEnvTest(t)
	defer func() { _ = env.Stop() }()

	cl, err := ctrlclient.New(cfg.Config, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	r := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    cl,
		APIReader: cl,
		Scheme:    scheme,
	})

	ctx := context.Background()
	if err := cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace: %v", err)
	}

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1},
	}
	if err := cl.Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	var cm corev1.ConfigMap
	cmKey := types.NamespacedName{Namespace: "default", Name: "demo-config"}
	mustEventually(t, 5*time.Second, func() bool {
		return cl.Get(ctx, cmKey, &cm) == nil
	})
	originalCMVersion := cm.ResourceVersion

	var masterSvc corev1.Service
	masterKey := types.NamespacedName{Namespace: "default", Name: "demo-master"}
	mustEventually(t, 5*time.Second, func() bool {
		return cl.Get(ctx, masterKey, &masterSvc) == nil
	})
	originalMasterVersion := masterSvc.ResourceVersion

	if res, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	} else if res.RequeueAfter != stableRequeueInterval {
		t.Fatalf("expected periodic requeue of %s on idempotent reconcile, got %s", stableRequeueInterval, res.RequeueAfter)
	}

	var cmAfter corev1.ConfigMap
	if err := cl.Get(ctx, cmKey, &cmAfter); err != nil {
		t.Fatalf("get configmap after reconcile: %v", err)
	}
	if cmAfter.ResourceVersion != originalCMVersion {
		t.Fatalf("configmap resourceVersion changed: before=%s after=%s", originalCMVersion, cmAfter.ResourceVersion)
	}

	var masterAfter corev1.Service
	if err := cl.Get(ctx, masterKey, &masterAfter); err != nil {
		t.Fatalf("get master service after reconcile: %v", err)
	}
	if masterAfter.ResourceVersion != originalMasterVersion {
		t.Fatalf("master service resourceVersion changed: before=%s after=%s", originalMasterVersion, masterAfter.ResourceVersion)
	}
}

func mustEventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
