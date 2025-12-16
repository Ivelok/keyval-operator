//go:build envtest
// +build envtest

package controllers

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

func TestReconcile_StandaloneToSentinel_Migration(t *testing.T) {
	env, scheme, cfg := startEnvTest(t)
	defer func() { _ = env.Stop() }()

	mgr, err := ctrl.NewManager(cfg.Config, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
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

	// 1. Create Standalone CR
	crName := "migration-demo"
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 1,
		},
	}
	if err := mgr.GetClient().Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	// 2. Wait for Standalone resources
	mustEventually(t, 10*time.Second, func() bool {
		var ss appsv1.StatefulSet
		return mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName}, &ss) == nil
	})
	// Check replica count is 1
	var ssStandalone appsv1.StatefulSet
	if err := mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName}, &ssStandalone); err != nil {
		t.Fatalf("get standalone ss: %v", err)
	}
	if *ssStandalone.Spec.Replicas != 1 {
		t.Fatalf("expected 1 replica in standalone, got %d", *ssStandalone.Spec.Replicas)
	}

	// 3. Update to Sentinel
	// Fetch latest CR to avoid conflict
	var crLatest keyvalv1alpha1.KeyValCluster
	if err := mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName}, &crLatest); err != nil {
		t.Fatalf("get CR: %v", err)
	}

	sentinelCount := int32(3)
	crLatest.Spec.Mode = keyvalv1alpha1.ModeSentinel
	crLatest.Spec.RedisReplicas = 3
	crLatest.Spec.SentinelCount = &sentinelCount

	if err := mgr.GetClient().Update(ctx, &crLatest); err != nil {
		t.Fatalf("update CR to Sentinel: %v", err)
	}

	// 4. Verify Sentinel Resources
	// a. Redis StatefulSet scaled to 3
	mustEventually(t, 15*time.Second, func() bool {
		var ss appsv1.StatefulSet
		if err := mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName}, &ss); err != nil {
			return false
		}
		return ss.Spec.Replicas != nil && *ss.Spec.Replicas == 3
	})

	// b. Sentinel StatefulSet created
	mustEventually(t, 15*time.Second, func() bool {
		var ssSentinel appsv1.StatefulSet
		return mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName + "-sentinel"}, &ssSentinel) == nil
	})

	// c. Sentinel Service created
	mustEventually(t, 15*time.Second, func() bool {
		var svcSentinel corev1.Service
		return mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName + "-sentinel"}, &svcSentinel) == nil
	})
}

func TestReconcile_SentinelToStandalone_Migration(t *testing.T) {
	env, scheme, cfg := startEnvTest(t)
	defer func() { _ = env.Stop() }()

	mgr, err := ctrl.NewManager(cfg.Config, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
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

	// 1. Create Sentinel CR
	crName := "downgrade-demo"
	sentinelCount := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: &sentinelCount,
		},
	}
	if err := mgr.GetClient().Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	// 2. Wait for Sentinel resources
	mustEventually(t, 15*time.Second, func() bool {
		var ssSentinel appsv1.StatefulSet
		return mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName + "-sentinel"}, &ssSentinel) == nil
	})

	// 3. Downgrade to Standalone
	var crLatest keyvalv1alpha1.KeyValCluster
	if err := mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName}, &crLatest); err != nil {
		t.Fatalf("get CR: %v", err)
	}
	crLatest.Spec.Mode = keyvalv1alpha1.ModeStandalone
	crLatest.Spec.RedisReplicas = 1
	crLatest.Spec.SentinelCount = nil
	if err := mgr.GetClient().Update(ctx, &crLatest); err != nil {
		t.Fatalf("update CR to Standalone: %v", err)
	}

	// 4. Verify Sentinel Resources are Deleted
	// a. Sentinel StatefulSet deleted
	mustEventually(t, 15*time.Second, func() bool {
		var ssSentinel appsv1.StatefulSet
		err := mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName + "-sentinel"}, &ssSentinel)
		return apierrors.IsNotFound(err) || ssSentinel.DeletionTimestamp != nil
	})

	// b. Sentinel Service deleted
	mustEventually(t, 15*time.Second, func() bool {
		var svcSentinel corev1.Service
		err := mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName + "-sentinel"}, &svcSentinel)
		return apierrors.IsNotFound(err) || svcSentinel.DeletionTimestamp != nil
	})

	// c. Sentinel Headless Service deleted
	mustEventually(t, 15*time.Second, func() bool {
		var svcSentinelHeadless corev1.Service
		err := mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: "default", Name: crName + "-sentinel-headless"}, &svcSentinelHeadless)
		return apierrors.IsNotFound(err) || svcSentinelHeadless.DeletionTimestamp != nil
	})
}
