//go:build envtest
// +build envtest

package controllers

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestFinalizer_AddAndRemove(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("no envtest assets")
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../config/crd/bases"}}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("env start: %v", err)
	}
	defer func() { _ = env.Stop() }()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatalf("manager: %v", err)
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
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1}}
	if err := mgr.GetClient().Create(ctx, cr); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Wait for finalizer added
	mustEventually(t, 10*time.Second, func() bool {
		var got keyvalv1alpha1.KeyValCluster
		if err := mgr.GetClient().Get(ctx, types.NamespacedName{Name: "demo", Namespace: "default"}, &got); err != nil {
			return false
		}
		for _, f := range got.Finalizers {
			if f == core.FinalizerName {
				return true
			}
		}
		return false
	})

	// Delete CR and ensure it's removed eventually
	if err := mgr.GetClient().Delete(ctx, cr); err != nil {
		t.Fatalf("delete: %v", err)
	}
	mustEventually(t, 15*time.Second, func() bool {
		var got keyvalv1alpha1.KeyValCluster
		err := mgr.GetClient().Get(ctx, types.NamespacedName{Name: "demo", Namespace: "default"}, &got)
		return err != nil
	})
}

func TestFinalizer_CleanupRemovesPVCs(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("no envtest assets")
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../config/crd/bases"}}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("env start: %v", err)
	}
	defer func() { _ = env.Stop() }()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatalf("manager: %v", err)
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
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cleanup", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 1,
			Storage: &keyvalv1alpha1.StorageSpec{
				CleanupOnDelete: true,
			},
		},
	}
	if err := mgr.GetClient().Create(ctx, cr); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	mustEventually(t, 10*time.Second, func() bool {
		var got keyvalv1alpha1.KeyValCluster
		if err := mgr.GetClient().Get(ctx, types.NamespacedName{Name: "cleanup", Namespace: "default"}, &got); err != nil {
			return false
		}
		for _, f := range got.Finalizers {
			if f == core.FinalizerName {
				return true
			}
		}
		return false
	})

	for i := 0; i < 2; i++ {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("data-cleanup-%d", i),
				Namespace: "default",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "cleanup",
				}},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		if err := mgr.GetClient().Create(ctx, pvc); err != nil {
			t.Fatalf("create pvc: %v", err)
		}
	}

	if err := mgr.GetClient().Delete(ctx, cr); err != nil {
		t.Fatalf("delete cluster: %v", err)
	}
	mustEventually(t, 20*time.Second, func() bool {
		var got keyvalv1alpha1.KeyValCluster
		return mgr.GetClient().Get(ctx, types.NamespacedName{Name: "cleanup", Namespace: "default"}, &got) != nil
	})

	mustEventually(t, 20*time.Second, func() bool {
		for i := 0; i < 2; i++ {
			var pvc corev1.PersistentVolumeClaim
			err := mgr.GetClient().Get(ctx, types.NamespacedName{Name: fmt.Sprintf("data-cleanup-%d", i), Namespace: "default"}, &pvc)
			if err == nil {
				return false
			}
		}
		return true
	})
}
