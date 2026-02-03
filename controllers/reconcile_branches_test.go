package controllers

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

type noopFactory struct{}

func (noopFactory) ForPod(_ context.Context, _ corev1.Pod, _ ClientOptions) (Client, error) {
	return nil, fmt.Errorf("noop client factory")
}

func TestReconcile_FinalizerDeletion_RemovesFinalizer(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	now := metav1.Now()
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "demo",
			Namespace:         "default",
			Finalizers:        []string{core.FinalizerName},
			DeletionTimestamp: &now,
		},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 1,
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    client,
		APIReader: client,
		Scheme:    scheme,
	})

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}
	res, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("expected no requeue on deletion path, got %s", res.RequeueAfter)
	}

	var updated keyvalv1alpha1.KeyValCluster
	if err := client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo"}, &updated); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Fatalf("get updated CR: %v", err)
	}
	if controllerutil.ContainsFinalizer(&updated, core.FinalizerName) {
		t.Fatalf("expected finalizer removed")
	}
}

func TestReconcile_ExternalImportError_UpdatesStatus(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 1,
			Bootstrap: &keyvalv1alpha1.BootstrapSpec{
				ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{Address: "redis://source:6379"},
			},
		},
	}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&keyvalv1alpha1.KeyValCluster{}, &appsv1.StatefulSet{}, &corev1.Pod{}).
		WithObjects(cr).
		Build()
	applyClient := &applyAwareClient{Client: baseClient}

	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    applyClient,
		APIReader: applyClient,
		Scheme:    scheme,
	})

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}
	_, err := reconciler.Reconcile(ctx, req)
	if err == nil {
		t.Fatalf("expected error when external import missing client factory")
	}

	var updated keyvalv1alpha1.KeyValCluster
	if err := applyClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo"}, &updated); err != nil {
		t.Fatalf("get updated CR: %v", err)
	}
	cond := findCondition(updated.Status.Conditions, string(keyvalv1alpha1.ConditionExternalImport))
	if cond == nil {
		t.Fatalf("expected ExternalImport condition")
	}
	if cond.Reason != "MissingDependency" {
		t.Fatalf("expected MissingDependency reason, got %s", cond.Reason)
	}
}

func TestReconcile_ExternalImportAbort_Requeues(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 1,
			Bootstrap: &keyvalv1alpha1.BootstrapSpec{
				ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{Address: "redis://source:6379"},
			},
		},
	}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&keyvalv1alpha1.KeyValCluster{}, &appsv1.StatefulSet{}, &corev1.Pod{}).
		WithObjects(cr).
		Build()
	applyClient := &applyAwareClient{Client: baseClient}

	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:        applyClient,
		APIReader:     applyClient,
		Scheme:        scheme,
		ClientFactory: noopFactory{},
	})

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}
	res, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("expected requeue when external import is waiting for pods")
	}
}

func TestReconcile_NoPods_Requeues(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 1,
		},
	}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&keyvalv1alpha1.KeyValCluster{}, &appsv1.StatefulSet{}, &corev1.Pod{}).
		WithObjects(cr).
		Build()
	applyClient := &applyAwareClient{Client: baseClient}

	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    applyClient,
		APIReader: applyClient,
		Scheme:    scheme,
	})

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}
	res, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("expected requeue when no pods are present")
	}
}
