package controllers

import (
	"context"
	"io"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

func TestServiceLifecycle_DisableDeletesService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1},
	}
	cr.UID = types.UID("demo-uid")

	existing := resources.MasterService(cr.DeepCopy())
	if err := controllerutil.SetControllerReference(cr, existing, scheme); err != nil {
		t.Fatalf("set owner: %v", err)
	}

	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cr, existing).
		Build()
	recorder := record.NewFakeRecorder(5)
	opobs.ServiceRemovedCounter().DeleteLabelValues(cr.Namespace, cr.Name, "master")

	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    client,
		APIReader: client,
		Scheme:    scheme,
		Recorder:  recorder,
	})
	reconciler.baseLogger = logging.New(slog.New(slog.NewTextHandler(io.Discard, nil)))

	desired := resources.MasterService(cr.DeepCopy())
	ctx := context.Background()
	if err := reconciler.reconcileServiceLifecycle(ctx, cr, desired, serviceLifecycleOptions{
		Enabled:           false,
		PreserveClusterIP: true,
		ServiceType:       "master",
	}, logging.New(slog.New(slog.NewTextHandler(io.Discard, nil)))); err != nil {
		t.Fatalf("reconcile service lifecycle: %v", err)
	}

	var remaining corev1.Service
	err := client.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: core.MasterServiceName(cr)}, &remaining)
	switch {
	case apierrors.IsNotFound(err):
		// deleted
	case err == nil && remaining.DeletionTimestamp != nil:
		// deletion in progress is acceptable for fake client
	case err == nil:
		t.Fatalf("expected master service deletion timestamp, ownerRefs=%+v", remaining.OwnerReferences)
	default:
		t.Fatalf("get master service after delete: %v", err)
	}
	if !waitForEvent(recorder.Events, "ServiceRemoved") {
		t.Fatalf("expected ServiceRemoved event")
	}
	metric, mErr := opobs.ServiceRemovedCounter().GetMetricWith(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"service":   "master",
	})
	if mErr != nil {
		t.Fatalf("get metric: %v", mErr)
	}
	if got := testutil.ToFloat64(metric); got < 1.0 {
		t.Fatalf("expected service_removed metric >=1, got %f", got)
	}
}

func TestServiceLifecycle_SkipWhenNotOwned(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1},
	}
	cr.UID = types.UID("demo-uid")

	existing := resources.MasterService(cr.DeepCopy())
	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cr, existing).
		Build()
	recorder := record.NewFakeRecorder(2)
	opobs.ServiceRemovedCounter().DeleteLabelValues(cr.Namespace, cr.Name, "master")

	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    client,
		APIReader: client,
		Scheme:    scheme,
		Recorder:  recorder,
	})
	reconciler.baseLogger = logging.New(slog.New(slog.NewTextHandler(io.Discard, nil)))

	desired := resources.MasterService(cr.DeepCopy())
	ctx := context.Background()
	if err := reconciler.reconcileServiceLifecycle(ctx, cr, desired, serviceLifecycleOptions{
		Enabled:           false,
		PreserveClusterIP: true,
		ServiceType:       "master",
	}, logging.New(slog.New(slog.NewTextHandler(io.Discard, nil)))); err != nil {
		t.Fatalf("reconcile service lifecycle: %v", err)
	}

	var current corev1.Service
	if err := client.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: core.MasterServiceName(cr)}, &current); err != nil {
		t.Fatalf("expected master service intact, got err=%v", err)
	}
	select {
	case evt := <-recorder.Events:
		t.Fatalf("unexpected event: %s", evt)
	default:
	}
	metric, mErr := opobs.ServiceRemovedCounter().GetMetricWith(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"service":   "master",
	})
	if mErr == nil {
		if got := testutil.ToFloat64(metric); got != 0 {
			t.Fatalf("unexpected metric value: %f", got)
		}
	}
}

func TestServiceLifecycle_EnableApplies(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1},
	}
	cr.UID = types.UID("demo-uid")

	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cr).Build()
	client := &applyAwareClient{Client: baseClient}
	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    client,
		APIReader: client,
		Scheme:    scheme,
	})
	reconciler.baseLogger = logging.New(slog.New(slog.NewTextHandler(io.Discard, nil)))

	desired := resources.ReplicasService(cr.DeepCopy())
	ctx := context.Background()
	if err := reconciler.reconcileServiceLifecycle(ctx, cr, desired, serviceLifecycleOptions{
		Enabled:           true,
		PreserveClusterIP: true,
		ServiceType:       "replicas",
	}, logging.New(slog.New(slog.NewTextHandler(io.Discard, nil)))); err != nil {
		t.Fatalf("reconcile service lifecycle: %v", err)
	}

	var svc corev1.Service
	if err := client.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: core.ReplicasServiceName(cr)}, &svc); err != nil {
		t.Fatalf("expected replicas service created: %v", err)
	}
	if !metav1.IsControlledBy(&svc, cr) {
		t.Fatalf("replicas service missing owner reference")
	}
}

func TestServiceLifecycle_ImmutableGuardClusterIP(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1},
	}
	cr.UID = types.UID("demo-uid")

	existing := resources.MasterService(cr.DeepCopy())
	existing.Spec.ClusterIP = "10.0.0.10"
	existing.Spec.ClusterIPs = []string{"10.0.0.10"}
	existing.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	ipPolicy := corev1.IPFamilyPolicySingleStack
	existing.Spec.IPFamilyPolicy = &ipPolicy

	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cr, existing).
		Build()
	recorder := record.NewFakeRecorder(5)
	opobs.ServiceImmutableChangeCounter().DeleteLabelValues(cr.Namespace, cr.Name, "master", "spec.clusterIP")

	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    client,
		APIReader: client,
		Scheme:    scheme,
		Recorder:  recorder,
	})
	reconciler.baseLogger = logging.New(slog.New(slog.NewTextHandler(io.Discard, nil)))

	desired := resources.MasterService(cr.DeepCopy())
	desired.Spec.ClusterIP = "10.0.0.20"

	ctx := context.Background()
	if err := reconciler.reconcileServiceLifecycle(ctx, cr, desired, serviceLifecycleOptions{
		Enabled:           true,
		PreserveClusterIP: true,
		ServiceType:       "master",
	}, logging.New(slog.New(slog.NewTextHandler(io.Discard, nil)))); err != nil {
		t.Fatalf("reconcile service lifecycle: %v", err)
	}

	if !waitForEvent(recorder.Events, "ServiceImmutableField") {
		t.Fatalf("expected ServiceImmutableField event")
	}

	metric, err := opobs.ServiceImmutableChangeCounter().GetMetricWith(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"service":   "master",
		"field":     "spec.clusterIP",
	})
	if err != nil {
		t.Fatalf("get metric: %v", err)
	}
	if got := testutil.ToFloat64(metric); got < 1.0 {
		t.Fatalf("expected immutable change metric >=1, got %f", got)
	}

	var current corev1.Service
	if err := client.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: core.MasterServiceName(cr)}, &current); err != nil {
		t.Fatalf("get master service: %v", err)
	}
	if current.Spec.ClusterIP != "10.0.0.10" {
		t.Fatalf("expected clusterIP to remain unchanged, got %s", current.Spec.ClusterIP)
	}
}
