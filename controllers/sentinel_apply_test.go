package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestReconcile_SentinelApplyImmutableFieldSignals(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: &v,
		},
	}

	redisPods := readyPodsForCluster(cr, int(cr.Spec.RedisReplicas))
	objects := make([]runtime.Object, 0, len(redisPods)+2)
	objects = append(objects, ns, cr)
	for _, pod := range redisPods {
		objects = append(objects, pod)
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&keyvalv1alpha1.KeyValCluster{}, &appsv1.StatefulSet{}, &corev1.Pod{}).
		WithRuntimeObjects(objects...).
		Build()

	applyClient := &applyAwareClient{Client: baseClient}
	immutableErr := apierrors.NewInvalid(
		appsv1.SchemeGroupVersion.WithKind("StatefulSet").GroupKind(),
		"demo-sentinel",
		field.ErrorList{
			field.Forbidden(field.NewPath("spec", "serviceName"), "cannot change once set"),
		},
	)
	client := &sentinelFailingClient{Client: applyClient, failErr: immutableErr}
	recorder := record.NewFakeRecorder(10)

	opobs.SentinelApplyFailureCounter().DeleteLabelValues("default", "demo", opobs.SentinelApplyFailureReasonImmutable)

	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    client,
		APIReader: client,
		Scheme:    scheme,
		Recorder:  recorder,
	})

	ctx := context.Background()
	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if client.patchHits == 0 {
		t.Fatalf("sentinel patch was not invoked")
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("expected requeue due to pending sentinel pods, got %s", result.RequeueAfter)
	}

	foundEvent := waitForEvent(recorder.Events, "SentinelApplyImmutableField")
	if !foundEvent {
		t.Fatalf("expected SentinelApplyImmutableField event")
	}

	var updated keyvalv1alpha1.KeyValCluster
	if err := client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo"}, &updated); err != nil {
		t.Fatalf("get updated CR: %v", err)
	}
	cond := findCondition(updated.Status.Conditions, string(keyvalv1alpha1.ConditionReconciled))
	if cond == nil {
		t.Fatalf("expected Reconciled condition present")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("expected Reconciled status False, got %s", cond.Status)
	}
	if cond.Reason != "ImmutableField" {
		t.Fatalf("expected Reconciled reason ImmutableField, got %s", cond.Reason)
	}
	if !strings.Contains(cond.Message, "spec.serviceName") {
		t.Fatalf("expected condition message to mention spec.serviceName, got %q", cond.Message)
	}

	counter, err := opobs.SentinelApplyFailureCounter().GetMetricWith(prometheus.Labels{
		"namespace": "default",
		"cluster":   "demo",
		"reason":    opobs.SentinelApplyFailureReasonImmutable,
	})
	if err != nil {
		t.Fatalf("get metric: %v", err)
	}
	if got := testutil.ToFloat64(counter); got < 1.0 {
		t.Fatalf("expected immutable metric >=1, got %f", got)
	}
}

func TestReconcile_SentinelApplyGenericError(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: &v,
		},
	}

	redisPods := readyPodsForCluster(cr, int(cr.Spec.RedisReplicas))
	objects := make([]runtime.Object, 0, len(redisPods)+2)
	objects = append(objects, ns, cr)
	for _, pod := range redisPods {
		objects = append(objects, pod)
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&keyvalv1alpha1.KeyValCluster{}, &appsv1.StatefulSet{}, &corev1.Pod{}).
		WithRuntimeObjects(objects...).
		Build()

	applyClient := &applyAwareClient{Client: baseClient}
	failing := &sentinelFailingClient{Client: applyClient, failErr: fmt.Errorf("boom")}
	recorder := record.NewFakeRecorder(10)

	opobs.SentinelApplyFailureCounter().DeleteLabelValues("default", "demo", opobs.SentinelApplyFailureReasonError)

	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    failing,
		APIReader: failing,
		Scheme:    scheme,
		Recorder:  recorder,
	})

	ctx := context.Background()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}})
	if err == nil {
		t.Fatalf("expected error from reconcile")
	}
	if !controllererrors.IsTransient(err) {
		t.Fatalf("expected transient error, got %v", err)
	}
	if failing.patchHits == 0 {
		t.Fatalf("sentinel patch was not invoked")
	}

	foundEvent := waitForEvent(recorder.Events, "SentinelApplyFailed")
	if !foundEvent {
		t.Fatalf("expected SentinelApplyFailed event")
	}

	counter, cErr := opobs.SentinelApplyFailureCounter().GetMetricWith(prometheus.Labels{
		"namespace": "default",
		"cluster":   "demo",
		"reason":    opobs.SentinelApplyFailureReasonError,
	})
	if cErr != nil {
		t.Fatalf("get metric: %v", cErr)
	}
	if got := testutil.ToFloat64(counter); got < 1.0 {
		t.Fatalf("expected error metric >=1, got %f", got)
	}
}

type sentinelFailingClient struct {
	client.Client
	failErr   error
	patchHits int
}

func (s *sentinelFailingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if patch.Type() == types.ApplyPatchType {
		if sts, ok := obj.(*appsv1.StatefulSet); ok && strings.HasSuffix(sts.Name, "-sentinel") {
			s.patchHits++
			if s.failErr != nil {
				return s.failErr
			}
		}
	}
	return s.Client.Patch(ctx, obj, patch, opts...)
}

func waitForEvent(ch <-chan string, reason string) bool {
	deadline := time.After(2 * time.Second)
	for {
		select {
		case evt := <-ch:
			if strings.Contains(evt, reason) {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

func findCondition(conditions []metav1.Condition, typ string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == typ {
			return &conditions[i]
		}
	}
	return nil
}

func readyPodsForCluster(cr *keyvalv1alpha1.KeyValCluster, count int) []*corev1.Pod {
	if count <= 0 {
		return nil
	}
	pods := make([]*corev1.Pod, 0, count)
	appLabel := core.AppLabel(cr)
	for i := 0; i < count; i++ {
		labels := map[string]string{
			core.LabelAppKey:     appLabel,
			core.LabelClusterKey: cr.Name,
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", cr.Name, i),
				Namespace: cr.Namespace,
				Labels:    labels,
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				}},
			},
		}
		pods = append(pods, pod)
	}
	return pods
}
