package controllers

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	ctrl "sigs.k8s.io/controller-runtime"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func TestReconcile_RoleLabelPatchRetriesOnConflict(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 2,
		},
	}
	sec := &security.Settings{}
	hash := resources.ConfigHash(cr, sec)
	runtimeHash := resources.RedisRuntimeHash(cr, sec)
	podSpec := resources.StatefulSet(cr, hash, "", sec).Spec.Template.Spec
	desiredConfig := resources.EffectiveRedisConfig(cr, sec)

	baseLabels := map[string]string{
		core.LabelAppKey:     core.AppLabel(cr),
		core.LabelClusterKey: cr.Name,
	}

	masterPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "demo-0",
			Namespace:       "default",
			Labels:          mergeLabels(baseLabels, map[string]string{core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster)}),
			ResourceVersion: "1",
			Annotations: map[string]string{
				resources.ConfigHashAnnotationKey:  hash,
				resources.RuntimeHashAnnotationKey: runtimeHash,
			},
		},
		Spec: podSpec,
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	relabeledPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "demo-1",
			Namespace:       "default",
			Labels:          mergeLabels(baseLabels, map[string]string{core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster)}),
			ResourceVersion: "1",
			Annotations: map[string]string{
				resources.ConfigHashAnnotationKey:  hash,
				resources.RuntimeHashAnnotationKey: runtimeHash,
			},
		},
		Spec: podSpec,
		Status: corev1.PodStatus{
			PodIP: "10.0.0.2",
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: baseLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: baseLabels},
			},
			ServiceName: "demo-headless",
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.OnDeleteStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
		},
	}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&keyvalv1alpha1.KeyValCluster{}, &appsv1.StatefulSet{}, &corev1.Pod{}).
		WithObjects(ns, cr.DeepCopy(), masterPod.DeepCopy(), relabeledPod.DeepCopy(), ss.DeepCopy()).
		Build()

	applyClient := &applyAwareClient{Client: baseClient}

	conflictClient := &roleLabelConflictClient{
		Client:    applyClient,
		remaining: map[string]int{"demo-1": 1},
	}

	factory := &trackingFactory{clients: map[string]*trackingClient{
		"demo-0": {name: "demo-0", role: "master", masterHost: "10.0.0.1", masterPort: 6379, config: cloneStringMap(desiredConfig)},
		"demo-1": {name: "demo-1", role: "replica", masterHost: "10.0.0.1", masterPort: 6379, config: cloneStringMap(desiredConfig)},
	}}

	opobs.PodLabelPatchConflictCounter().DeleteLabelValues("default", "demo", "demo-1")
	opobs.PodLabelPatchRetryCounter().DeleteLabelValues("default", "demo", "demo-1")

	reconciler := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:        conflictClient,
		APIReader:     conflictClient,
		Scheme:        scheme,
		ClientFactory: factory,
	})

	ctx := context.Background()
	res, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if res.RequeueAfter < 2*time.Second {
		t.Fatalf("expected requeue after conflict to be at least 2s, got %s", res.RequeueAfter)
	}

	var updated corev1.Pod
	if err := conflictClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-1"}, &updated); err != nil {
		t.Fatalf("get relabeled pod: %v", err)
	}
	if got := updated.Labels[core.RoleLabelKey]; got != string(keyvalv1alpha1.PodRoleReplica) {
		t.Fatalf("expected demo-1 relabeled replica, got %s", got)
	}

	conflictMetric, err := opobs.PodLabelPatchConflictCounter().GetMetricWith(prometheusLabels("default", "demo", "demo-1"))
	if err != nil {
		t.Fatalf("get conflict metric: %v", err)
	}
	if got := testutil.ToFloat64(conflictMetric); got < 1 {
		t.Fatalf("expected conflict metric >=1, got %f", got)
	}
	retryMetric, err := opobs.PodLabelPatchRetryCounter().GetMetricWith(prometheusLabels("default", "demo", "demo-1"))
	if err != nil {
		t.Fatalf("get retry metric: %v", err)
	}
	if got := testutil.ToFloat64(retryMetric); got < 1 {
		t.Fatalf("expected retry metric >=1, got %f", got)
	}
}

type roleLabelConflictClient struct {
	client.Client
	mu        sync.Mutex
	remaining map[string]int
}

func (c *roleLabelConflictClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && patch.Type() == types.MergePatchType {
		c.mu.Lock()
		defer c.mu.Unlock()
		if n := c.remaining[pod.Name]; n > 0 {
			c.remaining[pod.Name] = n - 1
			return apierrors.NewConflict(corev1.Resource("pods"), pod.Name, fmt.Errorf("simulated conflict"))
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func mergeLabels(base map[string]string, extra map[string]string) map[string]string {
	labels := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		labels[k] = v
	}
	for k, v := range extra {
		labels[k] = v
	}
	return labels
}

func prometheusLabels(namespace, cluster, pod string) prometheus.Labels {
	return prometheus.Labels{"namespace": namespace, "cluster": cluster, "pod": pod}
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
