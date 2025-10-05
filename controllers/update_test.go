package controllers

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opeviction "github.com/ivelok/keyval-operator/controllers/internal/ops/eviction"
	opstorage "github.com/ivelok/keyval-operator/controllers/internal/ops/storage"
	opupdate "github.com/ivelok/keyval-operator/controllers/internal/ops/update"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

type panicEvictor struct{}

func (panicEvictor) Evict(context.Context, opeviction.Request) error {
	panic("unexpected eviction")
}

type rejectingEvictor struct{}

func (rejectingEvictor) Evict(context.Context, opeviction.Request) error {
	return opeviction.ErrRejected
}

func TestPlanUpdates_OrdersByDescendingOrdinal(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 2}}
	ss := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{resources.ConfigHashAnnotationKey: "h1"}}}}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Annotations: map[string]string{resources.ConfigHashAnnotationKey: "old"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: core.RedisContainerName, Image: "valkey/valkey:7.1"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "default", Annotations: map[string]string{resources.ConfigHashAnnotationKey: "old"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: core.RedisContainerName, Image: "valkey/valkey:7.1"}}}},
	}
	plan := opupdate.PlanUpdates(context.TODO(), cr, ss, pods, nil, nil)
	if len(plan.PodNames) != 2 || plan.PodNames[0] != "demo-1" || plan.PodNames[1] != "demo-0" {
		t.Fatalf("unexpected plan order: %#v", plan.PodNames)
	}
}

func TestPlanUpdates_ReasonsIncludeConfigAndImage(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1}}
	ss := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{resources.ConfigHashAnnotationKey: "newh"}}}}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Annotations: map[string]string{resources.ConfigHashAnnotationKey: "old"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: core.RedisContainerName, Image: "valkey/valkey:7.1"}}}}
	plan := opupdate.PlanUpdates(context.TODO(), cr, ss, []corev1.Pod{pod}, nil, nil)
	rsn := plan.Reasons["demo-0"]
	if len(rsn) < 2 {
		t.Fatalf("expected multiple reasons, got %v", rsn)
	}
}

func TestPlanUpdates_HealthReasonAdded(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1}}
	ss := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{}}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}}
	health := map[string]keyvalv1alpha1.PodHealth{"demo-0": keyvalv1alpha1.PodHealthLagging}
	plan := opupdate.PlanUpdates(context.TODO(), cr, ss, []corev1.Pod{pod}, health, nil)
	rsn := plan.Reasons["demo-0"]
	found := false
	for _, r := range rsn {
		if r == "health:lagging" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected health reason, got %v", rsn)
	}
}

func TestPlanUpdates_ScaleDownTargetsHighestOrdinal(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1}}
	ss := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}}}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "default"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-2", Namespace: "default"}},
	}
	plan := opupdate.PlanUpdates(context.TODO(), cr, ss, pods, nil, nil)
	if len(plan.PodNames) == 0 || plan.PodNames[0] != "demo-2" {
		t.Fatalf("expected demo-2 first for scale down, got %v", plan.PodNames)
	}
	if rsn := plan.Reasons["demo-2"]; len(rsn) == 0 {
		t.Fatalf("expected reasons for demo-2 including scale-down")
	}
}

func TestPlanUpdates_DoesNotRestartReadyReplicaOnTransientDesync(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 2}}
	ss := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{resources.ConfigHashAnnotationKey: "h"}}}}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Annotations: map[string]string{resources.ConfigHashAnnotationKey: "h"}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "default", Annotations: map[string]string{resources.ConfigHashAnnotationKey: "h"}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
	}
	health := map[string]keyvalv1alpha1.PodHealth{"demo-1": keyvalv1alpha1.PodHealthDesynced}
	plan := opupdate.PlanUpdates(context.Background(), cr, ss, pods, health, nil)
	if len(plan.PodNames) != 0 {
		t.Fatalf("expected no restart plan, got %v", plan)
	}
}

func TestPlanUpdates_DoesNotRestartOfflineNotReadyPod(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1}}
	ss := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{resources.ConfigHashAnnotationKey: "h"}}}}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Annotations: map[string]string{resources.ConfigHashAnnotationKey: "h"}}, Status: corev1.PodStatus{}},
	}
	health := map[string]keyvalv1alpha1.PodHealth{"demo-0": keyvalv1alpha1.PodHealthOffline}
	plan := opupdate.PlanUpdates(context.Background(), cr, ss, pods, health, nil)
	if len(plan.PodNames) != 0 {
		t.Fatalf("expected no restart plan for offline unready pod, got %v", plan)
	}
}

func TestPlanUpdates_SentinelDefaultsRespected(t *testing.T) {
	t.Parallel()
	scnt := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: &scnt,
		},
	}
	ss := resources.SentinelStatefulSet(cr, "h", "", &security.Settings{})
	desired := resources.DesiredSentinelResources(cr)
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "demo-sentinel-0",
				Namespace:   "default",
				Annotations: map[string]string{resources.ConfigHashAnnotationKey: "h"},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:      core.SentinelContainerName,
				Image:     "valkey/valkey:7.2",
				Resources: desired,
			}}},
		},
	}
	plan := opupdate.PlanUpdates(context.Background(), cr, ss, pods, nil, nil)
	if len(plan.PodNames) != 0 {
		t.Fatalf("expected no restart when sentinel pods match defaults, got %v", plan.Reasons)
	}
}

func TestPlanUpdates_SentinelRedisResourcesTriggerUpdate(t *testing.T) {
	t.Parallel()
	scnt := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: &scnt,
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
		},
	}
	ss := resources.SentinelStatefulSet(cr, "h", "", &security.Settings{})
	redisResources := resources.ValueOrEmptyResources(cr.Spec.Resources)
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "demo-sentinel-0",
				Namespace:   "default",
				Annotations: map[string]string{resources.ConfigHashAnnotationKey: "h"},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:      core.SentinelContainerName,
				Image:     "valkey/valkey:7.2",
				Resources: redisResources,
			}}},
		},
	}
	plan := opupdate.PlanUpdates(context.Background(), cr, ss, pods, nil, nil)
	if len(plan.PodNames) == 0 {
		t.Fatalf("expected restart plan when sentinel pods use redis resources")
	}
	reasons := plan.Reasons[plan.PodNames[0]]
	found := false
	for _, r := range reasons {
		if r == "resources:sentinel" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected resources:sentinel reason, got %v", reasons)
	}
}

func TestPlanUpdates_SentinelNodeSelectorDriftTriggersUpdate(t *testing.T) {
	t.Parallel()
	scnt := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: &scnt,
			SentinelPod: &keyvalv1alpha1.SentinelPodSpec{
				NodeSelector: map[string]string{"key": "original"},
			},
		},
	}
	ss := resources.SentinelStatefulSet(cr, "hash", "", &security.Settings{})
	podSpec := *ss.Spec.Template.Spec.DeepCopy()
	podSpec.NodeSelector = map[string]string{"key": "drift"}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "demo-sentinel-0",
			Namespace:   "default",
			Annotations: map[string]string{resources.ConfigHashAnnotationKey: "hash"},
		},
		Spec: podSpec,
	}
	plan := opupdate.PlanUpdates(context.Background(), cr, ss, []corev1.Pod{pod}, nil, nil)
	if len(plan.PodNames) != 1 {
		t.Fatalf("expected exactly one sentinel pod in plan, got %v", plan.PodNames)
	}
	reasons := plan.Reasons[plan.PodNames[0]]
	found := false
	for _, r := range reasons {
		if r == "spec:node-selector" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected spec:node-selector reason, got %v", reasons)
	}
}

func TestPlanUpdates_AddsStorageReasons(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 1}}
	ss := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{}}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}}
	storageReasons := map[string][]string{"demo-0": {opstorage.ReasonResize}}
	plan := opupdate.PlanUpdates(context.Background(), cr, ss, []corev1.Pod{pod}, nil, storageReasons)
	reasons := plan.Reasons["demo-0"]
	found := false
	for _, r := range reasons {
		if r == opstorage.ReasonResize {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected storage resize reason, got %v", reasons)
	}
}

func TestExecuteUpdatePlan_SkipsWhenNoHealthyReplica(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, RedisReplicas: 2}}
	plan := opupdate.Plan{PodNames: []string{"demo-0"}, Reasons: map[string][]string{"demo-0": {"config-hash"}}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
	}
	health := map[string]keyvalv1alpha1.PodHealth{"demo-0": keyvalv1alpha1.PodHealthLagging, "demo-1": keyvalv1alpha1.PodHealthLagging}
	c := fake.NewClientBuilder().Build()
	evicted, err := opupdate.Execute(context.Background(), c, cr, plan, pods, health, opupdate.ExecuteOptions{Evictor: panicEvictor{}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if evicted != "" {
		t.Fatalf("should not evict when no healthy replicas remain")
	}
}

func TestExecuteSentinelFallbackDeletesUnreadyPod(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, RedisReplicas: 3}}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-sentinel-0",
			Namespace: "default",
			Labels: map[string]string{
				core.LabelAppKey:     "demo-sentinel",
				core.LabelClusterKey: "demo",
				core.RoleLabelKey:    string(keyvalv1alpha1.PodRoleSentinel),
			},
		},
	}
	plan := opupdate.Plan{
		PodNames: []string{pod.Name},
		Reasons:  map[string][]string{pod.Name: {"spec:node-selector"}},
	}
	c := fake.NewClientBuilder().WithObjects(pod.DeepCopy()).Build()
	evicted, err := opupdate.Execute(context.Background(), c, cr, plan, []corev1.Pod{pod}, nil, opupdate.ExecuteOptions{
		Evictor:   rejectingEvictor{},
		Component: opupdate.ComponentSentinel,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evicted != pod.Name {
		t.Fatalf("expected pod %s to be returned, got %s", pod.Name, evicted)
	}
	var remaining corev1.Pod
	getErr := c.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &remaining)
	if !apierrors.IsNotFound(getErr) {
		t.Fatalf("expected pod deletion fallback, got err=%v", getErr)
	}
}
