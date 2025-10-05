package update

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/eviction"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
)

type stubEvictor struct {
	t      *testing.T
	client ctrlclient.Client
	called []string
}

func (s *stubEvictor) Evict(ctx context.Context, req eviction.Request) error {
	if req.Pod == nil {
		s.t.Fatalf("expected pod in eviction request")
	}
	if err := s.client.Delete(ctx, req.Pod); err != nil && !apierrors.IsNotFound(err) {
		s.t.Fatalf("unexpected delete error: %v", err)
	}
	s.called = append(s.called, req.Pod.Name)
	return nil
}

func TestPlanUpdatesDetectsTLSHashChange(t *testing.T) {
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, RedisReplicas: 1},
	}
	ss := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{resources.ConfigHashAnnotationKey: "cfg", resources.TLSSecretHashAnnotationKey: "new"}}}}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Annotations: map[string]string{resources.ConfigHashAnnotationKey: "cfg", resources.TLSSecretHashAnnotationKey: "old"}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}

	plan := PlanUpdates(context.Background(), cr, ss, []corev1.Pod{pod}, nil, nil)
	reasons := plan.Reasons[pod.Name]
	found := false
	for _, r := range reasons {
		if r == "tls-hash" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected tls-hash reason, got %v", reasons)
	}
}

func TestExecuteAllowsTLSRotationWithUnhealthyReplicas(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add api scheme: %v", err)
	}

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			RedisReplicas: 2,
		},
	}
	ss := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{resources.ConfigHashAnnotationKey: "cfg", resources.TLSSecretHashAnnotationKey: "new"}}}}}
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-0",
				Namespace: "default",
				Labels: map[string]string{
					core.LabelAppKey:     "demo",
					core.LabelClusterKey: "demo",
					core.RoleLabelKey:    string(keyvalv1alpha1.PodRoleMaster),
				},
				Annotations: map[string]string{resources.ConfigHashAnnotationKey: "cfg", resources.TLSSecretHashAnnotationKey: "old"},
			},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-1",
				Namespace: "default",
				Labels: map[string]string{
					core.LabelAppKey:     "demo",
					core.LabelClusterKey: "demo",
					core.RoleLabelKey:    string(keyvalv1alpha1.PodRoleReplica),
				},
				Annotations: map[string]string{resources.ConfigHashAnnotationKey: "cfg", resources.TLSSecretHashAnnotationKey: "old"},
			},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		},
	}
	plan := PlanUpdates(context.Background(), cr, ss, pods, nil, nil)
	if len(plan.PodNames) == 0 {
		t.Fatalf("expected plan to include pods")
	}
	health := map[string]keyvalv1alpha1.PodHealth{
		"demo-0": keyvalv1alpha1.PodHealthOffline,
		"demo-1": keyvalv1alpha1.PodHealthOffline,
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods[0].DeepCopy(), pods[1].DeepCopy()).Build()
	evictor := &stubEvictor{t: t, client: client}
	deleted, err := Execute(context.Background(), client, cr, plan, pods, health, ExecuteOptions{Evictor: evictor})
	if err != nil {
		t.Fatalf("execute returned error: %v", err)
	}
	if deleted != "demo-1" {
		t.Fatalf("expected demo-1 to be deleted, got %q", deleted)
	}
	if len(evictor.called) != 1 || evictor.called[0] != "demo-1" {
		t.Fatalf("expected eviction for demo-1, got %v", evictor.called)
	}
	var remaining corev1.Pod
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-1"}, &remaining); err == nil {
		t.Fatalf("expected demo-1 pod to be deleted")
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("expected not found error, got %v", err)
	}
}
