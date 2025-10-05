package runtime

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestPodLifecyclePredicate_UpdateReadyTransition(t *testing.T) {
	t.Parallel()
	pred := PodLifecyclePredicate()
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "default", Labels: map[string]string{core.LabelClusterKey: "demo"}}}
	newPod := oldPod.DeepCopy()
	newPod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if !pred.Update(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}) {
		t.Fatalf("expected predicate to trigger on readiness transition")
	}
}

func TestPodLifecyclePredicate_UpdateRoleLabelChange(t *testing.T) {
	t.Parallel()
	pred := PodLifecyclePredicate()
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Labels: map[string]string{core.LabelClusterKey: "demo", core.RoleLabelKey: string(keyvalv1alpha1.PodRoleReplica)}}}
	newPod := oldPod.DeepCopy()
	newPod.Labels[core.RoleLabelKey] = string(keyvalv1alpha1.PodRoleMaster)
	if !pred.Update(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}) {
		t.Fatalf("expected predicate to trigger on role label change")
	}
}

func TestPodToClusterRequests_UsesLabelOrOwner(t *testing.T) {
	t.Parallel()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "default", Labels: map[string]string{core.LabelClusterKey: "demo"}}}
	reqs := PodToClusterRequests(context.Background(), pod)
	if len(reqs) != 1 || reqs[0].NamespacedName.Name != "demo" {
		t.Fatalf("expected request for demo, got %v", reqs)
	}
	ownerPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:            "demo-sentinel-0",
		Namespace:       "default",
		Labels:          map[string]string{core.LabelAppKey: core.SentinelAppLabelForName("demo")},
		OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "demo-sentinel"}},
	}}
	reqs = PodToClusterRequests(context.Background(), ownerPod)
	if len(reqs) != 1 || reqs[0].NamespacedName.Name != "demo" {
		t.Fatalf("expected sentinel owner to map to demo, got %v", reqs)
	}
}

func TestIsClusterManagedPod_ForeignStatefulSetIgnored(t *testing.T) {
	t.Parallel()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "other-0",
		Namespace: "default",
		Labels:    map[string]string{"app": "other"},
		OwnerReferences: []metav1.OwnerReference{{
			Kind: "StatefulSet",
			Name: "other",
		}},
	}}
	if isClusterManagedPod(pod) {
		t.Fatalf("pod from foreign statefulset must not be treated as managed")
	}
	if _, ok := clusterNamespacedName(pod); ok {
		t.Fatalf("clusterNamespacedName should not resolve foreign pod")
	}
}
