package controllers

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
)

func TestComputeStatus_NoChurnWhenUnchanged(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, RedisReplicas: 2}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
	}
	roles := map[string]keyvalv1alpha1.PodRole{"demo-0": keyvalv1alpha1.PodRoleMaster, "demo-1": keyvalv1alpha1.PodRoleReplica}
	health := map[string]keyvalv1alpha1.PodHealth{"demo-0": keyvalv1alpha1.PodHealthHealthy, "demo-1": keyvalv1alpha1.PodHealthHealthy}
	st := opstatus.ClusterState{Master: "demo-0", Pods: pods, Roles: roles, Health: health}
	s1 := opstatus.ComputeStatus(cr, st)
	// set previous status
	cr.Status = s1
	s2 := opstatus.ComputeStatus(cr, st)
	if !reflect.DeepEqual(s1, s2) {
		t.Fatalf("status churn detected:\n%+v\n!=\n%+v", s1, s2)
	}
}
