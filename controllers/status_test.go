package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
)

func TestComputeStatus_Basic(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, RedisReplicas: 2}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}},
	}
	roles := map[string]keyvalv1alpha1.PodRole{"demo-0": keyvalv1alpha1.PodRoleMaster, "demo-1": keyvalv1alpha1.PodRoleReplica}
	health := map[string]keyvalv1alpha1.PodHealth{"demo-0": keyvalv1alpha1.PodHealthHealthy, "demo-1": keyvalv1alpha1.PodHealthLagging}
	lag := map[string]*int32{"demo-1": func() *int32 { v := int32(7); return &v }()}
	st := opstatus.ClusterState{Master: "demo-0", Pods: pods, Roles: roles, Health: health, LagSeconds: lag}
	s := opstatus.ComputeStatus(cr, st)
	if s.MasterPod != "demo-0" || s.ReadyReplicas != 1 || s.Replicas != 2 {
		t.Fatalf("unexpected status: %+v", s)
	}
	if len(s.Roles) != 2 {
		t.Fatalf("expected 2 roles entries")
	}
	for _, r := range s.Roles {
		if r.Name == "demo-1" && (r.Health != keyvalv1alpha1.PodHealthLagging || r.LagSeconds == nil || *r.LagSeconds != 7) {
			t.Fatalf("expected lagging health, got %+v", r)
		}
	}
	if s.HealthGate == nil {
		t.Fatalf("expected healthGate to be populated")
	}
	if s.HealthGate.AllowDisruptions {
		t.Fatalf("expected disruptions to be disallowed due to replica lag")
	}
	conds := map[string]metav1.Condition{}
	for _, c := range s.Conditions {
		conds[c.Type] = c
	}
	if cond, ok := conds[string(keyvalv1alpha1.ConditionAvailable)]; !ok || cond.Status != metav1.ConditionTrue {
		t.Fatalf("expected Available=True, got %+v", cond)
	}
	if cond, ok := conds[string(keyvalv1alpha1.ConditionReplicationHealthy)]; !ok || cond.Status != metav1.ConditionFalse {
		t.Fatalf("expected ReplicationHealthy=False, got %+v", cond)
	}
}

func TestComputeStatus_AllHealthyAllowsDisruptions(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, RedisReplicas: 2}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
	}
	roles := map[string]keyvalv1alpha1.PodRole{"demo-0": keyvalv1alpha1.PodRoleMaster, "demo-1": keyvalv1alpha1.PodRoleReplica}
	health := map[string]keyvalv1alpha1.PodHealth{"demo-0": keyvalv1alpha1.PodHealthHealthy, "demo-1": keyvalv1alpha1.PodHealthHealthy}
	st := opstatus.ClusterState{Master: "demo-0", Pods: pods, Roles: roles, Health: health}
	s := opstatus.ComputeStatus(cr, st)
	if s.HealthGate == nil {
		t.Fatalf("expected healthGate populated")
	}
	if !s.HealthGate.AllowDisruptions {
		t.Fatalf("expected disruptions to be allowed when cluster healthy")
	}
	condMap := map[string]metav1.Condition{}
	for _, c := range s.Conditions {
		condMap[c.Type] = c
	}
	if cond := condMap[string(keyvalv1alpha1.ConditionReplicationHealthy)]; cond.Status != metav1.ConditionTrue {
		t.Fatalf("expected ReplicationHealthy=True, got %+v", cond)
	}
}
