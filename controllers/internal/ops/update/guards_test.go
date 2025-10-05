package update

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestEvaluateGuardsAllowsEmptyPlan(t *testing.T) {
	t.Parallel()
	decision := EvaluateGuards(GuardInput{Plan: Plan{}})
	if decision.Blocked {
		t.Fatalf("expected no block for empty plan, got %+v", decision)
	}
}

func TestEvaluateGuardsBlocksWhenDisruptionsPaused(t *testing.T) {
	t.Parallel()
	decision := EvaluateGuards(GuardInput{AllowDisruptions: false, Plan: Plan{PodNames: []string{"demo-0"}}})
	if !decision.Blocked {
		t.Fatalf("expected disruptions paused to block plan")
	}
	if decision.Reason != "DisruptionsPaused" {
		t.Fatalf("unexpected reason %s", decision.Reason)
	}
}

func TestEvaluateGuardsBlocksDuringFailover(t *testing.T) {
	t.Parallel()
	status := &keyvalv1alpha1.KeyValClusterStatus{Conditions: []metav1.Condition{{Type: string(keyvalv1alpha1.ConditionFailoverInProgress), Status: metav1.ConditionTrue}}}
	decision := EvaluateGuards(GuardInput{AllowDisruptions: true, Status: status, Plan: Plan{PodNames: []string{"demo-0"}}})
	if !decision.Blocked {
		t.Fatalf("expected failover condition to block plan")
	}
	if decision.Reason != "FailoverInProgress" {
		t.Fatalf("unexpected reason %s", decision.Reason)
	}
}

func TestEvaluateGuardsRequiresHealthyReplica(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Labels: map[string]string{core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster)}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Labels: map[string]string{core.RoleLabelKey: string(keyvalv1alpha1.PodRoleReplica)}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}},
	}
	decision := EvaluateGuards(GuardInput{
		AllowDisruptions: true,
		Plan:             Plan{PodNames: []string{"demo-0"}},
		Pods:             pods,
		DesiredReplicas:  2,
		Mode:             keyvalv1alpha1.ModeSentinel,
		Master:           "demo-0",
	})
	if !decision.Blocked {
		t.Fatalf("expected guard to block when no healthy replica")
	}
	if decision.Reason != "NoHealthyReplicas" {
		t.Fatalf("unexpected reason %s", decision.Reason)
	}
	if decision.RequeueAfter <= 0 {
		t.Fatalf("expected requeue after delay, got %s", decision.RequeueAfter)
	}
}

func TestEvaluateGuardsAllowsWhenReplicaHealthy(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Labels: map[string]string{core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster)}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Labels: map[string]string{core.RoleLabelKey: string(keyvalv1alpha1.PodRoleReplica)}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
	}
	decision := EvaluateGuards(GuardInput{
		AllowDisruptions: true,
		Plan:             Plan{PodNames: []string{"demo-1"}},
		Pods:             pods,
		DesiredReplicas:  2,
		Master:           "demo-0",
		Mode:             keyvalv1alpha1.ModeSentinel,
	})
	if decision.Blocked {
		t.Fatalf("expected healthy replica to allow plan, got %+v", decision)
	}
}

func TestEvaluateGuardsBlocksDuringBootstrap(t *testing.T) {
	t.Parallel()
	status := &keyvalv1alpha1.KeyValClusterStatus{Conditions: []metav1.Condition{{Type: string(keyvalv1alpha1.ConditionBootstrapInProgress), Status: metav1.ConditionTrue}}}
	decision := EvaluateGuards(GuardInput{AllowDisruptions: true, Status: status, Plan: Plan{PodNames: []string{"demo-0"}}})
	if !decision.Blocked {
		t.Fatalf("expected bootstrap in progress to block plan")
	}
	if decision.RequeueAfter != 5*time.Second {
		t.Fatalf("expected default requeue 5s, got %s", decision.RequeueAfter)
	}
}
func TestEvaluateGuardsAllowsTLSRotationWhenDisruptionsPaused(t *testing.T) {
	t.Parallel()
	plan := Plan{PodNames: []string{"demo-0"}, Reasons: map[string][]string{"demo-0": {"tls-hash"}}}
	decision := EvaluateGuards(GuardInput{AllowDisruptions: false, Plan: plan})
	if decision.Blocked {
		t.Fatalf("expected tls rotation to proceed even when disruptions paused")
	}
}
