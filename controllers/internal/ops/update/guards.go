package update

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
)

// GuardInput describes context for evaluating rolling update guards.
type GuardInput struct {
	AllowDisruptions bool
	Status           *keyvalv1alpha1.KeyValClusterStatus
	Plan             Plan
	Pods             []corev1.Pod
	Health           map[string]keyvalv1alpha1.PodHealth
	Master           string
	DesiredReplicas  int32
	Mode             keyvalv1alpha1.Mode
}

// GuardDecision is the outcome of guard evaluation.
type GuardDecision struct {
	Blocked      bool
	Reason       string
	Detail       string
	RequeueAfter time.Duration
}

// EvaluateGuards applies disruption policies to determine whether rolling updates may proceed.
func EvaluateGuards(input GuardInput) GuardDecision {
	if len(input.Plan.PodNames) == 0 {
		return GuardDecision{}
	}
	hasTLSReason := planHasReason(input.Plan, "tls-hash")
	if hasTLSReason {
		return GuardDecision{}
	}
	if !input.AllowDisruptions {
		return GuardDecision{Blocked: true, Reason: "DisruptionsPaused", Detail: "health gate forbids disruptions", RequeueAfter: 5 * time.Second}
	}
	if hasCondition(input.Status, keyvalv1alpha1.ConditionFailoverInProgress, metav1.ConditionTrue) {
		return GuardDecision{Blocked: true, Reason: "FailoverInProgress", Detail: "waiting for failover to finish", RequeueAfter: 5 * time.Second}
	}
	if hasCondition(input.Status, keyvalv1alpha1.ConditionBootstrapInProgress, metav1.ConditionTrue) {
		return GuardDecision{Blocked: true, Reason: "BootstrapInProgress", Detail: "bootstrap gating updates", RequeueAfter: 5 * time.Second}
	}
	if input.DesiredReplicas > 1 {
		healthyReplicas := countHealthyReplicas(input.Pods, input.Health, input.Master)
		if healthyReplicas == 0 {
			return GuardDecision{Blocked: true, Reason: "NoHealthyReplicas", Detail: "no healthy replica available", RequeueAfter: 5 * time.Second}
		}
	}
	return GuardDecision{}
}

func hasCondition(status *keyvalv1alpha1.KeyValClusterStatus, typ keyvalv1alpha1.ConditionType, want metav1.ConditionStatus) bool {
	if status == nil {
		return false
	}
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == string(typ) && c.Status == want {
			return true
		}
	}
	return false
}

func countHealthyReplicas(pods []corev1.Pod, health map[string]keyvalv1alpha1.PodHealth, master string) int {
	count := 0
	for i := range pods {
		p := &pods[i]
		if p.Name == master {
			continue
		}
		if p.Labels[core.RoleLabelKey] != string(keyvalv1alpha1.PodRoleReplica) {
			continue
		}
		if healthStateForPod(*p, health) {
			count++
		}
	}
	return count
}

func healthStateForPod(pod corev1.Pod, health map[string]keyvalv1alpha1.PodHealth) bool {
	if health == nil {
		return runtimepkg.IsPodReady(&pod)
	}
	h, ok := health[pod.Name]
	if !ok {
		return runtimepkg.IsPodReady(&pod)
	}
	return h == keyvalv1alpha1.PodHealthHealthy
}

func planHasReason(plan Plan, reason string) bool {
	if reason == "" {
		return false
	}
	for _, reasons := range plan.Reasons {
		for _, r := range reasons {
			if r == reason {
				return true
			}
		}
	}
	return false
}
