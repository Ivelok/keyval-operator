package ssa

import (
	"testing"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestPDBTrim(t *testing.T) {
	source := &policyv1.PodDisruptionBudget{}
	source.Name = "redis"
	source.Namespace = "ns"
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}}
	source.Spec.Selector = selector
	value := intstr.FromInt(1)
	source.Spec.MinAvailable = &value

	result := PodDisruptionBudget(source)

	if result.APIVersion != "policy/v1" || result.Kind != "PodDisruptionBudget" {
		t.Fatalf("unexpected typemeta: %s %s", result.APIVersion, result.Kind)
	}
	if result.Spec.Selector == nil || result.Spec.Selector.MatchLabels["app"] != "redis" {
		t.Fatalf("selector not copied: %#v", result.Spec.Selector)
	}
	if result.Spec.MinAvailable == nil || result.Spec.MinAvailable.IntValue() != 1 {
		t.Fatalf("minAvailable not copied: %#v", result.Spec.MinAvailable)
	}
	if result.Status.CurrentHealthy != 0 {
		t.Fatalf("status should not be copied")
	}
}
