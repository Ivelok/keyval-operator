package ssa

import (
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodDisruptionBudget returns a PDB containing only operator-owned fields for SSA patches.
func PodDisruptionBudget(src *policyv1.PodDisruptionBudget) *policyv1.PodDisruptionBudget {
	if src == nil {
		return nil
	}
	out := &policyv1.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        src.Name,
			Namespace:   src.Namespace,
			Labels:      copyStringMap(src.Labels),
			Annotations: copyStringMap(src.Annotations),
		},
	}

	spec := policyv1.PodDisruptionBudgetSpec{}
	if src.Spec.Selector != nil {
		spec.Selector = src.Spec.Selector.DeepCopy()
	}
	if src.Spec.MinAvailable != nil {
		val := *src.Spec.MinAvailable
		spec.MinAvailable = &val
	}
	if src.Spec.MaxUnavailable != nil {
		val := *src.Spec.MaxUnavailable
		spec.MaxUnavailable = &val
	}

	out.Spec = spec
	return out
}
