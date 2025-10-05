package ssa

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StatefulSet returns a StatefulSet containing only operator-owned fields for SSA patches.
func StatefulSet(src *appsv1.StatefulSet) *appsv1.StatefulSet {
	if src == nil {
		return nil
	}
	out := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        src.Name,
			Namespace:   src.Namespace,
			Labels:      copyStringMap(src.Labels),
			Annotations: copyStringMap(src.Annotations),
		},
	}

	spec := appsv1.StatefulSetSpec{
		ServiceName:          src.Spec.ServiceName,
		PodManagementPolicy:  src.Spec.PodManagementPolicy,
		RevisionHistoryLimit: src.Spec.RevisionHistoryLimit,
	}

	if src.Spec.Replicas != nil {
		replicas := *src.Spec.Replicas
		spec.Replicas = &replicas
	}

	if src.Spec.Selector != nil {
		spec.Selector = src.Spec.Selector.DeepCopy()
	}

	spec.Template = *src.Spec.Template.DeepCopy()
	spec.UpdateStrategy = *src.Spec.UpdateStrategy.DeepCopy()

	if src.Spec.MinReadySeconds != 0 {
		spec.MinReadySeconds = src.Spec.MinReadySeconds
	}
	if src.Spec.PersistentVolumeClaimRetentionPolicy != nil {
		spec.PersistentVolumeClaimRetentionPolicy = src.Spec.PersistentVolumeClaimRetentionPolicy.DeepCopy()
	}
	if src.Spec.Ordinals != nil {
		spec.Ordinals = src.Spec.Ordinals.DeepCopy()
	}

	if len(src.Spec.VolumeClaimTemplates) > 0 {
		templates := make([]corev1.PersistentVolumeClaim, len(src.Spec.VolumeClaimTemplates))
		for i := range src.Spec.VolumeClaimTemplates {
			templates[i] = *src.Spec.VolumeClaimTemplates[i].DeepCopy()
		}
		spec.VolumeClaimTemplates = templates
	}

	out.Spec = spec
	return out
}
