package ssa

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Service returns a Service containing only operator-owned fields for SSA patches.
func Service(src *corev1.Service) *corev1.Service {
	if src == nil {
		return nil
	}
	out := &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        src.Name,
			Namespace:   src.Namespace,
			Labels:      copyStringMap(src.Labels),
			Annotations: copyStringMap(src.Annotations),
		},
	}

	spec := corev1.ServiceSpec{
		Type:            src.Spec.Type,
		Selector:        copyStringMap(src.Spec.Selector),
		Ports:           copyServicePorts(src.Spec.Ports),
		SessionAffinity: src.Spec.SessionAffinity,
	}

	if src.Spec.PublishNotReadyAddresses {
		spec.PublishNotReadyAddresses = true
	}

	if src.Spec.ClusterIP == corev1.ClusterIPNone {
		spec.ClusterIP = corev1.ClusterIPNone
		if len(src.Spec.ClusterIPs) > 0 {
			spec.ClusterIPs = append(spec.ClusterIPs, corev1.ClusterIPNone)
		}
	}

	if src.Spec.ClusterIP != "" && src.Spec.ClusterIP != corev1.ClusterIPNone {
		v := src.Spec.ClusterIP
		spec.ClusterIP = v
		if len(src.Spec.ClusterIPs) > 0 {
			spec.ClusterIPs = append(spec.ClusterIPs, src.Spec.ClusterIPs...)
		}
	}

	if len(src.Spec.IPFamilies) > 0 {
		spec.IPFamilies = append([]corev1.IPFamily(nil), src.Spec.IPFamilies...)
	}

	if src.Spec.IPFamilyPolicy != nil {
		policy := *src.Spec.IPFamilyPolicy
		spec.IPFamilyPolicy = &policy
	}

	if src.Spec.Type == corev1.ServiceTypeExternalName && src.Spec.ExternalName != "" {
		spec.ExternalName = src.Spec.ExternalName
	}

	out.Spec = spec
	return out
}
