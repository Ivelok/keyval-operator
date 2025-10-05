package runtime

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PreserveClusterIP copies the existing Service IP configuration into desired so that server-side apply
// keeps immutable fields stable across reconciles.
func PreserveClusterIP(ctx context.Context, c client.Client, desired *corev1.Service) error {
	var existing corev1.Service
	if err := c.Get(ctx, client.ObjectKey{Namespace: desired.Namespace, Name: desired.Name}, &existing); err == nil {
		desired.Spec.ClusterIP = existing.Spec.ClusterIP
		desired.Spec.ClusterIPs = append([]string(nil), existing.Spec.ClusterIPs...)
		desired.Spec.IPFamilies = append([]corev1.IPFamily(nil), existing.Spec.IPFamilies...)
		if existing.Spec.IPFamilyPolicy != nil {
			policy := *existing.Spec.IPFamilyPolicy
			desired.Spec.IPFamilyPolicy = &policy
		}
	}
	return nil
}
