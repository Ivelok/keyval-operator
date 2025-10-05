package replication

import (
	corev1 "k8s.io/api/core/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

// UpdatePodRoleLabels returns a copy of pods with role labels aligned so that exactly one pod
// carries the master label and all others are replicas.
func UpdatePodRoleLabels(pods []corev1.Pod, master string) []corev1.Pod {
	out := make([]corev1.Pod, len(pods))
	for i := range pods {
		out[i] = *pods[i].DeepCopy()
	}
	for i := range out {
		p := &out[i]
		if p.Labels == nil {
			p.Labels = map[string]string{}
		}
		if p.Labels[core.RoleLabelKey] == string(keyvalv1alpha1.PodRoleMaster) && p.Name != master {
			p.Labels[core.RoleLabelKey] = string(keyvalv1alpha1.PodRoleReplica)
		}
	}
	for i := range out {
		p := &out[i]
		if p.Name == master {
			p.Labels[core.RoleLabelKey] = string(keyvalv1alpha1.PodRoleMaster)
		} else {
			if p.Labels == nil {
				p.Labels = map[string]string{}
			}
			p.Labels[core.RoleLabelKey] = string(keyvalv1alpha1.PodRoleReplica)
		}
	}
	return out
}
