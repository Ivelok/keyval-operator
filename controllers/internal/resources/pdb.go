package resources

import (
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func RedisPDB(cr *keyvalv1alpha1.KeyValCluster, minAvailable int32) *policyv1.PodDisruptionBudget {
	if minAvailable < 0 {
		minAvailable = 0
	}
	labels := core.LabelsFor(cr)
	min := intstr.FromInt(int(minAvailable))
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-redis",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				core.LabelAppKey:     labels[core.LabelAppKey],
				core.LabelClusterKey: labels[core.LabelClusterKey],
			}},
			MinAvailable: &min,
		},
	}
	return pdb
}

func SentinelPDB(cr *keyvalv1alpha1.KeyValCluster, minAvailable int32) *policyv1.PodDisruptionBudget {
	labels := core.SentinelLabelsFor(cr)
	if minAvailable < 0 {
		minAvailable = 0
	}
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-sentinel",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				core.LabelAppKey:     labels[core.LabelAppKey],
				core.LabelClusterKey: labels[core.LabelClusterKey],
			}},
		},
	}
	var minPtr *intstr.IntOrString
	if cr.Spec.SentinelPDB != nil {
		if cr.Spec.SentinelPDB.MinAvailable != nil {
			v := *cr.Spec.SentinelPDB.MinAvailable
			minPtr = &v
		}
		if cr.Spec.SentinelPDB.MaxUnavailable != nil {
			v := *cr.Spec.SentinelPDB.MaxUnavailable
			pdb.Spec.MaxUnavailable = &v
		}
	}
	if minPtr == nil && pdb.Spec.MaxUnavailable == nil {
		min := intstr.FromInt(int(minAvailable))
		minPtr = &min
	}
	if minPtr != nil {
		pdb.Spec.MinAvailable = minPtr
	}
	return pdb
}
