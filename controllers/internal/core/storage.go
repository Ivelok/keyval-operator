package core

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

var defaultStorageRequest = resource.MustParse("1Gi")

// HasPersistentData reports whether the cluster is configured to use
// persistent storage for Redis pods. When false, the operator should avoid
// performing persistence-specific operations such as PVC annotations.
func HasPersistentData(cr *keyvalv1alpha1.KeyValCluster) bool {
	if cr == nil {
		return false
	}
	if cr.Spec.Storage == nil {
		return true
	}
	switch cr.Spec.Storage.Type {
	case "", "Persistent":
		return true
	case "Ephemeral":
		return false
	default:
		return true
	}
}

// StorageAccessModes returns the access modes requested for Redis data
// persistent volumes. When the cluster is configured for ephemeral storage
// it returns a default of ReadWriteOnce to preserve backwards compatibility
// with callers that expect a non-empty result.
func StorageAccessModes(cr *keyvalv1alpha1.KeyValCluster) []corev1.PersistentVolumeAccessMode {
	if cr == nil {
		return []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	if cr.Spec.Storage != nil && len(cr.Spec.Storage.AccessModes) > 0 {
		return cr.Spec.Storage.AccessModes
	}
	return []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
}

func DataPVCName(cr *keyvalv1alpha1.KeyValCluster, podName string) string {
	if cr == nil {
		return DataVolumeName + "-" + podName
	}
	return DataVolumeName + "-" + podName
}

// DesiredStorageQuantity returns the desired storage quantity for Redis PVCs,
// applying defaults and parsing the user provided spec.
func DesiredStorageQuantity(cr *keyvalv1alpha1.KeyValCluster) resource.Quantity {
	if cr == nil || cr.Spec.Storage == nil || cr.Spec.Storage.Size == nil {
		return defaultStorageRequest.DeepCopy()
	}
	q, err := resource.ParseQuantity(string(*cr.Spec.Storage.Size))
	if err != nil {
		return defaultStorageRequest.DeepCopy()
	}
	return q
}

// StorageCleanupEnabled reports whether PVCs should be deleted during finalizer cleanup.
func StorageCleanupEnabled(cr *keyvalv1alpha1.KeyValCluster) bool {
	if cr == nil || cr.Spec.Storage == nil {
		return false
	}
	if strings.EqualFold(cr.Spec.Storage.Type, "Ephemeral") {
		return false
	}
	return cr.Spec.Storage.CleanupOnDelete
}

// PodNameFromPVC derives the StatefulSet pod name from the PVC name.
func PodNameFromPVC(cr *keyvalv1alpha1.KeyValCluster, pvcName string) string {
	if cr == nil {
		return ""
	}
	prefix := fmt.Sprintf("%s-%s-", DataVolumeName, cr.Name)
	if !strings.HasPrefix(pvcName, prefix) {
		return ""
	}
	ordinal := strings.TrimPrefix(pvcName, prefix)
	if ordinal == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s", cr.Name, ordinal)
}

// PVCBelongsToCluster returns true when the PVC is owned by the cluster's StatefulSet.
func PVCBelongsToCluster(cr *keyvalv1alpha1.KeyValCluster, pvc *corev1.PersistentVolumeClaim) bool {
	if cr == nil || pvc == nil {
		return false
	}
	if pvc.Namespace != cr.Namespace {
		return false
	}
	prefix := fmt.Sprintf("%s-%s-", DataVolumeName, cr.Name)
	if strings.HasPrefix(pvc.Name, prefix) {
		return true
	}
	for _, owner := range pvc.OwnerReferences {
		if owner.Kind == "StatefulSet" && owner.Name == cr.Name {
			return true
		}
	}
	if pvc.Labels != nil {
		if pvc.Labels[LabelClusterKey] == cr.Name && pvc.Labels[LabelAppKey] == AppLabel(cr) {
			return true
		}
	}
	return false
}
