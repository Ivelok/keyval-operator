package runtime

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
)

// PodLifecyclePredicate returns a predicate that filters Pod events interesting to the operator.
func PodLifecyclePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			pod, ok := e.Object.(*corev1.Pod)
			return ok && (isClusterManagedPod(pod) || e.Object.GetLabels()[core.LabelClusterKey] != "")
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			pod, ok := e.Object.(*corev1.Pod)
			return ok && isClusterManagedPod(pod)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, okOld := e.ObjectOld.(*corev1.Pod)
			newPod, okNew := e.ObjectNew.(*corev1.Pod)
			if !okOld || !okNew {
				return false
			}
			managedOld := isClusterManagedPod(oldPod)
			managedNew := isClusterManagedPod(newPod)
			if !managedOld && !managedNew {
				return false
			}
			if oldPod.DeletionTimestamp == nil && newPod.DeletionTimestamp != nil {
				return true
			}
			if IsPodReady(oldPod) != IsPodReady(newPod) {
				return true
			}
			if oldPod.Status.PodIP != newPod.Status.PodIP {
				return true
			}
			if oldPod.Status.Phase != newPod.Status.Phase {
				return true
			}
			if operatorLabelChanged(oldPod.Labels, newPod.Labels) {
				return true
			}
			if operatorAnnotationChanged(oldPod.Annotations, newPod.Annotations) {
				return true
			}
			return false
		},
	}
}

// PodToClusterRequests maps a Pod event to the KeyValCluster reconcile request.
func PodToClusterRequests(_ context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	if nn, ok := clusterNamespacedName(pod); ok {
		return []reconcile.Request{{NamespacedName: nn}}
	}
	return nil
}

func isClusterManagedPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if name := pod.Labels[core.LabelClusterKey]; name != "" {
		return labelsMatchCluster(pod.Labels, name)
	}
	for _, ref := range pod.OwnerReferences {
		if ref.Kind != "StatefulSet" || ref.Name == "" {
			continue
		}
		if cluster, ok := clusterNameFromStatefulSet(ref.Name); ok && labelsMatchCluster(pod.Labels, cluster) {
			return true
		}
	}
	return false
}

func operatorLabelChanged(oldLabels, newLabels map[string]string) bool {
	keys := []string{core.RoleLabelKey, core.LabelClusterKey, core.LabelAppKey}
	for _, key := range keys {
		if valueForLabel(oldLabels, key) != valueForLabel(newLabels, key) {
			return true
		}
	}
	return false
}

func operatorAnnotationChanged(oldAnn, newAnn map[string]string) bool {
	return valueForLabel(oldAnn, resources.ConfigHashAnnotationKey) != valueForLabel(newAnn, resources.ConfigHashAnnotationKey)
}

func valueForLabel(m map[string]string, key string) string {
	if m == nil {
		return ""
	}
	return m[key]
}

func clusterNamespacedName(pod *corev1.Pod) (types.NamespacedName, bool) {
	if pod == nil {
		return types.NamespacedName{}, false
	}
	if name := pod.Labels[core.LabelClusterKey]; name != "" && labelsMatchCluster(pod.Labels, name) {
		return types.NamespacedName{Namespace: pod.Namespace, Name: name}, true
	}
	for _, ref := range pod.OwnerReferences {
		if ref.Kind != "StatefulSet" || ref.Name == "" {
			continue
		}
		if cluster, ok := clusterNameFromStatefulSet(ref.Name); ok && labelsMatchCluster(pod.Labels, cluster) {
			return types.NamespacedName{Namespace: pod.Namespace, Name: cluster}, true
		}
	}
	return types.NamespacedName{}, false
}

func clusterNameFromStatefulSet(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	if strings.HasSuffix(name, "-sentinel") {
		base := strings.TrimSuffix(name, "-sentinel")
		if base == "" {
			return "", false
		}
		return base, true
	}
	return name, true
}

func labelsMatchCluster(labels map[string]string, cluster string) bool {
	if labels == nil {
		return false
	}
	if labels[core.LabelClusterKey] == cluster {
		return true
	}
	app := labels[core.LabelAppKey]
	if app == core.AppLabelForName(cluster) || app == core.SentinelAppLabelForName(cluster) {
		return true
	}
	return false
}
