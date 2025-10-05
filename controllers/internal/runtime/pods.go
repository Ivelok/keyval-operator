package runtime

import (
	"context"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ListStatefulSetPods lists Pods owned by the StatefulSet via selector.
func ListStatefulSetPods(ctx context.Context, c client.Client, ss *appsv1.StatefulSet) ([]corev1.Pod, error) {
	var list corev1.PodList
	sel := labels.SelectorFromSet(ss.Spec.Selector.MatchLabels)
	if err := c.List(ctx, &list, &client.ListOptions{Namespace: ss.Namespace, LabelSelector: sel}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// IsPodReady returns true when the Pod has Ready condition set to True.
func IsPodReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// SortedPodsByName returns a copy of pods sorted by name ascending.
func SortedPodsByName(pods []corev1.Pod) []corev1.Pod {
	out := make([]corev1.Pod, len(pods))
	copy(out, pods)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
