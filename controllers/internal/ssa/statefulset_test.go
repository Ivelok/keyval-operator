package ssa

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestStatefulSetTrim(t *testing.T) {
	replicas := int32(3)
	source := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis",
			Namespace: "ns",
			Labels:    map[string]string{"app": "redis"},
			Annotations: map[string]string{
				"managed": "true",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "redis-headless",
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "redis"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "redis", Image: "redis:7"}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
		},
	}
	source.Status.ReadyReplicas = 2

	result := StatefulSet(source)

	if result.APIVersion != "apps/v1" || result.Kind != "StatefulSet" {
		t.Fatalf("unexpected typemeta: %s %s", result.APIVersion, result.Kind)
	}
	if result.Spec.Replicas == nil || *result.Spec.Replicas != replicas {
		t.Fatalf("replicas not copied: %#v", result.Spec.Replicas)
	}
	if len(result.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("volume claim templates missing")
	}
	result.Spec.Template.Labels["new"] = "value"
	if _, ok := source.Spec.Template.Labels["new"]; ok {
		t.Fatalf("template labels should be deep copied")
	}
	if result.Status.ReadyReplicas != 0 {
		t.Fatalf("status should not be copied")
	}
}
