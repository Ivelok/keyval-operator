package ssa

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConfigMap returns a ConfigMap containing only operator-owned fields for SSA patches.
func ConfigMap(src *corev1.ConfigMap) *corev1.ConfigMap {
	if src == nil {
		return nil
	}
	out := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        src.Name,
			Namespace:   src.Namespace,
			Labels:      copyStringMap(src.Labels),
			Annotations: copyStringMap(src.Annotations),
		},
		Data: map[string]string{},
	}
	for k, v := range src.Data {
		out.Data[k] = v
	}
	if len(src.BinaryData) > 0 {
		out.BinaryData = make(map[string][]byte, len(src.BinaryData))
		for k, v := range src.BinaryData {
			copied := make([]byte, len(v))
			copy(copied, v)
			out.BinaryData[k] = copied
		}
	}
	return out
}
