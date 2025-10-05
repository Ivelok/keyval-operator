package ssa

import (
	corev1 "k8s.io/api/core/v1"
)

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyServicePorts(in []corev1.ServicePort) []corev1.ServicePort {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.ServicePort, len(in))
	copy(out, in)
	return out
}
