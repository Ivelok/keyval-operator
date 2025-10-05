package resources

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

var (
	defaultRunAsUser  int64 = 1000
	defaultRunAsGroup int64 = 1000
)

func podSecurityContext() *corev1.PodSecurityContext {
	runAsNonRoot := true
	fsGroupChange := corev1.FSGroupChangeOnRootMismatch
	return &corev1.PodSecurityContext{
		RunAsNonRoot:        &runAsNonRoot,
		RunAsUser:           &defaultRunAsUser,
		RunAsGroup:          &defaultRunAsGroup,
		FSGroup:             &defaultRunAsGroup,
		FSGroupChangePolicy: &fsGroupChange,
	}
}

func containerSecurityContext(readOnly bool) *corev1.SecurityContext {
	allowPrivEsc := false
	sc := &corev1.SecurityContext{
		AllowPrivilegeEscalation: &allowPrivEsc,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		RunAsNonRoot:             ptr.To(true),
	}
	if readOnly {
		sc.ReadOnlyRootFilesystem = ptr.To(true)
	}
	return sc
}
