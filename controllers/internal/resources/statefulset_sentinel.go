package resources

import (
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func SentinelStatefulSet(cr *keyvalv1alpha1.KeyValCluster, configHash, tlsHash string, sec *security.Settings) *appsv1.StatefulSet {
	labels := core.SentinelLabelsFor(cr)
	replicas := int32(0)
	if cr.Spec.SentinelCount != nil {
		replicas = *cr.Spec.SentinelCount
	}

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-sentinel",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: core.SentinelHeadlessName(cr),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				core.LabelAppKey:     labels[core.LabelAppKey],
				core.LabelClusterKey: labels[core.LabelClusterKey],
			}},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: func() map[string]string {
						m := map[string]string{}
						for k, v := range labels {
							m[k] = v
						}
						m[core.RoleLabelKey] = string(keyvalv1alpha1.PodRoleSentinel)
						if cr.Spec.SentinelPod != nil && cr.Spec.SentinelPod.Labels != nil {
							for k, v := range cr.Spec.SentinelPod.Labels {
								m[k] = v
							}
						}
						return m
					}(),
					Annotations: func() map[string]string {
						if cr.Spec.SentinelPod != nil && cr.Spec.SentinelPod.Annotations != nil {
							out := map[string]string{}
							for k, v := range cr.Spec.SentinelPod.Annotations {
								out[k] = v
							}
							return out
						}
						return nil
					}(),
				},
				Spec: buildSentinelPodSpec(cr, sec),
			},
		},
	}

	if configHash != "" {
		if ss.Spec.Template.Annotations == nil {
			ss.Spec.Template.Annotations = map[string]string{}
		}
		ss.Spec.Template.Annotations[ConfigHashAnnotationKey] = configHash
	}
	if tlsHash != "" {
		if ss.Spec.Template.Annotations == nil {
			ss.Spec.Template.Annotations = map[string]string{}
		}
		ss.Spec.Template.Annotations[TLSSecretHashAnnotationKey] = tlsHash
	}

	applyTopologySettings(cr, labels[core.LabelAppKey], &ss.Spec.Template)

	return ss
}

func buildSentinelPodSpec(cr *keyvalv1alpha1.KeyValCluster, sec *security.Settings) corev1.PodSpec {
	image := core.ResolveImage(cr.Spec)
	if cr.Spec.SentinelImage != nil && *cr.Spec.SentinelImage != "" {
		image = *cr.Spec.SentinelImage
	}
	resources := DesiredSentinelResources(cr)
	sport, _ := core.SentinelPort(cr)

	spec := corev1.PodSpec{
		InitContainers: []corev1.Container{
			{
				Name:    "init-copy-config",
				Image:   "busybox:1.36",
				Command: []string{"sh", "-c"},
				Args:    []string{"cp /conf-src/sentinel.conf /conf/sentinel.conf && chmod 0644 /conf/sentinel.conf"},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "config-src", MountPath: "/conf-src"},
					{Name: "config", MountPath: "/conf"},
				},
				SecurityContext: containerSecurityContext(false),
			},
		},
		Containers: []corev1.Container{
			{
				Name:            core.SentinelContainerName,
				Image:           image,
				Command:         core.SentinelCommandForEngine(core.EffectiveSentinelEngine(cr.Spec)),
				Args:            []string{"/conf/sentinel.conf"},
				Ports:           []corev1.ContainerPort{{Name: "sentinel", ContainerPort: int32(sport)}},
				VolumeMounts:    []corev1.VolumeMount{{Name: "config", MountPath: "/conf"}, {Name: core.DataVolumeName, MountPath: "/data"}},
				Resources:       resources,
				SecurityContext: containerSecurityContext(true),
			},
		},
		Volumes: []corev1.Volume{
			{Name: "config-src", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: core.ConfigMapName(cr)}}}},
			{Name: "config", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: core.DataVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		},
	}

	selector := map[string]string{}
	if cr.Spec.SentinelPod != nil {
		if cr.Spec.SentinelPod.NodeSelector != nil {
			for k, v := range cr.Spec.SentinelPod.NodeSelector {
				selector[k] = v
			}
		}
		spec.Tolerations = cr.Spec.SentinelPod.Tolerations
		if cr.Spec.SentinelPod.Affinity != nil {
			spec.Affinity = cr.Spec.SentinelPod.Affinity.DeepCopy()
		}
	}
	if len(selector) == 0 {
		spec.NodeSelector = map[string]string{}
	} else {
		spec.NodeSelector = selector
	}

	spec.SecurityContext = podSecurityContext()
	grace := int64(25)
	spec.TerminationGracePeriodSeconds = &grace
	probeCmd := fmt.Sprintf("redis-cli -p %d PING", sport)
	if sec != nil && sec.TLS.Enabled {
		probeCmd = fmt.Sprintf("redis-cli --tls --cacert \"$TLS_CA_FILE\" --cert \"$TLS_CERT_FILE\" --key \"$TLS_KEY_FILE\" -p %d PING", sport)
	}
	spec.Containers[0].LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"sh", "-c", probeCmd}}}}
	spec.Containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"sh", "-c", probeCmd}}}}
	spec.Containers[0].Env = append(spec.Containers[0].Env, corev1.EnvVar{Name: "REDIS_PORT", Value: strconv.Itoa(sport)})
	spec.Containers[0].Lifecycle = redisLifecycle()
	if sec != nil && sec.TLS.Enabled {
		spec.Volumes = append(spec.Volumes, corev1.Volume{
			Name: TLSVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: sec.TLS.SecretName},
			},
		})
		mount := corev1.VolumeMount{Name: TLSVolumeName, MountPath: TLSMountPath, ReadOnly: true}
		spec.InitContainers[0].VolumeMounts = append(spec.InitContainers[0].VolumeMounts, mount)
		spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts, mount)
		tlsEnv := redisTLSEnvs(sec)
		spec.InitContainers[0].Env = append(spec.InitContainers[0].Env, tlsEnv...)
		spec.Containers[0].Env = append(spec.Containers[0].Env, tlsEnv...)
	}
	return spec
}

// DesiredSentinelResources returns the resource requirements that should be applied to Sentinel pods.
// When spec.sentinelResources is provided, it takes precedence. Otherwise, dedicated defaults are used
// instead of inheriting Redis resources, ensuring Sentinel can be tuned independently.
func DesiredSentinelResources(cr *keyvalv1alpha1.KeyValCluster) corev1.ResourceRequirements {
	if cr.Spec.SentinelResources != nil {
		return ValueOrEmptyResources(cr.Spec.SentinelResources)
	}
	if cr.Spec.Mode != keyvalv1alpha1.ModeSentinel {
		return corev1.ResourceRequirements{}
	}
	return defaultSentinelResources()
}

func defaultSentinelResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
}
