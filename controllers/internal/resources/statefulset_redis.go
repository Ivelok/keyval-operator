package resources

import (
	"fmt"
	"path"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func StatefulSet(cr *keyvalv1alpha1.KeyValCluster, configHash string, tlsHash string, sec *security.Settings) *appsv1.StatefulSet {
	labels := map[string]string{
		core.LabelAppKey:     core.AppLabel(cr),
		core.LabelClusterKey: cr.Name,
	}

	replicas := cr.Spec.RedisReplicas
	if replicas < 1 {
		replicas = 1
	}

	tmplLabels := map[string]string{}
	for k, v := range labels {
		tmplLabels[k] = v
	}
	if len(cr.Spec.PodLabels) > 0 {
		for k, v := range cr.Spec.PodLabels {
			if _, exists := labels[k]; exists {
				continue
			}
			tmplLabels[k] = v
		}
	}

	var tmplAnnotations map[string]string
	if len(cr.Spec.PodAnnotations) > 0 {
		tmplAnnotations = map[string]string{}
		for k, v := range cr.Spec.PodAnnotations {
			tmplAnnotations[k] = v
		}
	}

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: core.HeadlessName(cr),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					core.LabelAppKey:     labels[core.LabelAppKey],
					core.LabelClusterKey: labels[core.LabelClusterKey],
				},
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      tmplLabels,
					Annotations: tmplAnnotations,
				},
				Spec: buildPodSpec(cr, sec),
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

	if core.HasPersistentData(cr) {
		size := core.DesiredStorageQuantity(cr)
		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: core.DataVolumeName,
				Labels: map[string]string{
					core.LabelAppKey:     labels[core.LabelAppKey],
					core.LabelClusterKey: labels[core.LabelClusterKey],
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: core.StorageAccessModes(cr),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: size},
				},
			},
		}
		if sc := storageClassName(cr); sc != nil {
			pvc.Spec.StorageClassName = sc
		}
		ss.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}
	} else {
		v := corev1.Volume{
			Name: core.DataVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}
		ss.Spec.Template.Spec.Volumes = append(ss.Spec.Template.Spec.Volumes, v)
	}

	ss.Spec.Template.Spec.Volumes = append(ss.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: core.ConfigMapName(cr)}},
		},
	})

	applyTopologySettings(cr, labels[core.LabelAppKey], &ss.Spec.Template)

	return ss
}

func buildPodSpec(cr *keyvalv1alpha1.KeyValCluster, sec *security.Settings) corev1.PodSpec {
	rport, rportStr := core.RedisPort(cr)
	mport, _ := metricsExporterPort(cr)
	metricsOn := metricsEnabled(cr)
	redisContainer := corev1.Container{
		Name:    core.RedisContainerName,
		Image:   core.ResolveImage(cr.Spec),
		Command: core.ServerCommandForEngine(core.EffectiveServerEngine(cr.Spec)),
		Args:    []string{"/runtime-conf/redis.conf"},
		Ports:   []corev1.ContainerPort{{Name: "redis", ContainerPort: int32(rport)}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: core.DataVolumeName, MountPath: "/data"},
			{Name: "config", MountPath: "/conf"},
			{Name: core.RuntimeConfigVolumeName, MountPath: "/runtime-conf"},
		},
		Resources:       ValueOrEmptyResources(cr.Spec.Resources),
		SecurityContext: containerSecurityContext(true),
	}
	redisContainer.Env = append(redisContainer.Env, corev1.EnvVar{Name: "REDIS_PORT", Value: rportStr})
	redisContainer.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"sh", LivenessScriptPath}}}}
	redisContainer.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"sh", ReadinessScriptPath}}}}
	containers := []corev1.Container{redisContainer}
	if metricsOn {
		containers = append(containers, buildMetricsContainer(cr, sec, rportStr, mport))
	}
	init := bootstrapInitContainer(cr, rportStr)
	spec := corev1.PodSpec{
		InitContainers: []corev1.Container{init},
		Containers:     containers,
		Volumes: []corev1.Volume{
			{Name: core.RuntimeConfigVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		},
	}
	spec.SecurityContext = podSecurityContext()
	grace := int64(25)
	spec.TerminationGracePeriodSeconds = &grace

	if sec != nil && sec.Auth.Enabled {
		redEnv := redisAuthEnvs(sec)
		spec.InitContainers[0].Env = append(spec.InitContainers[0].Env, redEnv...)
		spec.Containers[0].Env = append(spec.Containers[0].Env, redEnv...)
	}

	if sec != nil && sec.TLS.Enabled {
		volume := corev1.Volume{
			Name: TLSVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: sec.TLS.SecretName},
			},
		}
		spec.Volumes = append(spec.Volumes, volume)
		mount := corev1.VolumeMount{Name: TLSVolumeName, MountPath: TLSMountPath, ReadOnly: true}
		spec.InitContainers[0].VolumeMounts = append(spec.InitContainers[0].VolumeMounts, mount)
		spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts, mount)
		tlsEnv := redisTLSEnvs(sec)
		spec.InitContainers[0].Env = append(spec.InitContainers[0].Env, tlsEnv...)
		spec.Containers[0].Env = append(spec.Containers[0].Env, tlsEnv...)
	}

	spec.Containers[0].Lifecycle = redisLifecycle()

	return spec
}

func bootstrapInitContainer(cr *keyvalv1alpha1.KeyValCluster, redisPort string) corev1.Container {
	image := core.ResolveImage(cr.Spec)
	sentinelSvc := ""
	if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
		sentinelSvc = fmt.Sprintf("%s.%s.svc", core.SentinelServiceName(cr), cr.Namespace)
	}
	_, sentinelPortStr := core.SentinelPort(cr)
	masterSvc := fmt.Sprintf("%s.%s.svc", core.MasterServiceName(cr), cr.Namespace)
	seedHost := core.PodFQDN(cr, 0)
	headless := core.HeadlessName(cr)
	annotationForceMaster := fmt.Sprintf("metadata.annotations['%s']", core.AnnotationForceMaster)

	return corev1.Container{
		Name:    core.BootstrapInitContainerName,
		Image:   image,
		Command: []string{"sh", BootstrapScriptPath},
		Env: []corev1.EnvVar{
			{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
			{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
			{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
			{Name: "CLUSTER_MODE", Value: string(cr.Spec.Mode)},
			{Name: "HEADLESS_SERVICE", Value: headless},
			{Name: "SEED_HOST", Value: seedHost},
			{Name: "REDIS_PORT", Value: redisPort},
			{Name: "SENTINEL_SVC", Value: sentinelSvc},
			{Name: "SENTINEL_PORT", Value: sentinelPortStr},
			{Name: "MONITOR_NAME", Value: cr.Name},
			{Name: "MASTER_SERVICE_HOST", Value: masterSvc},
			{Name: "FORCE_MASTER", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: annotationForceMaster}}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: core.RuntimeConfigVolumeName, MountPath: "/runtime-conf"},
			{Name: "config", MountPath: "/conf", ReadOnly: true},
		},
		SecurityContext: containerSecurityContext(false),
	}
}

func buildMetricsContainer(cr *keyvalv1alpha1.KeyValCluster, sec *security.Settings, redisPort string, metricsPort int32) corev1.Container {
	scheme := "redis"
	if sec != nil && sec.TLS.Enabled {
		scheme = "rediss"
	}
	args := []string{
		fmt.Sprintf("--redis.addr=%s://127.0.0.1:%s", scheme, redisPort),
		fmt.Sprintf("--web.listen-address=:%d", metricsPort),
	}
	envs := []corev1.EnvVar{
		{Name: "REDIS_ADDR", Value: fmt.Sprintf("%s://127.0.0.1:%s", scheme, redisPort)},
	}
	user, password := security.ResolveAuth(cr, sec)
	if sec != nil && sec.Auth.Enabled {
		envs = append(envs, corev1.EnvVar{
			Name: "REDIS_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: sec.Auth.SecretName},
				Key:                  sec.Auth.SecretKey,
			}},
		})
		if sec.Auth.Username != "" {
			envs = append(envs, corev1.EnvVar{Name: "REDIS_USER", Value: sec.Auth.Username})
		}
	} else {
		if password != "" {
			envs = append(envs, corev1.EnvVar{Name: "REDIS_PASSWORD", Value: password})
		}
		if user != "" {
			envs = append(envs, corev1.EnvVar{Name: "REDIS_USER", Value: user})
		}
	}
	if sec != nil && sec.TLS.Enabled {
		envs = append(envs, corev1.EnvVar{Name: "REDIS_EXPORTER_TLS_CA_CERT_FILE", Value: path.Join(TLSMountPath, sec.TLS.CACertKey)})
		if sec.TLS.RequireClientAuth {
			envs = append(envs,
				corev1.EnvVar{Name: "REDIS_EXPORTER_TLS_CLIENT_CERT_FILE", Value: path.Join(TLSMountPath, sec.TLS.CertKey)},
				corev1.EnvVar{Name: "REDIS_EXPORTER_TLS_CLIENT_KEY_FILE", Value: path.Join(TLSMountPath, sec.TLS.KeyKey)},
			)
		}
	}
	metricsContainer := corev1.Container{
		Name:            core.MetricsContainerName,
		Image:           metricsExporterImage(cr),
		Args:            args,
		Ports:           []corev1.ContainerPort{{Name: "metrics", ContainerPort: metricsPort}},
		Env:             envs,
		Resources:       desiredMetricsResources(cr),
		SecurityContext: containerSecurityContext(true),
	}
	if sec != nil && sec.TLS.Enabled {
		metricsContainer.VolumeMounts = append(metricsContainer.VolumeMounts, corev1.VolumeMount{Name: TLSVolumeName, MountPath: TLSMountPath, ReadOnly: true})
	}
	readiness := &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/metrics", Port: intstr.FromInt(int(metricsPort))}},
		InitialDelaySeconds: 5,
		TimeoutSeconds:      3,
		PeriodSeconds:       15,
		FailureThreshold:    3,
	}
	liveness := &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/metrics", Port: intstr.FromInt(int(metricsPort))}},
		InitialDelaySeconds: 15,
		TimeoutSeconds:      3,
		PeriodSeconds:       30,
		FailureThreshold:    5,
	}
	metricsContainer.ReadinessProbe = readiness
	metricsContainer.LivenessProbe = liveness
	return metricsContainer
}

func redisAuthEnvs(sec *security.Settings) []corev1.EnvVar {
	if sec == nil || !sec.Auth.Enabled {
		return nil
	}
	envs := []corev1.EnvVar{
		{
			Name: "MASTER_AUTH",
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: sec.Auth.SecretName},
				Key:                  sec.Auth.SecretKey,
			}},
		},
	}
	envs = append(envs, corev1.EnvVar{
		Name: "REDISCLI_AUTH",
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: sec.Auth.SecretName},
			Key:                  sec.Auth.SecretKey,
		}},
	})
	if sec.Auth.Username != "" {
		envs = append(envs, corev1.EnvVar{Name: "MASTER_USER", Value: sec.Auth.Username})
	}
	return envs
}

func redisTLSEnvs(sec *security.Settings) []corev1.EnvVar {
	if sec == nil || !sec.TLS.Enabled {
		return nil
	}
	return []corev1.EnvVar{
		{Name: "TLS_ENABLED", Value: "true"},
		{Name: "TLS_CA_FILE", Value: path.Join(TLSMountPath, sec.TLS.CACertKey)},
		{Name: "TLS_CERT_FILE", Value: path.Join(TLSMountPath, sec.TLS.CertKey)},
		{Name: "TLS_KEY_FILE", Value: path.Join(TLSMountPath, sec.TLS.KeyKey)},
	}
}

func redisLifecycle() *corev1.Lifecycle {
	return &corev1.Lifecycle{
		PreStop: &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", "-c", redisPreStopScript},
			},
		},
	}
}

const redisPreStopScript = `set -eu

PORT="${REDIS_PORT:-6379}"
ARGS="-h 127.0.0.1 -p ${PORT}"

if [ "${TLS_ENABLED:-false}" = "true" ]; then
  ARGS="$ARGS --tls"
  if [ -n "${TLS_CA_FILE:-}" ]; then
    ARGS="$ARGS --cacert ${TLS_CA_FILE}"
  fi
  if [ -n "${TLS_CERT_FILE:-}" ] && [ -n "${TLS_KEY_FILE:-}" ]; then
    ARGS="$ARGS --cert ${TLS_CERT_FILE} --key ${TLS_KEY_FILE}"
  fi
fi

START_NS=$(date +%s%N)
if timeout 5 redis-cli $ARGS shutdown nosave >/dev/null 2>&1; then
  END_NS=$(date +%s%N)
  DURATION_NS=$((END_NS - START_NS))
  echo "graceful_shutdown_duration_ns=${DURATION_NS}" >&2
  exit 0
fi
RC=$?
echo "RedisPreStopFailed: redis-cli shutdown failed (exit=${RC})" >&2
exit ${RC}
`

func storageClassName(cr *keyvalv1alpha1.KeyValCluster) *string {
	if cr.Spec.Storage != nil && cr.Spec.Storage.StorageClassName != nil {
		return cr.Spec.Storage.StorageClassName
	}
	return nil
}

func ValueOrEmptyResources(r *corev1.ResourceRequirements) corev1.ResourceRequirements {
	if r == nil {
		return corev1.ResourceRequirements{}
	}
	return *r
}
