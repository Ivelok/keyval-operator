package resources

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func HeadlessService(cr *keyvalv1alpha1.KeyValCluster) *corev1.Service {
	name := core.HeadlessName(cr)
	labels := core.LabelsFor(cr)
	publish := true
	if cr.Spec.Service != nil && cr.Spec.Service.PublishNotReadyAddresses != nil {
		publish = *cr.Spec.Service.PublishNotReadyAddresses
	}
	rport, _ := core.RedisPort(cr)
	svcLabels := mergeServiceLabels(labels, nil)
	annotations := map[string]string(nil)
	if cr.Spec.Service != nil {
		if cr.Spec.Service.Ports != nil && cr.Spec.Service.Ports.Redis != 0 {
			rport = int(cr.Spec.Service.Ports.Redis)
		}
		svcLabels = mergeServiceLabels(labels, cr.Spec.Service.Labels)
		annotations = copyStringMap(cr.Spec.Service.Annotations)
	}
	ports := []corev1.ServicePort{
		{Name: "redis", Port: int32(rport), TargetPort: intstr.FromInt(rport)},
	}
	if metricsEnabled(cr) {
		mport, _ := metricsExporterPort(cr)
		ports = append(ports, corev1.ServicePort{Name: "http-metrics", Port: mport, TargetPort: intstr.FromInt(int(mport))})
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   cr.Namespace,
			Labels:      svcLabels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: publish,
			Selector: map[string]string{
				core.LabelAppKey: labels[core.LabelAppKey],
			},
			Ports: ports,
		},
	}
}

func MasterService(cr *keyvalv1alpha1.KeyValCluster) *corev1.Service {
	name := core.MasterServiceName(cr)
	labels := core.LabelsFor(cr)
	rport, _ := core.RedisPort(cr)
	svcType := corev1.ServiceTypeClusterIP
	annotations := map[string]string(nil)
	svcLabels := mergeServiceLabels(labels, nil)
	if spec := cr.Spec.Service; spec != nil {
		if spec.Ports != nil && spec.Ports.Redis != 0 {
			rport = int(spec.Ports.Redis)
		}
		if spec.Type != "" {
			svcType = spec.Type
		}
		svcLabels = mergeServiceLabels(labels, spec.Labels)
		annotations = copyStringMap(spec.Annotations)
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   cr.Namespace,
			Labels:      svcLabels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type: svcType,
			Selector: map[string]string{
				core.LabelAppKey:  labels[core.LabelAppKey],
				core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster),
			},
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: int32(rport), TargetPort: intstr.FromInt(rport)},
			},
		},
	}
}

func ReplicasService(cr *keyvalv1alpha1.KeyValCluster) *corev1.Service {
	name := core.ReplicasServiceName(cr)
	labels := core.LabelsFor(cr)
	rport, _ := core.RedisPort(cr)
	svcType := corev1.ServiceTypeClusterIP
	annotations := map[string]string(nil)
	svcLabels := mergeServiceLabels(labels, nil)
	if spec := cr.Spec.ReplicasService; spec != nil {
		if spec.Ports != nil && spec.Ports.Redis != 0 {
			rport = int(spec.Ports.Redis)
		}
		if spec.Type != "" {
			svcType = spec.Type
		}
		svcLabels = mergeServiceLabels(labels, spec.Labels)
		annotations = copyStringMap(spec.Annotations)
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   cr.Namespace,
			Labels:      svcLabels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type: svcType,
			Selector: map[string]string{
				core.LabelAppKey:  labels[core.LabelAppKey],
				core.RoleLabelKey: string(keyvalv1alpha1.PodRoleReplica),
			},
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: int32(rport), TargetPort: intstr.FromInt(rport)},
			},
		},
	}
}

func SentinelService(cr *keyvalv1alpha1.KeyValCluster) *corev1.Service {
	name := core.SentinelServiceName(cr)
	labels := core.SentinelLabelsFor(cr)
	sport, _ := core.SentinelPort(cr)
	svcType := corev1.ServiceTypeClusterIP
	annotations := map[string]string(nil)
	svcLabels := mergeServiceLabels(labels, nil)
	if spec := cr.Spec.SentinelService; spec != nil {
		if spec.Ports != nil && spec.Ports.Sentinel != 0 {
			sport = int(spec.Ports.Sentinel)
		}
		if spec.Type != "" {
			svcType = spec.Type
		}
		svcLabels = mergeServiceLabels(labels, spec.Labels)
		annotations = copyStringMap(spec.Annotations)
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   cr.Namespace,
			Labels:      svcLabels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type: svcType,
			Selector: map[string]string{
				core.LabelAppKey: labels[core.LabelAppKey],
			},
			Ports: []corev1.ServicePort{
				{Name: "sentinel", Port: int32(sport), TargetPort: intstr.FromInt(sport)},
			},
		},
	}
}

func SentinelHeadlessService(cr *keyvalv1alpha1.KeyValCluster) *corev1.Service {
	name := core.SentinelHeadlessName(cr)
	labels := core.SentinelLabelsFor(cr)
	sport, _ := core.SentinelPort(cr)
	publish := true
	svcLabels := mergeServiceLabels(labels, nil)
	annotations := map[string]string(nil)
	if spec := cr.Spec.SentinelService; spec != nil {
		if spec.PublishNotReadyAddresses != nil {
			publish = *spec.PublishNotReadyAddresses
		}
		svcLabels = mergeServiceLabels(labels, spec.Labels)
		annotations = copyStringMap(spec.Annotations)
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   cr.Namespace,
			Labels:      svcLabels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: publish,
			Selector: map[string]string{
				core.LabelAppKey: labels[core.LabelAppKey],
			},
			Ports: []corev1.ServicePort{
				{Name: "sentinel", Port: int32(sport), TargetPort: intstr.FromInt(sport)},
			},
		},
	}
}

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

func mergeServiceLabels(base, extra map[string]string) map[string]string {
	out := copyStringMap(base)
	if out == nil {
		out = map[string]string{}
	}
	for k, v := range extra {
		if _, exists := base[k]; exists {
			continue
		}
		out[k] = v
	}
	return out
}
