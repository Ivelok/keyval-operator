package resources

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

const (
	defaultMetricsPort  int32  = 9121
	defaultMetricsImage string = "ghcr.io/oliver006/redis_exporter:v1.75.0"
)

func metricsEnabled(cr *keyvalv1alpha1.KeyValCluster) bool {
	if cr.Spec.Metrics == nil || cr.Spec.Metrics.Enabled == nil {
		return true
	}
	return *cr.Spec.Metrics.Enabled
}

// MetricsEnabled reports whether metrics are enabled for the cluster.
func MetricsEnabled(cr *keyvalv1alpha1.KeyValCluster) bool {
	return metricsEnabled(cr)
}

func metricsExporterImage(cr *keyvalv1alpha1.KeyValCluster) string {
	if cr.Spec.Metrics != nil && cr.Spec.Metrics.Image != "" {
		return cr.Spec.Metrics.Image
	}
	return defaultMetricsImage
}

// MetricsExporterImage resolves the desired metrics exporter image.
func MetricsExporterImage(cr *keyvalv1alpha1.KeyValCluster) string {
	return metricsExporterImage(cr)
}

func metricsExporterPort(cr *keyvalv1alpha1.KeyValCluster) (int32, string) {
	port := defaultMetricsPort
	if cr.Spec.Metrics != nil && cr.Spec.Metrics.Port != 0 {
		port = cr.Spec.Metrics.Port
	}
	return port, fmt.Sprintf("%d", port)
}

// MetricsExporterPort resolves the desired listen port for the metrics exporter.
func MetricsExporterPort(cr *keyvalv1alpha1.KeyValCluster) int32 {
	port, _ := metricsExporterPort(cr)
	return port
}

func desiredMetricsResources(cr *keyvalv1alpha1.KeyValCluster) corev1.ResourceRequirements {
	if cr.Spec.Metrics != nil && cr.Spec.Metrics.Resources != nil {
		return ValueOrEmptyResources(cr.Spec.Metrics.Resources)
	}
	return defaultMetricsResources()
}

// DesiredMetricsResources returns the desired resource requirements for the exporter.
func DesiredMetricsResources(cr *keyvalv1alpha1.KeyValCluster) corev1.ResourceRequirements {
	return desiredMetricsResources(cr)
}

func defaultMetricsResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("20m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
	}
}
