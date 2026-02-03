package phases

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/internal/ssa"
	"github.com/ivelok/keyval-operator/controllers/logging"
	"k8s.io/client-go/tools/record"
)

// ServiceLifecycleOptions controls how a Service should be reconciled.
type ServiceLifecycleOptions struct {
	Enabled           bool
	PreserveClusterIP bool
	ServiceType       string
}

// ServiceDependencies bundles shared dependencies for service reconciliation.
type ServiceDependencies struct {
	Client   client.Client
	Recorder record.EventRecorder
	Scheme   *runtime.Scheme
}

// ReconcileServiceLifecycle ensures the desired Service state matches cluster intent, mirroring the
// legacy controller behaviour.
func ReconcileServiceLifecycle(ctx context.Context, deps ServiceDependencies, cr *keyvalv1alpha1.KeyValCluster, desired *corev1.Service, opts ServiceLifecycleOptions, logger logging.Logger) error {
	if desired == nil || cr == nil {
		return nil
	}
	if deps.Client == nil {
		return controllererrors.WrapTransient(fmt.Errorf("service lifecycle missing client"))
	}
	if deps.Scheme == nil {
		return controllererrors.WrapTransient(fmt.Errorf("service lifecycle missing scheme"))
	}

	serviceType := opts.ServiceType
	if serviceType == "" {
		serviceType = "unknown"
	}
	svcLogger := logger.WithValues("component", "service", "serviceType", serviceType, "service", desired.Name)

	key := client.ObjectKey{Namespace: desired.Namespace, Name: desired.Name}
	var existing corev1.Service
	existingFound := false
	if err := deps.Client.Get(ctx, key, &existing); err == nil {
		existingFound = true
	} else if !apierrors.IsNotFound(err) {
		return controllererrors.WrapTransient(controllererrors.WrapKubeAPI(fmt.Errorf("get %s service: %w", serviceType, err)))
	}

	if !opts.Enabled {
		if !existingFound {
			return nil
		}
		if existing.DeletionTimestamp != nil {
			return nil
		}
		if !metav1.IsControlledBy(&existing, cr) {
			svcLogger.Info("service exists without operator ownership, skip removal")
			return nil
		}
		policy := metav1.DeletePropagationForeground
		if err := deps.Client.Delete(ctx, &existing, client.PropagationPolicy(policy)); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return controllererrors.WrapTransient(controllererrors.WrapKubeAPI(fmt.Errorf("delete %s service: %w", serviceType, err)))
		}
		svcLogger.Info("service disabled, initiating delete")
		opobs.EventServiceRemoved(deps.Recorder, cr, serviceType, existing.Name)
		opobs.IncServiceRemoved(cr, serviceType)
		return nil
	}

	if existingFound {
		if field, detail, changed := detectImmutableServiceChange(&existing, desired); changed {
			svcLogger.Info("immutable service field change detected, recreating service", "field", field, "detail", detail)
			opobs.EventServiceImmutableField(deps.Recorder, cr, serviceType, desired.Name, field, detail)
			opobs.IncServiceImmutableChange(cr, serviceType, field)
			if !metav1.IsControlledBy(&existing, cr) {
				svcLogger.Info("service exists without operator ownership, skip recreate")
				return nil
			}
			if existing.DeletionTimestamp != nil {
				svcLogger.Info("service deletion already in progress, waiting for completion")
				return nil
			}
			policy := metav1.DeletePropagationForeground
			if err := deps.Client.Delete(ctx, &existing, client.PropagationPolicy(policy)); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return controllererrors.WrapTransient(controllererrors.WrapKubeAPI(fmt.Errorf("delete %s service for recreate: %w", serviceType, err)))
			}
			svcLogger.Info("service deleted to apply immutable change")
			return nil
		}
	}

	if opts.PreserveClusterIP && existingFound {
		copyImmutableServiceFields(&existing, desired)
	}
	applySvc := ssa.Service(desired)
	if err := controllerutil.SetControllerReference(cr, applySvc, deps.Scheme); err != nil {
		return controllererrors.WrapTransient(fmt.Errorf("set owner on %s service: %w", serviceType, err))
	}
	if err := deps.Client.Patch(ctx, applySvc, client.Apply, client.FieldOwner(core.FieldOwner)); err != nil {
		return controllererrors.WrapTransient(controllererrors.WrapKubeAPI(fmt.Errorf("apply %s service: %w", serviceType, err)))
	}
	return nil
}

func detectImmutableServiceChange(existing, desired *corev1.Service) (field, detail string, changed bool) {
	if existing == nil || desired == nil {
		return "", "", false
	}
	if desired.Spec.ClusterIP != "" && existing.Spec.ClusterIP != "" && existing.Spec.ClusterIP != desired.Spec.ClusterIP {
		return "spec.clusterIP", fmt.Sprintf("existing=%s desired=%s", existing.Spec.ClusterIP, desired.Spec.ClusterIP), true
	}
	if len(desired.Spec.ClusterIPs) > 0 && !stringSlicesEqual(existing.Spec.ClusterIPs, desired.Spec.ClusterIPs) {
		return "spec.clusterIPs", fmt.Sprintf("existing=%v desired=%v", existing.Spec.ClusterIPs, desired.Spec.ClusterIPs), true
	}
	if len(desired.Spec.IPFamilies) > 0 && !ipFamilySlicesEqual(existing.Spec.IPFamilies, desired.Spec.IPFamilies) {
		return "spec.ipFamilies", fmt.Sprintf("existing=%v desired=%v", existing.Spec.IPFamilies, desired.Spec.IPFamilies), true
	}
	if desired.Spec.IPFamilyPolicy != nil {
		if existing.Spec.IPFamilyPolicy == nil || *existing.Spec.IPFamilyPolicy != *desired.Spec.IPFamilyPolicy {
			return "spec.ipFamilyPolicy", describePolicyChange(existing.Spec.IPFamilyPolicy, desired.Spec.IPFamilyPolicy), true
		}
	}
	for _, port := range desired.Spec.Ports {
		if port.NodePort == 0 {
			continue
		}
		if existingPort, ok := findServicePort(existing.Spec.Ports, port.Name, port.Port); ok {
			if existingPort.NodePort != 0 && existingPort.NodePort != port.NodePort {
				return fmt.Sprintf("spec.ports[%s].nodePort", port.Name), fmt.Sprintf("existing=%d desired=%d", existingPort.NodePort, port.NodePort), true
			}
		}
	}
	return "", "", false
}

func copyImmutableServiceFields(existing, desired *corev1.Service) {
	if existing == nil || desired == nil {
		return
	}
	desired.Spec.ClusterIP = existing.Spec.ClusterIP
	desired.Spec.ClusterIPs = append([]string(nil), existing.Spec.ClusterIPs...)
	desired.Spec.IPFamilies = append([]corev1.IPFamily(nil), existing.Spec.IPFamilies...)
	if existing.Spec.IPFamilyPolicy != nil {
		policy := *existing.Spec.IPFamilyPolicy
		desired.Spec.IPFamilyPolicy = &policy
	}
	for i := range desired.Spec.Ports {
		if desired.Spec.Ports[i].NodePort != 0 {
			continue
		}
		if existingPort, ok := findServicePort(existing.Spec.Ports, desired.Spec.Ports[i].Name, desired.Spec.Ports[i].Port); ok {
			desired.Spec.Ports[i].NodePort = existingPort.NodePort
		}
	}
}

func findServicePort(ports []corev1.ServicePort, name string, port int32) (corev1.ServicePort, bool) {
	for i := range ports {
		if ports[i].Name == name && ports[i].Port == port {
			return ports[i], true
		}
	}
	return corev1.ServicePort{}, false
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ipFamilySlicesEqual(a, b []corev1.IPFamily) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func describePolicyChange(existing, desired *corev1.IPFamilyPolicyType) string {
	existingVal := "<nil>"
	desiredVal := "<nil>"
	if existing != nil {
		existingVal = string(*existing)
	}
	if desired != nil {
		desiredVal = string(*desired)
	}
	return fmt.Sprintf("existing=%s desired=%s", existingVal, desiredVal)
}
