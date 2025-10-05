package update

import (
	"context"
	"errors"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/eviction"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

// Plan describes which pods should be restarted and why.
type Plan struct {
	PodNames []string
	Reasons  map[string][]string
}

const (
	ComponentRedis    = "redis"
	ComponentSentinel = "sentinel"
)

// ErrEvictionRejected captures PDB rejections so callers can surface disruption blocks.
var ErrEvictionRejected = eviction.ErrRejected

// ExecuteOptions configures how Execute performs pod evictions.
type ExecuteOptions struct {
	Evictor          eviction.Runner
	EvictionSettings eviction.Settings
	Logger           logging.Logger
	Component        string
}

// PlanUpdates determines which pods require restart to pick up desired spec or config.
func PlanUpdates(_ context.Context, cr *keyvalv1alpha1.KeyValCluster, ss *appsv1.StatefulSet, pods []corev1.Pod, health map[string]keyvalv1alpha1.PodHealth, storage map[string][]string) Plan {
	desiredHash := ""
	desiredTLSHash := ""
	if ss != nil && ss.Spec.Template.Annotations != nil {
		desiredHash = ss.Spec.Template.Annotations[resources.ConfigHashAnnotationKey]
		desiredTLSHash = ss.Spec.Template.Annotations[resources.TLSSecretHashAnnotationKey]
	}
	redisImage := cr.Spec.Image
	sentinelImage := redisImage
	if cr.Spec.SentinelImage != nil && *cr.Spec.SentinelImage != "" {
		sentinelImage = *cr.Spec.SentinelImage
	}

	reasons := map[string][]string{}

	var desiredTemplate *corev1.PodTemplateSpec
	if ss != nil {
		desiredTemplate = &ss.Spec.Template
	}

	for _, p := range pods {
		var rsn []string
		if desiredHash != "" {
			podHash := ""
			if p.Annotations != nil {
				podHash = p.Annotations[resources.ConfigHashAnnotationKey]
			}
			if podHash != desiredHash {
				rsn = append(rsn, "config-hash")
			}
		}
		if desiredTLSHash != "" || (p.Annotations != nil && p.Annotations[resources.TLSSecretHashAnnotationKey] != "") {
			podTLSHash := ""
			if p.Annotations != nil {
				podTLSHash = p.Annotations[resources.TLSSecretHashAnnotationKey]
			}
			if podTLSHash != desiredTLSHash {
				rsn = append(rsn, "tls-hash")
			}
		}
		for _, c := range p.Spec.Containers {
			switch c.Name {
			case core.RedisContainerName:
				if c.Image != redisImage {
					rsn = append(rsn, "image:redis")
				}
				desired := resources.ValueOrEmptyResources(cr.Spec.Resources)
				if !eqResourceRequirements(c.Resources, desired) {
					rsn = append(rsn, "resources:redis")
				}
			case core.SentinelContainerName:
				if c.Image != sentinelImage && cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
					rsn = append(rsn, "image:sentinel")
				}
				desired := resources.DesiredSentinelResources(cr)
				if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel && !eqResourceRequirements(c.Resources, desired) {
					rsn = append(rsn, "resources:sentinel")
				}
			}
		}
		hasSidecar := false
		hasRedis := false
		for _, c := range p.Spec.Containers {
			if c.Name == core.SentinelContainerName {
				hasSidecar = true
			}
			if c.Name == core.RedisContainerName {
				hasRedis = true
			}
		}
		if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel && hasRedis && hasSidecar {
			rsn = append(rsn, "layout:sentinel-removed")
		}
		if desiredTemplate != nil {
			desiredSelector := desiredTemplate.Spec.NodeSelector
			if !equalStringMap(desiredSelector, p.Spec.NodeSelector) {
				rsn = append(rsn, "spec:node-selector")
			}
		}
		if health != nil {
			if state, ok := health[p.Name]; ok {
				switch state {
				case keyvalv1alpha1.PodHealthLagging:
					if !runtimepkg.IsPodReady(&p) {
						rsn = append(rsn, "health:lagging")
					}
				case keyvalv1alpha1.PodHealthDesynced:
					if !runtimepkg.IsPodReady(&p) {
						rsn = append(rsn, "health:desynced")
					}
				case keyvalv1alpha1.PodHealthOffline:
					if runtimepkg.IsPodReady(&p) {
						rsn = append(rsn, "health:offline")
					}
				}
			}
		}
		if len(storage) > 0 {
			if extra, ok := storage[p.Name]; ok && len(extra) > 0 {
				rsn = appendUniqueReasons(rsn, extra...)
			}
		}
		if len(rsn) > 0 {
			reasons[p.Name] = rsn
		}
	}

	desired := int(cr.Spec.RedisReplicas)
	if desired < 0 {
		desired = 0
	}
	if len(pods) > desired {
		for _, p := range pods {
			o := core.Ordinal(p.Name)
			if o >= desired && o >= 0 {
				reasons[p.Name] = append(reasons[p.Name], "scale-down")
			}
		}
	}

	names := make([]string, 0, len(reasons))
	for name := range reasons {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return core.Ordinal(names[i]) > core.Ordinal(names[j]) })

	return Plan{PodNames: names, Reasons: reasons}
}

func appendUniqueReasons(dst []string, extras ...string) []string {
	if len(extras) == 0 {
		return dst
	}
	existing := make(map[string]struct{}, len(dst))
	for _, r := range dst {
		existing[r] = struct{}{}
	}
	for _, extra := range extras {
		if extra == "" {
			continue
		}
		if _, ok := existing[extra]; ok {
			continue
		}
		dst = append(dst, extra)
		existing[extra] = struct{}{}
	}
	return dst
}

func hasReasonForPod(plan Plan, podName, reason string) bool {
	reasons, ok := plan.Reasons[podName]
	if !ok {
		return false
	}
	for _, r := range reasons {
		if r == reason {
			return true
		}
	}
	return false
}

func equalStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			return false
		}
	}
	return true
}

func Execute(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, plan Plan, pods []corev1.Pod, health map[string]keyvalv1alpha1.PodHealth, opts ExecuteOptions) (string, error) {
	component := opts.Component
	if component == "" {
		component = ComponentRedis
	}

	logger := opts.Logger
	if logger.IsZero() {
		logger = logging.New(nil)
	}
	planLogger := logger.WithValues("component", component)

	if len(plan.PodNames) == 0 {
		planLogger.V(1).Info("rolling update plan empty")
		return "", nil
	}

	planLogger.Info("rolling update plan", "candidates", plan.PodNames, "reasons", plan.Reasons)

	evictor := opts.Evictor
	if evictor == nil {
		evictor = eviction.NewManager(c, logger, nil)
	}
	settings := opts.EvictionSettings
	for _, p := range pods {
		if p.DeletionTimestamp != nil {
			return "", nil
		}
	}
	candidateSet := make(map[string]struct{}, len(plan.PodNames))
	for _, n := range plan.PodNames {
		candidateSet[n] = struct{}{}
	}
	readyCount := 0
	for _, p := range pods {
		if runtimepkg.IsPodReady(&p) {
			readyCount++
		}
		if _, ok := candidateSet[p.Name]; !ok {
			if !runtimepkg.IsPodReady(&p) {
				return "", nil
			}
		}
	}

	for _, name := range plan.PodNames {
		var pod *corev1.Pod
		for i := range pods {
			if pods[i].Name == name {
				pod = &pods[i]
				break
			}
		}
		if pod == nil {
			continue
		}
		hasTLSReason := hasReasonForPod(plan, name, "tls-hash")
		if health != nil {
			if !hasTLSReason {
				healthyOthers := 0
				for _, other := range pods {
					if other.Name == name {
						continue
					}
					if healthStateForPod(other, health) {
						healthyOthers++
					}
				}
				if healthyOthers == 0 && len(pods) > 1 {
					continue
				}
			}
		}
		if pod.Labels != nil && pod.Labels[core.RoleLabelKey] == string(keyvalv1alpha1.PodRoleMaster) {
			if cr.Spec.RedisReplicas > 1 || cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
				continue
			}
		}
		if core.Ordinal(pod.Name) == 0 && readyCount <= 1 && cr.Spec.RedisReplicas > 1 && component != ComponentSentinel {
			continue
		}
		toDel := &corev1.Pod{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, toDel); err != nil {
			return "", fmt.Errorf("get pod for delete %s: %w", pod.Name, err)
		}
		req := eviction.Request{
			Cluster:   cr,
			Pod:       toDel,
			Component: component,
			Settings:  settings,
		}
		if err := evictor.Evict(ctx, req); err != nil {
			if errors.Is(err, eviction.ErrRejected) {
				if component == ComponentSentinel && !runtimepkg.IsPodReady(pod) {
					if delErr := c.Delete(ctx, toDel, client.GracePeriodSeconds(0)); delErr != nil && !apierrors.IsNotFound(delErr) {
						return "", fmt.Errorf("delete pod %s: %w", pod.Name, delErr)
					}
					observability.IncRollingDeletions(cr)
					return pod.Name, nil
				}
				return "", err
			}
			return "", fmt.Errorf("evict pod %s: %w", pod.Name, err)
		}
		observability.IncRollingDeletions(cr)
		return pod.Name, nil
	}
	return "", nil
}

func eqResourceRequirements(a, b corev1.ResourceRequirements) bool {
	if !resourceListEqual(a.Limits, b.Limits) {
		return false
	}
	if !resourceListEqual(a.Requests, b.Requests) {
		return false
	}
	return true
}

func resourceListEqual(x, y corev1.ResourceList) bool {
	if len(x) == 0 && len(y) == 0 {
		return true
	}
	for k, qx := range x {
		qy, ok := y[k]
		if !ok {
			if qx.IsZero() {
				continue
			}
			return false
		}
		if qx.Cmp(qy) != 0 {
			return false
		}
	}
	for k, qy := range y {
		if _, ok := x[k]; !ok {
			if qy.IsZero() {
				continue
			}
			return false
		}
	}
	return true
}
