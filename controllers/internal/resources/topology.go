package resources

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

var defaultTopologyKeys = []string{"topology.kubernetes.io/zone", corev1.LabelHostname}

// applyTopologySettings mutates the provided PodTemplateSpec with topology spread and anti-affinity rules
// computed from the cluster spec. componentLabel should be the value of core.LabelAppKey for the target pods.
func applyTopologySettings(cr *keyvalv1alpha1.KeyValCluster, componentLabel string, tpl *corev1.PodTemplateSpec) {
	topology := cr.Spec.Topology
	selectorLabels := map[string]string{core.LabelClusterKey: cr.Name}
	if componentLabel != "" {
		selectorLabels[core.LabelAppKey] = componentLabel
	}

	var spreadCfg *keyvalv1alpha1.TopologySpreadSpec
	if topology != nil {
		spreadCfg = topology.Spread
	}
	keys := topologySpreadKeys(spreadCfg)
	if spreadCfg != nil && spreadCfg.Disabled {
		tpl.Spec.TopologySpreadConstraints = pruneTopologyConstraints(tpl.Spec.TopologySpreadConstraints, selectorLabels, keys)
	} else {
		defaults := buildTopologySpreadConstraints(spreadCfg, selectorLabels, keys)
		tpl.Spec.TopologySpreadConstraints = mergeTopologyConstraints(tpl.Spec.TopologySpreadConstraints, defaults, selectorLabels, keys)
	}

	var antiCfg *keyvalv1alpha1.TopologyAntiAffinitySpec
	if topology != nil {
		antiCfg = topology.AntiAffinity
	}
	if antiCfg != nil && antiCfg.Disabled {
		prunePodAntiAffinity(tpl, selectorLabels)
	} else {
		applyPodAntiAffinity(antiCfg, selectorLabels, tpl)
	}
}

func buildTopologySpreadConstraints(cfg *keyvalv1alpha1.TopologySpreadSpec, selectorLabels map[string]string, keys []string) []corev1.TopologySpreadConstraint {
	if cfg != nil && cfg.Disabled {
		return nil
	}
	maxSkew := int32(1)
	if cfg != nil && cfg.MaxSkew != nil && *cfg.MaxSkew > 0 {
		maxSkew = *cfg.MaxSkew
	}
	action := corev1.ScheduleAnyway
	if cfg != nil && cfg.WhenUnsatisfiable != "" {
		action = cfg.WhenUnsatisfiable
	}

	constraints := make([]corev1.TopologySpreadConstraint, 0, len(keys))
	for _, key := range keys {
		sel := &metav1.LabelSelector{MatchLabels: cloneStringMap(selectorLabels)}
		constraints = append(constraints, corev1.TopologySpreadConstraint{
			MaxSkew:           maxSkew,
			TopologyKey:       key,
			WhenUnsatisfiable: action,
			LabelSelector:     sel,
		})
	}
	return constraints
}

func applyPodAntiAffinity(cfg *keyvalv1alpha1.TopologyAntiAffinitySpec, selectorLabels map[string]string, tpl *corev1.PodTemplateSpec) {
	if tpl.Spec.Affinity == nil {
		tpl.Spec.Affinity = &corev1.Affinity{}
	}
	if tpl.Spec.Affinity.PodAntiAffinity == nil {
		tpl.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}

	ensurePreferredAntiAffinity(tpl.Spec.Affinity.PodAntiAffinity, selectorLabels)

	if cfg != nil && cfg.Required {
		ensureRequiredAntiAffinity(tpl.Spec.Affinity.PodAntiAffinity, selectorLabels)
	} else {
		removeRequiredAntiAffinity(tpl.Spec.Affinity.PodAntiAffinity, selectorLabels)
	}
}

func topologySpreadKeys(cfg *keyvalv1alpha1.TopologySpreadSpec) []string {
	keys := defaultTopologyKeys
	if cfg == nil || len(cfg.TopologyKeys) == 0 {
		return keys
	}
	filtered := make([]string, 0, len(cfg.TopologyKeys))
	seen := map[string]struct{}{}
	for _, key := range cfg.TopologyKeys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		filtered = append(filtered, key)
	}
	if len(filtered) == 0 {
		return keys
	}
	return filtered
}

func mergeTopologyConstraints(existing, defaults []corev1.TopologySpreadConstraint, selector map[string]string, keys []string) []corev1.TopologySpreadConstraint {
	filtered := pruneTopologyConstraints(existing, selector, keys)
	if len(defaults) == 0 {
		return filtered
	}
	return append(filtered, defaults...)
}

func pruneTopologyConstraints(existing []corev1.TopologySpreadConstraint, selector map[string]string, keys []string) []corev1.TopologySpreadConstraint {
	if len(existing) == 0 {
		return existing
	}
	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
	result := make([]corev1.TopologySpreadConstraint, 0, len(existing))
	for _, c := range existing {
		if isOperatorConstraint(c, selector, keySet) {
			continue
		}
		result = append(result, c)
	}
	return result
}

func isOperatorConstraint(c corev1.TopologySpreadConstraint, selector map[string]string, keys map[string]struct{}) bool {
	if c.LabelSelector == nil || len(c.LabelSelector.MatchExpressions) > 0 {
		return false
	}
	if !matchLabelsEqual(c.LabelSelector.MatchLabels, selector) {
		return false
	}
	if len(keys) > 0 {
		if _, ok := keys[c.TopologyKey]; !ok {
			return false
		}
	}
	return true
}

func ensurePreferredAntiAffinity(paa *corev1.PodAntiAffinity, selector map[string]string) {
	if paa == nil {
		return
	}
	for _, term := range paa.PreferredDuringSchedulingIgnoredDuringExecution {
		if term.PodAffinityTerm.TopologyKey == corev1.LabelHostname && labelSelectorMatches(term.PodAffinityTerm.LabelSelector, selector) {
			return
		}
	}
	paa.PreferredDuringSchedulingIgnoredDuringExecution = append(paa.PreferredDuringSchedulingIgnoredDuringExecution, corev1.WeightedPodAffinityTerm{
		Weight: 100,
		PodAffinityTerm: corev1.PodAffinityTerm{
			TopologyKey:   corev1.LabelHostname,
			LabelSelector: &metav1.LabelSelector{MatchLabels: cloneStringMap(selector)},
		},
	})
}

func ensureRequiredAntiAffinity(paa *corev1.PodAntiAffinity, selector map[string]string) {
	if paa == nil {
		return
	}
	for _, term := range paa.RequiredDuringSchedulingIgnoredDuringExecution {
		if term.TopologyKey == corev1.LabelHostname && labelSelectorMatches(term.LabelSelector, selector) {
			return
		}
	}
	paa.RequiredDuringSchedulingIgnoredDuringExecution = append(paa.RequiredDuringSchedulingIgnoredDuringExecution, corev1.PodAffinityTerm{
		TopologyKey:   corev1.LabelHostname,
		LabelSelector: &metav1.LabelSelector{MatchLabels: cloneStringMap(selector)},
	})
}

func removeRequiredAntiAffinity(paa *corev1.PodAntiAffinity, selector map[string]string) {
	if paa == nil {
		return
	}
	required := paa.RequiredDuringSchedulingIgnoredDuringExecution
	if len(required) == 0 {
		return
	}
	filtered := required[:0]
	for _, term := range required {
		if term.TopologyKey == corev1.LabelHostname && labelSelectorMatches(term.LabelSelector, selector) {
			continue
		}
		filtered = append(filtered, term)
	}
	if len(filtered) == 0 {
		paa.RequiredDuringSchedulingIgnoredDuringExecution = nil
	} else if len(filtered) != len(required) {
		paa.RequiredDuringSchedulingIgnoredDuringExecution = append([]corev1.PodAffinityTerm(nil), filtered...)
	}
}

func prunePodAntiAffinity(tpl *corev1.PodTemplateSpec, selector map[string]string) {
	if tpl.Spec.Affinity == nil || tpl.Spec.Affinity.PodAntiAffinity == nil {
		return
	}
	paa := tpl.Spec.Affinity.PodAntiAffinity
	if len(paa.PreferredDuringSchedulingIgnoredDuringExecution) > 0 {
		kept := paa.PreferredDuringSchedulingIgnoredDuringExecution[:0]
		for _, term := range paa.PreferredDuringSchedulingIgnoredDuringExecution {
			if term.PodAffinityTerm.TopologyKey == corev1.LabelHostname && labelSelectorMatches(term.PodAffinityTerm.LabelSelector, selector) {
				continue
			}
			kept = append(kept, term)
		}
		if len(kept) == 0 {
			paa.PreferredDuringSchedulingIgnoredDuringExecution = nil
		} else if len(kept) != len(paa.PreferredDuringSchedulingIgnoredDuringExecution) {
			paa.PreferredDuringSchedulingIgnoredDuringExecution = append([]corev1.WeightedPodAffinityTerm(nil), kept...)
		}
	}
	removeRequiredAntiAffinity(paa, selector)
	if len(paa.PreferredDuringSchedulingIgnoredDuringExecution) == 0 && len(paa.RequiredDuringSchedulingIgnoredDuringExecution) == 0 {
		tpl.Spec.Affinity.PodAntiAffinity = nil
		if tpl.Spec.Affinity.PodAffinity == nil && tpl.Spec.Affinity.NodeAffinity == nil {
			tpl.Spec.Affinity = nil
		}
	}
}

func labelSelectorMatches(sel *metav1.LabelSelector, selector map[string]string) bool {
	if sel == nil {
		return false
	}
	if len(sel.MatchExpressions) > 0 {
		return false
	}
	return matchLabelsEqual(sel.MatchLabels, selector)
}

func matchLabelsEqual(a, b map[string]string) bool {
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

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
