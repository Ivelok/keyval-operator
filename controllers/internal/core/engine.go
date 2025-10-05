package core

import (
	"strings"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

const (
	DefaultRedisImage  = "redis:7.2"
	DefaultValkeyImage = "valkey/valkey:7.2"
)

func ResolveImage(spec keyvalv1alpha1.KeyValClusterSpec) string {
	if spec.Image != "" {
		return spec.Image
	}
	switch spec.Engine {
	case keyvalv1alpha1.EngineRedis:
		return DefaultRedisImage
	case keyvalv1alpha1.EngineValkey:
		fallthrough
	default:
		return DefaultValkeyImage
	}
}

func ServerCommandForEngine(engine keyvalv1alpha1.Engine) []string {
	switch engine {
	case keyvalv1alpha1.EngineValkey:
		return []string{"valkey-server"}
	case keyvalv1alpha1.EngineRedis:
		fallthrough
	default:
		return []string{"redis-server"}
	}
}

func SentinelCommandForEngine(engine keyvalv1alpha1.Engine) []string {
	switch engine {
	case keyvalv1alpha1.EngineValkey:
		return []string{"valkey-sentinel"}
	case keyvalv1alpha1.EngineRedis:
		fallthrough
	default:
		return []string{"redis-sentinel"}
	}
}

func inferEngineFromImage(image string) (keyvalv1alpha1.Engine, bool) {
	if image == "" {
		return "", false
	}
	lower := strings.ToLower(image)
	switch {
	case strings.Contains(lower, "valkey"):
		return keyvalv1alpha1.EngineValkey, true
	case strings.Contains(lower, "redis"):
		return keyvalv1alpha1.EngineRedis, true
	default:
		return "", false
	}
}

func effectiveEngine(explicit keyvalv1alpha1.Engine, image string) keyvalv1alpha1.Engine {
	if inferred, ok := inferEngineFromImage(image); ok {
		return inferred
	}
	if explicit != "" {
		return explicit
	}
	return keyvalv1alpha1.EngineValkey
}

func EffectiveServerEngine(spec keyvalv1alpha1.KeyValClusterSpec) keyvalv1alpha1.Engine {
	return effectiveEngine(spec.Engine, spec.Image)
}

func EffectiveSentinelEngine(spec keyvalv1alpha1.KeyValClusterSpec) keyvalv1alpha1.Engine {
	img := spec.Image
	if spec.SentinelImage != nil && *spec.SentinelImage != "" {
		img = *spec.SentinelImage
	}
	return effectiveEngine(spec.Engine, img)
}
