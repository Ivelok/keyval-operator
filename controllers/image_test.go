package controllers

import (
	"testing"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestResolveImage_DefaultsAndOverride(t *testing.T) {
	t.Parallel()
	// default Valkey when image empty
	spec := keyvalv1alpha1.KeyValClusterSpec{Engine: keyvalv1alpha1.EngineValkey}
	if got := core.ResolveImage(spec); got != core.DefaultValkeyImage {
		t.Fatalf("valkey default image mismatch: %s", got)
	}
	// default Redis when engine=Redis
	spec = keyvalv1alpha1.KeyValClusterSpec{Engine: keyvalv1alpha1.EngineRedis}
	if got := core.ResolveImage(spec); got != core.DefaultRedisImage {
		t.Fatalf("redis default image mismatch: %s", got)
	}
	// override via spec.image
	spec = keyvalv1alpha1.KeyValClusterSpec{Engine: keyvalv1alpha1.EngineRedis, Image: "custom:tag"}
	if got := core.ResolveImage(spec); got != "custom:tag" {
		t.Fatalf("override not respected: %s", got)
	}
}
