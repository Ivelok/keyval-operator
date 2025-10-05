//go:build e2e || chaos

package versions

import (
	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

// EngineCase represents a single engine/image combination exercised by the E2E matrix.
type EngineCase struct {
	Name   string
	Engine keyvalv1alpha1.Engine
	Image  string
}

var engineMatrix = []EngineCase{
	{
		Name:   "valkey-7",
		Engine: keyvalv1alpha1.EngineValkey,
		Image:  "valkey/valkey:7.2",
	},
	{
		Name:   "valkey-8",
		Engine: keyvalv1alpha1.EngineValkey,
		Image:  "valkey/valkey:8.1.2",
	},
	{
		Name:   "redis-7",
		Engine: keyvalv1alpha1.EngineRedis,
		Image:  "redis:7.2",
	},
	{
		Name:   "redis-8",
		Engine: keyvalv1alpha1.EngineRedis,
		Image:  "redis:8.2.1",
	},
}

// Engines returns a copy of all registered engine cases.
func Engines() []EngineCase {
	cases := make([]EngineCase, len(engineMatrix))
	copy(cases, engineMatrix)
	return cases
}
