//go:build e2e || chaos

package chaosmetrics

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	defaultIterations   = 3
	defaultPingInterval = time.Second
)

// Config captures chaos runtime settings.
type Config struct {
	Experiments   map[string]struct{}
	Iterations    int
	PingInterval  time.Duration
	KeepArtifacts bool
	KeepResources bool
}

// LoadConfig parses chaos-related environment variables.
func LoadConfig(t *testing.T) Config {
	t.Helper()
	cfg := Config{
		Experiments:  parseExperiments(os.Getenv("CHAOS_EXPERIMENTS")),
		Iterations:   defaultIterations,
		PingInterval: defaultPingInterval,
		KeepArtifacts: lookupBool(
			"KEEP_ARTIFACTS",
			"TESTS_NEW_KEEP_ARTIFACTS",
		),
		KeepResources: lookupBool(
			"KEEP_RESOURCES",
			"TESTS_NEW_KEEP_RESOURCES",
		),
	}

	if raw := strings.TrimSpace(os.Getenv("CHAOS_ITERATIONS")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			t.Fatalf("invalid CHAOS_ITERATIONS: %q", raw)
		}
		cfg.Iterations = value
	}
	if raw := strings.TrimSpace(os.Getenv("CHAOS_PING_INTERVAL")); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value <= 0 {
			t.Fatalf("invalid CHAOS_PING_INTERVAL: %q", raw)
		}
		cfg.PingInterval = value
	}
	return cfg
}

// Allows returns true when the experiment is enabled.
func (c Config) Allows(name string) bool {
	if len(c.Experiments) == 0 {
		return true
	}
	_, ok := c.Experiments[strings.ToLower(strings.TrimSpace(name))]
	return ok
}

// SkipUnless skips the test when the experiment is not selected.
func SkipUnless(t *testing.T, cfg Config, experiment string) {
	t.Helper()
	if cfg.Allows(experiment) {
		return
	}
	t.Skipf("chaos experiment %q not selected via CHAOS_EXPERIMENTS", experiment)
}

func parseExperiments(raw string) map[string]struct{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func lookupBool(keys ...string) bool {
	for _, key := range keys {
		if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
			value, err := parseBool(raw)
			if err == nil {
				return value
			}
		}
	}
	return false
}

func parseBool(value string) (bool, error) {
	switch value {
	case "1", "t", "T", "true", "TRUE", "True", "yes", "Y", "y":
		return true, nil
	case "0", "f", "F", "false", "FALSE", "False", "no", "N", "n":
		return false, nil
	default:
		return false, strconv.ErrSyntax
	}
}
