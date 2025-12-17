package clients

import "testing"

func TestFactoryConfig_ApplyDefaultsRespectsZeroRetries(t *testing.T) {
	cfg := FactoryConfig{MaxRetries: 0}

	normalized := cfg.ApplyDefaults()

	if normalized.MaxRetries != 0 {
		t.Fatalf("MaxRetries: got %d, want 0", normalized.MaxRetries)
	}
}

func TestFactoryConfig_ApplyDefaultsNegativeRetriesUsesDefault(t *testing.T) {
	cfg := FactoryConfig{MaxRetries: -1}

	normalized := cfg.ApplyDefaults()

	if normalized.MaxRetries <= 0 {
		t.Fatalf("MaxRetries: got %d, want positive default", normalized.MaxRetries)
	}
}
