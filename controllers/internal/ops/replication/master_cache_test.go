package replication

import (
	"testing"
	"time"
)

func TestMasterAddressCacheBasicLifecycle(t *testing.T) {
	cache := NewMasterAddressCache(50*time.Millisecond, 4)
	key := MasterCacheKey{Namespace: "ns", Name: "cluster"}
	if _, _, ok := cache.Lookup(key); ok {
		t.Fatalf("expected cache miss for empty cache")
	}
	cache.Remember(key, "cluster-0", "10.0.0.1:6379")
	master, detail, ok := cache.Lookup(key)
	if !ok {
		t.Fatalf("expected cache hit after remember")
	}
	if master != "cluster-0" {
		t.Fatalf("expected master cluster-0, got %q", master)
	}
	if detail != "10.0.0.1:6379" {
		t.Fatalf("expected detail to round-trip")
	}
	time.Sleep(70 * time.Millisecond)
	if _, _, ok := cache.Lookup(key); ok {
		t.Fatalf("expected cache miss after ttl expiry")
	}
}

func TestMasterAddressCacheInvalidate(t *testing.T) {
	cache := NewMasterAddressCache(200*time.Millisecond, 2)
	key := MasterCacheKey{Namespace: "ns", Name: "cluster"}
	cache.Remember(key, "cluster-0", "")
	cache.Invalidate(key)
	if _, _, ok := cache.Lookup(key); ok {
		t.Fatalf("expected miss after invalidate")
	}
}

func TestMasterAddressCacheEvictsWhenFull(t *testing.T) {
	cache := NewMasterAddressCache(time.Second, 2)
	first := MasterCacheKey{Namespace: "ns", Name: "cluster-a"}
	second := MasterCacheKey{Namespace: "ns", Name: "cluster-b"}
	third := MasterCacheKey{Namespace: "ns", Name: "cluster-c"}
	cache.Remember(first, "cluster-a-0", "")
	cache.Remember(second, "cluster-b-0", "")
	cache.Remember(third, "cluster-c-0", "")
	if _, _, ok := cache.Lookup(third); !ok {
		t.Fatalf("expected newest entry to be present")
	}
	remaining := 0
	for _, key := range []MasterCacheKey{first, second, third} {
		if _, _, ok := cache.Lookup(key); ok {
			remaining++
		}
	}
	if remaining > 2 {
		t.Fatalf("expected cache to evict to respect capacity, have %d entries", remaining)
	}
}
