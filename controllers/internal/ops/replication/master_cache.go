package replication

import (
	"sync"
	"time"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
)

// MasterCacheKey identifies a cluster entry in the master-address cache.
type MasterCacheKey struct {
	Namespace string
	Name      string
}

type cacheEntry struct {
	master  string
	detail  string
	expires time.Time
}

// MasterAddressCache caches master pod names for a bounded duration to reduce sentinel load.
type MasterAddressCache struct {
	ttlDuration time.Duration
	maxEntries  int
	mu          sync.RWMutex
	entries     map[MasterCacheKey]cacheEntry
}

// NewMasterAddressCache constructs a cache with the provided TTL and capacity.
func NewMasterAddressCache(ttl time.Duration, maxEntries int) *MasterAddressCache {
	if ttl <= 0 {
		ttl = 200 * time.Millisecond
	}
	if maxEntries <= 0 {
		maxEntries = 128
	}
	return &MasterAddressCache{
		ttlDuration: ttl,
		maxEntries:  maxEntries,
		entries:     make(map[MasterCacheKey]cacheEntry, maxEntries),
	}
}

// Lookup returns the cached master if present and not expired.
func (c *MasterAddressCache) Lookup(key MasterCacheKey) (string, string, bool) {
	if c == nil {
		return "", "", false
	}
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		opobs.IncMasterCacheMiss(key.Namespace, key.Name)
		return "", "", false
	}
	if now.After(entry.expires) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		opobs.IncMasterCacheMiss(key.Namespace, key.Name)
		return "", "", false
	}
	opobs.IncMasterCacheHit(key.Namespace, key.Name)
	return entry.master, entry.detail, true
}

// Remember stores the provided master in the cache.
func (c *MasterAddressCache) Remember(key MasterCacheKey, master string, detail string) {
	if c == nil {
		return
	}
	if master == "" {
		return
	}
	entry := cacheEntry{master: master, detail: detail, expires: time.Now().Add(c.ttlDuration)}
	c.mu.Lock()
	if len(c.entries) >= c.maxEntries {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[key] = entry
	c.mu.Unlock()
}

// Invalidate removes the cache entry for the provided key.
func (c *MasterAddressCache) Invalidate(key MasterCacheKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// CacheKeyForCluster returns the cache key for the given cluster.
func CacheKeyForCluster(cr *keyvalv1alpha1.KeyValCluster) MasterCacheKey {
	if cr == nil {
		return MasterCacheKey{}
	}
	return MasterCacheKey{Namespace: cr.Namespace, Name: cr.Name}
}
