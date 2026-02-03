package resources

import "sort"

var redisRuntimeAllow = map[string]struct{}{
	"maxmemory":               {},
	"maxmemory-policy":        {},
	"maxmemory-samples":       {},
	"lfu-log-factor":          {},
	"lfu-decay-time":          {},
	"repl-timeout":            {},
	"repl-backlog-size":       {},
	"repl-backlog-ttl":        {},
	"tcp-keepalive":           {},
	"hz":                      {},
	"stream-node-max-bytes":   {},
	"stream-node-max-entries": {},
}

var sentinelRuntimeAllow = map[string]struct{}{
	"down-after-milliseconds": {},
	"failover-timeout":        {},
	"parallel-syncs":          {},
}

// IsRedisRuntimeKey reports whether a redis.conf key can be applied at runtime.
func IsRedisRuntimeKey(key string) bool {
	_, ok := redisRuntimeAllow[key]
	return ok
}

// IsSentinelRuntimeKey reports whether a sentinel option can be applied at runtime.
func IsSentinelRuntimeKey(option string) bool {
	_, ok := sentinelRuntimeAllow[option]
	return ok
}

// RedisRuntimeKeys returns the sorted allowlist of redis.conf runtime keys.
func RedisRuntimeKeys() []string {
	return sortedRuntimeKeys(redisRuntimeAllow)
}

// SentinelRuntimeKeys returns the sorted allowlist of sentinel runtime options.
func SentinelRuntimeKeys() []string {
	return sortedRuntimeKeys(sentinelRuntimeAllow)
}

func sortedRuntimeKeys(src map[string]struct{}) []string {
	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
