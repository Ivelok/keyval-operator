package controllers

import (
	"math/rand"
	"sync"
	"time"
)

var backoffSteps = []time.Duration{3 * time.Second, 6 * time.Second, 12 * time.Second, 24 * time.Second, 48 * time.Second, 60 * time.Second}

const backoffJitterFraction = 0.1

type backoffTracker struct {
	mu      sync.Mutex
	entries map[string]*backoffEntry
	rng     *rand.Rand
}

type backoffEntry struct {
	attempts int
}

func newBackoffTracker() *backoffTracker {
	return &backoffTracker{
		entries: make(map[string]*backoffEntry),
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (b *backoffTracker) Next(key string) time.Duration {
	if key == "" {
		key = "default"
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.entries[key]
	if entry == nil {
		entry = &backoffEntry{}
		b.entries[key] = entry
	}
	entry.attempts++
	if entry.attempts > len(backoffSteps) {
		entry.attempts = len(backoffSteps)
	}
	base := backoffSteps[entry.attempts-1]
	if base <= 0 {
		return 0
	}
	jitter := 1.0
	if backoffJitterFraction > 0 {
		jitter = 1 + ((b.rng.Float64()*2 - 1) * backoffJitterFraction)
	}
	delay := time.Duration(float64(base) * jitter)
	if delay <= 0 {
		delay = base
	}
	return delay
}

func (b *backoffTracker) Reset(key string) {
	if key == "" {
		key = "default"
	}
	b.mu.Lock()
	delete(b.entries, key)
	b.mu.Unlock()
}
