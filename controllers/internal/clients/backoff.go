package clients

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

var (
	jitterRandMu sync.Mutex
	jitterRand   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func (cfg FactoryConfig) nextBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}
	delay := cfg.RetryInitialBackoff
	for i := 2; i < attempt; i++ {
		delay = time.Duration(float64(delay) * cfg.RetryBackoffFactor)
		if delay >= cfg.RetryMaxBackoff {
			delay = cfg.RetryMaxBackoff
			break
		}
	}
	if delay > cfg.RetryMaxBackoff {
		delay = cfg.RetryMaxBackoff
	}
	if delay <= 0 {
		return 0
	}
	if cfg.RetryJitter <= 0 {
		return delay
	}
	jitterRandMu.Lock()
	sample := jitterRand.Float64()
	jitterRandMu.Unlock()
	factor := 1 + (2*sample-1)*math.Min(cfg.RetryJitter, 1)
	if factor < 0.1 {
		factor = 0.1
	}
	return time.Duration(float64(delay) * factor)
}
