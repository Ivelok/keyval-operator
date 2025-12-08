package runtime

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Backoff controls retry delays.
type Backoff struct {
	Initial time.Duration // initial delay
	Max     time.Duration // maximum delay
	Factor  float64       // growth factor per attempt (>=1)
	Jitter  float64       // 0..1 fraction randomization of delay
}

var (
	retryRandMu sync.Mutex
	retryRand   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// WithTimeout returns a child context with the given timeout.
func WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

// Retry runs fn up to attempts times until it returns nil or context is done.
// Between attempts it sleeps with exponential backoff.
func Retry(ctx context.Context, attempts int, bo Backoff, fn func(attempt int) error) error {
	if attempts < 1 {
		attempts = 1
	}
	if bo.Initial <= 0 {
		bo.Initial = 50 * time.Millisecond
	} else if bo.Initial < time.Millisecond {
		bo.Initial = time.Millisecond
	}
	if bo.Factor < 1.0 {
		bo.Factor = 2.0
	}
	if bo.Max <= 0 {
		bo.Max = 2 * time.Second
	} else if bo.Max < bo.Initial {
		bo.Max = bo.Initial
	}
	if bo.Jitter < 0 {
		bo.Jitter = 0
	}
	if bo.Jitter > 1 {
		bo.Jitter = 1
	}
	var err error
	delay := bo.Initial
	for i := 1; i <= attempts; i++ {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if err = fn(i); err == nil {
			return nil
		}
		if i == attempts {
			break
		}
		d := delay
		if bo.Jitter > 0 {
			retryRandMu.Lock()
			sample := retryRand.Float64()
			retryRandMu.Unlock()
			frac := (sample*2 - 1) * bo.Jitter
			d = time.Duration(float64(d) * (1 + frac))
		}
		if d < time.Millisecond {
			d = time.Millisecond
		}
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		next := time.Duration(float64(delay) * bo.Factor)
		if next > bo.Max {
			next = bo.Max
		}
		if next < 0 || next > time.Duration(math.MaxInt64) {
			next = bo.Max
		}
		if next < time.Millisecond {
			next = time.Millisecond
		}
		delay = next
	}
	return err
}
