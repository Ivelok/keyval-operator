package runtime

import (
	"context"
	"testing"
	"time"
)

func TestRetry_SucceedsAfterFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	attempts := 0
	start := time.Now()
	err := Retry(ctx, 3, Backoff{Initial: time.Millisecond, Factor: 2, Max: 10 * time.Millisecond}, func(i int) error {
		attempts++
		if i < 3 {
			return assertError("fail")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if elapsed := time.Since(start); elapsed < 2*time.Millisecond {
		t.Fatalf("expected some backoff delay, got %v", elapsed)
	}
}

func TestRetryClampsJitterAndDelay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	attempts := 0
	start := time.Now()
	err := Retry(ctx, 2, Backoff{Initial: 500 * time.Microsecond, Factor: 1.1, Max: 2 * time.Millisecond, Jitter: 5}, func(i int) error {
		attempts++
		if i == 1 {
			return assertError("fail")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
	if elapsed := time.Since(start); elapsed < time.Millisecond {
		t.Fatalf("expected at least minimal backoff delay, got %v", elapsed)
	}
}

func TestRetryHandlesNegativeJitter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	attempts := 0
	err := Retry(ctx, 2, Backoff{Initial: time.Millisecond, Factor: 1.5, Max: 5 * time.Millisecond, Jitter: -0.25}, func(i int) error {
		attempts++
		if i == 1 {
			return assertError("fail")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error with negative jitter: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

type assertError string

func (e assertError) Error() string { return string(e) }
