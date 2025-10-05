package eviction

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clocktesting "k8s.io/utils/clock/testing"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestManagerEvict_RetriesOnRejected(t *testing.T) {
	t.Parallel()

	clk := clocktesting.NewFakeClock(time.Unix(0, 0))
	mgr := NewManager(nil, logging.New(nil), clk)

	attempts := 0
	mgr.WithEvictFunc(func(ctx context.Context, _ *corev1.Pod) error {
		attempts++
		if attempts == 1 {
			return apierrors.NewTooManyRequests("pdb limit", 0)
		}
		return nil
	})

	cluster := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default"}}
	counter := opobs.EvictionFailuresCounter()
	counter.DeleteLabelValues("default", "demo", "redis", "too_many_requests")

	var attemptLog []AttemptInfo
	var backoffLog []RetryInfo

	err := mgr.Evict(context.Background(), Request{
		Cluster:  cluster,
		Pod:      pod,
		Settings: Settings{MaxAttempts: 3, InitialBackoff: 0, MaxBackoff: 0},
		OnAttempt: func(info AttemptInfo) {
			attemptLog = append(attemptLog, info)
		},
		OnBackoff: func(info RetryInfo) {
			backoffLog = append(backoffLog, info)
		},
	})
	if err != nil {
		t.Fatalf("expected eviction to succeed after retry, got error: %v", err)
	}

	wantResults := []Result{ResultAttempt, ResultRejected, ResultAttempt, ResultSuccess}
	if len(attemptLog) != len(wantResults) {
		t.Fatalf("unexpected attempt log length: %d (log=%v)", len(attemptLog), attemptLog)
	}
	for i, want := range wantResults {
		if attemptLog[i].Result != want {
			t.Fatalf("attempt %d: want %s, got %s", i, want, attemptLog[i].Result)
		}
	}
	if len(backoffLog) != 1 {
		t.Fatalf("expected single backoff record, got %d", len(backoffLog))
	}
	if backoffLog[0].Backoff != 0 {
		t.Fatalf("expected zero backoff, got %v", backoffLog[0].Backoff)
	}
	metric, err := counter.GetMetricWith(prometheus.Labels{"namespace": "default", "cluster": "demo", "component": "redis", "reason": "too_many_requests"})
	if err != nil {
		t.Fatalf("get eviction failure metric: %v", err)
	}
	if got := testutil.ToFloat64(metric); got < 1 {
		t.Fatalf("expected failure metric >=1, got %f", got)
	}
}

func TestManagerEvict_PropagatesRejectedError(t *testing.T) {
	t.Parallel()

	clk := clocktesting.NewFakeClock(time.Unix(0, 0))
	mgr := NewManager(nil, logging.New(nil), clk)

	mgr.WithEvictFunc(func(ctx context.Context, _ *corev1.Pod) error {
		return apierrors.NewTooManyRequests("pdb limit", 0)
	})

	err := mgr.Evict(context.Background(), Request{
		Cluster:  &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}},
		Pod:      &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default"}},
		Settings: Settings{MaxAttempts: 2, InitialBackoff: 0, MaxBackoff: 0},
	})
	if err == nil {
		t.Fatalf("expected error when all attempts rejected")
	}
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("expected ErrRejected in chain, got %v", err)
	}
	if !controllererrors.IsTransient(err) {
		t.Fatalf("expected error to be marked transient")
	}
}

func TestManagerEvict_ForbiddenIsFatal(t *testing.T) {
	t.Parallel()

	clk := clocktesting.NewFakeClock(time.Unix(0, 0))
	mgr := NewManager(nil, logging.New(nil), clk)

	mgr.WithEvictFunc(func(ctx context.Context, _ *corev1.Pod) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: "", Resource: "pods"}, "demo-0", errors.New("denied"))
	})

	counter := opobs.EvictionFailuresCounter()
	counter.DeleteLabelValues("default", "demo", "redis", "forbidden")

	err := mgr.Evict(context.Background(), Request{
		Cluster:  &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}},
		Pod:      &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default"}},
		Settings: Settings{MaxAttempts: 3, InitialBackoff: 0, MaxBackoff: 0},
	})
	if err == nil {
		t.Fatalf("expected forbidden error to surface")
	}
	if controllererrors.IsTransient(err) {
		t.Fatalf("expected forbidden error to be fatal, got transient")
	}
	metric, mErr := counter.GetMetricWith(prometheus.Labels{"namespace": "default", "cluster": "demo", "component": "redis", "reason": "forbidden"})
	if mErr != nil {
		t.Fatalf("get forbidden failure metric: %v", mErr)
	}
	if got := testutil.ToFloat64(metric); got < 1 {
		t.Fatalf("expected forbidden failure metric >=1, got %f", got)
	}
}

func TestManagerEvict_ContextCancelled(t *testing.T) {
	t.Parallel()

	clk := clocktesting.NewFakeClock(time.Unix(0, 0))
	mgr := NewManager(nil, logging.New(nil), clk)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	counter := opobs.EvictionFailuresCounter()
	counter.DeleteLabelValues("default", "demo", "redis", "cancelled")

	err := mgr.Evict(ctx, Request{
		Cluster:  &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}},
		Pod:      &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default"}},
		Settings: Settings{MaxAttempts: 3, InitialBackoff: 0, MaxBackoff: 0},
	})
	if err == nil {
		t.Fatalf("expected cancellation error")
	}
	if !controllererrors.IsTransient(err) {
		t.Fatalf("expected cancellation to be transient")
	}
	metric, mErr := counter.GetMetricWith(prometheus.Labels{"namespace": "default", "cluster": "demo", "component": "redis", "reason": "cancelled"})
	if mErr != nil {
		t.Fatalf("get cancelled failure metric: %v", mErr)
	}
	if got := testutil.ToFloat64(metric); got < 1 {
		t.Fatalf("expected cancelled failure metric >=1, got %f", got)
	}
}

func TestNextBackoffCapsAtMax(t *testing.T) {
	t.Parallel()

	settings := Settings{InitialBackoff: time.Second, MaxBackoff: 3 * time.Second}
	if d := nextBackoff(settings, 1); d != time.Second {
		t.Fatalf("attempt 1: expected 1s, got %v", d)
	}
	if d := nextBackoff(settings, 2); d != 2*time.Second {
		t.Fatalf("attempt 2: expected 2s, got %v", d)
	}
	if d := nextBackoff(settings, 3); d != 3*time.Second {
		t.Fatalf("attempt 3: expected cap at 3s, got %v", d)
	}
}

// Ensure applyDefaults maintains zero backoff when explicitly requested.
func TestApplyDefaultsRespectsZeroBackoff(t *testing.T) {
	t.Parallel()

	got := applyDefaults(Settings{InitialBackoff: 0, MaxBackoff: 0})
	if got.InitialBackoff != 0 || got.MaxBackoff != 0 {
		t.Fatalf("expected zero backoff preserved, got %+v", got)
	}
	if got.MaxAttempts != 3 {
		t.Fatalf("expected default max attempts 3, got %d", got.MaxAttempts)
	}
}
