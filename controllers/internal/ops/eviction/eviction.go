package eviction

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

// ErrRejected indicates that the eviction request was rejected (for example by a PodDisruptionBudget).
var ErrRejected = errors.New("eviction rejected")

// Result describes the outcome of an eviction attempt.
type Result string

const (
	ResultAttempt   Result = "attempt"
	ResultSuccess   Result = "success"
	ResultRejected  Result = "rejected"
	ResultTimeout   Result = "timeout"
	ResultError     Result = "error"
	ResultForbidden Result = "forbidden"
	ResultCancelled Result = "cancelled"
)

// Settings control retry/backoff behaviour for eviction requests.
type Settings struct {
	// MaxAttempts is the maximum number of eviction attempts before giving up.
	MaxAttempts int
	// PerAttemptTimeout limits how long each individual eviction call may run.
	PerAttemptTimeout time.Duration
	// InitialBackoff is the first backoff duration applied after a failed attempt.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential backoff applied between attempts.
	MaxBackoff time.Duration
}

// Request bundles the arguments required to evict a pod.
type Request struct {
	Cluster   *keyvalv1alpha1.KeyValCluster
	Pod       *corev1.Pod
	Component string
	Settings  Settings
	// OnAttempt is invoked after every attempt with the observed result.
	OnAttempt func(AttemptInfo)
	// OnBackoff is invoked before sleeping prior to a retry.
	OnBackoff func(RetryInfo)
}

// AttemptInfo captures the outcome of a single eviction attempt.
type AttemptInfo struct {
	Attempt int
	Result  Result
	Err     error
}

// RetryInfo captures the metadata for a scheduled retry.
type RetryInfo struct {
	Attempt int
	Err     error
	Backoff time.Duration
}

// Runner represents an eviction orchestrator.
type Runner interface {
	Evict(ctx context.Context, req Request) error
}

// EvictFunc evicts the provided pod using the underlying Kubernetes client. It is overrideable for tests.
type EvictFunc func(ctx context.Context, pod *corev1.Pod) error

// Manager implements eviction retries with timeouts, exponential backoff, logging, and metrics.
type Manager struct {
	client  client.Client
	clock   clock.Clock
	logger  logging.Logger
	evictFn EvictFunc
}

// NewManager constructs a Manager backed by the provided client, logger, and clock.
func NewManager(c client.Client, logger logging.Logger, clk clock.Clock) *Manager {
	if clk == nil {
		clk = clock.RealClock{}
	}
	if logger.IsZero() {
		logger = logging.New(nil)
	}
	return &Manager{
		client:  c,
		clock:   clk,
		logger:  logger.WithGroup("eviction"),
		evictFn: func(ctx context.Context, pod *corev1.Pod) error { return kubeEvict(ctx, c, pod) },
	}
}

// WithEvictFunc overrides the eviction implementation. Intended for tests.
func (m *Manager) WithEvictFunc(fn EvictFunc) {
	if fn == nil {
		m.evictFn = func(ctx context.Context, pod *corev1.Pod) error { return kubeEvict(ctx, m.client, pod) }
		return
	}
	m.evictFn = fn
}

func kubeEvict(ctx context.Context, c client.Client, pod *corev1.Pod) error {
	if c == nil {
		return errors.New("client is nil")
	}
	eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name}}
	return c.SubResource("eviction").Create(ctx, pod, eviction)
}

// Evict attempts to evict the requested pod honouring retry/backoff policies and observability.
func (m *Manager) Evict(ctx context.Context, req Request) error {
	if req.Pod == nil {
		return controllererrors.WrapFatal(errors.New("pod is nil"))
	}
	component := req.Component
	if component == "" {
		component = "redis"
	}
	settings := applyDefaults(req.Settings)
	logger := m.logger.WithValues("pod", req.Pod.Name, "component", component)

	var lastErr error
	for attempt := 1; attempt <= settings.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			err := controllererrors.WrapTransient(fmt.Errorf("eviction context cancelled: %w", ctx.Err()))
			emitAttempt(req, AttemptInfo{Attempt: attempt, Result: ResultCancelled, Err: err})
			opobs.IncEvictionResult(req.Cluster, component, string(ResultCancelled))
			opobs.IncEvictionFailure(req.Cluster, component, "cancelled")
			return err
		default:
		}

		attemptCtx := ctx
		var cancel context.CancelFunc
		if settings.PerAttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, settings.PerAttemptTimeout)
		}

		opobs.IncEvictionAttempt(req.Cluster, component)
		emitAttempt(req, AttemptInfo{Attempt: attempt, Result: ResultAttempt})
		logger.Debug("attempting eviction", "attempt", attempt, "maxAttempts", settings.MaxAttempts)
		err := m.evictFn(attemptCtx, req.Pod)
		if cancel != nil {
			cancel()
		}
		if err == nil {
			logger.Debug("eviction succeeded", "attempt", attempt)
			opobs.IncEvictionResult(req.Cluster, component, string(ResultSuccess))
			emitAttempt(req, AttemptInfo{Attempt: attempt, Result: ResultSuccess})
			return nil
		}
		if apierrors.IsNotFound(err) {
			logger.Debug("pod already deleted, treating eviction as successful", "attempt", attempt)
			opobs.IncEvictionResult(req.Cluster, component, string(ResultSuccess))
			emitAttempt(req, AttemptInfo{Attempt: attempt, Result: ResultSuccess})
			return nil
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			lastErr = controllererrors.WrapTransient(fmt.Errorf("eviction timed out: %w", err))
			opobs.IncEvictionResult(req.Cluster, component, string(ResultTimeout))
			opobs.IncEvictionFailure(req.Cluster, component, "timeout")
			logger.Info("eviction attempt timed out", "attempt", attempt, "timeout", settings.PerAttemptTimeout)
			emitAttempt(req, AttemptInfo{Attempt: attempt, Result: ResultTimeout, Err: lastErr})
		} else if apierrors.IsTooManyRequests(err) {
			lastErr = controllererrors.WrapTransient(errors.Join(ErrRejected, err))
			policy := string(apierrors.ReasonForError(err))
			if policy == "" {
				policy = "TooManyRequests"
			}
			opobs.IncEvictionResult(req.Cluster, component, string(ResultRejected))
			opobs.IncEvictionFailure(req.Cluster, component, policy)
			logger.Info("eviction rejected by policy", "attempt", attempt, "policy", policy)
			emitAttempt(req, AttemptInfo{Attempt: attempt, Result: ResultRejected, Err: lastErr})
		} else if apierrors.IsForbidden(err) {
			policy := string(apierrors.ReasonForError(err))
			if policy == "" {
				policy = "Forbidden"
			}
			wrapped := controllererrors.WrapFatal(fmt.Errorf("eviction forbidden: %w", err))
			opobs.IncEvictionResult(req.Cluster, component, string(ResultForbidden))
			opobs.IncEvictionFailure(req.Cluster, component, policy)
			emitAttempt(req, AttemptInfo{Attempt: attempt, Result: ResultForbidden, Err: wrapped})
			logger.Error(err, "eviction forbidden", "attempt", attempt, "policy", policy)
			return wrapped
		} else {
			lastErr = controllererrors.WrapTransient(fmt.Errorf("eviction attempt failed: %w", err))
			opobs.IncEvictionResult(req.Cluster, component, string(ResultError))
			reason := string(apierrors.ReasonForError(err))
			if reason == "" {
				reason = "error"
			}
			opobs.IncEvictionFailure(req.Cluster, component, reason)
			emitAttempt(req, AttemptInfo{Attempt: attempt, Result: ResultError, Err: lastErr})
			logger.Error(err, "eviction attempt failed", "attempt", attempt)
		}

		if attempt >= settings.MaxAttempts {
			if lastErr == nil {
				lastErr = controllererrors.WrapTransient(errors.New("eviction attempts exhausted"))
			}
			return lastErr
		}

		backoff := nextBackoff(settings, attempt)
		logger.Debug("eviction retry scheduled", "attempt", attempt+1, "backoff", backoff)
		emitBackoff(req, RetryInfo{Attempt: attempt, Err: lastErr, Backoff: backoff})
		if backoff > 0 {
			if err := wait(ctx, m.clock, backoff); err != nil {
				wrapped := controllererrors.WrapTransient(fmt.Errorf("eviction backoff interrupted: %w", err))
				opobs.IncEvictionResult(req.Cluster, component, string(ResultCancelled))
				emitAttempt(req, AttemptInfo{Attempt: attempt, Result: ResultCancelled, Err: wrapped})
				return wrapped
			}
		}
	}
	if lastErr == nil {
		lastErr = controllererrors.WrapTransient(errors.New("eviction failed without error"))
	}
	return lastErr
}

func emitAttempt(req Request, info AttemptInfo) {
	if req.OnAttempt != nil {
		req.OnAttempt(info)
	}
}

func emitBackoff(req Request, info RetryInfo) {
	if req.OnBackoff != nil {
		req.OnBackoff(info)
	}
}

func wait(ctx context.Context, clk clock.Clock, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := clk.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return nil
	}
}

func applyDefaults(in Settings) Settings {
	out := in
	if out.MaxAttempts <= 0 {
		out.MaxAttempts = 3
	}
	if out.PerAttemptTimeout < 0 {
		out.PerAttemptTimeout = 0
	}
	if out.InitialBackoff < 0 {
		out.InitialBackoff = time.Second
	}
	if out.MaxBackoff < 0 {
		out.MaxBackoff = 5 * time.Second
	}
	if out.MaxBackoff < out.InitialBackoff {
		out.MaxBackoff = out.InitialBackoff
	}
	return out
}

func nextBackoff(settings Settings, attempt int) time.Duration {
	if settings.InitialBackoff == 0 {
		return 0
	}
	if attempt <= 0 {
		return settings.InitialBackoff
	}
	multiplier := time.Duration(1) << (attempt - 1)
	candidate := settings.InitialBackoff * multiplier
	if multiplier > 0 && candidate/multiplier != settings.InitialBackoff {
		return settings.MaxBackoff
	}
	if candidate > settings.MaxBackoff {
		return settings.MaxBackoff
	}
	return candidate
}
