package reconcile

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/clients"
	opbootstrap "github.com/ivelok/keyval-operator/controllers/internal/ops/bootstrap"
	opeviction "github.com/ivelok/keyval-operator/controllers/internal/ops/eviction"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
	opruntimecfg "github.com/ivelok/keyval-operator/controllers/internal/ops/runtimeconfig"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	opstorage "github.com/ivelok/keyval-operator/controllers/internal/ops/storage"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

// Handler represents the orchestration entry that executes reconcile phases.
type Handler interface {
	Run(context.Context, *State) (ctrl.Result, error)
}

// HandlerFunc adapts a bare function into a Handler.
type HandlerFunc func(context.Context, *State) (ctrl.Result, error)

// Run executes the function with the provided state.
func (f HandlerFunc) Run(ctx context.Context, state *State) (ctrl.Result, error) {
	return f(ctx, state)
}

// TraceHook starts an optional tracing span for a named reconcile phase. It returns
// the derived context alongside a closure that must be invoked to finish the span.
type TraceHook func(context.Context, string) (context.Context, func())

// Dependencies capture the shared controller inputs required by the pipeline.
type Dependencies struct {
	Client                  client.Client
	APIReader               client.Reader
	Recorder                record.EventRecorder
	Scheme                  *runtime.Scheme
	BaseLogger              logging.Logger
	ControllerName          string
	QueueDepth              func() int
	ResetBackoff            func(string)
	ClientFactory           clients.Factory
	SentinelFactory         clients.SentinelFactory
	MasterCache             *opreplication.MasterAddressCache
	EvictionSettings        opeviction.Settings
	ObserveGracefulShutdown func(*keyvalv1alpha1.KeyValCluster, []corev1.Pod)
	TraceHook               TraceHook
}

// PodTemplateMetadata captures the template metadata used to reconcile existing pods.
type PodTemplateMetadata struct {
	DesiredLabels       map[string]string
	PreviousLabels      map[string]string
	DesiredAnnotations  map[string]string
	PreviousAnnotations map[string]string
}

// State carries request-scoped context for the reconcile handler.
type State struct {
	Request                  ctrl.Request
	Logger                   logging.Logger
	Cluster                  *keyvalv1alpha1.KeyValCluster
	ResourceKey              string
	Accumulator              *RequeueAccumulator
	Dependencies             Dependencies
	Security                 SecurityState
	Config                   ConfigState
	RedisStatefulSet         *appsv1.StatefulSet
	SentinelStatefulSet      *appsv1.StatefulSet
	RedisPods                []corev1.Pod
	SentinelPods             []corev1.Pod
	RedisTemplateMetadata    PodTemplateMetadata
	SentinelTemplateMetadata PodTemplateMetadata
	StoragePlan              opstorage.ResizeResult
	ConditionOverrides       map[keyvalv1alpha1.ConditionType]*opstatus.ConditionState
	ApplyBackoff             func(time.Duration) time.Duration
	Runtime                  RuntimeState
	Sentinel                 SentinelState
	Status                   StatusState
	Disruption               DisruptionState
	Import                   ImportState
	AbortDirective           *AbortDirective
}

// AbortDirective signals that reconciliation should skip remaining phases after status persistence.
type AbortDirective struct {
	Err error
}

// ensureAccumulator guarantees the per-request accumulator exists and returns it.
// The method is idempotent and safe for repeated calls.
func (s *State) ensureAccumulator() *RequeueAccumulator {
	if s == nil {
		return nil
	}
	if s.Accumulator == nil {
		s.Accumulator = &RequeueAccumulator{}
	}
	return s.Accumulator
}

// EnsureConditionOverrides returns the condition override map, initialising it
// lazily to spare callers from nil checks. The API is stable for other packages.
func (s *State) EnsureConditionOverrides() map[keyvalv1alpha1.ConditionType]*opstatus.ConditionState {
	if s == nil {
		return nil
	}
	if s.ConditionOverrides == nil {
		s.ConditionOverrides = make(map[keyvalv1alpha1.ConditionType]*opstatus.ConditionState)
	}
	return s.ConditionOverrides
}

// RequeueAfter schedules a requeue after the provided duration. Successive calls
// keep the smallest strictly positive delay. The helper is part of the stable API.
func (s *State) RequeueAfter(d time.Duration) {
	if d <= 0 {
		return
	}
	if acc := s.ensureAccumulator(); acc != nil {
		acc.Set(d)
	}
}

// RequeueMin enforces a minimum delay for subsequent requeues. This mirrors the
// legacy accumulator behaviour while avoiding direct manipulation from callers.
func (s *State) RequeueMin(d time.Duration) {
	if d <= 0 {
		return
	}
	if acc := s.ensureAccumulator(); acc != nil {
		acc.SetMin(d)
	}
}

// NextRequeue reports the currently scheduled delay without mutating it.
func (s *State) NextRequeue() time.Duration {
	if acc := s.ensureAccumulator(); acc != nil {
		return acc.Result()
	}
	return 0
}

// StartTrace delegates to the dependency-provided TraceHook if available. It
// returns the derived context and a closer that must be invoked to finish the span.
// When tracing is disabled the original context and a no-op closer are returned.
// This helper is experimental and may evolve.
func (s *State) StartTrace(ctx context.Context, name string) (context.Context, func()) {
	if s == nil || s.Dependencies.TraceHook == nil {
		return ctx, func() {}
	}
	return s.Dependencies.TraceHook(ctx, name)
}

// AbortWithError requests that reconciliation stop after persisting status, returning the provided error.
func (s *State) AbortWithError(err error) {
	if s == nil {
		return
	}
	s.AbortDirective = &AbortDirective{Err: err}
}

// SecurityState captures resolved authentication/TLS settings and client options.
type SecurityState struct {
	Settings              security.Settings
	RedisClientOptions    clients.ClientOptions
	SentinelClientOptions clients.SentinelOptions
	TLSHash               string
}

// ConfigState stores derived configuration metadata for subsequent phases.
type ConfigState struct {
	ConfigHash          string
	RedisRuntimeHash    string
	SentinelRuntimeHash string
}

// RuntimeState stores replication, health, and runtime configuration outputs for downstream phases.
type RuntimeState struct {
	Master             string
	Roles              map[string]keyvalv1alpha1.PodRole
	Infos              map[string]clients.ReplicationInfo
	Topology           opreplication.EnsureResult
	BootstrapCandidate opbootstrap.Candidate
	ReplicationError   error
	NeedSentinelReset  bool
	Health             map[string]keyvalv1alpha1.PodHealth
	LagSeconds         map[string]*int32
	RuntimeResults     []opruntimecfg.Result
	RuntimeCondition   *opstatus.RuntimeCondition
	AnyNotReady        bool
	NeedHealthRetry    bool
	ReadyCount         int
	ActivePods         int
	HealthyReplicas    int
}

// SentinelState captures quorum observations and reset guidance for sentinel mode.
type SentinelState struct {
	Managed      bool
	Ready        int
	QuorumTarget int
	QuorumOK     bool
	Reason       string
	Detail       string
}

// StatusState captures the computed cluster status and its inputs for downstream phases.
type StatusState struct {
	ClusterState opstatus.ClusterState
	Computed     keyvalv1alpha1.KeyValClusterStatus
}

// DisruptionState captures PodDisruptionBudget metadata and disruption allowances.
type DisruptionState struct {
	AllowDisruptions bool
	AllowSentinel    bool
	RedisMin         int32
	SentinelMin      int32
}

// ImportState carries external import progress shared between phases.
type ImportState struct {
	Status *keyvalv1alpha1.ExternalImportStatus
}

// Pipeline wires controller inputs to the reconcile handler while preserving observability contracts.
type Pipeline struct {
	deps    Dependencies
	handler Handler
}

// NewPipeline constructs a pipeline with the supplied dependencies and handler.
func NewPipeline(deps Dependencies, handler Handler) *Pipeline {
	return &Pipeline{deps: deps, handler: handler}
}

// Run executes the reconcile flow, ensuring metrics/backoff semantics remain consistent with the legacy entrypoint.
func (p *Pipeline) Run(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	if p == nil {
		return ctrl.Result{}, controllererrors.WrapFatal(fmt.Errorf("reconcile pipeline is nil"))
	}

	logger := logging.FromContext(ctx)
	if logger.IsZero() {
		logger = p.deps.BaseLogger
	}
	if logger.IsZero() {
		logger = logging.New(nil)
	}
	logger = logger.WithValues("cluster", req.Name, "namespace", req.Namespace, "request", req.NamespacedName.String())
	ctx = logging.IntoContext(ctx, logger)

	state := &State{
		Request:     req,
		Logger:      logger,
		Cluster:     &keyvalv1alpha1.KeyValCluster{},
		ResourceKey: req.NamespacedName.String(),
		Accumulator: &RequeueAccumulator{},
		Dependencies: Dependencies{
			Client:                  p.deps.Client,
			APIReader:               p.deps.APIReader,
			Recorder:                p.deps.Recorder,
			Scheme:                  p.deps.Scheme,
			BaseLogger:              p.deps.BaseLogger,
			ControllerName:          p.deps.ControllerName,
			QueueDepth:              p.deps.QueueDepth,
			ResetBackoff:            p.deps.ResetBackoff,
			ClientFactory:           p.deps.ClientFactory,
			SentinelFactory:         p.deps.SentinelFactory,
			MasterCache:             p.deps.MasterCache,
			EvictionSettings:        p.deps.EvictionSettings,
			ObserveGracefulShutdown: p.deps.ObserveGracefulShutdown,
			TraceHook:               p.deps.TraceHook,
		},
		ConditionOverrides: make(map[keyvalv1alpha1.ConditionType]*opstatus.ConditionState),
	}

	start := time.Now()
	controllerName := p.deps.ControllerName
	if controllerName == "" {
		controllerName = "keyvalcluster"
	}
	if p.deps.QueueDepth != nil {
		if depth := p.deps.QueueDepth(); depth >= 0 {
			opobs.ObserveControllerQueueDepth(controllerName, depth)
		}
	}

	defer func() {
		cluster := state.Cluster
		if cluster == nil {
			return
		}
		if cluster.Name == "" && cluster.Namespace == "" {
			return
		}
		result := opobs.ReconcileResultSuccess
		switch {
		case err != nil && controllererrors.IsFatal(err):
			result = opobs.ReconcileResultError
		case err != nil && controllererrors.IsTransient(err):
			result = opobs.ReconcileResultStalled
		case err != nil:
			result = opobs.ReconcileResultError
		case res.RequeueAfter > 0:
			result = opobs.ReconcileResultStalled
		}
		if err != nil {
			opobs.IncReconcileErrorClass(cluster, controllererrors.Classify(err))
		}
		opobs.IncReconcileResult(cluster, result)
		opobs.ObserveReconcile(cluster, time.Since(start))
	}()

	defer func() {
		if p.deps.ResetBackoff == nil {
			return
		}
		if err != nil || res.RequeueAfter <= 0 {
			p.deps.ResetBackoff(state.ResourceKey)
		}
	}()

	if p.deps.Client == nil {
		return ctrl.Result{}, controllererrors.WrapFatal(fmt.Errorf("pipeline client is nil"))
	}

	if err := p.deps.Client.Get(ctx, req.NamespacedName, state.Cluster); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, controllererrors.WrapTransient(controllererrors.WrapKubeAPI(fmt.Errorf("get KeyValCluster: %w", err)))
	}

	if p.handler == nil {
		return ctrl.Result{}, controllererrors.WrapFatal(fmt.Errorf("pipeline handler is nil"))
	}

	res, err = p.handler.Run(ctx, state)
	return res, err
}
