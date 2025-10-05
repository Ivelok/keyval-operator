package finalizer

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metautil "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
	opstorage "github.com/ivelok/keyval-operator/controllers/internal/ops/storage"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

const (
	defaultCleanupTimeout    = 2 * time.Minute
	defaultOperationTimeout  = 20 * time.Second
	defaultCleanupBatchLimit = 3
)

// Handler represents a finalizer pipeline executor.
type Handler interface {
	Run(context.Context, *State) error
}

// HandlerFunc adapts a function into a Handler.
type HandlerFunc func(context.Context, *State) error

// Run executes the underlying function.
func (f HandlerFunc) Run(ctx context.Context, state *State) error {
	return f(ctx, state)
}

// Dependencies captures controller inputs required by the finalizer pipeline.
type Dependencies struct {
	Client            client.Client
	APIReader         client.Reader
	Recorder          record.EventRecorder
	Logger            logging.Logger
	ClientFactory     clients.Factory
	Now               func() time.Time
	CleanupTimeout    time.Duration
	OperationTimeout  time.Duration
	CleanupBatchLimit int
}

// State carries request scoped data for the finalizer pipeline.
type State struct {
	Cluster      *keyvalv1alpha1.KeyValCluster
	Logger       logging.Logger
	Policy       string
	Dependencies Dependencies
	StartedAt    time.Time
}

// Pipeline orchestrates the finalizer execution.
type Pipeline struct {
	deps    Dependencies
	handler Handler
}

// NewPipeline returns a configured finalizer pipeline.
func NewPipeline(deps Dependencies, handler Handler) *Pipeline {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.CleanupTimeout <= 0 {
		deps.CleanupTimeout = defaultCleanupTimeout
	}
	if deps.OperationTimeout <= 0 {
		deps.OperationTimeout = defaultOperationTimeout
	}
	if deps.CleanupBatchLimit <= 0 {
		deps.CleanupBatchLimit = defaultCleanupBatchLimit
	}
	return &Pipeline{deps: deps, handler: handler}
}

// Run executes the pipeline with the provided cluster.
func (p *Pipeline) Run(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster) error {
	if p == nil {
		return fmt.Errorf("finalizer pipeline is nil")
	}
	if cr == nil {
		return nil
	}

	logger := logging.FromContext(ctx)
	if logger.IsZero() {
		logger = p.deps.Logger
	}
	if logger.IsZero() {
		logger = logging.New(nil)
	}
	logger = logger.WithValues("cluster", cr.Name, "namespace", cr.Namespace)
	ctx = logging.IntoContext(ctx, logger)

	state := &State{
		Cluster:      cr,
		Logger:       logger,
		Policy:       storageCleanupPolicy(cr),
		Dependencies: p.deps,
	}

	if p.handler == nil {
		return fmt.Errorf("finalizer handler is nil")
	}
	return p.handler.Run(ctx, state)
}

// Execute is the default handler used by the controller finalizer.
func Execute(ctx context.Context, state *State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	cr := state.Cluster
	logger := state.Logger
	deps := state.Dependencies

	alignReplication(ctx, state)

	if !core.HasPersistentData(cr) {
		state.clearFinalizerStart(ctx)
		state.setStorageCleanupCondition(ctx, metav1.ConditionTrue, "EphemeralStorage", "Ephemeral storage configured; no PVC cleanup required")
		return nil
	}
	if !core.StorageCleanupEnabled(cr) {
		state.clearFinalizerStart(ctx)
		state.setStorageCleanupCondition(ctx, metav1.ConditionTrue, "RetentionPolicy", "PVC cleanup disabled; resources retained after deletion")
		return nil
	}
	// Ensure disruption budgets do not block pod termination while cleanup proceeds.
	unlockDisruptions(ctx, state)

	startedAt, err := state.ensureFinalizerStart(ctx)
	if err != nil {
		state.setStorageCleanupCondition(ctx, metav1.ConditionFalse, "MarkStartFailed", fmt.Sprintf("failed to record finalizer start: %v", err))
		return fmt.Errorf("mark finalizer start: %w", err)
	}

	cleanupCtx, cancel := context.WithTimeout(ctx, deps.OperationTimeout)
	defer cancel()

	if err := drainStatefulSets(cleanupCtx, state); err != nil {
		state.setStorageCleanupCondition(ctx, metav1.ConditionFalse, "ScaleDownFailed", err.Error())
		opobs.IncStorageCleanupFailure(cr, state.Policy, "scale_down")
		return fmt.Errorf("drain statefulsets: %w", err)
	}

	result, err := opstorage.CleanupPVCs(cleanupCtx, deps.Client, cr, deps.CleanupBatchLimit, deps.Recorder, logger)
	if err != nil {
		state.setStorageCleanupCondition(ctx, metav1.ConditionFalse, "DeletionFailed", fmt.Sprintf("PVC delete failed: %v", err))
		opobs.IncStorageCleanupFailure(cr, state.Policy, "delete_error")
		return fmt.Errorf("cleanup pvcs: %w", err)
	}

	if len(result.Pending) == 0 {
		duration := deps.Now().Sub(startedAt)
		state.clearFinalizerStart(ctx)
		state.setStorageCleanupCondition(ctx, metav1.ConditionTrue, "PVCsDeleted", fmt.Sprintf("removed %d PVCs", len(result.Deleted)))
		opobs.EventStorageCleanupFinished(deps.Recorder, cr)
		opobs.ObserveStorageFinalizerDuration(cr, state.Policy, "completed", duration)
		logger.Info("PVC cleanup completed", "deleted", result.Deleted, "duration", duration.String())
		return nil
	}

	duration := deps.Now().Sub(startedAt)
	pendingSummary := summarizePVCs(result.Pending, 5)
	message := fmt.Sprintf("pending PVC deletions: %s", pendingSummary)
	existing := metautil.FindStatusCondition(cr.Status.Conditions, string(keyvalv1alpha1.ConditionStorageCleanup))
	if existing != nil && existing.Message == message {
		logger.V(1).Info("waiting for PVC deletion", "pending", result.Pending)
	} else {
		logger.Info("waiting for PVC deletion", "pending", result.Pending)
	}
	state.setStorageCleanupCondition(ctx, metav1.ConditionFalse, "DeletingPVCs", message)

	if duration > deps.CleanupTimeout {
		state.clearFinalizerStart(ctx)
		opobs.EventStorageCleanupTimedOut(deps.Recorder, cr, result.Pending)
		opobs.ObserveStorageFinalizerDuration(cr, state.Policy, "timeout", duration)
		opobs.IncStorageCleanupFailure(cr, state.Policy, "timeout")
		logger.Error(fmt.Errorf("PVC cleanup timed out"), "storage cleanup timeout", "pending", result.Pending, "timeout", deps.CleanupTimeout)
		return nil
	}
	return fmt.Errorf("waiting for pvc cleanup: %v", result.Pending)
}

func alignReplication(ctx context.Context, state *State) {
	if state == nil || state.Cluster == nil {
		return
	}
	deps := state.Dependencies
	if deps.Client == nil || deps.ClientFactory == nil {
		return
	}

	selector := labels.SelectorFromSet(map[string]string{core.LabelAppKey: core.AppLabel(state.Cluster)})
	var pods corev1.PodList
	if err := deps.Client.List(ctx, &pods, &client.ListOptions{Namespace: state.Cluster.Namespace, LabelSelector: selector}); err != nil {
		if !state.Logger.IsZero() {
			state.Logger.V(1).Info("skip replication alignment", "reason", "list pods", "error", err)
		}
		return
	}
	if len(pods.Items) == 0 {
		return
	}

	reader := deps.APIReader
	if reader == nil {
		reader = deps.Client
	}
	settings, err := security.FromSpec(ctx, reader, state.Cluster)
	if err != nil {
		if !state.Logger.IsZero() {
			state.Logger.V(1).Info("skip replication alignment", "reason", "security", "error", err)
		}
		return
	}
	_, _, _, _, err = opreplication.EnsureReplication(ctx, state.Cluster, pods.Items, deps.ClientFactory, &settings)
	if err != nil && !state.Logger.IsZero() {
		state.Logger.V(1).Info("ensure replication failed", "error", err)
	}
}

func unlockDisruptions(ctx context.Context, state *State) {
	if state == nil || state.Cluster == nil {
		return
	}
	deps := state.Dependencies
	if deps.Client == nil {
		return
	}
	logger := state.Logger
	cr := state.Cluster

	// Remove Redis PDB to allow StatefulSet to scale down fully.
	redisPDB := resources.RedisPDB(cr, 0)
	if err := deps.Client.Delete(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: redisPDB.Name, Namespace: redisPDB.Namespace}}); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete redis pdb during cleanup")
	}

	if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
		sentinelPDB := resources.SentinelPDB(cr, 0)
		if err := deps.Client.Delete(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: sentinelPDB.Name, Namespace: sentinelPDB.Namespace}}); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete sentinel pdb during cleanup")
		}
	}
}

func (s *State) ensureFinalizerStart(ctx context.Context) (time.Time, error) {
	if s == nil || s.Cluster == nil {
		return time.Time{}, nil
	}
	cr := s.Cluster
	if cr.Annotations != nil {
		if ts := cr.Annotations[core.AnnotationFinalizerStarted]; ts != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				return parsed, nil
			}
		}
	}
	started := s.Dependencies.Now().UTC()
	base := cr.DeepCopy()
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	cr.Annotations[core.AnnotationFinalizerStarted] = started.Format(time.RFC3339Nano)
	if err := s.Dependencies.Client.Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		return time.Time{}, err
	}
	opobs.EventStorageCleanupStarted(s.Dependencies.Recorder, cr, "deletePVCs")
	return started, nil
}

func (s *State) clearFinalizerStart(ctx context.Context) {
	if s == nil || s.Cluster == nil {
		return
	}
	cr := s.Cluster
	if cr.Annotations == nil {
		return
	}
	if _, ok := cr.Annotations[core.AnnotationFinalizerStarted]; !ok {
		return
	}
	base := cr.DeepCopy()
	delete(cr.Annotations, core.AnnotationFinalizerStarted)
	_ = s.Dependencies.Client.Patch(ctx, cr, client.MergeFrom(base))
}

func (s *State) setStorageCleanupCondition(ctx context.Context, status metav1.ConditionStatus, reason, message string) {
	if s == nil || s.Cluster == nil {
		return
	}
	cr := s.Cluster
	base := cr.DeepCopy()
	if reason == "" {
		reason = "Unknown"
	}
	cond := metav1.Condition{
		Type:               string(keyvalv1alpha1.ConditionStorageCleanup),
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cr.Generation,
	}
	metautil.SetStatusCondition(&cr.Status.Conditions, cond)
	if err := s.Dependencies.Client.Status().Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return
		}
		if !s.Logger.IsZero() {
			s.Logger.Error(err, "failed to patch storage cleanup condition", "reason", reason)
		}
	}
}

func storageCleanupPolicy(cr *keyvalv1alpha1.KeyValCluster) string {
	if cr == nil {
		return "unknown"
	}
	if !core.HasPersistentData(cr) {
		return "ephemeral"
	}
	if core.StorageCleanupEnabled(cr) {
		return "delete"
	}
	return "retain"
}

func summarizePVCs(names []string, limit int) string {
	if len(names) == 0 {
		return ""
	}
	if limit <= 0 || len(names) <= limit {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s (+%d)", strings.Join(names[:limit], ", "), len(names)-limit)
}

func drainStatefulSets(ctx context.Context, state *State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}
	deps := state.Dependencies
	if deps.Client == nil {
		return nil
	}
	cr := state.Cluster
	selector := labels.SelectorFromSet(map[string]string{core.LabelClusterKey: cr.Name})
	var stsList appsv1.StatefulSetList
	if err := deps.Client.List(ctx, &stsList, &client.ListOptions{Namespace: cr.Namespace, LabelSelector: selector}); err != nil {
		return err
	}
	zero := int32(0)
	for i := range stsList.Items {
		sts := stsList.Items[i]
		if sts.Spec.Replicas != nil && *sts.Spec.Replicas == zero {
			continue
		}
		patched := sts.DeepCopy()
		patched.Spec.Replicas = &zero
		if err := deps.Client.Patch(ctx, patched, client.MergeFrom(&sts)); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if !state.Logger.IsZero() {
			state.Logger.V(1).Info("scaled statefulset to zero for cleanup", "statefulset", sts.Name)
		}
	}

	return wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		var pods corev1.PodList
		if err := deps.Client.List(ctx, &pods, &client.ListOptions{Namespace: cr.Namespace, LabelSelector: selector}); err != nil {
			return false, err
		}
		if len(pods.Items) == 0 {
			return true, nil
		}
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp == nil {
				return false, nil
			}
		}
		return true, nil
	})
}
