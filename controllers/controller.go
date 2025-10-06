package controllers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opbootstrap "github.com/ivelok/keyval-operator/controllers/internal/ops/bootstrap"
	opeviction "github.com/ivelok/keyval-operator/controllers/internal/ops/eviction"
	ophealthy "github.com/ivelok/keyval-operator/controllers/internal/ops/health"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
	opruntimecfg "github.com/ivelok/keyval-operator/controllers/internal/ops/runtimeconfig"
	opsentinel "github.com/ivelok/keyval-operator/controllers/internal/ops/sentinel"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	opstorage "github.com/ivelok/keyval-operator/controllers/internal/ops/storage"
	opupdate "github.com/ivelok/keyval-operator/controllers/internal/ops/update"
	reconcilepkg "github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile/phases"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
	"github.com/ivelok/keyval-operator/controllers/internal/ssa"
	"github.com/ivelok/keyval-operator/controllers/logging"
	managerpkg "github.com/ivelok/keyval-operator/controllers/manager"
)

const (
	scaleDirectionUp   = "up"
	scaleDirectionDown = "down"
)

func (r *KeyValClusterReconciler) applyBackoffDelay(cr *keyvalv1alpha1.KeyValCluster, key string, base time.Duration) time.Duration {
	delay := base
	if delay < 0 {
		delay = 0
	}
	if r.backoffs == nil {
		if cr != nil && delay > 0 {
			opobs.ObserveRequeueBackoff(cr, delay)
		}
		return delay
	}
	backoffDelay := r.backoffs.Next(key)
	if backoffDelay > delay {
		delay = backoffDelay
	}
	if cr != nil && delay > 0 {
		opobs.ObserveRequeueBackoff(cr, delay)
	}
	return delay
}

// KeyValClusterReconciler reconciles a KeyValCluster object.
type KeyValClusterReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	// Optional client factory for Redis/Sentinel interactions; when nil, replication and
	// role detection are skipped and labels are derived heuristically.
	ClientFactory ClientFactory
	// Optional sentinel factory used for controlled failover in Sentinel mode
	SentinelFactory         SentinelFactory
	Recorder                record.EventRecorder
	rateLimiter             workqueue.TypedRateLimiter[reconcile.Request]
	baseLogger              logging.Logger
	evictionSettings        opeviction.Settings
	backoffs                *backoffTracker
	masterCache             *opreplication.MasterAddressCache
	maxConcurrentReconciles int
	controllerName          string
	queueDepthMu            sync.RWMutex
	queueDepthFn            func() int
	shutdownMu              sync.Mutex
	shutdownObserved        map[string]struct{}
}

func (r *KeyValClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	deps := reconcilepkg.Dependencies{
		Client:         r.Client,
		APIReader:      r.APIReader,
		Recorder:       r.Recorder,
		Scheme:         r.Scheme,
		BaseLogger:     r.baseLogger,
		ControllerName: r.controllerName,
		QueueDepth: func() int {
			return r.queueDepth()
		},
		ResetBackoff: func(key string) {
			if r.backoffs != nil {
				r.backoffs.Reset(key)
			}
		},
		ClientFactory:    r.ClientFactory,
		SentinelFactory:  r.SentinelFactory,
		MasterCache:      r.masterCache,
		EvictionSettings: r.evictionSettings,
		ObserveGracefulShutdown: func(cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod) {
			r.observeGracefulShutdown(cr, pods)
		},
	}

	pipeline := reconcilepkg.NewPipeline(deps, reconcilepkg.HandlerFunc(r.reconcileClusterImpl))
	return pipeline.Run(ctx, req)
}

func (r *KeyValClusterReconciler) reconcileClusterImpl(ctx context.Context, state *reconcilepkg.State) (res ctrl.Result, err error) {
	logger := state.Logger
	if logger.IsZero() {
		logger = r.baseLogger
	}
	ctx = logging.IntoContext(ctx, logger)
	resourceKey := state.ResourceKey

	cr := *state.Cluster
	defer func() {
		*state.Cluster = cr
	}()

	// Handle deletion via finalizer
	if !cr.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cr, core.FinalizerName) {
			if err := r.finalizeCluster(ctx, &cr); err != nil {
				return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("finalize: %w", err))
			}
			base := cr.DeepCopy()
			controllerutil.RemoveFinalizer(&cr, core.FinalizerName)
			if err := r.Patch(ctx, &cr, client.MergeFrom(base)); err != nil {
				return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("remove finalizer: %w", err))
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer present
	if !controllerutil.ContainsFinalizer(&cr, core.FinalizerName) {
		base := cr.DeepCopy()
		controllerutil.AddFinalizer(&cr, core.FinalizerName)
		if err := r.Patch(ctx, &cr, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("add finalizer: %w", err))
		}
	}

	if err := phases.Security(ctx, state); err != nil {
		return ctrl.Result{}, err
	}
	secSettings := state.Security.Settings
	clientOpts := state.Security.RedisClientOptions
	tlsHash := state.Security.TLSHash

	if err := phases.Config(ctx, state); err != nil {
		return ctrl.Result{}, err
	}
	hash := state.Config.ConfigHash
	if hash == "" {
		hash = resources.ConfigHash(&cr, &secSettings)
	}

	if err := phases.Services(ctx, state); err != nil {
		return ctrl.Result{}, err
	}

	state.ApplyBackoff = func(base time.Duration) time.Duration {
		return r.applyBackoffDelay(&cr, resourceKey, base)
	}

	if err := phases.Workloads(ctx, state); err != nil {
		return ctrl.Result{}, err
	}
	if err := phases.ExternalImport(ctx, state); err != nil {
		if statusErr := phases.Status(ctx, &cr, state); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		result := ctrl.Result{}
		if delay := state.NextRequeue(); delay > 0 {
			result.RequeueAfter = delay
		}
		return result, err
	}
	if state.AbortDirective != nil {
		if err := phases.Status(ctx, &cr, state); err != nil {
			return ctrl.Result{}, err
		}
		result := ctrl.Result{}
		if delay := state.NextRequeue(); delay > 0 {
			result.RequeueAfter = delay
		}
		return result, state.AbortDirective.Err
	}

	pods := state.RedisPods
	if len(pods) == 0 && cr.Spec.RedisReplicas > 0 {
		delay := state.NextRequeue()
		if delay <= 0 {
			delay = r.applyBackoffDelay(&cr, resourceKey, 2*time.Second)
			state.RequeueAfter(delay)
		}
		ssName := ""
		if state.RedisStatefulSet != nil {
			ssName = state.RedisStatefulSet.Name
		} else {
			ssName = resources.StatefulSet(&cr, hash, tlsHash, &secSettings).Name
		}
		logger.Info("No pods created yet, requeueing", "statefulset", ssName, "expectedReplicas", cr.Spec.RedisReplicas, "after", delay)
		return ctrl.Result{RequeueAfter: delay}, nil
	}

	sentinelPods := state.SentinelPods
	storagePlan := opstorage.ResizeResult{}

	if plan, err := opstorage.EnsureResize(ctx, r.Client, &cr, pods, r.Recorder, logger); err != nil {
		return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("ensure storage resize: %w", err))
	} else {
		storagePlan = plan
		state.StoragePlan = storagePlan
		if storagePlan.MinRequeueAfter > 0 {
			state.RequeueMin(storagePlan.MinRequeueAfter)
		}
		if storagePlan.Waiting {
			if storagePlan.RequeueAfter > 0 {
				state.RequeueAfter(storagePlan.RequeueAfter)
			} else {
				state.RequeueAfter(5 * time.Second)
			}
			if len(storagePlan.PendingPVCs) > 0 {
				logger.V(1).Info("waiting for pvc resize", "pendingPVCs", storagePlan.PendingPVCs)
			}
		}
	}

	// Detect roles and ensure replication via runtime phase
	if err := phases.Runtime(ctx, state); err != nil {
		return ctrl.Result{}, err
	}

	runtimeState := state.Runtime
	infoMap := runtimeState.Infos
	if infoMap == nil {
		infoMap = map[string]ReplicationInfo{}
	}
	master := runtimeState.Master
	topology := runtimeState.Topology
	readyCount := runtimeState.ReadyCount
	activePods := runtimeState.ActivePods
	healthStates := runtimeState.Health
	if healthStates == nil {
		healthStates = map[string]keyvalv1alpha1.PodHealth{}
	}
	runtimeResults := runtimeState.RuntimeResults
	prevRolesSource := keyvalv1alpha1.RolesSource(cr.Status.RolesSource)
	newRolesSource := reconcilepkg.EnsureSourceToRolesSource(topology.Source)
	if newRolesSource != "" {
		if topology.Changed {
			opobs.IncFailoverDecision(&cr, newRolesSource)
		} else if prevRolesSource != newRolesSource {
			opobs.IncFailoverDecision(&cr, newRolesSource)
		}
	}
	if prevRolesSource != newRolesSource && newRolesSource == keyvalv1alpha1.RolesSourceProbe && cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
		detail := strings.TrimSpace(topology.Reason)
		if detail == "" {
			detail = "sentinel metadata unavailable; using direct pod probes"
		}
		opobs.EventReplicationHeuristicFallback(r.Recorder, &cr, detail)
	}
	if runtimeState.ReplicationError != nil {
		state.RequeueAfter(3 * time.Second)
		logger.V(1).Info("ensure replication will retry", "after", 3*time.Second, "error", runtimeState.ReplicationError.Error())
	}
	if runtimeState.AnyNotReady {
		state.RequeueAfter(4 * time.Second)
		logger.V(1).Info("redis pods not all ready; scheduling requeue", "ready", readyCount, "total", len(pods), "after", 4*time.Second)
	}
	if runtimeState.NeedHealthRetry {
		if state.NextRequeue() == 0 {
			state.RequeueAfter(5 * time.Second)
			logger.V(1).Info("replica health pending; scheduling refresh", "after", 5*time.Second)
		}
	}
	if err := phases.Sentinel(ctx, state); err != nil {
		return ctrl.Result{}, err
	}
	runtimeState = state.Runtime
	sentinelState := state.Sentinel
	sentinelQuorumOK := sentinelState.QuorumOK
	sentinelQuorumDetail := sentinelState.Detail

	opbootstrap.UpdatePVCFreshness(ctx, r.Client, &cr, pods, infoMap)

	runtimeCond := buildRuntimeCondition(runtimeResults)
	state.Runtime.RuntimeCondition = runtimeCond

	if err := phases.Labels(ctx, state); err != nil {
		return ctrl.Result{}, err
	}
	pods = state.RedisPods

	if err := phases.Status(ctx, &cr, state); err != nil {
		return ctrl.Result{}, err
	}
	wantStatus := state.Status.Computed
	allowDisruptions := state.Disruption.AllowDisruptions
	allowSentinel := state.Disruption.AllowSentinel
	if err := phases.PDB(ctx, state); err != nil {
		return ctrl.Result{}, err
	}

	// Rolling restart orchestrator (delete at most one pod if drift)
	ssDesired := state.RedisStatefulSet
	if ssDesired == nil {
		ssDesired = ssa.StatefulSet(resources.StatefulSet(&cr, hash, tlsHash, &secSettings))
	}
	plan := opupdate.PlanUpdates(ctx, &cr, ssDesired, pods, healthStates, storagePlan.PodReasons)
	scalePhase := ""
	if cr.Annotations != nil {
		scalePhase = cr.Annotations[core.AnnotationScalePhase]
	}
	desiredReplicas := int(cr.Spec.RedisReplicas)
	if desiredReplicas < 0 {
		desiredReplicas = 0
	}
	planIncludesScaleDown := planHasReason(plan, "scale-down")
	detectedDirection := ""
	if desiredReplicas > activePods {
		detectedDirection = scaleDirectionUp
	} else if planIncludesScaleDown || activePods > desiredReplicas {
		detectedDirection = scaleDirectionDown
	}
	if scalePhase == "" && detectedDirection != "" {
		if changed, err := setScalePhaseAnnotation(ctx, r.Client, &cr, detectedDirection); err != nil {
			return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("set scale annotation: %w", err))
		} else if changed {
			logger.Info("scale operation started", "direction", detectedDirection, "current", activePods, "target", desiredReplicas)
			opobs.EventScaleStarted(r.Recorder, &cr, detectedDirection, int32(activePods), cr.Spec.RedisReplicas)
			opobs.SetScaleInProgress(&cr, true)
		}
		scalePhase = detectedDirection
	} else if scalePhase != "" && detectedDirection != "" && detectedDirection != scalePhase {
		if changed, err := setScalePhaseAnnotation(ctx, r.Client, &cr, detectedDirection); err != nil {
			return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("update scale annotation: %w", err))
		} else if changed {
			logger.Info("scale direction changed", "direction", detectedDirection, "current", activePods, "target", desiredReplicas)
			opobs.EventScaleStarted(r.Recorder, &cr, detectedDirection, int32(activePods), cr.Spec.RedisReplicas)
			opobs.SetScaleInProgress(&cr, true)
		}
		scalePhase = detectedDirection
	}
	if scalePhase != "" {
		opobs.SetScaleInProgress(&cr, true)
	}
	if scalePhase != "" {
		completed := false
		switch scalePhase {
		case scaleDirectionUp:
			if activePods == desiredReplicas && readyCount == desiredReplicas && len(plan.PodNames) == 0 {
				completed = true
			}
		case scaleDirectionDown:
			if activePods == desiredReplicas && len(plan.PodNames) == 0 && allowDisruptions {
				completed = true
			}
		}
		if completed {
			if _, prev, err := clearScalePhaseAnnotation(ctx, r.Client, &cr); err != nil {
				return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("clear scale annotation: %w", err))
			} else {
				direction := scalePhase
				if prev != "" {
					direction = prev
				}
				logger.Info("scale operation completed", "direction", direction, "ready", readyCount, "target", desiredReplicas)
				opobs.EventScaleCompleted(r.Recorder, &cr, direction, cr.Spec.RedisReplicas, int32(readyCount))
				opobs.IncScaleOperation(&cr, direction)
				opobs.SetScaleInProgress(&cr, false)
				scalePhase = ""
			}
		}
	}
	guard := opupdate.EvaluateGuards(opupdate.GuardInput{
		AllowDisruptions: wantStatus.HealthGate != nil && wantStatus.HealthGate.AllowDisruptions,
		Status:           &wantStatus,
		Plan:             plan,
		Pods:             pods,
		Health:           healthStates,
		Master:           master,
		DesiredReplicas:  cr.Spec.RedisReplicas,
		Mode:             cr.Spec.Mode,
	})
	if len(plan.PodNames) > 0 {
		if guard.Blocked {
			opobs.SetUpdateInProgress(&cr, false)
			if changed, err := setUpdateBlockedAnnotation(ctx, r.Client, &cr, guard.Reason); err != nil {
				return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("set update-blocked annotation: %w", err))
			} else if changed {
				opobs.IncDisruptionsBlocked(&cr, guard.Reason)
				opobs.EventRollingStepBlocked(r.Recorder, &cr, guard.Reason, guard.Detail)
				if planIncludesScaleDown {
					opobs.EventScaleBlocked(r.Recorder, &cr, scaleDirectionDown, guard.Reason, guard.Detail)
				}
			}
			delay := r.applyBackoffDelay(&cr, resourceKey, guard.RequeueAfter)
			res = ctrl.Result{RequeueAfter: delay}
			return res, nil
		}
		if cleared, prevReason, err := clearUpdateBlockedAnnotation(ctx, r.Client, &cr); err != nil {
			return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("clear update-blocked annotation: %w", err))
		} else if cleared {
			opobs.EventRollingStepResumed(r.Recorder, &cr, prevReason)
		}
		opobs.SetUpdateInProgress(&cr, true)
		// If only the master remains to update and we're in Sentinel mode, perform a controlled failover first
		onlyMasterPending := false
		if master != "" && len(plan.PodNames) == 1 && plan.PodNames[0] == master {
			onlyMasterPending = true
		}
		if onlyMasterPending && cr.Spec.Mode == keyvalv1alpha1.ModeSentinel && r.ClientFactory != nil && r.SentinelFactory != nil {
			if !reconcilepkg.ConditionTrue(&wantStatus, keyvalv1alpha1.ConditionSentinelQuorum) {
				delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
				logger.Info("waiting for sentinel quorum before controlled failover", "cluster", cr.Name, "after", delay)
				res = ctrl.Result{RequeueAfter: delay}
				return res, nil
			}
			if !reconcilepkg.ConditionTrue(&wantStatus, keyvalv1alpha1.ConditionReplicationHealthy) {
				delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
				logger.Info("waiting for replication health before controlled failover", "cluster", cr.Name, "after", delay)
				res = ctrl.Result{RequeueAfter: delay}
				return res, nil
			}
			// Gate: ensure there is at least one "good slave" with master_link_status=up and attached to current master
			if has, detail := hasGoodSlave(ctx, &cr, pods, master, r.ClientFactory, clientOpts); has {
				if !sentinelQuorumOK {
					delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
					logger.Info("sentinel quorum not ready for failover", "cluster", cr.Name, "detail", sentinelQuorumDetail, "after", delay)
					res = ctrl.Result{RequeueAfter: delay}
					return res, nil
				}
				former := master
				opobs.EventStartFailover(r.Recorder, &cr, opobs.FailoverTypeAutomatic, fmt.Sprintf("master=%s", master))
				opobs.EventFailoverTriggered(r.Recorder, &cr, master)
				if newMaster, _, err := opsentinel.TriggerFailover(ctx, &cr, pods, sentinelPods, r.SentinelFactory, r.ClientFactory, &secSettings); err != nil {
					if errors.Is(err, opsentinel.ErrNoGoodSlave) {
						if shouldEmitNoGoodSlaveEvent(&cr) {
							opobs.EventNoGoodSlave(r.Recorder, &cr, "sentinel reported NOGOODSLAVE")
						}
						delay := r.applyBackoffDelay(&cr, resourceKey, 10*time.Second)
						logger.Info("sentinel refused failover: NOGOODSLAVE; waiting for replicas", "master", master, "after", delay)
						res = ctrl.Result{RequeueAfter: delay}
						return res, nil
					}
					logger.Error(err, "controlled failover failed")
					opobs.EventFailoverCompleted(r.Recorder, &cr, "", err)
					opobs.EventNewMaster(r.Recorder, &cr, "", opobs.FailoverTypeAutomatic, err)
					delay := r.applyBackoffDelay(&cr, resourceKey, 7*time.Second)
					res = ctrl.Result{RequeueAfter: delay}
					return res, nil
				} else {
					opobs.EventFailoverCompleted(r.Recorder, &cr, newMaster, nil)
					opobs.EventNewMaster(r.Recorder, &cr, newMaster, opobs.FailoverTypeAutomatic, nil)
					// Mark former master for one-off restart if not captured by plan
					base := cr.DeepCopy()
					if cr.Annotations == nil {
						cr.Annotations = map[string]string{}
					}
					cr.Annotations[core.AnnotationRestartFormerMaster] = former
					if err := r.Patch(ctx, &cr, client.MergeFrom(base)); err != nil {
						logger.Error(err, "annotate former master for restart failed")
					}
				}
				// Requeue to observe new roles and then update the former master as a replica
				delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
				res = ctrl.Result{RequeueAfter: delay}
				return res, nil
			} else {
				if shouldEmitNoGoodSlaveEvent(&cr) {
					opobs.EventNoGoodSlave(r.Recorder, &cr, detail)
				}
				delay := r.applyBackoffDelay(&cr, resourceKey, 7*time.Second)
				logger.Info("waiting for good slave before failover", "detail", detail, "after", delay)
				res = ctrl.Result{RequeueAfter: delay}
				return res, nil
			}
		}
		evictedPod, err := opupdate.Execute(ctx, r.Client, &cr, plan, pods, healthStates, opupdate.ExecuteOptions{
			Logger:           logger,
			Component:        opupdate.ComponentRedis,
			EvictionSettings: r.evictionSettings,
		})
		if err != nil {
			if errors.Is(err, opupdate.ErrEvictionRejected) {
				opobs.SetUpdateInProgress(&cr, false)
				if changed, annErr := setUpdateBlockedAnnotation(ctx, r.Client, &cr, "PDBLimit"); annErr != nil {
					return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("set PDB block annotation: %w", annErr))
				} else if changed {
					opobs.IncDisruptionsBlocked(&cr, "PDBLimit")
					opobs.EventRollingStepBlocked(r.Recorder, &cr, "PDBLimit", "PodDisruptionBudget rejected eviction")
				}
				delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
				res = ctrl.Result{RequeueAfter: delay}
				return res, nil
			}
			return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("execute update plan: %w", err))
		}
		if evictedPod != "" {
			delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
			logger.Info("evicted pod for rolling update", "pod", evictedPod, "reasons", plan.Reasons[evictedPod], "after", delay)
			opobs.EventPodEvicted(r.Recorder, &cr, evictedPod, plan.Reasons[evictedPod])
			res = ctrl.Result{RequeueAfter: delay}
			return res, nil
		}
		// If nothing deleted and we marked a former master to restart, try a one-off safe deletion
		if cr.Annotations != nil {
			if fm := cr.Annotations[core.AnnotationRestartFormerMaster]; fm != "" {
				evicted, evictErr := r.tryEvictFormerMaster(ctx, logger, &cr, fm, pods)
				if evictErr != nil {
					if errors.Is(evictErr, opeviction.ErrRejected) {
						if changed, annErr := setUpdateBlockedAnnotation(ctx, r.Client, &cr, "PDBLimit"); annErr != nil {
							return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("set PDB block annotation: %w", annErr))
						} else if changed {
							opobs.IncDisruptionsBlocked(&cr, "PDBLimit")
							opobs.EventRollingStepBlocked(r.Recorder, &cr, "PDBLimit", "PodDisruptionBudget rejected eviction")
						}
						delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
						logger.Info("former master eviction blocked", "pod", fm, "after", delay)
						res = ctrl.Result{RequeueAfter: delay}
						return res, nil
					}
					return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("evict former master %s: %w", fm, evictErr))
				}
				if evicted {
					base := cr.DeepCopy()
					delete(cr.Annotations, core.AnnotationRestartFormerMaster)
					if len(cr.Annotations) == 0 {
						cr.Annotations = nil
					}
					if err := r.Patch(ctx, &cr, client.MergeFrom(base)); err != nil {
						logger.Error(err, "clear restart-former-master annotation failed")
					}
					delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
					logger.Info("evicted former master for restart", "pod", fm, "after", delay)
					opobs.EventPodEvicted(r.Recorder, &cr, fm, []string{"restart-former-master"})
					res = ctrl.Result{RequeueAfter: delay}
					return res, nil
				}
			}
		}
	} else {
		opobs.SetUpdateInProgress(&cr, false)
		if cleared, prevReason, err := clearUpdateBlockedAnnotation(ctx, r.Client, &cr); err != nil {
			return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("clear update-blocked annotation: %w", err))
		} else if cleared {
			opobs.EventRollingStepResumed(r.Recorder, &cr, prevReason)
		}
	}

	// Sentinel rolling plan (Dedicated mode)
	if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel && len(sentinelPods) > 0 {
		ssSent := resources.SentinelStatefulSet(&cr, hash, tlsHash, &secSettings)
		sp := opupdate.PlanUpdates(ctx, &cr, ssSent, sentinelPods, healthStates, nil)
		if len(sp.PodNames) > 1 {
			sort.Slice(sp.PodNames, func(i, j int) bool { return core.Ordinal(sp.PodNames[i]) < core.Ordinal(sp.PodNames[j]) })
		}
		if len(sp.PodNames) > 0 {
			statusForGuard := wantStatus
			if allowSentinel {
				mutated := false
				for i := range statusForGuard.Conditions {
					cond := statusForGuard.Conditions[i]
					if cond.Status != metav1.ConditionTrue {
						continue
					}
					switch cond.Type {
					case string(keyvalv1alpha1.ConditionDisruptionsPaused), string(keyvalv1alpha1.ConditionBootstrapInProgress):
						if cond.Reason != "SentinelQuorum" {
							continue
						}
						if !mutated {
							statusForGuard.Conditions = append([]metav1.Condition(nil), statusForGuard.Conditions...)
							mutated = true
						}
						statusForGuard.Conditions[i].Status = metav1.ConditionFalse
						statusForGuard.Conditions[i].Reason = "SentinelRecovery"
						statusForGuard.Conditions[i].Message = cond.Message
					}
				}
			}
			sGuard := opupdate.EvaluateGuards(opupdate.GuardInput{
				AllowDisruptions: allowSentinel,
				Status:           &statusForGuard,
				Plan:             sp,
				Pods:             pods,
				Health:           healthStates,
				Master:           master,
				DesiredReplicas:  cr.Spec.RedisReplicas,
				Mode:             cr.Spec.Mode,
			})
			if sGuard.Blocked {
				opobs.SetUpdateInProgress(&cr, false)
				if changed, err := setUpdateBlockedAnnotation(ctx, r.Client, &cr, sGuard.Reason); err != nil {
					return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("set update-blocked annotation: %w", err))
				} else if changed {
					opobs.IncDisruptionsBlocked(&cr, sGuard.Reason)
					opobs.EventRollingStepBlocked(r.Recorder, &cr, sGuard.Reason, sGuard.Detail)
				}
				delay := r.applyBackoffDelay(&cr, resourceKey, sGuard.RequeueAfter)
				res = ctrl.Result{RequeueAfter: delay}
				return res, nil
			}
			if cleared, prevReason, err := clearUpdateBlockedAnnotation(ctx, r.Client, &cr); err != nil {
				return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("clear update-blocked annotation: %w", err))
			} else if cleared {
				opobs.EventRollingStepResumed(r.Recorder, &cr, prevReason)
			}
			opobs.SetUpdateInProgress(&cr, true)
			evictedPod, err := opupdate.Execute(ctx, r.Client, &cr, sp, sentinelPods, healthStates, opupdate.ExecuteOptions{
				Logger:           logger,
				Component:        opupdate.ComponentSentinel,
				EvictionSettings: r.evictionSettings,
			})
			if err != nil {
				if errors.Is(err, opupdate.ErrEvictionRejected) {
					opobs.SetUpdateInProgress(&cr, false)
					if changed, annErr := setUpdateBlockedAnnotation(ctx, r.Client, &cr, "PDBLimit"); annErr != nil {
						return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("set PDB block annotation: %w", annErr))
					} else if changed {
						opobs.IncDisruptionsBlocked(&cr, "PDBLimit")
						opobs.EventRollingStepBlocked(r.Recorder, &cr, "PDBLimit", "PodDisruptionBudget rejected eviction")
					}
					delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
					res = ctrl.Result{RequeueAfter: delay}
					return res, nil
				}
				return ctrl.Result{}, controllererrors.WrapTransient(fmt.Errorf("execute sentinel update plan: %w", err))
			}
			if evictedPod != "" {
				delay := r.applyBackoffDelay(&cr, resourceKey, 5*time.Second)
				logger.Info("evicted sentinel pod for rolling update", "pod", evictedPod, "reasons", sp.Reasons[evictedPod], "after", delay)
				opobs.EventPodEvicted(r.Recorder, &cr, evictedPod, sp.Reasons[evictedPod])
				res = ctrl.Result{RequeueAfter: delay}
				return res, nil
			}
			opobs.SetUpdateInProgress(&cr, false)
		}
	}

	if after := state.NextRequeue(); after > 0 {
		delay := r.applyBackoffDelay(&cr, resourceKey, after)
		logger.V(1).Info("pod lifecycle pending; requeue scheduled", "after", delay)
		res = ctrl.Result{RequeueAfter: delay}
		return res, nil
	}
	logger.V(1).Info("reconciled headless service, configmap, statefulset; no rolling action")
	return ctrl.Result{}, nil
}

// goodSlaveEventCooldown tracks last emission per cluster to avoid event spam
var goodSlaveEventCooldown = struct {
	mu   sync.Mutex
	last map[string]time.Time
}{last: map[string]time.Time{}}

func keyForCR(cr *keyvalv1alpha1.KeyValCluster) string { return cr.Namespace + "/" + cr.Name }

func shouldEmitNoGoodSlaveEvent(cr *keyvalv1alpha1.KeyValCluster) bool {
	k := keyForCR(cr)
	now := time.Now()
	goodSlaveEventCooldown.mu.Lock()
	defer goodSlaveEventCooldown.mu.Unlock()
	if t, ok := goodSlaveEventCooldown.last[k]; ok && now.Sub(t) < 30*time.Second {
		return false
	}
	goodSlaveEventCooldown.last[k] = now
	return true
}

func (r *KeyValClusterReconciler) observeGracefulShutdown(cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod) {
	if cr == nil || len(pods) == 0 {
		return
	}
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp == nil {
			continue
		}
		component := "redis"
		if pod.Labels[core.LabelAppKey] == core.SentinelAppLabel(cr) {
			component = "sentinel"
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != core.RedisContainerName && status.Name != core.SentinelContainerName {
				continue
			}
			term := status.State.Terminated
			if term == nil || term.FinishedAt.IsZero() {
				continue
			}
			key := fmt.Sprintf("%s/%s@%d", pod.Namespace, pod.Name, term.FinishedAt.UnixNano())
			r.shutdownMu.Lock()
			if r.shutdownObserved == nil {
				r.shutdownObserved = make(map[string]struct{})
			}
			if _, exists := r.shutdownObserved[key]; exists {
				r.shutdownMu.Unlock()
				continue
			}
			if len(r.shutdownObserved) > 2048 {
				for recorded := range r.shutdownObserved {
					delete(r.shutdownObserved, recorded)
					if len(r.shutdownObserved) <= 1024 {
						break
					}
				}
			}
			r.shutdownObserved[key] = struct{}{}
			r.shutdownMu.Unlock()

			finished := term.FinishedAt.Time
			start := pod.DeletionTimestamp.Time
			duration := finished.Sub(start)
			if duration < 0 {
				duration = 0
			}
			opobs.ObserveGracefulShutdown(cr, component, pod.Name, duration)
			if term.ExitCode != 0 {
				detail := term.Message
				if strings.TrimSpace(detail) == "" {
					detail = term.Reason
				}
				opobs.EventRedisPreStopFailed(r.Recorder, cr, pod.Name, detail)
			}
		}
	}
}

// hasGoodSlave returns whether there is at least one ready replica with link up to the current master.
func hasGoodSlave(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod, master string, cf ClientFactory, opts ClientOptions) (bool, string) {
	masterIP := ""
	for _, p := range pods {
		if p.Name == master {
			masterIP = p.Status.PodIP
			break
		}
	}
	if masterIP == "" {
		masterIP = core.PodDNSName(cr, master)
	}
	rpInt, _ := core.RedisPort(cr)
	detail := "no replica with master_link_status up and attached to current master"
	threshold := ophealthy.LagThresholdSeconds(cr)
	for _, p := range pods {
		if p.Name == master {
			continue
		}
		if !runtimepkg.IsPodReady(&p) {
			continue
		}
		c, err := cf.ForPod(ctx, p, opts)
		if err != nil {
			continue
		}
		infoCtx, cancel := runtimepkg.WithTimeout(ctx, 2*time.Second)
		info, err := c.ReplicationInfo(infoCtx)
		cancel()
		if err != nil {
			continue
		}
		if strings.EqualFold(info.Role, "master") {
			continue
		}
		if !opreplication.HostMatchesMaster(info.MasterHost, master, masterIP, pods) || (info.MasterPort != 0 && info.MasterPort != rpInt) {
			detail = fmt.Sprintf("replica %s not following current master", p.Name)
			continue
		}
		health, _ := ophealthy.EvaluatePodHealth(keyvalv1alpha1.PodRoleReplica, info, true, threshold)
		if health == keyvalv1alpha1.PodHealthHealthy {
			return true, ""
		}
		detail = fmt.Sprintf("replica %s health=%s", p.Name, health)
	}
	return false, detail
}

func buildRuntimeCondition(results []opruntimecfg.Result) *opstatus.RuntimeCondition {
	if len(results) == 0 {
		return nil
	}
	messages := make([]string, 0, len(results))
	status := metav1.ConditionTrue
	reason := "RuntimeConfigInSync"
	for _, res := range results {
		if res.Component == "" && res.Mode == "" && res.Message == "" {
			continue
		}
		if res.Message != "" {
			component := res.Component
			if component != "" {
				messages = append(messages, fmt.Sprintf("%s: %s", component, res.Message))
			} else {
				messages = append(messages, res.Message)
			}
		}
		switch res.Mode {
		case opruntimecfg.ModeFailed:
			status = metav1.ConditionFalse
			reason = fmt.Sprintf("%sRuntimeFailed", capitalize(res.Component))
		case opruntimecfg.ModeNeedsRestart:
			if status != metav1.ConditionFalse {
				status = metav1.ConditionFalse
				reason = fmt.Sprintf("%sRequiresRestart", capitalize(res.Component))
			}
		case opruntimecfg.ModeSkipped:
			if status == metav1.ConditionTrue {
				status = metav1.ConditionUnknown
				reason = fmt.Sprintf("%sRuntimeSkipped", capitalize(res.Component))
			}
		}
	}
	if len(messages) == 0 {
		switch status {
		case metav1.ConditionTrue:
			messages = append(messages, "runtime config in sync")
		case metav1.ConditionUnknown:
			messages = append(messages, "runtime config synchronization skipped")
		}
	}
	return &opstatus.RuntimeCondition{
		Status:  status,
		Reason:  reason,
		Message: strings.Join(messages, "; "),
	}
}

func capitalize(s string) string {
	if s == "" {
		return ""
	}
	if len(s) == 1 {
		return strings.ToUpper(s)
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// tryEvictFormerMaster attempts to evict the annotated former master pod using the eviction helper.
func (r *KeyValClusterReconciler) tryEvictFormerMaster(ctx context.Context, logger logging.Logger, cr *keyvalv1alpha1.KeyValCluster, name string, pods []corev1.Pod) (bool, error) {
	if cr == nil || name == "" {
		return false, nil
	}
	var pod *corev1.Pod
	for i := range pods {
		if pods[i].Name == name {
			pod = &pods[i]
			break
		}
	}
	if pod == nil {
		return false, nil
	}
	if pod.DeletionTimestamp != nil {
		return false, nil
	}
	readyOthers := 0
	for i := range pods {
		if pods[i].Name == name {
			continue
		}
		if runtimepkg.IsPodReady(&pods[i]) {
			readyOthers++
		}
	}
	if readyOthers == 0 {
		return false, nil
	}
	var current corev1.Pod
	if err := r.Get(ctx, client.ObjectKeyFromObject(pod), &current); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get former master pod %s: %w", name, err)
	}
	if logger.IsZero() {
		logger = r.baseLogger
	}
	evictLogger := logger.WithValues("pod", current.Name, "operation", "restart-former-master")
	evictor := opeviction.NewManager(r.Client, evictLogger, nil)
	req := opeviction.Request{
		Cluster:   cr,
		Pod:       &current,
		Component: opupdate.ComponentRedis,
		Settings:  r.evictionSettings,
	}
	if err := evictor.Evict(ctx, req); err != nil {
		return false, err
	}
	opobs.IncRollingDeletions(cr)
	return true, nil
}

func planHasReason(plan opupdate.Plan, reason string) bool {
	if reason == "" {
		return false
	}
	for _, reasons := range plan.Reasons {
		for _, r := range reasons {
			if r == reason {
				return true
			}
		}
	}
	return false
}

func setScalePhaseAnnotation(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, phase string) (bool, error) {
	if phase == "" {
		return false, nil
	}
	current := ""
	if cr.Annotations != nil {
		current = cr.Annotations[core.AnnotationScalePhase]
	}
	if current == phase {
		return false, nil
	}
	base := cr.DeepCopy()
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	cr.Annotations[core.AnnotationScalePhase] = phase
	if err := c.Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		cr.Annotations = base.Annotations
		return false, err
	}
	return true, nil
}

func clearScalePhaseAnnotation(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster) (bool, string, error) {
	current := ""
	if cr.Annotations != nil {
		current = cr.Annotations[core.AnnotationScalePhase]
	}
	if current == "" {
		return false, "", nil
	}
	base := cr.DeepCopy()
	delete(cr.Annotations, core.AnnotationScalePhase)
	if len(cr.Annotations) == 0 {
		cr.Annotations = nil
	}
	if err := c.Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		cr.Annotations = base.Annotations
		return false, current, err
	}
	return true, current, nil
}

func setUpdateBlockedAnnotation(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, reason string) (bool, error) {
	if reason == "" {
		reason = "unknown"
	}
	current := ""
	if cr.Annotations != nil {
		current = cr.Annotations[core.AnnotationUpdateBlocked]
	}
	if current == reason {
		return false, nil
	}
	base := cr.DeepCopy()
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	cr.Annotations[core.AnnotationUpdateBlocked] = reason
	if err := c.Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		cr.Annotations = base.Annotations
		return false, err
	}
	return true, nil
}

func clearUpdateBlockedAnnotation(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster) (bool, string, error) {
	current := ""
	if cr.Annotations != nil {
		current = cr.Annotations[core.AnnotationUpdateBlocked]
	}
	if current == "" {
		return false, "", nil
	}
	base := cr.DeepCopy()
	delete(cr.Annotations, core.AnnotationUpdateBlocked)
	if len(cr.Annotations) == 0 {
		cr.Annotations = nil
	}
	if err := c.Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		cr.Annotations = base.Annotations
		return false, current, err
	}
	return true, current, nil
}

// SetupWithManager wires the controller into the manager.
func (r *KeyValClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("keyval-operator")
	}
	controllerName := r.controllerName
	if controllerName == "" {
		controllerName = "keyvalcluster"
	}

	cfg := managerpkg.SetupConfig{
		Manager:                 mgr,
		Reconciler:              r,
		ControllerName:          controllerName,
		RateLimiter:             r.rateLimiterOrDefault(),
		MaxConcurrentReconciles: r.maxConcurrentReconciles,
		SetQueueDepth:           r.setQueueDepthProvider,
		SecretMap:               managerpkg.TLSSecretMapper(r.Client),
		PodMap:                  managerpkg.PodToClusterMapper(),
		PodPredicate:            managerpkg.PodLifecyclePredicate(),
	}

	return managerpkg.SetupWithManager(cfg)
}

func (r *KeyValClusterReconciler) setQueueDepthProvider(fn func() int) {
	r.queueDepthMu.Lock()
	defer r.queueDepthMu.Unlock()
	r.queueDepthFn = fn
}

func (r *KeyValClusterReconciler) queueDepth() int {
	r.queueDepthMu.RLock()
	fn := r.queueDepthFn
	r.queueDepthMu.RUnlock()
	if fn == nil {
		return 0
	}
	return fn()
}

func (r *KeyValClusterReconciler) rateLimiterOrDefault() workqueue.TypedRateLimiter[reconcile.Request] {
	if r.rateLimiter != nil {
		return r.rateLimiter
	}
	return defaultRateLimiter()
}
