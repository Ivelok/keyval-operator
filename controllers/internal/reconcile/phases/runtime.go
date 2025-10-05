package phases

import (
	"context"
	"fmt"
	"time"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/clients"
	opbootstrap "github.com/ivelok/keyval-operator/controllers/internal/ops/bootstrap"
	ophealthy "github.com/ivelok/keyval-operator/controllers/internal/ops/health"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
	opruntimecfg "github.com/ivelok/keyval-operator/controllers/internal/ops/runtimeconfig"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Runtime evaluates replication topology, per-pod health, and runtime configuration.
func Runtime(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	cr := state.Cluster
	logger := state.Logger
	pods := state.RedisPods
	sentinelPods := state.SentinelPods
	deps := state.Dependencies
	overrides := state.EnsureConditionOverrides()
	prevStatus := cr.Status

	runtime := reconcile.RuntimeState{
		Health:     make(map[string]keyvalv1alpha1.PodHealth, len(pods)+len(sentinelPods)),
		LagSeconds: make(map[string]*int32, len(pods)+len(sentinelPods)),
	}

	// Track ready/active pods for downstream scaling logic.
	for i := range pods {
		pod := pods[i]
		if pod.DeletionTimestamp == nil {
			runtime.ActivePods++
		}
		if runtimepkg.IsPodReady(&pod) {
			runtime.ReadyCount++
		}
	}
	if len(pods) > 0 && runtime.ReadyCount < len(pods) {
		runtime.AnyNotReady = true
		state.RequeueAfter(4 * time.Second)
	}

	conditionOverrides := overrides

	var (
		roles              map[string]keyvalv1alpha1.PodRole
		infos              map[string]clients.ReplicationInfo
		master             string
		replicationErr     error
		topology           opreplication.EnsureResult
		bootstrapCandidate opbootstrap.Candidate
		needSentinelReset  bool
	)

	if deps.ClientFactory != nil {
		req := opreplication.EnsureRequest{
			Cluster:         cr,
			RedisPods:       pods,
			SentinelPods:    sentinelPods,
			Factory:         deps.ClientFactory,
			SentinelFactory: deps.SentinelFactory,
			Security:        &state.Security.Settings,
			MasterCache:     deps.MasterCache,
		}
		res, err := opreplication.EnsureTopology(ctx, req)
		if err != nil && runtime.ReadyCount == 0 {
			if deps.Client != nil {
				cand := opbootstrap.SelectCandidate(ctx, deps.Client, cr, pods)
				if cand.Name != "" {
					req.ForceMaster = cand.Name
					req.ForceReason = cand.Source
					bootstrapCandidate = cand
					res, err = opreplication.EnsureTopology(ctx, req)
					if err == nil {
						topology = res
						needSentinelReset = true
						conditionOverrides[keyvalv1alpha1.ConditionBootstrapInProgress] = &opstatus.ConditionState{
							Status:  metav1.ConditionTrue,
							Reason:  "BootstrapSelectingMaster",
							Message: fmt.Sprintf("promoting %s via %s", cand.Name, cand.Source),
						}
						conditionOverrides[keyvalv1alpha1.ConditionDisruptionsPaused] = &opstatus.ConditionState{
							Status:  metav1.ConditionTrue,
							Reason:  "Bootstrap",
							Message: "bootstrap master selection in progress",
						}
					}
				}
			}
		}
		if err != nil {
			logger.Error(err, "ensure replication topology failed")
			if conditionOverrides[keyvalv1alpha1.ConditionBootstrapInProgress] != nil {
				opobs.IncBootstrapFailure(cr)
			}
			replicationErr = err
		} else {
			topology = res
			roles = res.Roles
			infos = res.Infos
			master = res.Master
			if res.Changed {
				opobs.EventReplicationAligned(deps.Recorder, cr, master)
				if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
					needSentinelReset = true
				}
			}
			if len(res.Drift) > 0 {
				opobs.EventReplicationDrift(deps.Recorder, cr, res.Drift)
			}
			if len(res.Pending) > 0 {
				state.RequeueAfter(4 * time.Second)
				logger.V(1).Info("waiting for replicas to finish syncing", "pending", res.Pending)
			}
			if res.Source == opreplication.EnsureSourceForced {
				needSentinelReset = true
			}
			if deps.MasterCache != nil {
				cacheKey := opreplication.CacheKeyForCluster(cr)
				if replicationErr != nil || len(res.Drift) > 0 {
					deps.MasterCache.Invalidate(cacheKey)
				} else if master != "" {
					deps.MasterCache.Remember(cacheKey, master, res.Reason)
				}
			}
		}
	}

	if roles == nil {
		roles = make(map[string]keyvalv1alpha1.PodRole, len(pods))
		infos = make(map[string]clients.ReplicationInfo, len(pods))
		if len(pods) > 0 {
			master = pods[0].Name
		}
		for i := range pods {
			pod := pods[i]
			role := keyvalv1alpha1.PodRoleReplica
			if pod.Name == master {
				role = keyvalv1alpha1.PodRoleMaster
			}
			roles[pod.Name] = role
		}
	}

	runtime.Master = master
	runtime.Roles = roles
	runtime.Infos = infos
	runtime.Topology = topology
	runtime.BootstrapCandidate = bootstrapCandidate
	runtime.ReplicationError = replicationErr
	runtime.NeedSentinelReset = needSentinelReset

	threshold := ophealthy.LagThresholdSeconds(cr)
	prevHealth := make(map[string]keyvalv1alpha1.PodHealth, len(cr.Status.Roles))
	for _, role := range cr.Status.Roles {
		prevHealth[role.Name] = role.Health
	}

	healthyReplicaCount := 0
	var (
		lagTracked    bool
		maxLagSeconds int32
	)

	for i := range pods {
		pod := pods[i]
		role := roles[pod.Name]
		if role == "" {
			role = keyvalv1alpha1.PodRoleReplica
			if pod.Name == master {
				role = keyvalv1alpha1.PodRoleMaster
			}
		}
		info := infos[pod.Name]
		readyNow := runtimepkg.IsPodReady(&pod)
		health, lag := ophealthy.EvaluatePodHealth(role, info, readyNow, threshold)
		runtime.Health[pod.Name] = health
		if lag != nil {
			v := *lag
			runtime.LagSeconds[pod.Name] = &v
			lagTracked = true
			if v > maxLagSeconds {
				maxLagSeconds = v
			}
		}
		switch health {
		case keyvalv1alpha1.PodHealthLagging:
			if prevHealth[pod.Name] != keyvalv1alpha1.PodHealthLagging {
				opobs.EventReplicaLagging(deps.Recorder, cr, pod.Name, runtime.LagSeconds[pod.Name])
			}
		case keyvalv1alpha1.PodHealthDesynced:
			if prevHealth[pod.Name] != keyvalv1alpha1.PodHealthDesynced {
				opobs.EventReplicaDesynced(deps.Recorder, cr, pod.Name)
			}
		case keyvalv1alpha1.PodHealthHealthy:
			if prev := prevHealth[pod.Name]; prev == keyvalv1alpha1.PodHealthLagging || prev == keyvalv1alpha1.PodHealthDesynced {
				opobs.EventReplicaRecovered(deps.Recorder, cr, pod.Name)
			}
		}
		if role == keyvalv1alpha1.PodRoleReplica {
			if health != keyvalv1alpha1.PodHealthHealthy {
				runtime.NeedHealthRetry = true
			} else {
				healthyReplicaCount++
			}
		}
	}

	for i := range sentinelPods {
		pod := sentinelPods[i]
		health, lag := ophealthy.EvaluatePodHealth(keyvalv1alpha1.PodRoleSentinel, clients.ReplicationInfo{}, runtimepkg.IsPodReady(&pod), threshold)
		runtime.Health[pod.Name] = health
		if lag != nil {
			v := *lag
			runtime.LagSeconds[pod.Name] = &v
		}
	}

	// Publish replication lag metrics and clear stale entries.
	currentLagPods := make(map[string]struct{}, len(pods)+len(sentinelPods))
	for i := range pods {
		name := pods[i].Name
		currentLagPods[name] = struct{}{}
		opobs.ObserveReplicationLag(cr, name, runtime.LagSeconds[name])
	}
	for i := range sentinelPods {
		name := sentinelPods[i].Name
		currentLagPods[name] = struct{}{}
		opobs.ObserveReplicationLag(cr, name, runtime.LagSeconds[name])
	}
	for _, prev := range cr.Status.Roles {
		if _, ok := currentLagPods[prev.Name]; !ok {
			opobs.ObserveReplicationLag(cr, prev.Name, nil)
		}
	}

	if replicationErr != nil {
		state.RequeueAfter(3 * time.Second)
	}
	if runtime.NeedHealthRetry {
		if state.NextRequeue() == 0 {
			state.RequeueAfter(5 * time.Second)
		}
	}

	if deps.ClientFactory != nil {
		prevHealthyReplicas := 0
		for _, roleStatus := range prevStatus.Roles {
			if roleStatus.Role == keyvalv1alpha1.PodRoleReplica && roleStatus.Health == keyvalv1alpha1.PodHealthHealthy {
				prevHealthyReplicas++
			}
		}
		repLogger := logger.WithValues(
			"component", "replication",
			"rolesSource", topology.Source,
			"master", master,
			"changed", topology.Changed,
			"driftCount", len(topology.Drift),
			"pendingSync", len(topology.Pending),
			"healthyReplicas", healthyReplicaCount,
		)
		if topology.Reason != "" {
			repLogger = repLogger.WithValues("rolesReason", topology.Reason)
		}
		if lagTracked {
			repLogger = repLogger.WithValues("maxReplicaLagSeconds", maxLagSeconds)
		}
		logAtInfo := topology.Changed || len(topology.Drift) > 0 || len(topology.Pending) > 0 || replicationErr != nil
		if master != "" && master != prevStatus.MasterPod {
			logAtInfo = true
		}
		if prevStatus.MasterPod == "" {
			logAtInfo = true
		}
		if int32(healthyReplicaCount) != prevStatus.ReadyReplicas {
			logAtInfo = true
		}
		if reconcile.EnsureSourceToRolesSource(topology.Source) != prevStatus.RolesSource {
			logAtInfo = true
		}
		if healthyReplicaCount != prevHealthyReplicas {
			logAtInfo = true
		}
		if logAtInfo {
			repLogger.Info("replication state evaluated")
		} else {
			repLogger.V(1).Info("replication state evaluated")
		}
	}

	// Runtime configuration (redis + sentinel).
	runtimeResults := make([]opruntimecfg.Result, 0, 2)
	redisRuntime := opruntimecfg.ApplyRedisRuntime(ctx, deps.Client, deps.ClientFactory, cr, pods, state.Config.ConfigHash, logger.Logr(), deps.Recorder, &state.Security.Settings)
	runtimeResults = append(runtimeResults, redisRuntime)
	if redisRuntime.RequeueAfter > 0 {
		state.RequeueAfter(redisRuntime.RequeueAfter)
	}
	switch redisRuntime.Mode {
	case opruntimecfg.ModeApplied:
		logger.Info("redis runtime config applied", "changedKeys", redisRuntime.ChangedKeys)
	case opruntimecfg.ModeNeedsRestart:
		logger.Info("redis runtime config requires restart", "detail", redisRuntime.Message)
	case opruntimecfg.ModeFailed:
		opobs.EventRuntimeConfigFailed(deps.Recorder, cr, redisRuntime.Component, redisRuntime.Err)
		opobs.IncRuntimeConfigFailed(cr, redisRuntime.Component)
		logger.Error(redisRuntime.Err, "redis runtime config apply failed", "detail", redisRuntime.Message)
	}

	if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
		sentinelRuntime := opruntimecfg.ApplySentinelRuntime(ctx, deps.SentinelFactory, cr, sentinelPods, logger.Logr(), deps.Recorder, &state.Security.Settings)
		runtimeResults = append(runtimeResults, sentinelRuntime)
		switch sentinelRuntime.Mode {
		case opruntimecfg.ModeApplied:
			logger.Info("sentinel runtime config applied", "changedKeys", sentinelRuntime.ChangedKeys)
		case opruntimecfg.ModeNeedsRestart:
			logger.Info("sentinel config requires restart", "detail", sentinelRuntime.Message)
		case opruntimecfg.ModeFailed:
			opobs.EventRuntimeConfigFailed(deps.Recorder, cr, sentinelRuntime.Component, sentinelRuntime.Err)
			opobs.IncRuntimeConfigFailed(cr, sentinelRuntime.Component)
			logger.Error(sentinelRuntime.Err, "sentinel runtime config apply failed", "detail", sentinelRuntime.Message)
		}
	}
	runtime.RuntimeResults = runtimeResults
	runtime.HealthyReplicas = healthyReplicaCount

	state.Runtime = runtime
	return nil
}
