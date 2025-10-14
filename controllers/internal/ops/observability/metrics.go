package observability

import (
	"strings"
	"time"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

const (
	StatefulSetApplyFailureReasonImmutable = "immutable_field"
	StatefulSetApplyFailureReasonAPIError  = "api_error"
	StatefulSetApplyFailureReasonConflict  = "conflict"

	SentinelApplyFailureReasonImmutable = StatefulSetApplyFailureReasonImmutable
	SentinelApplyFailureReasonError     = StatefulSetApplyFailureReasonAPIError
	SentinelApplyFailureReasonConflict  = StatefulSetApplyFailureReasonConflict

	RedisApplyFailureReasonImmutable = StatefulSetApplyFailureReasonImmutable
	RedisApplyFailureReasonError     = StatefulSetApplyFailureReasonAPIError
	RedisApplyFailureReasonConflict  = StatefulSetApplyFailureReasonConflict
)

var (
	replicationChanges = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_replication_changes_total",
			Help: "Number of times replication topology was changed",
		},
		[]string{"namespace", "cluster"},
	)
	controllerQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_controller_reconcile_queue_depth",
			Help: "Current depth of the reconcile workqueue per controller",
		},
		[]string{"controller"},
	)
	masterCacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_redis_master_cache_hits_total",
			Help: "Number of master-address cache hits",
		},
		[]string{"namespace", "cluster"},
	)
	masterCacheMisses = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_redis_master_cache_misses_total",
			Help: "Number of master-address cache lookups that missed or expired",
		},
		[]string{"namespace", "cluster"},
	)
	externalImportAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_external_import_attempt_total",
			Help: "Number of external import attempts started",
		},
		[]string{"namespace", "cluster", "mode"},
	)
	externalImportFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_external_import_failure_total",
			Help: "Number of external import attempts that failed",
		},
		[]string{"namespace", "cluster", "reason"},
	)
	externalImportSuccesses = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_external_import_success_total",
			Help: "Number of external import attempts that completed successfully",
		},
		[]string{"namespace", "cluster", "mode"},
	)
	externalImportDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "keyval_external_import_duration_seconds",
			Help:    "Duration of external import operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"namespace", "cluster", "mode"},
	)
	replicationLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_replication_lag_seconds",
			Help: "Per-pod replication lag as observed by the operator",
		},
		[]string{"namespace", "cluster", "pod"},
	)
	sentinelQuorumHealthy = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_sentinel_quorum_healthy",
			Help: "Whether sentinel quorum is satisfied (1) or not (0)",
		},
		[]string{"namespace", "cluster"},
	)
	sentinelQuorumLossSince = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_sentinel_quorum_loss_since",
			Help: "UNIX timestamp (seconds) when sentinel quorum loss was first observed; 0 when healthy",
		},
		[]string{"namespace", "cluster"},
	)
	sentinelNoQuorumReports = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_sentinel_noquorum_reports",
			Help: "Number of sentinel instances reporting NOQUORUM during the last check",
		},
		[]string{"namespace", "cluster"},
	)
	sentinelReadyMembers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_sentinel_ready_members",
			Help: "Number of ready sentinel pods observed during the last quorum evaluation",
		},
		[]string{"namespace", "cluster"},
	)
	sentinelQuorumTargetGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_sentinel_quorum_target",
			Help: "Required sentinel quorum target (majority) computed for the cluster",
		},
		[]string{"namespace", "cluster"},
	)
	labelCorrections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_label_corrections_total",
			Help: "Number of pod role label corrections applied",
		},
		[]string{"namespace", "cluster"},
	)
	podLabelPatchConflicts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_pod_label_patch_conflicts_total",
			Help: "Number of pod role label patch operations that hit a resourceVersion conflict",
		},
		[]string{"namespace", "cluster", "pod"},
	)
	podLabelPatchRetries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_pod_label_patch_retries_total",
			Help: "Number of additional attempts required to successfully patch pod role labels",
		},
		[]string{"namespace", "cluster", "pod"},
	)
	rollingDeletions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_rolling_deletions_total",
			Help: "Number of pods deleted during rolling updates",
		},
		[]string{"namespace", "cluster"},
	)
	evictionAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_eviction_attempt_total",
			Help: "Number of eviction attempts initiated by the operator",
		},
		[]string{"namespace", "cluster", "component"},
	)
	evictionResults = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_eviction_result_total",
			Help: "Number of eviction outcomes recorded by result",
		},
		[]string{"namespace", "cluster", "component", "result"},
	)
	evictionFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_pod_eviction_failures_total",
			Help: "Number of pod evictions that failed or were rejected, classified by reason",
		},
		[]string{"namespace", "cluster", "component", "reason"},
	)
	serviceRemoved = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_service_removed_total",
			Help: "Number of optional services deleted after being disabled",
		},
		[]string{"namespace", "cluster", "service"},
	)
	serviceImmutableChanges = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_service_immutable_change_total",
			Help: "Number of rejected service updates due to immutable field changes",
		},
		[]string{"namespace", "cluster", "service", "field"},
	)
	tlsSecretRotations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_tls_secret_rotations_total",
			Help: "Number of TLS secret rotations detected by the operator",
		},
		[]string{"namespace", "cluster", "component"},
	)
	scaleOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_scale_operations_total",
			Help: "Number of completed scale operations by direction",
		},
		[]string{"namespace", "cluster", "direction"},
	)
	scaleInProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_scale_in_progress",
			Help: "Whether the operator is actively executing a scale operation (1=in progress)",
		},
		[]string{"namespace", "cluster"},
	)
	failoverTriggered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_failover_triggered_total",
			Help: "Number of manually triggered failovers",
		},
		[]string{"namespace", "cluster", "type"},
	)
	failoverCompleted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_failover_completed_total",
			Help: "Number of successfully completed failovers",
		},
		[]string{"namespace", "cluster"},
	)
	failoverDecisions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_failover_decisions_total",
			Help: "Number of replication failover decisions recorded by source",
		},
		[]string{"namespace", "cluster", "source"},
	)
	reconcileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "keyval_reconcile_duration_seconds",
			Help:    "Reconcile duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"namespace", "cluster"},
	)
	bootstrapAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_bootstrap_attempt_total",
			Help: "Number of bootstrap attempts by mode",
		},
		[]string{"namespace", "cluster", "mode"},
	)
	bootstrapFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_bootstrap_failure_total",
			Help: "Number of bootstrap failures",
		},
		[]string{"namespace", "cluster"},
	)
	runtimeConfigApplied = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_runtime_config_applied_total",
			Help: "Number of runtime configuration synchronizations applied",
		},
		[]string{"namespace", "cluster", "component"},
	)
	runtimeConfigFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_runtime_config_failed_total",
			Help: "Number of runtime configuration attempts that failed",
		},
		[]string{"namespace", "cluster", "component"},
	)
	reconcileResults = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_reconcile_result_total",
			Help: "Number of reconciles by result",
		},
		[]string{"namespace", "cluster", "result"},
	)
	disruptionsBlocked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_disruptions_blocked_total",
			Help: "Number of times rolling updates were blocked by guard conditions",
		},
		[]string{"namespace", "cluster", "reason"},
	)
	updateInProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_update_in_progress",
			Help: "Whether the operator is actively executing a rolling update (1=in progress)",
		},
		[]string{"namespace", "cluster"},
	)
	finalizerDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "keyval_operator_finalizer_duration_seconds",
			Help:    "Duration of storage cleanup finalizer execution",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"namespace", "cluster", "policy", "result"},
	)
	storageCleanupFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_storage_cleanup_failures_total",
			Help: "Number of storage cleanup attempts that failed",
		},
		[]string{"namespace", "cluster", "policy", "reason"},
	)
	sentinelResetsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_sentinel_resets_total",
			Help: "Number of sentinel reset commands issued by the operator",
		},
		[]string{"namespace", "cluster"},
	)
	sentinelApplyFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_sentinel_apply_failures_total",
			Help: "Number of sentinel StatefulSet apply failures by reason",
		},
		[]string{"namespace", "cluster", "reason"},
	)
	redisApplyFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_redis_apply_failures_total",
			Help: "Number of redis StatefulSet apply failures by reason",
		},
		[]string{"namespace", "cluster", "reason"},
	)
	externalCallTimeouts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_external_call_timeouts_total",
			Help: "Number of Redis/Sentinel calls that timed out",
		},
		[]string{"namespace", "cluster", "command", "endpoint"},
	)
	redisClientRetries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_operator_redis_client_retries_total",
			Help: "Number of Redis/Sentinel client retries attempted",
		},
		[]string{"namespace", "cluster", "command", "endpoint"},
	)
	requeueBackoffSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "keyval_operator_requeue_backoff_seconds",
			Help:    "Observed requeue backoff delays in seconds",
			Buckets: []float64{3, 6, 12, 24, 48, 60},
		},
		[]string{"namespace", "cluster"},
	)
	statusUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "keyval_status_update_total",
			Help: "Number of status reconciliation attempts classified by whether a patch was applied",
		},
		[]string{"namespace", "cluster", "mutated"},
	)
	gracefulShutdownDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "keyval_operator_graceful_shutdown_duration_seconds",
			Help:    "Observed duration between pod deletion and container termination",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		},
		[]string{"namespace", "cluster", "component", "pod"},
	)
	bootstrapFreshnessGap = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_bootstrap_freshness_gap_bytes",
			Help: "Difference in replication offset (bytes) between the selected DR master and the next candidate",
		},
		[]string{"namespace", "cluster"},
	)
	bootstrapFreshnessSource = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "keyval_operator_bootstrap_freshness_source",
			Help: "Source of DR bootstrap decision (0=unknown,1=force-master,2=freshness,3=seed)",
		},
		[]string{"namespace", "cluster", "source"},
	)
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		replicationChanges,
		controllerQueueDepth,
		masterCacheHits,
		masterCacheMisses,
		externalImportAttempts,
		externalImportFailures,
		externalImportSuccesses,
		externalImportDuration,
		replicationLag,
		sentinelQuorumHealthy,
		sentinelQuorumLossSince,
		sentinelNoQuorumReports,
		sentinelReadyMembers,
		sentinelQuorumTargetGauge,
		labelCorrections,
		podLabelPatchConflicts,
		podLabelPatchRetries,
		rollingDeletions,
		evictionAttempts,
		evictionResults,
		evictionFailures,
		serviceRemoved,
		serviceImmutableChanges,
		tlsSecretRotations,
		failoverTriggered,
		failoverCompleted,
		failoverDecisions,
		reconcileDuration,
		bootstrapAttempts,
		bootstrapFailures,
		runtimeConfigApplied,
		runtimeConfigFailed,
		disruptionsBlocked,
		updateInProgress,
		reconcileResults,
		finalizerDuration,
		storageCleanupFailures,
		sentinelResetsTotal,
		scaleOperations,
		scaleInProgress,
		sentinelApplyFailures,
		redisApplyFailures,
		externalCallTimeouts,
		redisClientRetries,
		statusUpdates,
		requeueBackoffSeconds,
		gracefulShutdownDuration,
		bootstrapFreshnessGap,
		bootstrapFreshnessSource,
	)
}

func labelsForCR(cr *keyvalv1alpha1.KeyValCluster) prometheus.Labels {
	return prometheus.Labels{"namespace": cr.Namespace, "cluster": cr.Name}
}

func labelsForEviction(cr *keyvalv1alpha1.KeyValCluster, component string) prometheus.Labels {
	if cr == nil {
		return prometheus.Labels{"namespace": "", "cluster": "", "component": component}
	}
	if component == "" {
		component = "redis"
	}
	return prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"component": component,
	}
}

func ObserveGracefulShutdown(cr *keyvalv1alpha1.KeyValCluster, component, pod string, duration time.Duration) {
	if cr == nil {
		return
	}
	component = normalizeLabelValue(component, "redis")
	pod = normalizeLabelValue(pod, "unknown")
	if duration < 0 {
		duration = 0
	}
	gracefulShutdownDuration.With(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"component": component,
		"pod":       pod,
	}).Observe(duration.Seconds())
}

func SetBootstrapFreshnessGap(cr *keyvalv1alpha1.KeyValCluster, gapBytes int64) {
	if cr == nil {
		return
	}
	if gapBytes < 0 {
		gapBytes = 0
	}
	bootstrapFreshnessGap.With(labelsForCR(cr)).Set(float64(gapBytes))
}

func SetBootstrapFreshnessSource(cr *keyvalv1alpha1.KeyValCluster, source string) {
	if cr == nil {
		return
	}
	source = normalizeLabelValue(source, "unknown")
	value := 0.0
	switch source {
	case "force-master":
		value = 1
	case "freshness":
		value = 2
	case "seed":
		value = 3
	}
	bootstrapFreshnessSource.With(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"source":    source,
	}).Set(value)
}

func normalizeEvictionReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "unknown"
	}
	var builder strings.Builder
	prevUnderscore := false
	for i, r := range reason {
		switch {
		case r == ' ' || r == '-' || r == '\t':
			if !prevUnderscore {
				builder.WriteByte('_')
			}
			prevUnderscore = true
			continue
		case unicode.IsUpper(r):
			if i > 0 {
				prev := rune(reason[i-1])
				if (unicode.IsLower(prev) || unicode.IsDigit(prev)) && !prevUnderscore {
					builder.WriteByte('_')
				}
			}
			r = unicode.ToLower(r)
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			r = unicode.ToLower(r)
		default:
			continue
		}
		builder.WriteRune(r)
		prevUnderscore = false
	}
	out := strings.Trim(builder.String(), "_")
	if out == "" {
		return "unknown"
	}
	return out
}

func IncEvictionFailure(cr *keyvalv1alpha1.KeyValCluster, component, reason string) {
	if cr == nil {
		return
	}
	if component == "" {
		component = "redis"
	}
	reason = normalizeEvictionReason(reason)
	evictionFailures.With(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"component": component,
		"reason":    reason,
	}).Inc()
}

func EvictionFailuresCounter() *prometheus.CounterVec {
	return evictionFailures
}

func IncReplicationChanges(cr *keyvalv1alpha1.KeyValCluster) {
	replicationChanges.With(labelsForCR(cr)).Inc()
}

func normalizeLabelValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func IncExternalCallTimeout(namespace, cluster, command, endpoint string) {
	namespace = normalizeLabelValue(namespace, "default")
	cluster = normalizeLabelValue(cluster, "unknown")
	command = normalizeLabelValue(command, "unknown")
	endpoint = normalizeLabelValue(endpoint, "unknown")
	externalCallTimeouts.With(prometheus.Labels{
		"namespace": namespace,
		"cluster":   cluster,
		"command":   command,
		"endpoint":  endpoint,
	}).Inc()
}

func AddRedisClientRetries(namespace, cluster, command, endpoint string, retries int) {
	if retries <= 0 {
		return
	}
	namespace = normalizeLabelValue(namespace, "default")
	cluster = normalizeLabelValue(cluster, "unknown")
	command = normalizeLabelValue(command, "unknown")
	endpoint = normalizeLabelValue(endpoint, "unknown")
	redisClientRetries.With(prometheus.Labels{
		"namespace": namespace,
		"cluster":   cluster,
		"command":   command,
		"endpoint":  endpoint,
	}).Add(float64(retries))
}

func ObserveRequeueBackoff(cr *keyvalv1alpha1.KeyValCluster, delay time.Duration) {
	if cr == nil || delay <= 0 {
		return
	}
	requeueBackoffSeconds.With(labelsForCR(cr)).Observe(delay.Seconds())
}

// ObserveStorageFinalizerDuration records how long the storage cleanup finalizer took.
func ObserveStorageFinalizerDuration(cr *keyvalv1alpha1.KeyValCluster, policy, result string, duration time.Duration) {
	if cr == nil {
		return
	}
	policy = normalizeLabelValue(policy, "unknown")
	result = normalizeLabelValue(result, "unknown")
	labels := prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"policy":    policy,
		"result":    result,
	}
	if duration < 0 {
		duration = 0
	}
	finalizerDuration.With(labels).Observe(duration.Seconds())
}

// IncStorageCleanupFailure increments the storage cleanup failure counter.
func IncStorageCleanupFailure(cr *keyvalv1alpha1.KeyValCluster, policy, reason string) {
	if cr == nil {
		return
	}
	policy = normalizeLabelValue(policy, "unknown")
	reason = normalizeLabelValue(reason, "unknown")
	labels := prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"policy":    policy,
		"reason":    reason,
	}
	storageCleanupFailures.With(labels).Inc()
}

// ObserveControllerQueueDepth records the current reconcile queue depth for the controller.
func ObserveControllerQueueDepth(controller string, depth int) {
	if controller == "" {
		controller = "unknown"
	}
	controllerQueueDepth.WithLabelValues(controller).Set(float64(depth))
}

// IncMasterCacheHit increments the master-address cache hit counter for the cluster.
func IncMasterCacheHit(namespace, cluster string) {
	namespace = normalizeLabelValue(namespace, "default")
	cluster = normalizeLabelValue(cluster, "unknown")
	masterCacheHits.With(prometheus.Labels{
		"namespace": namespace,
		"cluster":   cluster,
	}).Inc()
}

// IncMasterCacheMiss increments the master-address cache miss counter for the cluster.
func IncMasterCacheMiss(namespace, cluster string) {
	namespace = normalizeLabelValue(namespace, "default")
	cluster = normalizeLabelValue(cluster, "unknown")
	masterCacheMisses.With(prometheus.Labels{
		"namespace": namespace,
		"cluster":   cluster,
	}).Inc()
}

func ObserveReplicationLag(cr *keyvalv1alpha1.KeyValCluster, pod string, lag *int32) {
	if cr == nil || pod == "" {
		return
	}
	labels := prometheus.Labels{"namespace": cr.Namespace, "cluster": cr.Name, "pod": pod}
	if lag == nil {
		replicationLag.Delete(labels)
		return
	}
	replicationLag.With(labels).Set(float64(*lag))
}

func SetSentinelQuorum(cr *keyvalv1alpha1.KeyValCluster, healthy bool) {
	if cr == nil {
		return
	}
	value := 0.0
	if healthy {
		value = 1.0
	}
	sentinelQuorumHealthy.With(labelsForCR(cr)).Set(value)
}

func ObserveSentinelReady(cr *keyvalv1alpha1.KeyValCluster, ready int) {
	if cr == nil {
		return
	}
	if ready < 0 {
		ready = 0
	}
	sentinelReadyMembers.With(labelsForCR(cr)).Set(float64(ready))
}

func ObserveSentinelQuorumTarget(cr *keyvalv1alpha1.KeyValCluster, target int) {
	if cr == nil {
		return
	}
	if target < 0 {
		target = 0
	}
	sentinelQuorumTargetGauge.With(labelsForCR(cr)).Set(float64(target))
}

func ObserveSentinelQuorumLoss(cr *keyvalv1alpha1.KeyValCluster, lossSince *time.Time) {
	if cr == nil {
		return
	}
	labels := labelsForCR(cr)
	gauge := sentinelQuorumLossSince.With(labels)
	if lossSince == nil || lossSince.IsZero() {
		gauge.Set(0)
		return
	}
	gauge.Set(float64(lossSince.UTC().Unix()))
}

func ObserveSentinelNoQuorum(cr *keyvalv1alpha1.KeyValCluster, count int) {
	if cr == nil {
		return
	}
	value := float64(count)
	sentinelNoQuorumReports.With(labelsForCR(cr)).Set(value)
}

func IncLabelCorrectionsBy(cr *keyvalv1alpha1.KeyValCluster, n int) {
	if n <= 0 {
		return
	}
	labelCorrections.With(labelsForCR(cr)).Add(float64(n))
}

func IncPodLabelPatchConflict(cr *keyvalv1alpha1.KeyValCluster, pod string) {
	if cr == nil || pod == "" {
		return
	}
	podLabelPatchConflicts.With(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"pod":       pod,
	}).Inc()
}

func AddPodLabelPatchRetry(cr *keyvalv1alpha1.KeyValCluster, pod string, retries int) {
	if cr == nil || pod == "" || retries <= 0 {
		return
	}
	podLabelPatchRetries.With(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"pod":       pod,
	}).Add(float64(retries))
}

func IncRollingDeletions(cr *keyvalv1alpha1.KeyValCluster) {
	rollingDeletions.With(labelsForCR(cr)).Inc()
}

func IncEvictionAttempt(cr *keyvalv1alpha1.KeyValCluster, component string) {
	if cr == nil {
		return
	}
	evictionAttempts.With(labelsForEviction(cr, component)).Inc()
}

func IncEvictionResult(cr *keyvalv1alpha1.KeyValCluster, component, result string) {
	if cr == nil {
		return
	}
	labels := labelsForEviction(cr, component)
	if result == "" {
		result = "unknown"
	}
	labels = prometheus.Labels{
		"namespace": labels["namespace"],
		"cluster":   labels["cluster"],
		"component": labels["component"],
		"result":    result,
	}
	evictionResults.With(labels).Inc()
}

func IncFailoverTriggered(cr *keyvalv1alpha1.KeyValCluster) {
	IncFailover(cr, FailoverTypeManual)
}

func IncFailoverCompleted(cr *keyvalv1alpha1.KeyValCluster) {
	failoverCompleted.With(labelsForCR(cr)).Inc()
}

func IncFailoverDecision(cr *keyvalv1alpha1.KeyValCluster, source keyvalv1alpha1.RolesSource) {
	if cr == nil {
		return
	}
	if source == "" {
		return
	}
	failoverDecisions.With(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"source":    string(source),
	}).Inc()
}

func ObserveReconcile(cr *keyvalv1alpha1.KeyValCluster, d time.Duration) {
	reconcileDuration.With(labelsForCR(cr)).Observe(d.Seconds())
}

func IncRuntimeConfigApplied(cr *keyvalv1alpha1.KeyValCluster, component string) {
	if cr == nil {
		return
	}
	if component == "" {
		component = "redis"
	}
	runtimeConfigApplied.With(prometheus.Labels{"namespace": cr.Namespace, "cluster": cr.Name, "component": component}).Inc()
}

func IncRuntimeConfigFailed(cr *keyvalv1alpha1.KeyValCluster, component string) {
	if cr == nil {
		return
	}
	if component == "" {
		component = "redis"
	}
	runtimeConfigFailed.With(prometheus.Labels{"namespace": cr.Namespace, "cluster": cr.Name, "component": component}).Inc()
}

func IncSentinelApplyFailure(cr *keyvalv1alpha1.KeyValCluster, reason string) {
	incStatefulSetApplyFailure(sentinelApplyFailures, cr, reason)
}

// SentinelApplyFailureCounter exposes the sentinel-specific counter for tests and diagnostics.
func SentinelApplyFailureCounter() *prometheus.CounterVec {
	return sentinelApplyFailures
}

func IncRedisApplyFailure(cr *keyvalv1alpha1.KeyValCluster, reason string) {
	incStatefulSetApplyFailure(redisApplyFailures, cr, reason)
}

// RedisApplyFailureCounter exposes the redis-specific counter for tests and diagnostics.
func RedisApplyFailureCounter() *prometheus.CounterVec {
	return redisApplyFailures
}

func incStatefulSetApplyFailure(counter *prometheus.CounterVec, cr *keyvalv1alpha1.KeyValCluster, reason string) {
	if cr == nil || counter == nil {
		return
	}
	if reason == "" {
		reason = StatefulSetApplyFailureReasonAPIError
	}
	switch reason {
	case StatefulSetApplyFailureReasonImmutable, StatefulSetApplyFailureReasonAPIError, StatefulSetApplyFailureReasonConflict:
		// accepted as-is
	default:
		reason = StatefulSetApplyFailureReasonAPIError
	}
	labels := labelsForCR(cr)
	labels["reason"] = reason
	counter.With(labels).Inc()
}

func PodLabelPatchConflictCounter() *prometheus.CounterVec {
	return podLabelPatchConflicts
}

func PodLabelPatchRetryCounter() *prometheus.CounterVec {
	return podLabelPatchRetries
}

func IncServiceRemoved(cr *keyvalv1alpha1.KeyValCluster, serviceType string) {
	if cr == nil {
		return
	}
	if serviceType == "" {
		serviceType = "unknown"
	}
	labels := labelsForCR(cr)
	labels["service"] = serviceType
	serviceRemoved.With(labels).Inc()
}

func ServiceRemovedCounter() *prometheus.CounterVec {
	return serviceRemoved
}

func IncServiceImmutableChange(cr *keyvalv1alpha1.KeyValCluster, serviceType, field string) {
	if cr == nil {
		return
	}
	if serviceType == "" {
		serviceType = "unknown"
	}
	if field == "" {
		field = "unknown"
	}
	labels := labelsForCR(cr)
	labels["service"] = serviceType
	labels["field"] = field
	serviceImmutableChanges.With(labels).Inc()
}

func ServiceImmutableChangeCounter() *prometheus.CounterVec {
	return serviceImmutableChanges
}

func IncTLSSecretRotation(cr *keyvalv1alpha1.KeyValCluster, component string) {
	if cr == nil {
		return
	}
	if component == "" {
		component = "redis"
	}
	labels := labelsForCR(cr)
	labels["component"] = component
	tlsSecretRotations.With(labels).Inc()
}

func IncDisruptionsBlocked(cr *keyvalv1alpha1.KeyValCluster, reason string) {
	if cr == nil {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	disruptionsBlocked.With(prometheus.Labels{"namespace": cr.Namespace, "cluster": cr.Name, "reason": reason}).Inc()
}

func SetUpdateInProgress(cr *keyvalv1alpha1.KeyValCluster, inProgress bool) {
	if cr == nil {
		return
	}
	value := 0.0
	if inProgress {
		value = 1.0
	}
	updateInProgress.With(labelsForCR(cr)).Set(value)
}

func SetScaleInProgress(cr *keyvalv1alpha1.KeyValCluster, inProgress bool) {
	if cr == nil {
		return
	}
	value := 0.0
	if inProgress {
		value = 1.0
	}
	scaleInProgress.With(labelsForCR(cr)).Set(value)
}

func IncScaleOperation(cr *keyvalv1alpha1.KeyValCluster, direction string) {
	if cr == nil {
		return
	}
	if direction == "" {
		direction = "unknown"
	}
	scaleOperations.With(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"direction": direction,
	}).Inc()
}

func IncExternalImportAttempt(cr *keyvalv1alpha1.KeyValCluster, mode string) {
	if cr == nil {
		return
	}
	if mode == "" {
		mode = string(keyvalv1alpha1.ExternalSourceSyncModeSnapshot)
	}
	labels := labelsForCR(cr)
	labels["mode"] = strings.ToLower(mode)
	externalImportAttempts.With(labels).Inc()
}

func IncExternalImportFailure(cr *keyvalv1alpha1.KeyValCluster, reason string) {
	if cr == nil {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	labels := labelsForCR(cr)
	labels["reason"] = strings.ToLower(reason)
	externalImportFailures.With(labels).Inc()
}

func IncExternalImportSuccess(cr *keyvalv1alpha1.KeyValCluster, mode string) {
	if cr == nil {
		return
	}
	if mode == "" {
		mode = string(keyvalv1alpha1.ExternalSourceSyncModeSnapshot)
	}
	labels := labelsForCR(cr)
	labels["mode"] = strings.ToLower(mode)
	externalImportSuccesses.With(labels).Inc()
}

func ObserveExternalImportDuration(cr *keyvalv1alpha1.KeyValCluster, mode string, duration time.Duration) {
	if cr == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	if mode == "" {
		mode = string(keyvalv1alpha1.ExternalSourceSyncModeSnapshot)
	}
	labels := labelsForCR(cr)
	labels["mode"] = strings.ToLower(mode)
	externalImportDuration.With(labels).Observe(duration.Seconds())
}

func IncSentinelReset(cr *keyvalv1alpha1.KeyValCluster) {
	if cr == nil {
		return
	}
	sentinelResetsTotal.With(labelsForCR(cr)).Inc()
}

type FailoverType string

const (
	FailoverTypeAutomatic FailoverType = "automatic"
	FailoverTypeManual    FailoverType = "manual"
	FailoverTypeForced    FailoverType = "forced"
)

func IncFailover(cr *keyvalv1alpha1.KeyValCluster, typ FailoverType) {
	if cr == nil {
		return
	}
	if typ == "" {
		typ = FailoverTypeManual
	}
	failoverTriggered.With(prometheus.Labels{"namespace": cr.Namespace, "cluster": cr.Name, "type": string(typ)}).Inc()
}

type BootstrapMode string

const (
	BootstrapModeStandalone BootstrapMode = "standalone"
	BootstrapModeSentinel   BootstrapMode = "sentinel"
)

func IncBootstrapAttempt(cr *keyvalv1alpha1.KeyValCluster, mode BootstrapMode) {
	if cr == nil {
		return
	}
	if mode == "" {
		mode = BootstrapModeStandalone
	}
	bootstrapAttempts.With(prometheus.Labels{"namespace": cr.Namespace, "cluster": cr.Name, "mode": string(mode)}).Inc()
}

func IncBootstrapFailure(cr *keyvalv1alpha1.KeyValCluster) {
	if cr == nil {
		return
	}
	bootstrapFailures.With(labelsForCR(cr)).Inc()
}

type ReconcileResult string

const (
	ReconcileResultSuccess ReconcileResult = "success"
	ReconcileResultError   ReconcileResult = "error"
	ReconcileResultStalled ReconcileResult = "stalled"
)

func IncReconcileResult(cr *keyvalv1alpha1.KeyValCluster, result ReconcileResult) {
	if cr == nil {
		return
	}
	if result == "" {
		result = ReconcileResultSuccess
	}
	reconcileResults.With(prometheus.Labels{"namespace": cr.Namespace, "cluster": cr.Name, "result": string(result)}).Inc()
}

func IncStatusUpdate(cr *keyvalv1alpha1.KeyValCluster, mutated bool) {
	if cr == nil {
		return
	}
	label := "false"
	if mutated {
		label = "true"
	}
	statusUpdates.With(prometheus.Labels{
		"namespace": cr.Namespace,
		"cluster":   cr.Name,
		"mutated":   label,
	}).Inc()
}
