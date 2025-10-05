package observability

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"golang.org/x/time/rate"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

const heuristicFallbackInterval = 30 * time.Second

var (
	heuristicLimiterMu sync.Mutex
	heuristicLimiter   = map[string]*rate.Limiter{}
)

func allowHeuristicEvent(key string) bool {
	heuristicLimiterMu.Lock()
	defer heuristicLimiterMu.Unlock()
	limiter, ok := heuristicLimiter[key]
	if !ok {
		limiter = rate.NewLimiter(rate.Every(heuristicFallbackInterval), 1)
		heuristicLimiter[key] = limiter
	}
	return limiter.Allow()
}

func EventRoleCorrected(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, master string) {
	if rec == nil {
		return
	}
	rec.Event(cr, corev1.EventTypeNormal, "RoleCorrected", fmt.Sprintf("master=%s", master))
}

func EventReplicationDrift(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pods []string) {
	if rec == nil || len(pods) == 0 {
		return
	}
	rec.Event(cr, corev1.EventTypeWarning, "ReplicationDrift", fmt.Sprintf("pods=%s", strings.Join(pods, ",")))
}

func EventReplicationAligned(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, master string) {
	if rec == nil {
		return
	}
	rec.Event(cr, corev1.EventTypeNormal, "ReplicationAligned", fmt.Sprintf("master=%s", master))
}

func EventReplicationHeuristicFallback(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, detail string) {
	if rec == nil || cr == nil {
		return
	}
	if !allowHeuristicEvent(cr.Namespace + "/" + cr.Name) {
		return
	}
	if strings.TrimSpace(detail) == "" {
		detail = "falling back to direct pod probes while sentinel metadata unavailable"
	}
	rec.Event(cr, corev1.EventTypeWarning, "ReplicationHeuristicFallback", detail)
}

func EventSentinelQuorumLost(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, detail string) {
	if rec == nil {
		return
	}
	if detail == "" {
		detail = "sentinel quorum check failed"
	}
	detail = fmt.Sprintf("%s; observed=%s", detail, time.Now().UTC().Format(time.RFC3339Nano))
	rec.Event(cr, corev1.EventTypeWarning, "SentinelQuorumLost", detail)
}

func EventSentinelQuorumRestored(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, detail string) {
	if rec == nil {
		return
	}
	if detail == "" {
		detail = "sentinel quorum restored"
	}
	detail = fmt.Sprintf("%s; observed=%s", detail, time.Now().UTC().Format(time.RFC3339Nano))
	rec.Event(cr, corev1.EventTypeNormal, "SentinelQuorumRestored", detail)
}

func EventSentinelResetDone(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pod string) {
	if rec == nil {
		return
	}
	detail := pod
	if detail == "" {
		detail = "sentinel reset"
	}
	rec.Event(cr, corev1.EventTypeNormal, "ResetSentinelDone", detail)
}

func EventStartFailover(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, failoverType FailoverType, detail string) {
	if rec == nil {
		return
	}
	if detail == "" {
		detail = "initiating failover"
	}
	rec.Event(cr, corev1.EventTypeNormal, "StartFailover", fmt.Sprintf("type=%s %s", string(failoverType), detail))
}

func EventFailoverTriggered(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, currentMaster string) {
	if rec == nil {
		return
	}
	if currentMaster == "" {
		currentMaster = "unknown"
	}
	rec.Event(cr, corev1.EventTypeNormal, "FailoverTriggered", fmt.Sprintf("master=%s", currentMaster))
}

func EventFailoverCompleted(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, newMaster string, err error) {
	if rec == nil {
		return
	}
	if err != nil {
		rec.Event(cr, corev1.EventTypeWarning, "FailoverCompleted", fmt.Sprintf("error=%v", err))
		return
	}
	if newMaster == "" {
		newMaster = "unknown"
	}
	rec.Event(cr, corev1.EventTypeNormal, "FailoverCompleted", fmt.Sprintf("master=%s", newMaster))
}

func EventServiceRemoved(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, serviceType, name string) {
	if rec == nil {
		return
	}
	if serviceType == "" {
		serviceType = "unknown"
	}
	if name == "" {
		name = serviceType
	}
	rec.Event(cr, corev1.EventTypeWarning, "ServiceRemoved", fmt.Sprintf("service=%s type=%s action=deleted", name, serviceType))
}

func EventServiceImmutableField(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, serviceType, name, field, detail string) {
	if rec == nil {
		return
	}
	if serviceType == "" {
		serviceType = "unknown"
	}
	if name == "" {
		name = serviceType
	}
	if field == "" {
		field = "unknown"
	}
	if detail == "" {
		detail = "change rejected"
	}
	rec.Event(cr, corev1.EventTypeWarning, "ServiceImmutableField", fmt.Sprintf("service=%s type=%s field=%s detail=%s", name, serviceType, field, detail))
}

func EventRedisPreStopFailed(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pod, detail string) {
	if rec == nil || cr == nil {
		return
	}
	if strings.TrimSpace(detail) == "" {
		detail = "redis-cli shutdown failed"
	}
	if pod == "" {
		pod = "unknown"
	}
	rec.Event(cr, corev1.EventTypeWarning, "RedisPreStopFailed", fmt.Sprintf("pod=%s detail=%s", pod, detail))
}

func EventTLSSecretRotated(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, component, secretName, oldHash, newHash string) {
	if rec == nil {
		return
	}
	if component == "" {
		component = "redis"
	}
	if secretName == "" {
		secretName = "unknown"
	}
	rec.Event(cr, corev1.EventTypeNormal, "TLSSecretRotated", fmt.Sprintf("component=%s secret=%s old=%s new=%s", component, secretName, oldHash, newHash))
}

func EventNewMaster(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, newMaster string, failoverType FailoverType, err error) {
	if rec == nil {
		return
	}
	if err != nil {
		rec.Event(cr, corev1.EventTypeWarning, "NewMaster", fmt.Sprintf("type=%s error=%v", string(failoverType), err))
		return
	}
	if newMaster == "" {
		newMaster = "unknown"
	}
	rec.Event(cr, corev1.EventTypeNormal, "NewMaster", fmt.Sprintf("type=%s master=%s", string(failoverType), newMaster))
}

func EventNoGoodSlave(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, detail string) {
	if rec == nil {
		return
	}
	if detail == "" {
		detail = "no replica with master_link_status=up"
	}
	rec.Event(cr, corev1.EventTypeWarning, "NoGoodSlave", detail)
}

func EventReplicaLagging(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pod string, lag *int32) {
	if rec == nil {
		return
	}
	detail := pod
	if lag != nil {
		detail = detail + " lagSeconds=" + strconv.Itoa(int(*lag))
	}
	rec.Event(cr, corev1.EventTypeWarning, "ReplicaLagging", detail)
}

func EventReplicaDesynced(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pod string) {
	if rec == nil {
		return
	}
	rec.Event(cr, corev1.EventTypeWarning, "ReplicaDesynced", pod)
}

func EventReplicaRecovered(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pod string) {
	if rec == nil {
		return
	}
	rec.Event(cr, corev1.EventTypeNormal, "ReplicaRecovered", pod)
}

func EventRuntimeConfigApplied(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, component string, keys []string) {
	if rec == nil {
		return
	}
	detail := component
	if len(keys) > 0 {
		detail = fmt.Sprintf("%s keys=%s", component, strings.Join(keys, ","))
	}
	rec.Event(cr, corev1.EventTypeNormal, "RuntimeConfigApplied", detail)
}

func EventRuntimeConfigFailed(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, component string, err error) {
	if rec == nil {
		return
	}
	msg := component
	if err != nil {
		msg = fmt.Sprintf("%s error=%v", component, err)
	}
	rec.Event(cr, corev1.EventTypeWarning, "RuntimeConfigFailed", msg)
}

func EventSentinelApplyImmutableField(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, field, detail string) {
	if rec == nil {
		return
	}
	msg := field
	if msg == "" {
		msg = "sentinel statefulset immutable field"
	}
	if detail != "" {
		msg = fmt.Sprintf("%s: %s", msg, detail)
	}
	rec.Event(cr, corev1.EventTypeWarning, "SentinelApplyImmutableField", msg)
}

func EventSentinelApplyFailed(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, err error) {
	if rec == nil {
		return
	}
	msg := "sentinel statefulset apply failed"
	if err != nil && err.Error() != "" {
		msg = err.Error()
	}
	rec.Event(cr, corev1.EventTypeWarning, "SentinelApplyFailed", msg)
}

func EventSentinelApplyConflict(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, manager, field, detail string) {
	eventStatefulSetApplyConflict(rec, cr, "SentinelApplyConflict", manager, field, detail)
}

func EventRedisApplyFailed(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, err error) {
	if rec == nil {
		return
	}
	msg := "redis statefulset apply failed"
	if err != nil && err.Error() != "" {
		msg = err.Error()
	}
	rec.Event(cr, corev1.EventTypeWarning, "RedisApplyFailed", msg)
}

func EventRedisApplyConflict(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, manager, field, detail string) {
	eventStatefulSetApplyConflict(rec, cr, "RedisApplyConflict", manager, field, detail)
}

func eventStatefulSetApplyConflict(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, reason, manager, field, detail string) {
	if rec == nil {
		return
	}
	parts := make([]string, 0, 3)
	if manager != "" {
		parts = append(parts, fmt.Sprintf("manager=%s", manager))
	}
	if field != "" {
		parts = append(parts, fmt.Sprintf("field=%s", field))
	}
	if detail != "" {
		parts = append(parts, detail)
	}
	msg := "statefulset apply conflict"
	if len(parts) > 0 {
		msg = strings.Join(parts, " ")
	}
	rec.Event(cr, corev1.EventTypeWarning, reason, msg)
}

func EventBootstrapStart(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, detail string) {
	if rec == nil {
		return
	}
	rec.Event(cr, corev1.EventTypeNormal, "BootstrapStart", detail)
}

func EventBootstrapFinish(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, detail string) {
	if rec == nil {
		return
	}
	rec.Event(cr, corev1.EventTypeNormal, "BootstrapFinish", detail)
}

func EventRollingStepBlocked(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, reason, detail string) {
	if rec == nil {
		return
	}
	if detail == "" {
		detail = reason
	} else {
		detail = fmt.Sprintf("%s: %s", reason, detail)
	}
	rec.Event(cr, corev1.EventTypeWarning, "RollingStepBlocked", detail)
}

func EventRollingStepResumed(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, reason string) {
	if rec == nil {
		return
	}
	if reason == "" {
		reason = "resumed"
	}
	rec.Event(cr, corev1.EventTypeNormal, "RollingStepResumed", reason)
}

func EventScaleStarted(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, direction string, current, target int32) {
	if rec == nil {
		return
	}
	if direction == "" {
		direction = "unknown"
	}
	msg := fmt.Sprintf("direction=%s current=%d target=%d", direction, current, target)
	rec.Event(cr, corev1.EventTypeNormal, "ScaleOperationStarted", msg)
}

func EventScaleCompleted(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, direction string, target, ready int32) {
	if rec == nil {
		return
	}
	if direction == "" {
		direction = "unknown"
	}
	msg := fmt.Sprintf("direction=%s target=%d ready=%d", direction, target, ready)
	rec.Event(cr, corev1.EventTypeNormal, "ScaleOperationCompleted", msg)
}

func EventScaleBlocked(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, direction, reason, detail string) {
	if rec == nil {
		return
	}
	if direction == "" {
		direction = "unknown"
	}
	if reason == "" {
		reason = "unknown"
	}
	msg := fmt.Sprintf("direction=%s reason=%s", direction, reason)
	if detail != "" {
		msg = fmt.Sprintf("%s detail=%s", msg, detail)
	}
	rec.Event(cr, corev1.EventTypeWarning, "ScaleOperationBlocked", msg)
}

func EventPodEvicted(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pod string, reasons []string) {
	if rec == nil {
		return
	}
	detail := pod
	if len(reasons) > 0 {
		detail = fmt.Sprintf("pod=%s reasons=%s", pod, strings.Join(reasons, ","))
	}
	rec.Event(cr, corev1.EventTypeNormal, "PodEvicted", detail)
}

func EventPDBModeSwitched(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, component, mode string, minAvailable int32) {
	if rec == nil {
		return
	}
	if component == "" {
		component = "unknown"
	}
	if mode == "" {
		mode = "unknown"
	}
	detail := fmt.Sprintf("component=%s mode=%s minAvailable=%d", component, mode, minAvailable)
	rec.Event(cr, corev1.EventTypeNormal, "PDBModeSwitched", detail)
}

func EventStorageResizeRequested(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pvc, from, to string) {
	if rec == nil {
		return
	}
	if pvc == "" {
		pvc = "unknown"
	}
	if from == "" {
		from = "current"
	}
	if to == "" {
		to = "target"
	}
	rec.Event(cr, corev1.EventTypeNormal, "StorageResizeRequested", fmt.Sprintf("pvc=%s from=%s to=%s", pvc, from, to))
}

func EventStorageResizeCompleted(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pvc, size string) {
	if rec == nil {
		return
	}
	if pvc == "" {
		pvc = "unknown"
	}
	if size == "" {
		size = "unknown"
	}
	rec.Event(cr, corev1.EventTypeNormal, "StorageResizeCompleted", fmt.Sprintf("pvc=%s size=%s", pvc, size))
}

func EventStorageResizeIgnored(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pvc, from, to string) {
	if rec == nil {
		return
	}
	if pvc == "" {
		pvc = "unknown"
	}
	if from == "" {
		from = "current"
	}
	if to == "" {
		to = "target"
	}
	rec.Event(cr, corev1.EventTypeWarning, "StorageResizeIgnored", fmt.Sprintf("pvc=%s from=%s to=%s", pvc, from, to))
}

func EventStorageResizeRestartQueued(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pvc, pod string, age time.Duration) {
	if rec == nil {
		return
	}
	if pvc == "" {
		pvc = "unknown"
	}
	if pod == "" {
		pod = "unknown"
	}
	rounded := age.Round(time.Second)
	if rounded < 0 {
		rounded = 0
	}
	rec.Event(cr, corev1.EventTypeWarning, "StorageResizeRestartQueued", fmt.Sprintf("pvc=%s pod=%s age=%s", pvc, pod, rounded))
}

func EventStorageResizeBackoff(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pvc string, backoff time.Duration) {
	if rec == nil {
		return
	}
	if pvc == "" {
		pvc = "unknown"
	}
	if backoff < 0 {
		backoff = 0
	}
	rec.Event(cr, corev1.EventTypeWarning, "StorageResizeBackoff", fmt.Sprintf("pvc=%s backoff=%s", pvc, backoff.Round(time.Second)))
}

func EventStoragePVCDeletionRequested(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, pvc string) {
	if rec == nil {
		return
	}
	if pvc == "" {
		pvc = "unknown"
	}
	rec.Event(cr, corev1.EventTypeNormal, "StoragePVCDeletionRequested", fmt.Sprintf("pvc=%s", pvc))
}

func EventStorageCleanupStarted(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, policy string) {
	if rec == nil {
		return
	}
	rec.Event(cr, corev1.EventTypeNormal, "StorageCleanupStarted", fmt.Sprintf("policy=%s", policy))
}

func EventStorageCleanupFinished(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster) {
	if rec == nil {
		return
	}
	rec.Event(cr, corev1.EventTypeNormal, "StorageCleanupFinished", "PVC cleanup complete")
}

func EventStorageCleanupTimedOut(rec record.EventRecorder, cr *keyvalv1alpha1.KeyValCluster, remaining []string) {
	if rec == nil {
		return
	}
	detail := "PVC cleanup timed out"
	if len(remaining) > 0 {
		detail = fmt.Sprintf("PVC cleanup timed out, remaining=%s", strings.Join(remaining, ","))
	}
	rec.Event(cr, corev1.EventTypeWarning, "StorageCleanupTimedOut", detail)
}
