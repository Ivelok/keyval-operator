package status

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	ophealthy "github.com/ivelok/keyval-operator/controllers/internal/ops/health"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/internal/runtime"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

// ConditionState expresses a condition's desired state prior to persistence.
type ConditionState struct {
	Status  metav1.ConditionStatus
	Reason  string
	Message string
}

type ClusterState struct {
	Master         string
	Pods           []corev1.Pod
	Roles          map[string]keyvalv1alpha1.PodRole
	SentinelPods   []corev1.Pod
	Health         map[string]keyvalv1alpha1.PodHealth
	LagSeconds     map[string]*int32
	RolesSource    keyvalv1alpha1.RolesSource
	RolesReason    string
	RuntimeConfig  *RuntimeCondition
	Conditions     map[keyvalv1alpha1.ConditionType]*ConditionState
	ExternalImport *keyvalv1alpha1.ExternalImportStatus
}

// RuntimeCondition captures the reconcile outcome for runtime configuration synchronization.
type RuntimeCondition struct {
	Status  metav1.ConditionStatus
	Reason  string
	Message string
}

func ComputeStatus(cr *keyvalv1alpha1.KeyValCluster, st ClusterState) keyvalv1alpha1.KeyValClusterStatus {
	var s keyvalv1alpha1.KeyValClusterStatus
	s.MasterPod = st.Master
	s.Replicas = cr.Spec.RedisReplicas
	s.RolesSource = st.RolesSource

	now := metav1.NewTime(time.Now())
	healthByName := st.Health
	if healthByName == nil {
		healthByName = map[string]keyvalv1alpha1.PodHealth{}
	}
	lagByName := st.LagSeconds
	if lagByName == nil {
		lagByName = map[string]*int32{}
	}

	prevByName := map[string]keyvalv1alpha1.PodRoleStatus{}
	for _, pr := range cr.Status.Roles {
		prevByName[pr.Name] = pr
	}

	roles := make([]keyvalv1alpha1.PodRoleStatus, 0, len(st.Pods)+len(st.SentinelPods))
	readyCount := int32(0)
	masters := 0
	masterReady := false
	masterHealth := keyvalv1alpha1.PodHealthHealthy

	for _, p := range st.Pods {
		r := st.Roles[p.Name]
		if r == "" {
			if st.Master != "" && p.Name == st.Master {
				r = keyvalv1alpha1.PodRoleMaster
			} else {
				r = keyvalv1alpha1.PodRoleReplica
			}
		}
		readyNow := runtime.IsPodReady(&p)
		health := healthByName[p.Name]
		if health == "" {
			if readyNow {
				health = keyvalv1alpha1.PodHealthHealthy
			} else {
				health = keyvalv1alpha1.PodHealthOffline
			}
		}
		entry := keyvalv1alpha1.PodRoleStatus{
			Name:   p.Name,
			Role:   r,
			Ready:  readyNow,
			Health: health,
		}
		if lag := lagByName[p.Name]; lag != nil {
			v := *lag
			entry.LagSeconds = &v
		}
		if prev, ok := prevByName[p.Name]; ok && prev.Role == r && prev.Ready == readyNow {
			lagEqual := (prev.LagSeconds == nil && entry.LagSeconds == nil) || (prev.LagSeconds != nil && entry.LagSeconds != nil && *prev.LagSeconds == *entry.LagSeconds)
			if prev.Health == entry.Health && lagEqual {
				entry.LastTransitionTime = prev.LastTransitionTime
			}
		} else {
			entry.LastTransitionTime = &now
		}
		roles = append(roles, entry)
		if readyNow {
			readyCount++
		}
		if r == keyvalv1alpha1.PodRoleMaster {
			masters++
			masterReady = readyNow
			masterHealth = health
		}
	}

	for _, sp := range st.SentinelPods {
		readyNow := runtime.IsPodReady(&sp)
		health := healthByName[sp.Name]
		if health == "" {
			if readyNow {
				health = keyvalv1alpha1.PodHealthHealthy
			} else {
				health = keyvalv1alpha1.PodHealthOffline
			}
		}
		entry := keyvalv1alpha1.PodRoleStatus{
			Name:   sp.Name,
			Role:   keyvalv1alpha1.PodRoleSentinel,
			Ready:  readyNow,
			Health: health,
		}
		if lag := lagByName[sp.Name]; lag != nil {
			v := *lag
			entry.LagSeconds = &v
		}
		if prev, ok := prevByName[sp.Name]; ok && prev.Role == keyvalv1alpha1.PodRoleSentinel && prev.Ready == readyNow {
			lagEqual := (prev.LagSeconds == nil && entry.LagSeconds == nil) || (prev.LagSeconds != nil && entry.LagSeconds != nil && *prev.LagSeconds == *entry.LagSeconds)
			if prev.Health == entry.Health && lagEqual {
				entry.LastTransitionTime = prev.LastTransitionTime
			}
		} else {
			entry.LastTransitionTime = &now
		}
		roles = append(roles, entry)
	}

	s.ReadyReplicas = readyCount
	s.Roles = roles

	prevConds := map[string]metav1.Condition{}
	for _, c := range cr.Status.Conditions {
		prevConds[c.Type] = c
	}

	settings := ophealthy.SettingsFor(cr)
	requiredMembers := settings.MinReplicasForSafety
	if cr.Spec.Mode == keyvalv1alpha1.ModeStandalone && requiredMembers > 1 {
		requiredMembers = 1
	}
	if requiredMembers < 1 {
		requiredMembers = 1
	}

	getOverride := func(typ keyvalv1alpha1.ConditionType) *ConditionState {
		if st.Conditions == nil {
			return nil
		}
		if cs, ok := st.Conditions[typ]; ok {
			return cs
		}
		return nil
	}

	conds := make([]metav1.Condition, 0, 8)
	built := map[keyvalv1alpha1.ConditionType]metav1.Condition{}
	build := func(typ keyvalv1alpha1.ConditionType, state ConditionState) {
		cond := metav1.Condition{
			Type:               string(typ),
			Status:             state.Status,
			Reason:             state.Reason,
			Message:            state.Message,
			LastTransitionTime: now,
			ObservedGeneration: cr.Generation,
		}
		if prev, ok := prevConds[cond.Type]; ok {
			if prev.Status == cond.Status && prev.Reason == cond.Reason {
				cond.LastTransitionTime = prev.LastTransitionTime
			}
		}
		conds = append(conds, cond)
		built[typ] = cond
	}

	reconciledState := ConditionState{Status: metav1.ConditionTrue, Reason: "Success", Message: "Reconcile completed"}
	if override := getOverride(keyvalv1alpha1.ConditionReconciled); override != nil {
		reconciledState = *override
	}
	build(keyvalv1alpha1.ConditionReconciled, reconciledState)

	availableState := computeAvailableCondition(cr, st, masters, masterReady, readyCount)
	if override := getOverride(keyvalv1alpha1.ConditionAvailable); override != nil {
		availableState = *override
	}
	build(keyvalv1alpha1.ConditionAvailable, availableState)

	sentinelState := computeSentinelCondition(cr, st)
	if override := getOverride(keyvalv1alpha1.ConditionSentinelQuorum); override != nil {
		sentinelState = *override
	}
	build(keyvalv1alpha1.ConditionSentinelQuorum, sentinelState)

	replicationState := computeReplicationHealthyCondition(cr, st, masterHealth, requiredMembers)
	if override := getOverride(keyvalv1alpha1.ConditionReplicationHealthy); override != nil {
		replicationState = *override
	}
	build(keyvalv1alpha1.ConditionReplicationHealthy, replicationState)

	failoverState := computeFailoverCondition(masters)
	if override := getOverride(keyvalv1alpha1.ConditionFailoverInProgress); override != nil {
		failoverState = *override
	}
	build(keyvalv1alpha1.ConditionFailoverInProgress, failoverState)

	disruptState := ConditionState{Status: metav1.ConditionFalse, Reason: "DisruptionsAllowed", Message: "no disruption pause requested"}
	if override := getOverride(keyvalv1alpha1.ConditionDisruptionsPaused); override != nil {
		disruptState = *override
	}
	build(keyvalv1alpha1.ConditionDisruptionsPaused, disruptState)

	upgradeState := ConditionState{Status: metav1.ConditionFalse, Reason: "Idle", Message: "no upgrade in progress"}
	if override := getOverride(keyvalv1alpha1.ConditionUpgradeInProgress); override != nil {
		upgradeState = *override
	}
	build(keyvalv1alpha1.ConditionUpgradeInProgress, upgradeState)

	bootstrapState := ConditionState{Status: metav1.ConditionFalse, Reason: "Idle", Message: "bootstrap complete"}
	if override := getOverride(keyvalv1alpha1.ConditionBootstrapInProgress); override != nil {
		bootstrapState = *override
	}
	build(keyvalv1alpha1.ConditionBootstrapInProgress, bootstrapState)

	externalState := ConditionState{Status: metav1.ConditionTrue, Reason: "Disabled", Message: "external import not configured"}
	if override := getOverride(keyvalv1alpha1.ConditionExternalImport); override != nil {
		externalState = *override
	}
	build(keyvalv1alpha1.ConditionExternalImport, externalState)

	storageState := ConditionState{Status: metav1.ConditionTrue, Reason: "RetentionPolicy", Message: "PVC cleanup disabled; resources retained after deletion"}
	if !core.HasPersistentData(cr) {
		storageState = ConditionState{Status: metav1.ConditionTrue, Reason: "EphemeralStorage", Message: "Ephemeral storage configured; no PVC cleanup required"}
	} else if core.StorageCleanupEnabled(cr) {
		storageState = ConditionState{Status: metav1.ConditionTrue, Reason: "CleanupEnabled", Message: "PVCs will be deleted when the cluster is removed"}
	}
	if override := getOverride(keyvalv1alpha1.ConditionStorageCleanup); override != nil {
		storageState = *override
	}
	build(keyvalv1alpha1.ConditionStorageCleanup, storageState)

	if st.RuntimeConfig != nil {
		runtimeState := ConditionState{Status: st.RuntimeConfig.Status, Reason: st.RuntimeConfig.Reason, Message: st.RuntimeConfig.Message}
		build(keyvalv1alpha1.ConditionRuntimeConfigApplied, runtimeState)
	} else if prev, ok := prevConds[string(keyvalv1alpha1.ConditionRuntimeConfigApplied)]; ok {
		conds = append(conds, prev)
	}

	if st.ExternalImport != nil {
		s.ExternalImport = st.ExternalImport.DeepCopy()
	}

	s.Conditions = conds
	allow := conditionSatisfied(built[keyvalv1alpha1.ConditionAvailable], true) &&
		conditionSatisfied(built[keyvalv1alpha1.ConditionSentinelQuorum], true) &&
		conditionSatisfied(built[keyvalv1alpha1.ConditionReplicationHealthy], true) &&
		conditionSatisfied(built[keyvalv1alpha1.ConditionFailoverInProgress], false) &&
		conditionSatisfied(built[keyvalv1alpha1.ConditionDisruptionsPaused], false) &&
		conditionSatisfied(built[keyvalv1alpha1.ConditionUpgradeInProgress], false) &&
		conditionSatisfied(built[keyvalv1alpha1.ConditionBootstrapInProgress], false) &&
		conditionSatisfied(built[keyvalv1alpha1.ConditionExternalImport], true)

	s.HealthGate = &keyvalv1alpha1.HealthGateStatus{AllowDisruptions: allow}
	return s
}

func computeAvailableCondition(cr *keyvalv1alpha1.KeyValCluster, st ClusterState, masters int, masterReady bool, readyCount int32) ConditionState {
	if len(st.Pods) == 0 {
		msg := augmentReplicationMessage("redis pods have not been created yet", st)
		return ConditionState{Status: metav1.ConditionUnknown, Reason: "NoPods", Message: msg}
	}
	if st.Master == "" {
		return ConditionState{Status: metav1.ConditionFalse, Reason: "MasterUnknown", Message: "master pod undetermined"}
	}
	if masters != 1 {
		return ConditionState{Status: metav1.ConditionFalse, Reason: "MasterElection", Message: fmt.Sprintf("observed %d master candidates", masters)}
	}
	if !masterReady {
		return ConditionState{Status: metav1.ConditionFalse, Reason: "MasterNotReady", Message: fmt.Sprintf("master pod %s is not Ready", st.Master)}
	}
	return ConditionState{Status: metav1.ConditionTrue, Reason: "MasterReady", Message: fmt.Sprintf("master pod %s is ready", st.Master)}
}

func computeSentinelCondition(cr *keyvalv1alpha1.KeyValCluster, st ClusterState) ConditionState {
	if cr.Spec.Mode != keyvalv1alpha1.ModeSentinel {
		return ConditionState{Status: metav1.ConditionTrue, Reason: "SentinelNotRequired", Message: "sentinel quorum not required in Standalone mode"}
	}
	expected := int32(3)
	if cr.Spec.SentinelCount != nil && *cr.Spec.SentinelCount > 0 {
		expected = *cr.Spec.SentinelCount
	}
	observed := int32(0)
	ready := 0
	for i := range st.SentinelPods {
		pod := st.SentinelPods[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		observed++
		if runtime.IsPodReady(&pod) {
			ready++
		}
	}
	if observed < expected {
		return ConditionState{Status: metav1.ConditionFalse, Reason: "InsufficientSentinels", Message: fmt.Sprintf("observed %d/%d sentinel pods", observed, expected)}
	}
	quorum := int(expected/2) + 1
	if ready < quorum {
		return ConditionState{Status: metav1.ConditionFalse, Reason: "SentinelQuorumNotReady", Message: fmt.Sprintf("ready sentinel pods %d, quorum %d", ready, quorum)}
	}
	return ConditionState{Status: metav1.ConditionTrue, Reason: "SentinelQuorumAchieved", Message: fmt.Sprintf("ready sentinel pods %d/%d", ready, expected)}
}

func computeReplicationHealthyCondition(cr *keyvalv1alpha1.KeyValCluster, st ClusterState, masterHealth keyvalv1alpha1.PodHealth, requiredMembers int32) ConditionState {
	if len(st.Pods) == 0 {
		return ConditionState{Status: metav1.ConditionUnknown, Reason: "NoPods", Message: "redis pods have not been created yet"}
	}
	healthyMembers := int32(0)
	issues := make([]string, 0)
	if st.Master != "" {
		if masterHealth == keyvalv1alpha1.PodHealthHealthy {
			healthyMembers++
		} else {
			issues = append(issues, fmt.Sprintf("master %s health=%s", st.Master, masterHealth))
		}
	}
	replicaCount := 0
	for _, p := range st.Pods {
		role := st.Roles[p.Name]
		if role == keyvalv1alpha1.PodRoleReplica {
			replicaCount++
			health := st.Health[p.Name]
			if health == "" {
				health = keyvalv1alpha1.PodHealthOffline
			}
			if health == keyvalv1alpha1.PodHealthHealthy {
				healthyMembers++
			} else {
				issues = append(issues, fmt.Sprintf("replica %s health=%s", p.Name, health))
			}
		}
	}
	if replicaCount == 0 && cr.Spec.Mode != keyvalv1alpha1.ModeStandalone {
		issues = append(issues, "no replica pods available")
	}
	if healthyMembers < requiredMembers {
		message := fmt.Sprintf("healthy members %d below safety threshold %d", healthyMembers, requiredMembers)
		if len(issues) > 0 {
			message = fmt.Sprintf("%s; %s", message, strings.Join(issues, "; "))
		}
		return ConditionState{Status: metav1.ConditionFalse, Reason: "InsufficientHealthyMembers", Message: augmentReplicationMessage(message, st)}
	}
	if len(issues) > 0 {
		msg := augmentReplicationMessage(strings.Join(issues, "; "), st)
		return ConditionState{Status: metav1.ConditionFalse, Reason: "ReplicaUnhealthy", Message: msg}
	}
	msg := augmentReplicationMessage(fmt.Sprintf("%d healthy members", healthyMembers), st)
	return ConditionState{Status: metav1.ConditionTrue, Reason: "ReplicationAligned", Message: msg}
}

func computeFailoverCondition(masters int) ConditionState {
	if masters == 1 {
		return ConditionState{Status: metav1.ConditionFalse, Reason: "Stable", Message: "single master observed"}
	}
	if masters == 0 {
		return ConditionState{Status: metav1.ConditionTrue, Reason: "NoMaster", Message: "master election in progress"}
	}
	return ConditionState{Status: metav1.ConditionTrue, Reason: "MultipleMasters", Message: fmt.Sprintf("observed %d masters", masters)}
}

func augmentReplicationMessage(msg string, st ClusterState) string {
	if st.RolesSource == "" {
		return msg
	}
	detail := fmt.Sprintf("rolesSource=%s", st.RolesSource)
	if st.RolesReason != "" {
		detail = fmt.Sprintf("%s reason=%s", detail, st.RolesReason)
	}
	if msg == "" {
		return detail
	}
	if strings.Contains(msg, detail) {
		return msg
	}
	return msg + "; " + detail
}

func conditionSatisfied(cond metav1.Condition, wantTrue bool) bool {
	if cond.Type == "" {
		return false
	}
	if cond.Status == metav1.ConditionUnknown {
		return false
	}
	if wantTrue {
		return cond.Status == metav1.ConditionTrue
	}
	return cond.Status == metav1.ConditionFalse
}

func UpdateStatus(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, st ClusterState) error {
	logger := logging.FromContext(ctx)
	want := ComputeStatus(cr, st)
	if reflect.DeepEqual(cr.Status, want) {
		opobs.IncStatusUpdate(cr, false)
		logger.V(1).Info("status is up to date", "namespace", cr.Namespace, "cluster", cr.Name)
		return nil
	}
	changes := summarizeStatusChanges(cr.Status, want)
	base := cr.DeepCopy()
	cr.Status = want
	if err := c.Status().Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		return err
	}
	opobs.IncStatusUpdate(cr, true)
	logger.V(1).Info("status updated", "namespace", cr.Namespace, "cluster", cr.Name, "changes", strings.Join(changes, ","))
	return nil
}

func summarizeStatusChanges(prev, next keyvalv1alpha1.KeyValClusterStatus) []string {
	changes := make([]string, 0, 8)
	if prev.MasterPod != next.MasterPod {
		changes = append(changes, fmt.Sprintf("master:%s->%s", prev.MasterPod, next.MasterPod))
	}
	if prev.RolesSource != next.RolesSource {
		changes = append(changes, fmt.Sprintf("rolesSource:%s->%s", prev.RolesSource, next.RolesSource))
	}
	if prev.ReadyReplicas != next.ReadyReplicas {
		changes = append(changes, fmt.Sprintf("readyReplicas:%d->%d", prev.ReadyReplicas, next.ReadyReplicas))
	}
	condChanges := diffConditions(prev.Conditions, next.Conditions)
	if len(condChanges) > 0 {
		changes = append(changes, condChanges...)
	}
	roleChanges := diffRoles(prev.Roles, next.Roles)
	if len(roleChanges) > 0 {
		changes = append(changes, fmt.Sprintf("roles[%s]", strings.Join(roleChanges, ",")))
	}
	if prev.HealthGate == nil && next.HealthGate != nil {
		changes = append(changes, "healthGate:new")
	} else if prev.HealthGate != nil && next.HealthGate != nil && prev.HealthGate.AllowDisruptions != next.HealthGate.AllowDisruptions {
		changes = append(changes, fmt.Sprintf("healthGate.allowDisruptions:%t->%t", prev.HealthGate.AllowDisruptions, next.HealthGate.AllowDisruptions))
	}
	if len(changes) == 0 {
		changes = append(changes, "conditions")
	}
	return changes
}

func diffConditions(prev, next []metav1.Condition) []string {
	prevMap := make(map[string]metav1.Condition, len(prev))
	for _, cond := range prev {
		prevMap[cond.Type] = cond
	}
	changes := make([]string, 0, len(next))
	seen := make(map[string]struct{}, len(next))
	for _, cond := range next {
		seen[cond.Type] = struct{}{}
		if prevCond, ok := prevMap[cond.Type]; ok {
			if condDiff := describeConditionChange(prevCond, cond); condDiff != "" {
				changes = append(changes, fmt.Sprintf("condition:%s[%s]", cond.Type, condDiff))
			}
		} else {
			changes = append(changes, fmt.Sprintf("condition:%s[new]", cond.Type))
		}
	}
	for typ := range prevMap {
		if _, ok := seen[typ]; !ok {
			changes = append(changes, fmt.Sprintf("condition:%s[removed]", typ))
		}
	}
	sort.Strings(changes)
	return changes
}

func describeConditionChange(prev, next metav1.Condition) string {
	parts := make([]string, 0, 5)
	if prev.Status != next.Status {
		parts = append(parts, "status")
	}
	if prev.Reason != next.Reason {
		parts = append(parts, "reason")
	}
	if prev.Message != next.Message {
		parts = append(parts, "message")
	}
	if !prev.LastTransitionTime.Equal(&next.LastTransitionTime) {
		parts = append(parts, "lastTransitionTime")
	}
	if prev.ObservedGeneration != next.ObservedGeneration {
		parts = append(parts, "observedGeneration")
	}
	return strings.Join(parts, "|")
}

func diffRoles(prev, next []keyvalv1alpha1.PodRoleStatus) []string {
	prevMap := make(map[string]keyvalv1alpha1.PodRoleStatus, len(prev))
	for _, role := range prev {
		prevMap[role.Name] = role
	}
	changes := make([]string, 0, len(next))
	seen := make(map[string]struct{}, len(next))
	for _, role := range next {
		seen[role.Name] = struct{}{}
		if prevRole, ok := prevMap[role.Name]; ok {
			if !rolesEqual(prevRole, role) {
				changes = append(changes, role.Name)
			}
		} else {
			changes = append(changes, role.Name+"[new]")
		}
	}
	for name := range prevMap {
		if _, ok := seen[name]; !ok {
			changes = append(changes, name+"[removed]")
		}
	}
	sort.Strings(changes)
	return changes
}

func rolesEqual(a, b keyvalv1alpha1.PodRoleStatus) bool {
	if a.Role != b.Role || a.Ready != b.Ready || a.Health != b.Health {
		return false
	}
	if (a.LagSeconds == nil) != (b.LagSeconds == nil) {
		return false
	}
	if a.LagSeconds != nil && b.LagSeconds != nil && *a.LagSeconds != *b.LagSeconds {
		return false
	}
	return true
}
