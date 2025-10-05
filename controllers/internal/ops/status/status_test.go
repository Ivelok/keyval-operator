package status

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

func TestComputeStatusPreservesConditionLastTransitionTime(t *testing.T) {
	t.Parallel()

	cr := &keyvalv1alpha1.KeyValCluster{}
	cr.Spec.Mode = keyvalv1alpha1.ModeStandalone
	cr.Spec.RedisReplicas = 1

	oldTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	cr.Status.Conditions = []metav1.Condition{{
		Type:               string(keyvalv1alpha1.ConditionAvailable),
		Status:             metav1.ConditionTrue,
		Reason:             "MasterReady",
		Message:            "master pod demo-0 is ready",
		LastTransitionTime: oldTime,
	}}

	st := ClusterState{
		Master: "demo-0",
		Pods: []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-0"},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}},
		Roles:  map[string]keyvalv1alpha1.PodRole{"demo-0": keyvalv1alpha1.PodRoleMaster},
		Health: map[string]keyvalv1alpha1.PodHealth{"demo-0": keyvalv1alpha1.PodHealthHealthy},
		Conditions: map[keyvalv1alpha1.ConditionType]*ConditionState{
			keyvalv1alpha1.ConditionAvailable: {
				Status:  metav1.ConditionTrue,
				Reason:  "MasterReady",
				Message: "custom available message",
			},
		},
	}

	got := ComputeStatus(cr, st)

	cond := findCondition(t, got.Conditions, keyvalv1alpha1.ConditionAvailable)
	if cond.Message != "custom available message" {
		t.Fatalf("expected message to update, got %q", cond.Message)
	}
	if !cond.LastTransitionTime.Equal(&oldTime) {
		t.Fatalf("expected LTT to be preserved, want %v got %v", oldTime, cond.LastTransitionTime)
	}
}

func TestUpdateStatus_NoPatchOnNoop(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	cr := &keyvalv1alpha1.KeyValCluster{}
	cr.Name = "demo"
	cr.Namespace = "default"
	cr.Spec.Mode = keyvalv1alpha1.ModeStandalone
	cr.Spec.RedisReplicas = 1

	st := readyClusterState()
	baseline := ComputeStatus(cr, st)
	cr.Status = baseline

	stored := cr.DeepCopy()
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(stored).WithRuntimeObjects(stored).Build()
	rc := &recordingClient{Client: client}

	if err := client.Get(context.Background(), clientObjectKey(cr), cr); err != nil {
		t.Fatalf("pre-fetch cluster: %v", err)
	}

	if err := UpdateStatus(context.Background(), rc, cr, st); err != nil {
		t.Fatalf("update status noop: %v", err)
	}
	if rc.patched {
		t.Fatalf("expected no patch on noop status update")
	}
}

func TestUpdateStatus_PatchOnChange(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	cr := &keyvalv1alpha1.KeyValCluster{}
	cr.Name = "demo"
	cr.Namespace = "default"
	cr.Spec.Mode = keyvalv1alpha1.ModeStandalone
	cr.Spec.RedisReplicas = 1

	st := readyClusterState()
	baseline := ComputeStatus(cr, st)
	cr.Status = baseline

	stored := cr.DeepCopy()
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(stored).WithRuntimeObjects(stored).Build()
	rc := &recordingClient{Client: client}

	if err := client.Get(context.Background(), clientObjectKey(cr), cr); err != nil {
		t.Fatalf("pre-fetch cluster: %v", err)
	}

	// Mark the master as not ready to force a condition change.
	st.Pods[0].Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	st.Health[st.Pods[0].Name] = keyvalv1alpha1.PodHealthOffline

	if err := UpdateStatus(context.Background(), rc, cr, st); err != nil {
		t.Fatalf("update status changed: %v", err)
	}
	if !rc.patched {
		t.Fatalf("expected patch when status changes")
	}

	updated := &keyvalv1alpha1.KeyValCluster{}
	if err := client.Get(context.Background(), clientObjectKey(cr), updated); err != nil {
		t.Fatalf("fetch updated cluster: %v", err)
	}
	cond := findCondition(t, updated.Status.Conditions, keyvalv1alpha1.ConditionAvailable)
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("expected available status false, got %s", cond.Status)
	}
	if cond.Reason != "MasterNotReady" {
		t.Fatalf("expected reason MasterNotReady, got %s", cond.Reason)
	}
}

func TestComputeStatusIncludesRolesSourceDetail(t *testing.T) {
	t.Parallel()

	cr := &keyvalv1alpha1.KeyValCluster{}
	cr.Name = "demo"
	cr.Namespace = "default"
	cr.Spec.Mode = keyvalv1alpha1.ModeSentinel
	cr.Spec.RedisReplicas = 3

	st := ClusterState{
		Master:      "demo-0",
		Roles:       map[string]keyvalv1alpha1.PodRole{"demo-0": keyvalv1alpha1.PodRoleMaster, "demo-1": keyvalv1alpha1.PodRoleReplica, "demo-2": keyvalv1alpha1.PodRoleReplica},
		Health:      map[string]keyvalv1alpha1.PodHealth{"demo-0": keyvalv1alpha1.PodHealthHealthy, "demo-1": keyvalv1alpha1.PodHealthHealthy, "demo-2": keyvalv1alpha1.PodHealthHealthy},
		Pods:        []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}, {ObjectMeta: metav1.ObjectMeta{Name: "demo-1"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}, {ObjectMeta: metav1.ObjectMeta{Name: "demo-2"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}},
		RolesSource: keyvalv1alpha1.RolesSourceProbe,
		RolesReason: "sentinel timeout",
	}

	status := ComputeStatus(cr, st)
	if status.RolesSource != keyvalv1alpha1.RolesSourceProbe {
		t.Fatalf("expected roles source probe, got %s", status.RolesSource)
	}
	cond := findCondition(t, status.Conditions, keyvalv1alpha1.ConditionReplicationHealthy)
	if !strings.Contains(cond.Message, "rolesSource=probe") {
		t.Fatalf("expected replication condition message to include rolesSource, got %q", cond.Message)
	}
	if !strings.Contains(cond.Message, "reason=sentinel timeout") {
		t.Fatalf("expected replication condition message to include reason, got %q", cond.Message)
	}
}

func TestComputeStatusStorageCleanupPolicy(t *testing.T) {
	t.Parallel()

	base := &keyvalv1alpha1.KeyValCluster{}
	base.Name = "demo"
	base.Namespace = "default"
	base.Spec.Mode = keyvalv1alpha1.ModeStandalone
	base.Spec.RedisReplicas = 1

	state := readyClusterState()

	defaultStatus := ComputeStatus(base.DeepCopy(), state)
	cond := findCondition(t, defaultStatus.Conditions, keyvalv1alpha1.ConditionStorageCleanup)
	if cond.Reason != "RetentionPolicy" {
		t.Fatalf("expected retention policy reason, got %s", cond.Reason)
	}

	cleanup := base.DeepCopy()
	cleanup.Spec.Storage = &keyvalv1alpha1.StorageSpec{CleanupOnDelete: true}
	cleanupStatus := ComputeStatus(cleanup, state)
	cond = findCondition(t, cleanupStatus.Conditions, keyvalv1alpha1.ConditionStorageCleanup)
	if cond.Reason != "CleanupEnabled" {
		t.Fatalf("expected cleanup enabled reason, got %s", cond.Reason)
	}

	ephemeral := base.DeepCopy()
	ephemeral.Spec.Storage = &keyvalv1alpha1.StorageSpec{Type: "Ephemeral"}
	ephemeralStatus := ComputeStatus(ephemeral, state)
	cond = findCondition(t, ephemeralStatus.Conditions, keyvalv1alpha1.ConditionStorageCleanup)
	if cond.Reason != "EphemeralStorage" {
		t.Fatalf("expected ephemeral storage reason, got %s", cond.Reason)
	}
}

func readyClusterState() ClusterState {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	return ClusterState{
		Master: "demo-0",
		Pods:   []corev1.Pod{pod},
		Roles: map[string]keyvalv1alpha1.PodRole{
			"demo-0": keyvalv1alpha1.PodRoleMaster,
		},
		Health: map[string]keyvalv1alpha1.PodHealth{
			"demo-0": keyvalv1alpha1.PodHealthHealthy,
		},
	}
}

func findCondition(t *testing.T, conds []metav1.Condition, typ keyvalv1alpha1.ConditionType) metav1.Condition {
	t.Helper()
	for _, cond := range conds {
		if cond.Type == string(typ) {
			return cond
		}
	}
	t.Fatalf("condition %s not found", typ)
	return metav1.Condition{}
}

func clientObjectKey(cr *keyvalv1alpha1.KeyValCluster) client.ObjectKey {
	return client.ObjectKey{Namespace: cr.Namespace, Name: cr.Name}
}

type recordingClient struct {
	client.Client
	patched bool
}

type recordingStatusWriter struct {
	client.StatusWriter
	patched *bool
}

func (r *recordingClient) Status() client.StatusWriter {
	return &recordingStatusWriter{StatusWriter: r.Client.Status(), patched: &r.patched}
}

func (w *recordingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	*w.patched = true
	return w.StatusWriter.Patch(ctx, obj, patch, opts...)
}
