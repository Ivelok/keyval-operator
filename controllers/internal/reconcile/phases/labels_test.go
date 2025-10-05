package phases

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"k8s.io/client-go/tools/record"
)

func TestLabelsNoop(t *testing.T) {
	scheme := newScheme(t)
	cluster := baseCluster()
	pods := []*corev1.Pod{
		newRedisPod(cluster, "redis-0", string(keyvalv1alpha1.PodRoleMaster)),
		newRedisPod(cluster, "redis-1", string(keyvalv1alpha1.PodRoleReplica)),
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy()).
		WithRuntimeObjects(toRuntimeObjects(pods)...).
		Build()

	rc := &recordingClient{Client: k8sClient}

	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   rc,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Runtime:     reconcile.RuntimeState{Master: "redis-0"},
		RedisPods:   copyPods(pods),
	}

	if err := Labels(context.Background(), state); err != nil {
		t.Fatalf("labels phase failed: %v", err)
	}
	if got := len(rc.patches); got != 0 {
		t.Fatalf("expected no patches, got %d", got)
	}
	if state.Accumulator.Result() != 0 {
		t.Fatalf("expected no requeue delay, got %s", state.Accumulator.Result())
	}
}

func TestLabelsDemoteFirst(t *testing.T) {
	scheme := newScheme(t)
	cluster := baseCluster()
	pods := []*corev1.Pod{
		newRedisPod(cluster, "redis-0", string(keyvalv1alpha1.PodRoleMaster)),
		newRedisPod(cluster, "redis-1", string(keyvalv1alpha1.PodRoleMaster)),
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy()).
		WithRuntimeObjects(toRuntimeObjects(pods)...).
		Build()

	rc := &recordingClient{Client: k8sClient}
	recorder := record.NewFakeRecorder(10)

	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   rc,
			Recorder: recorder,
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Runtime:     reconcile.RuntimeState{Master: "redis-0"},
		RedisPods:   copyPods(pods),
	}

	if err := Labels(context.Background(), state); err != nil {
		t.Fatalf("labels phase failed: %v", err)
	}

	if len(rc.patches) != 1 {
		t.Fatalf("expected 1 patch, got %d", len(rc.patches))
	}
	if rc.patches[0] != "redis-1" {
		t.Fatalf("expected first patch to demote redis-1, got %s", rc.patches[0])
	}

	for i := range state.RedisPods {
		pod := &state.RedisPods[i]
		role := pod.Labels[core.RoleLabelKey]
		switch pod.Name {
		case "redis-0":
			if role != string(keyvalv1alpha1.PodRoleMaster) {
				t.Fatalf("expected redis-0 master label, got %s", role)
			}
		case "redis-1":
			if role != string(keyvalv1alpha1.PodRoleReplica) {
				t.Fatalf("expected redis-1 demoted to replica, got %s", role)
			}
		}
	}

	select {
	case evt := <-recorder.Events:
		if !strings.Contains(evt, "RoleCorrected") {
			t.Fatalf("expected RoleCorrected event, got %q", evt)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected RoleCorrected event")
	}
}

func TestLabelsConflictSchedulesBackoff(t *testing.T) {
	scheme := newScheme(t)
	cluster := baseCluster()
	pod := newRedisPod(cluster, "redis-0", string(keyvalv1alpha1.PodRoleReplica))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy(), pod).
		Build()

	rc := &recordingClient{
		Client:    k8sClient,
		conflicts: map[string]int{"redis-0": 1},
	}

	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   rc,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Runtime:     reconcile.RuntimeState{Master: "redis-0"},
		RedisPods:   copyPods([]*corev1.Pod{pod}),
	}

	if err := Labels(context.Background(), state); err != nil {
		t.Fatalf("labels phase failed: %v", err)
	}

	if got, want := state.Accumulator.Result(), reconcile.RoleLabelConflictBackoff(1); got != want {
		t.Fatalf("expected backoff %s, got %s", want, got)
	}

	var updated corev1.Pod
	if err := rc.Get(context.Background(), ctrlclient.ObjectKey{Namespace: cluster.Namespace, Name: "redis-0"}, &updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if updated.Labels[core.RoleLabelKey] != string(keyvalv1alpha1.PodRoleMaster) {
		t.Fatalf("expected redis-0 labeled master, got %s", updated.Labels[core.RoleLabelKey])
	}
}

// recordingClient wraps a controller-runtime client to capture patch order and inject conflicts.
type recordingClient struct {
	ctrlclient.Client
	patches   []string
	conflicts map[string]int
}

func (c *recordingClient) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
	name := obj.GetName()
	c.patches = append(c.patches, name)
	if c.conflicts != nil {
		if remaining := c.conflicts[name]; remaining > 0 {
			c.conflicts[name] = remaining - 1
			return apierrors.NewConflict(schema.GroupResource{Group: "", Resource: "pods"}, name, fmt.Errorf("conflict"))
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func baseCluster() *keyvalv1alpha1.KeyValCluster {
	return &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 2,
		},
	}
}

func newRedisPod(cluster *keyvalv1alpha1.KeyValCluster, name string, role string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       cluster.Namespace,
			Labels:          map[string]string{core.RoleLabelKey: role},
			ResourceVersion: "1",
		},
	}
}

func toRuntimeObjects(pods []*corev1.Pod) []runtime.Object {
	out := make([]runtime.Object, len(pods))
	for i := range pods {
		out[i] = pods[i]
	}
	return out
}

func copyPods(pods []*corev1.Pod) []corev1.Pod {
	out := make([]corev1.Pod, len(pods))
	for i := range pods {
		out[i] = *pods[i]
	}
	return out
}
