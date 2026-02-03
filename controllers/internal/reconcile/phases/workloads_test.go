package phases

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
	"k8s.io/client-go/tools/record"
)

func TestWorkloadsRedisOnly(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 1,
		},
	}
	hash := "cfg-hash"
	secSettings := security.Settings{}
	redisSS := resources.StatefulSet(cluster, hash, "", &secSettings)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      redisSS.Name + "-0",
			Namespace: cluster.Namespace,
			Labels:    redisSS.Spec.Selector.MatchLabels,
		},
	}

	objs := []runtime.Object{cluster.DeepCopy(), pod}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

	observed := 0
	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   client,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
			ObserveGracefulShutdown: func(_ *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod) {
				observed += len(pods)
			},
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Security:    reconcile.SecurityState{Settings: secSettings},
		Config:      reconcile.ConfigState{ConfigHash: hash},
	}

	if err := Workloads(context.Background(), state); err != nil {
		t.Fatalf("workloads phase failed: %v", err)
	}
	if state.RedisStatefulSet == nil {
		t.Fatalf("expected redis statefulset captured")
	}
	if got := len(state.RedisPods); got != 1 {
		t.Fatalf("expected 1 redis pod, got %d", got)
	}
	if state.Accumulator.Result() != 0 {
		t.Fatalf("expected no requeue, got %s", state.Accumulator.Result())
	}
	if len(state.SentinelPods) != 0 {
		t.Fatalf("expected no sentinel pods, got %d", len(state.SentinelPods))
	}
	if observed != 1 {
		t.Fatalf("expected graceful shutdown observer to see 1 pod, saw %d", observed)
	}
}

func TestWorkloadsSentinelMissingPods(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 3,
			SentinelCount: ptr.To[int32](3),
		},
	}
	hash := "cfg-hash"
	secSettings := security.Settings{}
	redisSS := resources.StatefulSet(cluster, hash, "", &secSettings)
	pods := []*corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      redisSS.Name + "-0",
				Namespace: cluster.Namespace,
				Labels:    redisSS.Spec.Selector.MatchLabels,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      redisSS.Name + "-1",
				Namespace: cluster.Namespace,
				Labels:    redisSS.Spec.Selector.MatchLabels,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      redisSS.Name + "-2",
				Namespace: cluster.Namespace,
				Labels:    redisSS.Spec.Selector.MatchLabels,
			},
		},
	}
	objs := []runtime.Object{cluster.DeepCopy()}
	for _, p := range pods {
		objs = append(objs, p)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   client,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Security:    reconcile.SecurityState{Settings: secSettings},
		Config:      reconcile.ConfigState{ConfigHash: hash},
	}

	if err := Workloads(context.Background(), state); err != nil {
		t.Fatalf("workloads phase failed: %v", err)
	}
	if len(state.RedisPods) != len(pods) {
		t.Fatalf("expected %d redis pods, got %d", len(pods), len(state.RedisPods))
	}
	if state.RedisStatefulSet == nil {
		t.Fatalf("expected redis statefulset captured")
	}
	if state.SentinelStatefulSet == nil {
		t.Fatalf("expected sentinel statefulset captured")
	}
	if len(state.SentinelPods) != 0 {
		t.Fatalf("expected sentinel pods empty, got %d", len(state.SentinelPods))
	}
	delay := state.Accumulator.Result()
	if delay < 2*time.Second {
		t.Fatalf("expected sentinel requeue >=2s, got %s", delay)
	}
}

func TestWorkloadsRedisApplyConflictSetsCondition(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 1,
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster.DeepCopy()).Build()
	c := &applyConflictClient{
		Client:  baseClient,
		target:  cluster.Name,
		message: "Apply failed with 1 conflict: conflict with \"conflict-manager\": .spec.template.spec.terminationGracePeriodSeconds",
	}
	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   c,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Security:    reconcile.SecurityState{Settings: security.Settings{}},
		Config:      reconcile.ConfigState{ConfigHash: "cfg"},
	}

	opobs.RedisApplyFailureCounter().DeleteLabelValues("default", "demo", opobs.RedisApplyFailureReasonConflict)

	if err := Workloads(context.Background(), state); err != nil {
		t.Fatalf("workloads returned error: %v", err)
	}
	if state.AbortDirective == nil {
		t.Fatalf("expected abort directive recorded")
	}
	if state.AbortDirective.Err == nil || !controllererrors.IsFatal(state.AbortDirective.Err) {
		t.Fatalf("expected fatal deferred error, got %v", state.AbortDirective.Err)
	}
	cond := state.ConditionOverrides[keyvalv1alpha1.ConditionReconciled]
	if cond == nil {
		t.Fatalf("expected reconciled condition override")
	}
	if cond.Reason != conflictConditionReason {
		t.Fatalf("expected reason %s, got %s", conflictConditionReason, cond.Reason)
	}
	if !strings.Contains(cond.Message, "conflict-manager") {
		t.Fatalf("expected message to mention field manager, got %q", cond.Message)
	}
	if state.RedisStatefulSet != nil {
		t.Fatalf("expected redis statefulset not persisted on conflict")
	}
	recorder := state.Dependencies.Recorder.(*record.FakeRecorder)
	expectEvent(t, recorder, "RedisApplyConflict")
	counter, err := opobs.RedisApplyFailureCounter().GetMetricWith(prometheus.Labels{
		"namespace": "default",
		"cluster":   "demo",
		"reason":    opobs.RedisApplyFailureReasonConflict,
	})
	if err != nil {
		t.Fatalf("get redis conflict metric: %v", err)
	}
	if got := testutil.ToFloat64(counter); got < 1.0 {
		t.Fatalf("expected redis conflict metric >=1, got %f", got)
	}
}

func TestWorkloadsSentinelApplyConflictSetsCondition(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 3,
			SentinelCount: ptr.To[int32](3),
		},
	}
	redisSS := resources.StatefulSet(cluster, "cfg", "", &security.Settings{})
	var objs []runtime.Object
	objs = append(objs, cluster.DeepCopy())
	for i := 0; i < int(ptr.Deref(redisSS.Spec.Replicas, 0)); i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", redisSS.Name, i),
				Namespace: cluster.Namespace,
				Labels:    redisSS.Spec.Selector.MatchLabels,
			},
		}
		objs = append(objs, pod)
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	c := &applyConflictClient{
		Client:  baseClient,
		target:  cluster.Name + "-sentinel",
		message: "Apply failed with 1 conflict: conflict with \"kubectl-replace\": .spec.serviceName",
	}
	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   c,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Security:    reconcile.SecurityState{Settings: security.Settings{}},
		Config:      reconcile.ConfigState{ConfigHash: "cfg"},
	}

	opobs.SentinelApplyFailureCounter().DeleteLabelValues("default", "demo", opobs.SentinelApplyFailureReasonConflict)

	if err := Workloads(context.Background(), state); err != nil {
		t.Fatalf("workloads returned error: %v", err)
	}
	if state.AbortDirective == nil {
		t.Fatalf("expected abort directive recorded")
	}
	if state.AbortDirective.Err == nil || !controllererrors.IsFatal(state.AbortDirective.Err) {
		t.Fatalf("expected fatal deferred error, got %v", state.AbortDirective.Err)
	}
	cond := state.ConditionOverrides[keyvalv1alpha1.ConditionReconciled]
	if cond == nil {
		t.Fatalf("expected reconciled condition override")
	}
	if cond.Reason != conflictConditionReason {
		t.Fatalf("expected reason %s, got %s", conflictConditionReason, cond.Reason)
	}
	if !strings.Contains(cond.Message, "kubectl-replace") {
		t.Fatalf("expected message to mention conflicting manager, got %q", cond.Message)
	}
	if state.SentinelStatefulSet != nil {
		t.Fatalf("expected sentinel statefulset not persisted on conflict")
	}
	if state.RedisStatefulSet == nil {
		t.Fatalf("expected redis statefulset persisted when sentinel conflict occurs")
	}
	recorder := state.Dependencies.Recorder.(*record.FakeRecorder)
	expectEvent(t, recorder, "SentinelApplyConflict")
	counter, err := opobs.SentinelApplyFailureCounter().GetMetricWith(prometheus.Labels{
		"namespace": "default",
		"cluster":   "demo",
		"reason":    opobs.SentinelApplyFailureReasonConflict,
	})
	if err != nil {
		t.Fatalf("get sentinel conflict metric: %v", err)
	}
	if got := testutil.ToFloat64(counter); got < 1.0 {
		t.Fatalf("expected sentinel conflict metric >=1, got %f", got)
	}
}

func TestWorkloadsRedisApplyErrorEmitsMetrics(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 1,
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster.DeepCopy()).Build()
	c := &applyErrorClient{
		Client: baseClient,
		target: cluster.Name,
		err:    fmt.Errorf("boom"),
	}
	recorder := record.NewFakeRecorder(10)
	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   c,
			Recorder: recorder,
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Security:    reconcile.SecurityState{Settings: security.Settings{}},
		Config:      reconcile.ConfigState{ConfigHash: "cfg"},
	}

	opobs.RedisApplyFailureCounter().DeleteLabelValues("default", "demo", opobs.RedisApplyFailureReasonError)

	err := Workloads(context.Background(), state)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !controllererrors.IsTransient(err) {
		t.Fatalf("expected transient error, got %v", err)
	}
	if state.AbortDirective != nil {
		t.Fatalf("did not expect abort directive for transient error")
	}
	expectEvent(t, recorder, "RedisApplyFailed")
	counter, cErr := opobs.RedisApplyFailureCounter().GetMetricWith(prometheus.Labels{
		"namespace": "default",
		"cluster":   "demo",
		"reason":    opobs.RedisApplyFailureReasonError,
	})
	if cErr != nil {
		t.Fatalf("get redis error metric: %v", cErr)
	}
	if got := testutil.ToFloat64(counter); got < 1.0 {
		t.Fatalf("expected redis error metric >=1, got %f", got)
	}
}

func TestWorkloadsRedisApplyImmutableMarksConfigDrift(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 1,
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster.DeepCopy()).Build()
	invalidErr := apierrors.NewInvalid(
		schema.GroupKind{Group: appsv1.GroupName, Kind: "StatefulSet"},
		cluster.Name,
		field.ErrorList{field.Invalid(field.NewPath("spec").Child("serviceName"), "", "field is immutable")},
	)
	c := &applyErrorClient{
		Client: baseClient,
		target: cluster.Name,
		err:    invalidErr,
	}
	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   c,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Security:    reconcile.SecurityState{Settings: security.Settings{}},
		Config:      reconcile.ConfigState{ConfigHash: "cfg"},
	}

	err := Workloads(context.Background(), state)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !controllererrors.IsTransient(err) {
		t.Fatalf("expected transient error, got %v", err)
	}
	if !errors.Is(err, controllererrors.ErrConfigDrift) {
		t.Fatalf("expected ErrConfigDrift, got %v", err)
	}
}

type applyConflictClient struct {
	ctrlclient.Client
	target  string
	message string
}

func (c *applyConflictClient) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
	if obj.GetName() == c.target {
		return apierrors.NewConflict(schema.GroupResource{Group: appsv1.GroupName, Resource: "statefulsets"}, c.target, fmt.Errorf("%s", c.message))
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

type applyErrorClient struct {
	ctrlclient.Client
	target string
	err    error
}

func (c *applyErrorClient) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
	if obj.GetName() == c.target {
		return c.err
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func expectEvent(t *testing.T, recorder *record.FakeRecorder, reason string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case evt := <-recorder.Events:
			if strings.Contains(evt, reason) {
				return
			}
		case <-deadline:
			t.Fatalf("expected event with reason %s", reason)
		}
	}
}

func TestWorkloadsTLSRotationEvent(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 1,
			Security: &keyvalv1alpha1.SecuritySpec{
				TLS: &keyvalv1alpha1.TLSSpec{
					Enabled:    true,
					SecretName: "redis-tls",
				},
			},
		},
	}
	hash := "cfg-hash"
	secSettings := security.Settings{}
	secSettings.TLS.Enabled = true
	secSettings.TLS.SecretName = "redis-tls"

	redisSS := resources.StatefulSet(cluster, hash, "old-hash", &secSettings)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      redisSS.Name + "-0",
			Namespace: cluster.Namespace,
			Labels:    redisSS.Spec.Selector.MatchLabels,
		},
	}

	objs := []runtime.Object{cluster.DeepCopy(), pod}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client: client,
			Scheme: scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Security: reconcile.SecurityState{
			Settings: secSettings,
		},
		Config: reconcile.ConfigState{ConfigHash: hash},
	}

	state.Security.TLSHash = "old-hash"
	if err := Workloads(context.Background(), state); err != nil {
		t.Fatalf("initial workloads phase failed: %v", err)
	}

	recorder := record.NewFakeRecorder(1)
	state.Dependencies.Recorder = recorder
	state.Security.TLSHash = "new-hash"
	state.Accumulator = &reconcile.RequeueAccumulator{}

	if err := Workloads(context.Background(), state); err != nil {
		t.Fatalf("workloads phase failed: %v", err)
	}

	select {
	case evt := <-recorder.Events:
		if !strings.Contains(evt, "TLSSecretRotated") {
			t.Fatalf("expected TLSSecretRotated event, got %q", evt)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected TLS secret rotation event")
	}
}

func TestWorkloadsRedisPodsMissingSchedulesRequeue(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 2,
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster.DeepCopy()).Build()

	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   client,
			Recorder: record.NewFakeRecorder(10),
			Scheme:   scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
		Security:    reconcile.SecurityState{},
		Config:      reconcile.ConfigState{ConfigHash: "cfg-hash"},
	}

	called := false
	state.ApplyBackoff = func(base time.Duration) time.Duration {
		called = true
		return base + time.Second
	}

	if err := Workloads(context.Background(), state); err != nil {
		t.Fatalf("workloads phase failed: %v", err)
	}
	if !called {
		t.Fatalf("expected applyBackoff to be used when pods missing")
	}
	want := 3 * time.Second
	if got := state.Accumulator.Result(); got != want {
		t.Fatalf("expected requeue %s, got %s", want, got)
	}
	if len(state.RedisPods) != 0 {
		t.Fatalf("expected redis pods empty, got %d", len(state.RedisPods))
	}
}

// newScheme and newLogger are defined in phases_test.go and available to all tests in this package.
