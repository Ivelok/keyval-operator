package reconcile

import (
	"context"
	"fmt"
	"testing"
	"time"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	"github.com/ivelok/keyval-operator/controllers/logging"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type traceKey struct{}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return scheme
}

func newCluster(name, ns string) *keyvalv1alpha1.KeyValCluster {
	return &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	}
}

func TestPipelineRunSuccessWithRequeue(t *testing.T) {
	scheme := newScheme(t)
	cluster := newCluster("demo", "default")
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster.DeepCopy()).Build()

	var (
		resetCalled      bool
		queueDepthCalled bool
	)

	deps := Dependencies{
		Client:         client,
		APIReader:      client,
		BaseLogger:     logging.New(nil),
		ControllerName: "",
		QueueDepth: func() int {
			queueDepthCalled = true
			return 7
		},
		ResetBackoff: func(string) {
			resetCalled = true
		},
	}

	handler := HandlerFunc(func(ctx context.Context, state *State) (ctrl.Result, error) {
		if state.Cluster == nil {
			t.Fatalf("expected cluster populated")
		}
		if state.Cluster.Name != "demo" || state.Cluster.Namespace != "default" {
			t.Fatalf("unexpected cluster identity: %+v", state.Cluster.ObjectMeta)
		}
		if state.ResourceKey != "default/demo" {
			t.Fatalf("unexpected resource key: %s", state.ResourceKey)
		}
		state.Cluster.Spec.RedisReplicas = 3
		return ctrl.Result{RequeueAfter: time.Second}, nil
	})

	pipeline := NewPipeline(deps, handler)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}
	res, err := pipeline.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}
	if res.RequeueAfter != time.Second {
		t.Fatalf("expected requeue after 1s, got %s", res.RequeueAfter)
	}
	if resetCalled {
		t.Fatalf("resetBackoff should not be called when requeue is scheduled")
	}
	if !queueDepthCalled {
		t.Fatalf("expected queue depth callback invoked")
	}
}

func TestPipelineRunResetsBackoffOnCompletion(t *testing.T) {
	scheme := newScheme(t)
	cluster := newCluster("demo", "default")
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster.DeepCopy()).Build()

	var resetCalled bool

	deps := Dependencies{
		Client:     client,
		APIReader:  client,
		BaseLogger: logging.New(nil),
		ResetBackoff: func(string) {
			resetCalled = true
		},
	}

	handler := HandlerFunc(func(ctx context.Context, state *State) (ctrl.Result, error) {
		return ctrl.Result{}, nil
	})

	pipeline := NewPipeline(deps, handler)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}
	if _, err := pipeline.Run(context.Background(), req); err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}
	if !resetCalled {
		t.Fatalf("expected resetBackoff to be invoked")
	}
}

func TestPipelinePropagatesFatalError(t *testing.T) {
	scheme := newScheme(t)
	cluster := newCluster("demo", "default")
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster.DeepCopy()).Build()

	var resetCalled bool

	deps := Dependencies{
		Client:     client,
		APIReader:  client,
		BaseLogger: logging.New(nil),
		ResetBackoff: func(string) {
			resetCalled = true
		},
	}

	runErr := controllererrors.WrapFatal(fmt.Errorf("boom"))
	handler := HandlerFunc(func(ctx context.Context, state *State) (ctrl.Result, error) {
		return ctrl.Result{}, runErr
	})

	pipeline := NewPipeline(deps, handler)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}
	_, err := pipeline.Run(context.Background(), req)
	if err == nil {
		t.Fatalf("expected fatal error")
	}
	if !controllererrors.IsFatal(err) {
		t.Fatalf("expected fatal error classification, got %v", err)
	}
	if !resetCalled {
		t.Fatalf("expected resetBackoff for fatal errors")
	}
}

func TestPipelineSkipsHandlerWhenClusterMissing(t *testing.T) {
	scheme := newScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	deps := Dependencies{
		Client:       client,
		APIReader:    client,
		BaseLogger:   logging.New(nil),
		ResetBackoff: func(string) {},
	}

	handlerCalled := false
	handler := HandlerFunc(func(ctx context.Context, state *State) (ctrl.Result, error) {
		handlerCalled = true
		return ctrl.Result{}, nil
	})

	pipeline := NewPipeline(deps, handler)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "missing"}}
	res, err := pipeline.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("expected zero result for not found, got %+v", res)
	}
	if handlerCalled {
		t.Fatalf("handler should not be invoked for missing resources")
	}
}

func TestStateRequeueHelpers(t *testing.T) {
	state := &State{}
	if got := state.NextRequeue(); got != 0 {
		t.Fatalf("expected zero delay initially, got %s", got)
	}
	state.RequeueAfter(5 * time.Second)
	if got := state.NextRequeue(); got != 5*time.Second {
		t.Fatalf("expected 5s, got %s", got)
	}
	state.RequeueAfter(2 * time.Second)
	if got := state.NextRequeue(); got != 2*time.Second {
		t.Fatalf("expected 2s after shrink, got %s", got)
	}
	state.RequeueMin(10 * time.Second)
	if got := state.NextRequeue(); got != 10*time.Second {
		t.Fatalf("expected min raised to 10s, got %s", got)
	}
	state.RequeueAfter(1 * time.Second)
	if got := state.NextRequeue(); got != 10*time.Second {
		t.Fatalf("min should clamp to 10s, got %s", got)
	}
}

func TestStateEnsureConditionOverrides(t *testing.T) {
	state := &State{}
	first := state.EnsureConditionOverrides()
	if first == nil {
		t.Fatalf("expected non-nil overrides map")
	}
	first[keyvalv1alpha1.ConditionReconciled] = &opstatus.ConditionState{Reason: "foo"}
	second := state.EnsureConditionOverrides()
	if len(second) != 1 {
		t.Fatalf("expected overrides map to be preserved: %+v", second)
	}
	if state.ConditionOverrides == nil {
		t.Fatalf("state should keep overrides reference")
	}
}

func TestStateStartTrace(t *testing.T) {
	var (
		called bool
		closed bool
	)
	hook := func(ctx context.Context, name string) (context.Context, func()) {
		called = true
		if name != "phase" {
			t.Fatalf("unexpected phase name: %s", name)
		}
		return context.WithValue(ctx, traceKey{}, true), func() { closed = true }
	}
	state := &State{Dependencies: Dependencies{TraceHook: hook}}
	ctx, done := state.StartTrace(context.Background(), "phase")
	if !called {
		t.Fatalf("expected trace hook to fire")
	}
	if ctx.Value(traceKey{}) == nil {
		t.Fatalf("expected context to be decorated")
	}
	done()
	if !closed {
		t.Fatalf("expected closer invoked")
	}
	ctx, done = state.StartTrace(context.Background(), "phase")
	if ctx == nil || done == nil {
		t.Fatalf("expected non-nil results on subsequent calls")
	}
}
