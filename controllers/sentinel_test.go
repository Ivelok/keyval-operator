package controllers

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	opsentinel "github.com/ivelok/keyval-operator/controllers/internal/ops/sentinel"
)

type fakeSentinel struct {
	host           string
	port           int
	failoverCalled bool
	nogood         bool
	failErr        error
	failErrCount   int
	failoverCalls  int
	info           map[string]string
}

func (f *fakeSentinel) GetMasterAddrByName(ctx context.Context, name string) (string, int, error) {
	// After failover, return configured host/port
	if f.failoverCalled {
		return f.host, f.port, nil
	}
	// before failover, return empty to force polling
	return "", 0, nil
}
func (f *fakeSentinel) Failover(ctx context.Context, name string) error {
	f.failoverCalls++
	if f.failErr != nil && f.failErrCount != 0 {
		if f.failErrCount > 0 {
			f.failErrCount--
		}
		return f.failErr
	}
	if f.nogood {
		return assertError("NOGOODSLAVE no suitable replica")
	}
	f.failoverCalled = true
	return nil
}

func (f *fakeSentinel) Set(context.Context, string, string, string) error { return nil }

func (f *fakeSentinel) Master(ctx context.Context, name string) (map[string]string, error) {
	if f.info != nil {
		return f.info, nil
	}
	return map[string]string{
		"down-after-milliseconds": "5000",
		"failover-timeout":        "60000",
		"parallel-syncs":          "1",
	}, nil
}

func (f *fakeSentinel) CheckQuorum(context.Context, string) (bool, error) { return true, nil }

func (f *fakeSentinel) Reset(context.Context, string) error { return nil }

type fakeSentinelFactory struct {
	defaultCli *fakeSentinel
	byPod      map[string]*fakeSentinel
}

func (ff *fakeSentinelFactory) ForPod(ctx context.Context, pod corev1.Pod, _ SentinelOptions) (SentinelClient, error) {
	if ff.byPod != nil {
		if cli, ok := ff.byPod[pod.Name]; ok {
			return cli, nil
		}
	}
	if ff.defaultCli != nil {
		return ff.defaultCli, nil
	}
	return nil, assertError("no sentinel client for pod " + pod.Name)
}

func TestTriggerFailover_SelectsNewMasterAndEnforcesReplication(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default"}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "default"}, Status: corev1.PodStatus{PodIP: "10.0.0.2"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-2", Namespace: "default"}, Status: corev1.PodStatus{PodIP: "10.0.0.3"}},
	}
	// fake redis clients: start with demo-0 as master
	f0 := &fakeClient{name: "demo-0", role: "master"}
	f1 := &fakeClient{name: "demo-1", role: "replica"}
	f2 := &fakeClient{name: "demo-2", role: "replica"}
	rf := &fakeFactory{m: map[string]*fakeClient{"demo-0": f0, "demo-1": f1, "demo-2": f2}}

	// sentinel elects demo-2 as new master via headless DNS name
	sentinelPods := []corev1.Pod{newSentinelPod("demo-sentinel-0", "10.1.0.10", true)}
	sf := &fakeSentinelFactory{defaultCli: &fakeSentinel{host: "demo-2.demo-headless", port: 6379}}

	cr := newCluster(keyvalv1alpha1.ModeSentinel)
	m, roles, err := opsentinel.TriggerFailover(context.TODO(), cr, pods, sentinelPods, sf, rf, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if m != "demo-2" {
		t.Fatalf("expected new master demo-2, got %s", m)
	}
	if f2.noOneCount == 0 {
		t.Fatalf("expected demo-2 to be promoted with NO ONE")
	}
	if len(f0.replicaOfReqs) == 0 || f0.replicaOfReqs[0] != "10.0.0.3:6379" {
		t.Fatalf("expected demo-0 to replicate from 10.0.0.3, got %v", f0.replicaOfReqs)
	}
	if roles["demo-2"] != keyvalv1alpha1.PodRoleMaster {
		t.Fatalf("roles not updated: %+v", roles)
	}
}

func TestTriggerFailover_NoGoodSlaveError(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default"}},
	}
	rf := &fakeFactory{m: map[string]*fakeClient{"demo-0": {name: "demo-0", role: "master"}}}
	sentinelPods := []corev1.Pod{newSentinelPod("demo-sentinel-0", "10.1.0.10", true)}
	sf := &fakeSentinelFactory{defaultCli: &fakeSentinel{nogood: true}}
	cr := newCluster(keyvalv1alpha1.ModeSentinel)
	_, _, err := opsentinel.TriggerFailover(context.TODO(), cr, pods, sentinelPods, sf, rf, nil)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, opsentinel.ErrNoGoodSlave) {
		t.Fatalf("expected ErrNoGoodSlave, got %v", err)
	}
}

func TestTriggerFailover_TriesNextSentinelOnConnectionError(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default"}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "default"}, Status: corev1.PodStatus{PodIP: "10.0.0.2"}},
	}
	f0 := &fakeClient{name: "demo-0", role: "master"}
	f1 := &fakeClient{name: "demo-1", role: "replica"}
	rf := &fakeFactory{m: map[string]*fakeClient{"demo-0": f0, "demo-1": f1}}

	sentinelPods := []corev1.Pod{
		newSentinelPod("demo-sentinel-0", "10.1.0.10", true),
		newSentinelPod("demo-sentinel-1", "10.1.0.11", true),
	}
	s0 := &fakeSentinel{failErr: assertError("dial tcp 10.1.0.10:26379: connect: connection refused"), failErrCount: -1}
	s1 := &fakeSentinel{host: "demo-1.demo-headless", port: 6379}
	sf := &fakeSentinelFactory{byPod: map[string]*fakeSentinel{
		"demo-sentinel-0": s0,
		"demo-sentinel-1": s1,
	}}
	cr := newCluster(keyvalv1alpha1.ModeSentinel)
	m, roles, err := opsentinel.TriggerFailover(context.TODO(), cr, pods, sentinelPods, sf, rf, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m != "demo-1" {
		t.Fatalf("expected master to switch to demo-1, got %s", m)
	}
	if s0.failoverCalls == 0 {
		t.Fatalf("expected first sentinel to be attempted")
	}
	if !s1.failoverCalled {
		t.Fatalf("expected second sentinel failover to succeed")
	}
	if roles["demo-1"] != keyvalv1alpha1.PodRoleMaster {
		t.Fatalf("roles map not updated: %+v", roles)
	}
	if f1.noOneCount == 0 {
		t.Fatalf("expected demo-1 to be promoted")
	}
	if len(f0.replicaOfReqs) == 0 || f0.replicaOfReqs[0] != "10.0.0.2:6379" {
		t.Fatalf("expected demo-0 to point at demo-1, got %v", f0.replicaOfReqs)
	}
}

func newSentinelPod(name, ip string, ready bool) corev1.Pod {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status:     corev1.PodStatus{PodIP: ip},
	}
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	return pod
}
