package replication

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

// waitClient simulates a Redis client with controllable replication info responses.
type waitClient struct {
	role           string
	infoSequence   []ReplicationInfo
	infoIndex      int
	replicaOfCalls int
	noOneCalls     int
	resetSentinel  int
}

func (c *waitClient) Role(ctx context.Context) (string, error) { return c.role, nil }
func (c *waitClient) ReplicationInfo(ctx context.Context) (ReplicationInfo, error) {
	if len(c.infoSequence) == 0 {
		return ReplicationInfo{Role: c.role}, nil
	}
	if c.infoIndex >= len(c.infoSequence) {
		return c.infoSequence[len(c.infoSequence)-1], nil
	}
	info := c.infoSequence[c.infoIndex]
	c.infoIndex++
	return info, nil
}
func (c *waitClient) ReplicaOf(ctx context.Context, host string, port int) error {
	c.replicaOfCalls++
	c.role = "replica"
	return nil
}
func (c *waitClient) NoOne(ctx context.Context) error {
	c.noOneCalls++
	c.role = "master"
	return nil
}
func (c *waitClient) ResetSentinel(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster) error {
	c.resetSentinel++
	return nil
}
func (c *waitClient) ConfigGet(context.Context, string) (string, bool, error) { return "", false, nil }
func (c *waitClient) ConfigSet(context.Context, string, string) error         { return nil }
func (c *waitClient) ConfigRewrite(context.Context) error                     { return nil }
func (c *waitClient) Auth(context.Context, string, string) error              { return nil }
func (c *waitClient) DBSize(context.Context) (int64, error)                   { return 0, nil }

type waitFactory struct {
	clients map[string]*waitClient
}

func (f *waitFactory) ForPod(ctx context.Context, pod corev1.Pod, _ ClientOptions) (Client, error) {
	return f.clients[pod.Name], nil
}

func podWithIP(name, ns, ip string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Status: corev1.PodStatus{PodIP: ip}}
}

func makeCluster(mode keyvalv1alpha1.Mode, ns, name string) *keyvalv1alpha1.KeyValCluster {
	var replicas int32 = 2
	if mode == keyvalv1alpha1.ModeStandalone {
		replicas = 1
	}
	return &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: mode, Image: "valkey:7", RedisReplicas: replicas}}
}

func TestEnsureTopologyWaitsForReplicaSync(t *testing.T) {
	t.Parallel()
	cluster := makeCluster(keyvalv1alpha1.ModeSentinel, "default", "demo")
	pods := []corev1.Pod{
		podWithIP("demo-0", "default", "10.0.0.1"),
		podWithIP("demo-1", "default", "10.0.0.2"),
	}
	masterClient := &waitClient{role: "master", infoSequence: []ReplicationInfo{{Role: "master"}}}
	replicaClient := &waitClient{role: "master", infoSequence: []ReplicationInfo{
		{Role: "master"},
		{Role: "replica", MasterHost: "10.0.0.1", MasterLinkStatus: "down"},
		{Role: "replica", MasterHost: "10.0.0.1", MasterLinkStatus: "up"},
	}}
	factory := &waitFactory{clients: map[string]*waitClient{
		"demo-0": masterClient,
		"demo-1": replicaClient,
	}}

	res, err := EnsureTopology(context.Background(), EnsureRequest{
		Cluster:      cluster,
		RedisPods:    pods,
		Factory:      factory,
		WaitTimeout:  200 * time.Millisecond,
		WaitInterval: 10 * time.Millisecond,
		Security:     nil,
	})
	if err != nil {
		t.Fatalf("ensure topology failed: %v", err)
	}
	if res.Master != "demo-0" {
		t.Fatalf("expected master demo-0, got %s", res.Master)
	}
	if res.Roles["demo-1"] != keyvalv1alpha1.PodRoleReplica {
		t.Fatalf("replica role unexpected: %+v", res.Roles)
	}
	if res.Infos["demo-1"].MasterLinkStatus != "up" {
		t.Fatalf("expected link status up, got %+v", res.Infos["demo-1"])
	}
	if replicaClient.replicaOfCalls == 0 {
		t.Fatalf("ReplicaOf should have been invoked")
	}
	if len(res.Pending) != 0 {
		t.Fatalf("no pending replicas expected, got %v", res.Pending)
	}
}

func TestEnsureTopologyMarksPendingOnTimeout(t *testing.T) {
	t.Parallel()
	cluster := makeCluster(keyvalv1alpha1.ModeSentinel, "default", "demo")
	pods := []corev1.Pod{
		podWithIP("demo-0", "default", "10.0.0.1"),
		podWithIP("demo-1", "default", "10.0.0.2"),
	}
	masterClient := &waitClient{role: "master", infoSequence: []ReplicationInfo{{Role: "master"}}}
	replicaClient := &waitClient{role: "master", infoSequence: []ReplicationInfo{
		{Role: "master"},
		{Role: "replica", MasterHost: "10.0.0.1", MasterLinkStatus: "down"},
		{Role: "replica", MasterHost: "10.0.0.1", MasterLinkStatus: "down"},
	}}
	factory := &waitFactory{clients: map[string]*waitClient{
		"demo-0": masterClient,
		"demo-1": replicaClient,
	}}

	res, err := EnsureTopology(context.Background(), EnsureRequest{
		Cluster:      cluster,
		RedisPods:    pods,
		Factory:      factory,
		WaitTimeout:  50 * time.Millisecond,
		WaitInterval: 10 * time.Millisecond,
		Security:     nil,
	})
	if err != nil {
		t.Fatalf("ensure topology failed: %v", err)
	}
	if len(res.Pending) != 1 || res.Pending[0] != "demo-1" {
		t.Fatalf("expected pending demo-1, got %v", res.Pending)
	}
}

type fakeSentinelFactory struct {
	host string
	port int
}

type fakeSentinelClient struct {
	host string
	port int
}

func (f *fakeSentinelFactory) ForPod(context.Context, corev1.Pod, SentinelOptions) (SentinelClient, error) {
	return &fakeSentinelClient{host: f.host, port: f.port}, nil
}

func (c *fakeSentinelClient) GetMasterAddrByName(context.Context, string) (string, int, error) {
	return c.host, c.port, nil
}
func (c *fakeSentinelClient) Failover(context.Context, string) error            { return nil }
func (c *fakeSentinelClient) Set(context.Context, string, string, string) error { return nil }
func (c *fakeSentinelClient) Master(context.Context, string) (map[string]string, error) {
	return map[string]string{"name": c.host}, nil
}
func (c *fakeSentinelClient) CheckQuorum(context.Context, string) (bool, error) { return true, nil }
func (c *fakeSentinelClient) Reset(context.Context, string) error               { return nil }

type errorSentinelFactory struct {
	err error
}

type errorSentinelClient struct {
	err error
}

func (f *errorSentinelFactory) ForPod(context.Context, corev1.Pod, SentinelOptions) (SentinelClient, error) {
	return &errorSentinelClient{err: f.err}, nil
}

func (c *errorSentinelClient) GetMasterAddrByName(context.Context, string) (string, int, error) {
	return "", 0, c.err
}

func (c *errorSentinelClient) Failover(context.Context, string) error            { return nil }
func (c *errorSentinelClient) Set(context.Context, string, string, string) error { return nil }
func (c *errorSentinelClient) Master(context.Context, string) (map[string]string, error) {
	return map[string]string{}, nil
}
func (c *errorSentinelClient) CheckQuorum(context.Context, string) (bool, error) { return false, nil }
func (c *errorSentinelClient) Reset(context.Context, string) error               { return nil }

func TestEnsureTopologyUsesSentinelMaster(t *testing.T) {
	t.Parallel()
	cluster := makeCluster(keyvalv1alpha1.ModeSentinel, "default", "demo")
	pods := []corev1.Pod{
		podWithIP("demo-0", "default", "10.0.0.1"),
		podWithIP("demo-1", "default", "10.0.0.2"),
	}
	masterClient := &waitClient{role: "master", infoSequence: []ReplicationInfo{{Role: "master"}}}
	replicaClient := &waitClient{role: "master", infoSequence: []ReplicationInfo{{Role: "master"}}}
	factory := &waitFactory{clients: map[string]*waitClient{
		"demo-0": masterClient,
		"demo-1": replicaClient,
	}}
	sentinelPods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-sentinel-0", Namespace: "default"},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}}
	sentinelFactory := &fakeSentinelFactory{host: "demo-0.demo-headless.default.svc", port: 6379}

	res, err := EnsureTopology(context.Background(), EnsureRequest{
		Cluster:         cluster,
		RedisPods:       pods,
		SentinelPods:    sentinelPods,
		Factory:         factory,
		SentinelFactory: sentinelFactory,
		WaitTimeout:     50 * time.Millisecond,
		WaitInterval:    10 * time.Millisecond,
		Security:        nil,
	})
	if err != nil {
		t.Fatalf("ensure topology failed: %v", err)
	}
	if res.Master != "demo-0" {
		t.Fatalf("expected sentinel-selected master demo-0, got %s", res.Master)
	}
	if res.Source != EnsureSourceSentinel {
		t.Fatalf("expected sentinel source, got %s", res.Source)
	}
	if res.Reason != "demo-0.demo-headless.default.svc:6379" {
		t.Fatalf("unexpected sentinel reason: %s", res.Reason)
	}
	if masterClient.role != "master" {
		t.Fatalf("master should remain master, got role %s", masterClient.role)
	}
}

func TestEnsureTopologyFallbackFromSentinelAddsReason(t *testing.T) {
	t.Parallel()
	cluster := makeCluster(keyvalv1alpha1.ModeSentinel, "default", "demo")
	pods := []corev1.Pod{
		podWithIP("demo-0", "default", "10.0.0.1"),
		podWithIP("demo-1", "default", "10.0.0.2"),
	}
	masterClient := &waitClient{role: "master", infoSequence: []ReplicationInfo{{Role: "master"}}}
	replicaClient := &waitClient{role: "replica", infoSequence: []ReplicationInfo{{Role: "replica", MasterHost: "demo-0", MasterLinkStatus: "up"}}}
	factory := &waitFactory{clients: map[string]*waitClient{
		"demo-0": masterClient,
		"demo-1": replicaClient,
	}}
	sentinelPods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-sentinel-0", Namespace: "default"},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}}
	errSentinel := &errorSentinelFactory{err: errors.New("sentinel timeout")}

	res, err := EnsureTopology(context.Background(), EnsureRequest{
		Cluster:         cluster,
		RedisPods:       pods,
		SentinelPods:    sentinelPods,
		Factory:         factory,
		SentinelFactory: errSentinel,
		WaitTimeout:     50 * time.Millisecond,
		WaitInterval:    5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("ensure topology fallback failed: %v", err)
	}
	if res.Source != EnsureSourceProbe {
		t.Fatalf("expected probe source when sentinel fails, got %s", res.Source)
	}
	if res.Reason == "" {
		t.Fatalf("expected fallback reason to be populated")
	}
	if !strings.Contains(res.Reason, "sentinel timeout") {
		t.Fatalf("expected reason to mention sentinel timeout, got %s", res.Reason)
	}
}

func TestEnsureTopologyIdleReplicaDoesNotReissueReplicaOf(t *testing.T) {
	t.Parallel()
	cluster := makeCluster(keyvalv1alpha1.ModeSentinel, "default", "demo")
	pods := []corev1.Pod{
		podWithIP("demo-0", "default", "10.0.0.1"),
		podWithIP("demo-1", "default", "10.0.0.2"),
	}
	masterClient := &waitClient{role: "master", infoSequence: []ReplicationInfo{{Role: "master"}}}
	replicaClient := &waitClient{role: "replica", infoSequence: []ReplicationInfo{
		{Role: "replica", MasterHost: "10.0.0.1", MasterPort: 6379, MasterLinkStatus: "up", MasterLastIOSecondsAgo: 120},
	}}
	factory := &waitFactory{clients: map[string]*waitClient{
		"demo-0": masterClient,
		"demo-1": replicaClient,
	}}

	res, err := EnsureTopology(context.Background(), EnsureRequest{
		Cluster:      cluster,
		RedisPods:    pods,
		Factory:      factory,
		WaitTimeout:  100 * time.Millisecond,
		WaitInterval: 10 * time.Millisecond,
		Security:     nil,
	})
	if err != nil {
		t.Fatalf("ensure topology failed: %v", err)
	}
	if res.Changed {
		t.Fatalf("expected no topology change, got %+v", res)
	}
	if replicaClient.replicaOfCalls != 0 {
		t.Fatalf("ReplicaOf should not be invoked for idle replica, got %d", replicaClient.replicaOfCalls)
	}
	if len(res.Drift) != 0 {
		t.Fatalf("expected no drift, got %v", res.Drift)
	}
}

func TestEnsureTopologyAttachedReplicaDoesNotReissueReplicaOfWhileSyncing(t *testing.T) {
	t.Parallel()
	cluster := makeCluster(keyvalv1alpha1.ModeSentinel, "default", "demo")
	pods := []corev1.Pod{
		podWithIP("demo-0", "default", "10.0.0.1"),
		podWithIP("demo-1", "default", "10.0.0.2"),
	}
	masterClient := &waitClient{role: "master", infoSequence: []ReplicationInfo{{Role: "master"}}}
	replicaClient := &waitClient{role: "replica", infoSequence: []ReplicationInfo{
		{Role: "replica", MasterHost: "10.0.0.1", MasterPort: 6379, MasterLinkStatus: "down"},
	}}
	factory := &waitFactory{clients: map[string]*waitClient{
		"demo-0": masterClient,
		"demo-1": replicaClient,
	}}

	res, err := EnsureTopology(context.Background(), EnsureRequest{
		Cluster:      cluster,
		RedisPods:    pods,
		Factory:      factory,
		WaitTimeout:  50 * time.Millisecond,
		WaitInterval: 10 * time.Millisecond,
		Security:     nil,
	})
	if err != nil {
		t.Fatalf("ensure topology failed: %v", err)
	}
	if res.Changed {
		t.Fatalf("expected no topology change, got %+v", res)
	}
	if replicaClient.replicaOfCalls != 0 {
		t.Fatalf("ReplicaOf should not be reissued when replica already attached, got %d", replicaClient.replicaOfCalls)
	}
	if len(res.Pending) != 1 || res.Pending[0] != "demo-1" {
		t.Fatalf("expected pending demo-1 while syncing, got %v", res.Pending)
	}
}

func TestSanitizeEnsureReasonTrimsNoise(t *testing.T) {
	input := "  sentinel error\ncontext deadline exceeded  "
	got := sanitizeEnsureReason(input)
	if strings.Contains(got, "\n") {
		t.Fatalf("expected newline to be removed, got %q", got)
	}
	if !strings.Contains(got, "context deadline exceeded") {
		t.Fatalf("expected sanitized reason to retain message, got %q", got)
	}
	if len([]rune(got)) > 200 {
		t.Fatalf("expected sanitized reason to be <=200 runes, got %d", len([]rune(got)))
	}
}
