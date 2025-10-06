package controllers

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
)

type fakeClient struct {
	name          string
	role          string // "master" or "replica"
	noOneCount    int
	replicaOfReqs []string // host:port
	resetCount    int
	masterHost    string
	masterPort    int
	linkStatus    string
}

func (f *fakeClient) Role(ctx context.Context) (string, error) { return f.role, nil }
func (f *fakeClient) ReplicationInfo(ctx context.Context) (ReplicationInfo, error) {
	return ReplicationInfo{Role: f.role, MasterHost: f.masterHost, MasterPort: f.masterPort, MasterLinkStatus: f.linkStatus}, nil
}
func (f *fakeClient) ReplicaOf(ctx context.Context, host string, port int) error {
	f.replicaOfReqs = append(f.replicaOfReqs, host+":"+itoa(port))
	f.role = "replica"
	f.masterHost = host
	f.masterPort = port
	f.linkStatus = "up"
	return nil
}
func (f *fakeClient) NoOne(ctx context.Context) error {
	f.noOneCount++
	f.role = "master"
	f.masterHost = ""
	f.masterPort = 0
	f.linkStatus = ""
	return nil
}
func (f *fakeClient) ResetSentinel(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster) error {
	f.resetCount++
	return nil
}
func (f *fakeClient) ConfigGet(context.Context, string) (string, bool, error) { return "", false, nil }
func (f *fakeClient) ConfigSet(context.Context, string, string) error         { return nil }
func (f *fakeClient) ConfigRewrite(context.Context) error                     { return nil }
func (f *fakeClient) Auth(context.Context, string, string) error              { return nil }
func (f *fakeClient) DBSize(context.Context) (int64, error)                   { return 0, nil }

type fakeFactory struct{ m map[string]*fakeClient }

func (ff *fakeFactory) ForPod(ctx context.Context, pod corev1.Pod, _ ClientOptions) (Client, error) {
	return ff.m[pod.Name], nil
}

func itoa(n int) string { return fmtInt(n) }
func fmtInt(n int) string {
	// simple int to string without importing strconv for minimalism in test
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func pod(name, ip string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Status: corev1.PodStatus{PodIP: ip}}
}

func newCluster(mode keyvalv1alpha1.Mode) *keyvalv1alpha1.KeyValCluster {
	var replicas int32 = 1
	var sc *int32
	if mode == keyvalv1alpha1.ModeSentinel {
		replicas = 3
		v := int32(3)
		sc = &v
	}
	return &keyvalv1alpha1.KeyValCluster{Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: mode, Image: "valkey/valkey:7.2", RedisReplicas: replicas, SentinelCount: sc}}
}

func TestEnsureReplication_NoChange_SingleMaster(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{pod("demo-0", "10.0.0.1"), pod("demo-1", "10.0.0.2")}
	ff := &fakeFactory{m: map[string]*fakeClient{
		"demo-0": {name: "demo-0", role: "master"},
		"demo-1": {name: "demo-1", role: "replica", masterHost: "10.0.0.1", masterPort: 6379, linkStatus: "up"},
	}}
	changed, master, roles, infos, err := opreplication.EnsureReplication(context.Background(), newCluster(keyvalv1alpha1.ModeStandalone), pods, ff, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if changed {
		t.Fatalf("expected no changes")
	}
	if master != "demo-0" {
		t.Fatalf("expected master demo-0, got %s", master)
	}
	if roles["demo-0"] != keyvalv1alpha1.PodRoleMaster || roles["demo-1"] != keyvalv1alpha1.PodRoleReplica {
		t.Fatalf("unexpected roles: %+v", roles)
	}
	if infos["demo-1"].MasterLinkStatus != "up" {
		t.Fatalf("expected replication info captured, got %v", infos["demo-1"])
	}
}

func TestEnsureReplicationToMaster_SkipReplicaOf_WhenAlreadyAttached(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{pod("demo-0", "10.0.0.1"), pod("demo-1", "10.0.0.2")}
	f0 := &fakeClient{name: "demo-0", role: "master"}
	f1 := &fakeClient{name: "demo-1", role: "replica", masterHost: "10.0.0.1", masterPort: 6379, linkStatus: "up"}
	ff := &fakeFactory{m: map[string]*fakeClient{"demo-0": f0, "demo-1": f1}}
	changed, roles, infos, err := opreplication.EnsureReplicationToMaster(context.Background(), newCluster(keyvalv1alpha1.ModeStandalone), pods, ff, "demo-0", nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if changed {
		t.Fatalf("expected no changes when already attached")
	}
	if roles["demo-1"] != keyvalv1alpha1.PodRoleReplica {
		t.Fatalf("demo-1 should be replica")
	}
	if _, ok := infos["demo-0"]; !ok {
		t.Fatalf("expected master info present")
	}
}

func TestEnsureReplicationToMaster_FallbacksToDNSWhenNoIP(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{pod("demo-0", ""), pod("demo-1", ""), pod("demo-2", "")}
	f0 := &fakeClient{name: "demo-0", role: "replica"}
	f1 := &fakeClient{name: "demo-1", role: "replica"}
	f2 := &fakeClient{name: "demo-2", role: "master"}
	ff := &fakeFactory{m: map[string]*fakeClient{"demo-0": f0, "demo-1": f1, "demo-2": f2}}
	cr := newCluster(keyvalv1alpha1.ModeStandalone)
	cr.Name = "demo"
	cr.Namespace = "default"
	changed, roles, infos, err := opreplication.EnsureReplicationToMaster(context.Background(), cr, pods, ff, "demo-2", nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !changed {
		t.Fatalf("expected changes when attaching replicas via DNS fallback")
	}
	expHost := "demo-2.demo-headless.default.svc:6379"
	if len(f0.replicaOfReqs) == 0 || f0.replicaOfReqs[0] != expHost {
		t.Fatalf("demo-0 should replicate from %s, got %v", expHost, f0.replicaOfReqs)
	}
	if len(f1.replicaOfReqs) == 0 || f1.replicaOfReqs[0] != expHost {
		t.Fatalf("demo-1 should replicate from %s, got %v", expHost, f1.replicaOfReqs)
	}
	if roles["demo-2"] != keyvalv1alpha1.PodRoleMaster {
		t.Fatalf("demo-2 must remain master")
	}
	if infos["demo-2"].Role != "master" {
		t.Fatalf("expected master info recorded, got %v", infos["demo-2"])
	}
}

func TestEnsureReplication_TwoMasters_DemotesOne(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{pod("demo-0", "10.0.0.1"), pod("demo-1", "10.0.0.2")}
	f0 := &fakeClient{name: "demo-0", role: "master"}
	f1 := &fakeClient{name: "demo-1", role: "master"}
	ff := &fakeFactory{m: map[string]*fakeClient{"demo-0": f0, "demo-1": f1}}
	changed, master, roles, _, err := opreplication.EnsureReplication(context.Background(), newCluster(keyvalv1alpha1.ModeStandalone), pods, ff, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !changed {
		t.Fatalf("expected changes")
	}
	if master != "demo-0" {
		t.Fatalf("expected master demo-0, got %s", master)
	}
	if len(f1.replicaOfReqs) != 1 || f1.replicaOfReqs[0] != "10.0.0.1:6379" {
		t.Fatalf("expected demo-1 to replicate from 10.0.0.1, got %v", f1.replicaOfReqs)
	}
	if roles["demo-1"] != keyvalv1alpha1.PodRoleReplica {
		t.Fatalf("demo-1 should be replica")
	}
}

func TestEnsureReplication_NoMaster_PromotesFirst(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{pod("demo-0", "10.0.0.1"), pod("demo-1", "10.0.0.2")}
	f0 := &fakeClient{name: "demo-0", role: "replica"}
	f1 := &fakeClient{name: "demo-1", role: "replica"}
	ff := &fakeFactory{m: map[string]*fakeClient{"demo-0": f0, "demo-1": f1}}
	changed, master, roles, _, err := opreplication.EnsureReplication(context.Background(), newCluster(keyvalv1alpha1.ModeStandalone), pods, ff, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !changed {
		t.Fatalf("expected changes")
	}
	if master != "demo-0" {
		t.Fatalf("expected master demo-0, got %s", master)
	}
	if f0.noOneCount == 0 {
		t.Fatalf("expected promotion on demo-0")
	}
	if len(f1.replicaOfReqs) != 1 || f1.replicaOfReqs[0] != "10.0.0.1:6379" {
		t.Fatalf("expected demo-1 to replicate from 10.0.0.1, got %v", f1.replicaOfReqs)
	}
	if roles["demo-0"] != keyvalv1alpha1.PodRoleMaster || roles["demo-1"] != keyvalv1alpha1.PodRoleReplica {
		t.Fatalf("unexpected roles %+v", roles)
	}
}

func TestEnsureReplication_SentinelMode_ResetsOnChange(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{pod("demo-0", "10.0.0.1"), pod("demo-1", "10.0.0.2"), pod("demo-2", "10.0.0.3")}
	f0 := &fakeClient{name: "demo-0", role: "replica"}
	f1 := &fakeClient{name: "demo-1", role: "replica"}
	f2 := &fakeClient{name: "demo-2", role: "replica"}
	ff := &fakeFactory{m: map[string]*fakeClient{"demo-0": f0, "demo-1": f1, "demo-2": f2}}
	changed, _, _, _, err := opreplication.EnsureReplication(context.Background(), newCluster(keyvalv1alpha1.ModeSentinel), pods, ff, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !changed {
		t.Fatalf("expected changes")
	}
	if got := f0.resetCount + f1.resetCount + f2.resetCount; got != 0 {
		t.Fatalf("expected sentinel resets to be deferred, got %d", got)
	}
}
