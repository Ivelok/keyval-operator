package controllers

import (
	"context"
	"testing"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
	corev1 "k8s.io/api/core/v1"
)

// flakyClient fails ReplicaOf N times before succeeding.
type flakyClient struct {
	role         string
	failReplica  int
	replicaCalls int
	noOneCalls   int
}

func (f *flakyClient) Role(ctx context.Context) (string, error) { return f.role, nil }
func (f *flakyClient) ReplicationInfo(ctx context.Context) (ReplicationInfo, error) {
	return ReplicationInfo{}, assertError("info unavailable")
}
func (f *flakyClient) ReplicaOf(ctx context.Context, host string, port int) error {
	f.replicaCalls++
	if f.failReplica > 0 {
		f.failReplica--
		return assertError("replica transient")
	}
	f.role = "replica"
	return nil
}
func (f *flakyClient) NoOne(ctx context.Context) error { f.noOneCalls++; f.role = "master"; return nil }
func (f *flakyClient) ResetSentinel(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster) error {
	return nil
}
func (f *flakyClient) ConfigGet(context.Context, string) (string, bool, error) { return "", false, nil }
func (f *flakyClient) ConfigSet(context.Context, string, string) error         { return nil }
func (f *flakyClient) ConfigRewrite(context.Context) error                     { return nil }
func (f *flakyClient) Auth(context.Context, string, string) error              { return nil }

type flakyFactory struct{ m map[string]*flakyClient }

func (ff *flakyFactory) ForPod(ctx context.Context, pod corev1.Pod, _ ClientOptions) (Client, error) {
	return ff.m[pod.Name], nil
}

func TestEnsureReplication_RetryReplicaOf(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{pod("demo-0", "10.0.0.1"), pod("demo-1", "10.0.0.2")}
	f0 := &flakyClient{role: "master"}
	f1 := &flakyClient{role: "master", failReplica: 2}
	ff := &flakyFactory{m: map[string]*flakyClient{"demo-0": f0, "demo-1": f1}}
	_, master, roles, _, err := opreplication.EnsureReplication(context.Background(), newCluster(keyvalv1alpha1.ModeStandalone), pods, ff, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if master != "demo-0" {
		t.Fatalf("master should be demo-0, got %s", master)
	}
	if roles["demo-1"] != keyvalv1alpha1.PodRoleReplica {
		t.Fatalf("demo-1 should be replica")
	}
	if f1.replicaCalls < 3 {
		t.Fatalf("expected at least 3 ReplicaOf calls due to retries, got %d", f1.replicaCalls)
	}
}
