package runtimeconfig

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	clientspkg "github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func TestApplyRedisRuntime_ApplyAllowlistedKeys(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			RedisReplicas: 1,
			RedisConfig:   map[string]string{"maxmemory": "256mb"},
		},
	}
	sec := &security.Settings{}
	hash := resources.RedisRuntimeHash(cr, sec)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "demo-0",
			Namespace:   "default",
			Annotations: map[string]string{resources.RuntimeHashAnnotationKey: "old"},
			Labels:      map[string]string{"app": "demo-redis"},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(&pod).Build()
	currentCfg := resources.EffectiveRedisConfig(cr, sec)
	currentCfg["maxmemory"] = "134217728"
	factory := &stubRedisFactory{clients: map[string]*stubRedisClient{
		"demo-0": {config: currentCfg},
	}}

	res := ApplyRedisRuntime(context.Background(), fakeClient, factory, cr, []corev1.Pod{pod}, hash, testLogger(t), nil, sec)
	if res.Mode != ModeApplied {
		t.Fatalf("expected ModeApplied, got %v (%s)", res.Mode, res.Message)
	}
	if len(res.ChangedKeys) != 1 || res.ChangedKeys[0] != "maxmemory" {
		t.Fatalf("unexpected changed keys: %v", res.ChangedKeys)
	}
	if factory.clients["demo-0"].setCount != 1 {
		t.Fatalf("expected config set to be invoked")
	}
	var updated corev1.Pod
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if updated.Annotations[resources.RuntimeHashAnnotationKey] != hash {
		t.Fatalf("expected annotation patched to %s", hash)
	}
}

func TestApplyRedisRuntime_NeedsRestartForDenylistedKeys(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{RedisReplicas: 1, RedisConfig: map[string]string{"appendonly": "no"}},
	}
	sec := &security.Settings{}
	hash := resources.RedisRuntimeHash(cr, sec)
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Annotations: map[string]string{resources.RuntimeHashAnnotationKey: "old"}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(&pod).Build()
	currentCfg := resources.EffectiveRedisConfig(cr, sec)
	currentCfg["appendonly"] = "yes"
	factory := &stubRedisFactory{clients: map[string]*stubRedisClient{"demo-0": {config: currentCfg}}}

	res := ApplyRedisRuntime(context.Background(), fakeClient, factory, cr, []corev1.Pod{pod}, hash, testLogger(t), nil, sec)
	if res.Mode != ModeNeedsRestart {
		t.Fatalf("expected ModeNeedsRestart, got %v", res.Mode)
	}
	if res.Err == nil || !errors.Is(res.Err, controllererrors.ErrConfigDrift) {
		t.Fatalf("expected ErrConfigDrift, got %v", res.Err)
	}
	if factory.clients["demo-0"].setCount != 0 {
		t.Fatalf("config set should not run when restart required")
	}
	var unchanged corev1.Pod
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &unchanged)
	if unchanged.Annotations[resources.RuntimeHashAnnotationKey] != "old" {
		t.Fatalf("annotation should remain old when restart required")
	}
}

func TestApplySentinelRuntime_AppliesChanges(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			RedisReplicas: 3,
			SentinelCount: pointer(int32(3)),
			SentinelConfig: map[string]string{
				"down-after-milliseconds": "5000",
			},
		},
	}
	pods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-sentinel-0", Namespace: "default"},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}}
	factory := &stubSentinelFactory{client: &stubSentinelClient{info: map[string]string{
		"down-after-milliseconds": "3000",
		"failover-timeout":        "60000",
		"parallel-syncs":          "1",
	}}}

	runtimeHash := resources.SentinelRuntimeHash(cr, nil)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(&pods[0]).Build()
	res := ApplySentinelRuntime(context.Background(), fakeClient, factory, cr, pods, runtimeHash, testLogger(t), nil, nil)
	if res.Mode != ModeApplied {
		t.Fatalf("expected ModeApplied, got %v (%s)", res.Mode, res.Message)
	}
	if factory.client.setCalls != 1 {
		t.Fatalf("expected sentinel SET to be called once, got %d", factory.client.setCalls)
	}
}

func TestApplySentinelRuntime_SetFailureNeedsRestart(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			RedisReplicas: 3,
			SentinelCount: pointer(int32(3)),
			SentinelConfig: map[string]string{
				"down-after-milliseconds": "5000",
			},
		},
	}
	pods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-sentinel-0", Namespace: "default"},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}}
	factory := &stubSentinelFactory{client: &stubSentinelClient{
		info: map[string]string{
			"down-after-milliseconds": "3000",
			"failover-timeout":        "60000",
			"parallel-syncs":          "1",
		},
		setErr: errors.New("boom"),
	}}

	runtimeHash := resources.SentinelRuntimeHash(cr, nil)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(&pods[0]).Build()
	res := ApplySentinelRuntime(context.Background(), fakeClient, factory, cr, pods, runtimeHash, testLogger(t), nil, nil)
	if res.Mode != ModeNeedsRestart {
		t.Fatalf("expected ModeNeedsRestart, got %v (%s)", res.Mode, res.Message)
	}
	if res.Err == nil || !errors.Is(res.Err, controllererrors.ErrConfigDrift) {
		t.Fatalf("expected ErrConfigDrift, got %v", res.Err)
	}
	if len(res.ChangedKeys) != 1 || res.ChangedKeys[0] != "down-after-milliseconds" {
		t.Fatalf("unexpected changed keys: %v", res.ChangedKeys)
	}
}

// --- stubs ---

type stubRedisFactory struct {
	clients map[string]*stubRedisClient
}

func (f *stubRedisFactory) ForPod(_ context.Context, pod corev1.Pod, _ clientspkg.ClientOptions) (clientspkg.Client, error) {
	cl, ok := f.clients[pod.Name]
	if !ok {
		return nil, fmt.Errorf("no client for %s", pod.Name)
	}
	return cl, nil
}

type stubRedisClient struct {
	config   map[string]string
	setCount int
}

func (c *stubRedisClient) Role(context.Context) (string, error) { return "replica", nil }
func (c *stubRedisClient) ReplicationInfo(context.Context) (clientspkg.ReplicationInfo, error) {
	return clientspkg.ReplicationInfo{}, nil
}
func (c *stubRedisClient) ReplicaOf(context.Context, string, int) error { return nil }
func (c *stubRedisClient) NoOne(context.Context) error                  { return nil }
func (c *stubRedisClient) ResetSentinel(context.Context, *keyvalv1alpha1.KeyValCluster) error {
	return nil
}
func (c *stubRedisClient) ConfigGet(_ context.Context, key string) (string, bool, error) {
	if c.config == nil {
		return "", false, nil
	}
	v, ok := c.config[key]
	return v, ok, nil
}
func (c *stubRedisClient) ConfigSet(_ context.Context, key, value string) error {
	if c.config == nil {
		c.config = map[string]string{}
	}
	c.config[key] = value
	c.setCount++
	return nil
}
func (c *stubRedisClient) ConfigRewrite(context.Context) error        { return nil }
func (c *stubRedisClient) Auth(context.Context, string, string) error { return nil }

func (c *stubRedisClient) Close() error                          { return nil }
func (c *stubRedisClient) DBSize(context.Context) (int64, error) { return 0, nil }

type stubSentinelFactory struct {
	client *stubSentinelClient
}

func (f *stubSentinelFactory) ForPod(context.Context, corev1.Pod, clientspkg.SentinelOptions) (clientspkg.SentinelClient, error) {
	return f.client, nil
}

type stubSentinelClient struct {
	info     map[string]string
	setCalls int
	setErr   error
}

func (s *stubSentinelClient) GetMasterAddrByName(context.Context, string) (string, int, error) {
	return "demo-0", 6379, nil
}
func (s *stubSentinelClient) Failover(context.Context, string) error { return nil }
func (s *stubSentinelClient) Set(context.Context, string, string, string) error {
	s.setCalls++
	return s.setErr
}
func (s *stubSentinelClient) Master(context.Context, string) (map[string]string, error) {
	return s.info, nil
}
func (s *stubSentinelClient) CheckQuorum(context.Context, string) (bool, error) { return true, nil }
func (s *stubSentinelClient) Reset(context.Context, string) error               { return nil }

func pointer[T any](v T) *T { return &v }

func testLogger(t *testing.T) logr.Logger {
	return testr.New(t)
}
