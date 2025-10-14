package importer

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/clients"
)

func TestEnsure_DisabledWhenSpecAbsent(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{}
	res, err := Ensure(context.Background(), Options{Cluster: cr})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Condition.Status != metav1.ConditionTrue || res.Condition.Reason != "Disabled" {
		t.Fatalf("expected disabled condition, got %+v", res.Condition)
	}
	if res.Abort {
		t.Fatalf("expected abort=false")
	}
}

func TestEnsure_WaitsForPodsWhenNoneAvailable(t *testing.T) {
	t.Parallel()
	cr := minimalCluster()
	spec := &keyvalv1alpha1.ExternalSourceSpec{Address: "redis://example:6379"}
	scheme := runtime.NewScheme()
	_ = keyvalv1alpha1.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	res, err := Ensure(context.Background(), Options{
		Cluster:       cr,
		Spec:          spec,
		Pods:          nil,
		ClientFactory: &stubFactory{client: newStubClient()},
		ClientOptions: clients.ClientOptions{},
		KubeClient:    kube,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Condition.Reason != "WaitingForPods" {
		t.Fatalf("expected WaitingForPods, got %s", res.Condition.Reason)
	}
	if !res.Abort {
		t.Fatalf("expected abort to be true")
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected requeue delay")
	}
}

func TestEnsure_AbortIfExistingData(t *testing.T) {
	t.Parallel()
	cr := minimalCluster()
	spec := &keyvalv1alpha1.ExternalSourceSpec{Address: "redis://example:6379"}
	scheme := runtime.NewScheme()
	_ = keyvalv1alpha1.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	client := newStubClient()
	client.dbSize = 5

	res, err := Ensure(context.Background(), Options{
		Cluster:       cr,
		Spec:          spec,
		Pods:          []corev1.Pod{readyPod("demo-0")},
		ClientFactory: &stubFactory{client: client},
		ClientOptions: clients.ClientOptions{},
		KubeClient:    kube,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Status.State != importStateFailed {
		t.Fatalf("expected failed state, got %s", res.Status.State)
	}
	if res.Condition.Reason != "TargetNotEmpty" {
		t.Fatalf("expected TargetNotEmpty reason, got %s", res.Condition.Reason)
	}
}

func TestEnsure_CompletesSnapshot(t *testing.T) {
	t.Parallel()
	cr := minimalCluster()
	spec := &keyvalv1alpha1.ExternalSourceSpec{Address: "redis://example:6379"}
	scheme := runtime.NewScheme()
	_ = keyvalv1alpha1.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	client := newStubClient()
	client.masterHost = "example"
	client.masterPort = 6379
	client.masterLinkStatus = "up"
	client.masterSync = false
	client.masterOffset = 100
	client.replicaOffset = 100

	started := metav1.NewTime(time.Now().Add(-time.Minute))

	res, err := Ensure(context.Background(), Options{
		Cluster:       cr,
		Spec:          spec,
		CurrentStatus: &keyvalv1alpha1.ExternalImportStatus{State: importStateInProgress, StartedAt: &started},
		Pods:          []corev1.Pod{readyPod("demo-0")},
		ClientFactory: &stubFactory{client: client},
		ClientOptions: clients.ClientOptions{},
		KubeClient:    kube,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Status.State != importStateCompleted {
		t.Fatalf("expected completed state, got %s", res.Status.State)
	}
	if res.Status.Mode != string(keyvalv1alpha1.ExternalSourceSyncModeSnapshot) {
		t.Fatalf("expected mode snapshot, got %s", res.Status.Mode)
	}
	if res.Abort {
		t.Fatalf("expected abort=false")
	}
	if client.noOneCalls != 1 {
		t.Fatalf("expected NoOne called once, got %d", client.noOneCalls)
	}
	if val := client.configSet["masterauth"]; val != "" {
		t.Fatalf("expected masterauth cleared, got %s", val)
	}
}

func TestEnsure_LiveModeWaitsForReadiness(t *testing.T) {
	t.Parallel()
	cr := minimalCluster()
	replicas := int32(3)
	cr.Spec.RedisReplicas = replicas
	spec := &keyvalv1alpha1.ExternalSourceSpec{Address: "redis://example:6379", SyncMode: keyvalv1alpha1.ExternalSourceSyncModeLive}
	scheme := runtime.NewScheme()
	_ = keyvalv1alpha1.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	client := newStubClient()
	client.masterHost = "example"
	client.masterPort = 6379
	client.masterLinkStatus = "up"
	client.masterOffset = 5
	client.replicaOffset = 5

	started := metav1.Now()

	res, err := Ensure(context.Background(), Options{
		Cluster:       cr,
		Spec:          spec,
		CurrentStatus: &keyvalv1alpha1.ExternalImportStatus{State: importStateInProgress, StartedAt: &started},
		Pods:          []corev1.Pod{readyPod("demo-0"), notReadyPod("demo-1"), notReadyPod("demo-2")},
		ClientFactory: &stubFactory{client: client},
		ClientOptions: clients.ClientOptions{},
		KubeClient:    kube,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Condition.Reason != "WaitingForReadiness" {
		t.Fatalf("expected WaitingForReadiness, got %s", res.Condition.Reason)
	}
	if !res.Abort {
		t.Fatalf("expected abort=true")
	}
	if client.noOneCalls != 0 {
		t.Fatalf("expected no promotion yet, got %d", client.noOneCalls)
	}
}

func TestEnsure_LiveModeFollowingUntilCutover(t *testing.T) {
	t.Parallel()
	cr := minimalCluster()
	spec := &keyvalv1alpha1.ExternalSourceSpec{Address: "redis://example:6379", SyncMode: keyvalv1alpha1.ExternalSourceSyncModeLive}
	scheme := runtime.NewScheme()
	_ = keyvalv1alpha1.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	client := newStubClient()
	client.masterHost = "example"
	client.masterPort = 6379
	client.masterLinkStatus = "up"
	client.masterSync = false
	client.masterOffset = 42
	client.replicaOffset = 42

	res, err := Ensure(context.Background(), Options{
		Cluster:       cr,
		Spec:          spec,
		CurrentStatus: &keyvalv1alpha1.ExternalImportStatus{State: importStateInProgress},
		Pods:          []corev1.Pod{readyPod("demo-0")},
		ClientFactory: &stubFactory{client: client},
		ClientOptions: clients.ClientOptions{},
		KubeClient:    kube,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Status.State != importStateFollowing {
		t.Fatalf("expected following state, got %s", res.Status.State)
	}
	if res.Condition.Reason != "Following" {
		t.Fatalf("expected Following reason, got %s", res.Condition.Reason)
	}
	if res.Abort != true {
		t.Fatalf("expected abort=true")
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected requeue while following")
	}
	if client.noOneCalls != 0 {
		t.Fatalf("expected no promotion while following, got %d", client.noOneCalls)
	}
}

func TestEnsure_LiveModeFollowingDoesNotTimeout(t *testing.T) {
	t.Parallel()
	cr := minimalCluster()
	spec := &keyvalv1alpha1.ExternalSourceSpec{Address: "redis://example:6379", SyncMode: keyvalv1alpha1.ExternalSourceSyncModeLive}
	scheme := runtime.NewScheme()
	_ = keyvalv1alpha1.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	client := newStubClient()
	client.masterHost = "example"
	client.masterPort = 6379
	client.masterLinkStatus = "up"
	client.masterSync = false
	client.masterOffset = 512
	client.replicaOffset = 512

	started := metav1.NewTime(time.Now().Add(-2 * time.Hour))

	res, err := Ensure(context.Background(), Options{
		Cluster: cr,
		Spec:    spec,
		CurrentStatus: &keyvalv1alpha1.ExternalImportStatus{
			State:      importStateFollowing,
			Mode:       string(keyvalv1alpha1.ExternalSourceSyncModeLive),
			Source:     spec.Address,
			StartedAt:  &started,
			LastSynced: &started,
		},
		Pods:          []corev1.Pod{readyPod("demo-0")},
		ClientFactory: &stubFactory{client: client},
		ClientOptions: clients.ClientOptions{},
		KubeClient:    kube,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Status.State != importStateFollowing {
		t.Fatalf("expected following state, got %s", res.Status.State)
	}
	if res.Condition.Reason != "Following" {
		t.Fatalf("expected Following reason, got %s", res.Condition.Reason)
	}
	if !res.Abort {
		t.Fatalf("expected abort=true while following")
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected periodic requeue while following")
	}
	if client.noOneCalls != 0 {
		t.Fatalf("expected no promotion while following, got %d", client.noOneCalls)
	}
}

func TestFinalizeDetachOnSpecRemoval(t *testing.T) {
	t.Parallel()
	cr := minimalCluster()
	scheme := runtime.NewScheme()
	_ = keyvalv1alpha1.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	client := newStubClient()
	client.masterHost = "example"
	client.masterPort = 6379

	status := &keyvalv1alpha1.ExternalImportStatus{
		State: importStateFollowing,
		Mode:  string(keyvalv1alpha1.ExternalSourceSyncModeLive),
	}

	res, err := Ensure(context.Background(), Options{
		Cluster:       cr,
		CurrentStatus: status,
		Pods:          []corev1.Pod{readyPod("demo-0")},
		ClientFactory: &stubFactory{client: client},
		ClientOptions: clients.ClientOptions{},
		KubeClient:    kube,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Status.State != importStateCompleted {
		t.Fatalf("expected completed state, got %s", res.Status.State)
	}
	if res.Abort {
		t.Fatalf("expected abort=false after cutover")
	}
	if res.Condition.Reason != "Completed" {
		t.Fatalf("expected Completed reason, got %s", res.Condition.Reason)
	}
	if client.noOneCalls != 1 {
		t.Fatalf("expected promotion during cutover, got %d", client.noOneCalls)
	}
}

type stubFactory struct {
	client *stubClient
}

func (f *stubFactory) ForPod(ctx context.Context, pod corev1.Pod, _ clients.ClientOptions) (clients.Client, error) {
	return f.client, nil
}

type stubClient struct {
	dbSize           int64
	masterHost       string
	masterPort       int
	masterLinkStatus string
	masterSync       bool
	masterOffset     int64
	replicaOffset    int64
	role             string
	configSet        map[string]string
	replicaOfCalls   []string
	noOneCalls       int
}

func newStubClient() *stubClient {
	return &stubClient{role: "replica", configSet: make(map[string]string)}
}

func (s *stubClient) Role(context.Context) (string, error) { return s.role, nil }

func (s *stubClient) ReplicationInfo(context.Context) (clients.ReplicationInfo, error) {
	return clients.ReplicationInfo{
		Role:                 s.role,
		MasterHost:           s.masterHost,
		MasterPort:           s.masterPort,
		MasterLinkStatus:     s.masterLinkStatus,
		MasterSyncInProgress: s.masterSync,
		MasterReplOffset:     s.masterOffset,
		ReplicaReplOffset:    s.replicaOffset,
	}, nil
}

func (s *stubClient) ReplicaOf(context.Context, string, int) error {
	s.replicaOfCalls = append(s.replicaOfCalls, "call")
	s.role = "replica"
	return nil
}

func (s *stubClient) NoOne(context.Context) error {
	s.noOneCalls++
	s.role = "master"
	s.masterHost = ""
	s.masterPort = 0
	return nil
}

func (s *stubClient) ResetSentinel(context.Context, *keyvalv1alpha1.KeyValCluster) error { return nil }

func (s *stubClient) ConfigGet(context.Context, string) (string, bool, error) { return "", false, nil }

func (s *stubClient) ConfigSet(_ context.Context, parameter, value string) error {
	s.configSet[parameter] = value
	return nil
}

func (s *stubClient) ConfigRewrite(context.Context) error { return nil }

func (s *stubClient) Auth(context.Context, string, string) error { return nil }

func (s *stubClient) DBSize(context.Context) (int64, error) { return s.dbSize, nil }

func minimalCluster() *keyvalv1alpha1.KeyValCluster {
	return &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 1,
		},
	}
}

func readyPod(name string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func notReadyPod(name string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}},
		},
	}
}
