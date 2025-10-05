package clients

import (
	"context"
	"crypto/tls"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestSentinelFactoryForPod_UsesPodAddressAndCustomPort(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:           keyvalv1alpha1.ModeSentinel,
			Image:          "valkey/valkey:7.2",
			RedisReplicas:  3,
			SentinelConfig: map[string]string{"port": "36379"},
			SentinelCount:  func() *int32 { v := int32(3); return &v }(),
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cr).Build()
	factory := NewGoRedisSentinelFactory(c)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-sentinel-0",
			Namespace: "ns",
			Labels:    map[string]string{core.LabelClusterKey: "demo"},
		},
		Status: corev1.PodStatus{PodIP: "10.1.2.3"},
	}
	clientIntf, err := factory.ForPod(context.Background(), pod, SentinelOptions{})
	if err != nil {
		t.Fatalf("ForPod err: %v", err)
	}
	s, ok := clientIntf.(*goRedisSentinel)
	if !ok {
		t.Fatalf("unexpected sentinel client type %T", clientIntf)
	}
	if got := s.rdb.Options().Addr; got != "10.1.2.3:36379" {
		t.Fatalf("expected pod IP address, got %s", got)
	}
}

func TestSentinelFactoryForPod_TLSUsesDNSAndServerName(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 3,
			SentinelCount: func() *int32 { v := int32(3); return &v }(),
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cr).Build()
	factory := NewGoRedisSentinelFactory(c)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-sentinel-0",
			Namespace: "ns",
			Labels:    map[string]string{core.LabelClusterKey: "demo"},
		},
	}
	clientIntf, err := factory.ForPod(context.Background(), pod, SentinelOptions{TLSConfig: &tls.Config{}})
	if err != nil {
		t.Fatalf("ForPod err: %v", err)
	}
	s, ok := clientIntf.(*goRedisSentinel)
	if !ok {
		t.Fatalf("unexpected sentinel client type %T", clientIntf)
	}
	wantHost := "demo-sentinel-0.demo-sentinel-headless.ns.svc"
	if got := s.rdb.Options().Addr; got != wantHost+":26379" {
		t.Fatalf("expected DNS address %s:26379, got %s", wantHost, got)
	}
	cfg := s.rdb.Options().TLSConfig
	if cfg == nil {
		t.Fatalf("expected TLS config")
	}
	if cfg.ServerName != wantHost {
		t.Fatalf("expected ServerName %s, got %s", wantHost, cfg.ServerName)
	}
}

func TestSentinelFactoryForPodFallsBackToServiceAddress(t *testing.T) {
	t.Parallel()
	factory := NewGoRedisSentinelFactory(nil)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-sentinel-0",
			Namespace: "ns",
			Labels:    map[string]string{core.LabelClusterKey: "demo"},
		},
	}
	clientIntf, err := factory.ForPod(context.Background(), pod, SentinelOptions{})
	if err != nil {
		t.Fatalf("ForPod err: %v", err)
	}
	s, ok := clientIntf.(*goRedisSentinel)
	if !ok {
		t.Fatalf("unexpected client type %T", clientIntf)
	}
	if got := s.rdb.Options().Addr; got != "demo-sentinel.ns.svc:26379" {
		t.Fatalf("expected fallback sentinel service address, got %s", got)
	}
	_ = s.rdb.Close()
}
