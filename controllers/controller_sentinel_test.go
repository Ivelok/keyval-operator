package controllers

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	reconcilepkg "github.com/ivelok/keyval-operator/controllers/internal/reconcile"
)

type sentinelClientStub struct {
	ok  bool
	err error
}

func (s *sentinelClientStub) GetMasterAddrByName(context.Context, string) (string, int, error) {
	return "", 0, nil
}
func (s *sentinelClientStub) Failover(context.Context, string) error            { return nil }
func (s *sentinelClientStub) Set(context.Context, string, string, string) error { return nil }
func (s *sentinelClientStub) Master(context.Context, string) (map[string]string, error) {
	return nil, nil
}
func (s *sentinelClientStub) CheckQuorum(context.Context, string) (bool, error) { return s.ok, s.err }
func (s *sentinelClientStub) Reset(context.Context, string) error               { return nil }

type sentinelFactoryStub struct {
	client SentinelClient
	err    error
}

func (f *sentinelFactoryStub) ForPod(context.Context, corev1.Pod, SentinelOptions) (SentinelClient, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

func TestHasSentinelQuorumTrue(t *testing.T) {
	cr := keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-sentinel-0"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
	}
	report, err := reconcilepkg.HasSentinelQuorum(context.Background(), &cr, pods, &sentinelFactoryStub{client: &sentinelClientStub{ok: true}}, 1, SentinelOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.Healthy {
		t.Fatalf("expected quorum, got report=%+v", report)
	}
}

func TestHasSentinelQuorumNoReadyPods(t *testing.T) {
	cr := keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel}}
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "demo-sentinel-0"}}}
	report, err := reconcilepkg.HasSentinelQuorum(context.Background(), &cr, pods, &sentinelFactoryStub{client: &sentinelClientStub{ok: false}}, 1, SentinelOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Healthy {
		t.Fatalf("expected quorum=false")
	}
	if report.Detail == "" {
		t.Fatalf("expected detail message")
	}
}

func TestHasSentinelQuorumError(t *testing.T) {
	cr := keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel}}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-sentinel-0"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
	}
	expectedErr := errors.New("dial error")
	report, err := reconcilepkg.HasSentinelQuorum(context.Background(), &cr, pods, &sentinelFactoryStub{client: &sentinelClientStub{err: expectedErr}}, 1, SentinelOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}
	if report.Healthy {
		t.Fatalf("expected quorum=false")
	}
	if report.Detail == "" {
		t.Fatalf("expected detail message")
	}
}
