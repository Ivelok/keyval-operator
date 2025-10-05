package controllers

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

func TestBackoffTrackerSequence(t *testing.T) {
	tracker := newBackoffTracker()
	key := "ns/test"
	expected := []int64{3, 6, 12, 24, 48, 60}
	for i, base := range expected {
		got := tracker.Next(key)
		lower := float64(base) * 0.9
		upper := float64(base) * 1.1
		if got.Seconds() < lower || got.Seconds() > upper {
			t.Fatalf("attempt %d: expected delay within [%.1fs, %.1fs], got %s", i+1, lower, upper, got)
		}
	}
	// Subsequent calls should stay within final bucket
	for i := 0; i < 3; i++ {
		got := tracker.Next(key)
		if got.Seconds() < 54 || got.Seconds() > 66 {
			t.Fatalf("saturated attempt produced unexpected delay %s", got)
		}
	}
	tracker.Reset(key)
	resetDelay := tracker.Next(key)
	if resetDelay.Seconds() < 2.7 || resetDelay.Seconds() > 3.3 {
		t.Fatalf("expected reset delay near 3s, got %s", resetDelay)
	}
}

func TestApplyBackoffDelay(t *testing.T) {
	r := &KeyValClusterReconciler{backoffs: newBackoffTracker()}
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "demo"}}
	key := "ns/demo"

	first := r.applyBackoffDelay(cr, key, 0)
	if first.Seconds() < 2.7 || first.Seconds() > 3.3 {
		t.Fatalf("expected backoff around 3s, got %s", first)
	}

	second := r.applyBackoffDelay(cr, key, 4*time.Second)
	if second < 4*time.Second {
		t.Fatalf("expected min delay of base 4s, got %s", second)
	}
	if second.Seconds() > 48*1.1 { // unreasonable upper bound
		t.Fatalf("second delay unexpectedly large: %s", second)
	}
}
