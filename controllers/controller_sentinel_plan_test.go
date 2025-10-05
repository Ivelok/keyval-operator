package controllers

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	reconcileutil "github.com/ivelok/keyval-operator/controllers/internal/reconcile"
)

func TestAllowSentinelDisruptions_HealthGateAllows(t *testing.T) {
	status := &keyvalv1alpha1.KeyValClusterStatus{
		HealthGate: &keyvalv1alpha1.HealthGateStatus{AllowDisruptions: true},
	}
	if !reconcileutil.AllowSentinelDisruptions(status) {
		t.Fatalf("expected allowSentinelDisruptions to return true when health gate permits disruptions")
	}
}

func TestAllowSentinelDisruptions_AllowsWhenQuorumRecoverable(t *testing.T) {
	status := &keyvalv1alpha1.KeyValClusterStatus{
		Conditions: []metav1.Condition{
			{Type: string(keyvalv1alpha1.ConditionSentinelQuorum), Status: metav1.ConditionFalse, Reason: "SentinelQuorumNotReady"},
		},
	}
	if !reconcileutil.AllowSentinelDisruptions(status) {
		t.Fatalf("expected quorum loss to allow sentinel disruptions")
	}
}

func TestAllowSentinelDisruptions_RespectsDisruptionPause(t *testing.T) {
	status := &keyvalv1alpha1.KeyValClusterStatus{
		Conditions: []metav1.Condition{
			{Type: string(keyvalv1alpha1.ConditionSentinelQuorum), Status: metav1.ConditionFalse, Reason: "Other"},
			{Type: string(keyvalv1alpha1.ConditionDisruptionsPaused), Status: metav1.ConditionTrue},
		},
	}
	if reconcileutil.AllowSentinelDisruptions(status) {
		t.Fatalf("expected disruptions pause to override sentinel allowance for non-recoverable reason")
	}
}

func TestAllowSentinelDisruptions_UnrecognizedReason(t *testing.T) {
	status := &keyvalv1alpha1.KeyValClusterStatus{
		Conditions: []metav1.Condition{{Type: string(keyvalv1alpha1.ConditionSentinelQuorum), Status: metav1.ConditionFalse, Reason: "Other"}},
	}
	if reconcileutil.AllowSentinelDisruptions(status) {
		t.Fatalf("expected unknown sentinel reason to keep disruptions blocked")
	}
}

func TestAllowSentinelDisruptions_AllowsWhenPausedForSentinelQuorum(t *testing.T) {
	status := &keyvalv1alpha1.KeyValClusterStatus{
		Conditions: []metav1.Condition{
			{Type: string(keyvalv1alpha1.ConditionSentinelQuorum), Status: metav1.ConditionFalse, Reason: "SentinelQuorumLost"},
			{Type: string(keyvalv1alpha1.ConditionDisruptionsPaused), Status: metav1.ConditionTrue, Reason: "SentinelQuorum"},
		},
	}
	if !reconcileutil.AllowSentinelDisruptions(status) {
		t.Fatalf("expected disruptions to proceed when pause reason is sentinel quorum recovery")
	}
}
