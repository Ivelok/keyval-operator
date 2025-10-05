package controllers

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
)

func TestMetrics_Smoke(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	// Simple smoke: these calls should not panic
	opobs.IncReplicationChanges(cr)
	opobs.IncLabelCorrectionsBy(cr, 2)
	opobs.IncRollingDeletions(cr)
	opobs.IncFailover(cr, opobs.FailoverTypeForced)
	opobs.IncBootstrapAttempt(cr, opobs.BootstrapModeSentinel)
	opobs.IncBootstrapFailure(cr)
	opobs.IncReconcileResult(cr, opobs.ReconcileResultSuccess)
	opobs.ObserveReconcile(cr, 0)
}
