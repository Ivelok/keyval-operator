package controllers

import (
	"strings"
	"testing"

	"k8s.io/client-go/tools/record"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
)

func TestEventHelpers_Format(t *testing.T) {
	t.Parallel()
	rec := record.NewFakeRecorder(10)
	cr := &keyvalv1alpha1.KeyValCluster{}

	opobs.EventRoleCorrected(rec, cr, "demo-0")
	opobs.EventReplicationAligned(rec, cr, "demo-0")
	opobs.EventStartFailover(rec, cr, opobs.FailoverTypeForced, "target=demo-2")
	opobs.EventNewMaster(rec, cr, "demo-2", opobs.FailoverTypeForced, nil)
	opobs.EventReplicationHeuristicFallback(rec, cr, "sentinel down")
	opobs.EventPodEvicted(rec, cr, "demo-1", []string{"config-hash"})

	// Drain events and check contents
	got := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		e := <-rec.Events
		got = append(got, e)
	}
	joined := strings.Join(got, "\n")
	for _, sub := range []string{"RoleCorrected", "ReplicationAligned", "StartFailover", "NewMaster", "PodEvicted", "ReplicationHeuristicFallback"} {
		if !strings.Contains(joined, sub) {
			t.Fatalf("expected event %s in %s", sub, joined)
		}
	}
}
