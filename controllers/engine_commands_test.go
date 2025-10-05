package controllers

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func TestValkeyCommands_SelectedForEngine(t *testing.T) {
	t.Parallel()
	v := int32(3)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel, Engine: keyvalv1alpha1.EngineValkey, Image: "valkey/valkey:7.2", RedisReplicas: 3, SentinelCount: &v},
	}
	sec := &security.Settings{}
	ss := resources.StatefulSet(cr, "", "", sec)
	if len(ss.Spec.Template.Spec.Containers) == 0 || ss.Spec.Template.Spec.Containers[0].Command[0] != "valkey-server" {
		t.Fatalf("expected valkey-server command, got: %v", ss.Spec.Template.Spec.Containers[0].Command)
	}
	sss := resources.SentinelStatefulSet(cr, "", "", sec)
	if len(sss.Spec.Template.Spec.Containers) == 0 || sss.Spec.Template.Spec.Containers[0].Command[0] != "valkey-sentinel" {
		t.Fatalf("expected valkey-sentinel command, got: %v", sss.Spec.Template.Spec.Containers[0].Command)
	}
}
