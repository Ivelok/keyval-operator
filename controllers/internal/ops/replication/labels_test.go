package replication

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestUpdatePodRoleLabels(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Labels: map[string]string{}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Labels: map[string]string{}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-2", Labels: map[string]string{}}},
	}
	out := UpdatePodRoleLabels(pods, "demo-1")
	if len(out) != len(pods) {
		t.Fatalf("expected %d pods, got %d", len(pods), len(out))
	}
	for _, p := range out {
		role := p.Labels["role"]
		switch p.Name {
		case "demo-1":
			if role != string(keyvalv1alpha1.PodRoleMaster) {
				t.Fatalf("expected master label for demo-1, got %s", role)
			}
		default:
			if role != string(keyvalv1alpha1.PodRoleReplica) {
				t.Fatalf("expected replica label for %s, got %s", p.Name, role)
			}
		}
	}
}

func TestUpdatePodRoleLabelsPreservesUserLabels(t *testing.T) {
	t.Parallel()
	original := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Labels: map[string]string{"app": "demo", "role": string(keyvalv1alpha1.PodRoleReplica)}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Labels: map[string]string{"custom": "keep", "role": string(keyvalv1alpha1.PodRoleMaster)}}},
	}
	copyBefore := original[1].Labels["custom"]
	updated := UpdatePodRoleLabels(original, "demo-0")
	if original[1].Labels["role"] != string(keyvalv1alpha1.PodRoleMaster) {
		t.Fatalf("expected original slice untouched, got %s", original[1].Labels["role"])
	}
	if got := updated[1].Labels["custom"]; got != copyBefore {
		t.Fatalf("expected custom label preserved, got %s", got)
	}
	if got := updated[0].Labels[core.RoleLabelKey]; got != string(keyvalv1alpha1.PodRoleMaster) {
		t.Fatalf("expected demo-0 to be master, got %s", got)
	}
}

func TestUpdatePodRoleLabelsEnsuresSingleMaster(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Labels: map[string]string{core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster)}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Labels: map[string]string{core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster)}}},
	}
	updated := UpdatePodRoleLabels(pods, "demo-1")
	masters := 0
	for _, p := range updated {
		if p.Labels[core.RoleLabelKey] == string(keyvalv1alpha1.PodRoleMaster) {
			masters++
		}
	}
	if masters != 1 {
		t.Fatalf("expected exactly one master label, got %d", masters)
	}
	if updated[0].Labels[core.RoleLabelKey] != string(keyvalv1alpha1.PodRoleReplica) {
		t.Fatalf("expected demo-0 to be relabeled replica, got %s", updated[0].Labels[core.RoleLabelKey])
	}
}
