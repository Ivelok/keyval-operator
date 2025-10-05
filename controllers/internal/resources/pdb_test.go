package resources

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestSentinelPDBDefaultsMinAvailableWhenUnset(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel},
	}
	pdb := SentinelPDB(cr, 2)
	if pdb.Spec.MinAvailable == nil {
		t.Fatalf("expected MinAvailable default when custom spec omitted")
	}
	if pdb.Spec.MaxUnavailable != nil {
		t.Fatalf("expected MaxUnavailable nil by default")
	}
	if got := pdb.Spec.MinAvailable.IntValue(); got != 2 {
		t.Fatalf("expected MinAvailable 2, got %d", got)
	}
}

func TestSentinelPDBHonorsMaxUnavailableOverride(t *testing.T) {
	t.Parallel()
	one := intstr.FromInt(1)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:        keyvalv1alpha1.ModeSentinel,
			SentinelPDB: &keyvalv1alpha1.SentinelPDB{MaxUnavailable: &one},
		},
	}
	pdb := SentinelPDB(cr, 2)
	if pdb.Spec.MinAvailable != nil {
		t.Fatalf("expected MinAvailable nil when MaxUnavailable provided, got %+v", pdb.Spec.MinAvailable)
	}
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 {
		t.Fatalf("expected MaxUnavailable 1, got %+v", pdb.Spec.MaxUnavailable)
	}
}

func TestRedisPDBClampsNegativeMinAvailable(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	pdb := RedisPDB(cr, -5)
	if pdb.Spec.MinAvailable == nil {
		t.Fatalf("expected MinAvailable to be set")
	}
	if pdb.Spec.MinAvailable.Type != intstr.Int {
		t.Fatalf("expected int MinAvailable, got %v", pdb.Spec.MinAvailable.Type)
	}
	if pdb.Spec.MinAvailable.IntValue() != 0 {
		t.Fatalf("expected MinAvailable clamped to 0, got %d", pdb.Spec.MinAvailable.IntValue())
	}
	if pdb.Spec.Selector == nil || len(pdb.Spec.Selector.MatchLabels) == 0 {
		t.Fatalf("expected selector labels populated")
	}
	if pdb.Labels[core.LabelClusterKey] == "" {
		t.Fatalf("expected cluster label propagated")
	}
}

func TestSentinelPDBCopiesExplicitMinAvailable(t *testing.T) {
	t.Parallel()
	two := intstr.FromInt(2)
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:        keyvalv1alpha1.ModeSentinel,
			SentinelPDB: &keyvalv1alpha1.SentinelPDB{MinAvailable: &two},
		},
	}
	pdb := SentinelPDB(cr, 3)
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntValue() != 2 {
		t.Fatalf("expected MinAvailable propagated from spec, got %+v", pdb.Spec.MinAvailable)
	}
	if pdb.Spec.MaxUnavailable != nil {
		t.Fatalf("expected MaxUnavailable nil when MinAvailable provided")
	}
}

func TestSentinelPDBSelectorLabels(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel}}
	pdb := SentinelPDB(cr, 1)
	selector := pdb.Spec.Selector
	if selector == nil {
		t.Fatalf("expected selector defined")
	}
	required := []string{"app", "keyvalcluster"}
	for _, k := range required {
		if _, ok := selector.MatchLabels[k]; !ok {
			t.Fatalf("expected selector label %s", k)
		}
	}
}

func TestSentinelPDBDoesNotMutateInputCluster(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeSentinel}}
	_ = SentinelPDB(cr, 1)
	if cr.Spec.SentinelPDB != nil {
		t.Fatalf("expected cluster spec untouched, got %+v", cr.Spec.SentinelPDB)
	}
}

func TestRedisPDBTemplate(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"}}
	pdb := RedisPDB(cr, 2)
	if pdb.ObjectMeta.Name != "demo-redis" {
		t.Fatalf("unexpected name %s", pdb.ObjectMeta.Name)
	}
	if pdb.Spec.Selector == nil || len(pdb.Spec.Selector.MatchLabels) == 0 {
		t.Fatalf("expected selector labels")
	}
	if pdb.Spec.MinAvailable.IntValue() != 2 {
		t.Fatalf("expected MinAvailable 2, got %d", pdb.Spec.MinAvailable.IntValue())
	}
}
