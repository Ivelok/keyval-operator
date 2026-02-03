package update

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/eviction"
)

type rejectingEvictor struct {
	called int
}

func (r *rejectingEvictor) Evict(context.Context, eviction.Request) error {
	r.called++
	return eviction.ErrRejected
}

func TestIsConfigDriftReason(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"config-hash":                   true,
		"tls-hash":                      true,
		"resources:redis":               true,
		"resources:sentinel":            true,
		"resources:metrics":             true,
		"spec:node-selector":            true,
		"metrics:missing":               true,
		"metrics:present-when-disabled": true,
		"image:redis":                   true,
		"image:sentinel":                true,
		"layout:sentinel-removed":       true,
		"health:lagging":                false,
		"health:desynced":               false,
		"health:offline":                false,
		"scale-down":                    false,
		"":                              false,
	}
	for reason, expected := range cases {
		if got := IsConfigDriftReason(reason); got != expected {
			t.Fatalf("reason %q expected %v, got %v", reason, expected, got)
		}
	}
}

func TestPlanHasConfigDrift(t *testing.T) {
	t.Parallel()
	plan := Plan{
		PodNames: []string{"demo-0"},
		Reasons: map[string][]string{
			"demo-0": {"health:lagging", "config-hash"},
		},
	}
	if !PlanHasConfigDrift(plan) {
		t.Fatalf("expected config drift in plan")
	}
	noDrift := Plan{
		PodNames: []string{"demo-1"},
		Reasons: map[string][]string{
			"demo-1": {"health:offline", "scale-down"},
		},
	}
	if PlanHasConfigDrift(noDrift) {
		t.Fatalf("did not expect config drift in plan")
	}
}

func TestExecuteSentinelFallbackDeletesUnreadyPod(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add api scheme: %v", err)
	}

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			RedisReplicas: 3,
		},
	}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-sentinel-0",
			Namespace: "default",
			Labels: map[string]string{
				core.LabelAppKey:     "demo-sentinel",
				core.LabelClusterKey: "demo",
			},
		},
	}
	plan := Plan{PodNames: []string{pod.Name}, Reasons: map[string][]string{pod.Name: []string{"config-hash"}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).Build()
	evictor := &rejectingEvictor{}
	deleted, err := Execute(context.Background(), client, cr, plan, []corev1.Pod{pod}, nil, ExecuteOptions{Evictor: evictor, Component: ComponentSentinel})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if deleted != pod.Name {
		t.Fatalf("expected deleted pod %q, got %q", pod.Name, deleted)
	}
	if evictor.called != 1 {
		t.Fatalf("expected single eviction attempt, got %d", evictor.called)
	}
	var still corev1.Pod
	if getErr := client.Get(context.Background(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, &still); !apierrors.IsNotFound(getErr) {
		t.Fatalf("expected pod to be deleted, got error: %v", getErr)
	}
}
