//go:build !(e2e || chaos)

package harness

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

type Harness struct{}

func New(t *testing.T) *Harness {
	t.Helper()
	t.Skip("test/internal/harness requires -tags=e2e")
	return &Harness{}
}

func (h *Harness) Context() context.Context { return context.Background() }

func (h *Harness) Client() client.Client { panic("test/internal/harness requires -tags=e2e") }

func (h *Harness) Kube() kubernetes.Interface { panic("test/internal/harness requires -tags=e2e") }

func (h *Harness) RestConfig() *rest.Config { panic("test/internal/harness requires -tags=e2e") }

func (h *Harness) Namespace() string { return "" }

func (h *Harness) RedisImage() string { return "" }

func (h *Harness) TrackCluster(client.Object) {}

func (h *Harness) WaitForCondition(context.Context, string, keyvalv1alpha1.ConditionType, metav1.ConditionStatus, time.Duration) *keyvalv1alpha1.KeyValCluster {
	panic("test/internal/harness requires -tags=e2e")
}

func (h *Harness) WaitForReadyReplicas(context.Context, string, int32, time.Duration) *keyvalv1alpha1.KeyValCluster {
	panic("test/internal/harness requires -tags=e2e")
}

func (h *Harness) WaitForDeploymentRollout(context.Context, string, time.Duration) {
	panic("test/internal/harness requires -tags=e2e")
}

func (h *Harness) WaitForService(context.Context, string, time.Duration) *corev1.Service {
	panic("test/internal/harness requires -tags=e2e")
}

func (h *Harness) Exec(context.Context, string, string, ...string) (string, string, error) {
	return "", "", errors.New("test/internal/harness requires -tags=e2e")
}
