//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

func listSentinelPods(t *testing.T, s *suite.Suite, cr *keyvalv1alpha1.KeyValCluster) []corev1.Pod {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Context(), 90*time.Second)
	defer cancel()

	selector := labels.Set{"app": fmt.Sprintf("%s-sentinel", cr.Name)}
	var pods corev1.PodList
	if err := s.Harness.Client().List(ctx, &pods, client.InNamespace(cr.Namespace), client.MatchingLabels(selector)); err != nil {
		t.Fatalf("list sentinel pods: %v", err)
	}
	if len(pods.Items) == 0 {
		t.Fatalf("no sentinel pods found")
	}
	return pods.Items
}
