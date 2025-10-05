//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

func TestSentinelNetworkPartition(t *testing.T) {
	s := suite.New(t)

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName(fmt.Sprintf("csp-%s", uniqueSuffix())).
		WithLabels(map[string]string{"suite": "chaos", "scenario": "sentinel-network-partition"}).
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage()
	cr := builder.Build()

	manager := cluster.NewManager(s.Harness)

	var (
		masterBefore string
		masterUID    types.UID
		masterIP     string
		sentinels    []corev1.Pod
	)

	s.Step("create-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply cluster: %v", err)
		}
	})

	s.Step("wait-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 6*time.Minute)
		*cr = *updated
		assert.MasterService(t, s.Harness, cr, time.Minute)
		assert.SentinelService(t, s.Harness, cr, 5*time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)

		master := assert.MasterPod(t, s.Harness, cr)
		if master.Status.PodIP == "" {
			t.Fatalf("master pod has no IP")
		}
		masterBefore = master.Name
		masterUID = master.UID
		masterIP = master.Status.PodIP

		sentinels = listSentinelPods(t, s, cr)
	})

	s.Step("check-iptables", func(ctx context.Context) {
		if len(sentinels) == 0 {
			t.Fatalf("no sentinel pods discovered")
		}
		// ensure iptables present; reuse first sentinel
		if err := ensureBinary(ctx, s, sentinels[0].Name, "sentinel", "iptables"); err != nil {
			t.Skipf("iptables unavailable in sentinel container: %v", err)
		}
	})

	s.Step("partition-master", func(ctx context.Context) {
		for _, pod := range sentinels {
			addPartitionRule(ctx, t, s, pod.Name, masterIP)
		}
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for _, pod := range sentinels {
				removePartitionRule(cleanupCtx, s, pod.Name, masterIP)
			}
		})
	})

	s.Step("wait-failover", func(ctx context.Context) {
		assert.WaitForMasterChange(t, s.Harness, cr, masterBefore, masterUID, 5*time.Minute)
	})

	s.Step("heal-partition", func(ctx context.Context) {
		for _, pod := range sentinels {
			removePartitionRule(ctx, s, pod.Name, masterIP)
		}
	})

	s.Step("verify-recovery", func(ctx context.Context) {
		assert.ReplicationHealthy(t, s.Harness, cr, 3*time.Minute)
		assert.SentinelService(t, s.Harness, cr, time.Minute)
	})
}

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

func ensureBinary(ctx context.Context, s *suite.Suite, podName, container, binary string) error {
	cmd := []string{"sh", "-c", fmt.Sprintf("command -v %s", binary)}
	_, _, err := s.Harness.Exec(ctx, podName, container, cmd...)
	if err != nil && strings.Contains(err.Error(), "executable file not found") {
		return err
	}
	return err
}

func addPartitionRule(ctx context.Context, t *testing.T, s *suite.Suite, podName, targetIP string) {
	t.Helper()
	cmd := fmt.Sprintf("iptables -w 5 -I OUTPUT 1 -d %s/32 -p tcp --dport 6379 -j REJECT", targetIP)
	if _, stderr, err := s.Harness.Exec(ctx, podName, "sentinel", "sh", "-c", cmd); err != nil {
		t.Fatalf("add iptables rule on %s: %v stderr=%s", podName, err, strings.TrimSpace(stderr))
	}
}

func removePartitionRule(ctx context.Context, s *suite.Suite, podName, targetIP string) {
	cmd := fmt.Sprintf("iptables -w 5 -D OUTPUT -d %s/32 -p tcp --dport 6379 -j REJECT", targetIP)
	_, _, _ = s.Harness.Exec(ctx, podName, "sentinel", "sh", "-c", cmd)
}

func uniqueSuffix() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}
