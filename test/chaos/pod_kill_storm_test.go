//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/chaosmetrics"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

func TestSentinelPodKillStorm(t *testing.T) {
	cfg := chaosmetrics.LoadConfig(t)
	chaosmetrics.SkipUnless(t, cfg, "pod-kill-storm")

	s := suite.New(t)

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName(fmt.Sprintf("cps-%s", uniqueSuffix())).
		WithLabels(map[string]string{"suite": "chaos", "scenario": "pod-kill-storm"}).
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage()
	cr := builder.Build()

	manager := cluster.NewManager(s.Harness)
	scenarioMetrics := chaosmetrics.NewScenario(t, s.Harness, cr, cfg, "pod-kill-storm")
	defer scenarioMetrics.Finish()

	s.Step("create-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply cluster: %v", err)
		}
	})

	s.Step("wait-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 6*time.Minute)
		*cr = *updated
		assert.MasterService(t, s.Harness, cr, time.Minute)
		assert.SentinelService(t, s.Harness, cr, time.Minute)
		assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
	})

	rand.Seed(time.Now().UnixNano())
	var (
		masterBefore  string
		masterUID     types.UID
		failoverStart time.Time
	)
	scenarios := []struct {
		name string
		kill func(ctx context.Context)
	}{
		{
			name: "kill-sentinel",
			kill: func(ctx context.Context) {
				pod := pickSentinelPod(t, s, cr)
				deletePod(ctx, t, s, pod.Name)
			},
		},
		{
			name: "kill-master",
			kill: func(ctx context.Context) {
				master := assert.MasterPod(t, s.Harness, cr)
				masterBefore = master.Name
				masterUID = master.UID
				failoverStart = time.Now()
				deletePod(ctx, t, s, master.Name)
			},
		},
		{
			name: "kill-replica",
			kill: func(ctx context.Context) {
				replica := pickReplicaPod(t, s, cr)
				deletePod(ctx, t, s, replica.Name)
			},
		},
	}

	for idx, scenario := range scenarios {
		scenario := scenario
		var availability *chaosmetrics.AvailabilityRecorder
		s.Step(fmt.Sprintf("%02d-%s", idx+1, scenario.name), func(ctx context.Context) {
			availability = chaosmetrics.NewAvailabilityRecorder(t, s.Harness, cr, cfg)
			if err := availability.Start(ctx); err != nil {
				t.Fatalf("start availability recorder: %v", err)
			}
			scenario.kill(ctx)
		})

		s.Step(fmt.Sprintf("%02d-wait-recovery", idx+1), func(ctx context.Context) {
			if scenario.name == "kill-master" {
				start := failoverStart
				assert.WaitForMasterChange(t, s.Harness, cr, masterBefore, masterUID, 5*time.Minute)
				scenarioMetrics.RecordDuration("failover.kill_master", time.Since(start))
			}
			updated := manager.WaitReady(ctx, cr, 6*time.Minute)
			*cr = *updated
			assert.MasterService(t, s.Harness, cr, time.Minute)
			if scenario.name == "kill-sentinel" {
				start := time.Now()
				assert.SentinelService(t, s.Harness, cr, time.Minute)
				scenarioMetrics.RecordDuration("quorum.kill_sentinel", time.Since(start))
			} else {
				assert.SentinelService(t, s.Harness, cr, time.Minute)
			}
			assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
			if availability == nil {
				t.Fatalf("availability recorder missing for %s", scenario.name)
			}
			downtime := availability.Stop(ctx)
			scenarioMetrics.RecordDowntime(scenario.name, downtime)
		})
	}
}

func pickSentinelPod(t *testing.T, s *suite.Suite, cr *keyvalv1alpha1.KeyValCluster) *corev1.Pod {
	t.Helper()
	pods := listSentinelPods(t, s, cr)
	return &pods[rand.Intn(len(pods))]
}

func pickReplicaPod(t *testing.T, s *suite.Suite, cr *keyvalv1alpha1.KeyValCluster) *corev1.Pod {
	t.Helper()
	pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
	masterName := cr.Status.MasterPod
	for i := range pods {
		if pods[i].Name != masterName {
			return &pods[i]
		}
	}
	t.Fatalf("no replica pod found")
	return nil
}

func deletePod(ctx context.Context, t *testing.T, s *suite.Suite, name string) {
	t.Helper()
	var pod corev1.Pod
	key := types.NamespacedName{Namespace: s.Harness.Namespace(), Name: name}
	if err := s.Harness.Client().Get(ctx, key, &pod); err != nil {
		t.Fatalf("get pod %s: %v", name, err)
	}
	if err := s.Harness.Client().Delete(ctx, &pod); client.IgnoreNotFound(err) != nil {
		t.Fatalf("delete pod %s: %v", name, err)
	}
}
