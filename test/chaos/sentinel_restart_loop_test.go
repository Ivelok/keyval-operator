//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"testing"
	"time"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/chaosmetrics"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

func TestSentinelSentinelRestartLoop(t *testing.T) {
	cfg := chaosmetrics.LoadConfig(t)
	chaosmetrics.SkipUnless(t, cfg, "sentinel-restart-loop")

	s := suite.New(t)

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName(fmt.Sprintf("csl-%s", uniqueSuffix())).
		WithLabels(map[string]string{"suite": "chaos", "scenario": "sentinel-restart-loop"}).
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage()
	cr := builder.Build()

	manager := cluster.NewManager(s.Harness)
	scenarioMetrics := chaosmetrics.NewScenario(t, s.Harness, cr, cfg, "sentinel-restart-loop")
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

	s.Step("restart-loop", func(ctx context.Context) {
		availability := chaosmetrics.NewAvailabilityRecorder(t, s.Harness, cr, cfg)
		if err := availability.Start(ctx); err != nil {
			t.Fatalf("start availability recorder: %v", err)
		}

		var maxQuorum time.Duration
		for i := 0; i < cfg.Iterations; i++ {
			pods := listSentinelPods(t, s, cr)
			for _, pod := range pods {
				deletePod(ctx, t, s, pod.Name)
				start := time.Now()
				assert.SentinelService(t, s.Harness, cr, time.Minute)
				quorumDuration := time.Since(start)
				if quorumDuration > maxQuorum {
					maxQuorum = quorumDuration
				}
				updated := manager.WaitReady(ctx, cr, 6*time.Minute)
				*cr = *updated
				assert.MasterService(t, s.Harness, cr, time.Minute)
				assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
			}
		}

		downtime := availability.Stop(ctx)
		scenarioMetrics.RecordDowntime("sentinel-restart-loop", downtime)
		if maxQuorum > 0 {
			scenarioMetrics.RecordDuration("quorum.sentinel_restart_loop", maxQuorum)
		}
	})
}
