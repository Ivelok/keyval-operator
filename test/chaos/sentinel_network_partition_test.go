//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/chaosmetrics"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

func TestSentinelNetworkPartition(t *testing.T) {
	cfg := chaosmetrics.LoadConfig(t)
	chaosmetrics.SkipUnless(t, cfg, "sentinel-network-partition")

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
	scenarioMetrics := chaosmetrics.NewScenario(t, s.Harness, cr, cfg, "sentinel-network-partition")
	defer scenarioMetrics.Finish()

	var (
		masterBefore string
		masterUID    types.UID
		availability *chaosmetrics.AvailabilityRecorder
		policyName   string
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
	})

	s.Step("apply-network-policy", func(ctx context.Context) {
		availability = chaosmetrics.NewAvailabilityRecorder(t, s.Harness, cr, cfg)
		if err := availability.Start(ctx); err != nil {
			t.Fatalf("start availability recorder: %v", err)
		}
		name, err := applyCiliumPartition(ctx, s, cr, masterBefore)
		if err != nil {
			if apierrors.IsNotFound(err) {
				t.Skipf("cilium network policy CRD unavailable: %v", err)
			}
			t.Fatalf("apply cilium network policy: %v", err)
		}
		policyName = name
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = removeCiliumPartition(cleanupCtx, s, policyName, cr.Namespace)
		})
	})

	s.Step("wait-failover", func(ctx context.Context) {
		start := time.Now()
		assert.WaitForMasterChange(t, s.Harness, cr, masterBefore, masterUID, 5*time.Minute)
		scenarioMetrics.RecordDuration("failover.network_partition", time.Since(start))
	})

	s.Step("heal-partition", func(ctx context.Context) {
		if policyName == "" {
			t.Fatalf("cilium policy name missing")
		}
		if err := removeCiliumPartition(ctx, s, policyName, cr.Namespace); err != nil {
			t.Fatalf("remove cilium network policy: %v", err)
		}
	})

	s.Step("verify-recovery", func(ctx context.Context) {
		assert.ReplicationHealthy(t, s.Harness, cr, 3*time.Minute)
		start := time.Now()
		assert.SentinelService(t, s.Harness, cr, time.Minute)
		scenarioMetrics.RecordDuration("quorum.network_partition", time.Since(start))
		if availability == nil {
			t.Fatalf("availability recorder missing for network partition")
		}
		downtime := availability.Stop(ctx)
		scenarioMetrics.RecordDowntime("network-partition", downtime)
	})
}

func applyCiliumPartition(ctx context.Context, s *suite.Suite, cr *keyvalv1alpha1.KeyValCluster, masterName string) (string, error) {
	if cr == nil {
		return "", fmt.Errorf("cluster is nil")
	}
	if masterName == "" {
		return "", fmt.Errorf("master name is empty")
	}
	cfg := s.Harness.RestConfig()
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("build dynamic client: %w", err)
	}
	name := ciliumPolicyName(cr.Name)
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "cilium.io/v2",
			"kind":       "CiliumNetworkPolicy",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": cr.Namespace,
				"labels": map[string]interface{}{
					"keyvalcluster": cr.Name,
					"scenario":      "sentinel-network-partition",
				},
			},
			"spec": map[string]interface{}{
				"endpointSelector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						"keyvalcluster": cr.Name,
						"role":          "master",
					},
				},
				"ingress": []interface{}{
					map[string]interface{}{
						"fromEntities": []interface{}{"all"},
						"toPorts": []interface{}{
							map[string]interface{}{
								"ports": []interface{}{
									map[string]interface{}{
										"port":     "6379",
										"protocol": "TCP",
									},
								},
							},
						},
					},
				},
				"ingressDeny": []interface{}{
					map[string]interface{}{
						"fromEndpoints": []interface{}{
							map[string]interface{}{
								"matchLabels": map[string]interface{}{
									"keyvalcluster": cr.Name,
									"role":          "sentinel",
								},
							},
						},
						"toPorts": []interface{}{
							map[string]interface{}{
								"ports": []interface{}{
									map[string]interface{}{
										"port":     "6379",
										"protocol": "TCP",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	gvr := schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}
	_, err = dyn.Resource(gvr).Namespace(cr.Namespace).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	return name, nil
}

func removeCiliumPartition(ctx context.Context, s *suite.Suite, name, namespace string) error {
	cfg := s.Harness.RestConfig()
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("build dynamic client: %w", err)
	}
	gvr := schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}
	err = dyn.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func uniqueSuffix() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func ciliumPolicyName(cluster string) string {
	base := fmt.Sprintf("kv-%s-block-master", cluster)
	suffix := uniqueSuffix()
	maxBase := 63 - 1 - len(suffix)
	if maxBase < 1 {
		maxBase = 1
	}
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	base = strings.Trim(base, "-")
	if base == "" {
		base = "kv-block-master"
	}
	return fmt.Sprintf("%s-%s", base, suffix)
}
