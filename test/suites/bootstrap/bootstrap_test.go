//go:build e2e

package bootstrap

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/harness"
	"github.com/ivelok/keyval-operator/test/internal/suite"
	"github.com/ivelok/keyval-operator/test/internal/versions"
)

func TestStandaloneBootstrapEngines(t *testing.T) {
	t.Parallel()

	for _, tc := range versions.Engines() {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			s := suite.New(t)

			builder := cluster.NewBuilder(s.Harness.Namespace(), tc.Image).
				WithName(fmt.Sprintf("standalone-%s", tc.Name)).
				WithLabels(map[string]string{"suite": "bootstrap", "engine": string(tc.Engine), "mode": "standalone"}).
				WithEngine(tc.Engine).
				WithImage(tc.Image).
				WithEphemeralStorage()
			cr := builder.Build()

			manager := cluster.NewManager(s.Harness)

			s.Step("create-cluster", func(ctx context.Context) {
				if err := manager.Apply(ctx, cr); err != nil {
					t.Fatalf("apply cluster: %v", err)
				}
			})

			s.Step("wait-ready", func(ctx context.Context) {
				updated := manager.WaitReady(ctx, cr, 2*time.Minute)
				if updated.Status.MasterPod == "" {
					t.Fatalf("expected masterPod to be populated")
				}
				*cr = *updated
			})

			s.Step("verify-master-service", func(ctx context.Context) {
				assert.MasterService(t, s.Harness, cr, 45*time.Second)
			})

			s.Step("verify-replication", func(context.Context) {
				assert.ReplicationHealthy(t, s.Harness, cr, 1*time.Minute)
			})

			s.Step("verify-statefulset-image", func(context.Context) {
				assertStatefulSetImage(t, s, cr, tc.Image, 45*time.Second)
			})

			s.Step("log-pod-distribution", func(context.Context) {
				assert.LogPodDistribution(t, s.Harness, cr, 30*time.Second)
			})
		})
	}
}

func assertStatefulSetImage(t *testing.T, s *suite.Suite, cluster *keyvalv1alpha1.KeyValCluster, expectedImage string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Context(), timeout)
	defer cancel()

	name := cluster.Name
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: name}
	var sts appsv1.StatefulSet
	if err := s.Harness.Client().Get(ctx, key, &sts); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}
	if len(sts.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("statefulset has no containers")
	}
	if image := sts.Spec.Template.Spec.Containers[0].Image; image != expectedImage {
		t.Fatalf("statefulset image=%s, expected %s", image, expectedImage)
	}
}

func quorumSummary(conditions []metav1.Condition) string {
	for _, cond := range conditions {
		if cond.Type == string(keyvalv1alpha1.ConditionSentinelQuorum) {
			return fmt.Sprintf("%s/%s", cond.Status, cond.Reason)
		}
	}
	return "unknown"
}

func waitMasterService(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.Context(), timeout)
	defer cancel()

	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var latest keyvalv1alpha1.KeyValCluster
		if err := h.Client().Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, &latest); err != nil {
			return false, err
		}
		*cluster = latest
		if cluster.Status.MasterPod == "" {
			return false, nil
		}
		svc := h.WaitForService(ctx, fmt.Sprintf("%s-master", cluster.Name), 30*time.Second)
		if svc.Spec.Selector["role"] != string(keyvalv1alpha1.PodRoleMaster) {
			return false, nil
		}
		var pods corev1.PodList
		if err := h.Client().List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(labels.Set{"role": string(keyvalv1alpha1.PodRoleMaster), "keyvalcluster": cluster.Name})); err != nil {
			return false, err
		}
		if len(pods.Items) != 1 {
			return false, nil
		}
		pod := pods.Items[0]
		if pod.Name != cluster.Status.MasterPod {
			return false, nil
		}
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("wait for master service update: %v", err)
	}
}

func TestSentinelBootstrapEngines(t *testing.T) {
	t.Parallel()

	for _, tc := range versions.Engines() {
		tc := tc
		name := fmt.Sprintf("%s-sentinel", tc.Name)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := suite.New(t)
			scaler := cluster.NewScaler(s.Harness)

			builder := cluster.NewBuilder(s.Harness.Namespace(), tc.Image).
				WithName(fmt.Sprintf("sentinel-%s", tc.Name)).
				WithLabels(map[string]string{"suite": "bootstrap", "engine": string(tc.Engine), "mode": "sentinel"}).
				WithMode(keyvalv1alpha1.ModeSentinel).
				WithEngine(tc.Engine).
				WithImage(tc.Image).
				WithReplicas(3).
				WithSentinelCount(3).
				WithEphemeralStorage()
			cr := builder.Build()

			manager := cluster.NewManager(s.Harness)

			s.Step("create-cluster", func(ctx context.Context) {
				if err := manager.Apply(ctx, cr); err != nil {
					t.Fatalf("apply sentinel cluster: %v", err)
				}
			})

			s.Step("wait-ready", func(ctx context.Context) {
				updated := manager.WaitReady(ctx, cr, 3*time.Minute)
				if updated.Status.MasterPod == "" {
					t.Fatalf("expected masterPod to be populated")
				}
				*cr = *updated
			})

			s.Step("verify-master-service", func(context.Context) {
				assert.MasterService(t, s.Harness, cr, 1*time.Minute)
			})

			s.Step("verify-redis-statefulset", func(context.Context) {
				assertStatefulSetImage(t, s, cr, tc.Image, 1*time.Minute)
			})

			s.Step("verify-replication", func(context.Context) {
				assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
			})

			s.Step("verify-sentinel-service", func(context.Context) {
				assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
			})

			s.Step("verify-sentinel-statefulset", func(context.Context) {
				assert.SentinelStatefulSet(t, s.Harness, cr, tc.Image, 2*time.Minute)
			})

			s.Step("log-pod-distribution", func(context.Context) {
				assert.LogPodDistribution(t, s.Harness, cr, 30*time.Second)
			})

			s.Step("wait-sentinel-quorum", func(ctx context.Context) {
				updated := s.Harness.WaitForCondition(ctx, cr.Name, keyvalv1alpha1.ConditionSentinelQuorum, metav1.ConditionTrue, 3*time.Minute)
				*cr = *updated
			})

			quorum := quorumSummary(cr.Status.Conditions)
			t.Logf("pre-failover master=%s ready=%d quorum=%s", cr.Status.MasterPod, cr.Status.ReadyReplicas, quorum)
			master := assert.MasterPod(t, s.Harness, cr)
			oldMaster := master.Name
			oldMasterUID := master.UID
			s.Step("delete-master-pod", func(ctx context.Context) {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: oldMaster, Namespace: s.Harness.Namespace()}}
				if err := s.Harness.Client().Delete(ctx, pod); err != nil {
					t.Fatalf("delete master pod: %v", err)
				}
			})

			s.Step("wait-failover", func(ctx context.Context) {
				assert.WaitForMasterChange(t, s.Harness, cr, oldMaster, oldMasterUID, 5*time.Minute)
				waitMasterService(t, s.Harness, cr, 1*time.Minute)
				assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
				assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
			})

			s.Step("scale-up-5", func(ctx context.Context) {
				updated, err := scaler.Scale(ctx, cr, 5, 4*time.Minute)
				if err != nil {
					t.Fatalf("scale up redis replicas: %v", err)
				}
				*cr = *updated
				assert.RedisPodOrdinals(t, s.Harness, cr, 5)
				assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
			})

			s.Step("scale-down-3", func(ctx context.Context) {
				updated, err := scaler.Scale(ctx, cr, 3, 4*time.Minute)
				if err != nil {
					t.Fatalf("scale down redis replicas: %v", err)
				}
				*cr = *updated
				assert.RedisPodOrdinals(t, s.Harness, cr, 3)
				assert.ReplicationHealthy(t, s.Harness, cr, 2*time.Minute)
				assert.SentinelService(t, s.Harness, cr, 1*time.Minute)
			})
		})
	}
}
