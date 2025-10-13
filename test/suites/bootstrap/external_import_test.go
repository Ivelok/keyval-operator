//go:build e2e

package bootstrap

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

const (
	importStatePending   = "Pending"
	importStateFollowing = "Following"
	importStateCompleted = "Completed"
	importStateFailed    = "Failed"
)

func TestExternalImportSnapshot(t *testing.T) {

	s := suite.New(t)
	ns := s.Harness.Namespace()
	image := s.Harness.RedisImage()

	manager := cluster.NewManager(s.Harness)

	sourceBuilder := cluster.NewBuilder(ns, image).
		WithName("import-source").
		WithLabels(map[string]string{"suite": "bootstrap", "scenario": "external-import", "role": "source"}).
		WithEphemeralStorage()

	source := sourceBuilder.Build()

	s.Step("create-source-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, source); err != nil {
			t.Fatalf("apply source cluster: %v", err)
		}
	})

	s.Step("wait-source-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, source, 2*time.Minute)
		*source = *updated
	})

	var seedKey string
	s.Step("seed-source-data", func(ctx context.Context) {
		seedKey = fmt.Sprintf("ext:%d", time.Now().UnixNano())
		cmd := []string{"redis-cli", "SET", seedKey, "seeded-from-source"}
		pod := source.Status.MasterPod
		if pod == "" {
			t.Fatalf("source master pod not populated")
		}
		if _, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...); err != nil {
			t.Fatalf("seed data: %v (stderr=%s)", err, stderr)
		}
	})

	targetBuilder := cluster.NewBuilder(ns, image).
		WithName("import-target").
		WithLabels(map[string]string{"suite": "bootstrap", "scenario": "external-import", "role": "target"}).
		WithReplicas(1).
		WithEphemeralStorage()
	target := targetBuilder.Build()

	address := fmt.Sprintf("redis://%s-master.%s.svc:6379", source.Name, source.Namespace)
	target.Spec.Bootstrap = &keyvalv1alpha1.BootstrapSpec{
		ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{
			Address: address,
		},
	}

	s.Step("create-target-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, target); err != nil {
			t.Fatalf("apply target cluster: %v", err)
		}
	})

	s.Step("wait-target-ready", func(ctx context.Context) {
		s.Harness.WaitForCondition(ctx, target.Name, keyvalv1alpha1.ConditionExternalImport, metav1.ConditionTrue, 3*time.Minute)
		updated := manager.WaitReady(ctx, target, 3*time.Minute)
		*target = *updated
	})

	s.Step("verify-external-import-condition", func(context.Context) {
		cond := findCondition(target.Status.Conditions, keyvalv1alpha1.ConditionExternalImport)
		if cond == nil {
			t.Fatalf("external import condition missing")
		}
		if cond.Status != metav1.ConditionTrue {
			t.Fatalf("external import condition not satisfied: %+v", cond)
		}
		if target.Status.ExternalImport == nil || target.Status.ExternalImport.State != "Completed" {
			t.Fatalf("external import status unexpected: %+v", target.Status.ExternalImport)
		}
	})

	s.Step("assert-seeded-key", func(ctx context.Context) {
		if seedKey == "" {
			t.Fatalf("seed key not recorded")
		}
		cmd := []string{"redis-cli", "GET", seedKey}
		pod := target.Status.MasterPod
		if pod == "" {
			t.Fatalf("target master pod not populated")
		}
		stdout, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...)
		if err != nil {
			t.Fatalf("read imported key: %v (stderr=%s)", err, stderr)
		}
		if strings.TrimSpace(stdout) != "seeded-from-source" {
			t.Fatalf("unexpected value for key %s: %q", seedKey, stdout)
		}
	})

	s.Step("verify-replication-health", func(context.Context) {
		assert.ReplicationHealthy(t, s.Harness, target, time.Minute)
	})
}

func TestExternalImportLiveCutover(t *testing.T) {

	s := suite.New(t)
	ns := s.Harness.Namespace()
	image := s.Harness.RedisImage()

	manager := cluster.NewManager(s.Harness)

	sourceBuilder := cluster.NewBuilder(ns, image).
		WithName("live-source").
		WithLabels(map[string]string{"suite": "bootstrap", "scenario": "external-import-live", "role": "source"}).
		WithEphemeralStorage()

	source := sourceBuilder.Build()

	s.Step("create-source-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, source); err != nil {
			t.Fatalf("apply source cluster: %v", err)
		}
	})

	s.Step("wait-source-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, source, 2*time.Minute)
		*source = *updated
	})

	seedKey := fmt.Sprintf("live:%d", time.Now().UnixNano())
	s.Step("seed-source-initial", func(ctx context.Context) {
		pod := source.Status.MasterPod
		if pod == "" {
			t.Fatalf("source master pod missing")
		}
		cmd := []string{"redis-cli", "SET", seedKey, "seeded-live"}
		if _, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...); err != nil {
			t.Fatalf("seed source data: %v (stderr=%s)", err, stderr)
		}
	})

	targetBuilder := cluster.NewBuilder(ns, image).
		WithName("live-target").
		WithLabels(map[string]string{"suite": "bootstrap", "scenario": "external-import-live", "role": "target"}).
		WithReplicas(1).
		WithEphemeralStorage()
	target := targetBuilder.Build()

	address := fmt.Sprintf("redis://%s-master.%s.svc:6379", source.Name, source.Namespace)
	target.Spec.Bootstrap = &keyvalv1alpha1.BootstrapSpec{
		ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{
			Address:  address,
			SyncMode: keyvalv1alpha1.ExternalSourceSyncModeLive,
		},
	}

	s.Step("create-target-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, target); err != nil {
			t.Fatalf("apply target cluster: %v", err)
		}
	})

	s.Step("wait-target-following", func(ctx context.Context) {
		updated := waitExternalImportState(ctx, t, s.Harness.Client(), ns, target.Name, importStateFollowing, "Following", 3*time.Minute)
		*target = *updated
		if target.Status.MasterPod != "" {
			t.Fatalf("expected no local master while following, got %s", target.Status.MasterPod)
		}
	})

	s.Step("verify-live-replication", func(ctx context.Context) {
		pod := target.Status.MasterPod
		if pod == "" {
			pod = fmt.Sprintf("%s-0", target.Name)
		}
		cmd := []string{"redis-cli", "GET", seedKey}
		stdout, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...)
		if err != nil {
			t.Fatalf("read replicated key: %v (stderr=%s)", err, stderr)
		}
		if strings.TrimSpace(stdout) != "seeded-live" {
			t.Fatalf("unexpected replicated value %q", stdout)
		}

		// Seed another key to ensure ongoing replication while following.
		freshKey := fmt.Sprintf("live:%d", time.Now().UnixNano())
		cmdSet := []string{"redis-cli", "SET", freshKey, "seeded-again"}
		if _, stderr, err := s.Harness.Exec(ctx, source.Status.MasterPod, "redis", cmdSet...); err != nil {
			t.Fatalf("seed follow-up data: %v (stderr=%s)", err, stderr)
		}
		time.Sleep(2 * time.Second)
		cmdGet := []string{"redis-cli", "GET", freshKey}
		stdout, stderr, err = s.Harness.Exec(ctx, pod, "redis", cmdGet...)
		if err != nil {
			t.Fatalf("read replicated follow-up key: %v (stderr=%s)", err, stderr)
		}
		if strings.TrimSpace(stdout) != "seeded-again" {
			t.Fatalf("unexpected value for follow-up key: %q", stdout)
		}
	})

	s.Step("detach-target-cluster", func(ctx context.Context) {
		removeExternalSource(t, s, target.Name)
	})

	s.Step("wait-cutover", func(ctx context.Context) {
		updated := waitExternalImportState(ctx, t, s.Harness.Client(), ns, target.Name, importStateCompleted, "Completed", 3*time.Minute)
		s.Harness.WaitForCondition(ctx, target.Name, keyvalv1alpha1.ConditionReconciled, metav1.ConditionTrue, 3*time.Minute)
		ready := manager.WaitReady(ctx, updated, 3*time.Minute)
		*target = *ready
	})

	s.Step("verify-target-writable", func(ctx context.Context) {
		pod := target.Status.MasterPod
		if pod == "" {
			t.Fatalf("target master pod not populated after cutover")
		}
		cmd := []string{"redis-cli", "SET", "live-cutover", "done"}
		if _, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...); err != nil {
			t.Fatalf("write after cutover failed: %v (stderr=%s)", err, stderr)
		}
	})
}

func TestExternalImportSentinelSnapshot(t *testing.T) {

	s := suite.New(t)
	ns := s.Harness.Namespace()
	image := s.Harness.RedisImage()
	manager := cluster.NewManager(s.Harness)

	source := cluster.NewBuilder(ns, image).
		WithName("sentinel-source").
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage().
		Build()

	s.Step("create-sentinel-source", func(ctx context.Context) {
		if err := manager.Apply(ctx, source); err != nil {
			t.Fatalf("apply sentinel source: %v", err)
		}
	})

	s.Step("wait-sentinel-source-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, source, 4*time.Minute)
		*source = *updated
		waitSentinelPodsReady(t, s, source.Name, 3)
	})

	var seedKey string
	s.Step("seed-sentinel-source", func(ctx context.Context) {
		seedKey = fmt.Sprintf("sentinel:%d", time.Now().UnixNano())
		pod := source.Status.MasterPod
		if pod == "" {
			t.Fatalf("sentinel source master pod missing")
		}
		cmd := []string{"redis-cli", "SET", seedKey, "sentinel-seed"}
		if _, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...); err != nil {
			t.Fatalf("seed sentinel data: %v (stderr=%s)", err, stderr)
		}
	})

	target := cluster.NewBuilder(ns, image).
		WithName("sentinel-target").
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage().
		Build()

	address := fmt.Sprintf("redis://%s-master.%s.svc:6379", source.Name, source.Namespace)
	target.Spec.Bootstrap = &keyvalv1alpha1.BootstrapSpec{
		ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{Address: address, SyncMode: keyvalv1alpha1.ExternalSourceSyncModeSnapshot},
	}

	s.Step("create-sentinel-target", func(ctx context.Context) {
		if err := manager.Apply(ctx, target); err != nil {
			t.Fatalf("apply sentinel target: %v", err)
		}
	})

	s.Step("wait-sentinel-target-ready", func(ctx context.Context) {
		s.Harness.WaitForCondition(ctx, target.Name, keyvalv1alpha1.ConditionExternalImport, metav1.ConditionTrue, 4*time.Minute)
		updated := manager.WaitReady(ctx, target, 4*time.Minute)
		*target = *updated
	})

	s.Step("verify-sentinel-completion", func(context.Context) {
		cond := findCondition(target.Status.Conditions, keyvalv1alpha1.ConditionExternalImport)
		if cond == nil || cond.Status != metav1.ConditionTrue {
			t.Fatalf("external import condition unexpected: %+v", cond)
		}
		if target.Status.ExternalImport == nil || target.Status.ExternalImport.State != importStateCompleted {
			t.Fatalf("external import state unexpected: %+v", target.Status.ExternalImport)
		}
		waitSentinelPodsReady(t, s, target.Name, 3)
		assert.MasterService(t, s.Harness, target, 2*time.Minute)
	})

	s.Step("validate-sentinel-data", func(ctx context.Context) {
		pod := target.Status.MasterPod
		if pod == "" {
			t.Fatalf("sentinel target master pod missing")
		}
		cmd := []string{"redis-cli", "GET", seedKey}
		stdout, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...)
		if err != nil {
			t.Fatalf("read imported sentinel key: %v (stderr=%s)", err, stderr)
		}
		if strings.TrimSpace(stdout) != "sentinel-seed" {
			t.Fatalf("unexpected sentinel value %q", stdout)
		}
	})
}

func TestExternalImportSentinelLiveCutover(t *testing.T) {

	s := suite.New(t)
	ns := s.Harness.Namespace()
	image := s.Harness.RedisImage()
	manager := cluster.NewManager(s.Harness)

	source := cluster.NewBuilder(ns, image).
		WithName("sentinel-live-source").
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage().
		Build()

	s.Step("create-sentinel-live-source", func(ctx context.Context) {
		if err := manager.Apply(ctx, source); err != nil {
			t.Fatalf("apply sentinel live source: %v", err)
		}
	})

	s.Step("wait-sentinel-live-source-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, source, 4*time.Minute)
		*source = *updated
		waitSentinelPodsReady(t, s, source.Name, 3)
	})

	seedKey := fmt.Sprintf("sentinel-live:%d", time.Now().UnixNano())
	s.Step("seed-sentinel-live", func(ctx context.Context) {
		pod := source.Status.MasterPod
		if pod == "" {
			t.Fatalf("sentinel live source master missing")
		}
		cmd := []string{"redis-cli", "SET", seedKey, "sentinel-live"}
		if _, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...); err != nil {
			t.Fatalf("seed sentinel live data: %v (stderr=%s)", err, stderr)
		}
	})

	target := cluster.NewBuilder(ns, image).
		WithName("sentinel-live-target").
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage().
		Build()

	address := fmt.Sprintf("redis://%s-master.%s.svc:6379", source.Name, source.Namespace)
	target.Spec.Bootstrap = &keyvalv1alpha1.BootstrapSpec{
		ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{
			Address:  address,
			SyncMode: keyvalv1alpha1.ExternalSourceSyncModeLive,
		},
	}

	s.Step("create-sentinel-live-target", func(ctx context.Context) {
		if err := manager.Apply(ctx, target); err != nil {
			t.Fatalf("apply sentinel live target: %v", err)
		}
	})

	s.Step("wait-following", func(ctx context.Context) {
		updated := waitExternalImportState(ctx, t, s.Harness.Client(), ns, target.Name, importStateFollowing, "Following", 4*time.Minute)
		*target = *updated
		waitSentinelPodsReady(t, s, target.Name, 3)
	})

	s.Step("validate-sentinel-following", func(ctx context.Context) {
		pod := fmt.Sprintf("%s-0", target.Name)
		cmd := []string{"redis-cli", "GET", seedKey}
		stdout, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...)
		if err != nil {
			t.Fatalf("read sentinel following key: %v (stderr=%s)", err, stderr)
		}
		if strings.TrimSpace(stdout) != "sentinel-live" {
			t.Fatalf("unexpected sentinel following value %q", stdout)
		}
	})

	s.Step("detach-sentinel-live", func(ctx context.Context) {
		removeExternalSource(t, s, target.Name)
	})

	s.Step("wait-sentinel-live-cutover", func(ctx context.Context) {
		updated := waitExternalImportState(ctx, t, s.Harness.Client(), ns, target.Name, importStateCompleted, "Completed", 4*time.Minute)
		*target = *updated
		s.Harness.WaitForCondition(ctx, target.Name, keyvalv1alpha1.ConditionReconciled, metav1.ConditionTrue, 4*time.Minute)
		waitSentinelPodsReady(t, s, target.Name, 3)
	})

	s.Step("write-after-cutover", func(ctx context.Context) {
		pod := target.Status.MasterPod
		if pod == "" {
			t.Fatalf("sentinel live master pod missing after cutover")
		}
		cmd := []string{"redis-cli", "SET", "sentinel-live-cutover", "ok"}
		if _, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...); err != nil {
			t.Fatalf("sentinel live write failed: %v (stderr=%s)", err, stderr)
		}
	})
}

func TestExternalImportSourceUnavailable(t *testing.T) {

	s := suite.New(t)
	ns := s.Harness.Namespace()
	image := s.Harness.RedisImage()
	manager := cluster.NewManager(s.Harness)

	target := cluster.NewBuilder(ns, image).
		WithName("import-unreachable").
		WithEphemeralStorage().
		Build()

	target.Spec.Bootstrap = &keyvalv1alpha1.BootstrapSpec{
		ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{
			Address:  "redis://no-such-host.keyval-e2e.svc:6379",
			SyncMode: keyvalv1alpha1.ExternalSourceSyncModeSnapshot,
		},
	}

	s.Step("create-unreachable-target", func(ctx context.Context) {
		if err := manager.Apply(ctx, target); err != nil {
			t.Fatalf("apply unreachable target: %v", err)
		}
	})

	s.Step("observe-source-unavailable", func(ctx context.Context) {
		updated := waitExternalImportReason(ctx, t, s.Harness.Client(), ns, target.Name, "SourceUnavailable", 90*time.Second)
		if updated.Status.ExternalImport == nil || updated.Status.ExternalImport.State != importStatePending {
			t.Fatalf("expected pending state for unreachable source, got %+v", updated.Status.ExternalImport)
		}
	})
}

func TestExternalImportTimeout(t *testing.T) {

	s := suite.New(t)
	ns := s.Harness.Namespace()
	image := s.Harness.RedisImage()
	manager := cluster.NewManager(s.Harness)
	ctx := s.Context()

	const authPassword = "P@s$w0rd"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "import-auth", Namespace: ns},
		StringData: map[string]string{"password": authPassword},
	}
	if err := s.Harness.Client().Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			t.Fatalf("create auth secret: %v", err)
		}
		existing := &corev1.Secret{}
		if err := s.Harness.Client().Get(ctx, types.NamespacedName{Name: secret.Name, Namespace: ns}, existing); err != nil {
			t.Fatalf("get existing auth secret: %v", err)
		}
		if existing.Data == nil {
			existing.Data = make(map[string][]byte, 1)
		}
		if string(existing.Data["password"]) != authPassword {
			existing.Data["password"] = []byte(authPassword)
			if err := s.Harness.Client().Update(ctx, existing); err != nil {
				t.Fatalf("update auth secret: %v", err)
			}
		}
	}
	t.Cleanup(func() {
		_ = s.Harness.Client().Delete(context.Background(), secret)
	})

	source := cluster.NewBuilder(ns, image).
		WithName("timeout-source").
		WithEphemeralStorage().
		Build()
	source.Spec.Security = &keyvalv1alpha1.SecuritySpec{
		Auth: &keyvalv1alpha1.AuthSpec{
			Enabled:           true,
			PasswordSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name}, Key: "password"},
		},
	}

	s.Step("create-timeout-source", func(ctx context.Context) {
		if err := manager.Apply(ctx, source); err != nil {
			t.Fatalf("apply timeout source: %v", err)
		}
	})

	s.Step("wait-timeout-source-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, source, 2*time.Minute)
		*source = *updated
	})

	s.Step("seed-timeout-source", func(ctx context.Context) {
		pod := source.Status.MasterPod
		if pod == "" {
			t.Fatalf("timeout source master pod missing")
		}
		cmd := []string{"bash", "-lc", "for i in $(seq 1 6000); do redis-cli SET bulk:$i value-$i >/dev/null; done"}
		if _, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...); err != nil {
			t.Fatalf("seed timeout source data: %v (stderr=%s)", err, stderr)
		}
	})

	target := cluster.NewBuilder(ns, image).
		WithName("timeout-target").
		WithEphemeralStorage().
		Build()

	maxDuration := metav1.Duration{Duration: 5 * time.Second}
	target.Spec.Bootstrap = &keyvalv1alpha1.BootstrapSpec{
		ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{
			Address:                fmt.Sprintf("redis://%s-master.%s.svc:6379", source.Name, source.Namespace),
			SyncMode:               keyvalv1alpha1.ExternalSourceSyncModeSnapshot,
			MaxInitialSyncDuration: &maxDuration,
			Auth: &keyvalv1alpha1.ExternalSourceAuthSpec{
				PasswordSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name}, Key: "password"},
			},
		},
	}

	s.Step("create-timeout-target", func(ctx context.Context) {
		if err := manager.Apply(ctx, target); err != nil {
			t.Fatalf("apply timeout target: %v", err)
		}
	})

	s.Step("wait-timeout", func(ctx context.Context) {
		updated := waitExternalImportReason(ctx, t, s.Harness.Client(), ns, target.Name, "Timeout", 2*time.Minute)
		if updated.Status.ExternalImport == nil || updated.Status.ExternalImport.State != importStateFailed {
			t.Fatalf("expected failed state after timeout, got %+v", updated.Status.ExternalImport)
		}
	})
}

func TestExternalImportReplicaOfError(t *testing.T) {

	s := suite.New(t)
	ns := s.Harness.Namespace()
	image := s.Harness.RedisImage()
	manager := cluster.NewManager(s.Harness)

	source := cluster.NewBuilder(ns, image).
		WithName("replicaof-error-source").
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage().
		Build()

	s.Step("create-replicaof-error-source", func(ctx context.Context) {
		if err := manager.Apply(ctx, source); err != nil {
			t.Fatalf("apply replicaof error source: %v", err)
		}
	})

	s.Step("wait-replicaof-error-source-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, source, 3*time.Minute)
		*source = *updated
	})

	target := cluster.NewBuilder(ns, image).
		WithName("replicaof-error-target").
		WithEphemeralStorage().
		Build()
	target.Spec.RedisConfig = map[string]string{"rename-command": "REPLICAOF disabled"}

	sentinelAddress := fmt.Sprintf("redis://%s-sentinel.%s.svc:26379", source.Name, source.Namespace)
	shortDuration := metav1.Duration{Duration: 30 * time.Second}
	target.Spec.Bootstrap = &keyvalv1alpha1.BootstrapSpec{
		ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{
			Address:                sentinelAddress,
			SyncMode:               keyvalv1alpha1.ExternalSourceSyncModeSnapshot,
			MaxInitialSyncDuration: &shortDuration,
		},
	}

	s.Step("create-replicaof-error-target", func(ctx context.Context) {
		if err := manager.Apply(ctx, target); err != nil {
			t.Fatalf("apply replicaof error target: %v", err)
		}
	})

	s.Step("wait-replicaof-error", func(ctx context.Context) {
		waitExternalImportReason(ctx, t, s.Harness.Client(), ns, target.Name, "ReplicaOf", 2*time.Minute)
	})
}

func TestExternalImportNoOneError(t *testing.T) {

	s := suite.New(t)
	ns := s.Harness.Namespace()
	image := s.Harness.RedisImage()
	manager := cluster.NewManager(s.Harness)

	source := cluster.NewBuilder(ns, image).
		WithName("noone-error-source").
		WithEphemeralStorage().
		Build()

	s.Step("create-noone-error-source", func(ctx context.Context) {
		if err := manager.Apply(ctx, source); err != nil {
			t.Fatalf("apply source: %v", err)
		}
	})

	s.Step("wait-noone-error-source-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, source, 2*time.Minute)
		*source = *updated
	})

	target := cluster.NewBuilder(ns, image).
		WithName("noone-error-target").
		WithEphemeralStorage().
		Build()

	address := fmt.Sprintf("redis://%s-master.%s.svc:6379", source.Name, source.Namespace)
	target.Spec.Bootstrap = &keyvalv1alpha1.BootstrapSpec{
		ExternalSource: &keyvalv1alpha1.ExternalSourceSpec{
			Address:  address,
			SyncMode: keyvalv1alpha1.ExternalSourceSyncModeLive,
		},
	}

	s.Step("create-noone-error-target", func(ctx context.Context) {
		if err := manager.Apply(ctx, target); err != nil {
			t.Fatalf("apply target: %v", err)
		}
	})

	s.Step("wait-following", func(ctx context.Context) {
		updated := waitExternalImportState(ctx, t, s.Harness.Client(), ns, target.Name, importStateFollowing, "Following", 2*time.Minute)
		*target = *updated
	})

	s.Step("revoke-replicaof-acl", func(ctx context.Context) {
		pod := fmt.Sprintf("%s-0", target.Name)
		cmd := []string{"redis-cli", "ACL", "SETUSER", "default", "+@all", "-replicaof"}
		if _, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...); err != nil {
			t.Fatalf("revoke replicaof acl: %v (stderr=%s)", err, stderr)
		}
	})

	s.Step("detach-external-source", func(ctx context.Context) {
		removeExternalSource(t, s, target.Name)
	})

	s.Step("wait-promote-error", func(ctx context.Context) {
		waitExternalImportReason(ctx, t, s.Harness.Client(), ns, target.Name, "Promote", 2*time.Minute)
	})

	s.Step("restore-replicaof-acl", func(ctx context.Context) {
		pod := fmt.Sprintf("%s-0", target.Name)
		cmd := []string{"redis-cli", "ACL", "SETUSER", "default", "+@all"}
		if _, stderr, err := s.Harness.Exec(ctx, pod, "redis", cmd...); err != nil {
			t.Fatalf("restore replicaof acl: %v (stderr=%s)", err, stderr)
		}
	})

	s.Step("wait-completed-after-retry", func(ctx context.Context) {
		updated := waitExternalImportState(ctx, t, s.Harness.Client(), ns, target.Name, importStateCompleted, "Completed", 3*time.Minute)
		*target = *updated
	})
}

func findCondition(conds []metav1.Condition, typ keyvalv1alpha1.ConditionType) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == string(typ) {
			return &conds[i]
		}
	}
	return nil
}

func waitExternalImportState(ctx context.Context, t *testing.T, cl client.Client, namespace, name, state, reason string, timeout time.Duration) *keyvalv1alpha1.KeyValCluster {
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	key := types.NamespacedName{Namespace: namespace, Name: name}
	var latest keyvalv1alpha1.KeyValCluster
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		if err := cl.Get(ctx, key, &latest); err != nil {
			return false, err
		}
		st := latest.Status.ExternalImport
		if st == nil || st.State != state {
			return false, nil
		}
		cond := findCondition(latest.Status.Conditions, keyvalv1alpha1.ConditionExternalImport)
		if cond == nil {
			return false, nil
		}
		if cond.Reason != reason {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("wait for external import state=%s reason=%s: %v", state, reason, err)
	}
	return latest.DeepCopy()
}

func waitExternalImportReason(ctx context.Context, t *testing.T, cl client.Client, namespace, name, reason string, timeout time.Duration) *keyvalv1alpha1.KeyValCluster {
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	key := types.NamespacedName{Namespace: namespace, Name: name}
	var latest keyvalv1alpha1.KeyValCluster
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		if err := cl.Get(ctx, key, &latest); err != nil {
			return false, err
		}
		cond := findCondition(latest.Status.Conditions, keyvalv1alpha1.ConditionExternalImport)
		if cond == nil {
			return false, nil
		}
		return cond.Reason == reason, nil
	})
	if err != nil {
		t.Fatalf("wait for external import reason=%s: %v", reason, err)
	}
	return latest.DeepCopy()
}

func waitSentinelPodsReady(t *testing.T, s *suite.Suite, clusterName string, count int) {
	t.Helper()
	ctx := s.Context()
	selector := labels.Set{"app": fmt.Sprintf("%s-sentinel", clusterName)}
	var pods corev1.PodList
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := s.Harness.Client().List(ctx, &pods, client.InNamespace(s.Harness.Namespace()), client.MatchingLabels(selector)); err != nil {
			return false, err
		}
		if len(pods.Items) != count {
			return false, nil
		}
		for i := range pods.Items {
			if !podReady(&pods.Items[i]) {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait for sentinel pods ready: %v", err)
	}
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for i := range pod.Status.Conditions {
		cond := pod.Status.Conditions[i]
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func removeExternalSource(t *testing.T, s *suite.Suite, name string) {
	t.Helper()
	ctx := s.Context()
	key := types.NamespacedName{Namespace: s.Harness.Namespace(), Name: name}
	var latest keyvalv1alpha1.KeyValCluster
	if err := s.Harness.Client().Get(ctx, key, &latest); err != nil {
		t.Fatalf("get cluster %s: %v", name, err)
	}
	base := latest.DeepCopy()
	latest.Spec.Bootstrap = nil
	if err := s.Harness.Client().Patch(ctx, &latest, client.MergeFrom(base)); err != nil {
		t.Fatalf("remove external source: %v", err)
	}
}
