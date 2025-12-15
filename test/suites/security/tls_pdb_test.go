//go:build e2e

package security

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/assert"
	"github.com/ivelok/keyval-operator/test/internal/cluster"
	"github.com/ivelok/keyval-operator/test/internal/suite"
)

const (
	annotationUpdateBlocked = "keyval.ivelok.io/update-blocked"
	blockedReasonPDB        = "PDBLimit"
)

func TestTLSPDBEviction(t *testing.T) {
	t.Parallel()

	s := suite.New(t)
	clusterName := fmt.Sprintf("security-tls-pdb-%d", time.Now().UnixNano())

	caPEM, caKeyPEM, certPEM, keyPEM := generateTLSMaterial(t, clusterName, s.Harness.Namespace())
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("tls-secret-%d", time.Now().UnixNano()),
			Namespace: s.Harness.Namespace(),
		},
		Data: map[string][]byte{
			"ca.crt":  caPEM,
			"tls.crt": certPEM,
			"tls.key": keyPEM,
		},
	}
	createSecret(t, s.Harness, secret)
	t.Cleanup(func() {
		_ = s.Harness.Client().Delete(context.Background(), secret)
	})

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage()).
		WithName(clusterName).
		WithLabels(map[string]string{"suite": "security", "feature": "tls-pdb"}).
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage()
	cr := builder.Build()
	cr.Spec.Security = &keyvalv1alpha1.SecuritySpec{
		TLS: &keyvalv1alpha1.TLSSpec{
			Enabled:          true,
			SecretName:       secret.Name,
			DisablePlaintext: ptr.To(true),
		},
	}

	manager := cluster.NewManager(s.Harness)
	var (
		initialUIDs    map[string]types.UID
		initialTLSHash string
		pdbName        = fmt.Sprintf("%s-redis", cr.Name)
	)

	s.Step("create-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply cluster: %v", err)
		}
	})

	s.Step("wait-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 6*time.Minute)
		*cr = *updated
		assert.SentinelService(t, s.Harness, cr, 2*time.Minute)
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
		initialUIDs = make(map[string]types.UID, len(pods))
		for _, pod := range pods {
			initialUIDs[pod.Name] = pod.UID
		}
		var ss appsv1.StatefulSet
		if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, &ss); err != nil {
			t.Fatalf("get redis statefulset: %v", err)
		}
		initialTLSHash = ss.Spec.Template.Annotations[tlsSecretHashAnnotation]
		if initialTLSHash == "" {
			t.Fatalf("expected initial tls hash annotation to be set")
		}
	})

	s.Step("tighten-pdb", func(ctx context.Context) {
		var pdb policyv1.PodDisruptionBudget
		if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: pdbName}, &pdb); err != nil {
			t.Fatalf("get redis pdb: %v", err)
		}
		patched := pdb.DeepCopy()
		min := intstr.FromInt(int(cr.Spec.RedisReplicas))
		patched.Spec.MinAvailable = &min
		patched.Spec.MaxUnavailable = nil
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(&pdb)); err != nil {
			t.Fatalf("tighten pdb: %v", err)
		}
	})

	s.Step("trigger-rotation-blocked", func(ctx context.Context) {
		var newCertPEM, newKeyPEM []byte
		caPEM, caKeyPEM, newCertPEM, newKeyPEM = generateTLSMaterialWithExistingCA(t, clusterName, s.Harness.Namespace(), nil, caPEM, caKeyPEM)
		var currentSecret corev1.Secret
		if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: secret.Namespace, Name: secret.Name}, &currentSecret); err != nil {
			t.Fatalf("get tls secret: %v", err)
		}
		patched := currentSecret.DeepCopy()
		patched.Data["ca.crt"] = caPEM
		patched.Data["tls.crt"] = newCertPEM
		patched.Data["tls.key"] = newKeyPEM
		if err := s.Harness.Client().Patch(ctx, patched, client.MergeFrom(&currentSecret)); err != nil {
			t.Fatalf("patch tls secret: %v", err)
		}

		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 4*time.Minute, true, func(ctx context.Context) (bool, error) {
			var updated keyvalv1alpha1.KeyValCluster
			if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, &updated); err == nil {
				if updated.Annotations != nil && updated.Annotations[annotationUpdateBlocked] == blockedReasonPDB {
					pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
					for _, pod := range pods {
						if podReady(&pod) && pod.UID != initialUIDs[pod.Name] {
							t.Fatalf("pod %s restarted despite PDB block", pod.Name)
						}
					}
					return true, nil
				}
			} else if client.IgnoreNotFound(err) != nil {
				return false, err
			}

			events, err := s.Harness.Kube().CoreV1().Events(cr.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			for _, evt := range events.Items {
				if evt.InvolvedObject.Kind != "KeyValCluster" || evt.InvolvedObject.Name != cr.Name {
					continue
				}
				if evt.Reason == "RollingStepBlocked" && strings.Contains(evt.Message, "PodDisruptionBudget") {
					pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
					for _, pod := range pods {
						if podReady(&pod) && pod.UID != initialUIDs[pod.Name] {
							t.Fatalf("pod %s restarted despite PDB block", pod.Name)
						}
					}
					return true, nil
				}
			}
			return false, nil
		}); err != nil {
			t.Fatalf("expected update-blocked annotation=%s: %v", blockedReasonPDB, err)
		}
	})

	s.Step("relax-pdb", func(ctx context.Context) {
		var current policyv1.PodDisruptionBudget
		if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: pdbName}, &current); err != nil {
			t.Fatalf("get pdb: %v", err)
		}
		patch := current.DeepCopy()
		minVal := cr.Spec.RedisReplicas - 1
		if minVal < 1 {
			minVal = 1
		}
		min := intstr.FromInt(int(minVal))
		patch.Spec.MinAvailable = &min
		patch.Spec.MaxUnavailable = nil
		if err := s.Harness.Client().Patch(ctx, patch, client.MergeFrom(&current)); err != nil {
			t.Fatalf("restore pdb: %v", err)
		}
	})

	s.Step("wait-first-restart", func(ctx context.Context) {
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 6*time.Minute, true, func(ctx context.Context) (bool, error) {
			pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
			for _, pod := range pods {
				if initialUIDs[pod.Name] != pod.UID {
					var updated keyvalv1alpha1.KeyValCluster
					if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, &updated); err != nil {
						return false, client.IgnoreNotFound(err)
					}
					if updated.Annotations != nil && updated.Annotations[annotationUpdateBlocked] != "" {
						return false, nil
					}
					return true, nil
				}
			}
			return false, nil
		}); err != nil {
			t.Fatalf("no redis pod restarted after relaxing PDB: %v", err)
		}
	})
}
