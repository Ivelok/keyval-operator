//go:build e2e

package security

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"sort"
	"strings"
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
	"k8s.io/utils/ptr"
)

const authPassword = "S3cureP@ssw0rd"
const tlsSecretHashAnnotation = "keyval.ivelok.io/tls-secret-hash"

func TestAuthSentinel(t *testing.T) {
	t.Parallel()

	s := suite.New(t)

	passSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("auth-pass-%d", time.Now().UnixNano()),
			Namespace: s.Harness.Namespace(),
		},
		Data: map[string][]byte{"password": []byte(authPassword)},
	}
	createSecret(t, s.Harness, passSecret)
	t.Cleanup(func() {
		_ = s.Harness.Client().Delete(context.Background(), passSecret)
	})

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage())
	builder = builder.
		WithName(fmt.Sprintf("security-auth-%d", time.Now().UnixNano())).
		WithLabels(map[string]string{"suite": "security", "feature": "auth"}).
		WithMode(keyvalv1alpha1.ModeStandalone).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithImage(s.Harness.RedisImage()).
		WithEphemeralStorage()
	cr := builder.Build()
	cr.Spec.Security = &keyvalv1alpha1.SecuritySpec{
		Auth: &keyvalv1alpha1.AuthSpec{
			Enabled: true,
			PasswordSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: passSecret.Name},
				Key:                  "password",
			},
		},
	}

	manager := cluster.NewManager(s.Harness)

	s.Step("create-cluster", func(ctx context.Context) {
		if err := manager.Apply(ctx, cr); err != nil {
			t.Fatalf("apply cluster: %v", err)
		}
	})

	s.Step("wait-ready", func(ctx context.Context) {
		updated := manager.WaitReady(ctx, cr, 6*time.Minute)
		*cr = *updated
		assert.MasterService(t, s.Harness, cr, 1*time.Minute)
	})

	s.Step("unauthorized-access", func(ctx context.Context) {
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, 1)
		cmd := []string{"sh", "-c", "REDISCLI_AUTH= redis-cli PING"}
		stdout, stderr, err := s.Harness.Exec(ctx, pods[0].Name, "redis", cmd...)
		_ = err // redis-cli may exit 0 even when returning NOAUTH; rely on output inspection.
		payload := stdout + stderr
		if !strings.Contains(payload, "NOAUTH") {
			t.Fatalf("expected NOAUTH message, got stdout=%q stderr=%q", stdout, stderr)
		}
	})

	s.Step("authorized-access", func(ctx context.Context) {
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, 1)
		stdout, stderr, err := s.Harness.Exec(ctx, pods[0].Name, "redis", "redis-cli", "-a", authPassword, "PING")
		if err != nil {
			t.Fatalf("redis-cli with auth failed: %v stderr=%q", err, stderr)
		}
		if !strings.Contains(strings.TrimSpace(stdout), "PONG") {
			t.Fatalf("expected PONG, got %q", stdout)
		}
	})
}

func TestTLSSentinel(t *testing.T) {
	t.Parallel()

	s := suite.New(t)

	clusterName := fmt.Sprintf("security-tls-%d", time.Now().UnixNano())
	caPEM, caKeyPEM, certPEM, keyPEM := generateTLSMaterial(t, clusterName, s.Harness.Namespace())

	tlsSecret := &corev1.Secret{
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
	createSecret(t, s.Harness, tlsSecret)
	t.Cleanup(func() {
		_ = s.Harness.Client().Delete(context.Background(), tlsSecret)
	})

	builder := cluster.NewBuilder(s.Harness.Namespace(), s.Harness.RedisImage())
	builder = builder.
		WithName(clusterName).
		WithLabels(map[string]string{"suite": "security", "feature": "tls"}).
		WithMode(keyvalv1alpha1.ModeSentinel).
		WithEngine(keyvalv1alpha1.EngineValkey).
		WithImage(s.Harness.RedisImage()).
		WithReplicas(3).
		WithSentinelCount(3).
		WithEphemeralStorage()
	cr := builder.Build()
	cr.Spec.Security = &keyvalv1alpha1.SecuritySpec{
		TLS: &keyvalv1alpha1.TLSSpec{
			Enabled:          true,
			SecretName:       tlsSecret.Name,
			DisablePlaintext: ptr.To(true),
		},
	}

	manager := cluster.NewManager(s.Harness)
	initialUIDs := map[string]types.UID{}
	initialTLSHash := ""

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
		for _, pod := range pods {
			if pod.Annotations[tlsSecretHashAnnotation] != initialTLSHash {
				t.Fatalf("pod %s missing initial tls hash annotation", pod.Name)
			}
		}
	})

	s.Step("plain-fails", func(ctx context.Context) {
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
		stdout, stderr, err := s.Harness.Exec(ctx, pods[0].Name, "redis", "redis-cli", "PING")
		if err == nil || strings.Contains(stdout, "PONG") {
			t.Fatalf("expected plaintext connection to fail, stdout=%q stderr=%q", stdout, stderr)
		}
	})

	s.Step("tls-succeeds", func(ctx context.Context) {
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
		cmd := []string{"redis-cli", "--tls", "--insecure", "--cacert", "/tls/ca.crt", "PING"}
		stdout, stderr, err := s.Harness.Exec(ctx, pods[0].Name, "redis", cmd...)
		if err != nil {
			t.Fatalf("redis-cli tls failed: %v stderr=%q", err, stderr)
		}
		if !strings.Contains(strings.TrimSpace(stdout), "PONG") {
			t.Fatalf("expected PONG, got %q", stdout)
		}
	})

	s.Step("tls-replication", func(ctx context.Context) {
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
		if err := wait.PollUntilContextTimeout(ctx, time.Second, 4*time.Minute, true, func(ctx context.Context) (bool, error) {
			master := 0
			for _, pod := range pods {
				cmd := []string{"redis-cli", "--tls", "--insecure", "--cacert", "/tls/ca.crt", "INFO", "replication"}
				stdout, _, err := s.Harness.Exec(ctx, pod.Name, "redis", cmd...)
				if err != nil {
					return false, nil
				}
				out := strings.TrimSpace(stdout)
				if strings.Contains(out, "role:master") {
					master++
				} else if !strings.Contains(out, "master_link_status:up") {
					return false, nil
				}
			}
			return master == 1, nil
		}); err != nil {
			t.Fatalf("replication did not stabilize with TLS: %v", err)
		}
	})

	s.Step("rotate-tls-secret", func(ctx context.Context) {
		pods := assert.RedisPodOrdinals(t, s.Harness, cr, int(cr.Spec.RedisReplicas))
		sentinelPods := listSentinelPods(ctx, t, s.Harness, cr)
		var podIPs []net.IP
		for _, pod := range pods {
			if ip := net.ParseIP(pod.Status.PodIP); ip != nil {
				podIPs = append(podIPs, ip)
			}
		}
		for _, pod := range sentinelPods {
			if ip := net.ParseIP(pod.Status.PodIP); ip != nil {
				podIPs = append(podIPs, ip)
			}
		}
		var newCertPEM, newKeyPEM []byte
		caPEM, caKeyPEM, newCertPEM, newKeyPEM = generateTLSMaterialWithExistingCA(t, clusterName, s.Harness.Namespace(), podIPs, caPEM, caKeyPEM)
		var secret corev1.Secret
		if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: tlsSecret.Namespace, Name: tlsSecret.Name}, &secret); err != nil {
			t.Fatalf("get tls secret: %v", err)
		}
		patch := secret.DeepCopy()
		patch.Data["ca.crt"] = caPEM
		patch.Data["tls.crt"] = newCertPEM
		patch.Data["tls.key"] = newKeyPEM
		if err := s.Harness.Client().Patch(ctx, patch, client.MergeFrom(&secret)); err != nil {
			t.Fatalf("patch tls secret: %v", err)
		}
		newHash := ""
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			var ss appsv1.StatefulSet
			if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, &ss); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			hash := ss.Spec.Template.Annotations[tlsSecretHashAnnotation]
			if hash != "" && hash != initialTLSHash {
				newHash = hash
				return true, nil
			}
			return false, nil
		}); err != nil {
			t.Fatalf("tls hash annotation did not change: %v", err)
		}
		var updatedCR keyvalv1alpha1.KeyValCluster
		if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, &updatedCR); err == nil {
			if updatedCR.Status.HealthGate != nil && !updatedCR.Status.HealthGate.AllowDisruptions {
				t.Logf("health gate disallowing disruptions after tls rotation: %#v", updatedCR.Status.HealthGate)
				for _, cond := range updatedCR.Status.Conditions {
					t.Logf("condition %s=%s reason=%s", cond.Type, cond.Status, cond.Reason)
				}
			}
		}
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 6*time.Minute, true, func(ctx context.Context) (bool, error) {
			pods, err := listRedisPods(ctx, s.Harness, cr)
			if err != nil {
				return false, err
			}
			if len(pods) != int(cr.Spec.RedisReplicas) {
				return false, nil
			}
			restartedAll := true
			hashAligned := true
			for i := range pods {
				pod := pods[i]
				ord, err := podOrdinal(pod.Name)
				if err != nil {
					return false, err
				}
				if ord != i {
					return false, nil
				}
				if !podReady(&pod) {
					return false, nil
				}
				if pod.Annotations[tlsSecretHashAnnotation] != newHash {
					hashAligned = false
				}
				if initialUIDs[pod.Name] == pod.UID {
					restartedAll = false
				}
			}
			return restartedAll && hashAligned, nil
		}); err != nil {
			t.Fatalf("redis pods did not restart after TLS rotation: %v", err)
		}
		updated := manager.WaitReady(ctx, cr, 6*time.Minute)
		*cr = *updated
		var ss appsv1.StatefulSet
		if err := s.Harness.Client().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, &ss); err != nil {
			t.Fatalf("get redis statefulset: %v", err)
		}
		if hash := ss.Spec.Template.Annotations[tlsSecretHashAnnotation]; hash == "" {
			t.Fatalf("expected tls hash annotation to be set")
		} else if hash == initialTLSHash {
			t.Fatalf("expected tls hash annotation to change, hash=%s", hash)
		}
		_ = newHash
	})
}

func listRedisPods(ctx context.Context, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster) ([]corev1.Pod, error) {
	selector := labels.Set{"app": fmt.Sprintf("%s-redis", cluster.Name)}
	var pods corev1.PodList
	if err := h.Client().List(ctx, &pods, client.InNamespace(h.Namespace()), client.MatchingLabels(selector)); err != nil {
		return nil, err
	}
	items := pods.Items
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items, nil
}

func listSentinelPods(ctx context.Context, t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster) []corev1.Pod {
	selector := labels.Set{"app": fmt.Sprintf("%s-sentinel", cluster.Name)}
	var pods corev1.PodList
	if err := h.Client().List(ctx, &pods, client.InNamespace(h.Namespace()), client.MatchingLabels(selector)); err != nil {
		t.Fatalf("list sentinel pods: %v", err)
	}
	items := pods.Items
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items
}

func podReady(p *corev1.Pod) bool {
	if p == nil {
		return false
	}
	for i := range p.Status.Conditions {
		cond := p.Status.Conditions[i]
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podOrdinal(name string) (int, error) {
	idx := strings.LastIndex(name, "-")
	if idx == -1 || idx == len(name)-1 {
		return -1, fmt.Errorf("invalid pod name %q", name)
	}
	var ord int
	if _, err := fmt.Sscanf(name[idx+1:], "%d", &ord); err != nil {
		return -1, err
	}
	return ord, nil
}

func createSecret(t *testing.T, h *harness.Harness, secret *corev1.Secret) {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.Context(), 30*time.Second)
	defer cancel()
	if err := h.Client().Create(ctx, secret); err != nil {
		t.Fatalf("create secret %s: %v", secret.Name, err)
	}
}

func generateTLSMaterial(t *testing.T, clusterName, namespace string) (caPEM, caKeyPEM, certPEM, keyPEM []byte) {
	return generateTLSMaterialWithOptions(t, clusterName, namespace, nil, nil, nil)
}

func generateTLSMaterialWithIPs(t *testing.T, clusterName, namespace string, ipSANs []net.IP) (caPEM, caKeyPEM, certPEM, keyPEM []byte) {
	return generateTLSMaterialWithOptions(t, clusterName, namespace, ipSANs, nil, nil)
}

func generateTLSMaterialWithExistingCA(t *testing.T, clusterName, namespace string, ipSANs []net.IP, caPEM, caKeyPEM []byte) (newCAPEM, newCAKeyPEM, certPEM, keyPEM []byte) {
	return generateTLSMaterialWithOptions(t, clusterName, namespace, ipSANs, caPEM, caKeyPEM)
}

func generateTLSMaterialWithOptions(t *testing.T, clusterName, namespace string, ipSANs []net.IP, existingCAPEM, existingCAKeyPEM []byte) (caPEM, caKeyPEM, certPEM, keyPEM []byte) {
	t.Helper()

	var caPriv *ecdsa.PrivateKey
	var caCert *x509.Certificate
	var err error
	if len(existingCAKeyPEM) > 0 {
		block, _ := pem.Decode(existingCAKeyPEM)
		if block == nil {
			t.Fatalf("decode existing CA key")
		}
		caPriv, err = x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			t.Fatalf("parse existing CA key: %v", err)
		}
		block, _ = pem.Decode(existingCAPEM)
		if block == nil {
			t.Fatalf("decode existing CA cert")
		}
		caCert, err = x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("parse existing CA cert: %v", err)
		}
	} else {
		caPriv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate ca key: %v", err)
		}
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}
	caTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"keyval-operator"},
			CommonName:   "KeyVal Test CA",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	if len(existingCAPEM) > 0 {
		caPEM = existingCAPEM
		caKeyPEM = existingCAKeyPEM
	} else {
		caDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caPriv.PublicKey, caPriv)
		if err != nil {
			t.Fatalf("create ca cert: %v", err)
		}
		caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
		caKeyBytes, keyErr := x509.MarshalECPrivateKey(caPriv)
		if keyErr != nil {
			t.Fatalf("marshal ca key: %v", keyErr)
		}
		caKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: caKeyBytes})
		caCert = &caTemplate
	}

	serverPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	serialNumber, err = rand.Int(rand.Reader, serialLimit)
	if err != nil {
		t.Fatalf("generate server serial: %v", err)
	}
	hosts := []string{
		"127.0.0.1",
		fmt.Sprintf("%s-headless.%s.svc", clusterName, namespace),
		fmt.Sprintf("%s.%s.svc", clusterName, namespace),
	}
	for i := 0; i < 6; i++ {
		hosts = append(hosts,
			fmt.Sprintf("%s-%d", clusterName, i),
			fmt.Sprintf("%s-%d.%s-headless.%s.svc", clusterName, i, clusterName, namespace),
		)
	}
	// Sentinel services/pods
	for _, name := range []string{
		fmt.Sprintf("%s-sentinel.%s.svc", clusterName, namespace),
		fmt.Sprintf("%s-sentinel-headless.%s.svc", clusterName, namespace),
	} {
		hosts = append(hosts, name)
	}
	for i := 0; i < 6; i++ {
		base := fmt.Sprintf("%s-sentinel-%d", clusterName, i)
		hosts = append(hosts,
			base,
			fmt.Sprintf("%s.%s-sentinel-headless.%s.svc", base, clusterName, namespace),
		)
	}
	serverTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"keyval-operator"},
			CommonName:   fmt.Sprintf("%s-headless.%s.svc", clusterName, namespace),
		},
		DNSNames:    hosts,
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if len(ipSANs) > 0 {
		serverTemplate.IPAddresses = append(serverTemplate.IPAddresses, ipSANs...)
	}
	parent := caCert
	if parent == nil {
		parent = &caTemplate
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, parent, &serverPriv.PublicKey, caPriv)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(serverPriv)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	return
}
