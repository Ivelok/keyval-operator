package replication

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func TestEnsureTopologyWithTLSSecurityPassesClientOptions(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add api scheme: %v", err)
	}
	caPEM := generateSelfSignedCAPEM(t)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-tls", Namespace: "default"},
		Data: map[string][]byte{
			"ca.crt":  caPEM,
			"tls.crt": []byte("dummy-cert"),
			"tls.key": []byte("dummy-key"),
		},
	}
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 2,
			Security: &keyvalv1alpha1.SecuritySpec{TLS: &keyvalv1alpha1.TLSSpec{
				Enabled:    true,
				SecretName: "redis-tls",
			}},
		},
	}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Labels: core.LabelsFor(cr)}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "default", Labels: core.LabelsFor(cr)}, Status: corev1.PodStatus{PodIP: "10.0.0.2"}},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(secret).Build()
	settings, err := security.FromSpec(context.Background(), fakeClient, cr)
	if err != nil {
		t.Fatalf("security.FromSpec error: %v", err)
	}
	masterClient := &capturingClient{name: "demo-0", info: ReplicationInfo{Role: "master"}}
	replicaClient := &capturingClient{name: "demo-1", info: ReplicationInfo{Role: "replica", MasterHost: "10.0.0.1", MasterPort: 6379, MasterLinkStatus: "up"}}
	factory := &capturingFactory{
		clients: map[string]*capturingClient{
			"demo-0": masterClient,
			"demo-1": replicaClient,
		},
		options: map[string]ClientOptions{},
	}

	res, err := EnsureTopology(context.Background(), EnsureRequest{
		Cluster:      cr,
		RedisPods:    pods,
		Factory:      factory,
		ForceMaster:  "demo-0",
		WaitTimeout:  200 * time.Millisecond,
		WaitInterval: 10 * time.Millisecond,
		Security:     &settings,
	})
	if err != nil {
		t.Fatalf("EnsureTopology error: %v", err)
	}
	if res.Master != "demo-0" {
		t.Fatalf("expected master demo-0, got %s", res.Master)
	}
	factory.mu.Lock()
	defer factory.mu.Unlock()
	for _, name := range []string{"demo-0", "demo-1"} {
		opts, ok := factory.options[name]
		if !ok {
			t.Fatalf("missing options for pod %s", name)
		}
		if opts.TLSConfig == nil {
			t.Fatalf("expected TLS config for pod %s", name)
		}
		if opts.TLSConfig.RootCAs == nil {
			t.Fatalf("expected RootCAs populated for pod %s", name)
		}
	}
	if factory.options["demo-0"].TLSConfig != factory.options["demo-1"].TLSConfig {
		t.Fatalf("expected shared TLS config instance across pods")
	}
}

func TestEnsureTopologyTLSConfigErrorIsFatal(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey", RedisReplicas: 1},
	}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Labels: core.LabelsFor(cr)}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}}
	factory := &capturingFactory{clients: map[string]*capturingClient{"demo-0": {name: "demo-0", info: ReplicationInfo{Role: "master"}}}, options: map[string]ClientOptions{}}
	settings := &security.Settings{TLS: security.TLSSettings{Enabled: true}}
	_, err := EnsureTopology(context.Background(), EnsureRequest{
		Cluster:      cr,
		RedisPods:    []corev1.Pod{pod},
		Factory:      factory,
		ForceMaster:  "demo-0",
		WaitTimeout:  50 * time.Millisecond,
		WaitInterval: 10 * time.Millisecond,
		Security:     settings,
	})
	if err == nil {
		t.Fatalf("expected error due to TLS config failure")
	}
	if !controllererrors.IsFatal(err) {
		t.Fatalf("expected fatal error, got %v", err)
	}
}

type capturingFactory struct {
	mu      sync.Mutex
	clients map[string]*capturingClient
	options map[string]ClientOptions
}

func (f *capturingFactory) ForPod(_ context.Context, pod corev1.Pod, opts ClientOptions) (Client, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.options[pod.Name] = opts
	client, ok := f.clients[pod.Name]
	if !ok {
		return nil, nil
	}
	return client, nil
}

type capturingClient struct {
	name       string
	info       ReplicationInfo
	role       string
	replicaOps int
	authCalls  int
}

func (c *capturingClient) Role(context.Context) (string, error) {
	if c.role != "" {
		return c.role, nil
	}
	return c.info.Role, nil
}

func (c *capturingClient) ReplicationInfo(context.Context) (ReplicationInfo, error) {
	return c.info, nil
}

func (c *capturingClient) ReplicaOf(context.Context, string, int) error {
	c.role = "replica"
	c.replicaOps++
	return nil
}

func (c *capturingClient) NoOne(context.Context) error {
	c.role = "master"
	return nil
}

func (c *capturingClient) ResetSentinel(context.Context, *keyvalv1alpha1.KeyValCluster) error {
	return nil
}
func (c *capturingClient) ConfigGet(context.Context, string) (string, bool, error) {
	return "", false, nil
}
func (c *capturingClient) ConfigSet(context.Context, string, string) error { return nil }
func (c *capturingClient) ConfigRewrite(context.Context) error             { return nil }
func (c *capturingClient) Auth(context.Context, string, string) error {
	c.authCalls++
	return nil
}

func generateSelfSignedCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	now := time.Now()
	templ := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, templ, templ, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
