package phases

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

const (
	caPEM = `-----BEGIN CERTIFICATE-----
MIIDBzCCAe+gAwIBAgIUSSs3aAidenDIZ5daRGYHK5CqLG8wDQYJKoZIhvcNAQEL
BQAwEzERMA8GA1UEAwwIbG9jYWwtY2EwHhcNMjUwOTMwMDg1MjQ5WhcNMjYwOTMw
MDg1MjQ5WjATMREwDwYDVQQDDAhsb2NhbC1jYTCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBAJSZJblMauFFw6UB/pUW1JnuSypCmSrQbXIEUiPwSv66Nt0v
Bjoy9aTmVdzHWjNKJKqMXg400t3oVxKLtjGgql3BVehT3rac+Xkbmj0GDtUEkzWI
DjI1oCHAUYUwfEiDmUQlIdOD6tuGYE+iJjeHb49GPSkKkMmyDQ7pKgjNR4gIbyTi
GP+093DxLcMvulE6ldnrqyrCCFktdnP2sePe/bhHl2N5SqAfPHCDDFkrIgodj1pq
4AKH0Op0ilPCZ9qlaXlPy4l1zvQu4oMD2N0cUQowZllYi/qS7BTYrcY4cv79kVj+
M5BIepE7ZUQZFx/xql1jUQAigmI+vLNlKFWNElcCAwEAAaNTMFEwHQYDVR0OBBYE
FG7gFsHD214QAaHxfzZy5z0rPXJuMB8GA1UdIwQYMBaAFG7gFsHD214QAaHxfzZy
5z0rPXJuMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAGd2kqwe
7B5z5Etzk1pb+QAPHAwgtCT5q7DL0ry4sQz3ldx/f25gh9HiDf5lpRHpOLwm3XxA
CXxxtCpAnkgFhkRpS0kBGIYk8Tl9qYN6oPm0znWwdHfHAT+KEClCwfBfhczPyEfm
E72XBK3LZSmkFZUKGjteXBdRlW8iSQobarZIZJ9Td37Oz3E3lM3433enCsvqpsHx
cHmRM6rKbS4NFtBdVPxB6rmfmZ+L27WIzTxJssdQVpCkeTa7hVU2KjENu7Ji71XA
NysW5omS2Io6OrjFl7Mhcyo7IXIm07yoqGnAim/263ZL5acPhYce+vsK6MGWQqyd
Jj9s0npv0VXRoXo=
-----END CERTIFICATE-----`

	certPEM = caPEM
	keyPEM  = `-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCUmSW5TGrhRcOl
Af6VFtSZ7ksqQpkq0G1yBFIj8Er+ujbdLwY6MvWk5lXcx1ozSiSqjF4ONNLd6FcS
i7YxoKpdwVXoU962nPl5G5o9Bg7VBJM1iA4yNaAhwFGFMHxIg5lEJSHTg+rbhmBP
oiY3h2+PRj0pCpDJsg0O6SoIzUeICG8k4hj/tPdw8S3DL7pROpXZ66sqwghZLXZz
9rHj3v24R5djeUqgHzxwgwxZKyIKHY9aauACh9DqdIpTwmfapWl5T8uJdc70LuKD
A9jdHFEKMGZZWIv6kuwU2K3GOHL+/ZFY/jOQSHqRO2VEGRcf8apdY1EAIoJiPryz
ZShVjRJXAgMBAAECggEARdAlvwa9+BRUtINZXiYZwDAKNFKRr2G46aZKCQRt13sZ
J5VeMZ2bdtuYX19xa0NcMpw73CTJJORxdA8bi/lV0TJx7/LnYZgiRNnI/v6HnWDG
4JGJSeGT2AeIxTBgU5kwOqnPWJZTkstxGDiwB8qjiQaQ2WVTXM8//09gJj57atcD
8V7ZrfSvEq2wMKOnvaaRqrvOw3T54z91rO5NzLqSijrAnq6EeLsUamZStNYq8U+U
0f5yghG97pCBW6Kr0Yt+PFPUGU8Tn00+yI042Aycl9KaG9L94B8S9+psTApUnPV8
FFGVm7ynuwGzKi+5vSRlGW5pUZ3pVay7Lu++nduKQQKBgQDPjUB85cePGaIAZ+SF
cc09GWCXVtlz9x5vmXRB6tQmRHDk5mXMRiR6fTwdA7fgcwbgsEhRcRQ/tkzIPikp
hfjucmJW5WKW3+Q6Fy2v1Ly2oPPj/JQAhQncZkuJmBc32yHGv6InCniMd1becsrP
y0ouwO/evLv3ByGDCTlCRL565wKBgQC3SPy6CwIilrEdCoMxRSSNNIAGA+9A0lMy
c3HNc4KbrpgQ8dm/tXVJGMCQsJXUSyhB/8M8kT/zk7edRGAHRWXLIR4Klvp7/PAH
xbcNiObbNbXiG3laGH1P8Z7IXm5iEu39xHbSZObVE+C3m3zW/v+QP7diocq5jdkX
C9/b6KWvEQKBgEi+n02nU6xqNYei4kuLOX4iuOISRKEKihZfWIoJ/lVzQ4ZW4nMY
Woy7/CfHN9lpN43k+1PgKFK0WHEOqGqvVDJu0NzYBRgQXPOnUBICpCn2e8T6r/0G
pBAlonAVaH3hRhNc8z5vwxSodz/8R+1QuS1t3iTZTaAlVa+RugnqAkEdAoGAVs+y
LGhEZZ+cWhX9l5uZOWxxauf4LWqxT0cQ0u/wH87tZbE3oq7O04Vux9lrzfafJct/
bLObZ8JCiLG3DhqXoUOZWAi0sX9XLUc/caCzP4bMFEFRFBRfXjsiKuNXQwqWQMkK
QpLaJnhYyn5R/f8fivIy6Pua6pI+DcVpDV6/AxECgYBzEHAcIeWg0qKVIoROCka1
NhWEACkxiINXO1hAdcYmuSvWPobG+l1AxR0+TN2OVI/t/5rsotFHu9OyJnevx4O/
QNisBRGNNs+yfDPP3KBBjJjr/nzUcwDDq9n4vhOZWQa1dxdVDk2FmzWqU6Lsd5ox
uE5EuPFrPYI9/H3mCJRaeA==
-----END PRIVATE KEY-----`
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add api scheme: %v", err)
	}
	return scheme
}

func newLogger() logging.Logger {
	return logging.New(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestSecurityConfigServicesPhases(t *testing.T) {
	scheme := newScheme(t)

	authSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-auth", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("topsecret")},
	}
	tlsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-tls", Namespace: "default"},
		Data: map[string][]byte{
			"ca.crt":  []byte(caPEM),
			"tls.crt": []byte(certPEM),
			"tls.key": []byte(keyPEM),
		},
	}

	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			Engine:        keyvalv1alpha1.EngineRedis,
			Image:         "redis:7.2",
			RedisReplicas: 3,
			SentinelCount: ptrInt32(3),
			Service: &keyvalv1alpha1.ServiceSpec{
				Create: ptrBool(true),
			},
			Security: &keyvalv1alpha1.SecuritySpec{
				Auth: &keyvalv1alpha1.AuthSpec{
					Enabled: true,
					PasswordSecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: authSecret.Name},
						Key:                  "password",
					},
				},
				TLS: &keyvalv1alpha1.TLSSpec{
					Enabled:    true,
					SecretName: tlsSecret.Name,
				},
			},
		},
	}
	cluster.Status.ReadyReplicas = cluster.Spec.RedisReplicas

	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy(), authSecret, tlsSecret).
		Build()

	state := &reconcile.State{
		Cluster:     cluster.DeepCopy(),
		Logger:      newLogger(),
		Request:     ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}},
		ResourceKey: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}.String(),
		Dependencies: reconcile.Dependencies{
			Client:    client,
			APIReader: client,
			Recorder:  record.NewFakeRecorder(10),
			Scheme:    scheme,
		},
		Accumulator: &reconcile.RequeueAccumulator{},
	}

	if err := Security(context.Background(), state); err != nil {
		t.Fatalf("security phase: %v", err)
	}
	if !state.Security.Settings.HasAuth() {
		t.Fatalf("expected auth enabled in security state")
	}
	if !state.Security.Settings.HasTLS() {
		t.Fatalf("expected tls enabled in security state")
	}
	if state.Security.RedisClientOptions.Password != "topsecret" {
		t.Fatalf("unexpected redis password: %q", state.Security.RedisClientOptions.Password)
	}
	if state.Security.RedisClientOptions.TLSConfig == nil {
		t.Fatalf("expected tls config for redis client")
	}
	if state.Security.TLSHash == "" {
		t.Fatalf("expected tls hash populated")
	}

	if err := Config(context.Background(), state); err != nil {
		t.Fatalf("config phase: %v", err)
	}
	if state.Config.ConfigHash == "" {
		t.Fatalf("expected config hash recorded")
	}

	if err := Services(context.Background(), state); err != nil {
		t.Fatalf("services phase: %v", err)
	}

	var master corev1.Service
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: core.MasterServiceName(cluster)}, &master); err != nil {
		t.Fatalf("fetch master service: %v", err)
	}
	if master.Spec.Selector["role"] != string(keyvalv1alpha1.PodRoleMaster) {
		t.Fatalf("unexpected master selector: %+v", master.Spec.Selector)
	}
}

func TestServicesPhaseRespectsCreateFlags(t *testing.T) {
	scheme := newScheme(t)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Engine:        keyvalv1alpha1.EngineValkey,
			Image:         "valkey:7.2",
			RedisReplicas: 1,
			Service: &keyvalv1alpha1.ServiceSpec{
				Create: ptrBool(false),
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy()).
		Build()

	state := &reconcile.State{
		Cluster: cluster.DeepCopy(),
		Logger:  newLogger(),
		Dependencies: reconcile.Dependencies{
			Client:   client,
			Recorder: record.NewFakeRecorder(5),
			Scheme:   scheme,
		},
	}

	if err := Services(context.Background(), state); err != nil {
		t.Fatalf("services phase: %v", err)
	}

	var svc corev1.Service
	err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: core.MasterServiceName(cluster)}, &svc)
	if err == nil {
		t.Fatalf("expected master service to be absent when service.create=false")
	}
}

func TestSentinelPhaseSkipsResetDuringFailover(t *testing.T) {
	scheme := newScheme(t)
	count := int32(3)
	cluster := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeSentinel,
			RedisReplicas: 3,
			SentinelCount: &count,
		},
		Status: keyvalv1alpha1.KeyValClusterStatus{
			Conditions: []metav1.Condition{
				{
					Type:               string(keyvalv1alpha1.ConditionFailoverInProgress),
					Status:             metav1.ConditionTrue,
					Reason:             "SentinelFailover",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}
	cluster.Status.ReadyReplicas = cluster.Spec.RedisReplicas

	readyCond := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
	sentinels := make([]corev1.Pod, 3)
	for i := range sentinels {
		sentinels[i] = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("demo-sentinel-%d", i),
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					core.LabelClusterKey: cluster.Name,
					core.LabelAppKey:     core.AppLabel(cluster),
				},
			},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{readyCond}},
		}
	}

	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster.DeepCopy()).
		Build()
	factory := &fakeSentinelFactory{quorumOK: true}

	state := &reconcile.State{
		Cluster:            cluster.DeepCopy(),
		Logger:             newLogger(),
		SentinelPods:       sentinels,
		ConditionOverrides: make(map[keyvalv1alpha1.ConditionType]*status.ConditionState),
		Runtime: reconcile.RuntimeState{
			NeedSentinelReset: true,
		},
		Dependencies: reconcile.Dependencies{
			Client:          client,
			SentinelFactory: factory,
		},
	}

	if err := Sentinel(context.Background(), state); err != nil {
		t.Fatalf("sentinel phase: %v", err)
	}
	if factory.resets != 0 {
		t.Fatalf("expected no sentinel resets while failover active, got %d", factory.resets)
	}
}

func ptrBool(v bool) *bool { return &v }

func ptrInt32(v int32) *int32 { return &v }

type fakeSentinelFactory struct {
	quorumOK bool
	checkErr error
	resetErr error
	resets   int
}

func (f *fakeSentinelFactory) ForPod(ctx context.Context, pod corev1.Pod, opts clients.SentinelOptions) (clients.SentinelClient, error) {
	return &fakeSentinelClient{factory: f}, nil
}

type fakeSentinelClient struct {
	factory *fakeSentinelFactory
}

func (c *fakeSentinelClient) GetMasterAddrByName(ctx context.Context, name string) (string, int, error) {
	return "", 0, nil
}

func (c *fakeSentinelClient) Failover(ctx context.Context, name string) error {
	return nil
}

func (c *fakeSentinelClient) Set(ctx context.Context, name, option, value string) error {
	return nil
}

func (c *fakeSentinelClient) Master(ctx context.Context, name string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (c *fakeSentinelClient) CheckQuorum(ctx context.Context, name string) (bool, error) {
	return c.factory.quorumOK, c.factory.checkErr
}

func (c *fakeSentinelClient) Reset(ctx context.Context, name string) error {
	c.factory.resets++
	return c.factory.resetErr
}
