package clients

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func makeRedisPod() corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-0",
			Namespace: "default",
			Labels: map[string]string{
				core.LabelAppKey:     "demo-redis",
				core.LabelClusterKey: "demo",
			},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.5"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  core.RedisContainerName,
				Ports: []corev1.ContainerPort{{Name: "redis", ContainerPort: 6379}},
			}},
		},
	}
}

func TestRedisFactory_NonTLSUsesPodIP(t *testing.T) {
	t.Parallel()
	factory := NewGoRedisFactory().(*goRedisFactory)
	pod := makeRedisPod()

	client, err := factory.ForPod(context.Background(), pod, ClientOptions{})
	if err != nil {
		t.Fatalf("ForPod error: %v", err)
	}
	rc := client.(*goRedisClient)
	if got := rc.rdb.Options().Addr; got != "10.0.0.5:6379" {
		t.Fatalf("expected IP addr, got %s", got)
	}
	if rc.rdb.Options().TLSConfig != nil {
		t.Fatalf("expected nil TLS config")
	}
	if got := rc.rdb.Options().DialTimeout; got != factory.cfg.DialTimeout {
		t.Fatalf("expected dial timeout %s, got %s", factory.cfg.DialTimeout, got)
	}
	if got := rc.rdb.Options().MinIdleConns; got != factory.cfg.MinIdleConns {
		t.Fatalf("expected min idle %d, got %d", factory.cfg.MinIdleConns, got)
	}
}

func TestRedisFactory_TLSUsesDNSAndServerName(t *testing.T) {
	t.Parallel()
	factory := NewGoRedisFactory().(*goRedisFactory)
	pod := makeRedisPod()
	tlsCfg := &tls.Config{}
	client, err := factory.ForPod(context.Background(), pod, ClientOptions{TLSConfig: tlsCfg})
	if err != nil {
		t.Fatalf("ForPod error: %v", err)
	}
	rc := client.(*goRedisClient)
	wantHost := "demo-0.demo-headless.default.svc"
	if got := rc.rdb.Options().Addr; got != wantHost+":6379" {
		t.Fatalf("expected DNS addr %s:6379, got %s", wantHost, got)
	}
	cfg := rc.rdb.Options().TLSConfig
	if cfg == nil {
		t.Fatalf("expected TLS config")
	}
	if cfg == tlsCfg {
		t.Fatalf("expected TLS config clone, not original pointer")
	}
	if cfg.ServerName != wantHost {
		t.Fatalf("expected ServerName %s, got %s", wantHost, cfg.ServerName)
	}
}

func TestRedisClient_RespectsOperationTimeout(t *testing.T) {
	factoryCfg := FactoryConfig{
		OperationTimeout:         100 * time.Millisecond,
		DialTimeout:              time.Second,
		ReadTimeout:              time.Second,
		WriteTimeout:             time.Second,
		PoolTimeout:              time.Second,
		SentinelOperationTimeout: time.Second,
		MaxRetries:               0,
	}.ApplyDefaults()
	factoryCfg.MaxRetries = 0
	factoryCfg.sleepFunc = func(time.Duration) {}

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})

	go func() {
		defer serverConn.Close()
		buf := make([]byte, 1024)
		if _, err := serverConn.Read(buf); err != nil {
			return
		}
		_, _ = serverConn.Write([]byte("-ERR unknown command 'HELLO'\r\n"))
		for {
			if _, err := serverConn.Read(buf); err != nil {
				return
			}
		}
	}()

	redisOpts := &redis.Options{
		Addr:                  "demo-0.demo-headless.default.svc:6379",
		Dialer:                func(context.Context, string, string) (net.Conn, error) { return clientConn, nil },
		DialTimeout:           factoryCfg.DialTimeout,
		ReadTimeout:           factoryCfg.ReadTimeout,
		WriteTimeout:          factoryCfg.WriteTimeout,
		PoolTimeout:           factoryCfg.PoolTimeout,
		MinIdleConns:          factoryCfg.MinIdleConns,
		MaxRetries:            0,
		ContextTimeoutEnabled: true,
	}
	rdb := redis.NewClient(redisOpts)
	t.Cleanup(func() { _ = rdb.Close() })

	client := &goRedisClient{
		rdb:          rdb,
		dnsHost:      "demo-0.demo-headless.default.svc",
		namespace:    "default",
		cluster:      "demo",
		endpointAddr: redisOpts.Addr,
		cfg:          factoryCfg,
	}

	labels := map[string]string{
		"namespace": "default",
		"cluster":   "demo",
		"command":   "INFO replication",
		"endpoint":  redisOpts.Addr,
	}
	beforeTimeouts := readMetricCounter(t, "keyval_operator_external_call_timeouts_total", labels)
	beforeRetries := readMetricCounter(t, "keyval_operator_redis_client_retries_total", labels)

	start := time.Now()
	_, err := client.ReplicationInfo(context.Background())
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("operation exceeded expected timeout; elapsed=%s", elapsed)
	}

	afterTimeouts := readMetricCounter(t, "keyval_operator_external_call_timeouts_total", labels)
	if diff := afterTimeouts - beforeTimeouts; diff != 1 {
		dumpMetric(t, "keyval_operator_external_call_timeouts_total")
		t.Fatalf("expected timeout counter to increase by 1, delta=%f", diff)
	}

	afterRetries := readMetricCounter(t, "keyval_operator_redis_client_retries_total", labels)
	if diff := afterRetries - beforeRetries; diff != 0 {
		t.Fatalf("expected retry counter unchanged (maxRetries=0), delta=%f", diff)
	}
}

func readMetricCounter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if labelsMatch(metric.GetLabel(), labels) {
				if metric.GetCounter() != nil {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func labelsMatch(protoLabels []*io_prometheus_client.LabelPair, expected map[string]string) bool {
	if len(expected) == 0 {
		return len(protoLabels) == 0
	}
	matched := 0
	for _, pair := range protoLabels {
		if val, ok := expected[pair.GetName()]; ok && val == pair.GetValue() {
			matched++
		}
	}
	return matched == len(expected)
}

func dumpMetric(t *testing.T, name string) {
	t.Helper()
	mfs, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Logf("gather error: %v", err)
		return
	}
	found := false
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		t.Logf("metric %s:", name)
		for _, metric := range mf.GetMetric() {
			kv := map[string]string{}
			for _, label := range metric.GetLabel() {
				kv[label.GetName()] = label.GetValue()
			}
			val := 0.0
			if metric.GetCounter() != nil {
				val = metric.GetCounter().GetValue()
			}
			t.Logf("  labels=%v value=%f", kv, val)
		}
		found = true
	}
	if !found {
		t.Logf("metric %s not found", name)
	}
}
