package resources

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func crBase(name string, mode keyvalv1alpha1.Mode) *keyvalv1alpha1.KeyValCluster {
	var replicas int32 = 1
	var sc *int32
	if mode == keyvalv1alpha1.ModeSentinel {
		replicas = 3
		v := int32(3)
		sc = &v
	}
	return &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          mode,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: replicas,
			SentinelCount: sc,
		},
	}
}

func TestBuildRedisConfig_Defaults(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	sec := &security.Settings{}
	cfg := buildRedisConfig(cr, sec)
	if !strings.Contains(cfg, "port 6379\n") {
		t.Fatalf("expected port line in redis.conf, got:\n%s", cfg)
	}
	if !strings.Contains(cfg, "dir /data\n") {
		t.Fatalf("expected dir line in redis.conf")
	}
	if strings.Contains(cfg, "replicaof") || strings.Contains(cfg, "slaveof") {
		t.Fatalf("replicaof/slaveof must not be present in redis.conf")
	}
	if !strings.Contains(cfg, "include /runtime-conf/role.conf\n") {
		t.Fatalf("expected runtime role include in redis.conf, got:\n%s", cfg)
	}
}

func TestBuildRedisConfig_Overrides(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	cr.Spec.RedisConfig = map[string]string{"appendonly": "no"}
	sec := &security.Settings{}
	cfg := buildRedisConfig(cr, sec)
	if !strings.Contains(cfg, "appendonly no\n") {
		t.Fatalf("expected override in redis.conf, got:\n%s", cfg)
	}
}

func TestBuildSentinelConfig_MonitorLine(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeSentinel)
	sec := &security.Settings{}
	cfg := buildSentinelConfig(cr, sec)
	exp := "sentinel monitor demo demo-0.demo-headless.default.svc 6379 2\n"
	if !strings.HasPrefix(cfg, strings.TrimSpace(exp)) {
		t.Fatalf("expected monitor line prefix, got:\n%s\nwant prefix: %s", cfg, exp)
	}
}

func TestBuildSentinelConfig_MonitorLine_PortOverride(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeSentinel)
	cr.Spec.RedisConfig = map[string]string{"port": "6388"}
	sec := &security.Settings{}
	cfg := buildSentinelConfig(cr, sec)
	exp := "sentinel monitor demo demo-0.demo-headless.default.svc 6388 2\n"
	if !strings.HasPrefix(cfg, strings.TrimSpace(exp)) {
		t.Fatalf("expected monitor line prefix with overridden port, got:\n%s\nwant prefix: %s", cfg, exp)
	}
}

func TestBuildRedisConfig_WithAuthAndTLS(t *testing.T) {
	t.Parallel()
	cr := crBase("secure", keyvalv1alpha1.ModeStandalone)
	sec := &security.Settings{
		Auth: security.AuthSettings{Enabled: true, Username: "ops", Password: "topsecret"},
		TLS:  security.TLSSettings{Enabled: true, CACertKey: "ca.pem", CertKey: "tls.pem", KeyKey: "tls.key"},
	}
	cfg := buildRedisConfig(cr, sec)
	if !strings.Contains(cfg, "requirepass topsecret\n") {
		t.Fatalf("expected requirepass in redis.conf, got:\n%s", cfg)
	}
	if !strings.Contains(cfg, "masteruser ops\n") {
		t.Fatalf("expected masteruser in redis.conf")
	}
	if !strings.Contains(cfg, "tls-port 6379\n") {
		t.Fatalf("expected tls-port entry in redis.conf")
	}
	if !strings.Contains(cfg, "tls-replication yes\n") {
		t.Fatalf("expected tls-replication entry")
	}
	if !strings.Contains(cfg, "tls-cert-file /tls/tls.pem\n") {
		t.Fatalf("expected tls cert path in redis.conf")
	}
}

func TestBuildSentinelConfig_WithAuthAndTLS(t *testing.T) {
	t.Parallel()
	cr := crBase("secure", keyvalv1alpha1.ModeSentinel)
	sec := &security.Settings{
		Auth: security.AuthSettings{Enabled: true, Username: "ops", Password: "topsecret"},
		TLS:  security.TLSSettings{Enabled: true, CACertKey: "ca.pem", CertKey: "tls.pem", KeyKey: "tls.key"},
	}
	cfg := buildSentinelConfig(cr, sec)
	if !strings.Contains(cfg, "requirepass topsecret\n") {
		t.Fatalf("expected requirepass in sentinel.conf, got:\n%s", cfg)
	}
	if !strings.Contains(cfg, "sentinel auth-pass secure topsecret\n") {
		t.Fatalf("expected sentinel auth-pass entry")
	}
	if !strings.Contains(cfg, "sentinel auth-user secure ops\n") {
		t.Fatalf("expected sentinel auth-user entry")
	}
	if !strings.Contains(cfg, "tls-port 26379\n") {
		t.Fatalf("expected tls-port entry in sentinel.conf")
	}
	if !strings.Contains(cfg, "tls-cert-file /tls/tls.pem\n") {
		t.Fatalf("expected tls cert path in sentinel.conf")
	}
}

func TestConfigMap_Keys(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeSentinel)
	sec := &security.Settings{}
	cm := ConfigMap(cr, sec)
	if cm.Data["redis.conf"] == "" || cm.Data["sentinel.conf"] == "" {
		t.Fatalf("expected both redis.conf and sentinel.conf keys")
	}
	if script := cm.Data[BootstrapScriptKey]; script == "" {
		t.Fatalf("expected bootstrap script key %q", BootstrapScriptKey)
	} else if script != redisBootstrapScript() {
		t.Fatalf("unexpected bootstrap script content")
	}
	if script := cm.Data[LivenessScriptKey]; script == "" {
		t.Fatalf("expected liveness script key %q", LivenessScriptKey)
	} else if script != redisLivenessScript() {
		t.Fatalf("unexpected liveness script content")
	}
	if script := cm.Data[ReadinessScriptKey]; script == "" {
		t.Fatalf("expected readiness script key %q", ReadinessScriptKey)
	} else if script != redisReadinessScript() {
		t.Fatalf("unexpected readiness script content")
	}
}

func TestConfigHash_Changes(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	sec := &security.Settings{}
	h1 := ConfigHash(cr, sec)
	cr.Spec.RedisConfig = map[string]string{"appendonly": "no"}
	h2 := ConfigHash(cr, sec)
	if h1 == h2 {
		t.Fatalf("expected hash to change when config changes")
	}
}

func TestConfigHash_IgnoresRuntimeKeys(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	sec := &security.Settings{}
	cr.Spec.RedisConfig = map[string]string{"maxmemory": "256mb"}
	h1 := ConfigHash(cr, sec)
	cr.Spec.RedisConfig["maxmemory"] = "512mb"
	h2 := ConfigHash(cr, sec)
	if h1 != h2 {
		t.Fatalf("expected config hash to ignore runtime keys")
	}
}

func TestRedisRuntimeHash_Changes(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeStandalone)
	sec := &security.Settings{}
	cr.Spec.RedisConfig = map[string]string{"maxmemory": "256mb"}
	h1 := RedisRuntimeHash(cr, sec)
	cr.Spec.RedisConfig["maxmemory"] = "512mb"
	h2 := RedisRuntimeHash(cr, sec)
	if h1 == h2 {
		t.Fatalf("expected runtime hash to change when runtime config changes")
	}
}

func TestSentinelRuntimeHash_Changes(t *testing.T) {
	t.Parallel()
	cr := crBase("demo", keyvalv1alpha1.ModeSentinel)
	sec := &security.Settings{}
	cr.Spec.SentinelConfig = map[string]string{"down-after-milliseconds": "5000"}
	h1 := SentinelRuntimeHash(cr, sec)
	cr.Spec.SentinelConfig["down-after-milliseconds"] = "10000"
	h2 := SentinelRuntimeHash(cr, sec)
	if h1 == h2 {
		t.Fatalf("expected sentinel runtime hash to change when runtime config changes")
	}
}
