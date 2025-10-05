package resources

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

const (
	// ConfigHashAnnotationKey is added to PodTemplate to trigger manual rolling under OnDelete.
	ConfigHashAnnotationKey = "keyval.ivelok.io/config-hash"
	// TLSSecretHashAnnotationKey links PodTemplates to TLS secret content for rollout decisions.
	TLSSecretHashAnnotationKey = "keyval.ivelok.io/tls-secret-hash"
)

// ConfigMap builds the ConfigMap with redis.conf and optionally sentinel.conf.
func ConfigMap(cr *keyvalv1alpha1.KeyValCluster, sec *security.Settings) *corev1.ConfigMap {
	name := core.ConfigMapName(cr)
	labels := core.LabelsFor(cr)
	data := map[string]string{
		"redis.conf": buildRedisConfig(cr, sec),
	}
	if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
		data["sentinel.conf"] = buildSentinelConfig(cr, sec)
	}
	data[BootstrapScriptKey] = redisBootstrapScript()
	data[LivenessScriptKey] = redisLivenessScript()
	data[ReadinessScriptKey] = redisReadinessScript()
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Data: data,
	}
}

// ConfigHash computes a stable hash for the effective config content.
func ConfigHash(cr *keyvalv1alpha1.KeyValCluster, sec *security.Settings) string {
	redis := buildRedisConfig(cr, sec)
	sentinel := ""
	if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
		sentinel = buildSentinelConfig(cr, sec)
	}
	bootstrap := redisBootstrapScript()
	liveness := redisLivenessScript()
	readiness := redisReadinessScript()
	h := sha256.New()
	h.Write([]byte(redis))
	h.Write([]byte("\n--\n"))
	h.Write([]byte(sentinel))
	h.Write([]byte("\n--bootstrap\n"))
	h.Write([]byte(bootstrap))
	h.Write([]byte("\n--liveness\n"))
	h.Write([]byte(liveness))
	h.Write([]byte("\n--readiness\n"))
	h.Write([]byte(readiness))
	if sec != nil {
		if sec.Auth.Enabled {
			h.Write([]byte(sec.Auth.SecretName))
			h.Write([]byte(sec.Auth.ResourceVersion))
		}
		if sec.TLS.Enabled {
			h.Write([]byte(sec.TLS.SecretName))
			h.Write([]byte(sec.TLS.ResourceVersion))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func buildRedisConfig(cr *keyvalv1alpha1.KeyValCluster, sec *security.Settings) string {
	content := renderKVConfig(EffectiveRedisConfig(cr, sec))
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	return content + "include /runtime-conf/role.conf\n"
}

func buildSentinelConfig(cr *keyvalv1alpha1.KeyValCluster, sec *security.Settings) string {
	name := cr.Name
	headless := core.HeadlessName(cr)
	masterAddr := fmt.Sprintf("%s-0.%s.%s.svc", cr.Name, headless, cr.Namespace)
	rport, _ := core.RedisPort(cr)

	quorum := int32(1)
	if cr.Spec.SentinelCount != nil && *cr.Spec.SentinelCount > 0 {
		quorum = (*cr.Spec.SentinelCount)/2 + 1
	}

	defaults := EffectiveSentinelConfig(cr, sec)
	port := defaults["port"]
	dir := defaults["dir"]
	resolveHostnames := defaults["sentinel resolve-hostnames"]
	announceHostnames := defaults["sentinel announce-hostnames"]
	downAfter := defaults["sentinel down-after-milliseconds"]
	failoverTimeout := defaults["sentinel failover-timeout"]

	var b strings.Builder
	fmt.Fprintf(&b, "sentinel monitor %s %s %d %d\n", name, masterAddr, rport, quorum)
	fmt.Fprintf(&b, "port %s\n", port)
	fmt.Fprintf(&b, "dir %s\n", dir)
	fmt.Fprintf(&b, "sentinel resolve-hostnames %s\n", resolveHostnames)
	fmt.Fprintf(&b, "sentinel announce-hostnames %s\n", announceHostnames)
	fmt.Fprintf(&b, "sentinel down-after-milliseconds %s %s\n", name, downAfter)
	fmt.Fprintf(&b, "sentinel failover-timeout %s %s\n", name, failoverTimeout)
	fmt.Fprintf(&b, "sentinel parallel-syncs %s %s\n", name, defaults["sentinel parallel-syncs"])

	if pass := defaults["sentinel auth-pass"]; pass != "" {
		fmt.Fprintf(&b, "sentinel auth-pass %s %s\n", name, pass)
	}
	if user := defaults["sentinel auth-user"]; user != "" {
		fmt.Fprintf(&b, "sentinel auth-user %s %s\n", name, user)
	}
	if pass := defaults["requirepass"]; pass != "" {
		fmt.Fprintf(&b, "requirepass %s\n", pass)
	}
	if tlsPort := defaults["tls-port"]; tlsPort != "" {
		fmt.Fprintf(&b, "tls-port %s\n", tlsPort)
		if cert := defaults["tls-cert-file"]; cert != "" {
			fmt.Fprintf(&b, "tls-cert-file %s\n", cert)
		}
		if key := defaults["tls-key-file"]; key != "" {
			fmt.Fprintf(&b, "tls-key-file %s\n", key)
		}
		if ca := defaults["tls-ca-cert-file"]; ca != "" {
			fmt.Fprintf(&b, "tls-ca-cert-file %s\n", ca)
		}
		if authClients := defaults["tls-auth-clients"]; authClients != "" {
			fmt.Fprintf(&b, "tls-auth-clients %s\n", authClients)
		}
		if repl := defaults["tls-replication"]; repl != "" {
			fmt.Fprintf(&b, "tls-replication %s\n", repl)
		}
	}

	return b.String()
}

// EffectiveRedisConfig returns the merged Redis configuration map that will be rendered into redis.conf.
// The returned map is a copy and can be safely mutated by callers.
func EffectiveRedisConfig(cr *keyvalv1alpha1.KeyValCluster, sec *security.Settings) map[string]string {
	cfg := map[string]string{
		"port":       "6379",
		"dir":        "/data",
		"save":       "\"\"", // disable RDB by default
		"appendonly": "yes",  // enable AOF by default
	}
	for k, v := range cr.Spec.RedisConfig {
		cfg[k] = v
	}
	if sec != nil && sec.Auth.Enabled {
		cfg["requirepass"] = sec.Auth.Password
		cfg["masterauth"] = sec.Auth.Password
		if sec.Auth.Username != "" {
			cfg["masteruser"] = sec.Auth.Username
		}
	}
	if sec != nil && sec.TLS.Enabled {
		redisPort, _ := core.RedisPort(cr)
		cfg["tls-port"] = fmt.Sprintf("%d", redisPort)
		cfg["tls-cert-file"] = path.Join(TLSMountPath, sec.TLS.CertKey)
		cfg["tls-key-file"] = path.Join(TLSMountPath, sec.TLS.KeyKey)
		cfg["tls-ca-cert-file"] = path.Join(TLSMountPath, sec.TLS.CACertKey)
		if sec.TLS.RequireClientAuth {
			cfg["tls-auth-clients"] = "yes"
		} else {
			cfg["tls-auth-clients"] = "no"
		}
		cfg["tls-replication"] = "yes"
		if sec.TLS.DisablePlaintext {
			cfg["port"] = "0"
		} else if port, ok := cfg["port"]; !ok || port == "" || port == "0" {
			cfg["port"] = fmt.Sprintf("%d", redisPort)
		}
	}
	return cfg
}

// EffectiveSentinelConfig returns the configuration values rendered into sentinel.conf.
// Keys use the exact directive strings emitted in the config file.
func EffectiveSentinelConfig(cr *keyvalv1alpha1.KeyValCluster, sec *security.Settings) map[string]string {
	cfg := map[string]string{
		"port":                             "26379",
		"dir":                              "/data",
		"sentinel resolve-hostnames":       "yes",
		"sentinel announce-hostnames":      "yes",
		"sentinel down-after-milliseconds": "5000",
		"sentinel failover-timeout":        "60000",
		"sentinel parallel-syncs":          "1",
	}
	for key, target := range map[string]string{
		"port":                    "port",
		"dir":                     "dir",
		"resolve-hostnames":       "sentinel resolve-hostnames",
		"announce-hostnames":      "sentinel announce-hostnames",
		"down-after-milliseconds": "sentinel down-after-milliseconds",
		"failover-timeout":        "sentinel failover-timeout",
		"parallel-syncs":          "sentinel parallel-syncs",
	} {
		if v, ok := cr.Spec.SentinelConfig[key]; ok && v != "" {
			cfg[target] = v
		}
	}
	if sec != nil && sec.Auth.Enabled {
		cfg["requirepass"] = sec.Auth.Password
		cfg["sentinel auth-pass"] = sec.Auth.Password
		if sec.Auth.Username != "" {
			cfg["sentinel auth-user"] = sec.Auth.Username
		}
	}
	if sec != nil && sec.TLS.Enabled {
		sport, _ := core.SentinelPort(cr)
		cfg["tls-port"] = fmt.Sprintf("%d", sport)
		cfg["tls-cert-file"] = path.Join(TLSMountPath, sec.TLS.CertKey)
		cfg["tls-key-file"] = path.Join(TLSMountPath, sec.TLS.KeyKey)
		cfg["tls-ca-cert-file"] = path.Join(TLSMountPath, sec.TLS.CACertKey)
		if sec.TLS.RequireClientAuth {
			cfg["tls-auth-clients"] = "yes"
		} else {
			cfg["tls-auth-clients"] = "no"
		}
		cfg["tls-replication"] = "yes"
		if sec.TLS.DisablePlaintext {
			cfg["port"] = "0"
		} else if port := cfg["port"]; port == "" || port == "0" {
			cfg["port"] = fmt.Sprintf("%d", sport)
		}
	}
	return cfg
}

func renderKVConfig(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		if k == "replicaof" || k == "slaveof" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s %s\n", k, m[k])
	}
	return b.String()
}
