package runtimeconfig

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	clientspkg "github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

// Mode describes the outcome of a runtime configuration attempt.
type Mode string

const (
	ModeSkipped      Mode = "Skipped"
	ModeNoChange     Mode = "NoChange"
	ModeApplied      Mode = "Applied"
	ModeNeedsRestart Mode = "NeedsRestart"
	ModeFailed       Mode = "Failed"
)

// Result captures the outcome of runtime configuration synchronization.
type Result struct {
	Component    string
	Mode         Mode
	ChangedKeys  []string
	Message      string
	Err          error
	RequeueAfter time.Duration
}

func (r Result) withError(err error, msg string) Result {
	r.Mode = ModeFailed
	r.Err = err
	if msg != "" {
		r.Message = msg
	} else if err != nil {
		r.Message = err.Error()
	}
	return r
}

var redisRuntimeAllow = map[string]struct{}{
	"maxmemory":               {},
	"maxmemory-policy":        {},
	"maxmemory-samples":       {},
	"lfu-log-factor":          {},
	"lfu-decay-time":          {},
	"repl-timeout":            {},
	"repl-backlog-size":       {},
	"repl-backlog-ttl":        {},
	"tcp-keepalive":           {},
	"hz":                      {},
	"stream-node-max-bytes":   {},
	"stream-node-max-entries": {},
}

var sentinelRuntimeAllow = map[string]struct{}{
	"down-after-milliseconds": {},
	"failover-timeout":        {},
	"parallel-syncs":          {},
}

// ApplyRedisRuntime attempts to align redis.conf with spec without restarting pods.
func ApplyRedisRuntime(
	ctx context.Context,
	kube client.Client,
	factory clientspkg.Factory,
	cr *keyvalv1alpha1.KeyValCluster,
	pods []corev1.Pod,
	desiredHash string,
	logger logr.Logger,
	rec record.EventRecorder,
	sec *security.Settings,
) Result {
	res := Result{Component: "redis"}
	if factory == nil {
		res.Mode = ModeSkipped
		res.Message = "redis client factory unavailable"
		return res
	}
	if len(pods) == 0 {
		res.Mode = ModeNoChange
		res.Message = "no redis pods"
		return res
	}
	restartKey := ""
	for _, pod := range pods {
		if !runtimepkg.IsPodReady(&pod) {
			res.Mode = ModeSkipped
			res.Message = "redis pods not ready"
			res.RequeueAfter = 5 * time.Second
			return res
		}
	}

	desired := resources.EffectiveRedisConfig(cr, sec)
	keys := make([]string, 0, len(desired))
	for k := range desired {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	username, password := security.ResolveAuth(cr, sec)
	var tlsConfig *tls.Config
	if sec != nil && sec.HasTLS() {
		cfg, err := sec.ClientTLSConfig()
		if err != nil {
			return Result{Component: "redis"}.withError(controllererrors.WrapFatal(fmt.Errorf("build tls client config: %w", err)), "build tls client config")
		}
		tlsConfig = cfg
	}
	clientOpts := clientspkg.ClientOptions{Username: username, Password: password, TLSConfig: tlsConfig}

	perPodChanges := make(map[string]map[string]string)
	changedKeys := map[string]struct{}{}

	for _, pod := range pods {
		client, err := factory.ForPod(ctx, pod, clientOpts)
		if err != nil {
			wrap := controllererrors.WrapTransient(fmt.Errorf("redis client for %s: %w", pod.Name, err))
			return Result{Component: "redis"}.withError(wrap, fmt.Sprintf("redis client for %s", pod.Name))
		}
		defer closeIfPossible(client)

		pending := make(map[string]string)
		needsRestart := false

		for _, key := range keys {
			desiredRaw := strings.TrimSpace(desired[key])
			desiredVal := normalizeRedisValue(key, desiredRaw)
			current, ok, err := client.ConfigGet(ctx, key)
			if err != nil {
				wrap := controllererrors.WrapTransient(fmt.Errorf("CONFIG GET %s: %w", key, err))
				return Result{Component: "redis"}.withError(wrap, fmt.Sprintf("CONFIG GET %s", key))
			}
			if !ok {
				current = ""
			}
			currentVal := normalizeRedisValue(key, strings.TrimSpace(current))
			if currentVal == desiredVal {
				continue
			}
			if _, allowed := redisRuntimeAllow[key]; !allowed {
				logger.Info("runtime config requires restart", "pod", pod.Name, "key", key, "desired", desiredRaw, "current", current)
				needsRestart = true
				restartKey = key
				break
			}
			pending[key] = desiredRaw
			changedKeys[key] = struct{}{}
		}

		if needsRestart {
			res.Mode = ModeNeedsRestart
			if restartKey != "" {
				res.Message = fmt.Sprintf("redis parameter %s requires restart", restartKey)
			} else if res.Message == "" {
				res.Message = "redis config change requires restart"
			}
			return res
		}
		if len(pending) > 0 {
			perPodChanges[pod.Name] = pending
		}
	}

	if len(perPodChanges) == 0 {
		if err := patchConfigHashAnnotations(ctx, kube, pods, desiredHash); err != nil {
			wrap := controllererrors.WrapTransient(fmt.Errorf("patch redis pod annotations: %w", err))
			return Result{Component: "redis"}.withError(wrap, "patch redis pod annotations")
		}
		res.Mode = ModeNoChange
		res.Message = "redis config already in sync"
		return res
	}

	for _, pod := range pods {
		opChanges := perPodChanges[pod.Name]
		if len(opChanges) == 0 {
			continue
		}
		client, err := factory.ForPod(ctx, pod, clientOpts)
		if err != nil {
			wrap := controllererrors.WrapTransient(fmt.Errorf("redis client for %s: %w", pod.Name, err))
			return Result{Component: "redis"}.withError(wrap, fmt.Sprintf("redis client for %s", pod.Name))
		}
		defer closeIfPossible(client)

		for key, val := range opChanges {
			if err := client.ConfigSet(ctx, key, val); err != nil {
				wrap := controllererrors.WrapTransient(fmt.Errorf("CONFIG SET %s: %w", key, err))
				return Result{Component: "redis"}.withError(wrap, fmt.Sprintf("CONFIG SET %s", key))
			}
		}
		if err := client.ConfigRewrite(ctx); err != nil {
			if isReadOnlyConfigRewrite(err) {
				logger.Info("redis runtime config requires restart", "pod", pod.Name, "error", err.Error())
				res.Mode = ModeNeedsRestart
				keysList := collectChangedKeys(changedKeys)
				res.ChangedKeys = keysList
				if len(keysList) > 0 {
					res.Message = fmt.Sprintf("redis config rewrite requires restart (%s)", strings.Join(keysList, ","))
				} else {
					res.Message = "redis config rewrite requires restart"
				}
				return res
			}
			wrap := controllererrors.WrapTransient(fmt.Errorf("CONFIG REWRITE %s: %w", pod.Name, err))
			return Result{Component: "redis"}.withError(wrap, fmt.Sprintf("CONFIG REWRITE %s", pod.Name))
		}
	}

	if err := patchConfigHashAnnotations(ctx, kube, pods, desiredHash); err != nil {
		return Result{Component: "redis"}.withError(err, "patch redis pod annotations")
	}

	keysList := collectChangedKeys(changedKeys)

	observability.EventRuntimeConfigApplied(rec, cr, "redis", keysList)
	observability.IncRuntimeConfigApplied(cr, "redis")

	res.Mode = ModeApplied
	res.ChangedKeys = keysList
	res.Message = fmt.Sprintf("redis runtime config applied: %s", strings.Join(keysList, ","))
	return res
}

func isReadOnlyConfigRewrite(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "read-only file system") {
		return true
	}
	if strings.Contains(msg, "rewriting config file") && strings.Contains(msg, "err") {
		return true
	}
	return false
}

func collectChangedKeys(changed map[string]struct{}) []string {
	if len(changed) == 0 {
		return nil
	}
	keys := make([]string, 0, len(changed))
	for k := range changed {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ApplySentinelRuntime aligns sentinel configuration via SENTINEL SET when possible.
func ApplySentinelRuntime(
	ctx context.Context,
	factory clientspkg.SentinelFactory,
	cr *keyvalv1alpha1.KeyValCluster,
	sentinelPods []corev1.Pod,
	logger logr.Logger,
	rec record.EventRecorder,
	sec *security.Settings,
) Result {
	res := Result{Component: "sentinel"}
	if cr.Spec.Mode != keyvalv1alpha1.ModeSentinel {
		res.Mode = ModeNoChange
		res.Message = "sentinel mode disabled"
		return res
	}
	if factory == nil {
		res.Mode = ModeSkipped
		res.Message = "sentinel factory unavailable"
		return res
	}
	if len(sentinelPods) == 0 {
		res.Mode = ModeSkipped
		res.Message = "no sentinel pods"
		return res
	}
	for i := range sentinelPods {
		if !runtimepkg.IsPodReady(&sentinelPods[i]) {
			res.Mode = ModeSkipped
			res.Message = "sentinel pods not ready"
			res.RequeueAfter = 5 * time.Second
			return res
		}
	}

	username, password := security.ResolveAuth(cr, sec)
	var tlsConfig *tls.Config
	if sec != nil && sec.HasTLS() {
		cfg, err := sec.ClientTLSConfig()
		if err != nil {
			wrap := controllererrors.WrapFatal(fmt.Errorf("build tls client config: %w", err))
			return Result{Component: "sentinel"}.withError(wrap, "build tls client config")
		}
		tlsConfig = cfg
	}
	client, err := factory.ForPod(ctx, sentinelPods[0], clientspkg.SentinelOptions{Username: username, Password: password, TLSConfig: tlsConfig})
	if err != nil {
		wrap := controllererrors.WrapTransient(fmt.Errorf("get sentinel client: %w", err))
		return Result{Component: "sentinel"}.withError(wrap, "get sentinel client")
	}
	defer closeIfPossible(client)

	info, err := client.Master(ctx, cr.Name)
	if err != nil {
		res.Mode = ModeSkipped
		res.Message = fmt.Sprintf("query sentinel master state: %v", err)
		res.RequeueAfter = 5 * time.Second
		return res
	}

	desired := resources.EffectiveSentinelConfig(cr, sec)
	pending := map[string]string{}

	for option := range sentinelRuntimeAllow {
		desiredKey := fmt.Sprintf("sentinel %s", option)
		desiredVal := desired[desiredKey]
		current := info[option]
		if strings.TrimSpace(current) == strings.TrimSpace(desiredVal) {
			continue
		}
		pending[option] = desiredVal
	}

	if len(pending) == 0 {
		res.Mode = ModeNoChange
		res.Message = "sentinel config already in sync"
		return res
	}

	for option, val := range pending {
		if err := client.Set(ctx, cr.Name, option, val); err != nil {
			wrap := controllererrors.WrapTransient(fmt.Errorf("SENTINEL SET %s: %w", option, err))
			return Result{Component: "sentinel"}.withError(wrap, fmt.Sprintf("SENTINEL SET %s", option))
		}
	}

	keys := make([]string, 0, len(pending))
	for k := range pending {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	observability.EventRuntimeConfigApplied(rec, cr, "sentinel", keys)
	observability.IncRuntimeConfigApplied(cr, "sentinel")

	res.Mode = ModeApplied
	res.ChangedKeys = keys
	res.Message = fmt.Sprintf("sentinel runtime config applied: %s", strings.Join(keys, ","))
	return res
}

func patchConfigHashAnnotations(ctx context.Context, kube client.Client, pods []corev1.Pod, hash string) error {
	if hash == "" {
		return nil
	}
	for i := range pods {
		pod := pods[i]
		if pod.Annotations != nil && pod.Annotations[resources.ConfigHashAnnotationKey] == hash {
			continue
		}
		base := pod.DeepCopy()
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[resources.ConfigHashAnnotationKey] = hash
		if err := kube.Patch(ctx, &pod, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("patch pod %s annotation: %w", pod.Name, err)
		}
		pods[i].Annotations = pod.Annotations
	}
	return nil
}

func closeIfPossible(obj interface{}) {
	type closer interface{ Close() error }
	if c, ok := obj.(closer); ok {
		_ = c.Close()
	}
}

func normalizeRedisValue(key, value string) string {
	trimmed := strings.TrimSpace(value)
	switch key {
	case "maxmemory", "repl-backlog-size", "stream-node-max-bytes":
		if n, err := parseRedisMemory(trimmed); err == nil {
			return strconv.FormatInt(n, 10)
		}
	case "save":
		if trimmed == "\"\"" || trimmed == "''" {
			return ""
		}
	}
	return trimmed
}

func parseRedisMemory(v string) (int64, error) {
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return 0, nil
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n, nil
	}
	idx := len(v) - 1
	for idx >= 0 && (v[idx] < '0' || v[idx] > '9') {
		idx--
	}
	if idx < 0 {
		return 0, fmt.Errorf("invalid redis memory value %q", v)
	}
	numPart := strings.TrimSpace(v[:idx+1])
	suffix := strings.TrimSpace(v[idx+1:])
	base, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, err
	}
	mult := float64(1)
	switch suffix {
	case "", "b":
		mult = 1
	case "k", "kb":
		mult = 1024
	case "m", "mb":
		mult = 1024 * 1024
	case "g", "gb":
		mult = 1024 * 1024 * 1024
	case "t", "tb":
		mult = 1024 * 1024 * 1024 * 1024
	case "p", "pb":
		mult = 1024 * 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unsupported redis memory suffix %q", suffix)
	}
	return int64(base*mult + 0.5), nil
}
