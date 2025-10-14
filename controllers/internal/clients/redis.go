package clients

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

// goRedisFactory implements Factory using github.com/redis/go-redis/v9 with guarded timeouts and retries.
type goRedisFactory struct {
	cfg FactoryConfig
}

// NewGoRedisFactory returns a Factory backed by go-redis v9 using default configuration.
func NewGoRedisFactory() Factory {
	return NewGoRedisFactoryWithConfig(FactoryConfig{})
}

// NewGoRedisFactoryWithConfig returns a Factory with the supplied configuration.
func NewGoRedisFactoryWithConfig(cfg FactoryConfig) Factory {
	normalized := cfg.ApplyDefaults()
	return &goRedisFactory{cfg: normalized}
}

func (f *goRedisFactory) ForPod(ctx context.Context, pod corev1.Pod, opts ClientOptions) (Client, error) {
	crName := pod.Labels[core.LabelClusterKey]
	if crName == "" {
		return nil, fmt.Errorf("pod %s missing %q label", pod.Name, core.LabelClusterKey)
	}
	dnsHost := fmt.Sprintf("%s.%s-headless.%s.svc", pod.Name, crName, pod.Namespace)
	host := dnsHost
	if opts.TLSConfig == nil && pod.Status.PodIP != "" {
		host = pod.Status.PodIP
	}
	port := redisPortFromPod(pod)
	redisOpts := &redis.Options{
		Addr:                  fmt.Sprintf("%s:%d", host, port),
		DialTimeout:           f.cfg.DialTimeout,
		ReadTimeout:           f.cfg.ReadTimeout,
		WriteTimeout:          f.cfg.WriteTimeout,
		PoolTimeout:           f.cfg.PoolTimeout,
		MinIdleConns:          f.cfg.MinIdleConns,
		MaxRetries:            0,
		ContextTimeoutEnabled: true,
	}
	if opts.Username != "" {
		redisOpts.Username = opts.Username
	}
	if opts.Password != "" {
		redisOpts.Password = opts.Password
	}
	if opts.TLSConfig != nil {
		cfg := opts.TLSConfig.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = dnsHost
		}
		redisOpts.TLSConfig = cfg
	}
	rdb := redis.NewClient(redisOpts)
	return &goRedisClient{
		rdb:          rdb,
		opts:         opts,
		dnsHost:      dnsHost,
		namespace:    pod.Namespace,
		cluster:      crName,
		endpointAddr: redisOpts.Addr,
		cfg:          f.cfg,
	}, nil
}

func redisPortFromPod(pod corev1.Pod) int {
	port := 6379
	for _, c := range pod.Spec.Containers {
		if c.Name != core.RedisContainerName {
			continue
		}
		if len(c.Ports) == 0 {
			break
		}
		selected := c.Ports[0].ContainerPort
		for _, cp := range c.Ports {
			if cp.Name == "redis" {
				selected = cp.ContainerPort
				break
			}
		}
		if selected > 0 {
			port = int(selected)
		}
		break
	}
	return port
}

type goRedisClient struct {
	rdb          *redis.Client
	opts         ClientOptions
	dnsHost      string
	namespace    string
	cluster      string
	endpointAddr string
	cfg          FactoryConfig
}

func (c *goRedisClient) Role(ctx context.Context) (string, error) {
	info, err := c.ReplicationInfo(ctx)
	if err == nil && info.Role != "" {
		return info.Role, nil
	}
	var raw string
	if err := c.execDefault(ctx, "ROLE", c.cfg.OperationTimeout, func(inner context.Context) error {
		res, execErr := c.rdb.Do(inner, "ROLE").Text()
		if execErr != nil {
			return execErr
		}
		raw = res
		return nil
	}); err != nil {
		return "", err
	}
	if strings.HasPrefix(raw, "master") {
		return "master", nil
	}
	return "replica", nil
}

func (c *goRedisClient) ReplicationInfo(ctx context.Context) (ReplicationInfo, error) {
	var raw string
	if err := c.execDefault(ctx, "INFO replication", c.cfg.OperationTimeout, func(inner context.Context) error {
		res, execErr := c.rdb.Info(inner, "replication").Result()
		if execErr != nil {
			return execErr
		}
		raw = res
		return nil
	}); err != nil {
		return ReplicationInfo{}, err
	}
	fields := parseInfoMap(raw)
	info := ReplicationInfo{}
	if v := fields["role"]; v != "" {
		switch v {
		case "master":
			info.Role = "master"
		case "slave", "replica":
			info.Role = "replica"
		default:
			info.Role = v
		}
	}
	info.MasterHost = fields["master_host"]
	info.MasterLinkStatus = fields["master_link_status"]
	if v := fields["master_port"]; v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			info.MasterPort = port
		}
	}
	if v := fields["master_last_io_seconds_ago"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			info.MasterLastIOSecondsAgo = n
		}
	}
	if v := fields["master_sync_in_progress"]; v != "" {
		info.MasterSyncInProgress = v == "1"
	}
	if v := fields["master_repl_offset"]; v != "" {
		if off, err := strconv.ParseInt(v, 10, 64); err == nil {
			info.MasterReplOffset = off
		}
	}
	if v := fields["slave_repl_offset"]; v != "" {
		if off, err := strconv.ParseInt(v, 10, 64); err == nil {
			info.ReplicaReplOffset = off
		}
	}
	if v := fields["slave_priority"]; v != "" {
		if prio, err := strconv.Atoi(v); err == nil {
			info.SlavePriority = prio
		}
	}
	return info, nil
}

func (c *goRedisClient) ReplicaOf(ctx context.Context, host string, port int) error {
	return c.execDefault(ctx, "REPLICAOF", c.cfg.OperationTimeout, func(inner context.Context) error {
		return c.rdb.Do(inner, "REPLICAOF", host, port).Err()
	})
}

func (c *goRedisClient) NoOne(ctx context.Context) error {
	return c.execDefault(ctx, "REPLICAOF NO ONE", c.cfg.OperationTimeout, func(inner context.Context) error {
		return c.rdb.Do(inner, "REPLICAOF", "NO", "ONE").Err()
	})
}

func (c *goRedisClient) DBSize(ctx context.Context) (int64, error) {
	var size int64
	err := c.execDefault(ctx, "DBSIZE", c.cfg.OperationTimeout, func(inner context.Context) error {
		res, execErr := c.rdb.DBSize(inner).Result()
		if execErr != nil {
			return execErr
		}
		size = res
		return nil
	})
	return size, err
}

func (c *goRedisClient) ResetSentinel(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster) error {
	if cr == nil || cr.Spec.Mode != keyvalv1alpha1.ModeSentinel {
		return nil
	}
	endpoints := sentinelServiceEndpoints(cr, c.namespaceLabel())
	if len(endpoints) == 0 {
		return fmt.Errorf("no sentinel endpoints available")
	}
	var errs []error
	for _, endpoint := range endpoints {
		opts := &redis.Options{
			Addr:                  endpoint,
			DialTimeout:           c.cfg.DialTimeout,
			ReadTimeout:           c.cfg.ReadTimeout,
			WriteTimeout:          c.cfg.WriteTimeout,
			PoolTimeout:           c.cfg.PoolTimeout,
			MinIdleConns:          c.cfg.MinIdleConns,
			MaxRetries:            0,
			ContextTimeoutEnabled: true,
		}
		if c.opts.Username != "" {
			opts.Username = c.opts.Username
		}
		if c.opts.Password != "" {
			opts.Password = c.opts.Password
		}
		if c.opts.TLSConfig != nil {
			cfg := c.opts.TLSConfig.Clone()
			if cfg.ServerName == "" {
				host, _, splitErr := net.SplitHostPort(endpoint)
				if splitErr == nil {
					cfg.ServerName = host
				}
			}
			opts.TLSConfig = cfg
		}
		client := redis.NewClient(opts)
		err := c.exec(ctx, "SENTINEL RESET", c.cfg.SentinelOperationTimeout, endpoint, func(inner context.Context) error {
			return client.Do(inner, "SENTINEL", "RESET", cr.Name).Err()
		})
		_ = client.Close()
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", endpoint, err))
	}
	return errors.Join(errs...)
}

func (c *goRedisClient) ConfigGet(ctx context.Context, parameter string) (string, bool, error) {
	var result map[string]string
	if err := c.execDefault(ctx, "CONFIG GET", c.cfg.OperationTimeout, func(inner context.Context) error {
		res, execErr := c.rdb.ConfigGet(inner, parameter).Result()
		if execErr != nil {
			return execErr
		}
		result = res
		return nil
	}); err != nil {
		return "", false, err
	}
	if result == nil {
		return "", false, nil
	}
	val, ok := result[parameter]
	return val, ok, nil
}

func (c *goRedisClient) ConfigSet(ctx context.Context, parameter, value string) error {
	return c.execDefault(ctx, "CONFIG SET", c.cfg.OperationTimeout, func(inner context.Context) error {
		return c.rdb.ConfigSet(inner, parameter, value).Err()
	})
}

func (c *goRedisClient) ConfigRewrite(ctx context.Context) error {
	return c.execDefault(ctx, "CONFIG REWRITE", c.cfg.OperationTimeout, func(inner context.Context) error {
		return c.rdb.ConfigRewrite(inner).Err()
	})
}

func (c *goRedisClient) Auth(ctx context.Context, username, password string) error {
	if password == "" {
		return nil
	}
	return c.execDefault(ctx, "AUTH", c.cfg.OperationTimeout, func(inner context.Context) error {
		if username != "" {
			return c.rdb.Do(inner, "AUTH", username, password).Err()
		}
		return c.rdb.Do(inner, "AUTH", password).Err()
	})
}

func (c *goRedisClient) execDefault(ctx context.Context, command string, timeout time.Duration, fn func(context.Context) error) error {
	endpoint := c.endpointAddr
	if endpoint == "" {
		endpoint = c.dnsHost
	}
	return c.exec(ctx, command, timeout, endpoint, fn)
}

func (c *goRedisClient) exec(ctx context.Context, command string, timeout time.Duration, endpoint string, fn func(context.Context) error) error {
	attempts := c.cfg.maxAttempts()
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			observability.AddRedisClientRetries(c.namespaceLabel(), c.cluster, command, endpoint, 1)
		}
		innerCtx, cancel := commandContext(ctx, timeout)
		err := fn(innerCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if isTimeoutError(err) {
			observability.IncExternalCallTimeout(c.namespaceLabel(), c.cluster, command, endpoint)
			logging.FromContext(ctx).Error(err, "redis command timed out", "command", command, "endpoint", endpoint, "attempt", attempt, "duration", timeout)
		}
		retryTimeout := timeout <= 0
		if timeout > 0 {
			retryTimeout = true
		}
		if !retryableError(err, retryTimeout) || attempt == attempts {
			return err
		}
		backoff := c.cfg.nextBackoff(attempt + 1)
		c.cfg.sleep(backoff)
	}
	return lastErr
}

func (c *goRedisClient) namespaceLabel() string {
	if c.namespace != "" {
		return c.namespace
	}
	return "default"
}

func parseInfoMap(info string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if idx := strings.IndexByte(line, ':'); idx > 0 {
			key := strings.TrimSpace(line[:idx])
			val := strings.TrimSpace(line[idx+1:])
			out[key] = val
		}
	}
	return out
}

func commandContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}
