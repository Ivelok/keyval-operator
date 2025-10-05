package clients

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

// goRedisSentinelFactory implements SentinelFactory using go-redis with explicit timeouts and retries.
type goRedisSentinelFactory struct {
	kube client.Client
	cfg  FactoryConfig
}

// NewGoRedisSentinelFactory returns a SentinelFactory backed by go-redis using default configuration.
func NewGoRedisSentinelFactory(c client.Client) SentinelFactory {
	return NewGoRedisSentinelFactoryWithConfig(c, FactoryConfig{})
}

// NewGoRedisSentinelFactoryWithConfig returns a SentinelFactory with supplied configuration.
func NewGoRedisSentinelFactoryWithConfig(c client.Client, cfg FactoryConfig) SentinelFactory {
	normalized := cfg.ApplyDefaults()
	return &goRedisSentinelFactory{kube: c, cfg: normalized}
}

func (f *goRedisSentinelFactory) ForPod(ctx context.Context, pod corev1.Pod, opts SentinelOptions) (SentinelClient, error) {
	crName := pod.Labels[core.LabelClusterKey]
	if crName == "" {
		return nil, fmt.Errorf("pod %s missing %q label", pod.Name, core.LabelClusterKey)
	}
	addr := ""
	if f.kube != nil {
		var cr keyvalv1alpha1.KeyValCluster
		if err := f.kube.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: crName}, &cr); err == nil {
			addr = sentinelPodAddress(&cr, pod)
		}
	}
	if addr == "" {
		addr = fmt.Sprintf("%s-sentinel.%s.svc:26379", crName, pod.Namespace)
	}
	redisOpts := &redis.Options{
		Addr:                  addr,
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
			host, _, err := net.SplitHostPort(addr)
			if err == nil {
				cfg.ServerName = host
			}
		}
		redisOpts.TLSConfig = cfg
	}
	rdb := redis.NewClient(redisOpts)
	return &goRedisSentinel{
		rdb:       rdb,
		cfg:       f.cfg,
		cluster:   crName,
		namespace: pod.Namespace,
		endpoint:  redisOpts.Addr,
	}, nil
}

type goRedisSentinel struct {
	rdb       *redis.Client
	cfg       FactoryConfig
	cluster   string
	namespace string
	endpoint  string
}

func (s *goRedisSentinel) GetMasterAddrByName(ctx context.Context, name string) (string, int, error) {
	var host string
	var port int
	err := s.exec(ctx, "SENTINEL GET-MASTER-ADDR-BY-NAME", func(inner context.Context) error {
		arr, execErr := s.rdb.Do(inner, "SENTINEL", "get-master-addr-by-name", name).Slice()
		if execErr != nil {
			return execErr
		}
		if len(arr) != 2 {
			return fmt.Errorf("unexpected reply size: %d", len(arr))
		}
		hostVal, _ := arr[0].(string)
		host = hostVal
		switch v := arr[1].(type) {
		case string:
			_, _ = fmt.Sscanf(v, "%d", &port)
		case int64:
			port = int(v)
		}
		if host == "" || port == 0 {
			return fmt.Errorf("invalid master addr: %v", arr)
		}
		return nil
	})
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

func (s *goRedisSentinel) Failover(ctx context.Context, name string) error {
	return s.exec(ctx, "SENTINEL FAILOVER", func(inner context.Context) error {
		return s.rdb.Do(inner, "SENTINEL", "FAILOVER", name).Err()
	})
}

func (s *goRedisSentinel) Set(ctx context.Context, name, option, value string) error {
	return s.exec(ctx, "SENTINEL SET", func(inner context.Context) error {
		return s.rdb.Do(inner, "SENTINEL", "SET", name, option, value).Err()
	})
}

func (s *goRedisSentinel) Master(ctx context.Context, name string) (map[string]string, error) {
	var result map[string]string
	err := s.exec(ctx, "SENTINEL MASTER", func(inner context.Context) error {
		res, execErr := s.rdb.Do(inner, "SENTINEL", "MASTER", name).Result()
		if execErr != nil {
			return execErr
		}
		switch v := res.(type) {
		case []interface{}:
			converted, convErr := convertSliceToStringMap(v)
			if convErr != nil {
				return convErr
			}
			result = converted
		case map[interface{}]interface{}:
			m := make(map[string]string, len(v))
			for key, val := range v {
				ks := fmt.Sprintf("%v", key)
				m[ks] = fmt.Sprintf("%v", val)
			}
			result = m
		case map[string]string:
			result = v
		default:
			return fmt.Errorf("unexpected sentinel master reply %T", res)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *goRedisSentinel) CheckQuorum(ctx context.Context, name string) (bool, error) {
	var ok bool
	err := s.exec(ctx, "SENTINEL CKQUORUM", func(inner context.Context) error {
		res, execErr := s.rdb.Do(inner, "SENTINEL", "CKQUORUM", name).Result()
		if execErr != nil {
			return execErr
		}
		switch v := res.(type) {
		case string:
			ok = strings.Contains(strings.ToLower(v), "ok")
		case []byte:
			ok = strings.Contains(strings.ToLower(string(v)), "ok")
		default:
			ok = true
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (s *goRedisSentinel) Reset(ctx context.Context, name string) error {
	return s.exec(ctx, "SENTINEL RESET", func(inner context.Context) error {
		return s.rdb.Do(inner, "SENTINEL", "RESET", name).Err()
	})
}

func (s *goRedisSentinel) exec(ctx context.Context, command string, fn func(context.Context) error) error {
	attempts := s.cfg.maxAttempts()
	var lastErr error
	timeout := s.cfg.SentinelOperationTimeout
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			observability.AddRedisClientRetries(s.namespace, s.cluster, command, s.endpoint, 1)
		}
		innerCtx, cancel := commandContext(ctx, timeout)
		err := fn(innerCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if isTimeoutError(err) {
			observability.IncExternalCallTimeout(s.namespace, s.cluster, command, s.endpoint)
			logging.FromContext(ctx).Error(err, "sentinel command timed out", "command", command, "endpoint", s.endpoint, "attempt", attempt, "duration", timeout)
		}
		retryTimeout := timeout <= 0
		if timeout > 0 {
			retryTimeout = true
		}
		if !retryableError(err, retryTimeout) || attempt == attempts {
			return err
		}
		backoff := s.cfg.nextBackoff(attempt + 1)
		s.cfg.sleep(backoff)
	}
	return lastErr
}

func (s *goRedisSentinel) Close() error {
	return s.rdb.Close()
}

func convertSliceToStringMap(arr []interface{}) (map[string]string, error) {
	if len(arr)%2 != 0 {
		return nil, fmt.Errorf("unexpected sentinel master payload length: %d", len(arr))
	}
	out := make(map[string]string, len(arr)/2)
	for i := 0; i < len(arr); i += 2 {
		key, _ := arr[i].(string)
		out[key] = fmt.Sprintf("%v", arr[i+1])
	}
	return out, nil
}
