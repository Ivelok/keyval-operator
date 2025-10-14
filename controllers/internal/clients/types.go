package clients

import (
	"context"
	"crypto/tls"
	"time"

	corev1 "k8s.io/api/core/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

// ReplicationInfo contains selected fields from `INFO replication`.
type ReplicationInfo struct {
	Role                   string
	MasterHost             string
	MasterPort             int
	MasterLinkStatus       string
	MasterLastIOSecondsAgo int
	MasterSyncInProgress   bool
	MasterReplOffset       int64
	ReplicaReplOffset      int64
	SlavePriority          int
}

// ClientOptions describes authentication and transport configuration for Redis clients.
type ClientOptions struct {
	Username  string
	Password  string
	TLSConfig *tls.Config
}

// Client provides the minimal Redis/Sentinel operations used by the replication ensurer.
type Client interface {
	Role(ctx context.Context) (string, error)
	ReplicationInfo(ctx context.Context) (ReplicationInfo, error)
	ReplicaOf(ctx context.Context, host string, port int) error
	NoOne(ctx context.Context) error
	ResetSentinel(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster) error
	ConfigGet(ctx context.Context, parameter string) (value string, found bool, err error)
	ConfigSet(ctx context.Context, parameter, value string) error
	ConfigRewrite(ctx context.Context) error
	Auth(ctx context.Context, username, password string) error
	DBSize(ctx context.Context) (int64, error)
}

// Factory provides a Client for a given Pod.
type Factory interface {
	ForPod(ctx context.Context, pod corev1.Pod, opts ClientOptions) (Client, error)
}

// SentinelClient is the minimal client for Redis Sentinel operations we need.
type SentinelClient interface {
	GetMasterAddrByName(ctx context.Context, name string) (host string, port int, err error)
	Failover(ctx context.Context, name string) error
	Set(ctx context.Context, name, option, value string) error
	Master(ctx context.Context, name string) (map[string]string, error)
	CheckQuorum(ctx context.Context, name string) (bool, error)
	Reset(ctx context.Context, name string) error
}

// SentinelOptions describes connection parameters for Sentinel clients.
type SentinelOptions struct {
	Username  string
	Password  string
	TLSConfig *tls.Config
}

// SentinelFactory produces a SentinelClient for a given Pod.
type SentinelFactory interface {
	ForPod(ctx context.Context, pod corev1.Pod, opts SentinelOptions) (SentinelClient, error)
}

// FactoryConfig controls connection behaviour for Redis/Sentinel clients produced by the factory.
type FactoryConfig struct {
	DialTimeout              time.Duration
	ReadTimeout              time.Duration
	WriteTimeout             time.Duration
	PoolTimeout              time.Duration
	OperationTimeout         time.Duration
	SentinelOperationTimeout time.Duration
	MaxRetries               int
	RetryInitialBackoff      time.Duration
	RetryMaxBackoff          time.Duration
	RetryBackoffFactor       float64
	RetryJitter              float64
	MinIdleConns             int
	sleepFunc                func(time.Duration)
}

// ApplyDefaults normalizes the configuration by filling in zero values with sensible defaults.
func (cfg FactoryConfig) ApplyDefaults() FactoryConfig {
	const (
		defaultDialTimeout      = 3 * time.Second
		defaultReadTimeout      = 2 * time.Second
		defaultWriteTimeout     = 2 * time.Second
		defaultPoolTimeout      = 2 * time.Second
		defaultOperationTimeout = 2 * time.Second
		defaultSentinelTimeout  = 5 * time.Second
		defaultRetries          = 2
	)

	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = defaultReadTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}
	if cfg.PoolTimeout <= 0 {
		cfg.PoolTimeout = defaultPoolTimeout
	}
	if cfg.OperationTimeout <= 0 {
		cfg.OperationTimeout = defaultOperationTimeout
	}
	if cfg.SentinelOperationTimeout <= 0 {
		cfg.SentinelOperationTimeout = defaultSentinelTimeout
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = defaultRetries
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = defaultRetries
	}
	if cfg.RetryInitialBackoff <= 0 {
		cfg.RetryInitialBackoff = 200 * time.Millisecond
	}
	if cfg.RetryMaxBackoff <= 0 {
		cfg.RetryMaxBackoff = time.Second
	}
	if cfg.RetryBackoffFactor <= 0 {
		cfg.RetryBackoffFactor = 2
	}
	if cfg.RetryJitter < 0 {
		cfg.RetryJitter = 0
	}
	if cfg.RetryJitter > 1 {
		cfg.RetryJitter = 1
	}
	if cfg.MinIdleConns < 0 {
		cfg.MinIdleConns = 0
	}
	if cfg.MinIdleConns == 0 {
		cfg.MinIdleConns = 1
	}
	if cfg.sleepFunc == nil {
		cfg.sleepFunc = time.Sleep
	}
	return cfg
}

// maxAttempts returns the total number of attempts (initial call + retries).
func (cfg FactoryConfig) maxAttempts() int {
	retries := cfg.MaxRetries
	if retries < 0 {
		retries = 0
	}
	return 1 + retries
}

// sleep sleeps for the requested duration using the configured sleep function.
func (cfg FactoryConfig) sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	cfg.sleepFunc(d)
}
