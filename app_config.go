package main

import (
	"flag"
	"time"

	"github.com/ivelok/keyval-operator/controllers"
)

// AppConfig owns defaults, env overrides, and flag bindings for the controller process.
type AppConfig struct {
	MetricsAddr    string
	ProbeAddr      string
	LeaderElection bool
	ZapDev         bool

	Client controllers.ClientFactoryConfig
}

func DefaultAppConfig() AppConfig {
	return AppConfig{
		MetricsAddr:    ":8080",
		ProbeAddr:      ":8081",
		LeaderElection: true,
		ZapDev:         true,
		Client: controllers.ClientFactoryConfig{
			DialTimeout:              3 * time.Second,
			ReadTimeout:              2 * time.Second,
			WriteTimeout:             2 * time.Second,
			PoolTimeout:              2 * time.Second,
			OperationTimeout:         2 * time.Second,
			SentinelOperationTimeout: 5 * time.Second,
			MaxRetries:               2,
			RetryInitialBackoff:      200 * time.Millisecond,
			RetryMaxBackoff:          time.Second,
			RetryBackoffFactor:       2.0,
			RetryJitter:              0.1,
			MinIdleConns:             1,
		},
	}
}

func (c *AppConfig) ApplyEnv() {
	applyClientConfigEnvOverrides(&c.Client)
}

func (c *AppConfig) BindFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.MetricsAddr, "metrics-bind-address", c.MetricsAddr, "The address the metric endpoint binds to.")
	fs.StringVar(&c.ProbeAddr, "health-probe-bind-address", c.ProbeAddr, "The address the probe endpoint binds to.")
	fs.BoolVar(&c.LeaderElection, "leader-elect", c.LeaderElection, "Enable leader election for controller manager.")
	fs.BoolVar(&c.ZapDev, "zap-devel", c.ZapDev, "Enable development logging (human-friendly)")

	fs.DurationVar(&c.Client.DialTimeout, "redis-dial-timeout", c.Client.DialTimeout, "Dial timeout for Redis/Sentinel clients.")
	fs.DurationVar(&c.Client.ReadTimeout, "redis-read-timeout", c.Client.ReadTimeout, "Socket read timeout for Redis/Sentinel clients.")
	fs.DurationVar(&c.Client.WriteTimeout, "redis-write-timeout", c.Client.WriteTimeout, "Socket write timeout for Redis/Sentinel clients.")
	fs.DurationVar(&c.Client.PoolTimeout, "redis-pool-timeout", c.Client.PoolTimeout, "Connection pool wait timeout for Redis/Sentinel clients.")
	fs.DurationVar(&c.Client.OperationTimeout, "redis-operation-timeout", c.Client.OperationTimeout, "Per-command timeout for Redis operations.")
	fs.DurationVar(&c.Client.SentinelOperationTimeout, "sentinel-operation-timeout", c.Client.SentinelOperationTimeout, "Per-command timeout for Sentinel operations.")
	fs.DurationVar(&c.Client.RetryInitialBackoff, "redis-retry-backoff-initial", c.Client.RetryInitialBackoff, "Initial backoff between Redis client retries.")
	fs.DurationVar(&c.Client.RetryMaxBackoff, "redis-retry-backoff-max", c.Client.RetryMaxBackoff, "Maximum backoff between Redis client retries.")
	fs.Float64Var(&c.Client.RetryBackoffFactor, "redis-retry-backoff-factor", c.Client.RetryBackoffFactor, "Multiplicative factor for Redis client retry backoff.")
	fs.Float64Var(&c.Client.RetryJitter, "redis-retry-jitter", c.Client.RetryJitter, "Jitter fraction (0-1) applied to Redis client retry backoff.")
	fs.IntVar(&c.Client.MaxRetries, "redis-max-retries", c.Client.MaxRetries, "Maximum number of retries per Redis/Sentinel command.")
	fs.IntVar(&c.Client.MinIdleConns, "redis-min-idle-conns", c.Client.MinIdleConns, "Minimum number of idle connections to maintain in the Redis/Sentinel pool.")
}

func (c *AppConfig) Normalize() {
	c.Client = c.Client.ApplyDefaults()
}

func (c *AppConfig) Validate() error {
	return nil
}
