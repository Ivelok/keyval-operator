package main

import (
	"errors"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/ivelok/keyval-operator/controllers"
)

func applyClientConfigEnvOverrides(cfg *controllers.ClientFactoryConfig) {
	if cfg == nil {
		return
	}
	overrideDurationFromEnv("KEYVAL_REDIS_DIAL_TIMEOUT", &cfg.DialTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_READ_TIMEOUT", &cfg.ReadTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_WRITE_TIMEOUT", &cfg.WriteTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_POOL_TIMEOUT", &cfg.PoolTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_OPERATION_TIMEOUT", &cfg.OperationTimeout)
	overrideDurationFromEnv("KEYVAL_SENTINEL_OPERATION_TIMEOUT", &cfg.SentinelOperationTimeout)
	overrideDurationFromEnv("KEYVAL_REDIS_RETRY_INITIAL_BACKOFF", &cfg.RetryInitialBackoff)
	overrideDurationFromEnv("KEYVAL_REDIS_RETRY_MAX_BACKOFF", &cfg.RetryMaxBackoff)
	overrideFloatFromEnv("KEYVAL_REDIS_RETRY_BACKOFF_FACTOR", &cfg.RetryBackoffFactor)
	overrideFloatFromEnv("KEYVAL_REDIS_RETRY_JITTER", &cfg.RetryJitter)
	overrideIntFromEnv("KEYVAL_REDIS_MAX_RETRIES", &cfg.MaxRetries)
	overrideIntFromEnv("KEYVAL_REDIS_MIN_IDLE_CONNS", &cfg.MinIdleConns)
}

func overrideDurationFromEnv(key string, target *time.Duration) {
	if target == nil {
		return
	}
	if val, ok := os.LookupEnv(key); ok {
		parsed, err := time.ParseDuration(val)
		if err != nil {
			setupLog.Info("invalid duration override", "key", key, "value", val, "error", err)
			return
		}
		*target = parsed
	}
}

func overrideIntFromEnv(key string, target *int) {
	if target == nil {
		return
	}
	if val, ok := os.LookupEnv(key); ok {
		parsed, err := strconv.Atoi(val)
		if err != nil {
			setupLog.Info("invalid integer override", "key", key, "value", val, "error", err)
			return
		}
		*target = parsed
	}
}

func overrideFloatFromEnv(key string, target *float64) {
	if target == nil {
		return
	}
	if val, ok := os.LookupEnv(key); ok {
		parsed, err := strconv.ParseFloat(val, 64)
		if err != nil {
			setupLog.Info("invalid float override", "key", key, "value", val, "error", err)
			return
		}
		if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			setupLog.Info("invalid float override", "key", key, "value", val, "error", errors.New("non-finite float"))
			return
		}
		*target = parsed
	}
}
