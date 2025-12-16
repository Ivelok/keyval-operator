package controllers

import "time"

// DefaultClientFactoryConfig returns the controller's default Redis/Sentinel client configuration.
//
// Keep the defaults in README.md (Controller Configuration) in sync with this function.
func DefaultClientFactoryConfig() ClientFactoryConfig {
	return ClientFactoryConfig{
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
	}
}
