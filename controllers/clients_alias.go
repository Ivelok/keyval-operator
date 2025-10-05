package controllers

import (
	"sigs.k8s.io/controller-runtime/pkg/client"

	clients "github.com/ivelok/keyval-operator/controllers/internal/clients"
)

// Alias exported types so existing controller code continues to compile.
type (
	ReplicationInfo     = clients.ReplicationInfo
	Client              = clients.Client
	ClientFactory       = clients.Factory
	ClientOptions       = clients.ClientOptions
	SentinelClient      = clients.SentinelClient
	SentinelFactory     = clients.SentinelFactory
	SentinelOptions     = clients.SentinelOptions
	ClientFactoryConfig = clients.FactoryConfig
)

// NewGoRedisFactory exposes the go-redis client factory to external packages.
func NewGoRedisFactory() ClientFactory {
	return clients.NewGoRedisFactory()
}

// NewGoRedisFactoryWithConfig exposes the configurable factory constructor.
func NewGoRedisFactoryWithConfig(cfg ClientFactoryConfig) ClientFactory {
	return clients.NewGoRedisFactoryWithConfig(cfg)
}

// NewGoRedisSentinelFactory exposes the go-redis Sentinel factory to external packages.
func NewGoRedisSentinelFactory(c client.Client) SentinelFactory {
	return clients.NewGoRedisSentinelFactory(c)
}

// NewGoRedisSentinelFactoryWithConfig exposes the configurable sentinel factory constructor.
func NewGoRedisSentinelFactoryWithConfig(c client.Client, cfg ClientFactoryConfig) SentinelFactory {
	return clients.NewGoRedisSentinelFactoryWithConfig(c, cfg)
}

// Ensure aliases are used to satisfy interface conversions at compile time.
var (
	_ ClientFactory   = NewGoRedisFactory()
	_ SentinelFactory = NewGoRedisSentinelFactory(nil)
)
