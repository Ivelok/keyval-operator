package phases

import (
	"context"
	"crypto/tls"
	"fmt"

	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

// Security resolves authentication/TLS settings and populates shared state for downstream phases.
func Security(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	reader := state.Dependencies.APIReader
	if reader == nil {
		reader = state.Dependencies.Client
	}
	if reader == nil {
		return controllererrors.WrapFatal(fmt.Errorf("security phase missing reader"))
	}

	settings, err := security.FromSpec(ctx, reader, state.Cluster)
	if err != nil {
		return controllererrors.WrapFatal(fmt.Errorf("resolve security settings: %w", err))
	}

	if settings.HasAuth() {
		state.Logger.V(1).Info("authentication enabled for cluster")
	}

	var redisTLSConfig *tls.Config
	if settings.HasTLS() {
		tlsCfg, err := settings.ClientTLSConfig()
		if err != nil {
			return controllererrors.WrapFatal(fmt.Errorf("build redis tls client config: %w", err))
		}
		redisTLSConfig = tlsCfg
	}

	username, password := security.ResolveAuth(state.Cluster, &settings)

	tlsHash := ""
	if settings.HasTLS() {
		if digest, ok := security.TLSDigest(ctx, reader, state.Cluster); ok {
			tlsHash = digest
		}
	}

	state.Security = reconcile.SecurityState{
		Settings: settings,
		RedisClientOptions: clients.ClientOptions{
			Username:  username,
			Password:  password,
			TLSConfig: redisTLSConfig,
		},
		SentinelClientOptions: clients.SentinelOptions{
			Username:  username,
			Password:  password,
			TLSConfig: redisTLSConfig,
		},
		TLSHash: tlsHash,
	}

	return nil
}
