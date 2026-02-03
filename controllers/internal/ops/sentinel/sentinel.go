package sentinel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	clientspkg "github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

// ErrNoGoodSlave indicates Sentinel refused a manual failover due to lack of a suitable replica.
var ErrNoGoodSlave = errors.New("sentinel NOGOODSLAVE")

func TriggerFailover(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, redisPods, sentinelPods []corev1.Pod, sf clientspkg.SentinelFactory, rf clientspkg.Factory, sec *security.Settings) (string, map[string]keyvalv1alpha1.PodRole, error) {
	if cr.Spec.Mode != keyvalv1alpha1.ModeSentinel {
		return "", nil, errors.New("failover only supported in Sentinel mode")
	}
	if len(redisPods) == 0 {
		return "", nil, errors.New("no pods provided")
	}
	if len(sentinelPods) == 0 {
		return "", nil, errors.New("no sentinel pods available for failover")
	}
	username, password := security.ResolveAuth(cr, sec)
	var tlsConfig *tls.Config
	if sec != nil && sec.HasTLS() {
		cfg, err := sec.ClientTLSConfig()
		if err != nil {
			return "", nil, controllererrors.WrapFatal(fmt.Errorf("build tls client config: %w", err))
		}
		tlsConfig = cfg
	}
	sentinelOpts := clientspkg.SentinelOptions{Username: username, Password: password, TLSConfig: tlsConfig}

	candidates := orderedSentinelCandidates(sentinelPods)
	var sentinel clientspkg.SentinelClient
	var errs []error

	backoff := runtimepkg.Backoff{Initial: 200 * time.Millisecond, Factor: 2, Max: 2 * time.Second}
	for _, sp := range candidates {
		cli, err := sf.ForPod(ctx, sp, sentinelOpts)
		if err != nil {
			errs = append(errs, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("%s: %w", sp.Name, err))))
			continue
		}
		sentinel = cli
		observability.IncFailoverTriggered(cr)
		failErr := runtimepkg.Retry(ctx, 3, backoff, func(int) error {
			opCtx, cancel := runtimepkg.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if e := sentinel.Failover(opCtx, cr.Name); e != nil {
				if strings.Contains(strings.ToUpper(e.Error()), "NOGOODSLAVE") {
					return ErrNoGoodSlave
				}
				return e
			}
			return nil
		})
		if failErr == nil {
			break
		}
		if errors.Is(failErr, ErrNoGoodSlave) {
			return "", nil, ErrNoGoodSlave
		}
		errs = append(errs, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("%s: %w", sp.Name, failErr))))
		sentinel = nil
	}

	if sentinel == nil {
		if len(errs) == 0 {
			return "", nil, errors.New("sentinel failover: no reachable sentinel candidates")
		}
		return "", nil, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("sentinel failover: %w", errors.Join(errs...))))
	}

	var host string
	var port int
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var h string
		var p int
		err := runtimepkg.Retry(ctx, 3, backoff, func(int) error {
			opCtx, cancel := runtimepkg.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			var e error
			h, p, e = sentinel.GetMasterAddrByName(opCtx, cr.Name)
			return e
		})
		if err == nil && h != "" && p > 0 {
			host, port = h, p
			break
		}
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}
	if host == "" || port == 0 {
		return "", nil, errors.New("failed to obtain new master address from sentinel")
	}
	masterName := opreplication.MatchPodByHost(redisPods, host)
	if masterName == "" {
		if net.ParseIP(host) == nil {
			return "", nil, controllererrors.WrapFatal(fmt.Errorf("cannot map sentinel host %q to pod name", host))
		}
	}
	if masterName == "" {
		for _, p := range redisPods {
			if p.Status.PodIP == host {
				masterName = p.Name
				break
			}
		}
	}
	if masterName == "" {
		return "", nil, controllererrors.WrapFatal(fmt.Errorf("unable to determine new master pod for host %q", host))
	}

	_, roles, _, err := opreplication.EnsureReplicationToMaster(ctx, cr, redisPods, rf, masterName, sec)
	if err != nil {
		return "", nil, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("ensure replication to %s: %w", masterName, err)))
	}
	observability.IncFailover(cr, observability.FailoverTypeForced)
	observability.IncFailoverCompleted(cr)
	return masterName, roles, nil
}

func orderedSentinelCandidates(pods []corev1.Pod) []corev1.Pod {
	sorted := runtimepkg.SortedPodsByName(pods)
	ready := make([]corev1.Pod, 0, len(sorted))
	pending := make([]corev1.Pod, 0, len(sorted))
	for _, p := range sorted {
		pod := p
		if runtimepkg.IsPodReady(&pod) {
			ready = append(ready, pod)
		} else {
			pending = append(pending, pod)
		}
	}
	return append(ready, pending...)
}
