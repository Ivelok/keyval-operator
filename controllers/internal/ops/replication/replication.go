package replication

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	clientspkg "github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	ophealthy "github.com/ivelok/keyval-operator/controllers/internal/ops/health"
	"github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

type (
	Client          = clientspkg.Client
	Factory         = clientspkg.Factory
	ReplicationInfo = clientspkg.ReplicationInfo
	SentinelClient  = clientspkg.SentinelClient
	SentinelFactory = clientspkg.SentinelFactory
	ClientOptions   = clientspkg.ClientOptions
	SentinelOptions = clientspkg.SentinelOptions
)

const (
	defaultWaitTimeout  = 30 * time.Second
	defaultWaitInterval = time.Second
)

// EnsureSource describes how the master was determined during EnsureTopology.
type EnsureSource string

const (
	EnsureSourceUnknown  EnsureSource = ""
	EnsureSourceSentinel EnsureSource = "sentinel"
	EnsureSourceProbe    EnsureSource = "probe"
	EnsureSourceForced   EnsureSource = "forced"
	EnsureSourceCache    EnsureSource = "cache"
)

// EnsureRequest captures inputs for EnsureTopology.
type EnsureRequest struct {
	Cluster         *keyvalv1alpha1.KeyValCluster
	RedisPods       []corev1.Pod
	SentinelPods    []corev1.Pod
	Factory         Factory
	SentinelFactory SentinelFactory
	Security        *security.Settings
	ForceMaster     string
	ForceReason     string
	WaitTimeout     time.Duration
	WaitInterval    time.Duration
	MasterCache     *MasterAddressCache
}

// EnsureResult represents the outcome of EnsureTopology.
type EnsureResult struct {
	Changed bool
	Master  string
	Roles   map[string]keyvalv1alpha1.PodRole
	Infos   map[string]ReplicationInfo
	Drift   []string
	Pending []string
	Source  EnsureSource
	Reason  string
}

type ensureOptions struct {
	username     string
	password     string
	tlsConfig    *tls.Config
	threshold    int32
	waitTimeout  time.Duration
	waitInterval time.Duration
}

// EnsureTopology enforces a single-master topology using Sentinel when
// available, otherwise falling back to probing Redis pods directly.
func EnsureTopology(ctx context.Context, req EnsureRequest) (EnsureResult, error) {
	res := EnsureResult{
		Roles: make(map[string]keyvalv1alpha1.PodRole, len(req.RedisPods)),
		Infos: make(map[string]ReplicationInfo, len(req.RedisPods)),
	}
	if req.Cluster == nil {
		return res, errors.New("cluster is nil")
	}
	if req.Factory == nil {
		return res, errors.New("client factory is nil")
	}
	if len(req.RedisPods) == 0 {
		return res, errors.New("no pods provided")
	}
	username, password := security.ResolveAuth(req.Cluster, req.Security)
	opts := ensureOptions{
		username:     username,
		password:     password,
		threshold:    ophealthy.LagThresholdSeconds(req.Cluster),
		waitTimeout:  req.WaitTimeout,
		waitInterval: req.WaitInterval,
	}
	if req.Security != nil && req.Security.HasTLS() {
		tlsConfig, err := req.Security.ClientTLSConfig()
		if err != nil {
			return res, controllererrors.WrapFatal(fmt.Errorf("build tls client config: %w", err))
		}
		opts.tlsConfig = tlsConfig
	}
	if opts.waitTimeout <= 0 {
		opts.waitTimeout = defaultWaitTimeout
	}
	if opts.waitInterval <= 0 {
		opts.waitInterval = defaultWaitInterval
	}

	cacheKey := MasterCacheKey{}
	if req.Cluster != nil {
		cacheKey = CacheKeyForCluster(req.Cluster)
	}
	if req.MasterCache != nil && req.Cluster != nil {
		if candidate, detail, ok := req.MasterCache.Lookup(cacheKey); ok {
			if hasRedisPod(req.RedisPods, candidate) {
				result, ensureErr := ensureWithMaster(ctx, req.Cluster, req.RedisPods, req.Factory, candidate, opts)
				result.Source = EnsureSourceCache
				if result.Reason == "" {
					result.Reason = detail
				}
				if ensureErr == nil {
					return result, nil
				}
				req.MasterCache.Invalidate(cacheKey)
			} else {
				req.MasterCache.Invalidate(cacheKey)
			}
		}
	}

	if req.ForceMaster != "" {
		result, err := ensureWithMaster(ctx, req.Cluster, req.RedisPods, req.Factory, req.ForceMaster, opts)
		result.Source = EnsureSourceForced
		result.Reason = req.ForceReason
		if err == nil && req.MasterCache != nil && req.Cluster != nil && result.Master != "" {
			req.MasterCache.Remember(cacheKey, result.Master, result.Reason)
		}
		return result, err
	}

	var sentinelFailure string
	desiredSelector := map[string]string{}
	if req.Cluster.Spec.SentinelPod != nil && req.Cluster.Spec.SentinelPod.NodeSelector != nil {
		desiredSelector = req.Cluster.Spec.SentinelPod.NodeSelector
	}
	sentinelReady := 0
	for i := range req.SentinelPods {
		pod := &req.SentinelPods[i]
		if runtimepkg.IsPodReady(pod) && selectorEquals(pod.Spec.NodeSelector, desiredSelector) {
			sentinelReady++
		}
	}
	if req.Cluster.Spec.Mode == keyvalv1alpha1.ModeSentinel && req.SentinelFactory != nil && len(req.SentinelPods) > 0 {
		if sentinelReady == 0 {
			sentinelFailure = "no ready sentinel pods"
		} else {
			masterName, detail, err := resolveMasterViaSentinel(ctx, req.Cluster, req.RedisPods, req.SentinelPods, req.SentinelFactory, opts)
			if err == nil && masterName != "" {
				result, ensureErr := ensureWithMaster(ctx, req.Cluster, req.RedisPods, req.Factory, masterName, opts)
				result.Source = EnsureSourceSentinel
				result.Reason = detail
				if ensureErr == nil && req.MasterCache != nil && req.Cluster != nil {
					req.MasterCache.Remember(cacheKey, masterName, detail)
				}
				return result, ensureErr
			}
			if err != nil {
				sentinelFailure = sanitizeEnsureReason(err.Error())
			}
		}
	}

	result, err := ensureByProbe(ctx, req.Cluster, req.RedisPods, req.Factory, opts)
	if err == nil && req.MasterCache != nil && req.Cluster != nil && result.Master != "" {
		req.MasterCache.Remember(cacheKey, result.Master, result.Reason)
	}
	result.Source = EnsureSourceProbe
	if result.Reason == "" {
		result.Reason = sentinelFailure
	}
	return result, err
}

// ensureByProbe detects master by querying Redis pods directly.
func ensureByProbe(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod, cf Factory, opts ensureOptions) (EnsureResult, error) {
	master, _, err := DetectRoles(ctx, cr, pods, cf, opts)
	if err != nil {
		return EnsureResult{Roles: map[string]keyvalv1alpha1.PodRole{}, Infos: map[string]ReplicationInfo{}}, err
	}
	return ensureWithMaster(ctx, cr, pods, cf, master, opts)
}

func hasRedisPod(pods []corev1.Pod, name string) bool {
	if name == "" {
		return false
	}
	for i := range pods {
		if pods[i].Name == name {
			return true
		}
	}
	return false
}

func sanitizeEnsureReason(reason string) string {
	reason = strings.TrimSpace(reason)
	reason = strings.ReplaceAll(reason, "\n", " ")
	reason = strings.ReplaceAll(reason, "\r", " ")
	fields := strings.Fields(reason)
	if len(fields) == 0 {
		return ""
	}
	reason = strings.Join(fields, " ")
	runes := []rune(reason)
	const limit = 200
	if len(runes) > limit {
		reason = string(runes[:limit]) + "…"
	}
	return reason
}

func ensureWithMaster(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod, cf Factory, masterName string, opts ensureOptions) (EnsureResult, error) {
	res := EnsureResult{
		Master: masterName,
		Roles:  make(map[string]keyvalv1alpha1.PodRole, len(pods)),
		Infos:  make(map[string]ReplicationInfo, len(pods)),
	}
	if len(pods) == 0 {
		return res, errors.New("no pods provided")
	}
	if masterName == "" {
		masterName = runtimepkg.SortedPodsByName(pods)[0].Name
		res.Master = masterName
	}
	masterHost := ""
	for _, p := range pods {
		if p.Name == masterName {
			masterHost = p.Status.PodIP
			break
		}
	}
	if masterHost == "" {
		masterHost = core.PodDNSName(cr, masterName)
	}
	masterPort, _ := core.RedisPort(cr)
	clientOpts := ClientOptions{Username: opts.username, Password: opts.password, TLSConfig: opts.tlsConfig}

	inFlightInfos := make(map[string]ReplicationInfo, len(pods))
	for _, p := range runtimepkg.SortedPodsByName(pods) {
		c, err := cf.ForPod(ctx, p, clientOpts)
		if err != nil {
			return res, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("client for pod %s: %w", p.Name, err)))
		}
		if err := maybeAuth(ctx, c, opts.username, opts.password); err != nil {
			return res, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("auth pod %s: %w", p.Name, err)))
		}
		if p.Name == masterName {
			info, infoErr := c.ReplicationInfo(ctx)
			if infoErr != nil {
				info = ReplicationInfo{Role: "master"}
			}
			if info.Role != "master" {
				promoteErr := runtimepkg.Retry(ctx, 3, runtimepkg.Backoff{Initial: 200 * time.Millisecond, Factor: 2, Max: 2 * time.Second}, func(int) error {
					opCtx, cancel := runtimepkg.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					return c.NoOne(opCtx)
				})
				if promoteErr != nil {
					return res, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("promote master %s: %w", p.Name, promoteErr)))
				}
				res.Changed = true
				res.Drift = append(res.Drift, p.Name)
				info.Role = "master"
				info.MasterHost = ""
			}
			res.Roles[p.Name] = keyvalv1alpha1.PodRoleMaster
			res.Infos[p.Name] = info
			inFlightInfos[p.Name] = info
			continue
		}

		infoCtx, cancel := runtimepkg.WithTimeout(ctx, 3*time.Second)
		info, infoErr := c.ReplicationInfo(infoCtx)
		cancel()

		attachedToMaster := false
		if infoErr == nil {
			attachedToMaster = strings.EqualFold(info.Role, "replica") &&
				HostMatchesMaster(info.MasterHost, masterName, masterHost, pods) &&
				(info.MasterPort == 0 || info.MasterPort == masterPort)
		}

		needsReplicaOf := true
		if attachedToMaster {
			needsReplicaOf = false
			if !replicaAligned(info, opts.threshold) {
				// Replica is already configured to follow the desired master but is still syncing.
				// Reissuing REPLICAOF here can reset the sync progress and delay readiness.
				res.Pending = append(res.Pending, p.Name)
			}
		}
		if needsReplicaOf {
			replicaErr := runtimepkg.Retry(ctx, 3, runtimepkg.Backoff{Initial: 200 * time.Millisecond, Factor: 2, Max: 2 * time.Second}, func(int) error {
				opCtx, cancel := runtimepkg.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				return c.ReplicaOf(opCtx, masterHost, masterPort)
			})
			if replicaErr != nil {
				return res, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("replica %s replicaof %s:%d: %w", p.Name, masterHost, masterPort, replicaErr)))
			}
			res.Changed = true
			res.Drift = append(res.Drift, p.Name)
			var waitErr error
			info, waitErr = waitForReplicaSync(ctx, c, opts)
			if waitErr != nil {
				res.Pending = append(res.Pending, p.Name)
			}
		} else if infoErr != nil {
			info = ReplicationInfo{Role: "replica"}
		}
		if info.Role == "" {
			info.Role = "replica"
		}
		res.Roles[p.Name] = keyvalv1alpha1.PodRoleReplica
		res.Infos[p.Name] = info
		inFlightInfos[p.Name] = info
	}

	if res.Changed {
		observability.IncReplicationChanges(cr)
	}
	return res, nil
}

func resolveMasterViaSentinel(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, redisPods, sentinelPods []corev1.Pod, sf SentinelFactory, opts ensureOptions) (string, string, error) {
	var errs []error
	for _, sp := range runtimepkg.SortedPodsByName(sentinelPods) {
		cli, err := sf.ForPod(ctx, sp, SentinelOptions{Username: opts.username, Password: opts.password, TLSConfig: opts.tlsConfig})
		if err != nil {
			errs = append(errs, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("sentinel client %s: %w", sp.Name, err))))
			continue
		}
		host, port, err := cli.GetMasterAddrByName(ctx, cr.Name)
		if err != nil {
			errs = append(errs, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("sentinel %s get-master: %w", sp.Name, err))))
			continue
		}
		if host == "" {
			errs = append(errs, controllererrors.WrapFatal(fmt.Errorf("sentinel %s returned empty host", sp.Name)))
			continue
		}
		name := MatchPodByHost(redisPods, host)
		if name == "" {
			for _, p := range redisPods {
				if p.Status.PodIP == host {
					name = p.Name
					break
				}
			}
		}
		if name == "" {
			errs = append(errs, controllererrors.WrapFatal(fmt.Errorf("sentinel %s host %s not mapped to pod", sp.Name, host)))
			continue
		}
		detail := fmt.Sprintf("%s:%d", host, port)
		return name, detail, nil
	}
	if len(errs) == 0 {
		return "", "", errors.New("no sentinel pods available")
	}
	return "", "", errors.Join(errs...)
}

func maybeAuth(ctx context.Context, c Client, username, password string) error {
	if password == "" {
		return nil
	}
	return runtimepkg.Retry(ctx, 2, runtimepkg.Backoff{Initial: 100 * time.Millisecond, Factor: 2, Max: time.Second}, func(int) error {
		opCtx, cancel := runtimepkg.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		usernames := []string{username}
		if username != "" {
			usernames = append(usernames, "")
		}
		var authErr error
		for i, user := range usernames {
			authErr = c.Auth(opCtx, user, password)
			if authErr == nil {
				return nil
			}
			if !isWrongPassError(authErr) || i == len(usernames)-1 {
				return authErr
			}
		}
		return authErr
	})
}

func isWrongPassError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToUpper(err.Error()), "WRONGPASS")
}

func selectorEquals(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			return false
		}
	}
	return true
}

func waitForReplicaSync(ctx context.Context, c Client, opts ensureOptions) (ReplicationInfo, error) {
	deadline := time.Now().Add(opts.waitTimeout)
	var lastInfo ReplicationInfo
	var lastErr error
	for {
		if ctx.Err() != nil {
			if lastErr != nil {
				return lastInfo, lastErr
			}
			return lastInfo, ctx.Err()
		}
		infoCtx, cancel := runtimepkg.WithTimeout(ctx, 3*time.Second)
		info, err := c.ReplicationInfo(infoCtx)
		cancel()
		if err == nil {
			lastInfo = info
			lastErr = nil
			if replicaAligned(info, opts.threshold) {
				return info, nil
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastInfo, lastErr
			}
			return lastInfo, errors.New("replication sync timeout")
		}
		timer := time.NewTimer(opts.waitInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if lastErr != nil {
				return lastInfo, lastErr
			}
			return lastInfo, ctx.Err()
		case <-timer.C:
		}
	}
}

func replicaAligned(info ReplicationInfo, threshold int32) bool {
	if !strings.EqualFold(info.Role, "replica") {
		return false
	}
	if info.MasterHost == "" {
		return false
	}
	if info.MasterLinkStatus != "" && !strings.EqualFold(info.MasterLinkStatus, "up") && !strings.EqualFold(info.MasterLinkStatus, "ok") {
		return false
	}
	if info.MasterSyncInProgress {
		return false
	}
	return true
}

// DetectRoles queries pods to discover which is master and the role of each pod.
// If multiple masters are reported, the lexicographically smallest Pod name is preferred.
// If no master is found, the first pod by name is proposed as master (but not changed here).
func DetectRoles(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod, cf Factory, opts ensureOptions) (master string, roles map[string]keyvalv1alpha1.PodRole, err error) {
	roles = make(map[string]keyvalv1alpha1.PodRole, len(pods))
	if len(pods) == 0 {
		return "", roles, nil
	}
	var masters []string
	for _, p := range runtimepkg.SortedPodsByName(pods) {
		c, err := cf.ForPod(ctx, p, ClientOptions{Username: opts.username, Password: opts.password, TLSConfig: opts.tlsConfig})
		if err != nil {
			return "", nil, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("client for pod %s: %w", p.Name, err)))
		}
		if err := maybeAuth(ctx, c, opts.username, opts.password); err != nil {
			return "", nil, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("auth pod %s: %w", p.Name, err)))
		}
		var r string
		err = runtimepkg.Retry(ctx, 3, runtimepkg.Backoff{Initial: 200 * time.Millisecond, Factor: 2, Max: 2 * time.Second}, func(int) error {
			opCtx, cancel := runtimepkg.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			var e error
			r, e = c.Role(opCtx)
			return e
		})
		if err != nil {
			return "", nil, controllererrors.WrapTransient(controllererrors.WrapExternalDependency(fmt.Errorf("role for pod %s: %w", p.Name, err)))
		}
		switch r {
		case "master":
			roles[p.Name] = keyvalv1alpha1.PodRoleMaster
			masters = append(masters, p.Name)
		default:
			roles[p.Name] = keyvalv1alpha1.PodRoleReplica
		}
	}
	if len(masters) == 0 {
		return runtimepkg.SortedPodsByName(pods)[0].Name, roles, nil
	}
	sort.Strings(masters)
	return masters[0], roles, nil
}

// EnsureReplication enforces a single-master topology by promoting the chosen master (REPLICAOF NO ONE)
// and configuring all other pods as replicas of it. In Sentinel mode, it issues RESET on each sentinel
// when changes were applied. It returns whether any changes were made, the chosen master name, and the final roles map.
func EnsureReplication(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod, cf Factory, sec *security.Settings) (bool, string, map[string]keyvalv1alpha1.PodRole, map[string]ReplicationInfo, error) {
	res, err := EnsureTopology(ctx, EnsureRequest{Cluster: cr, RedisPods: pods, Factory: cf, Security: sec})
	if err != nil {
		return false, "", nil, nil, err
	}
	return res.Changed, res.Master, res.Roles, res.Infos, nil
}

// EnsureReplicationToMaster enforces that the provided masterName is the sole master
// and all other pods replicate from it. This is primarily used after a Sentinel-driven
// failover where the target master is already chosen.
func EnsureReplicationToMaster(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod, cf Factory, masterName string, sec *security.Settings) (bool, map[string]keyvalv1alpha1.PodRole, map[string]ReplicationInfo, error) {
	res, err := EnsureTopology(ctx, EnsureRequest{Cluster: cr, RedisPods: pods, Factory: cf, ForceMaster: masterName, Security: sec})
	if err != nil {
		return false, map[string]keyvalv1alpha1.PodRole{}, map[string]ReplicationInfo{}, err
	}
	return res.Changed, res.Roles, res.Infos, nil
}

// HostMatchesMaster checks whether the host corresponds to the selected master.
func HostMatchesMaster(host string, masterName string, masterIP string, pods []corev1.Pod) bool {
	if host == "" {
		return false
	}
	if host == masterName {
		return true
	}
	if masterIP != "" && host == masterIP {
		return true
	}
	if MatchPodByHost(pods, host) == masterName {
		return true
	}
	return false
}

// MatchPodByHost resolves a sentinel host string to a pod name.
func MatchPodByHost(pods []corev1.Pod, host string) string {
	if i := strings.IndexByte(host, '.'); i > 0 {
		name := host[:i]
		for _, p := range pods {
			if p.Name == name {
				return p.Name
			}
		}
	}
	return ""
}
