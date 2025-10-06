package importer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	record "k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	opstatus "github.com/ivelok/keyval-operator/controllers/internal/ops/status"
	runtimepkg "github.com/ivelok/keyval-operator/controllers/internal/runtime"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

const (
	importStatePending    = "Pending"
	importStateInProgress = "InProgress"
	importStateFollowing  = "Following"
	importStateCompleted  = "Completed"
	importStateFailed     = "Failed"
)

type Options struct {
	Cluster       *keyvalv1alpha1.KeyValCluster
	Spec          *keyvalv1alpha1.ExternalSourceSpec
	CurrentStatus *keyvalv1alpha1.ExternalImportStatus
	Pods          []corev1.Pod
	SentinelPods  []corev1.Pod
	ClientFactory clients.Factory
	ClientOptions clients.ClientOptions
	KubeClient    client.Client
	Recorder      record.EventRecorder
	Logger        logging.Logger
}

type Result struct {
	Status       *keyvalv1alpha1.ExternalImportStatus
	Condition    opstatus.ConditionState
	RequeueAfter time.Duration
	Abort        bool
}

type externalConfig struct {
	Host      string
	Port      int
	DB        int
	Username  string
	Password  string
	TLSConfig *tls.Config
	Addr      string
}

type readinessSnapshot struct {
	RedisReady         bool
	SentinelReady      bool
	RedisReadyCount    int
	RedisDesired       int
	SentinelReadyCount int
	SentinelDesired    int
}

func Ensure(ctx context.Context, opts Options) (Result, error) {
	logger := opts.Logger
	if logger.IsZero() {
		logger = logging.New(nil)
	}

	res := Result{
		Status:    cloneStatus(opts.CurrentStatus),
		Condition: makeCondition(metav1.ConditionFalse, "Pending", "waiting for external import preconditions"),
		Abort:     true,
	}

	if opts.Cluster == nil || opts.Spec == nil {
		return finalizeImport(ctx, opts, res)
	}
	if opts.ClientFactory == nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "MissingDependency", "redis client factory not configured")
		return res, controllererrors.WrapFatal(fmt.Errorf("redis client factory not configured"))
	}
	if opts.KubeClient == nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "MissingDependency", "kube client not configured")
		return res, controllererrors.WrapFatal(fmt.Errorf("kube client not configured"))
	}

	syncMode := opts.Spec.SyncMode
	if syncMode == "" {
		syncMode = keyvalv1alpha1.ExternalSourceSyncModeSnapshot
	}

	status := res.Status
	prevState := status.State
	prevSource := status.Source
	prevMode := status.Mode
	stateReset := prevSource != opts.Spec.Address || prevMode != string(syncMode)
	if stateReset {
		status.State = ""
	}
	status.Source = opts.Spec.Address
	status.Mode = string(syncMode)
	if status.State == "" {
		status.State = importStatePending
		status.StartedAt = nil
		status.LastSynced = nil
		status.Message = "external import pending"
	}

	abortIfData := true
	if opts.Spec.AbortIfExistingData != nil {
		abortIfData = *opts.Spec.AbortIfExistingData
	}

	maxDuration := 30 * time.Minute
	if opts.Spec.MaxInitialSyncDuration != nil && opts.Spec.MaxInitialSyncDuration.Duration > 0 {
		maxDuration = opts.Spec.MaxInitialSyncDuration.Duration
	}

	cfg, err := buildExternalConfig(ctx, opts)
	if err != nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "Config", err.Error())
		return res, controllererrors.WrapFatal(err)
	}

	pod := selectBootstrapPod(opts.Pods)
	if pod == nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "WaitingForPods", "no redis pods available for bootstrap")
		res.RequeueAfter = 5 * time.Second
		return res, nil
	}
	if !runtimepkg.IsPodReady(pod) {
		res.Condition = makeCondition(metav1.ConditionFalse, "PodNotReady", fmt.Sprintf("pod %s not Ready", pod.Name))
		res.RequeueAfter = 5 * time.Second
		return res, nil
	}

	client, err := opts.ClientFactory.ForPod(ctx, *pod, opts.ClientOptions)
	if err != nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "ClientInit", fmt.Sprintf("create redis client: %v", err))
		return res, controllererrors.WrapTransient(fmt.Errorf("redis client for %s: %w", pod.Name, err))
	}

	if status.State == importStatePending && abortIfData {
		size, err := client.DBSize(ctx)
		if err != nil {
			res.Condition = makeCondition(metav1.ConditionFalse, "DBSize", fmt.Sprintf("query dbsize: %v", err))
			return res, controllererrors.WrapTransient(fmt.Errorf("dbsize: %w", err))
		}
		if size > 0 {
			status.State = importStateFailed
			status.Message = fmt.Sprintf("target already contains %d keys", size)
			res.Condition = makeCondition(metav1.ConditionFalse, "TargetNotEmpty", status.Message)
			res.RequeueAfter = 0
			recordFailure(opts.Cluster, "target_not_empty", status.Message, opts.Recorder)
			return res, nil
		}
	}

	if status.State == importStateCompleted {
		msg := status.Message
		if msg == "" {
			msg = fmt.Sprintf("external import completed from %s", cfg.Addr)
		}
		res.Condition = makeCondition(metav1.ConditionTrue, "Completed", msg)
		res.RequeueAfter = 0
		res.Abort = false
		return res, nil
	}
	if status.State == importStateFailed {
		msg := status.Message
		if msg == "" {
			msg = "external import previously failed"
		}
		res.Condition = makeCondition(metav1.ConditionFalse, "Failed", msg)
		res.RequeueAfter = 0
		return res, nil
	}

	if err := configureMasterAuth(ctx, client, cfg.Username, cfg.Password); err != nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "MasterAuth", err.Error())
		return res, controllererrors.WrapTransient(fmt.Errorf("configure master auth: %w", err))
	}

	info, err := client.ReplicationInfo(ctx)
	if err != nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "ReplicationInfo", fmt.Sprintf("query replication info: %v", err))
		return res, controllererrors.WrapTransient(fmt.Errorf("replication info: %w", err))
	}

	attached := strings.EqualFold(info.MasterHost, cfg.Host) && info.MasterPort == cfg.Port
	if !attached {
		if err := ensureExternalReachable(ctx, cfg); err != nil {
			res.Condition = makeCondition(metav1.ConditionFalse, "SourceUnavailable", err.Error())
			res.RequeueAfter = 5 * time.Second
			return res, controllererrors.WrapTransient(err)
		}
		if err := client.ReplicaOf(ctx, cfg.Host, cfg.Port); err != nil {
			res.Condition = makeCondition(metav1.ConditionFalse, "ReplicaOf", fmt.Sprintf("replicaof %s: %v", cfg.Addr, err))
			return res, controllererrors.WrapTransient(fmt.Errorf("replicaof %s: %w", cfg.Addr, err))
		}
		attached = true
	}

	if prevState != importStateInProgress {
		now := metav1.Now()
		status.StartedAt = &now
		status.State = importStateInProgress
		status.Message = fmt.Sprintf("replicating from %s (%s mode)", cfg.Addr, string(syncMode))
		res.Condition = makeCondition(metav1.ConditionFalse, "Replicating", status.Message)
		recordAttempt(opts.Cluster, string(syncMode), fmt.Sprintf("address=%s", cfg.Addr), opts.Recorder)
	}

	if status.StartedAt == nil {
		now := metav1.Now()
		status.StartedAt = &now
	}

	// Treat timeout enforcement as part of the initial sync window; once we reach
	// a steady-state live follow (state=Following) we allow the replica link to
	// run indefinitely until manual cutover.
	initialPhase := stateReset || prevState == "" || prevState == importStatePending || prevState == importStateInProgress
	if time.Since(status.StartedAt.Time) > maxDuration {
		skipTimeout := syncMode == keyvalv1alpha1.ExternalSourceSyncModeLive && !initialPhase
		if !skipTimeout {
			status.State = importStateFailed
			status.Message = fmt.Sprintf("external import timed out after %s", maxDuration)
			res.Condition = makeCondition(metav1.ConditionFalse, "Timeout", status.Message)
			recordFailure(opts.Cluster, "timeout", status.Message, opts.Recorder)
			return res, nil
		}
	}

	linkStatus := strings.ToLower(info.MasterLinkStatus)
	delta := diff64(info.MasterReplOffset, info.ReplicaReplOffset)
	if linkStatus != "up" || info.MasterSyncInProgress || delta > 1024 {
		msg := fmt.Sprintf("syncing from %s: link=%s offsetDelta=%d", cfg.Addr, info.MasterLinkStatus, delta)
		res.Condition = makeCondition(metav1.ConditionFalse, "Syncing", msg)
		res.RequeueAfter = 5 * time.Second
		return res, nil
	}

	ready := snapshotReadiness(opts.Cluster, opts.Pods, opts.SentinelPods)
	if syncMode == keyvalv1alpha1.ExternalSourceSyncModeLive {
		if !ready.RedisReady || !ready.SentinelReady {
			msg := fmt.Sprintf("replica in sync; waiting for readiness redis %d/%d, sentinel %d/%d", ready.RedisReadyCount, ready.RedisDesired, ready.SentinelReadyCount, ready.SentinelDesired)
			res.Condition = makeCondition(metav1.ConditionFalse, "WaitingForReadiness", msg)
			res.RequeueAfter = 5 * time.Second
			return res, nil
		}

		now := metav1.Now()
		status.State = importStateFollowing
		status.LastSynced = &now
		status.Message = fmt.Sprintf("replicating from %s; awaiting manual cutover", cfg.Addr)
		res.Condition = makeCondition(metav1.ConditionFalse, "Following", status.Message)
		res.RequeueAfter = 10 * time.Second
		res.Abort = true
		return res, nil
	}

	if err := client.NoOne(ctx); err != nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "Promote", fmt.Sprintf("replicaof no one: %v", err))
		return res, controllererrors.WrapTransient(fmt.Errorf("replicaof no one: %w", err))
	}

	if err := clearMasterAuth(ctx, client); err != nil {
		logger.Info("failed to clear master auth", "error", err)
	}

	now := metav1.Now()
	status.State = importStateCompleted
	status.LastSynced = &now
	status.Message = fmt.Sprintf("external import completed from %s", cfg.Addr)
	res.Condition = makeCondition(metav1.ConditionTrue, "Completed", status.Message)
	res.RequeueAfter = 0
	res.Abort = false
	recordSuccess(opts.Cluster, string(syncMode), status.StartedAt, &now, opts.Recorder)
	return res, nil
}

func finalizeImport(ctx context.Context, opts Options, res Result) (Result, error) {
	logger := opts.Logger
	if logger.IsZero() {
		logger = logging.New(nil)
	}

	status := res.Status
	if status == nil || status.State == "" {
		res.Status = nil
		res.Condition = makeCondition(metav1.ConditionTrue, "Disabled", "external import not configured")
		res.Abort = false
		return res, nil
	}

	switch status.State {
	case importStateCompleted:
		msg := status.Message
		if msg == "" {
			msg = "external import completed"
		}
		res.Condition = makeCondition(metav1.ConditionTrue, "Completed", msg)
		res.Abort = false
		return res, nil
	case importStateFailed:
		msg := status.Message
		if msg == "" {
			msg = "external import previously failed"
		}
		res.Condition = makeCondition(metav1.ConditionFalse, "Failed", msg)
		res.Abort = false
		return res, nil
	case importStatePending:
		res.Status = nil
		res.Condition = makeCondition(metav1.ConditionTrue, "Disabled", "external import not configured")
		res.Abort = false
		return res, nil
	}

	pod := selectBootstrapPod(opts.Pods)
	if pod == nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "WaitingForPods", "waiting for bootstrap pod to detach from external source")
		res.RequeueAfter = 5 * time.Second
		res.Abort = true
		return res, nil
	}
	if opts.ClientFactory == nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "MissingDependency", "redis client factory not configured")
		return res, controllererrors.WrapFatal(fmt.Errorf("redis client factory not configured"))
	}

	client, err := opts.ClientFactory.ForPod(ctx, *pod, opts.ClientOptions)
	if err != nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "ClientInit", fmt.Sprintf("create redis client: %v", err))
		return res, controllererrors.WrapTransient(fmt.Errorf("redis client for %s: %w", pod.Name, err))
	}

	if err := client.NoOne(ctx); err != nil {
		res.Condition = makeCondition(metav1.ConditionFalse, "Promote", fmt.Sprintf("replicaof no one: %v", err))
		return res, controllererrors.WrapTransient(fmt.Errorf("replicaof no one: %w", err))
	}
	if err := clearMasterAuth(ctx, client); err != nil {
		logger.Info("failed to clear master auth", "error", err)
	}

	now := metav1.Now()
	status.State = importStateCompleted
	status.LastSynced = &now
	status.Message = fmt.Sprintf("external import cutover executed, promoted %s", pod.Name)
	res.Condition = makeCondition(metav1.ConditionTrue, "Completed", status.Message)
	res.RequeueAfter = 0
	res.Abort = false

	mode := status.Mode
	if mode == "" {
		mode = string(keyvalv1alpha1.ExternalSourceSyncModeSnapshot)
	}
	recordSuccess(opts.Cluster, mode, status.StartedAt, &now, opts.Recorder)
	return res, nil
}

func buildExternalConfig(ctx context.Context, opts Options) (externalConfig, error) {
	var cfg externalConfig
	address := strings.TrimSpace(opts.Spec.Address)
	if address == "" {
		return cfg, fmt.Errorf("external source address is empty")
	}
	u, err := url.Parse(address)
	if err != nil {
		return cfg, fmt.Errorf("parse address %q: %w", address, err)
	}
	host := u.Hostname()
	if host == "" {
		return cfg, fmt.Errorf("address %q missing host", address)
	}
	port := 6379
	if p := u.Port(); p != "" {
		parsed, perr := net.LookupPort("tcp", p)
		if perr != nil {
			return cfg, fmt.Errorf("address %q invalid port: %w", address, perr)
		}
		port = parsed
	}
	db := 0
	if path := strings.Trim(u.Path, "/"); path != "" {
		if val, err := strconv.Atoi(path); err == nil && val >= 0 {
			db = val
		}
	}
	user := ""
	pass := ""
	if u.User != nil {
		user = strings.TrimSpace(u.User.Username())
		if pw, ok := u.User.Password(); ok {
			pass = pw
		}
	}
	if opts.Spec.Auth != nil {
		if sel := opts.Spec.Auth.UsernameSecretRef; sel != nil {
			val, err := readSecretKey(ctx, opts.KubeClient, opts.Cluster.Namespace, sel)
			if err != nil {
				return cfg, fmt.Errorf("read username secret: %w", err)
			}
			user = strings.TrimSpace(string(val))
		}
		if sel := opts.Spec.Auth.PasswordSecretRef; sel != nil {
			val, err := readSecretKey(ctx, opts.KubeClient, opts.Cluster.Namespace, sel)
			if err != nil {
				return cfg, fmt.Errorf("read password secret: %w", err)
			}
			pass = string(val)
		}
	}
	tlsCfg, err := buildTLSConfig(ctx, opts, host)
	if err != nil {
		return cfg, err
	}
	cfg = externalConfig{Host: host, Port: port, DB: db, Username: user, Password: pass, TLSConfig: tlsCfg}
	cfg.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	return cfg, nil
}

func buildTLSConfig(ctx context.Context, opts Options, serverName string) (*tls.Config, error) {
	useTLS := strings.HasPrefix(strings.ToLower(opts.Spec.Address), "rediss://")
	if opts.Spec.TLS != nil {
		useTLS = useTLS || opts.Spec.TLS.Enabled
	}
	if !useTLS {
		return nil, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if net.ParseIP(serverName) == nil {
		config.ServerName = serverName
	}
	if opts.Spec.TLS == nil {
		return config, nil
	}
	tlsSpec := opts.Spec.TLS
	if tlsSpec.InsecureSkipVerify {
		config.InsecureSkipVerify = true
	}
	if tlsSpec.CABundleSecretRef != nil {
		data, err := readSecretKey(ctx, opts.KubeClient, opts.Cluster.Namespace, tlsSpec.CABundleSecretRef)
		if err != nil {
			return nil, fmt.Errorf("read tls ca bundle: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("parse tls ca bundle: invalid pem")
		}
		config.RootCAs = pool
	}
	if tlsSpec.ClientCertSecretRef != nil || tlsSpec.ClientKeySecretRef != nil {
		if tlsSpec.ClientCertSecretRef == nil || tlsSpec.ClientKeySecretRef == nil {
			return nil, fmt.Errorf("tls client certificate and key must both be provided")
		}
		certData, err := readSecretKey(ctx, opts.KubeClient, opts.Cluster.Namespace, tlsSpec.ClientCertSecretRef)
		if err != nil {
			return nil, fmt.Errorf("read tls client certificate: %w", err)
		}
		keyData, err := readSecretKey(ctx, opts.KubeClient, opts.Cluster.Namespace, tlsSpec.ClientKeySecretRef)
		if err != nil {
			return nil, fmt.Errorf("read tls client key: %w", err)
		}
		cert, err := tls.X509KeyPair(certData, keyData)
		if err != nil {
			return nil, fmt.Errorf("parse tls client key pair: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}
	return config, nil
}

func readSecretKey(ctx context.Context, c client.Client, namespace string, sel *corev1.SecretKeySelector) ([]byte, error) {
	if sel == nil {
		return nil, fmt.Errorf("secret selector is nil")
	}
	name := sel.Name
	key := sel.Key
	if name == "" || key == "" {
		return nil, fmt.Errorf("secret selector missing name or key")
	}
	var secret corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &secret); err != nil {
		return nil, fmt.Errorf("get secret %q: %w", name, err)
	}
	val, ok := secret.Data[key]
	if !ok {
		return nil, fmt.Errorf("secret %q missing key %q", name, key)
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

func ensureExternalReachable(ctx context.Context, cfg externalConfig) error {
	opts := &redis.Options{Addr: cfg.Addr, DialTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second}
	if cfg.Username != "" {
		opts.Username = cfg.Username
	}
	if cfg.Password != "" {
		opts.Password = cfg.Password
	}
	if cfg.TLSConfig != nil {
		opts.TLSConfig = cfg.TLSConfig.Clone()
	}
	if cfg.DB > 0 {
		opts.DB = cfg.DB
	}
	client := redis.NewClient(opts)
	defer client.Close()
	ctxPing, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(ctxPing).Err(); err != nil {
		return fmt.Errorf("ping external redis %s: %w", cfg.Addr, err)
	}
	return nil
}

func selectBootstrapPod(pods []corev1.Pod) *corev1.Pod {
	if len(pods) == 0 {
		return nil
	}
	sorted := make([]corev1.Pod, len(pods))
	copy(sorted, pods)
	sort.Slice(sorted, func(i, j int) bool { return core.Ordinal(sorted[i].Name) < core.Ordinal(sorted[j].Name) })
	return &sorted[0]
}

func configureMasterAuth(ctx context.Context, client clients.Client, username, password string) error {
	if username != "" {
		if err := client.ConfigSet(ctx, "masteruser", username); err != nil {
			return err
		}
	} else {
		if err := client.ConfigSet(ctx, "masteruser", ""); err != nil {
			return err
		}
	}
	if password != "" {
		return client.ConfigSet(ctx, "masterauth", password)
	}
	return client.ConfigSet(ctx, "masterauth", "")
}

func clearMasterAuth(ctx context.Context, client clients.Client) error {
	if err := client.ConfigSet(ctx, "masterauth", ""); err != nil {
		return err
	}
	return client.ConfigSet(ctx, "masteruser", "")
}

func snapshotReadiness(cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod, sentinelPods []corev1.Pod) readinessSnapshot {
	ready := readinessSnapshot{}
	desired := int(cr.Spec.RedisReplicas)
	if desired <= 0 {
		desired = len(pods)
	}
	ready.RedisDesired = desired
	count := 0
	for i := range pods {
		if pods[i].DeletionTimestamp != nil {
			continue
		}
		if runtimepkg.IsPodReady(&pods[i]) {
			count++
		}
	}
	ready.RedisReadyCount = count
	ready.RedisReady = count >= desired && desired > 0
	ready.SentinelReady = true
	if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
		want := 0
		if cr.Spec.SentinelCount != nil {
			want = int(*cr.Spec.SentinelCount)
		}
		if want <= 0 {
			want = len(sentinelPods)
		}
		ready.SentinelDesired = want
		count := 0
		for i := range sentinelPods {
			if sentinelPods[i].DeletionTimestamp != nil {
				continue
			}
			if runtimepkg.IsPodReady(&sentinelPods[i]) {
				count++
			}
		}
		ready.SentinelReadyCount = count
		if want > 0 {
			ready.SentinelReady = count >= want
		}
	}
	return ready
}

func recordAttempt(cr *keyvalv1alpha1.KeyValCluster, mode string, detail string, rec record.EventRecorder) {
	opobs.IncExternalImportAttempt(cr, mode)
	if rec != nil {
		opobs.EventExternalImportStarted(rec, cr, detail)
	}
}

func recordSuccess(cr *keyvalv1alpha1.KeyValCluster, mode string, startedAt, finishedAt *metav1.Time, rec record.EventRecorder) {
	opobs.IncExternalImportSuccess(cr, mode)
	if rec != nil {
		opobs.EventExternalImportCompleted(rec, cr, fmt.Sprintf("mode=%s", mode))
	}
	if startedAt != nil && finishedAt != nil {
		d := finishedAt.Sub(startedAt.Time)
		if d < 0 {
			d = 0
		}
		opobs.ObserveExternalImportDuration(cr, mode, d)
	}
}

func recordFailure(cr *keyvalv1alpha1.KeyValCluster, reason, message string, rec record.EventRecorder) {
	opobs.IncExternalImportFailure(cr, reason)
	if rec != nil {
		opobs.EventExternalImportFailed(rec, cr, fmt.Sprintf("reason=%s %s", reason, message))
	}
}

func cloneStatus(st *keyvalv1alpha1.ExternalImportStatus) *keyvalv1alpha1.ExternalImportStatus {
	if st == nil {
		return &keyvalv1alpha1.ExternalImportStatus{}
	}
	return st.DeepCopy()
}

func makeCondition(status metav1.ConditionStatus, reason, message string) opstatus.ConditionState {
	return opstatus.ConditionState{Status: status, Reason: reason, Message: message}
}

func diff64(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}
