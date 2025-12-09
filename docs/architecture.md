# KeyVal Operator — Architecture

> The operator aims to deliver idempotent management for Redis/Valkey clusters (Standalone and Sentinel) with minimal downtime, safe updates, and clear observability.

## 1. Control Plane Overview
- **CRD**: `KeyValCluster` (namespaced). Users describe the operating mode, container images, resource requests, security settings, and PDB/Service options.
- **Controller**: a single `KeyValClusterReconciler` managed by controller-runtime. The workqueue uses a typed rate limiter (`MaxOfRateLimiter` plus a token bucket capped at 5 RPS) and receives its dependencies via `NewKeyValClusterReconciler`.
- **Invariants**: one master pod, a StatefulSet with `OnDelete`, replicas stay in sync, the `<cr>-master` Service selects exactly one pod, PDB safety gates are respected, and every action is idempotent.

## 2. Entry Point and Dependency Injection
- `controllers/reconciler_factory.go` defines `ReconcilerDependencies`: Kubernetes clients, scheme, Redis/Sentinel factories, logger, rate limiter, and eviction parameters.
- During startup `main.go` selects a reconcile profile (`small`/`medium`/`large`) based on the actual number of `KeyValCluster` objects and their replica counts. The profile configures `MaxConcurrentReconciles`, `client-go` QPS/Burst, and the TTL for the master address cache. Debug overrides are only allowed through `KEYVAL_OPERATOR_RECONCILE_PROFILE`; when the variable is set the operator logs a warning.
- `KeyValClusterReconciler` keeps no global state; all dependencies are injected on creation, which simplifies testing.
- Logging uses `log/slog` and integrates with controller-runtime’s `logr`. `logging.Initialize` executes lazily and is concurrency-safe.
- Package `controllers/errors` exposes typed wrappers `WrapTransient/WrapFatal`. The reconcile result is recorded in the `keyval_reconcile_result_total` metric.

## 3. Internal Module Layout
```
controllers/internal/
├── core            # constants, resource names, annotation keys, DNS helpers
├── resources       # Kubernetes object builders (ConfigMap, StatefulSet, Service, PDB)
├── runtime         # shared helpers: predicates, sorting, backoff, ClusterIP preservation
├── clients         # Redis/Sentinel factories (go-redis v9) with TLS/Auth from security.Settings
├── security        # resolve AUTH/TLS secrets, build tls.Config and passwords
├── finalizer       # Finalizer management and storage cleanup hooks
├── reconcile       # Reconcile phases and flow control
├── ssa             # Server-Side Apply patch construction and field management
└── ops
    ├── bootstrap   # persist PVC freshness annotations, pick a master candidate
    ├── eviction    # Eviction API wrapper with timeouts, backoff, metrics, events
    ├── health      # compute health gates and thresholds from spec.health
    ├── importer    # External source data import (Snapshot/Live modes)
    ├── observability
    │   ├── events  # standard Kubernetes events (failover, bootstrap, rolling updates)
    │   └── metrics # Prometheus metrics: register gauges/counters/histograms
    ├── replication # EnsureTopology, DetectRoles, EnsureReplication, sentinel reset triggers, master address TTL cache
    ├── runtimeconfig # apply allow-listed runtime settings (Redis CONFIG, SENTINEL SET)
    ├── sentinel    # TriggerFailover, validate good slave, handle NOQUORUM
    ├── status      # build KeyValClusterStatus and conditions, compute healthGate
    ├── storage     # PVC resize handling and filesystem check logic
    └── update      # plan and run rolling updates (PlanUpdates + EvaluateGuards)
```
- SSA patches use helpers from `controllers/internal/ssa` that build minimal ConfigMap/Service/StatefulSet/PDB payloads. The operator owns only the required fields (the `redis.conf`/`sentinel.conf` content, service selectors and ports, pod template and PVC, PDB parameters), eliminating conflicts with mesh injectors and load-balancer controllers. Attempts to change immutable Service fields (ClusterIP, IPFamilies/IPFamilyPolicy, NodePort) are logged, trigger the `ServiceImmutableField` event, and increment `keyval_operator_service_immutable_change_total{service,field}`.

### Redis Pod Health
- **Liveness** — the `liveness.sh` script (mounted from the ConfigMap) runs `redis-cli PING` against `127.0.0.1:${REDIS_PORT}` and automatically adds TLS flags (`--tls`, `--cacert`, `--cert`, `--key`) when the cluster is encrypted. The probe relies on `REDISCLI_AUTH`/`MASTER_USER`, so it catches authentication issues and process stalls better than a pure TCP socket.
- **Readiness** — the `readiness.sh` script executes `redis-cli --raw INFO replication` and applies role-specific rules: masters only need a successful response, replicas require `master_link_status=up`, `master_sync_in_progress=0`, `master_link_down_since_seconds=0`, and `master_last_io_seconds_ago` below the threshold (15s by default, overridden via `REDIS_READINESS_MAX_LAST_IO_SECONDS`). Diagnostics are printed to stderr, which simplifies `kubectl logs` triage.
- Both scripts ship alongside `redis.conf` in the ConfigMap and are part of the pod template hash; any change to the probe logic triggers a controlled rollout.
- `resources/topology.go` injects consistent pod distribution rules: topology spread (zone/hostname with `ScheduleAnyway`) and pod anti-affinity favoring `kubernetes.io/hostname`. Settings are read from `spec.topology`; Redis and Sentinel enable them by default, but users can disable or harden them (required anti-affinity, custom keys) through the CR or Helm chart.
- `resources/statefulset_*.go` add a `preStop` lifecycle hook (`redis-cli shutdown nosave`) and increase `terminationGracePeriodSeconds` to 25, giving Redis/Valkey time to exit cleanly and avoiding false failovers. Failures emit `RedisPreStopFailed` to stderr, surface in events, and feed the `keyval_operator_graceful_shutdown_duration_seconds` metric (observed once the pod exits).
- When `spec.metrics.enabled` the Redis StatefulSet appends a `redis_exporter` sidecar (default `ghcr.io/oliver006/redis_exporter:v1.75.0`). The exporter talks to `127.0.0.1:${REDIS_PORT}` using the resolved AUTH secret and automatically switches to `rediss://` with client certificates when TLS/mTLS is enabled. `/metrics` is served over HTTP on the configurable exporter port (default 9121) with dedicated readiness & liveness probes. The headless Service publishes an additional `metrics` port so Prometheus or ServiceMonitor CRs can scrape pods without touching the Redis listener.
- `ops/bootstrap` supports DR bootstrap through PVC freshness annotations: `UpdatePVCFreshness` stores replication offset/role/timestamp on PVCs, and `SelectCandidate` chooses the master (forced master → highest offset → seed). Metrics `keyval_operator_bootstrap_freshness_source` and `..._gap_seconds` capture the decision source and the gap between the leader and the next candidate.
- The client package (`controllers/internal/clients`) wraps every go-redis v9 command in a `context.WithTimeout`, limits retries (`MaxRetries=2` by default), honors TLS/Auth from `security.Settings`, and exposes metrics `keyval_operator_external_call_timeouts_total` and `keyval_operator_redis_client_retries_total` to monitor degradation.
- Metrics `keyval_operator_controller_reconcile_queue_depth{controller}` and the hit/miss counters `keyval_operator_redis_master_cache_hits_total|_misses_total` are produced in `ops/observability/metrics.go`, reflecting the reconcile queue depth and master cache efficiency.

## 4. Eviction Wrapper
- `ops/eviction.Manager` implements `Runner` with tunables `MaxAttempts`, `PerAttemptTimeout`, `InitialBackoff`, `MaxBackoff`.
- Every call publishes metrics `keyval_operator_eviction_attempt_total`, `keyval_operator_eviction_result_total{result=attempt|success|timeout|rejected|forbidden|error|cancelled}`, `keyval_operator_pod_eviction_failures_total{reason}`, and emits the `PodEvicted` event.
- Transient errors (`429 TooManyRequests`, timeouts) are wrapped with `ErrRejected` + `WrapTransient`. Permission errors (`403`) are considered fatal: the controller stops retrying and flags the reconcile as `error`.
- Rolling updates for Redis and Sentinel (`ops/update.Execute`) receive `ExecuteOptions{Evictor, EvictionSettings, Component}` and rely on the wrapper by default.

## 5. Reconcile Lifecycle
1. **Fetch CR and finalize** — ensure the finalizer exists; when deletion is requested, invoke `finalizeCluster`.
2. **Security** — `security.FromSpec` loads AUTH/TLS secrets and returns `Settings`; when `HasAuth` and/or `HasTLS` are true the client configuration is updated and the logger records the active modes.
3. **Render resources** — call `resources.ConfigMap`, `HeadlessService`, `MasterService`, optional `ReplicasService` and Sentinel services (including headless), plus Redis/Sentinel StatefulSets and the PDB. Everything is applied via SSA with a dedicated `FieldOwner`. Optional services follow `spec.service|replicasService|sentinelService.create`: when toggled to `false` the operator deletes the Service with `propagation=Foreground`, logs `component=service action=delete`, emits `ServiceRemoved`, and bumps `keyval_operator_service_removed_total{service=...}`. Re-enabling the flag recreates the Service while preserving the ClusterIP (`PreserveClusterIP`) and owner references. The toggles are meant for operator-managed services; keep `create=true` if an external system owns the Service to avoid removal. Sentinel StatefulSet `Apply` errors are classified: attempts to change immutable fields (e.g., `spec.serviceName`, `spec.selector`, `spec.volumeClaimTemplates`) raise the `SentinelApplyImmutableField` event, set `Reconciled=False` with reason `ImmutableField`, and increment `keyval_operator_sentinel_apply_failures_total{reason="immutable_field"}`; all other errors are treated as transient and stop reconcile for backoff.
4. **External import (optional)** — when `spec.bootstrap.externalSource` is present, the importer attaches the bootstrap pod as a replica of the remote instance and aborts the pipeline immediately after persisting status. For `Snapshot` mode it detaches with `REPLICAOF NO ONE` once the link is healthy, records `state=Completed`, then resumes the remaining phases. For `Live` mode it transitions to `state=Following`, keeps the link active, emits `ExternalImportStarted`, and requeues every few seconds while `ConditionExternalImport` reports reason `Following`. When the `externalSource` block is removed, the importer performs the cutover (detach + clear `masterauth`), emits `ExternalImportCompleted`, and allows reconciliation to continue with the usual runtime/label/update phases.
4. **Inventory pods** — list pods via the headless Service, sort by ordinal, readiness, and PVC annotations.
5. **Replication and roles** —
   - `opreplication.EnsureTopology` returns the master, role map, and drift list.
   - During bootstrap or with a forced candidate the controller sets overrides `ConditionBootstrapInProgress`, `ConditionDisruptionsPaused`, and schedules a Sentinel reset.
   - Pod labels `role=master|replica` are patched via a conflict-tolerant MergePatch: up to three attempts with pod reload between retries and raising the minimum requeue (2s → 4s → 8s). Conflicts are logged with `attempt` and `resourceVersion`, increment `keyval_operator_pod_label_patch_conflicts_total{pod}`, and successful retries increment `keyval_operator_pod_label_patch_retries_total{pod}`. Corrections also emit the `RoleCorrected` event and the `keyval_label_corrections_total` metric.
6. **Sentinel** — run `CKQUORUM` using `sentinel.Factory`, refresh `keyval_operator_sentinel_*` metrics, coordinate `SENTINEL RESET` with a cooldown, and emit `SentinelQuorumLost/Restored` plus `ResetSentinelDone`.
7. **Health gate** — compute status through `status.ComputeStatus`, update conditions `BootstrapInProgress`, `ReplicationHealthy`, `SentinelQuorum`, `FailoverInProgress`, `DisruptionsPaused`, `UpgradeInProgress`, and set `status.healthGate.allowDisruptions`.
8. **Rolling updates** —
   - `PlanUpdates` detects drift in annotations/images/resources/health/scale-down.
   - `EvaluateGuards` blocks updates during bootstrap, failover, lack of healthy replicas, or when disruptions are disabled. It emits `RollingStepBlocked/Resumed` and increments `keyval_disruptions_blocked_total`.
   - Before deleting the master pod in Sentinel mode the controller invokes `ops/sentinel.TriggerFailover` (events `StartFailover`, `FailoverTriggered`, `FailoverCompleted`, `NewMaster`).
   - Evictions go through the wrapper; when `ErrRejected` happens the controller resets `keyval_operator_update_in_progress`, sets annotation `keyval.ivelok.io/update-blocked`, and waits for backoff.
9. **Status** — `ops/status.UpdateStatus` syncs `status.masterPod`, `status.rolesSource`, ready pods, roles, lag, and conditions.
10. **Finalize metrics and events** — a `defer` captures the duration (`keyval_reconcile_duration_seconds`) and result (`keyval_reconcile_result_total`).
- Requeues without errors run through an accumulating backoff: the base interval merges with exponential backoff (3 → 6 → 12 → 24 → 48 → 60 s with ±10% jitter). The resulting delay is emitted to `keyval_operator_requeue_backoff_seconds`, preventing busy loops during Redis/Sentinel degradation.

## 6. Observability
### Prometheus Metrics
| Metric | Type / Labels | Purpose |
|--------|---------------|---------|
| `keyval_reconcile_result_total{namespace,cluster,result}` | Counter | Reconcile outcomes (`success`, `stalled`, `error`). |
| `keyval_reconcile_duration_seconds{namespace,cluster}` | Histogram | Reconcile time; use `histogram_quantile`. |
| `keyval_operator_eviction_attempt_total{namespace,cluster,component}` | Counter | Eviction attempts (redis/sentinel). |
| `keyval_operator_eviction_result_total{namespace,cluster,component,result}` | Counter | Eviction outcomes for PDB analytics. |
| `keyval_operator_pod_eviction_failures_total{namespace,cluster,component,reason}` | Counter | Breakdown of failed evictions (PDB, RBAC, timeout, errors). |
| `keyval_operator_update_in_progress{namespace,cluster}` | Gauge | Indicates whether an update is in flight (1) or not. |
| `keyval_disruptions_blocked_total{namespace,cluster,reason}` | Counter | Reasons why rollouts are blocked (PDBLimit, SentinelQuorum, Bootstrap, …). |
| `keyval_rolling_deletions_total{namespace,cluster}` | Counter | Pods deleted by the operator. |
| `keyval_replication_changes_total{namespace,cluster}` | Counter | Count of replication reconfigurations. |
| `keyval_operator_replication_lag_seconds{namespace,cluster,pod}` | Gauge | Current lag in seconds. |
| `keyval_label_corrections_total{namespace,cluster}` | Counter | Number of role label corrections. |
| `keyval_operator_external_call_timeouts_total{namespace,cluster,command,endpoint}` | Counter | Redis/Sentinel command timeouts recorded by the client wrappers. |
| `keyval_operator_redis_client_retries_total{namespace,cluster,command,endpoint}` | Counter | Extra Redis/Sentinel attempts beyond the initial call. |
| `keyval_failover_triggered_total{namespace,cluster,type}` / `keyval_failover_completed_total{namespace,cluster}` | Counter | Controlled failovers initiated and completed. |
| `keyval_bootstrap_attempt_total{namespace,cluster,mode}` / `keyval_bootstrap_failure_total{namespace,cluster}` | Counter | Bootstrap attempts and failures (`standalone`/`sentinel`). |
| `keyval_external_import_attempt_total{namespace,cluster,mode}` / `keyval_external_import_success_total{…}` | Counter | External import attempts and successful completions grouped by sync mode. |
| `keyval_external_import_failure_total{namespace,cluster,reason}` | Counter | External import failures (reason = target_not_empty, timeout, source_unreachable, …). |
| `keyval_external_import_duration_seconds{namespace,cluster,mode}` | Histogram | Duration of bootstrap imports (per mode). |
| `keyval_runtime_config_applied_total{namespace,cluster,component}` / `keyval_runtime_config_failed_total{…}` | Counter | Redis/Sentinel runtime configuration updates. |
| `keyval_operator_sentinel_*` (quorum gauges, ready members, resets) | Gauge/Counter | Sentinel health monitoring. |
| `keyval_operator_sentinel_apply_failures_total{namespace,cluster,reason}` | Counter | Sentinel StatefulSet SSA errors (`reason=immutable_field|error`); feeds drift alerts and runbooks. |
| `keyval_operator_requeue_backoff_seconds{namespace,cluster}` | Histogram | Effective delay before the next reconcile (exponential backoff + internal requirements). |

### Kubernetes Events
- `BootstrapStart` / `BootstrapFinish`, `ExternalImportStarted` / `ExternalImportCompleted` / `ExternalImportFailed`, `StartFailover`, `FailoverTriggered`, `FailoverCompleted`, `NewMaster`, `NoGoodSlave`.
- `SentinelQuorumLost` / `SentinelQuorumRestored`.
- `SentinelApplyImmutableField` / `SentinelApplyFailed`.
- `RollingStepBlocked` / `RollingStepResumed`, `PodEvicted`, `PDBModeSwitched`.
- `ReplicationAligned`, `ReplicationDrift`, `ReplicaLagging/Desynced/Recovered`, `RuntimeConfigApplied/Failed`.

## 7. Idempotence and Safety
- Resource builders generate deterministic objects, so SSA eliminates drift on repeated reconciles.
- Pod patches use `client.MergeFrom` with a strict `ResourceVersion` check.
- The eviction wrapper ensures retries keep state consistent and always respect `ctx.Done()`.
- Update blocks (bootstrap/failover/PDB) are tracked via annotation `keyval.ivelok.io/update-blocked` and metrics; the operator never deletes more than one pod per cycle.
- Metrics `keyval_reconcile_result_total` and `keyval_operator_update_in_progress` let operators spot stuck reconciles quickly.

## 8. Test Coverage and E2E
- **Unit/Integration**: table-driven tests for resource builders, replication, runtime config, eviction, and guard logic. Envtest scenarios validate resource creation and drift-free reconciliation (`TestReconcileStandalone_Idempotent`).
- **E2E (`make e2e`)**: covers Standalone and Sentinel clusters, runtime configs, failovers, DR bootstrap, rolling updates with PDB/health gates, and TLS (Redis + Sentinel). Runs are expected to finish with `PASS` and logs that demonstrate idempotent reconcile loops.

## 9. Storage Management
- **Finalizer and cleanup policy.** Field `spec.storage.cleanupOnDelete` enables PVC cleanup when the CR is deleted. The finalizer records the start time (`keyval.ivelok.io/finalizing-since`), emits `StorageCleanupStarted/Finished/TimedOut`, enforces a limit of three deletions per cycle, and updates the `StorageCleanup` status condition (`CleanupEnabled` / `RetentionPolicy` / `DeletingPVCs` / `PVCsDeleted`, `Timeout`). Metrics `keyval_operator_finalizer_duration_seconds{policy,result}` and `keyval_operator_storage_cleanup_failures_total{policy,reason}` track finalize duration and timeouts. If PVCs still exist after `2m`, a warning is raised and the finalizer releases the block, leaving the resources to the user.
- **PVC expansion.** Module `ops/storage.EnsureResize` watches `spec.storage.size`, patches PVC `spec.resources.requests.storage`, and manages the online expansion loop. While CSI has already enlarged the volume and kubelet has not set `FileSystemResizePending`, the operator skips pod restarts: rolling update marks `storage:resize` only when `FileSystemResizePending=True` for longer than `2m` or `status.capacity` stays below target for more than `15m`. Events `StorageResizeRequested/Completed`, `StorageResizeRestartQueued`, and `StorageResizeBackoff` capture progress plus provider rate limits.
- **StorageClass guidance.** Redis/Sentinel clusters require low-latency disks with guaranteed fsync (NVMe/SSD, synchronous writes). The StorageClass must support `allowVolumeExpansion`, `volumeBindingMode=WaitForFirstConsumer` (for deliberate node selection), and a `reclaimPolicy` that matches the DR plan. Without expansion support the reconcile loop remains pending (`StorageResizeRequested` without `Completed`), so production workloads should use a class with online expansion.

## 10. Extensibility
- Every new operation should live in its own subpackage under `controllers/internal/ops/` with clear ownership.
- New metrics are registered in `observability/metrics.go` (remember to update documentation and tests).
- For work outside the current scope, align on scope in .codex/tasks before implementation.

## 11. Scaling and Update Telemetry
- `PlanUpdates` tags pods deleted for scale-down with reason `scale-down`, while the StatefulSet creates replacements. Init container `kv-bootstrap-role` guarantees pods start as replicas, allowing `EnsureReplication` to add them to the topology immediately.
- The controller tracks the scaling phase via annotation `keyval.ivelok.io/scale-phase` (`up|down`) and emits `ScaleOperationStarted`, `ScaleOperationBlocked`, and `ScaleOperationCompleted`. Events include `direction`, `current/target`, and the specific block reason (`DisruptionsPaused`, `SentinelQuorum`, `NoHealthyReplicas`).
- Metrics `keyval_operator_scale_in_progress{namespace,cluster}` and `keyval_scale_operations_total{namespace,cluster,direction}` help build SLOs for scaling duration and highlight stuck operations. The gauge resets after `ScaleOperationCompleted`.
- When scaling down the master in Sentinel mode the operator performs a controlled `SENTINEL FAILOVER` before evicting the pod. Event chain: `ScaleOperationStarted` → `StartFailover`/`FailoverCompleted` → `PodEvicted` (reason `scale-down`). If the failover cannot proceed (no quorum, no healthy replica) the controller emits `ScaleOperationBlocked` and keeps the operation in the `down` phase until conditions recover.
- Runbook `docs/runbook/scale-and-upgrade.md` lists SRE steps (`kubectl scale/patch`, interpreting events/metrics, diagnosing common blockers).
