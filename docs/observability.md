# KeyVal Operator Observability

## Prometheus Metrics
The controller exposes metrics on `:8080/metrics` (deployment `keyval-operator-controller-manager`). Current dictionary:

| Metric | Type / Labels | Purpose |
|--------|---------------|---------|
| `keyval_reconcile_result_total{namespace,cluster,result}` | Counter | Outcome of each reconcile (`success`, `stalled`, `error`); reveals failure frequency and stuck loops. |
| `keyval_reconcile_duration_seconds{namespace,cluster}` | Histogram | Reconcile duration distribution; recommended SLI: `histogram_quantile(0.9, …)` over 5-minute windows. |
| `keyval_operator_controller_reconcile_queue_depth{controller}` | Gauge | Current depth of the reconcile workqueue per controller (e.g., `keyvalcluster`). |
| `keyval_operator_requeue_backoff_seconds{namespace,cluster}` | Histogram | Observed requeue backoff delays; helps track degradation loops. |
| `keyval_status_update_total{namespace,cluster,mutated}` | Counter | Number of status reconciliation attempts (`mutated=true|false`). |
| `keyval_operator_eviction_attempt_total{namespace,cluster,component}` | Counter | Number of evictions initiated by the operator (`component=redis|sentinel`). |
| `keyval_operator_eviction_result_total{namespace,cluster,component,result}` | Counter | Eviction results (`attempt`, `success`, `rejected`, `timeout`, `forbidden`, `error`, `cancelled`); helpful for PDB/RBAC diagnostics. |
| `keyval_operator_pod_eviction_failures_total{namespace,cluster,component,reason}` | Counter | Failed evictions classified by reason (`too_many_requests`, `forbidden`, `timeout`, `cancelled`, `error`). |
| `keyval_operator_update_in_progress{namespace,cluster}` | Gauge | Set to 1 while a rolling update (Redis or Sentinel) is active; detects stuck updates. |
| `keyval_operator_scale_in_progress{namespace,cluster}` | Gauge | Set to 1 while scaling is in progress; resets after `ScaleOperationCompleted`. |
| `keyval_disruptions_blocked_total{namespace,cluster,reason}` | Counter | Counts rollout blocks (`PDBLimit`, `Bootstrap`, `SentinelQuorum`, `DisruptionsPaused`, …). |
| `keyval_rolling_deletions_total{namespace,cluster}` | Counter | Pods deleted during rolling updates. |
| `keyval_scale_operations_total{namespace,cluster,direction}` | Counter | Completed scaling operations (`direction=up|down`); supports scale-duration SLOs. |
| `keyval_replication_changes_total{namespace,cluster}` | Counter | Topology reconfigurations (`REPLICAOF`). |
| `keyval_operator_replication_lag_seconds{namespace,cluster,pod}` | Gauge | Replication lag from `INFO replication`. |
| `keyval_label_corrections_total{namespace,cluster}` | Counter | Pod role label corrections (`role=master|replica`). |
| `keyval_operator_pod_label_patch_conflicts_total{namespace,cluster,pod}` | Counter | `resourceVersion` conflicts while patching a pod role. |
| `keyval_operator_pod_label_patch_retries_total{namespace,cluster,pod}` | Counter | Extra attempts required to patch a pod role successfully. |
| `keyval_operator_redis_master_cache_hits_total` / `_misses_total` | Counter | Internal cache performance for master address lookups. |
| `keyval_operator_failover_decisions_total{namespace,cluster,source}` | Counter | Source of leader decisions (`sentinel|probe|forced`); highlights non-Sentinel fallbacks. |
| `keyval_failover_triggered_total{namespace,cluster,type}` / `keyval_failover_completed_total{namespace,cluster}` | Counter | Controlled failovers triggered/completed (`type=manual|automatic|forced`). |
| `keyval_bootstrap_attempt_total{namespace,cluster,mode}` / `keyval_bootstrap_failure_total{namespace,cluster}` | Counter | Bootstrap attempts/failures (`mode=standalone|sentinel`). |
| `keyval_operator_bootstrap_freshness_gap_bytes{namespace,cluster}` | Gauge | Difference in replication offset (bytes) between the selected DR master and the next candidate. |
| `keyval_operator_bootstrap_freshness_source{namespace,cluster,source}` | Gauge | Source of DR bootstrap decision (1=force-master, 2=freshness, 3=seed). |
| `keyval_external_import_attempt_total{namespace,cluster,mode}` / `keyval_external_import_success_total{namespace,cluster,mode}` | Counter | External import attempts and successful completions (mode = snapshot/live). |
| `keyval_external_import_failure_total{namespace,cluster,reason}` | Counter | External import failures classified by reason (`target_not_empty`, `timeout`, `source_unreachable`, ...). |
| `keyval_external_import_duration_seconds{namespace,cluster,mode}` | Histogram | Time spent waiting for external import to finish. |
| `keyval_runtime_config_applied_total{namespace,cluster,component}` / `keyval_runtime_config_failed_total{…}` | Counter | Runtime config application results (`component=redis|sentinel`). |
| `keyval_operator_sentinel_quorum_healthy{namespace,cluster}` | Gauge | Result of `CKQUORUM` (1 = quorum present). |
| `keyval_operator_sentinel_ready_members{namespace,cluster}` | Gauge | Number of Ready Sentinel pods. |
| `keyval_operator_sentinel_quorum_target{namespace,cluster}` | Gauge | Required majority (`ceil(N/2)`). |
| `keyval_operator_sentinel_noquorum_reports{namespace,cluster}` | Gauge | Count of Sentinels that reported `NOQUORUM` in the last check. |
| `keyval_operator_sentinel_quorum_loss_since{namespace,cluster}` | Gauge | Unix timestamp when quorum was first lost (`0` once restored). |
| `keyval_operator_sentinel_resets_total{namespace,cluster}` | Counter | Number of `SENTINEL RESET` commands executed by the operator. |
| `keyval_operator_sentinel_apply_failures_total{namespace,cluster,reason}` | Counter | Sentinel StatefulSet SSA errors (`reason=immutable_field|error`). |
| `keyval_operator_redis_apply_failures_total{namespace,cluster,reason}` | Counter | Redis StatefulSet SSA errors (`reason=immutable_field|error`). |
| `keyval_operator_external_call_timeouts_total{namespace,cluster,command,endpoint}` | Counter | Redis/Sentinel command timeouts recorded by the client wrappers. |
| `keyval_operator_redis_client_retries_total{namespace,cluster,command,endpoint}` | Counter | Extra Redis/Sentinel attempts beyond the initial call. |
| `keyval_operator_finalizer_duration_seconds{namespace,cluster,policy,result}` | Histogram | Duration of storage cleanup finalizer execution. |
| `keyval_operator_storage_cleanup_failures_total{namespace,cluster,policy,reason}` | Counter | Storage cleanup failures (`reason=timeout|error`). |
| `keyval_operator_graceful_shutdown_duration_seconds{namespace,cluster,component,pod}` | Histogram | Time taken for pods to terminate (observed via preStop hook or container exit). |
| `keyval_operator_tls_secret_rotations_total{namespace,cluster,component}` | Counter | Number of TLS secret rotations detected. |
| `keyval_operator_service_immutable_change_total{namespace,cluster,service,field}` | Counter | Rejected service updates due to immutable field changes. |

Local verification:
```bash
kubectl -n keyval-operator-system port-forward deploy/keyval-operator-controller-manager 8080:8080
curl -s localhost:8080/metrics | grep keyval_
```

### Redis Exporter Sidecar
- `spec.metrics.enabled` (default) adds a `redis_exporter` sidecar to every Redis pod and publishes a `metrics` port on the headless service.
- The sidecar scrapes `127.0.0.1:${REDIS_PORT}`. When Redis AUTH/TLS is active the exporter reuses the resolved Secret and mounts `/tls`, switching to `rediss://` with client certificates automatically.
- Override the listening port with `spec.metrics.port`; the operator updates the container args (`--web.listen-address=:`), pod `containerPort`, and ServicePort together so hashes stay stable.
- Prometheus Operator example:
  ```yaml
  apiVersion: monitoring.coreos.com/v1
  kind: ServiceMonitor
  metadata:
    name: kv-redis-metrics
  spec:
    namespaceSelector:
      matchNames:
        - default
    selector:
      matchLabels:
        app: demo-redis
    endpoints:
      - port: metrics
        path: /metrics
        scheme: http
  ```
  For TLS-enabled clusters set `scheme: https` and reference the Redis CA / client certificates via `tlsConfig.ca`, `cert`, and `key`.

## Logging
The operator uses zap via controller-runtime. Flag `--zap-devel` controls verbosity and format:

- `--zap-devel=true` (default for Kustomize manifests) enables `logger.V(1)` messages and prints human readable `DEBUG\t...` output.
- `--zap-devel=false` (Helm default via `manager.logLevel=info`) disables the debug stream and leaves JSON info logs only.

To increase verbosity temporarily, add `--zap-devel=true` to the `keyval-operator-controller-manager` deployment or set `manager.logLevel=debug` in Helm, then restart the deployment (`kubectl -n keyval-operator-system rollout restart deploy/keyval-operator-controller-manager`).

Check the active mode by inspecting logs (`kubectl logs ...`): lines containing `level=debug` or `DEBUG` indicate debug is on. See the runbook [logging-verbosity.md](runbook/logging-verbosity.md) for detailed steps and ready-made patches.

## Kubernetes Events
The operator writes events to the `KeyValCluster` object. Inspect them via `kubectl -n <ns> describe keyvalcluster/<name>`.

| Reason | Type | Description |
|--------|------|-------------|
| `BootstrapStart` / `BootstrapFinish` | Normal | Bootstrap start/end (DR, Sentinel, TLS). Ensure Sentinel+TLS SLA ≤150 s between the two events. |
| `ExternalImportStarted` / `ExternalImportCompleted` / `ExternalImportFailed` | Normal / Normal / Warning | External data import lifecycle when `spec.bootstrap.externalSource` is set. `Completed`/`Failed` align with the `ExternalImport` condition; `Following` indicates `syncMode=Live` is keeping the replica link active until cutover. |
| `SentinelQuorumLost` / `SentinelQuorumRestored` | Warning / Normal | Sentinel quorum status (`CKQUORUM`). |
| `StartFailover` | Normal | Controlled `SENTINEL FAILOVER` invocation with type/master details. |
| `FailoverTriggered` / `FailoverCompleted` | Normal | Failover trigger and completion; `FailoverCompleted` becomes `Warning` on failure. |
| `NewMaster` | Normal/Warning | Announces the new master after failover (`Warning` when errors occur); increments `keyval_failover_completed_total`. |
| `PodEvicted` | Normal | Pod eviction during updates with reasons (`config-hash`, `image`, `resources`, `scale-down`, …). |
| `ScaleOperationStarted` / `ScaleOperationBlocked` / `ScaleOperationCompleted` | Normal / Warning / Normal | Scaling lifecycle; messages include `direction=up|down`, `current/target`, and block reasons (`DisruptionsPaused`, `SentinelQuorum`, …). |
| `RollingStepBlocked` / `RollingStepResumed` | Warning / Normal | Rollout blocks (PDBLimit, BootstrapInProgress, SentinelQuorum, DisruptionsPaused, ReplicaUnhealthy). |
| `PDBModeSwitched` | Normal | PDB mode changes (soft ↔ hard) with `component`, `mode`, `minAvailable`. |
| `ReplicationHeuristicFallback` | Warning | Sentinel unavailable or erroring; operator temporarily polls Redis pods directly. Limited to ≤1 per 30s per cluster. |
| `ReplicationAligned` / `ReplicationDrift` | Normal / Warning | Replication alignment state; handy during manual interventions. |
| `NoGoodSlave`, `ReplicaLagging`, `ReplicaRecovered`, `RuntimeConfigApplied`, `RuntimeConfigFailed`, `ResetSentinelDone`, `RoleCorrected` | Various | Additional diagnostics; consult specific messages for details. |

## Prometheus Operator Integration
Examples live in `examples/observability/`:
1. `servicemonitor.yaml` — scrape the controller metrics.
2. `podmonitor.yaml` — scrape external exporters (legacy deployments). Prefer a ServiceMonitor targeting the built-in `metrics` port when `spec.metrics.enabled`.

## Recommended Alerts
- **ReconcileFailures:** `increase(keyval_reconcile_result_total{result="error"}[15m]) > 0`
- **EvictionRejected:** `increase(keyval_operator_eviction_result_total{result="rejected"}[5m]) > 0`
- **UpdateStuck:** `max_over_time(keyval_operator_update_in_progress[10m]) == 1` and `increase(keyval_rolling_deletions_total[10m]) == 0`
- **SentinelQuorumMissing:** `keyval_operator_sentinel_quorum_healthy == 0` for longer than 2 minutes
- **BootstrapLoop:** `increase(keyval_bootstrap_failure_total[1h]) > 0`

When `KEEP_ARTIFACTS=1` is set, the e2e suite stores bootstrap-duration JSON files under `test/e2e/artifacts/<cluster>-<mode>-bootstrap.json`. Sentinel TLS scenarios include the measured duration and the 150 s SLA — useful for retrospectives and dashboard correlation.

## Grafana and Annotations
- Dashboard: `examples/observability/grafana-dashboard.json` (panels for updates, failover, bootstrap, Sentinel, eviction).
- Event annotations can be drawn from Loki/Prometheus queries such as `reason=~"StartFailover|RollingStepBlocked|PodEvicted"`.

## Diagnostics During E2E
`make e2e` deploys the operator and test clusters automatically. After a run:
```bash
# Ensure stalled metric remains zero
kubectl -n keyval-operator-system port-forward deploy/keyval-operator-controller-manager 8080 &
watch -n5 'curl -s localhost:8080/metrics | grep keyval_reconcile_result_total'

# Inspect events from a scenario
kubectl -n keyval-e2e describe keyvalcluster config-standalone-valkey | grep -A4 Events
```

For deeper debugging, enable `KEEP_ARTIFACTS=1` and review files in `test/e2e/artifacts/`.

## SLA Report (Failover & MTTR)
`make sla-report` aggregates data from `test/e2e/artifacts/` and `test/chaos/artifacts/`:

```bash
make sla-report
# for custom datasets:
make sla-report SLA_FLAGS="-chaos /tmp/chaos -e2e /tmp/e2e"
```

The command outputs `test/sla/report.json` plus a human-readable summary. The report includes:

- Average/maximum MTTR (keys `mttr.*` from chaos scenarios).
- Average/maximum quorum/failover recovery times (`quorum.*`, `failover.*`).
- Cumulative time with `allowDisruptions=false` (`disruptions.pauseSeconds`).
- Scenario inventory (name, duration, notes) and covered e2e tests.

To generate a report from custom artifacts directly via CLI:

```bash
go run ./cmd/sla-report \
  -chaos /path/to/chaos/artifacts \
  -e2e /path/to/e2e/artifacts \
  -output /tmp/sla-report.json
```

An example report lives in `test/sla/example-report.json`; unit tests in `internal/sla` use it to guard format compatibility.
