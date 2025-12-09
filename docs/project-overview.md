# KeyVal Operator — Overview

## 1. Purpose
KeyVal Operator automates Redis/Valkey 7+ clusters in Kubernetes (Standalone and Sentinel modes). Core principles: idempotent reconcile loops, minimal downtime, controlled updates, built-in DR bootstrap, and observability via events and metrics. The operator targets Kubernetes 1.26+ and installs the namespaced `KeyValCluster` CRD.

## 2. Repository Layout
- `api/v1alpha1/` — CRD types, status definitions, CEL validation.
- `controllers/` — public reconciler, dependency factory, envtest and e2e helper tests.
- `controllers/internal/` — domain logic modules:
  - `finalizer`, `reconcile`, `ssa` — reconcile loop phases and server-side apply helpers.
  - `resources`: ConfigMap/StatefulSet/Service/PDB builders using SSA.
  - `ops`: replication, Sentinel failover, status, health, runtime config, eviction, observability, bootstrap, importer, storage, update orchestration.
  - `security`: Auth/TLS resolution, `tls.Config` construction, password handling.
  - `clients`: Redis/Sentinel client factories built on go-redis v9.
  - `runtime`: controller-runtime helpers (predicates, backoff, ClusterIP preservation).
- `docs/` — architecture, observability, e2e guides, runbooks.
- `test/suites/` — Go tests compiled with `-tags=e2e` covering upgrades, failover, TLS.

## 3. Architectural Highlights
- **Dependency injection & rate limiting:** `NewKeyValClusterReconciler` receives all dependencies, including the typed rate limiter and eviction settings. No global state, which simplifies testing.
- **Logging & errors:** `log/slog` with a consistent context (`cluster`, `namespace`, `request`). Errors are labeled via `WrapTransient` / `WrapFatal`, and metric `keyval_reconcile_result_total` records the outcome.
- **Eviction wrapper:** `ops/eviction` adds timeouts, exponential backoff, metrics `keyval_operator_eviction_*`, and emits `PodEvicted` events.
- **Replication and failover:** `EnsureTopology` and `TriggerFailover` coordinate DR bootstrap, Sentinel `CKQUORUM`, `SENTINEL RESET`, and events `SentinelQuorumLost/Restored`, `StartFailover`, `NewMaster`.
- **Rolling updates:** `PlanUpdates` plus `EvaluateGuards` enforce single-pod deletion, honoring PDB, health, and bootstrap rules. The operator triggers a controlled failover before removing the master when required.

## 4. Observability
- **Prometheus metrics:**
  - `keyval_reconcile_result_total`, `keyval_reconcile_duration_seconds` — reconcile health.
  - `keyval_operator_eviction_attempt_total`, `keyval_operator_eviction_result_total` — PDB interaction diagnostics.
  - `keyval_operator_update_in_progress`, `keyval_disruptions_blocked_total`, `keyval_rolling_deletions_total` — update progress.
  - `keyval_replication_changes_total`, `keyval_operator_replication_lag_seconds`, `keyval_label_corrections_total`, `keyval_operator_pod_label_patch_conflicts_total`, `keyval_operator_pod_label_patch_retries_total` — replication and pod labels.
  - `keyval_bootstrap_attempt_total`, `keyval_bootstrap_failure_total`, `keyval_failover_triggered_total`, `keyval_failover_completed_total` — bootstrap and failover tracking.
  - `keyval_operator_sentinel_*` — Sentinel quorum state and resets.
  - `keyval_runtime_config_applied_total` / `_failed_total` — runtime config application.
  - `redis_exporter` sidecar — enabled by default via `spec.metrics`, served on the headless service port `metrics` (default 9121) with TLS/auth passthrough.
- **Kubernetes events:** `BootstrapStart/Finish`, `RollingStepBlocked/Resumed`, `SentinelQuorumLost/Restored`, `StartFailover`, `FailoverTriggered`, `NewMaster`, `PodEvicted`, `PDBModeSwitched`, `ReplicationAligned/Drift`, `RuntimeConfigApplied/Failed`.
- For a detailed table, see `docs/observability.md`.

## 5. Testing
- `go test ./...` (unit/integration) covers resource builders, replication, runtime config, eviction, status updates.
- `go test -race ./...` — mandatory before a release.
- `staticcheck ./...` — continuous quality gate.
- `make e2e` — full workflow (Standalone, Sentinel, DR, failover, TLS, PDB blocking). Key knobs: `IMG`, `E2E_NAMESPACE`, `KEEP_RESOURCES`, `KEEP_ARTIFACTS`.
- Envtest (`go test -tags=envtest ./controllers/...`) validates resource creation and idempotence (`TestReconcileStandalone_Idempotent`).

## 6. Workflow
1. **Specification** — capture scope in .codex/tasks (internal tracker) before coding.
2. **Implementation** — land small patches; after each step run `go fmt`, `goimports`, `staticcheck`, `go test`.
3. **Verification** — `go test -race ./...`, `make e2e`. Always run `make redeploy` (or `make e2e`, which includes it) before e2e checks.
4. **Documentation** — update `docs/` whenever architecture, metrics, or runbooks change.

## 7. Operations
- The operator image runs via the `keyval-operator-controller-manager` Deployment. Use `make redeploy` or `make restart` to pick up a new build.
- For manual diagnostics: `kubectl -n keyval-operator-system logs deploy/keyval-operator-controller-manager` and port-forward `:8080/metrics`.
- When using a local `controller:latest` image, ensure the cluster trusts the local registry (Docker Desktop shares it automatically).

More detail lives in `docs/architecture.md`, `docs/observability.md`, `docs/e2e.md`, and `docs/runbook/pdb-troubleshooting.md`.
