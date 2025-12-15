# Operator E2E Tests

The `make e2e` target runs the operator through its primary user workflows inside the local Kubernetes cluster. The suite performs a full cycle: builds the controller image, redeploys the operator, provisions Standalone and Sentinel clusters, injects failures, and asserts every invariant.

## Prerequisites
- Docker with permission to build local images.
- A reachable `kubectl` context (Docker Desktop/kind/minikube) with the `keyval-operator-system` namespace and the CRDs already installed.
- The cluster must be able to pull the image referenced by `IMG` (defaults to `controller:latest`).
- If you build on an ARM host (for example Apple Silicon) but your cluster nodes are `amd64`, set `DOCKER_PLATFORM=linux/amd64` when building/pushing images.
- If `go test` requires envtest assets, export `KUBEBUILDER_ASSETS` just as you do for the unit tests.

## Primary Scenario
```bash
make e2e
```
The command:
1. Runs `docker-build` and `kubectl apply` through `make deploy` (including a rollout restart).
2. Executes `go test -tags=e2e ./test/e2e`, creating the `keyval-e2e` namespace plus Standalone and Sentinel `KeyValCluster` objects.
3. Applies runtime-allowed changes to `redisConfig`/`sentinelConfig` (for example `maxmemory`, `down-after-milliseconds`) and verifies pod UIDs and Services stay intact.
4. Checks readiness, deletes the master pod, restarts Sentinel pods, triggers a rolling update with a restart-required flag (`appendonly`), and validates that all invariants hold.

Repeated runs are idempotent: the suite tears down every resource on exit unless instructed to keep them.

> **Important:** Always run `make redeploy` before any `go test -tags=e2e ...` invocation (including focused tests). This guarantees the freshly built operator image is loaded and the deployment restarted before validation.

The E2E harness also removes lingering PVCs labeled `keyvalcluster=<name>` before recreating a cluster so that stale Redis/Sentinel data never affects subsequent runs.

## Additional Makefile Targets
- `make e2e-setup` — build the image and apply the operator manifests only.
- `make e2e-test` — run the Go suite without rebuilding or redeploying (uses the current deployment).
- `make e2e-clean` — delete the namespace and KeyValCluster resources created by the suite.
- `go test -tags=e2e ./test/suites/security -run TLSPDBEviction` — targeted check for eviction blocking during TLS rotation with a strict PDB.
- `make chaos` / `make chaos-test` — resurrected chaos suite (see [`docs/runbook/failover-stuck.md`](runbook/failover-stuck.md)) that stresses failover under network faults and restart storms.

## Useful Environment Variables
| Variable | Default | Purpose |
| --- | --- | --- |
| `IMG` | `controller:latest` | Operator image to build and deploy before the test. |
| `DOCKER_PLATFORM` | _(empty)_ | Optional `docker build --platform` value (for example `linux/amd64`). |
| `E2E_NAMESPACE` | `keyval-e2e` | Namespace where the scenario provisions resources. |
| `E2E_TIMEOUT` | `15m` | `go test` timeout. |
| `KEEP_RESOURCES` | `0` | Set to `1/true` to keep CRs/pods after the suite finishes. |
| `KEEP_ARTIFACTS` | `0` | When enabled, saves state YAML and pod listings to `test/e2e/artifacts/`; automatically enabled on failure. |
| `E2E_REDIS_IMAGE` | `valkey/valkey:7.2` | Override the Redis/Valkey image used in the scenarios. |
| `E2E_SENTINEL_IMAGE` | _(empty)_ | Custom Sentinel sidecar image; defaults to the main image when left empty. |

For focused debugging you can skip the redeploy stage:
```bash
make e2e-test KEEP_RESOURCES=1 KEEP_ARTIFACTS=1 E2E_TIMEOUT=20m
```

## Logs and Artifacts
When a test fails (or when `KEEP_ARTIFACTS=1`) the suite persists:
- `test/e2e/artifacts/<cluster>.yaml` — final `KeyValCluster` status snapshot.
- `test/e2e/artifacts/<cluster>-pods.txt` — pod statuses at the end of the run.

These artifacts make reproducing issues easier without rerunning the entire suite.

## What the E2E Suite Verifies
- Proper provisioning of Standalone and Sentinel `KeyValCluster` objects.
- Single-master invariant plus correct Service/Endpoint resources, including custom port setups.
- Runtime application of allow-listed settings (`maxmemory`, `down-after-milliseconds`) without restarting pods.
- Successful recovery after deleting the master and two Sentinel pods in sequence.
- DR bootstrap: delete all Redis pods and Sentinels, ensure master selection (force-master → freshest PVC), capture `BootstrapStart/Finish`, sequential `ResetSentinelDone`, and release the strict PDB once quorum returns.
- Rolling update triggered by a restart-required parameter (`appendonly`) and completion without status drift.
- Rolling updates go through the Eviction API; the suite checks for the `keyval.ivelok.io/update-blocked` annotation and the reset of `keyval_operator_update_in_progress` gauge.
- Health gate enforcement: when `status.healthGate.allowDisruptions=false` (via `minReplicasForSafety`), the operator blocks updates, emits `RollingStepBlocked` with reason `DisruptionsPaused`, and increments `keyval_disruptions_blocked_total`.
- PDB pressure: an artificial `PodDisruptionBudget` returns `429`; the operator cycles `RollingStepBlocked/Resumed` and resumes rollout once the constraint is removed.
- Sentinel failover: when only the master remains in the plan, the update triggers a controlled `SENTINEL FAILOVER`, emits `FailoverTriggered/Completed`, and updates `status.masterPod`.
- Sentinel-only updates: changing `sentinelResources` restarts Sentinel pods one at a time, keeps Redis pod UIDs stable, and matches the expected resource requests.
- Metrics and events remain exposed via `/metrics`; scenarios assert `keyval_disruptions_blocked_total` and `keyval_operator_update_in_progress` before/after operations.
- Idempotence: after updates finish, the suite re-runs reconcile, confirms `keyval_reconcile_result_total{result="stalled"}` does not grow, and finds `reconciled ... no rolling action` in the logs without reapplying resources.

Run `make e2e` together with `go test ./...` and `staticcheck ./...` before committing substantial changes.

## Manual Replication Intervention

The operator automatically tracks role drift and replica lag; manual work is only required for diagnostics:

1. Promote a replica to master for experimentation:
   ```bash
   kubectl -n <ns> exec pod/<cluster>-redis-1 -c redis -- redis-cli replicaof no one
   ```
   The CR emits `ReplicationDrift` with pod names, and `keyval_operator_replication_lag_seconds{pod=...}` becomes non-zero temporarily.
2. Wait for `ReplicationAligned` — the operator issues `REPLICAOF <master>` and, in Sentinel mode, schedules sequential `SENTINEL RESET` operations until `ResetSentinelDone` fires for each pod.
3. Inspect status via:
   ```bash
   kubectl -n <ns> get keyvalcluster <cluster> -o jsonpath='{.status.conditions[?(@.type=="ReplicationHealthy")].status}'
   kubectl -n <ns> exec pod/<cluster>-redis-1 -c redis -- redis-cli info replication | grep master_link_status
   ```
4. To force a specific pod to become master, annotate it with `keyval.ivelok.io/force-master=true` and delete the pod; the next reconcile promotes it while resetting Sentinels.

Key metrics to watch:
- `keyval_operator_replication_lag_seconds{pod}` — actual lag in seconds.
- `keyval_operator_sentinel_quorum_healthy` — quorum status (0/1).
- `keyval_replication_changes_total` — topology change counter.
- `keyval_disruptions_blocked_total{reason="Bootstrap"}` — indicates rollouts paused until quorum/replication recovers.
