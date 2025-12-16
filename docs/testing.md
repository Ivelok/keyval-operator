# KeyVal Operator Test Matrix

This guide explains how to run the different testing layers (unit, e2e, chaos, SLA) locally and how they are wired into CI.

## 1. Unit Tests
- **Command:** `go test -race ./...`
- **Purpose:** fast coverage for controller logic and supporting packages.
- **Requirements:** Go 1.24+; no network access required.

```bash
# local run
GOFLAGS=-count=1 go test -race ./...
```

The CI job `unit` executes this command on every push/PR (see `.github/workflows/tests.yaml`).

## 2. E2E Scenarios
- **Command:** `make e2e`
- **Functionality:**
  - `docker-build` — builds the operator image (set `IMG` if you need a custom tag).
  - `deploy` — applies manifests to the cluster.
  - `go test -tags=e2e` — provisions Standalone/Sentinel `KeyValCluster` objects, injects failures, and validates invariants (OnDelete, labels, failover, runtime config).
  - Dedicated cases cover scaling (`TestSentinelScaleUpDown`, `TestSentinelScaleBlockedByQuorum`) and Sentinel rolling upgrade (`TestSentinelRollingUpgrade`), asserting events `ScaleOperation*`, `StartFailover`/`FailoverCompleted`, and metric `keyval_operator_scale_in_progress`.
  - The version matrix runs Standalone and Sentinel scenarios for Valkey 7.2 / 8.1.2 and Redis 7.2 / 8.2.1; sub jobs run in parallel limited by `E2E_PARALLELISM` to keep total duration bounded.
- **Variables:**
  - `IMG` — image name (defaults to `controller:latest`). The cluster must access it; for kind set `KIND_CLUSTER_NAME` as described below.
  - `E2E_PARALLELISM` — maximum number of concurrent e2e scenarios (default `min(6, max(1, GOMAXPROCS/2))`); backward compatible via `TESTS_NEW_PARALLELISM`.
  - `E2E_NAMESPACE`, `E2E_TIMEOUT`, `KEEP_RESOURCES`, `KEEP_ARTIFACTS` — see `docs/e2e.md`.
- **kind workflow:**
  - Create the cluster: `kind create cluster --name kv-dev`.
  - Run: `IMG=controller:dev KIND_CLUSTER_NAME=kv-dev KEEP_ARTIFACTS=1 make e2e`.
  - The target automatically calls `kind load docker-image` after the build.

In CI the matrix `k8s` (suite=`e2e`) spins up a kind cluster, runs `make e2e`, and stores artifacts (state YAML, pod list, `e2e-runtime.json`).

## 3. Chaos Scenarios
- **Command:** `make chaos`
- **What it does:** executes Go tests under `test/chaos`, which reuse the e2e framework and save JSON metrics (`*-metrics.json`). The suite includes:
  - `pod-kill-master` — kill the master plus Sentinel pods.
  - `network-latency` — temporarily hide Sentinel pods (simulate network issues).
  - `sentinel-flood` — mass Sentinel restarts.
- **Variables:**
  - `CHAOS_NAMESPACE`, `CHAOS_TIMEOUT`, `CHAOS_EXPERIMENTS`, `KEEP_ARTIFACTS`, `KEEP_RESOURCES` (see `test/chaos/README.md`).
- **Cluster preparation:** make sure the operator is built and deployed beforehand (for example `IMG=controller:dev KIND_CLUSTER_NAME=kv-dev make redeploy`).

The CI matrix `k8s` (suite=`chaos`) performs:
1. `make redeploy` (prepare image + manifests).
2. `make chaos` with `KEEP_ARTIFACTS=1`.
3. Upload artifacts from `test/chaos/artifacts`.

## 4. SLA Report
- **Command:** `make sla-report`
- **Purpose:** aggregates durations from e2e/chaos artifacts and produces `test/sla/report.json`.
- **Parameters:**
  - `SLA_FLAGS` — optional list of `-chaos/-e2e` directories to include.
  - Set `SLA_REPORT_NOW=2025-01-01T00:00:00Z` for deterministic runs (fixes the timestamp used by the CLI).

Example workflow:
```bash
# after make e2e/chaos with artifacts preserved
make sla-report
cat test/sla/report.json | jq '.summary'
```

The CI job `sla` downloads artifacts from `e2e` and `chaos`, runs `make sla-report`, and publishes the resulting JSON.

## 5. CI Overview (`.github/workflows/tests.yaml`)
| Job | Description | Artifacts |
|-----|-------------|-----------|
| `unit` | `go test -race ./...` | — |
| `k8s (suite=e2e)` | kind cluster → `make e2e` | `test/e2e/artifacts/` |
| `k8s (suite=chaos)` | kind cluster → `make redeploy` → `make chaos` | `test/chaos/artifacts/` |
| `sla` | download artifacts, run `make sla-report` | `test/sla/report.json` |

> **Runtime:** end-to-end execution takes roughly 35–40 minutes (unit ≈ 2 min, e2e ≈ 15 min, chaos ≈ 20 min, SLA < 1 min). Long-running steps are parallelized.

## 6. Working with Artifacts
- E2E: `e2e-runtime.json`, state YAMLs, and pod listings.
- Chaos: `*-metrics.json` (consumed by the SLA tool), `*-status.yaml`, `*-events.txt`.
- SLA: final `report.json` (attached in CI and available locally).

When debugging locally, set `KEEP_ARTIFACTS=1` so artifacts survive after the tests finish.

## 7. Quick Checklist Before a Run
1. `go test -race ./...`
2. `kind create cluster --name kv-dev` (if needed)
3. `IMG=controller:dev KIND_CLUSTER_NAME=kv-dev make e2e`
4. `IMG=controller:dev KIND_CLUSTER_NAME=kv-dev make chaos`
5. `make sla-report`

Compare the results with `test/sla/example-report.json` or push to CI for validation.
