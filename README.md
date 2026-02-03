# KeyVal Operator

KeyVal Operator provides lifecycle management for Redis and Valkey clusters on Kubernetes. It automates configuration, replication alignment, Sentinel-driven failover, rolling updates, and operational safeguards such as PodDisruptionBudget gating and storage resize orchestration.

## Features
- Declarative management of standalone and Sentinel-backed Redis/Valkey deployments via the `KeyValCluster` CRD.
- Idempotent reconciler with controlled failover, replica health evaluation, and disruption safety gates.
- Dedicated metrics and events for observability, including scale/upgrade telemetry and SLA aggregation.
- Built-in `redis_exporter` sidecar (enabled by default) that reuses Redis AUTH/TLS and exposes `/metrics` on the headless service.
- Chaos and e2e test suites covering failover, storage, Sentinel quorum, and network degradation scenarios.
- Helm chart packaging with configurable auth, TLS, storage, and health thresholds.

## Controller Configuration
The controller process exposes tunables for Redis/Sentinel client IO and the reconcile backoff loop. Defaults are applied through the `keyval-operator-config` ConfigMap and can be overridden via CLI flags or environment variables (the deployment consumes the ConfigMap via `envFrom`). Each value accepts standard Go duration syntax (`200ms`, `5s`, `1m`) unless noted otherwise.

| Flag / Env | Description | Default |
|-------------|-------------|---------|
| `--redis-dial-timeout` / `KEYVAL_REDIS_DIAL_TIMEOUT` | TCP connect timeout for Redis/Sentinel clients. | `3s` |
| `--redis-read-timeout` / `KEYVAL_REDIS_READ_TIMEOUT` | Socket read deadline per Redis command. | `2s` |
| `--redis-write-timeout` / `KEYVAL_REDIS_WRITE_TIMEOUT` | Socket write deadline per Redis command. | `2s` |
| `--redis-pool-timeout` / `KEYVAL_REDIS_POOL_TIMEOUT` | Wait time for an idle connection before failing a command. | `2s` |
| `--redis-operation-timeout` / `KEYVAL_REDIS_OPERATION_TIMEOUT` | Context timeout imposed on each Redis command before retry. | `2s` |
| `--sentinel-operation-timeout` / `KEYVAL_SENTINEL_OPERATION_TIMEOUT` | Timeout for Sentinel commands (`CKQUORUM`, `RESET`, `FAILOVER`). | `5s` |
| `--redis-max-retries` / `KEYVAL_REDIS_MAX_RETRIES` | Number of retries per Redis/Sentinel command (initial attempt + retries). | `2` |
| `--redis-min-idle-conns` / `KEYVAL_REDIS_MIN_IDLE_CONNS` | Minimum idle connections kept in the go-redis pool. | `1` |
| `--redis-retry-backoff-initial` / `KEYVAL_REDIS_RETRY_INITIAL_BACKOFF` | Initial delay between client retries. | `200ms` |
| `--redis-retry-backoff-max` / `KEYVAL_REDIS_RETRY_MAX_BACKOFF` | Maximum delay between client retries. | `1s` |
| `--redis-retry-backoff-factor` / `KEYVAL_REDIS_RETRY_BACKOFF_FACTOR` | Multiplier applied to the client retry backoff. | `2` |
| `--redis-retry-jitter` / `KEYVAL_REDIS_RETRY_JITTER` | Fractional jitter applied to client retry delays. | `0.1` |

Reconcile concurrency and Kubernetes API rate limits are derived automatically from the workload profile (`small`, `medium`, `large`). Profiles are detected at startup based on the number of `KeyValCluster` resources and their replica counts. A debug-only override is available through the environment variable `KEYVAL_OPERATOR_RECONCILE_PROFILE`; when set, the controller logs a warning. Helm values do not expose this override.

Updating the ConfigMap requires restarting the `keyval-operator-controller-manager` Deployment (`make restart` or `kubectl -n keyval-operator-system rollout restart deploy/keyval-operator-controller-manager`).

## Installation (Helm)
The chart is published as an OCI artifact in GitHub Container Registry (GHCR). Helm does not support `helm repo add` for OCI sources, so use the OCI workflow directly.

### Install from GHCR
```bash
# (Optional) authenticate if the GHCR repository is private
echo "$GITHUB_PAT" | helm registry login ghcr.io --username <github-username> --password-stdin

helm install keyval-operator oci://ghcr.io/ivelok/keyval-operator/keyval-operator \
  --namespace keyval-operator-system --create-namespace \
  --version <chart-version>
```

You can also download the chart locally with `helm pull oci://ghcr.io/ivelok/keyval-operator/keyval-operator --version <chart-version>`.

### Publish a new chart version
```bash
helm package charts/keyval-operator
helm push keyval-operator-<chart-version>.tgz oci://ghcr.io/ivelok/keyval-operator
```

Bump `version` and `appVersion` in [`charts/keyval-operator/Chart.yaml`](charts/keyval-operator/Chart.yaml) before packaging.

Key values (see [`charts/keyval-operator/values.yaml`](charts/keyval-operator/values.yaml)):

| Value | Description |
|-------|-------------|
| `image.repository`, `image.tag` | Override controller container image. |
| `manager.replicas` | Operator deployment replicas (default 1). |
| `manager.resources` | Resource requests/limits for the controller manager. |
| `manager.metricsService.create` | Expose metrics service. |
| `examples.enabled` | Deploy sample `KeyValCluster` resources. |
| `examples.clusters[].spec` | Declarative KeyValCluster spec supporting auth/tls/pdb thresholds. |

Detailed Helm values and defaults are documented in `charts/keyval-operator/README.md`.

CRDs are shipped in `charts/keyval-operator/crds/`. Helm installs them on first install, but does not
upgrade CRDs on `helm upgrade`. When upgrading across a version that changes the CRD schema, apply the
updated CRDs manually before upgrading the release:

```bash
helm show crds oci://ghcr.io/ivelok/keyval-operator/keyval-operator --version <new-version> | kubectl apply -f -
```

For advanced scenarios (auth secrets, TLS bundles, PDB strictness), consult [`docs/install.md`](docs/install.md).

## Creating a KeyValCluster
```yaml
apiVersion: keyval.ivelok.io/v1alpha1
kind: KeyValCluster
metadata:
  name: kv-sentinel
spec:
  mode: Sentinel
  redisReplicas: 3
  sentinelCount: 3
  image: valkey/valkey:7.4
  storage:
    type: Persistent
    size: 20Gi
  security:
    auth:
      enabled: true
      passwordSecretRef:
        name: kv-auth
        key: password
  health:
    replicationLagSecondsMax: 5
    failoverTimeoutSeconds: 20
    minReplicasForSafety: 2
```

Apply with `kubectl apply -f <file>` after installing the operator.

Sentinel clusters own a dedicated StatefulSet. Kubernetes treats fields like `spec.serviceName`, `spec.selector`, and `spec.volumeClaimTemplates` as immutable; if they drift, the operator emits a `Warning` event (`SentinelApplyImmutableField`), marks `status.conditions[Reconciled]` as `False` with reason `ImmutableField`, and increments the Prometheus counter `keyval_operator_sentinel_apply_failures_total{reason="immutable_field"}`. Other apply failures surface through `SentinelApplyFailed` and trigger reconcile backoff.

## Operational Runbooks
Runbooks are stored under [`docs/runbook/`](docs/runbook). Key scenarios include failover recovery, Sentinel quorum loss, replica desync, DR bootstrap, disruption resumption, log verbosity adjustments, and release execution. Validate their structure with `make runbook-check`.

## Development
- Format & lint: `make fmt` + `make lint`
- Unit tests: `go test -race -ldflags="-extldflags=-Wl,-w" ./...`
- E2E suite: `make e2e`
- Chaos suite: `make chaos`
- Helm lint/template: `make helm-lint helm-template`

## Release Workflow
Releases are tag-driven (`vX.Y.Z`) and executed by `.github/workflows/release.yaml`. The workflow builds & signs images, packages the Helm chart, uploads artifacts, and publishes changelog notes. Manual invocation is available via `workflow_dispatch`. Local dry-run: `make release VERSION=X.Y.Z RELEASE_REGISTRY=ghcr.io/<org>`.

## Contributing
Please review [`AGENTS.md`](AGENTS.md) for repository-wide conventions. Submit PRs against `develop`, ensure all tests (unit/e2e/chaos) pass, and update runbooks when adding operational functionality.
