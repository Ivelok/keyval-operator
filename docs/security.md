# KeyVal Operator Security Guide

The operator now supports first-class authentication, TLS, and hardened pod defaults. This document explains how to enable and operate those features.

## 1. Spec Overview

`spec.security` controls cluster-level authentication and transport settings:

```yaml
spec:
  security:
    auth:
      enabled: true
      username: app
      passwordSecretRef:
        name: demo-redis-auth
        key: password
    tls:
      enabled: true
      secretName: demo-redis-tls
      requireClientAuth: true
      disablePlaintext: true
```

- `auth.enabled`: when true the operator reads the referenced Secret and renders `requirepass`, `masterauth`, and Sentinel `auth-pass/auth-user` directives. Replication, failover, runtime config, and bootstrap clients use the same credentials automatically.
- `auth.username`: optional ACL user for replicas/Sentinels. Leave empty to keep the Redis `default` user.
- `auth.passwordSecretRef`: Secret key selector pointing at the Redis password. The Secret is never logged; only its resourceVersion/name participates in config hashes.
- `tls.enabled`: enables TLS listeners (`tls-port`) for Redis and Sentinel. The Secret must contain the usual keys (`ca.crt`, `tls.crt`, `tls.key`), overridable via `caCertKey/certKey/keyKey`.
- `tls.requireClientAuth`: switches replicas/Sentinals/operator clients to mTLS (`--tls --cert --key`).
- `tls.disablePlaintext`: sets `port 0`, so only `tls-port` stays open.

## 2. Authentication Workflow

1. Create the password Secret:
   ```bash
   kubectl create secret generic demo-redis-auth \
     --from-literal=password='S3cureP@ss!'
   ```
2. Patch the `KeyValCluster` with `spec.security.auth.enabled=true` and reference the Secret.
3. The operator reconciles the ConfigMap and statefulset; pods restart one-by-one with `requirepass`/`masterauth`. Sentinels authenticate automatically via `AUTH <user> <pass>`.
4. Clients must use `AUTH` (or ACL with username) from now on.

Changing the password is handled by updating the Secret and the spec; the operator recalculates the config hash and performs a safe rolling restart.

## 3. TLS Enablement

1. Prepare a Secret containing `ca.crt`, `tls.crt`, and `tls.key` (PEM):
   ```bash
   kubectl create secret tls demo-redis-tls \
     --cert=server.pem --key=server.key
   kubectl patch secret demo-redis-tls --type merge -p '{"data":{"ca.crt":"$(base64 < ca.pem)"}}'
   ```
2. Set `spec.security.tls.enabled=true` and reference the Secret. Optional:
   - `requireClientAuth=true` enables mTLS.
   - `disablePlaintext=true` closes the plain TCP port by rendering `port 0`.
3. The ConfigMap gains TLS directives, pods mount the Secret at `/tls`, and `redis-cli` inside the bootstrap/init container runs with `--tls`/`--cert`/`--key` flags.
4. Verify:
   ```bash
   kubectl exec demo-0 -- redis-cli --tls --cacert /tls/ca.crt \
     --cert /tls/tls.crt --key /tls/tls.key PING
   ```

### 3.1. TLS Sentinel bootstrap SLO

- **SLA:** A Sentinel cluster with TLS/mTLS enabled must reach `Available=True` no later than **150 s** after the CR is created. The threshold is enforced by the e2e test `TestSentinelAuthTLS` and validated in CI.
- **Monitoring:** Track the delta between the `BootstrapStart` and `BootstrapFinish` events (or inspect `kubectl describe kvc/<name>`). The metric `keyval_bootstrap_attempt_total` increments at the start, while `keyval_bootstrap_failure_total` indicates failures. Successful attempts equal `max_over_time(keyval_bootstrap_attempt_total{cluster="<name>"}[5m]) - max_over_time(keyval_bootstrap_failure_total{cluster="<name>"}[5m])`.
- **Artifacts:** When `KEEP_ARTIFACTS=1`, the e2e suite stores `artifacts/<cluster>-sentinel-tls-bootstrap.json` with the observed duration and threshold for later analysis.
- **Alerting:** If `BootstrapFinish` is missing within 150 s, inspect the Secret (`kubectl get secret <tls> -o yaml`), readiness probes (`kubectl get pod <name> -o jsonpath='{.status.conditions}'`), and init-container logs (`redis-cli --tls --cacert ...`). Maintain resource headroom—CPU or IO starvation often prolongs handshakes.

## 4. Pod Security Defaults

- All Redis and Sentinel pods run as a non-root user (`uid/gid 1000`, `fsGroup 1000`) and drop all Linux capabilities. `allowPrivilegeEscalation` is disabled.
- Redis containers use a read-only root filesystem; writable paths are provided via `/data`, `/conf`, and `/runtime-conf` volumes.
- Init containers inherit the same non-root context and rely on `fsGroup` to write into ConfigMap-backed volumes.

These defaults satisfy the Kubernetes Pod Security `restricted` profile out of the box.

## 5. Network Policies

The operator does not create NetworkPolicies automatically, but examples are provided under `examples/networkpolicy/`:

- `redis.yaml`: allows ingress to Redis pods only from labelled client namespaces and keeps replica traffic internal to the cluster.
- `sentinel.yaml`: restricts access to Sentinel TCP port 26379 to the operator and Redis pods.

Customize the placeholders (`<cluster-name>`, namespace labels) before applying them in your environment.

## 6. RBAC Footprint

The operator runs with a single ClusterRole scoped to the resources it reconciles. The table below lists the effective permissions applied by both Kustomize and Helm deployments:

| API Group              | Resource (subresource)      | Verbs                              | Purpose |
|------------------------|-----------------------------|------------------------------------|---------|
| `core`                 | `pods`                      | `get`, `list`, `watch`, `patch`, `delete` | Inspect readiness/labels, patch labels, evict failed pods |
| `core`                 | `services`                  | `get`, `list`, `watch`, `create`, `patch`, `delete` | Manage headless/master services and disable them on request |
| `core`                 | `configmaps`                | `get`, `list`, `watch`, `create`, `patch` | Render and roll config hashes |
| `core`                 | `events`                    | `create`, `patch`, `update`        | Emit reconciliation events |
| `core`                 | `secrets`                   | `get`, `list`, `watch`             | Read TLS/auth material referenced by the CR |
| `core`                 | `persistentvolumeclaims`    | `get`, `list`, `watch`, `patch`, `update`, `delete` | Track resize status, drop PVCs when `cleanupOnDelete=true` |
| `core`                 | `pods/eviction`             | `create`                           | Perform safe pod evictions during rolling updates |
| `apps`                 | `statefulsets`              | `get`, `list`, `watch`, `create`, `update`, `patch`, `delete` | Manage Redis/Sentinel StatefulSets via server-side apply and delete Sentinel workloads during mode transitions |
| `policy`               | `poddisruptionbudgets`      | `get`, `list`, `watch`, `create`, `patch`, `delete` | Coordinate disruption budgets for Redis/Sentinel, remove during cleanup |
| `coordination.k8s.io`  | `leases`                    | `get`, `list`, `watch`, `create`, `update`, `patch` | Controller-runtime leader election |
| `keyval.ivelok.io`     | `keyvalclusters`            | `get`, `list`, `watch`, `patch`, `update` | Read CR spec and annotate/patch status fields |
| `keyval.ivelok.io`     | `keyvalclusters/status`     | `get`, `patch`, `update`           | Publish status updates |
| `keyval.ivelok.io`     | `keyvalclusters/finalizers` | `get`, `patch`, `update`           | Manage finalizer lifecycle |

To verify a live deployment, run:

```bash
hack/verify-rbac.sh # optionally pass <serviceAccount> <namespace>
```

The check issues `kubectl auth can-i` calls for every rule while impersonating the controller ServiceAccount. CI runs `go test ./test/rbac` to ensure the rendered Kustomize and Helm roles stay in sync with the table above.

## 7. Troubleshooting

- If pods loop with `NOAUTH`, ensure the password Secret exists and `spec.security.auth.enabled=true`.
- For TLS handshake failures, verify the Secret keys align with `caCertKey/certKey/keyKey` and that clients pass `--tls`.
- The config hash stored in `StatefulSet.spec.template.annotations['keyval.ivelok.io/config-hash']` changes when secrets rotate; the operator orchestrates a rolling restart. Check the annotation to confirm.
