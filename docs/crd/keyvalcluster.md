**KeyValCluster CRD Design (v1alpha1)**
- API Group/Version: `keyval.ivelok.io/v1alpha1`
- Kind: `KeyValCluster`
- Scope: Namespaced
- shortNames: `kvcl`
- Modes: `Standalone` and `Sentinel`
- Kubernetes: v1.26+

**Overview**
- Manages Redis/Valkey 7+ clusters in Standalone and Sentinel modes.
- StatefulSet with OnDelete strategy (implementation task), generated ConfigMaps, role labels, failover via Sentinel.
- Clear `.status` with leader, roles, readiness, and conditions.

**.spec Fields**
- `mode` (string; required)
  - Enum: `Standalone`, `Sentinel`
- `image` (string; required)
  - Example: `valkey/valkey:7.2` or `redis:7.2`
- `engine` (string; default: `Valkey`)
  - Enum: `Redis`, `Valkey`
  - Used to select a default image when `spec.image` is not provided (`valkey/valkey:7.2` vs `redis:7.2`)
- `redisReplicas` (integer; default: 1)
  - Min: 1
  - Standalone: must be 1
  - Sentinel: must be >= 3
- `sentinelCount` (integer; optional)
  - Required for `Sentinel` mode; must be odd and >= 3
  - Must be omitted in `Standalone` mode
- `resources` (corev1.ResourceRequirements; optional)
  - Requests/Limits for Redis container
- `sentinelResources` (corev1.ResourceRequirements; optional)
  - Requests/Limits for Sentinel Pods; defaults to 50m CPU / 128Mi memory requests when omitted
  - See `examples/sentinel.yaml` for a cluster that sets distinct Redis vs Sentinel requests
- `metrics` (MetricsSpec; optional; default enabled)
  - Adds a `redis_exporter` sidecar to each Redis Pod and exposes HTTP metrics on the headless Service (`<cr>-headless`) when enabled
  - `enabled` (bool; default true) — disable to omit the exporter container and the `metrics` ServicePort
  - `image` (string; optional; default `ghcr.io/oliver006/redis_exporter:v1.75.0`)
  - `port` (integer; default 9121) — HTTP listen port for `/metrics`
  - `resources` (corev1.ResourceRequirements; optional; default requests: `20m` CPU, `64Mi` memory)
  - The exporter connects to `localhost`, reuses Redis AUTH credentials, and negotiates TLS/mTLS automatically when `spec.security` enables them
- `storage` (StorageSpec; optional)
  - `type` enum: `Persistent` (default) or `Ephemeral`
  - `size` (string quantity; required if `Persistent`)
  - `cleanupOnDelete` (bool; optional; default false) — delete PVCs when the CR is removed (`true`) or retain them for manual recovery (`false`)
  - `storageClassName` (string; optional)
  - `accessModes` ([]string; optional; default `ReadWriteOnce`)
  - Recommended StorageClass: support `allowVolumeExpansion`, deliver low latency with guaranteed fsync (NVMe/SSD). The operator suppresses pod restarts while online expansion completes; it adds `storage:resize` to the update plan only when `FileSystemResizePending` stays true for more than 2 minutes or `status.capacity` fails to reach the target for over 15 minutes. Events `StorageResizeRestartQueued` and `StorageResizeBackoff` expose fallback scenarios and provider throttling.
- `redisConfig` (map[string]string; optional)
  - Rendered into `redis.conf`
- `sentinelConfig` (map[string]string; optional)
  - Rendered into `sentinel.conf`; only in `Sentinel` mode
- `security` (SecuritySpec; optional)
  - `auth` (AuthSpec; optional)
    - `enabled` (bool) — enables Redis AUTH/ACL. When true, `passwordSecretRef` is required
    - `username` (string; optional) — ACL user name for replicas/sentinels; defaults to Redis default user when empty
    - `passwordSecretRef` (`corev1.SecretKeySelector`; required when auth enabled) — references Secret with Redis password
  - `tls` (TLSSpec; optional)
    - `enabled` (bool) — enables TLS listeners for Redis and Sentinel
    - `secretName` (string; required when enabled) — Secret containing `ca.crt`, `tls.crt`, `tls.key` (keys overridable via `caCertKey`, `certKey`, `keyKey`)
    - `requireClientAuth` (bool; default false) — enables mTLS; the operator uses the client certificate from the Secret
    - `disablePlaintext` (bool; default false) — sets `port 0`, leaving only `tls-port` active
- With `security.auth` enabled, the operator renders `requirepass`, `masterauth`, `masteruser`, and Sentinel `auth-pass/auth-user`, and the ConfigMap hash includes the Secret resourceVersion.
- With `security.tls` enabled, the configs add `tls-port`, `tls-{cert,key,ca-cert}-file`, and `tls-auth-clients`; the PodSpec mounts the Secret at `/tls` and turns on TLS for replication/Sentinel clients.
- `bootstrap` (BootstrapSpec; optional)
  - `externalSource` (ExternalSourceSpec; optional) — declarative seed from an existing Redis/Valkey deployment. When present, the operator connects to the remote endpoint before promoting local pods.
    - `address` (string; required) — redis/rediss URI of the source (supports host:port and optional `/db`).
    - `auth` (ExternalSourceAuthSpec; optional) — Secret references for ACL `usernameSecretRef` / `passwordSecretRef`. When omitted, the operator connects without authentication.
    - `tls` (ExternalSourceTLSSpec; optional) — TLS for the outbound connection. `caBundleSecretRef` supplies the CA bundle; `client{Cert,Key}SecretRef` enable mutual TLS; `insecureSkipVerify` skips server validation (not recommended).
    - `syncMode` (enum; default `Snapshot`) — `Snapshot` detaches once the replica is fully in sync; `Live` keeps the replica link active until you remove the `externalSource` block. While `Live` runs the controller reports `ExternalImport` with reason `Following` and aborts reconciliation after status updates to avoid promoting the local pods prematurely.
    - `abortIfExistingData` (bool; default true) — refuse to attach when the local datastore already contains keys.
    - `maxInitialSyncDuration` (duration; default 30m) — upper bound for the initial replication catch-up before the operator marks the import as failed.
- `health` (HealthSpec; optional)
  - `replicationLagSecondsMax` (int; default 5) — maximum lag for the `ReplicationHealthy` condition
  - `failoverTimeoutSeconds` (int; default 20) — allowed failover window before the operator treats it as an incident
  - `minReplicasForSafety` (int; default 2) — minimum number of healthy members (master + replicas) required to permit voluntary evictions; defaults effectively to 1 in Standalone mode
- `replicationHealth` (legacy name; optional)
  - Retained for backward compatibility with early releases; only controls `replicationLagSecondsMax`
- `service` (ServiceSpec; optional)
  - Controls `<cr>-master` and `<cr>-headless` Services
  - `create` (bool; default true), `type` (`ClusterIP|NodePort|LoadBalancer`), `annotations`, `labels`, `ports.redis`
  - When `create=false`, the operator deletes both Services (event `ServiceRemoved`, metric `keyval_operator_service_removed_total{service="master|headless"}`)
- `sentinelService` (ServiceSpec; optional)
  - Controls `<cr>-sentinel` Service in Sentinel mode
  - `ports.sentinel` default 26379
  - When `create=false`, the `<cr>-sentinel` Service is removed with event/metric parity (`service="sentinel"`)
- `replicasService` (ServiceSpec; optional)
  - Controls `<cr>-replicas` read service
  - `create=false` removes the Service (metric label `service="replicas"`)
- `podLabels` / `podAnnotations` (map[string]string; optional)

**.status Fields**
- `masterPod` (string)
  - Current master Pod name
- `replicas` (int)
  - Desired Redis Pods (mirrors `.spec.redisReplicas`)
- `rolesSource` (`sentinel|probe|forced`)
  - Role discovery source (`sentinel` = CKMASTER API, `probe` = direct Redis polling, `forced` = manual override during bootstrap)
- `readyReplicas` (int)
  - Number of ready Redis Pods
- `roles` ([]PodRoleStatus)
  - `name` (string), `role` (`master|replica|sentinel`), `ready` (bool), `lastTransitionTime` (time)
- `conditions` ([]Condition)
  - `Reconciled`, `Available`, `SentinelQuorum`, `ReplicationHealthy`, `FailoverInProgress`, `DisruptionsPaused`, `UpgradeInProgress`, `BootstrapInProgress`, `ExternalImport`, `RuntimeConfigApplied`, `StorageCleanup`
  - `StorageCleanup` reports the PVC policy (`CleanupEnabled` / `RetentionPolicy` / `EphemeralStorage`) and the finalizer progress (`DeletingPVCs`, `PVCsDeleted`, `Timeout`).
  - `BootstrapInProgress=True` means a DR/bootstrap cycle is active: the master is re-elected (force-master → freshest PVC → seed), the Redis PDB switches to strict mode, `DisruptionsPaused=True`, and the flag clears only after `CKQUORUM` succeeds followed by sequential `SENTINEL RESET` operations.
- `healthGate` (HealthGateStatus)
  - `allowDisruptions` (bool) — aggregated safety flag controlling voluntary evictions and updates
- `externalImport` (ExternalImportStatus; optional)
  - `mode` (string) — `Snapshot` or `Live` (mirrors `spec.bootstrap.externalSource.syncMode`).
  - `state` (string) — `Pending`, `InProgress`, `Following`, `Completed`, or `Failed`.
  - `source` (string) — address of the remote endpoint.
  - `startedAt` / `lastSynced` (timestamps) — most recent import timeline.
  - `message` (string) — human-readable progress/failure notes.

**Validation Rules (OpenAPI/CEL)**
- `mode` enum: `Standalone`, `Sentinel`
- `redisReplicas >= 1`
- `mode == Standalone => redisReplicas == 1`
- `mode == Standalone => sentinelCount, sentinelConfig omitted`
- `mode == Sentinel => redisReplicas >= 3`
- `mode == Sentinel => sentinelCount present, odd, and >= 3`
- `metadata.name` length <= 63 characters (root x-kubernetes-validations)
- Immutable: `spec.mode` cannot change after creation (`oldSelf` validation)

**Printer Columns**
- Mode: `.spec.mode`
- Replicas: `.spec.redisReplicas`
- Master: `.status.masterPod`

**Versioning**
- CRD version: `v1alpha1`
- Evolution path: `v1beta1` → `v1` after field stability and controller maturity
- Backward-compatibility: additive changes only; breaking changes gated behind new version

**Migration Constraints**
- Changing `spec.mode` is not supported after creation (immutable)
- Scaling:
  - Standalone: `redisReplicas` fixed at 1
  - Sentinel: `redisReplicas` may scale (>=3); `sentinelCount` scales in odd increments (3, 5, 7, ...). The controller emits `ScaleOperationStarted/Blocked/Completed` events and updates metrics `keyval_operator_scale_in_progress` and `keyval_scale_operations_total{direction}`; consult `docs/runbook/scale-and-upgrade.md` when operations are blocked.
- Resource split: pre-change clusters where Sentinels inherited Redis requests should set `spec.sentinelResources` before upgrading if they need to retain the previous sizing; otherwise defaults will apply on next reconcile.
- Force-master override: annotate the pod `kubectl annotate pod/<cr>-N keyval.ivelok.io/force-master=true` before restart/DR to make the operator pick that PVC as the source of truth.

**DNS/Names**
- Enforce CR `metadata.name` length <= 63 for safe child resource naming (`<cr>-headless`, `<cr>-master`, etc.)
- If stricter policy is required, consider a ValidatingWebhook to enforce cross-field invariants and name length consistently.

**Edge Cases**
- Very long names near 63 chars: ensure generated names (services, pods) remain <= 63
- Invalid combos: `sentinelCount` set when `mode=Standalone` → validation error
- Scale up/down in Sentinel: maintain odd `sentinelCount`, keep one-master invariant
- Sentinel StatefulSet: fields `spec.serviceName`, `spec.selector`, and `spec.volumeClaimTemplates` are immutable. Drift attempts trigger the `SentinelApplyImmutableField` event, set `Reconciled=False` with reason `ImmutableField`, and increment `keyval_operator_sentinel_apply_failures_total{reason="immutable_field"}`; other SSA errors are treated as transient and surface via `SentinelApplyFailed`.

**Examples**
- See `examples/standalone.yaml`, `examples/sentinel.yaml`, and `examples/external-import.yaml` for valid CRs.

**Kubebuilder Notes**
- CRD ready for generation via `controller-gen` (validation tags and `XValidation` rules present).
- Uses CEL validations that require Kubernetes v1.26+ (incl. `oldSelf` for immutability).
