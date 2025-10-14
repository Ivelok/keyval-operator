# Runbook: Disaster Recovery Bootstrap

## Runbook Metadata
Severity: Sev1
Time to Mitigate: 45m
Responsible: DR Coordinator

## Overview
Use this runbook to perform a disaster recovery (DR) bootstrap when the majority of Redis pods are lost or storage is restored from backups. It leverages PVC freshness annotations and force-master controls provided by the KeyVal operator.

## Detection
- More than one Redis pod missing or PVCs recently restored from snapshot/backups.
- Operator events `BootstrapStart` with `reason=BootstrapSelectingMaster` repeating without `BootstrapFinish`.
- `kubectl -n <ns> get keyvalcluster/<name>` reports `Available=False`, `BootstrapInProgress=True`, or `ExternalImport` stuck in `Pending`/`InProgress`.

## Mitigation
- [ ] Enumerate PVC freshness annotations: `kubectl -n <ns> get pvc -l keyvalcluster=<name> -o jsonpath='{range .items[*]}{.metadata.name}:{.metadata.annotations.keyval\.ivelok\.io/pvc-repl-offset}{"\n"}{end}'`.
- [ ] Identify the PVC with the largest offset and annotate the corresponding pod `kubectl -n <ns> annotate pod <pod> keyval.ivelok.io/force-master=true --overwrite`.
- [ ] If a donor Redis/Valkey instance is available (another KeyValCluster, managed service, or dedicated replica), configure external import before restarting pods:
      ```bash
      kubectl -n <ns> patch keyvalcluster/<name> --type merge -p '{"spec":{"bootstrap":{"externalSource":{"address":"redis://<donor-host>:6379","syncMode":"Snapshot"}}}}'
      ```
-      Switch `syncMode` to `Live` if you want the new cluster to follow the donor continuously until you decide to cut over. When authentication or TLS are required, pre-create a Secret and reference it via `spec.bootstrap.externalSource.auth` / `tls`. The operator attaches the bootstrap pod as a replica, copies data, and — in `Snapshot` mode — detaches automatically. In `Live` mode it holds the replica link and keeps reporting `ExternalImport` with reason `Following` until you remove the `externalSource` block.
-      Monitor the `ExternalImport` condition for progress: `WaitingForPods` → `Syncing` → `Following` (Live) or `Completed` (Snapshot). When you are ready to promote the new cluster in Live mode, execute `kubectl -n <ns> patch keyvalcluster/<name> --type merge -p '{"spec":{"bootstrap":{"externalSource":null}}}'` to trigger the cutover. Expect `ExternalImport` to flip to `Completed` and the master pod to be promoted locally.
- [ ] Restart Redis pods to trigger bootstrap: `kubectl -n <ns> delete pod -l keyvalcluster=<name>`.
- [ ] Monitor events `BootstrapStart`, `ReplicationAligned`, and `ResetSentinelDone`. Allow Sentinels to reset sequentially (wait ≥5s between pods if issuing manual resets).
- [ ] If no PVC contains valid data, permit the seed pod (ordinal 0) to become master and allow full resync.
- [ ] Once bootstrap succeeds, remove `keyval.ivelok.io/force-master` annotations to return to normal operations.

## Verification
- [ ] Event `BootstrapFinish` appears with the promoted master pod name. When external import is used, also confirm `ExternalImport` reports `Completed` with the expected source (or `Following` if you intentionally keep Live replication active).
- [ ] `kubectl -n <ns> get keyvalcluster/<name> -o jsonpath='{.status.masterPod}'` returns the expected pod and status conditions `Available=True`, `ReplicationHealthy=True`, `SentinelQuorum=True`.
- [ ] Master service `<name>-master` resolves to a single Ready pod.

## Cleanup
- [ ] When removing a temporary DR cluster, confirm `spec.storage.cleanupOnDelete=true` (or adjust the manifest before `kubectl delete`). If `spec.bootstrap.externalSource` was set, remove it once the seeding is complete to avoid accidental re-imports.
- [ ] Watch the `StorageCleanup` status condition: `DeletingPVCs` → `PVCsDeleted`. Metrics `keyval_operator_finalizer_duration_seconds{policy="delete",result}` and `keyval_operator_storage_cleanup_failures_total{reason}` show finalize duration and timeouts.
- [ ] If PVCs remain after 2 minutes (status `Timeout` or the `storage_cleanup_failures_total` counter increases), delete the leftovers manually (`kubectl delete pvc -l keyvalcluster=<name>`) and document the reason.

## Escalation
Escalate immediately to the incident commander and database reliability engineer if bootstrap fails twice or if replica data diverges (mismatched offsets). Share PVC annotation output, operator logs, and Sentinel responses.

## Related Dashboards
- [ ] Grafana: *KeyVal Bootstrap* (conditions timeline).
- [ ] Grafana: *KeyVal External Import* (counters `keyval_external_import_*`, duration histogram).
- [ ] Grafana: *KeyVal Operator / Failover* (event correlation during bootstrap).
