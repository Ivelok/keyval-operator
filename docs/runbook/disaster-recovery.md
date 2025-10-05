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
- `kubectl -n <ns> get keyvalcluster/<name>` reports `Available=False` and `BootstrapInProgress=True`.

## Mitigation
- [ ] Enumerate PVC freshness annotations: `kubectl -n <ns> get pvc -l keyvalcluster=<name> -o jsonpath='{range .items[*]}{.metadata.name}:{.metadata.annotations.keyval\.ivelok\.io/pvc-repl-offset}{"\n"}{end}'`.
- [ ] Identify the PVC with the largest offset and annotate the corresponding pod `kubectl -n <ns> annotate pod <pod> keyval.ivelok.io/force-master=true --overwrite`.
- [ ] Restart Redis pods to trigger bootstrap: `kubectl -n <ns> delete pod -l keyvalcluster=<name>`.
- [ ] Monitor events `BootstrapStart`, `ReplicationAligned`, and `ResetSentinelDone`. Allow Sentinels to reset sequentially (wait ≥5s between pods if issuing manual resets).
- [ ] If no PVC contains valid data, permit the seed pod (ordinal 0) to become master and allow full resync.
- [ ] Once bootstrap succeeds, remove `keyval.ivelok.io/force-master` annotations to return to normal operations.

## Verification
- [ ] Event `BootstrapFinish` appears with the promoted master pod name.
- [ ] `kubectl -n <ns> get keyvalcluster/<name> -o jsonpath='{.status.masterPod}'` returns the expected pod and status conditions `Available=True`, `ReplicationHealthy=True`, `SentinelQuorum=True`.
- [ ] Master service `<name>-master` resolves to a single Ready pod.

## Cleanup
- [ ] When removing a temporary DR cluster, confirm `spec.storage.cleanupOnDelete=true` (or adjust the manifest before `kubectl delete`).
- [ ] Watch the `StorageCleanup` status condition: `DeletingPVCs` → `PVCsDeleted`. Metrics `keyval_operator_finalizer_duration_seconds{policy="delete",result}` and `keyval_operator_storage_cleanup_failures_total{reason}` show finalize duration and timeouts.
- [ ] If PVCs remain after 2 minutes (status `Timeout` or the `storage_cleanup_failures_total` counter increases), delete the leftovers manually (`kubectl delete pvc -l keyvalcluster=<name>`) and document the reason.

## Escalation
Escalate immediately to the incident commander and database reliability engineer if bootstrap fails twice or if replica data diverges (mismatched offsets). Share PVC annotation output, operator logs, and Sentinel responses.

## Related Dashboards
- [ ] Grafana: *KeyVal Bootstrap* (conditions timeline).
- [ ] Grafana: *KeyVal Operator / Failover* (event correlation during bootstrap).
