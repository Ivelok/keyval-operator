# Runbook: Sentinel Failover Stuck or Not Completing

## Runbook Metadata
Severity: Sev1
Time to Mitigate: 15m
Responsible: SRE On-call

## Overview
When the KeyVal operator orchestrates a controlled failover, Sentinels must promote a replica within seconds. This runbook explains how to recover when failover events remain pending (`StartFailover` without `FailoverCompleted`) or the master label never changes.

## Detection
- Event stream shows repeated `StartFailover` or `FailoverCompleted` with `error=` messages.
- Condition `FailoverInProgress=True` persists for >2 minutes.
- Metrics: `keyval_operator_update_in_progress=1` and `keyval_operator_sentinel_quorum_healthy=1`, but `keyval_operator_replication_lag_seconds{role="replica"}` is high or `NoGoodSlave` events fire.

## Mitigation
- [ ] Check Sentinel health: `kubectl -n <ns> get pods -l app=<cluster>-sentinel` and ensure ≥quorum ready.
- [ ] Inspect operator logs for `NOGOODSLAVE` or auth failures: `kubectl -n keyval-operator-system logs deploy/keyval-operator-controller-manager | grep <cluster>`.
- [ ] Run `redis-cli -h <cluster>-master.<ns>.svc -p 6379 info replication` to confirm replicas are connected and `master_link_status=up`.
- [ ] If no replicas are fully synced, pick the freshest PVC via annotation `keyval.ivelok.io/pvc-repl-offset` and annotate pod `kubectl -n <ns> annotate pod <pod> keyval.ivelok.io/force-master=true --overwrite`.
- [ ] Trigger new failover by deleting the stuck master pod: `kubectl -n <ns> delete pod <master-pod>`; Sentinel will promote the forced target.
- [ ] If Sentinels lost state, execute `kubectl -n <ns> exec <sentinel-pod> -- redis-cli SENTINEL RESET <cluster>` sequentially (respect `ResetSentinelDone` events).

## Verification
- [ ] New event `FailoverCompleted` without errors followed by `NewMaster master=<pod>`.
- [ ] `kubectl -n <ns> get keyvalcluster/<name> -o jsonpath='{.status.masterPod}'` matches the promoted pod.
- [ ] `redis-cli -h <cluster>-master.<ns>.svc info replication` shows replicas with `master_link_status=up` and low lag.

## Escalation
If failover still fails after forcing a master or Sentinel reset, escalate immediately (Sev1) to the database reliability engineer and create an incident bridge. Provide PVC freshness metrics and operator logs.

## Related Dashboards
- [ ] Grafana: *KeyVal Operator / Failover* (`keyval_failover_triggered_total`, `keyval_failover_completed_total`).
- [ ] Grafana: *Redis Replication Lag*.

