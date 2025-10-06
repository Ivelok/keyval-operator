# Runbook: Replica Desynchronised or Lagging

## Runbook Metadata
Severity: Sev2
Time to Mitigate: 25m
Responsible: SRE On-call

## Overview
Replica lag or desynchronisation degrades failover safety and can stall rolling updates. This runbook covers diagnosis and remediation when replicas report `Lagging` or `Desynced` health states.

## Detection
- Status condition `ReplicationHealthy=False` and pod health `Lagging`/`Desynced` in `.status.roles`.
- Events `ReplicaLagging`, `ReplicaDesynced`, or `ScaleOperationBlocked reason=NoHealthyReplicas`.
- Metrics `keyval_operator_replication_lag_seconds{pod=...}` exceeding configured threshold.

## Mitigation
- [ ] Inspect Redis replication info: `kubectl -n <ns> exec <pod> -- redis-cli INFO replication` to confirm `master_link_status` and offset.
- [ ] Check network latency between replica and master (`mtr`/`kubectl exec -- ping`).
- [ ] For Sentinel mode, ensure Sentinel state is not forcing outdated master; if required, issue `redis-cli SENTINEL RESET <cluster>` sequentially.
- [ ] Re-sync replica by deleting pod (OnDelete strategy keeps PVC): `kubectl -n <ns> delete pod <pod>`.
- [ ] If the replica PVC is corrupt, delete the pod so the operator recreates it from scratch. To promote a different PVC, annotate the corresponding pod with `keyval.ivelok.io/force-master=true` before deletion.
- [ ] For steady high lag due to load, adjust `spec.health.replicationLagSecondsMax` temporarily and schedule capacity increase.

## Verification
- [ ] Events `ReplicaRecovered` fired for affected pods.
- [ ] `kubectl -n <ns> get keyvalcluster/<name> -o jsonpath='{.status.conditions[?(@.type=="ReplicationHealthy")].status}'` returns `True`.
- [ ] Metrics show lag back within threshold (`increase(keyval_operator_replication_lag_seconds[5m])` near zero).

## Escalation
Escalate to database reliability engineer if replica repeatedly desyncs within an hour or if no replica can sustain catch-up, as failover safety is compromised.

## Related Dashboards
- [ ] Grafana: *KeyVal Replication* (`keyval_operator_replication_lag_seconds`).
- [ ] Grafana: *KeyVal Operator / Health Conditions*.

