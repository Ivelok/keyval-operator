# Runbook: Sentinel Quorum Lost

## Runbook Metadata
Severity: Sev2
Time to Mitigate: 20m
Responsible: SRE On-call

## Overview
Loss of Sentinel quorum prevents automatic failover and pauses disruptive operations. This runbook details how to restore quorum and service continuity.

## Detection
- Condition `SentinelQuorum=False` on `KeyValCluster` status with reason `SentinelQuorumCheckFailed` or `SentinelQuorumLost`; field `status.rolesSource=probe` signals a fallback to direct probing.
- Events `SentinelQuorumLost`, `ReplicationHeuristicFallback`, and `ScaleOperationBlocked` (`reason=SentinelQuorum`).
- Metric `keyval_operator_sentinel_quorum_healthy=0` and `keyval_operator_sentinel_noquorum_reports>0`.

## Mitigation
- [ ] List Sentinel pods and check readiness: `kubectl -n <ns> get pods -l app=<cluster>-sentinel`.
- [ ] Restart failed Sentinels: `kubectl -n <ns> delete pod <cluster>-sentinel-<n>` one at a time.
- [ ] Validate network access from Sentinel to Redis master (port 26379/6379) using `kubectl -n <ns> exec <pod> -- redis-cli -p 26379 PING`.
- [ ] Ensure authentication secrets exist and match operator config (compare Secret referenced in `.spec.security.auth`).
- [ ] If persistent storage contains stale state, remove `sentinel-state` PVC or run `redis-cli SENTINEL RESET <cluster>` sequentially with a 5s delay between pods.
- [ ] For permanent loss (fewer than quorum replicas available), scale Sentinels: `kubectl -n <ns> patch keyvalcluster/<name> --type merge -p '{"spec":{"sentinelCount":5}}'` after ensuring resources.

## Verification
- [ ] Status condition `SentinelQuorum=True` and event `SentinelQuorumRestored` emitted.
- [ ] `kubectl -n <ns> exec <cluster>-sentinel-0 -- redis-cli SENTINEL ckquorum <cluster>` returns `OK`.
- [ ] Rolling operations resume (`keyval_operator_scale_in_progress` returns to 0, pending blocks disappear).

## Escalation
If quorum cannot be restored within 20 minutes, escalate to database reliability engineer; include outputs of `SENTINEL SENTINELS` and network diagnostics.

## Related Dashboards
- [ ] Grafana: *KeyVal Sentinels* (`keyval_operator_sentinel_quorum_healthy`, `keyval_operator_sentinel_ready_members`).
- [ ] Grafana: *KeyVal Operator / Disruptions*.

