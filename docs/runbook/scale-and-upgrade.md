# Runbook: Scaling and Upgrading KeyValCluster

## Runbook Metadata
Severity: Sev2
Time to Mitigate: 30m
Responsible: SRE On-call

## Overview
This runbook guides you through safe scaling (up/down) and rolling upgrades of KeyValCluster instances managed by the KeyVal operator. It covers event signals, guardrails, and recovery actions required to maintain write availability.

## Detection
- Scaling initiated via `kubectl patch` or GitOps manifests produces events `ScaleOperationStarted` with `direction=up|down`.
- Rolling upgrades surface through `PodEvicted` events with reasons `config-hash`, `image:redis`, `image:sentinel`, and gauge `keyval_operator_update_in_progress=1`. From that point the operator must finish within **≤5 minutes**; otherwise the rollout is considered stuck.
- Blocks appear as `ScaleOperationBlocked` or `RollingStepBlocked` events, often coupled with `allowDisruptions=false` in status.

## Mitigation
- [ ] Confirm cluster readiness: `kubectl -n <ns> get keyvalcluster/<name>` should report `SentinelQuorum=True`, `ReplicationHealthy=True`.
- [ ] Ensure at least one healthy replica by running `kubectl -n <ns> get pods -l keyvalcluster=<name> -o custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[0].ready`.
- [ ] Trigger scale via spec patch (example to 5 replicas):
      `kubectl -n <ns> patch keyvalcluster/<name> --type merge -p '{"spec":{"redisReplicas":5}}'`
- [ ] For scale-down ensure failover ready: if event `NoGoodSlave` repeats, pause and validate replica sync via `redis-cli INFO replication`.
- [ ] Apply rolling update by patching image/config; monitor `StartFailover`/`FailoverCompleted` events before the master eviction step. If `keyval_operator_update_in_progress=1` stays true for more than 5 minutes or new `PodEvicted` events stop appearing, declare an incident and follow the manual diagnostics below.
- [ ] If guard blocking persists, reference supporting runbooks: quorum loss, replica desync, or disruptions paused.

## Verification
- [ ] Event `ScaleOperationCompleted direction=<expected>` occurs with the intended `target` value.
- [ ] `keyval_operator_scale_in_progress` and `keyval_operator_update_in_progress` gauges drop to `0` within 5 minutes of the operation start.
- [ ] Services `<name>-master` and `<name>-sentinel` resolve to a single healthy endpoint each.
- [ ] `kubectl -n <ns> get pods -l keyvalcluster=<name>` matches desired replica counts (Redis + Sentinel) and all pods are Ready.

## Escalation
Escalate to platform engineering (`#keyval-operator` channel) if scaling or upgrade remains blocked for >30 minutes despite cleared health conditions. Provide event extracts and operator logs (`kubectl -n keyval-operator-system logs deploy/keyval-operator-controller-manager`).

## Related Dashboards
- [ ] Grafana: *KeyVal Operator / Lifecycle* (metrics `keyval_operator_scale_in_progress`, `keyval_scale_operations_total`, `keyval_operator_update_in_progress` with an alert >5 minutes).
- [ ] Grafana: *KeyVal Operator / Failover* (events + lag gauges).

## Notes
- When `KEEP_ARTIFACTS=1` is set, e2e tests save `artifacts/<cluster>-rolling-update.json` with a summary (duration, 5-minute threshold, list of pending/notReady pods). Use it for incident analysis and SLA tracking.

