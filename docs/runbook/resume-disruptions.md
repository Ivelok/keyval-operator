# Runbook: Resume Disruptions After Safety Pause

## Runbook Metadata
Severity: Sev3
Time to Mitigate: 10m
Responsible: SRE On-call

## Overview
The KeyVal operator pauses voluntary disruptions when safety conditions fail (`allowDisruptions=false`). This runbook details how to safely resume disruptions once the underlying issue is resolved.

## Detection
- `kubectl -n <ns> get keyvalcluster/<name>` shows `status.healthGate.allowDisruptions=false`.
- Annotation `keyval.ivelok.io/update-blocked` set on the KeyValCluster resource.
- Events `RollingStepBlocked` with reasons `DisruptionsPaused`, `Bootstrap`, or `SentinelQuorum`.

## Mitigation
- [ ] Identify root cause by inspecting status conditions (`Available`, `SentinelQuorum`, `ReplicationHealthy`, `BootstrapInProgress`).
- [ ] Resolve blocking runbook (e.g., quorum loss, replica desync, failover stuck) before resuming.
- [ ] Clear the blocking annotation: `kubectl -n <ns> annotate keyvalcluster/<name> keyval.ivelok.io/update-blocked-`.
- [ ] If disruptions were paused via manual configuration (e.g., GitOps lock), remove the override in source control.
- [ ] Trigger reconciliation by touching the resource: `kubectl -n <ns> annotate keyvalcluster/<name> keyval.ivelok.io/scale-phase- --overwrite` (no-op clears stale value).

## Verification
- [ ] Event `RollingStepResumed` appears with previous `reason`.
- [ ] `kubectl -n <ns> get keyvalcluster/<name> -o jsonpath='{.status.healthGate.allowDisruptions}'` returns `true`.
- [ ] Pending updates or scale operations continue (observe `ScaleOperationStarted` followed by `ScaleOperationCompleted`).

## Escalation
Escalate to platform engineering if `allowDisruptions` remains `false` despite cleared conditions or if the annotation reappears automatically; provide recent events and controller logs.

## Related Dashboards
- [ ] Grafana: *KeyVal Operator / Disruptions* (`keyval_operator_update_in_progress`, `keyval_disruptions_blocked_total`).

