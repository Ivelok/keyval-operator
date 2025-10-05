# Runbook: PDB Blocks KeyValCluster Rolling Operations

## Runbook Metadata
Severity: Sev3
Time to Mitigate: 20m
Responsible: SRE On-call

## Overview
This runbook explains how to restore rolling operations when a PodDisruptionBudget (PDB) prevents the KeyVal operator from evicting pods during scale or upgrade workflows. The goal is to re-open the disruption gate without risking data loss.

## Detection
- `kubectl -n <ns> describe keyvalcluster/<name>` shows events `RollingStepBlocked` with `reason=PDBLimit`.
- Metric `keyval_disruptions_blocked_total{reason="PDBLimit"}` increases.
- `kubectl -n <ns> get pdb <name>-redis` (and `<name>-sentinel` in Sentinel mode) reports `Allowed disruptions: 0` while pods are Ready.

## Mitigation
- [ ] Confirm at least one healthy replica exists:
      `kubectl -n <ns> get pods -l keyvalcluster=<name> --field-selector=status.phase=Running`
- [ ] Verify health gate status:
      `kubectl -n <ns> get keyvalcluster/<name> -o jsonpath='{.status.healthGate.allowDisruptions}'`
- [ ] If gate is `false`, resolve the underlying condition (Sentinel quorum, failover in progress, bootstrap). Use runbooks referenced below.
- [ ] If all conditions are green but PDB remains hard, patch `spec.health.minReplicasForSafety` to a value `< replicas` and revert once rolling completes.
- [ ] Remove stale annotation `keyval.ivelok.io/update-blocked` if present:
      `kubectl -n <ns> annotate keyvalcluster/<name> keyval.ivelok.io/update-blocked-`

## Verification
- [ ] Event `ScaleOperationBlocked` stops firing and new `ScaleOperationStarted` appears.
- [ ] `kubectl -n <ns> get pdb <name>-redis -o jsonpath='{.status.allowedDisruptions}'` returns `>= 1` (or `Sentinel` PDB if applicable).
- [ ] Gauge `keyval_operator_scale_in_progress` or `keyval_operator_update_in_progress` returns to `0` after rollout.

## Escalation
- If PDB remains hard despite healthy state and no manual overrides, escalate to the platform engineering channel (`#keyval-operator`) and page the primary maintainer.
- Provide: namespace, cluster name, recent operator logs (`kubectl -n keyval-operator-system logs deploy/keyval-operator-controller-manager`).

## Related Dashboards
- [ ] Grafana: *KeyVal Operator / Disruptions* (`keyval_disruptions_blocked_total`, `keyval_operator_update_in_progress`).
