# Runbook: Adjust KeyVal Operator Log Verbosity

## Runbook Metadata
Severity: Sev4
Time to Mitigate: 10m
Responsible: SRE / Platform Engineer

## Overview
Use this runbook when the default KeyVal Operator logs do not provide enough detail to explain reconcile decisions or when you need to reduce log volume in production. The controller uses controller-runtime's zap logger and bridges debug statements through `logger.V(1)`.

## Detection
- Operator logs lack lines with `level=debug` / `severity=DEBUG` even though reconciliation continues.
- Runbooks or on-call investigations require insight into decisions logged with `logger.V(1)` (rolling update plan, replication retries, PVC cleanup pauses).
- Alternatively, log ingestion quotas are at risk because the operator emits high-volume debug logs.

## Mitigation
- [ ] **Determine installation method.**
      `kubectl -n keyval-operator-system get deploy keyval-operator-controller-manager -o jsonpath='{.metadata.annotations.helm\.sh/release-name}'`
      (empty output → kustomize install; non-empty → Helm release.)
- [ ] **Kustomize install – enable debug (`--zap-devel=true`).**
      ```bash
      kubectl -n keyval-operator-system patch deploy keyval-operator-controller-manager \
        --type='json' \
        -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--zap-devel=true"}]'
      kubectl -n keyval-operator-system rollout restart deploy/keyval-operator-controller-manager
      ```
      (Replace `true` with `false` to return to JSON info-level logging.)
- [ ] **Helm install – switch `manager.logLevel`.**
      ```bash
      helm upgrade keyval-operator keyval-operator/keyval-operator \
        --reuse-values \
        --namespace keyval-operator-system \
        --set manager.logLevel=debug
      ```
      Use `manager.logLevel=info` to revert to info-only JSON logs.
- [ ] **Optional extra args.** Combine with `manager.extraArgs` (Helm) or overlay patches (kustomize) if you prefer to keep the flag in Git instead of a live patch.

## Verification
- [ ] `kubectl -n keyval-operator-system rollout status deploy/keyval-operator-controller-manager` succeeds.
- [ ] `kubectl -n keyval-operator-system logs deploy/keyval-operator-controller-manager --tail=20` shows lines with `level=debug` (JSON mode) or `DEBUG	` (dev console mode).
- [ ] Re-run the workflow that needed additional visibility. Debug-only messages (`plan empty`, `waiting for replicas to finish syncing`, `waiting for PVC deletion`) now appear.

## Side Effects
- Debug mode increases log throughput (≈2–3×) and uses the human-readable zap console encoder. Plan additional log storage and consider filtering in your collector.
- Info mode (Helm default) removes `logger.V(1)` lines and uses JSON encoding; this reduces log volume but hides the reconcilers' decision traces.

## Related Documentation
- [`docs/observability.md`](../observability.md#logging) — logging overview and signal interpretation.
- [`charts/keyval-operator/values.yaml`](../../charts/keyval-operator/values.yaml) — Helm value `manager.logLevel`.
