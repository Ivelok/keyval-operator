# Summary
<!-- What changed? Why? Link issues/docs if relevant. -->

## Verification
<!-- How did you validate this change? Paste commands + key output, or link CI run. -->

# Definition of Done (DoD)

## Required
- [ ] Formatting: `make fmt`
- [ ] Unit tests: `go test -race ./...`
- [ ] Import-layer rules / no cycles: `make check-imports`
- [ ] CI is green: `.github/workflows/tests.yaml` (`Test Matrix`)

## Conditional (run when relevant)
- [ ] Static analysis: `make lint` (if `staticcheck` is available)
- [ ] Helm chart changes: `make helm-lint helm-template`
- [ ] Runbooks/docs structure changes: `make runbook-check` (when editing `docs/runbook/**`)
- [ ] API/CRD changes: `make manifests`, and keep CRDs in sync (`config/crd/bases/*.yaml`, `charts/keyval-operator/crds/*.yaml`, and `docs/crd/keyvalcluster.md` as needed)
- [ ] Operator behavior changes: `make e2e` and/or `make chaos` (especially reconcile flow, updates, bootstrap/failover, eviction/PDB)
- [ ] Flags/env/defaults changed: update docs + chart defaults (`README.md`, `docs/**`, `charts/**`, and `.env*`/values where applicable)
