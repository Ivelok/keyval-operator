# Runbook: KeyVal Operator Release Process

## Runbook Metadata
Severity: Sev3
Time to Mitigate: 40m
Responsible: Release Manager

## Overview
This playbook describes the end-to-end process for cutting a new KeyVal operator release, including validation gates, artifact publication, and rollback guidance.

## Detection
- Scheduled release window or need to deliver new features/patches.
- GitHub milestone completed and CHANGELOG draft available.

## Mitigation
- [ ] Freeze main branch (communicate via `#keyval-operator` channel) and ensure CI green on latest commit.
- [ ] Run `make runbook-check`, `go test ./...`, `make helm-lint`, and `make e2e` locally; address failures.
- [ ] Bump version in Git tag (`git tag vX.Y.Z && git push origin vX.Y.Z`) or trigger `Release` workflow manually with `version` input.
- [ ] Monitor `.github/workflows/release.yaml` run; verify steps: image push, cosign sign, helm package upload, changelog generation.
- [ ] Announce availability with Helm/OCI coordinates (`helm install keyval-operator oci://ghcr.io/<org>/keyval-operator/keyval-operator --version X.Y.Z`).

## Verification
- [ ] Release workflow concludes successfully and publishes GitHub Release with changelog.
- [ ] New container image `ghcr.io/<org>/keyval-operator:X.Y.Z` exists and passes `cosign verify`.
- [ ] Helm repo/OCI registry stores chart `keyval-operator-X.Y.Z.tgz`.

## Escalation
If release automation fails, page the release manager and revert tag (`git tag -d vX.Y.Z; git push --delete origin vX.Y.Z`). If image has already propagated with regressions, execute rollback runbook: retag previous version (`make release VERSION=<prev>`), redeploy clusters, and inform stakeholders.

## Related Dashboards
- [ ] Grafana: *KeyVal CI/CD* (release workflow success rate).
- [ ] GH Actions dashboard: `https://github.com/ivelok/keyval-operator/actions/workflows/release.yaml`.

