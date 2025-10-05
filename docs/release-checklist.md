# Public Release Checklist

Use this checklist to prepare the KeyVal Operator repository for a public release. Each item should be confirmed before tagging and publishing artifacts.

## 1. Code & Tests
- [ ] All unit tests pass: `go test ./...`
- [ ] Static analysis clean: `staticcheck ./...`
- [ ] End-to-end suite green against local cluster: `make e2e`
- [ ] Chaos scenarios (optional for patch releases) validated: `make chaos`

## 2. Documentation
- [ ] README, install guide, and architecture docs are current and English-only
- [ ] Runbooks updated with any new operational behaviors
- [ ] CRD reference regenerated if schema changed: `make generate && make manifests`
- [ ] Release notes drafted for GitHub Release page

## 3. Packaging
- [ ] Controller image built and pushed to release registry (`make docker-build IMG=<repo>:<tag>`)
- [ ] Helm chart packaged and signed (if applicable): `make helm-package CHART_VERSION=<version>`
- [ ] Chart and image digests recorded in release notes
- [ ] Sample `KeyValCluster` manifests validated on Kubernetes v1.26+ and v1.30+

## 4. Security & Compliance
- [ ] Dependency scan (`make govulncheck` or `govulncheck ./...`) shows no blocking issues
- [ ] (Optional) Generate and publish SBOM alongside the container image
- [ ] Cosign signatures applied to container image and Helm chart
- [ ] CVE triage status documented in release notes

## 5. Observability & Operations
- [ ] Grafana dashboards reviewed and updated (`grafana/keyval-operator-overview.json`)
- [ ] Alert recommendations validated with Prometheus rules
- [ ] SLA report regenerated after latest e2e/chaos runs: `make sla-report`

## 6. Release Process
- [ ] Version bumped in `PROJECT`, Helm chart, and manifests
- [ ] Git tag created and pushed (`git tag -s v<version>`)
- [ ] GitHub release drafted with artifacts (images, charts, SBOM, release notes)
- [ ] Internal tracker updated in .codex/tasks

Retain this checklist with the release artifacts to demonstrate readiness and ease future audits.
