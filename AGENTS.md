# AGENTS.md

Guidelines for all AI agents working in this repository. This effort rebuilds the Kubernetes operator that manages Redis/Valkey clusters (Standalone and Sentinel modes).

- Scope: applies to the entire repository.
- Goal: deliver the operator while keeping work aligned with the internal .codex/task plan.
- Principle: idempotent reconciliation, minimal downtime, safe updates, clear ownership.

## System Instructions (External Source)
- Authoritative source file: `/Users/ivelok/.codex/system-prompt.md`.
- Agents must read and follow the instructions from that file at session start and before executing multi‑step plans. Do not embed or copy its full contents into this repo.
- Precedence: explicit system/developer/user instructions in the active conversation take priority; otherwise, the external system instructions guide behavior ahead of this AGENTS.md where they differ.
- Refresh policy: if the file changes, re‑read it; if missing or unreadable, proceed with this AGENTS.md and notify maintainers.
- Privacy: treat the file as sensitive; do not paste large excerpts, and never commit it or its content here.
- Optional override: if available, use env var `CODEX_SYSTEM_PROMPT` to point to an alternative path; fallback is the path above.

## Architecture Reference
- Authoritative plan: `docs/architecture.md` (modules, APIs, reconcile flow, events, policies).
- CRD reference: `docs/crd/keyvalcluster.md` and `config/crd/bases/*.yaml`.

## Development Environment
- Run all development and testing against the local Kubernetes cluster provisioned on the workstation.
- You have full cluster access: `kubectl` (get/apply/delete), log inspection, exec, describe, and more.
- Validate operator behavior exclusively in this cluster (deployments, upgrades, incident reproduction).
- Never edit operator-managed resources directly (StatefulSet, Service, PDB, etc.). Manual tweaks fight the controller and corrupt scenarios. Use the CR or other approved inputs to model situations.

## Task Workflow
- Coordinate scope using .codex/tasks (internal tracker) and follow the sequence agreed there.
- Keep scope changes small; if the existing plan no longer fits, update the tracker and get confirmation before expanding.
- Do not introduce functionality outside the approved task set. Discuss adjustments with maintainers before implementing.
- Before editing, read the relevant files, keep diffs small, and run checks immediately (`go fmt`, `goimports`, `staticcheck`, `go test -race ./...`, `make e2e`, `make build/test` when required).
- Before running e2e or scenarios that depend on operator behavior, ensure the freshly built image is deployed and the controller manager restarted (`make redeploy` or equivalently `make docker-build` → `kubectl apply -k config/default` → `kubectl -n keyval-operator-system rollout restart deploy/keyval-operator-controller-manager`).

## Development Flow
1. **Specification** — capture scope in .codex/tasks with goal, inputs, acceptance criteria, and required checks.
2. **Decomposition** — if the scope is large, split it into sub-tasks (`<NN>.<MM>-...`) so every stage is tracked.
3. **Pre-review** — reread the specification, identify risks, raise questions/hypotheses, and document them in the task file.
4. **Implementation** — follow the plan, land small patches, validate code frequently, and redeploy to the local cluster when behavior changes.
5. **Self-review** — audit your changes: list potential issues, confirm checks passed, and polish the code as needed.

Detailed instructions for working with the e2e/chaos suites live in `test/E2E-AGENTS.md`.

## Target Layout
Use this structure as you scaffold and refactor:

```
├── api/                      # CRD types and webhooks
│   └── v1alpha1/             # KeyValCluster, +kubebuilder annotations
├── controllers/              # Reconciler and helpers
│   ├── controller.go         # Main controller wiring
│   ├── statefulset.go        # Desired StatefulSet (OnDelete)
│   ├── service.go            # Headless + master Services
│   ├── configmap.go          # redis.conf / sentinel.conf generation
│   ├── replication.go        # Role detection and replication ensure
│   ├── sentinel.go           # Failover coordination (Sentinel mode)
│   └── update.go             # Rolling restarts & update orchestration
├── config/                   # Kustomize manifests (install/deploy)
└── examples/                 # Example CRs (standalone/sentinel)
```

## Modules & Responsibilities
- ConfigMap Generator: build `redis.conf` and `sentinel.conf`, annotate PodTemplate with a config hash.
- StatefulSet Builder: OnDelete policy, probes, volumes, ports, sidecar sentinel (Sentinel mode).
- Service Manager: headless service for discovery, `<cr>-master` ClusterIP with selector `role=master`.
- Role Labeler: maintain `role=master|replica` labels on Pods; avoid dual masters.
- Replication Ensurer: detect the master, align replicas (`REPLICAOF`), handle `SENTINEL RESET`.
- Failover Coordinator: controlled `SENTINEL FAILOVER`, confirm the new master, update labels/status.
- Update Orchestrator: image/config drift detection, single-pod rolling updates, safety checks.
- Status Updater: conditions, current master, role inventory, readiness aggregation.

## Reconcile Flow (Summary)
- Ensure Services (headless, master, sentinel when `mode=Sentinel`).
- Build ConfigMaps; compute `config-hash`; annotate the PodTemplate.
- Ensure the StatefulSet with `OnDelete` and a deterministic spec.
- Detect roles (master/replica), then sync Pod role labels (single-master invariant).
- Ensure replication (`REPLICAOF`) to the detected master.
- Plan and execute rolling updates one Pod at a time when drift is detected.
- In Sentinel mode, coordinate failover when required and validate resulting roles.
- Update `.status` (masterPod, replicas, readyReplicas, roles, conditions).

## Events and Triggers (Operational)
- CR changes: recompute desired state; re-run full reconcile.
- Pod Ready/Deleted/Restarted: re-detect roles, re-label, ensure replication, continue/hold rolling update.
- StatefulSet drift: enforce `OnDelete`, selectors, labels; schedule rolling update if needed.
- ConfigMap change: config-hash drift → rolling update planning.
- Service change: restore selectors/ports; ensure `<cr>-master` selects exactly one Pod.
- Sentinel role switches: reconcile labels, ensure replication, update status.

## Reconcile Invariants
- Exactly one master; every other pod is a replica.
- Operations are idempotent; safe to rerun on any event.
- StatefulSet uses `OnDelete`; never delete more than one Pod at a time.
- Sentinel mode only when `spec.mode=Sentinel` and `sentinelCount` is odd and ≥3.
- The master Service must always select exactly one Pod.

## Apply & Ownership Policy
- Use server-side apply with a stable field manager (e.g., `keyval-operator`).
- Preserve user-managed labels/annotations; own only operator-scoped fields.
- Set OwnerReferences to `KeyValCluster` for garbage collection.
- Update status using `Status().Patch` only when there is a diff.

## Resource Naming & Labels
- Labels: `app=<cr>-redis`, `keyvalcluster=<cr>`, `role=master|replica`.
- Headless Service: `<cr>-headless` (selector `app=<cr>-redis`).
- Master Service: `<cr>-master` (ClusterIP, selector `role=master`).
- Ports: Redis `6379`, Sentinel `26379` (sidecar in Sentinel mode).

## Development Standards
- Go 1.24+; accept `context.Context` first; wrap errors with `%w`; use `log/slog` for structured logging.
- No panics (except main/tests). Validate external inputs. Keep functions small and testable.
- Use table-driven tests; run with `-race`; cover error paths; avoid flakes with timeouts.
- Maintain least-privilege RBAC; set OwnerReferences and finalizers correctly.
- Prefer server-side apply; preserve user-managed fields.

## Commands
- Tests require envtest binaries. Place them under `bin/k8s/<k8sVer>-<os>-<arch>/` and set:
  - `export KUBEBUILDER_ASSETS="$(PWD)/bin/k8s/1.33.0-$(go env GOOS)-$(go env GOARCH)"`
- Build and test (after scaffolding):
  - `make build`
  - `make test`
  - `go test -race -ldflags="-extldflags=-Wl,-w" ./....`
  - `make e2e`
- Quality checks:
  - `go fmt ./... && go run golang.org/x/tools/cmd/goimports@latest -w .`
  - `staticcheck ./...`

## Local Dev: Image Updates & Restarts
- Re-tagging a local image (e.g., `controller:latest`) without changing the pod template will not trigger a rollout. Kubernetes only restarts when the template hash changes.
- Preferred ways to pick up a rebuilt local image:
  - `kubectl -n keyval-operator-system rollout restart deploy/keyval-operator-controller-manager`.
  - Bump the image tag (e.g., `controller:dev-<timestamp>`) and update the Deployment template.
  - Patch a dummy annotation on the Pod template: `kubectl -n keyval-operator-system patch deploy/keyval-operator-controller-manager -p '{"spec":{"template":{"metadata":{"annotations":{"redeployAt":"$(date +%s)"}}}}}'`.
- Shortcuts: `make restart` (rollout restart) or `make redeploy` (build + apply manifests + restart).

## Task 03 Acceptance
- `docs/architecture.md` exists and matches the CRD.
- Reconcile flow, modules/APIs, events, and policies are documented.
- Invariants are noted: `OnDelete`, single-master label, one-pod master service selection, Sentinel quorum rules.

## Editing Discipline
- Read, apply a small patch, build, test — repeat.
- Copy exact context (whitespace matters for patch tools).
- Keep commit messages scoped: `feat|fix|refactor|test(component): message`.

## Notes
- Internal task templates live under .codex/tasks for internal use only; refresh the spec there before starting work.
