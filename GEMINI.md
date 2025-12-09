# GEMINI.md

Guidelines for Gemini agent working in this repository. This effort rebuilds the Kubernetes operator that manages Redis/Valkey clusters (Standalone and Sentinel modes).

- Scope: applies to the entire repository.
- Goal: deliver the operator while keeping work aligned with the internal .codex/task plan.
- Principle: idempotent reconciliation, minimal downtime, safe updates, clear ownership.


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
│   └── internal/             # Domain logic modules
│       ├── finalizer/        # Resource cleanup
│       ├── reconcile/        # Reconcile phases
│       ├── resources/        # SSA builders (StatefulSet, Service, PDB)
│       ├── security/         # Auth/TLS handling
│       ├── ssa/              # Server-Side Apply helpers
│       └── ops/              # Operational logic
│           ├── bootstrap/    # DR/Initial master selection
│           ├── eviction/     # Safe pod eviction
│           ├── importer/     # External sync
│           ├── observability/# Metrics & Events
│           ├── replication/  # Topology & role detection
│           ├── sentinel/     # Failover & Quorum
│           ├── status/       # Status aggregation
│           ├── storage/      # PVC resize/cleanup
│           └── update/       # Rolling updates
├── config/                   # Kustomize manifests (install/deploy)
├── docs/                     # Documentation & Runbooks
├── test/                     # Testing infrastructure
│   ├── suites/               # E2E scenarios
│   └── chaos/                # Chaos testing
└── examples/                 # Example CRs (standalone/sentinel)
```

## Modules & Responsibilities
- `resources`: Build ConfigMap, StatefulSet (OnDelete), Service, PDB using SSA.
- `ops/replication`: Detect roles, ensure topology, handle `REPLICAOF`, manage master address cache.
- `ops/sentinel`: Coordinate failover (`SENTINEL FAILOVER`), validate good slaves, handle `NOQUORUM`.
- `ops/update`: Plan and execute rolling updates (single-pod), check health guards.
- `ops/bootstrap`: Handle DR master selection (force-master/freshness) and external import.
- `ops/status`: Aggregate conditions, roles, and health gate status.
- `ops/observability`: Manage Prometheus metrics and Kubernetes events.
- `security`: Resolve Auth/TLS secrets, build client configurations.

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
  - `make test`  # Runs go test -race ./...
  - `make e2e`   # Runs e2e suite in test/suites
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

## Editing Discipline
- Read, apply a small patch, build, test — repeat.
- Copy exact context (whitespace matters for patch tools).
- Keep commit messages scoped: `feat|fix|refactor|test(component): message`.

## Notes
- Internal task templates live under .codex/tasks for internal use only; refresh the spec there before starting work.
