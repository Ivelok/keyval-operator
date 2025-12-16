#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

require_cmd() {
	local cmd=$1
	if ! command -v "$cmd" >/dev/null 2>&1; then
		echo "[check-imports] ERROR: required command not found: $cmd" >&2
		exit 2
	fi
}

escape_rg_regex() {
	# Escape a string so it can be safely embedded into an rg regex.
	# Note: we intentionally keep this small and dependency-free.
	sed -E 's/[][(){}.*+?^$|\\]/\\&/g'
}

require_cmd go
require_cmd rg

MODULE_PATH="$(go list -m -f '{{.Path}}')"
if [[ -z "$MODULE_PATH" ]]; then
	echo "[check-imports] ERROR: failed to determine module path (go list -m)" >&2
	exit 1
fi

controllers_root_pkg="${MODULE_PATH}/controllers"
controllers_prefix="${MODULE_PATH}/controllers"
internal_prefix="${MODULE_PATH}/controllers/internal"

status=0

fail() {
	echo "[check-imports] $1" >&2
	status=1
}

print_block() {
	# Print a multi-line heredoc without mangling indentation.
	cat >&2
}

# (a) api/** must not import controllers/**.
api_deps="$(go list -deps ./api/...)"
escaped_controllers_prefix="$(printf '%s' "$controllers_prefix" | escape_rg_regex)"
api_offenders="$(printf '%s\n' "$api_deps" | rg -n "^${escaped_controllers_prefix}(/|$)" || true)"
if [[ -n "$api_offenders" ]]; then
	fail "Layer violation: ./api/... depends on controllers (api must not import controllers/**)"
	echo "Offending packages (from go list -deps):" >&2
	echo "$api_offenders" >&2
	print_block <<EOF
Remediation:
	- Move shared types/constants out of controllers/ into api/ or another shared package.
	- Keep reconciliation wiring in controllers/; API types must remain controller-agnostic.
	- Locate direct imports:
		rg -n --glob='*.go' --fixed-strings '${controllers_prefix}' api
EOF
fi

# (b) controllers/internal/** must not import the top-level controllers package.
internal_deps="$(go list -deps ./controllers/internal/...)"
escaped_root_pkg="$(printf '%s' "$controllers_root_pkg" | escape_rg_regex)"
internal_root_offenders="$(printf '%s\n' "$internal_deps" | rg -n "^${escaped_root_pkg}$" || true)"
if [[ -n "$internal_root_offenders" ]]; then
	fail "Layer violation: ./controllers/internal/... depends on ${controllers_root_pkg} (root package)"
	echo "Offending packages (from go list -deps):" >&2
	echo "$internal_root_offenders" >&2
	print_block <<EOF
Allowed leaf controller packages (explicit exceptions):
	- ${MODULE_PATH}/controllers/errors
	- ${MODULE_PATH}/controllers/logging

Remediation:
	- Invert the dependency: move helpers into controllers/internal and call them from controllers/.
	- Or define interfaces next to the consumer to avoid pulling high-level packages into low-level ones.
EOF
fi

# (c) controllers/logging and controllers/errors must not import controllers/internal/**.
logging_deps="$(go list -deps ./controllers/logging)"
escaped_internal_prefix="$(printf '%s' "$internal_prefix" | escape_rg_regex)"
logging_offenders="$(printf '%s\n' "$logging_deps" | rg -n "^${escaped_internal_prefix}(/|$)" || true)"
if [[ -n "$logging_offenders" ]]; then
	fail "Layer violation: ./controllers/logging depends on controllers/internal/**"
	echo "Offending packages (from go list -deps):" >&2
	echo "$logging_offenders" >&2
	print_block <<EOF
Remediation:
	- Keep controllers/logging as a leaf: move required helpers into controllers/logging (or another leaf package).
	- Or pass data/interfaces into logging instead of importing controllers/internal.
EOF
fi

errors_deps="$(go list -deps ./controllers/errors)"
errors_offenders="$(printf '%s\n' "$errors_deps" | rg -n "^${escaped_internal_prefix}(/|$)" || true)"
if [[ -n "$errors_offenders" ]]; then
	fail "Layer violation: ./controllers/errors depends on controllers/internal/**"
	echo "Offending packages (from go list -deps):" >&2
	echo "$errors_offenders" >&2
	print_block <<EOF
Remediation:
	- Keep controllers/errors as a leaf: move required helpers into controllers/errors (or another leaf package).
	- Or pass data/interfaces into errors instead of importing controllers/internal.
EOF
fi

if [[ $status -ne 0 ]]; then
	echo "[check-imports] FAILED" >&2
	exit 1
fi

echo "[check-imports] OK"
