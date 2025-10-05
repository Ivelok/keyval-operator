#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNBOOK_DIR="$ROOT_DIR/docs/runbook"

if ! [ -d "$RUNBOOK_DIR" ]; then
  echo "runbook directory $RUNBOOK_DIR not found" >&2
  exit 1
fi

status=0
required_headings=("## Overview" "## Detection" "## Mitigation" "## Verification")

while IFS= read -r -d '' file; do
  if [ "$(basename "$file")" = "README.md" ]; then
    continue
  fi
  missing=()
  for heading in "${required_headings[@]}"; do
    if ! grep -q "^${heading}" "$file"; then
      missing+=("$heading")
    fi
  done
  if [ ${#missing[@]} -gt 0 ]; then
    echo "[runbook-check] $file missing headings: ${missing[*]}" >&2
    status=1
  fi
  if ! grep -q '^## Runbook Metadata' "$file"; then
    echo "[runbook-check] $file missing '## Runbook Metadata' section" >&2
    status=1
  fi
  if ! grep -q '^## Escalation' "$file"; then
    echo "[runbook-check] $file missing '## Escalation' section" >&2
    status=1
  fi
  if ! grep -q '^## Related Dashboards' "$file"; then
    echo "[runbook-check] $file missing '## Related Dashboards' section" >&2
    status=1
  fi
  if ! grep -Eq '^[[:space:]]*-[[:space:]]*\[[[:space:]]*\]' "$file"; then
    echo "[runbook-check] $file missing checklist items (use '- [ ]' markdown list)" >&2
    status=1
  fi
  if ! grep -q '^Severity:' "$file"; then
    echo "[runbook-check] $file missing severity metadata (format 'Severity: <value>')" >&2
    status=1
  fi
  if ! grep -q '^Time to Mitigate:' "$file"; then
    echo "[runbook-check] $file missing 'Time to Mitigate' metadata" >&2
    status=1
  fi
  if ! grep -q '^Responsible:' "$file"; then
    echo "[runbook-check] $file missing 'Responsible' metadata" >&2
    status=1
  fi
  if ! tail -n 1 "$file" | grep -q '^$'; then
    echo "[runbook-check] $file should end with a newline" >&2
    status=1
  fi
  # basic lint: ensure headings are sequential (no skipping levels)
  last_level=1
  while IFS= read -r line; do
    if [[ $line =~ ^\#\#+ ]]; then
      level=${#BASH_REMATCH[0]}
      if (( level - last_level > 1 )); then
        echo "[runbook-check] $file heading jumps from level $last_level to $level" >&2
        status=1
      fi
      last_level=$level
    fi
  done <"$file"
done < <(find "$RUNBOOK_DIR" -maxdepth 1 -type f -name '*.md' -print0 | sort -z)

exit $status
