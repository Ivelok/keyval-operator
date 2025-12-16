#!/usr/bin/env bash
set -euo pipefail

SA_NAME=${1:-keyval-operator-controller-manager}
SA_NAMESPACE=${2:-keyval-operator-system}
KUBECTL=${KUBECTL:-kubectl}

if ! command -v "$KUBECTL" >/dev/null 2>&1; then
  echo "kubectl not found" >&2
  exit 1
fi

checks=$(cat <<'RULES'
core pods get,list,watch,patch,delete
core services get,list,watch,create,patch,delete
core configmaps get,list,watch,create,patch
core events create,patch,update
core secrets get,list,watch
core persistentvolumeclaims get,list,watch,patch,update,delete
core pods/eviction create
apps statefulsets get,list,watch,create,update,patch,delete
policy poddisruptionbudgets get,list,watch,create,patch,delete
coordination.k8s.io leases get,list,watch,create,update,patch
keyval.ivelok.io keyvalclusters get,list,watch,patch,update
keyval.ivelok.io keyvalclusters/status get,patch,update
keyval.ivelok.io keyvalclusters/finalizers get,patch,update
RULES
)

failed=0
while read -r group resource verbs; do
  [[ -z "$resource" ]] && continue
  IFS=',' read -ra verb_list <<<"$verbs"
  subresource=""
  base_resource="$resource"
  if [[ "$resource" == */* ]]; then
    base_resource="${resource%%/*}"
    subresource="${resource#*/}"
  fi
  for verb in "${verb_list[@]}"; do
    args=("auth" "can-i" "$verb" "$base_resource" "--as=system:serviceaccount:${SA_NAMESPACE}:${SA_NAME}")
    if [[ "$group" != "core" ]]; then
      args+=("--api-group=$group")
    fi
    if [[ -n "$subresource" ]]; then
      args+=("--subresource=$subresource")
    fi
    if ! $KUBECTL "${args[@]}" >/dev/null 2>&1; then
      echo "FORBIDDEN: $verb $resource (group=$group)" >&2
      failed=1
    else
      echo "ALLOWED:   $verb $resource (group=$group)"
    fi
  done
done <<<"$checks"

exit $failed
