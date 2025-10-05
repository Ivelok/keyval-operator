#!/usr/bin/env bash

set -euo pipefail

namespace="keyval-e2e"
cluster=""
interval=5
samples=12
output=""

usage() {
  cat <<'EOF'
Usage: redis-qps.sh --cluster <name> [options]

Options:
  --namespace <ns>   Namespace for the KeyValCluster (default: keyval-e2e)
  --cluster <name>   Name of the KeyValCluster to sample (required)
  --interval <sec>   Sampling interval in seconds (default: 5)
  --samples <n>      Number of samples to collect (default: 12)
  --output <path>    File to write CSV output (default: stdout)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)
      namespace="$2"
      shift 2
      ;;
    --cluster)
      cluster="$2"
      shift 2
      ;;
    --interval)
      interval="$2"
      shift 2
      ;;
    --samples)
      samples="$2"
      shift 2
      ;;
    --output)
      output="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$cluster" ]]; then
  echo "--cluster is required" >&2
  usage
  exit 1
fi

if ! [[ "$interval" =~ ^[0-9]+$ ]] || ! [[ "$samples" =~ ^[0-9]+$ ]]; then
  echo "--interval and --samples must be positive integers" >&2
  exit 1
fi

if [[ -n "$output" ]]; then
  mkdir -p "$(dirname "$output")"
  exec >"$output"
fi

redis_port=$(kubectl get svc -n "$namespace" "${cluster}-master" -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || true)
sentinel_port=$(kubectl get svc -n "$namespace" "${cluster}-sentinel" -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || true)

if [[ -z "$redis_port" ]]; then
  redis_port=6379
fi
if [[ -z "$sentinel_port" ]]; then
  sentinel_port=26379
fi

echo "timestamp,pod,role,port,instantaneous_ops_per_sec"

for ((i=0; i<samples; i++)); do
  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  pods="$(kubectl get pods -n "$namespace" -l "keyvalcluster=$cluster" -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.role}{"\n"}{end}' 2>/dev/null || true)"

  while IFS=$'\t' read -r pod role; do
    [[ -z "$pod" ]] && continue

    port=$redis_port
    container="redis"
    if [[ "$role" == "sentinel" ]]; then
      port=$sentinel_port
      container="sentinel"
    fi

    ops=$(kubectl exec -n "$namespace" "$pod" -c "$container" -- redis-cli -p "$port" info stats 2>/dev/null || true)
    ops=$(awk -F: '/instantaneous_ops_per_sec/{gsub("\r", "", $2); print $2}' <<< "$ops")

    if [[ -z "$ops" ]]; then
      ops="NA"
    fi

    echo "$timestamp,$pod,$role,$port,$ops"
  done <<< "$pods"

  if (( i < samples - 1 )); then
    sleep "$interval"
  fi
done
