package resources

import "strings"

const (
	BootstrapScriptKey  = "bootstrap.sh"
	BootstrapScriptPath = "/conf/" + BootstrapScriptKey

	redisBootstrapScriptRaw = `set -eu

log() {
  echo "[kv-bootstrap-role] $*"
}

role_dir="/runtime-conf"
out="${role_dir}/role.conf"
tmp="${out}.tmp"

config_src="/conf/redis.conf"
config_dst="${role_dir}/redis.conf"
config_tmp="${config_dst}.tmp"

mkdir -p "$role_dir"
: > "$tmp"

if [ ! -f "$config_dst" ]; then
  if [ -f "$config_src" ]; then
    cp "$config_src" "$config_tmp"
    if ! grep -q "include /runtime-conf/role.conf" "$config_tmp" 2>/dev/null; then
      echo "include /runtime-conf/role.conf" >> "$config_tmp"
    fi
    chmod 0644 "$config_tmp"
    mv "$config_tmp" "$config_dst"
    log "seeded redis config at $config_dst"
  else
    log "warning: redis config source $config_src not found"
    : > "$config_dst"
  fi
fi

force=$(printf '%s' "${FORCE_MASTER:-}" | tr '[:upper:]' '[:lower:]')
cluster_mode="${CLUSTER_MODE:-}"
redis_port="${REDIS_PORT:-6379}"
sentinel_svc="${SENTINEL_SVC:-}"
sentinel_port="${SENTINEL_PORT:-26379}"
monitor="${MONITOR_NAME:-}"
seed_host="${SEED_HOST:-}"
master_service="${MASTER_SERVICE_HOST:-}"
pod_name="${POD_NAME:-}"
pod_ns="${POD_NAMESPACE:-}"
headless="${HEADLESS_SERVICE:-}"

tls_enabled=$(printf '%s' "${TLS_ENABLED:-false}" | tr '[:upper:]' '[:lower:]')

redis_cli() {
  orig_args="$*"
  set -- redis-cli
  if [ "$tls_enabled" = "true" ]; then
    set -- "$@" --tls
    if [ -n "${TLS_CA_FILE:-}" ]; then
      set -- "$@" --cacert "$TLS_CA_FILE"
    fi
    if [ -n "${TLS_CERT_FILE:-}" ]; then
      set -- "$@" --cert "$TLS_CERT_FILE"
    fi
    if [ -n "${TLS_KEY_FILE:-}" ]; then
      set -- "$@" --key "$TLS_KEY_FILE"
    fi
  fi
  if [ -n "${MASTER_USER:-}" ]; then
    set -- "$@" --user "$MASTER_USER"
  fi
  if [ -n "${MASTER_AUTH:-}" ]; then
    set -- "$@" -a "$MASTER_AUTH"
  fi
  if [ -n "$orig_args" ]; then
    set -- "$@" $orig_args
  fi
  command "$@"
}

is_seed="false"
case "${IS_SEED:-}" in
  [Tt][Rr][Uu][Ee]) is_seed="true" ;;
esac
if [ "$is_seed" = "false" ] && [ -n "$pod_name" ]; then
  case "$pod_name" in
    *-0) is_seed="true" ;;
  esac
fi

if [ -z "$seed_host" ] && [ -n "$pod_name" ] && [ -n "$headless" ] && [ -n "$pod_ns" ]; then
  base="${pod_name%-*}"
  seed_host="${base}-0.${headless}.${pod_ns}.svc"
fi

my_dns=""
if [ -n "$pod_name" ] && [ -n "$headless" ] && [ -n "$pod_ns" ]; then
  my_dns="${pod_name}.${headless}.${pod_ns}.svc"
fi
my_ip="$(hostname -i | awk '{print $1}')"

write_master() {
  printf "# master\n" > "$tmp"
  if [ "$tls_enabled" = "true" ] && [ -n "$my_dns" ]; then
    printf "replica-announce-ip %s\n" "$my_dns" >> "$tmp"
    printf "replica-announce-port %s\n" "$redis_port" >> "$tmp"
  fi
  mv "$tmp" "$out"
  log "configured as master"
}

write_replica() {
  host="$1"
  port="${2:-$redis_port}"
  {
    printf "replicaof %s %s\n" "$host" "$port"
    if [ -n "${MASTER_AUTH:-}" ]; then
      printf "masterauth %s\n" "$MASTER_AUTH"
    fi
    if [ -n "${MASTER_USER:-}" ]; then
      printf "masteruser %s\n" "$MASTER_USER"
    fi
    if [ "$tls_enabled" = "true" ]; then
      printf "tls-replication yes\n"
    fi
    if [ "$tls_enabled" = "true" ] && [ -n "$my_dns" ]; then
      printf "replica-announce-ip %s\n" "$my_dns"
      printf "replica-announce-port %s\n" "$redis_port"
    fi
    printf "replica-read-only yes\n"
  } > "$tmp"
  mv "$tmp" "$out"
  log "configured as replica of ${host}:${port}"
}

if [ "$force" = "true" ]; then
  log "force-master annotation detected"
  write_master
  exit 0
fi

master_host=""
master_port=""

# Optimize bootstrap: for pod-0 in Sentinel mode, check if other pods exist first
if [ "$cluster_mode" = "Sentinel" ] && [ -n "$sentinel_svc" ]; then
  # For pod-0, quickly check if this is likely a full bootstrap scenario
  if [ "$is_seed" = "true" ]; then
    # Check if pod-1 exists (indicates cluster is running)
    if [ -n "$headless" ] && [ -n "$pod_ns" ]; then
      base="${pod_name%-*}"
      pod1_host="${base}-1.${headless}.${pod_ns}.svc"
      if ! getent ahostsv4 "$pod1_host" >/dev/null 2>&1; then
        log "pod-1 not found, assuming bootstrap scenario"
        write_master
        exit 0
      fi
    fi
  fi
  
  # Try to get master from Sentinel with reduced retries for faster bootstrap
  max_retries=3
  if [ "$is_seed" = "true" ]; then
    max_retries=1
  fi
  
  for retry in $(seq 1 $max_retries); do
    if info=$(redis_cli --raw -h "$sentinel_svc" -p "$sentinel_port" SENTINEL get-master-addr-by-name "$monitor" 2>/dev/null); then
      master_host=$(printf '%s\n' "$info" | sed -n '1p' | tr -d '\r')
      master_port=$(printf '%s\n' "$info" | sed -n '2p' | tr -d '\r')
      break
    fi
    [ "$retry" -lt "$max_retries" ] && sleep 2
  done
fi

match_self() {
  target="$1"
  if [ -z "$target" ]; then
    return 1
  fi
  if [ "$target" = "$my_ip" ] || [ "$target" = "$my_dns" ] || [ "$target" = "$pod_name" ] || [ "$target" = "${pod_name}.${pod_ns}.svc" ]; then
    return 0
  fi
  if resolved=$(getent ahostsv4 "$target" | awk 'NR==1 {print $1}' 2>/dev/null); then
    if [ -n "$resolved" ] && [ "$resolved" = "$my_ip" ]; then
      return 0
    fi
  fi
  if [ -n "$master_service" ] && [ "$target" = "$master_service" ]; then
    return 0
  fi
  return 1
}

if [ -n "$master_host" ]; then
  if match_self "$master_host"; then
    log "sentinel reports this pod as master ($master_host)"
    write_master
    exit 0
  fi
  port="${master_port:-$redis_port}"
  log "sentinel reports master ${master_host}:${port}"
  write_replica "$master_host" "$port"
  exit 0
fi

if [ "$is_seed" = "true" ]; then
  log "acting as seed master"
  write_master
  exit 0
fi

if [ -z "$seed_host" ]; then
  log "seed host unknown, defaulting to master"
  write_master
  exit 0
fi

port="$redis_port"
attempt=1
seed_ready="false"
for backoff in 1 2 3 4 5; do
  if redis_cli --raw -h "$seed_host" -p "$port" PING >/dev/null 2>&1; then
    log "seed ${seed_host}:${port} reachable on attempt ${attempt}"
    seed_ready="true"
    break
  fi
  rc=$?
  sleep_seconds=$((backoff * backoff))
  log "seed ${seed_host}:${port} unreachable on attempt ${attempt} (rc=${rc}); sleeping ${sleep_seconds}s before retry"
  sleep "${sleep_seconds}"
  attempt=$((attempt + 1))
done

if [ "$seed_ready" != "true" ]; then
  log "seed ${seed_host}:${port} still unreachable after $((attempt - 1)) attempts; proceeding as replica"
fi

write_replica "$seed_host" "$port"
exit 0`
)

func redisBootstrapScript() string {
	return strings.TrimSpace(redisBootstrapScriptRaw) + "\n"
}
