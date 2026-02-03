package resources

import (
	"strconv"
	"strings"
)

const (
	LivenessScriptKey   = "liveness.sh"
	LivenessScriptPath  = "/conf/" + LivenessScriptKey
	ReadinessScriptKey  = "readiness.sh"
	ReadinessScriptPath = "/conf/" + ReadinessScriptKey

	redisReadinessDefaultMaxLastIOSeconds = "15"
	redisProbeTimeoutSeconds              = 2
	redisProbeTimeoutToken                = "__REDIS_PROBE_TIMEOUT__"
)

const redisLivenessScriptRaw = `#!/bin/sh
set -eu

host="${REDIS_PROBE_HOST:-127.0.0.1}"
port="${REDIS_PORT:-6379}"
timeout="${REDIS_PROBE_TIMEOUT:-` + redisProbeTimeoutToken + `}"

tls_enabled=$(printf '%s' "${TLS_ENABLED:-false}" | tr '[:upper:]' '[:lower:]')
supports_timeout="0"
if redis-cli --help 2>/dev/null | grep -q " -t <timeout>"; then
  supports_timeout="1"
fi

redis_cli() {
  cmd_args="$*"
  set -- redis-cli -h "$host" -p "$port"
  if [ "$supports_timeout" = "1" ]; then
    set -- "$@" -t "$timeout"
  fi
  if [ "$tls_enabled" = "true" ]; then
    set -- "$@" --tls
    if [ -n "${TLS_CA_FILE:-}" ]; then
      set -- "$@" --cacert "$TLS_CA_FILE"
    fi
    if [ -n "${TLS_CERT_FILE:-}" ] && [ -n "${TLS_KEY_FILE:-}" ]; then
      set -- "$@" --cert "$TLS_CERT_FILE" --key "$TLS_KEY_FILE"
    fi
  fi
  if [ -n "${MASTER_USER:-}" ]; then
    set -- "$@" --user "$MASTER_USER"
  fi
  if [ -n "$cmd_args" ]; then
    set -- "$@" $cmd_args
  fi
  command "$@"
}

redis_cli PING
`

const redisReadinessScriptRaw = `#!/bin/sh
set -eu

host="${REDIS_PROBE_HOST:-127.0.0.1}"
port="${REDIS_PORT:-6379}"
timeout="${REDIS_PROBE_TIMEOUT:-` + redisProbeTimeoutToken + `}"
max_last_io="${REDIS_READINESS_MAX_LAST_IO_SECONDS:-` + redisReadinessDefaultMaxLastIOSeconds + `}"

tls_enabled=$(printf '%s' "${TLS_ENABLED:-false}" | tr '[:upper:]' '[:lower:]')
supports_timeout="0"
if redis-cli --help 2>/dev/null | grep -q " -t <timeout>"; then
  supports_timeout="1"
fi

redis_cli() {
  cmd_args="$*"
  set -- redis-cli -h "$host" -p "$port"
  if [ "$supports_timeout" = "1" ]; then
    set -- "$@" -t "$timeout"
  fi
  if [ "$tls_enabled" = "true" ]; then
    set -- "$@" --tls
    if [ -n "${TLS_CA_FILE:-}" ]; then
      set -- "$@" --cacert "$TLS_CA_FILE"
    fi
    if [ -n "${TLS_CERT_FILE:-}" ] && [ -n "${TLS_KEY_FILE:-}" ]; then
      set -- "$@" --cert "$TLS_CERT_FILE" --key "$TLS_KEY_FILE"
    fi
  fi
  if [ -n "${MASTER_USER:-}" ]; then
    set -- "$@" --user "$MASTER_USER"
  fi
  if [ -n "$cmd_args" ]; then
    set -- "$@" $cmd_args
  fi
  command "$@"
}

info="$(redis_cli --raw INFO replication | tr -d '\r')"
if [ -z "$info" ]; then
  echo "redis readiness: INFO replication returned empty output" >&2
  exit 1
fi

extract() {
  printf '%s\n' "$info" | awk -F: -v key="$1" '$1==key {print $2; exit}' | tr -d '[:space:]'
}

role="$(extract role)"
case "$role" in
  master)
    exit 0
    ;;
  slave|replica)
    link_status="$(extract master_link_status)"
    if [ "$link_status" != "up" ]; then
      echo "redis readiness: master_link_status=${link_status:-unknown}" >&2
      exit 1
    fi
    sync_in_progress="$(extract master_sync_in_progress)"
    if [ "${sync_in_progress:-1}" != "0" ]; then
      echo "redis readiness: master_sync_in_progress=${sync_in_progress:-unknown}" >&2
      exit 1
    fi
    down_since="$(extract master_link_down_since_seconds)"
    if [ -n "$down_since" ] && [ "$down_since" != "0" ]; then
      echo "redis readiness: master_link_down_since_seconds=$down_since" >&2
      exit 1
    fi
    last_io="$(extract master_last_io_seconds_ago)"
    if [ -z "$last_io" ]; then
      last_io=0
    fi
    if [ "$last_io" -gt "$max_last_io" ]; then
      echo "redis readiness: master_last_io_seconds_ago=$last_io exceeds limit $max_last_io" >&2
      exit 1
    fi
    exit 0
    ;;
  *)
    echo "redis readiness: unexpected role ${role:-unknown}" >&2
    exit 1
    ;;
esac
`

func redisLivenessScript() string {
	return renderProbeScript(redisLivenessScriptRaw)
}

func redisReadinessScript() string {
	return renderProbeScript(redisReadinessScriptRaw)
}

func renderProbeScript(raw string) string {
	script := strings.ReplaceAll(raw, redisProbeTimeoutToken, strconv.Itoa(redisProbeTimeoutSeconds))
	return strings.TrimSpace(script) + "\n"
}
