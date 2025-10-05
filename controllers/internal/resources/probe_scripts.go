package resources

import "strings"

const (
	LivenessScriptKey   = "liveness.sh"
	LivenessScriptPath  = "/conf/" + LivenessScriptKey
	ReadinessScriptKey  = "readiness.sh"
	ReadinessScriptPath = "/conf/" + ReadinessScriptKey

	redisReadinessDefaultMaxLastIOSeconds = "15"
)

const redisLivenessScriptRaw = `#!/bin/sh
set -eu

host="${REDIS_PROBE_HOST:-127.0.0.1}"
port="${REDIS_PORT:-6379}"
timeout="${REDIS_PROBE_TIMEOUT:-2}"

tls_enabled=$(printf '%s' "${TLS_ENABLED:-false}" | tr '[:upper:]' '[:lower:]')

add_arg() {
  value=$(printf '%s' "$1" | sed "s/'/'\\''/g")
  cmd="${cmd} '${value}'"
}

redis_cli() {
  cmd=""
  if command -v timeout >/dev/null 2>&1; then
    add_arg "timeout"
    add_arg "${timeout}s"
  fi
  add_arg "redis-cli"
  add_arg "-h"
  add_arg "${host}"
  add_arg "-p"
  add_arg "${port}"
  if [ "$tls_enabled" = "true" ]; then
    add_arg "--tls"
    if [ -n "${TLS_CA_FILE:-}" ]; then
      add_arg "--cacert"
      add_arg "${TLS_CA_FILE}"
    fi
    if [ -n "${TLS_CERT_FILE:-}" ] && [ -n "${TLS_KEY_FILE:-}" ]; then
      add_arg "--cert"
      add_arg "${TLS_CERT_FILE}"
      add_arg "--key"
      add_arg "${TLS_KEY_FILE}"
    fi
  fi
  if [ -n "${MASTER_USER:-}" ]; then
    add_arg "--user"
    add_arg "${MASTER_USER}"
  fi
  if [ $# -gt 0 ]; then
    while [ "$#" -gt 0 ]; do
      add_arg "$1"
      shift
    done
  fi
  eval "command ${cmd}"
}

redis_cli PING
`

const redisReadinessScriptRaw = `#!/bin/sh
set -eu

host="${REDIS_PROBE_HOST:-127.0.0.1}"
port="${REDIS_PORT:-6379}"
timeout="${REDIS_PROBE_TIMEOUT:-2}"
max_last_io="${REDIS_READINESS_MAX_LAST_IO_SECONDS:-` + redisReadinessDefaultMaxLastIOSeconds + `}"

tls_enabled=$(printf '%s' "${TLS_ENABLED:-false}" | tr '[:upper:]' '[:lower:]')

add_arg() {
  value=$(printf '%s' "$1" | sed "s/'/'\\''/g")
  cmd="${cmd} '${value}'"
}

redis_cli() {
  cmd=""
  if command -v timeout >/dev/null 2>&1; then
    add_arg "timeout"
    add_arg "${timeout}s"
  fi
  add_arg "redis-cli"
  add_arg "-h"
  add_arg "${host}"
  add_arg "-p"
  add_arg "${port}"
  if [ "$tls_enabled" = "true" ]; then
    add_arg "--tls"
    if [ -n "${TLS_CA_FILE:-}" ]; then
      add_arg "--cacert"
      add_arg "${TLS_CA_FILE}"
    fi
    if [ -n "${TLS_CERT_FILE:-}" ] && [ -n "${TLS_KEY_FILE:-}" ]; then
      add_arg "--cert"
      add_arg "${TLS_CERT_FILE}"
      add_arg "--key"
      add_arg "${TLS_KEY_FILE}"
    fi
  fi
  if [ -n "${MASTER_USER:-}" ]; then
    add_arg "--user"
    add_arg "${MASTER_USER}"
  fi
  if [ $# -gt 0 ]; then
    while [ "$#" -gt 0 ]; do
      add_arg "$1"
      shift
    done
  fi
  eval "command ${cmd}"
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
	return strings.TrimSpace(redisLivenessScriptRaw) + "\n"
}

func redisReadinessScript() string {
	return strings.TrimSpace(redisReadinessScriptRaw) + "\n"
}
