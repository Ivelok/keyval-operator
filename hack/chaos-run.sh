#!/usr/bin/env bash
set -euo pipefail

: "${IMG:=controller:dev}"
: "${CHAOS_NAMESPACE:=keyval-chaos}"
: "${CHAOS_EXPERIMENTS:=pod-kill-storm,sentinel-network-partition,replica-restart-loop,sentinel-restart-loop}"
: "${CHAOS_TIMEOUT:=20m}"
: "${KEEP_ARTIFACTS:=1}"
: "${KEEP_RESOURCES:=0}"

echo "Redeploying operator image ${IMG}..."
IMG="${IMG}" make redeploy

echo "Running chaos suite in namespace ${CHAOS_NAMESPACE}..."
CHAOS_NAMESPACE="${CHAOS_NAMESPACE}" \
CHAOS_EXPERIMENTS="${CHAOS_EXPERIMENTS}" \
CHAOS_TIMEOUT="${CHAOS_TIMEOUT}" \
KEEP_ARTIFACTS="${KEEP_ARTIFACTS}" \
KEEP_RESOURCES="${KEEP_RESOURCES}" \
make chaos

echo "Generating SLA report..."
make sla-report
