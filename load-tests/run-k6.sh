#!/usr/bin/env bash
# Runs the k6 scenario from the pinned grafana/k6 image, so nothing needs installing on the host.
# The container reaches the Registry API on the host through host.docker.internal.
#
# Usage:  load-tests/run-k6.sh                         # 10,000 iterations, 50 VUs
#         ITERATIONS=500 VUS=20 load-tests/run-k6.sh
#         RUN_ID=B-20261029 load-tests/run-k6.sh       # choose the run id (used in keys, emails, file name)
# Output: load-tests/results/<RUN_ID>.summary.json and a short text summary on stdout.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
export RUN_ID="${RUN_ID:-run-$(date +%Y%m%d-%H%M%S)}"
export VUS="${VUS:-50}"
export ITERATIONS="${ITERATIONS:-10000}"
export MAX_DURATION="${MAX_DURATION:-10m}"
export BASE_URL="${BASE_URL:-http://host.docker.internal:8080}"

mkdir -p "$HERE/results"
SCRIPTS_DIR="$HERE"
RESULTS_DIR="$HERE/results"
# Git Bash on Windows: hand docker real Windows paths and stop it rewriting the container paths.
if command -v cygpath > /dev/null 2>&1; then
  SCRIPTS_DIR="$(cygpath -w "$SCRIPTS_DIR")"
  RESULTS_DIR="$(cygpath -w "$RESULTS_DIR")"
fi
export MSYS_NO_PATHCONV=1

echo "k6: run id $RUN_ID, $ITERATIONS iterations, $VUS VUs, target $BASE_URL"
docker run --rm -i \
  --add-host=host.docker.internal:host-gateway \
  -e BASE_URL -e RUN_ID -e VUS -e ITERATIONS -e MAX_DURATION \
  -v "$SCRIPTS_DIR:/scripts:ro" \
  -v "$RESULTS_DIR:/results" \
  grafana/k6:2.3.0 run --quiet /scripts/registrations.js

echo "summary: $HERE/results/$RUN_ID.summary.json"
