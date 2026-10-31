#!/usr/bin/env bash
# Runs the pinned grafana/k6 image by default, or a native k6 executable supplied as K6_BIN.
# The container reaches the Registry API on the host through host.docker.internal.
#
# Usage:  load-tests/run-k6.sh                         # 10,000 iterations, 50 VUs
#         ITERATIONS=500 VUS=20 load-tests/run-k6.sh
#         MODE=sync load-tests/run-k6.sh
#         MODE=batched load-tests/run-k6.sh
#         CONTINUOUS=1 RATE=100 load-tests/run-k6.sh  # steady arrivals until Ctrl+C
# Application tuning comes from application.properties, never from this runner.
# Output: results/<RUN_ID>.summary.json, the shared results/index.html, and a terminal summary.
set -euo pipefail

command -v node > /dev/null || { echo "Node.js is required to update the local HTML run report."; exit 2; }

HERE="$(cd "$(dirname "$0")" && pwd)"
REPORT_SCRIPT="$HERE/render-report.mjs"
if command -v cygpath > /dev/null 2>&1; then REPORT_SCRIPT="$(cygpath -w "$REPORT_SCRIPT")"; fi
# Include failed/interrupted runs whenever k6 still produced a JSON summary.
finish_run() {
  local status="$1"
  trap - EXIT INT TERM HUP
  local summary="$RESULTS_DIR/$RUN_ID.summary.json"
  if [ -f "$summary" ]; then
    # k6's summary does not always expose interruption state. Keep the actual runner exit code.
    if ! node --input-type=module - "$summary" "$status" <<'JS'
import { readFile, writeFile, rename } from 'node:fs/promises';
const [path, status] = process.argv.slice(2);
const data = JSON.parse(await readFile(path, 'utf8'));
data.run ??= {};
data.run.exitCode = Number(status);
if ([105, 129, 130, 143].includes(Number(status))) data.run.interrupted = true;
const temporary = `${path}.tmp`;
await writeFile(temporary, JSON.stringify(data, null, 2), 'utf8');
await rename(temporary, path);
JS
    then
      [ "$status" -ne 0 ] || status=1
    fi
    echo "summary: $summary"
  else
    echo "No JSON summary was produced for $RUN_ID; existing runs are still available."
  fi
  if ! node "$REPORT_SCRIPT" "$RESULTS_DIR"; then
    [ "$status" -ne 0 ] || status=1
  fi
  exit "$status"
}
export MODE="${MODE:-async}"
case "$MODE" in async|sync|batched) ;; *) echo "MODE must be async, sync or batched"; exit 2 ;; esac
export CONTINUOUS="${CONTINUOUS:-0}"
case "$CONTINUOUS" in 0|1) ;; *) echo "CONTINUOUS must be 0 or 1"; exit 2 ;; esac
export RUN_ID="${RUN_ID:-$MODE-$(date +%Y%m%d-%H%M%S-%N)-$RANDOM}"
[[ "$RUN_ID" =~ ^[A-Za-z0-9-]+$ ]] || { echo "RUN_ID must contain only letters, digits and hyphens"; exit 2; }
export VUS="${VUS:-50}"
export ITERATIONS="${ITERATIONS:-10000}"
export MAX_DURATION="${MAX_DURATION:-10m}"
export RATE="${RATE:-100}"
export DURATION="${DURATION:-8760h}"
export MAX_VUS="${MAX_VUS:-200}"
if [ -n "${K6_BIN:-}" ]; then
  export BASE_URL="${BASE_URL:-http://localhost:8080}"
else
  export BASE_URL="${BASE_URL:-http://host.docker.internal:8080}"
fi

SCRIPTS_DIR="$HERE"
RESULTS_DIR="${RESULTS_DIR:-$HERE/results}"
mkdir -p "$RESULTS_DIR"
# A single Ctrl+C lets k6 write its summary, then the EXIT handler rebuilds the shared report.
trap 'finish_run "$?"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
if [ "$CONTINUOUS" = 1 ]; then
  workload="target $RATE requests/s until Ctrl+C (duration ceiling $DURATION, $VUS preallocated / $MAX_VUS max VUs)"
else
  workload="$ITERATIONS iterations, $VUS VUs"
fi
# Native execution bypasses Docker Desktop host forwarding and uses the same scenario/options.
if [ -n "${K6_BIN:-}" ]; then
  SCRIPT_PATH="$HERE/registrations.js"
  RESULTS_DIR="$(cd "$RESULTS_DIR" && pwd)"
  if command -v cygpath > /dev/null 2>&1; then
    SCRIPT_PATH="$(cygpath -m "$SCRIPT_PATH")"
    RESULTS_DIR="$(cygpath -m "$RESULTS_DIR")"
  fi
  export SUMMARY_DIR="$RESULTS_DIR"
  export MSYS_NO_PATHCONV=1
  echo "k6: run id $RUN_ID, mode $MODE, $workload, target $BASE_URL (native)"
  status=0
  "$K6_BIN" run --quiet "$SCRIPT_PATH" || status=$?
  exit "$status"
fi

# Git Bash on Windows: hand docker real Windows paths and stop it rewriting the container paths.
if command -v cygpath > /dev/null 2>&1; then
  SCRIPTS_DIR="$(cygpath -w "$SCRIPTS_DIR")"
  RESULTS_DIR="$(cygpath -w "$RESULTS_DIR")"
fi
export MSYS_NO_PATHCONV=1

# Docker Desktop already resolves this name. Injecting host-gateway there can also inject an
# unreachable IPv6 address. Native Linux Docker needs the mapping unless configured separately.
host_args=()
host_gateway="${DOCKER_HOST_GATEWAY:-auto}"
if [ "$host_gateway" = "auto" ]; then
  case "$(docker info --format '{{.OperatingSystem}}')" in
    *Docker\ Desktop*) host_gateway=0 ;;
    *) host_gateway=1 ;;
  esac
fi
case "$host_gateway" in
  1) host_args+=(--add-host=host.docker.internal:host-gateway) ;;
  0) ;;
  *) echo "DOCKER_HOST_GATEWAY must be auto, 0 or 1"; exit 2 ;;
esac

echo "k6: run id $RUN_ID, mode $MODE, $workload, target $BASE_URL"
status=0
docker run --rm -i --name "registration-k6-$RUN_ID" \
  "${host_args[@]}" \
  -e BASE_URL -e MODE -e RUN_ID -e VUS -e ITERATIONS -e MAX_DURATION \
  -e CONTINUOUS -e RATE -e DURATION -e MAX_VUS \
  -v "$SCRIPTS_DIR:/scripts:ro" \
  -v "$RESULTS_DIR:/results" \
  grafana/k6:2.3.0 run --quiet /scripts/registrations.js || status=$?

exit "$status"
