#!/usr/bin/env bash
# Bounded synchronous registration experiment. Existing Registry processes can remain idle on
# another port. Do not submit async work during this run: both instances share PostgreSQL.
# Usage: ITERATIONS=2000 VUS=50 PORT=8082 load-tests/sync-experiment.sh S1 1 [platform]
# Run with JDK 25 and a built Registry jar. Does not truncate databases or purge queues.
set -euo pipefail

NAME="${1:?usage: sync-experiment.sh NAME PARALLELISM [PROFILE]}"
PARALLELISM="${2:?usage: sync-experiment.sh NAME PARALLELISM [PROFILE]}"
PROFILE="${3:-}"
[[ "$NAME" =~ ^[A-Za-z0-9-]+$ ]] || { echo "NAME must contain only letters, digits and hyphens"; exit 2; }
[[ "$PARALLELISM" =~ ^[1-9][0-9]*$ ]] || { echo "PARALLELISM must be a positive integer"; exit 2; }
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PORT="${PORT:-8080}"
[[ "$PORT" =~ ^[0-9]+$ ]] && ((PORT >= 1 && PORT <= 65535)) || { echo "Invalid PORT"; exit 2; }
LOCAL_BASE_URL="${LOCAL_BASE_URL:-http://localhost:$PORT}"
HEALTH_URL="${HEALTH_URL:-$LOCAL_BASE_URL/actuator/health}"
if [ -n "${K6_BIN:-}" ]; then
  BASE_URL="${BASE_URL:-http://localhost:$PORT}"
else
  BASE_URL="${BASE_URL:-http://host.docker.internal:$PORT}"
fi
ITERATIONS="${ITERATIONS:-2000}"
VUS="${VUS:-50}"
MAX_WAITING_REQUESTS="${MAX_WAITING_REQUESTS:-64}"
ADMISSION_WAIT="${ADMISSION_WAIT:-200ms}"
WAIT_FOR_COMPLIANCE="${WAIT_FOR_COMPLIANCE:-1}"
COMPLIANCE_TIMEOUT="${COMPLIANCE_TIMEOUT:-900}"
RABBIT_API="${RABBIT_API:-http://localhost:15672}"
RABBIT_USER="${RABBIT_USER:-guest}"
RABBIT_PASSWORD="${RABBIT_PASSWORD:-guest}"
RUN_ID="${NAME}-sync-$(date +%Y%m%d-%H%M%S-%N)-$RANDOM"
LOG_DIR="${RESULTS_DIR:-$HERE/results}"
RESULTS_TSV="$LOG_DIR/sync-experiments.tsv"
mkdir -p "$LOG_DIR"
COMPOSE_FILE="$ROOT/infra/compose.yml"
if command -v cygpath > /dev/null 2>&1; then COMPOSE_FILE="$(cygpath -w "$COMPOSE_FILE")"; fi
export MSYS_NO_PATHCONV=1
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
REG_PID=""
SAMPLE_PID=""
cleanup() {
  if [ -n "$SAMPLE_PID" ]; then
    kill "$SAMPLE_PID" 2> /dev/null || true
    wait "$SAMPLE_PID" 2> /dev/null || true
  fi
  if [ -n "$REG_PID" ]; then
    kill "$REG_PID" 2> /dev/null || true
    wait "$REG_PID" 2> /dev/null || true
  fi
}
trap cleanup EXIT

sql_registry() { docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U postgres -d registry -tAc "$1" | tr -d '[:space:]'; }
sql_compliance() { docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U postgres -d compliance -tAc "$1" | tr -d '[:space:]'; }
# Fail on any existing command or dead-letter work; do not delete or consume it to clean a run.
assert_idle_queue() {
  local queue response status body ready unacked
  for queue in registration.commands registration.commands.dlq; do
    response="$(curl --max-time 10 -sS -u "$RABBIT_USER:$RABBIT_PASSWORD" \
      -w '\n%{http_code}' "$RABBIT_API/api/queues/%2F/$queue?disable_stats=true&enable_queue_totals=true")" \
      || { echo "Cannot inspect RabbitMQ management API"; return 1; }
    status="${response##*$'\n'}"
    body="${response%$'\n'*}"
    [ "$status" != "404" ] || continue # A queue not created yet has no old work.
    [ "$status" = "200" ] || { echo "RabbitMQ inspection failed: HTTP $status for $queue"; return 1; }
    ready="$(printf '%s\n' "$body" | grep -oE '"messages_ready"[[:space:]]*:[[:space:]]*[0-9]+' | sed 's/[^0-9]//g' || true)"
    unacked="$(printf '%s\n' "$body" | grep -oE '"messages_unacknowledged"[[:space:]]*:[[:space:]]*[0-9]+' | sed 's/[^0-9]//g' || true)"
    [ -n "$ready" ] && [ -n "$unacked" ] || { echo "Missing message counts for $queue; cannot establish idle state"; return 1; }
    if [ "$ready" != "0" ] || [ "$unacked" != "0" ]; then
      echo "Nonempty $queue: ready=$ready unacknowledged=$unacked. Inspect/drain existing work before benchmarking."
      return 1
    fi
  done
}
fnv1a_hex8() {
  local s="$1" h=2166136261 i c
  for ((i = 0; i < ${#s}; i++)); do
    printf -v c '%d' "'${s:i:1}"
    h=$(( ((h ^ c) * 16777619) & 0xFFFFFFFF ))
  done
  printf '%08x' "$h"
}
metric_value() {
  awk -v metric="$1" '$1 ~ ("^" metric "($|\\{)") { print $2; found = 1; exit } END { if (!found) print "n/a" }'
}
sample_metrics() {
  local metrics active waiting pending
  printf 'unix_s\tprocessing_active\thttp_waiting\thikari_pending\n'
  while :; do
    metrics="$(curl --max-time 3 -fsS "$LOCAL_BASE_URL/actuator/prometheus" 2> /dev/null || true)"
    active="$(printf '%s\n' "$metrics" | metric_value registry_processing_active)"
    waiting="$(printf '%s\n' "$metrics" | metric_value registry_processing_waiting_requests)"
    pending="$(printf '%s\n' "$metrics" | metric_value hikaricp_connections_pending)"
    printf '%s\t%s\t%s\t%s\n' "$(date +%s)" "$active" "$waiting" "$pending"
    sleep 0.5
  done
}
sample_max() {
  awk -v col="$1" 'NR > 1 && $col != "n/a" { if (!seen || $col + 0 > max) max = $col + 0; seen = 1 } END { if (seen) print max; else print "n/a" }' "$LOG_DIR/$RUN_ID.samples.tsv"
}

if curl --max-time 3 -s -o /dev/null "$HEALTH_URL"; then
  echo "Port $PORT is already serving HTTP; choose an unused PORT or stop that Registry first."
  exit 2
fi
if [ "$WAIT_FOR_COMPLIANCE" = "1" ]; then
  curl --max-time 3 -fsS localhost:8081/actuator/health > /dev/null || { echo "Compliance is not healthy on 8081; start it or use WAIT_FOR_COMPLIANCE=0"; exit 2; }
fi
assert_idle_queue
JAR="${REGISTRY_JAR:-}"
if [ -z "$JAR" ]; then
  JAR="$(ls "$ROOT"/user_registry/target/user_registry-*.jar 2> /dev/null | grep -v original | head -n 1 || true)"
fi
[ -n "$JAR" ] || { echo "Build the Registry jar first"; exit 2; }
if command -v cygpath > /dev/null 2>&1; then JAR="$(cygpath -w "$JAR")"; fi
args=(-jar "$JAR"
      "--server.port=$PORT"
      --spring.rabbitmq.listener.simple.auto-startup=false
      --spring.datasource.hikari.maximum-pool-size=4
      "--registry.processing.max-concurrency=$PARALLELISM"
      "--registry.processing.max-waiting-requests=$MAX_WAITING_REQUESTS"
      "--registry.processing.admission-wait=$ADMISSION_WAIT")
[ -z "$PROFILE" ] || args+=("--spring.profiles.active=$PROFILE")
echo "sync experiment: run=$RUN_ID parallelism=$PARALLELISM pool=4 vus=$VUS iterations=$ITERATIONS port=$PORT"
"$JAVA" "${args[@]}" > "$LOG_DIR/$RUN_ID.registry.log" 2>&1 &
REG_PID=$!
for _ in $(seq 1 60); do
  curl --max-time 3 -fsS "$HEALTH_URL" > /dev/null 2>&1 && break
  kill -0 "$REG_PID" 2> /dev/null || break
  sleep 2
done
curl --max-time 3 -fsS "$HEALTH_URL" > /dev/null || { echo "Registry did not become healthy; see $LOG_DIR/$RUN_ID.registry.log"; exit 2; }
assert_idle_queue
curl --max-time 15 --retry 2 --retry-all-errors --retry-delay 1 -fsS "$LOCAL_BASE_URL/actuator/prometheus" > "$LOG_DIR/$RUN_ID.before.prom"
processing_seen="$(metric_value registry_config_processing_concurrency < "$LOG_DIR/$RUN_ID.before.prom")"
pool_seen="$(metric_value hikaricp_connections_max < "$LOG_DIR/$RUN_ID.before.prom")"
virtual_seen="$(metric_value registry_threads_virtual_enabled < "$LOG_DIR/$RUN_ID.before.prom")"
awk -v actual="$processing_seen" -v wanted="$PARALLELISM" 'BEGIN { exit !(actual + 0 == wanted + 0) }' || { echo "Processing limit does not match: $processing_seen"; exit 2; }
awk -v actual="$pool_seen" 'BEGIN { exit !(actual + 0 == 4) }' || { echo "Hikari maximum does not match: $pool_seen"; exit 2; }
sample_metrics > "$LOG_DIR/$RUN_ID.samples.tsv" &
SAMPLE_PID=$!
RESULTS_DIR="$LOG_DIR" MODE=sync RUN_ID="$RUN_ID" ITERATIONS="$ITERATIONS" VUS="$VUS" BASE_URL="$BASE_URL" \
  bash "$HERE/run-k6.sh" | tee "$LOG_DIR/$RUN_ID.k6.txt"
kill "$SAMPLE_PID" 2> /dev/null || true
wait "$SAMPLE_PID" 2> /dev/null || true
SAMPLE_PID=""
curl --max-time 15 --retry 2 --retry-all-errors --retry-delay 1 -fsS "$LOCAL_BASE_URL/actuator/prometheus" > "$LOG_DIR/$RUN_ID.after.prom"

K6_TXT="$LOG_DIR/$RUN_ID.k6.txt"
read_count() { sed -n "s/^$1: *\\([0-9]*\\).*/\\1/p" "$K6_TXT"; }
iterations="$(read_count iterations)"
created="$(read_count 'created (201)')"
replayed="$(read_count 'replayed (200)')"
accepted="$(read_count 'accepted (202)')"
conflicts="$(read_count 'conflicts (409)')"
rejected="$(read_count 'rejected (503)')"
other="$(read_count other)"
invalid="$(read_count 'invalid bodies')"
: "${iterations:=0}" "${created:=0}" "${replayed:=0}" "${accepted:=0}" "${conflicts:=0}" "${rejected:=0}" "${other:=0}" "${invalid:=0}"
http_p50="$(sed -n 's#^http p50 / p95 ms: *\([^ ]*\) / \([^ ]*\).*#\1#p' "$K6_TXT")"
http_p95="$(sed -n 's#^http p50 / p95 ms: *\([^ ]*\) / \([^ ]*\).*#\2#p' "$K6_TXT")"
success_p50="$(sed -n 's#^success p50/p95 ms: *\([^ ]*\) / \([^ ]*\).*#\1#p' "$K6_TXT")"
success_p95="$(sed -n 's#^success p50/p95 ms: *\([^ ]*\) / \([^ ]*\).*#\2#p' "$K6_TXT")"
submit_s="$(sed -n 's/^submission took: *\([0-9.]*\) s.*/\1/p' "$K6_TXT")"
created_per_s="$(awk -v n="$created" -v seconds="${submit_s:-0}" 'BEGIN { if (seconds > 0) printf "%.1f", n / seconds; else print "n/a" }')"
email_filter="email ILIKE 'load-$RUN_ID-%'"
users="$(sql_registry "SELECT count(*) FROM users WHERE $email_filter")"
keys="$(sql_registry "SELECT count(DISTINCT registration_id) FROM users WHERE $email_filter")"
outbox="$(sql_registry "SELECT count(*) FROM outbox WHERE payload->>'email' ILIKE 'load-$RUN_ID-%'")"
compliance="skipped"
if [ "$WAIT_FOR_COMPLIANCE" = "1" ]; then
  key_prefix="$(fnv1a_hex8 "$RUN_ID")-0000-4000-8000-%"
  deadline=$((SECONDS + COMPLIANCE_TIMEOUT))
  while :; do
    compliance="$(sql_compliance "SELECT count(*) FROM compliance_results WHERE registration_id::text LIKE '$key_prefix'")"
    [ "$compliance" = "$created" ] && break
    curl --max-time 3 -fsS localhost:8081/actuator/health > /dev/null 2>&1 \
      || { echo "Compliance became unavailable: results=$compliance committed=$created"; break; }
    [ "$SECONDS" -lt "$deadline" ] || { echo "Compliance timeout: results=$compliance committed=$created"; break; }
    sleep 2
  done
fi
status="COMPLETE"
[ "$rejected" = "0" ] || status="COMPLETE_WITH_REJECTIONS"
[ "$iterations" = "$ITERATIONS" ] && [ "$users" = "$created" ] && [ "$keys" = "$created" ] && [ "$outbox" = "$created" ] || status="INCOMPLETE_COUNTS"
[ "$replayed" = "0" ] && [ "$accepted" = "0" ] && [ "$conflicts" = "0" ] && [ "$other" = "0" ] && [ "$invalid" = "0" ] || status="UNEXPECTED_RESPONSES"
[ "$compliance" = "skipped" ] || [ "$compliance" = "$created" ] || status="INCOMPLETE_COMPLIANCE"
assert_idle_queue || status="RABBIT_WORK_INTERFERENCE"
active_max="$(sample_max 2)"
waiting_max="$(sample_max 3)"
pending_max="$(sample_max 4)"
[ -f "$RESULTS_TSV" ] || printf 'run_id\tprocessing_limit\tpool_max\tclient_vus\tvirtual\titerations\tcreated_201\treplayed_200\taccepted_202\tconflicts_409\trejected_503\tother\tinvalid_bodies\tsubmit_s\tcreated_per_s\thttp_p50_ms\thttp_p95_ms\tsuccess_p50_ms\tsuccess_p95_ms\tactive_sample_max\twaiting_sample_max\thikari_pending_sample_max\tusers\toutbox\tcompliance\tstatus\n' > "$RESULTS_TSV"
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  "$RUN_ID" "$processing_seen" "$pool_seen" "$VUS" "$virtual_seen" "$iterations" "$created" "$replayed" "$accepted" "$conflicts" "$rejected" "$other" "$invalid" "${submit_s:-n/a}" "$created_per_s" "${http_p50:-n/a}" "${http_p95:-n/a}" "${success_p50:-n/a}" "${success_p95:-n/a}" "$active_max" "$waiting_max" "$pending_max" "$users" "$outbox" "$compliance" "$status" >> "$RESULTS_TSV"
echo "sync result: $status; committed=$users outbox=$outbox compliance=$compliance rejected=$rejected"
echo "samples (500ms, brief peaks can be missed): active=$active_max waiting=$waiting_max hikari_pending=$pending_max"
echo "results: $RESULTS_TSV"
case "$status" in COMPLETE*) ;; *) exit 3 ;; esac
