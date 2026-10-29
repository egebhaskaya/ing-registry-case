#!/usr/bin/env bash
# One consumer-concurrency experiment (docs/design.md sections 6 and 7):
#   1. start the Registry jar with N consumers (and optionally a Spring profile, e.g. "platform" for D)
#   2. fire the k6 burst (shared-iterations, 10,000 by default)
#   3. wait until every accepted registration is committed in Registry and checked in Compliance
#   4. derive the numbers from k6, the databases and Prometheus, and append a row to docs/benchmark-results.md
#
# Usage:   load-tests/experiment.sh NAME CONSUMERS [PROFILE]
#          load-tests/experiment.sh A 1
#          load-tests/experiment.sh B 4
#          load-tests/experiment.sh C 16
#          load-tests/experiment.sh D 4 platform
# Env:     ITERATIONS (10000), VUS (50), JAVA_HOME (JDK 25), PROM (http://localhost:9090)
# Needs:   compose stack up with the init profile applied, Compliance running on 8081,
#          NO Registry running on 8080, and user_registry/target/*.jar built.
set -euo pipefail

NAME="${1:?usage: experiment.sh NAME CONSUMERS [PROFILE]}"
CONSUMERS="${2:?usage: experiment.sh NAME CONSUMERS [PROFILE]}"
PROFILE="${3:-}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
ITERATIONS="${ITERATIONS:-10000}"
VUS="${VUS:-50}"
PROM="${PROM:-http://localhost:9090}"
RUN_ID="${NAME}-$(date +%Y%m%d-%H%M%S)"
RESULTS_MD="$ROOT/docs/benchmark-results.md"
LOG_DIR="$HERE/results"
mkdir -p "$LOG_DIR"

COMPOSE_FILE="$ROOT/infra/compose.yml"
if command -v cygpath > /dev/null 2>&1; then COMPOSE_FILE="$(cygpath -w "$COMPOSE_FILE")"; fi
export MSYS_NO_PATHCONV=1
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"

sql_registry() { docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U postgres -d registry -tAc "$1" | tr -d '[:space:]'; }
sql_compliance() { docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U postgres -d compliance -tAc "$1" | tr -d '[:space:]'; }
# Instant PromQL query at a given unix time; prints the scalar value or "n/a" when there is no series.
prom() {
  local v
  v="$(curl -s --get "$PROM/api/v1/query" --data-urlencode "query=$1" --data-urlencode "time=$2" \
      | grep -o '"value":\[[^]]*\]' | sed -E 's/.*,"([^"]*)".*/\1/' | head -n 1)"
  [ -n "$v" ] && printf '%s' "$v" || printf 'n/a'
}
# Same hash as registrations.js: the run id becomes the first 8 hex characters of every Idempotency-Key.
fnv1a_hex8() {
  local s="$1" h=2166136261 i c
  for ((i = 0; i < ${#s}; i++)); do
    printf -v c '%d' "'${s:i:1}"
    h=$(( ((h ^ c) * 16777619) & 0xFFFFFFFF ))
  done
  printf '%08x' "$h"
}
num() { awk "BEGIN { printf \"%.1f\", $1 }"; }

echo "== experiment $NAME: consumers=$CONSUMERS profile=${PROFILE:-default} iterations=$ITERATIONS vus=$VUS run=$RUN_ID"

# ---- preconditions ----------------------------------------------------------------------------
curl -sf localhost:8081/actuator/health > /dev/null || { echo "Compliance is not running on 8081"; exit 2; }
if curl -sf localhost:8080/actuator/health > /dev/null 2>&1; then echo "A Registry is already running on 8080; stop it first"; exit 2; fi
JAR="$(ls "$ROOT"/user_registry/target/user_registry-*.jar 2> /dev/null | grep -v original | head -n 1 || true)"
[ -n "$JAR" ] || { echo "Build the Registry jar first: user_registry/mvnw -f user_registry/pom.xml -DskipTests package"; exit 2; }
# java.exe is a Windows program: with path conversion disabled above, hand it a Windows path.
if command -v cygpath > /dev/null 2>&1; then JAR="$(cygpath -w "$JAR")"; fi

# ---- start Registry with this experiment's consumer count -------------------------------------
args=(-jar "$JAR"
      "--spring.rabbitmq.listener.simple.concurrency=$CONSUMERS"
      "--spring.rabbitmq.listener.simple.max-concurrency=$CONSUMERS")
[ -n "$PROFILE" ] && args+=("--spring.profiles.active=$PROFILE")
"$JAVA" "${args[@]}" > "$LOG_DIR/$RUN_ID.registry.log" 2>&1 &
REG_PID=$!
trap 'kill "$REG_PID" 2> /dev/null || true' EXIT
for _ in $(seq 1 60); do curl -sf localhost:8080/actuator/health > /dev/null 2>&1 && break; sleep 2; done
curl -sf localhost:8080/actuator/health > /dev/null || { echo "Registry did not become healthy; see $LOG_DIR/$RUN_ID.registry.log"; exit 2; }

metrics="$(curl -s localhost:8080/actuator/prometheus)"
consumers_seen="$(printf '%s\n' "$metrics" | awk '/^registry_config_consumers/ {print $2}' | head -n 1)"
prefetch_seen="$(printf '%s\n' "$metrics" | awk '/^registry_config_prefetch/ {print $2}' | head -n 1)"
pool_seen="$(printf '%s\n' "$metrics" | awk '/^hikaricp_connections_max/ {print $2}' | head -n 1)"
virtual_seen="$(printf '%s\n' "$metrics" | awk '/^registry_threads_virtual_enabled/ {print $2}' | head -n 1)"
java_seen="$("$JAVA" -version 2>&1 | head -n 1 | sed -E 's/.*"([^"]+)".*/\1/')"
echo "registry reports: consumers=${consumers_seen:-?} prefetch=${prefetch_seen:-?} pool=${pool_seen:-?} virtual=${virtual_seen:-?} java=$java_seen"

# ---- the burst --------------------------------------------------------------------------------
T0_EPOCH="$(date +%s)"
RUN_ID="$RUN_ID" ITERATIONS="$ITERATIONS" VUS="$VUS" bash "$HERE/run-k6.sh" | tee "$LOG_DIR/$RUN_ID.k6.txt"
K6_TXT="$LOG_DIR/$RUN_ID.k6.txt"
iterations="$(sed -n 's/^iterations: *\([0-9]*\).*/\1/p' "$K6_TXT")"
accepted="$(sed -n 's/^accepted (202): *\([0-9]*\).*/\1/p' "$K6_TXT")"
rejected="$(sed -n 's/^rejected (503): *\([0-9]*\).*/\1/p' "$K6_TXT")"
other="$(sed -n 's/^other: *\([0-9]*\).*/\1/p' "$K6_TXT")"
http_p50="$(sed -n 's#^http p50 / p95 ms: *\([0-9.]*\) / \([0-9.]*\).*#\1#p' "$K6_TXT")"
http_p95="$(sed -n 's#^http p50 / p95 ms: *\([0-9.]*\) / \([0-9.]*\).*#\2#p' "$K6_TXT")"
submit_s="$(sed -n 's/^submission took: *\([0-9.]*\) s.*/\1/p' "$K6_TXT")"
: "${iterations:=0}" "${accepted:=0}" "${rejected:=0}" "${other:=0}"

# ---- drain: every accepted registration committed in Registry ---------------------------------
# Registry stores emails lower-cased, so the run id (which may contain upper-case letters, e.g. "A-...")
# is matched case-insensitively with ILIKE.
email_filter="email ILIKE 'load-$RUN_ID-%'"
deadline=$((SECONDS + 900))
while :; do
  users="$(sql_registry "SELECT count(*) FROM users WHERE $email_filter")"
  [ "$users" = "$accepted" ] && break
  [ "$SECONDS" -ge "$deadline" ] && { echo "drain timeout: users=$users accepted=$accepted"; break; }
  sleep 2
done
first_commit="$(sql_registry "SELECT extract(epoch from min(created_at)) FROM users WHERE $email_filter")"
last_commit="$(sql_registry "SELECT extract(epoch from max(created_at)) FROM users WHERE $email_filter")"
distinct_keys="$(sql_registry "SELECT count(DISTINCT registration_id) FROM users WHERE $email_filter")"
outbox="$(sql_registry "SELECT count(*) FROM outbox WHERE payload->>'email' ILIKE 'load-$RUN_ID-%'")"
drain_s="$(num "${last_commit:-0} - $T0_EPOCH")"
committed_per_s="$(num "($accepted) / ((${last_commit:-1}) - (${first_commit:-0}) + 0.000001)")"
echo "registry: users=$users distinct keys=$distinct_keys outbox=$outbox drain=${drain_s}s committed/s=$committed_per_s"

# ---- Compliance: every accepted registration checked ------------------------------------------
run_hex="$(fnv1a_hex8 "$RUN_ID")"
key_prefix="$run_hex-0000-4000-8000-%"
deadline=$((SECONDS + 900))
while :; do
  results="$(sql_compliance "SELECT count(*) FROM compliance_results WHERE registration_id::text LIKE '$key_prefix'")"
  [ "$results" = "$accepted" ] && break
  [ "$SECONDS" -ge "$deadline" ] && { echo "compliance timeout: results=$results accepted=$accepted"; break; }
  sleep 2
done
last_check="$(sql_compliance "SELECT extract(epoch from max(checked_at)) FROM compliance_results WHERE registration_id::text LIKE '$key_prefix'")"
compliance_s="$(num "${last_check:-0} - $T0_EPOCH")"
echo "compliance: results=$results completion=${compliance_s}s"

# ---- Prometheus over the run window (5s scrapes: short waits can fall between samples) --------
end_epoch="$(date +%s)"
window="$((end_epoch - T0_EPOCH + 10))s"
round1() { case "$1" in n/a|NaN) printf '%s' "$1" ;; *) num "$1" ;; esac; }
pending_max="$(round1 "$(prom "max_over_time(hikaricp_connections_pending{application=\"registry-service\"}[$window])" "$end_epoch")")"
timeouts="$(round1 "$(prom "increase(hikaricp_connections_timeout_total{application=\"registry-service\"}[$window])" "$end_epoch")")"
reg_p95="$(round1 "$(prom "histogram_quantile(0.95, sum(rate(registry_registration_completion_seconds_bucket[$window])) by (le))" "$end_epoch")")"
comp_p95="$(round1 "$(prom "histogram_quantile(0.95, sum(rate(compliance_completion_seconds_bucket[$window])) by (le))" "$end_epoch")")"

status="COMPLETE"
[ "$iterations" = "$ITERATIONS" ] || status="INCOMPLETE (k6 ran $iterations of $ITERATIONS)"
[ "$users" = "$accepted" ] && [ "$results" = "$accepted" ] && [ "$distinct_keys" = "$accepted" ] || status="INCOMPLETE (counts differ)"

virtual_label="on"; [ "${virtual_seen:-1}" = "0.0" ] && virtual_label="off"
row="| $NAME | $RUN_ID | ${consumers_seen%.0} | ${pool_seen%.0} | ${prefetch_seen%.0} | $virtual_label | $java_seen | $iterations | $accepted | $rejected | $other | ${submit_s:-n/a} | ${http_p50:-n/a} | ${http_p95:-n/a} | $drain_s | $committed_per_s | $reg_p95 | $compliance_s | $comp_p95 | $pending_max | $timeouts | $users / $outbox / $results | $status |"
printf '%s\n' "$row" >> "$RESULTS_MD"
echo "appended to $RESULTS_MD:"
echo "$row"
