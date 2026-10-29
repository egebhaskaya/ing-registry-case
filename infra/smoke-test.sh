#!/usr/bin/env bash
# End-to-end smoke test against the local Compose stack (docs/design.md section 9: "verify the real
# Debezium path in an end-to-end test or repeatable scripted smoke test").
#
# Prerequisites: the stack is up (compose + init profile) and BOTH services are running.
# Usage:  infra/smoke-test.sh            # 20 registrations
#         COUNT=200 infra/smoke-test.sh  # more
# Runs on Git Bash (Windows), macOS and Linux. Exit code 0 only when at least one registration was
# accepted and every accepted registration became exactly one user, one outbox event, one Compliance
# ledger row and one Compliance result.
set -euo pipefail

COUNT="${COUNT:-20}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"
COMPOSE_FILE="$(cd "$(dirname "$0")" && pwd)/compose.yml"
RUN_ID="$(date +%s)-$RANDOM"
# Git Bash on Windows rewrites POSIX-looking arguments (such as /opt/kafka/bin/...) into Windows
# paths before handing them to docker. Disable that, and convert the one path that really is a
# Windows file (the compose file) ourselves. On macOS/Linux neither step changes anything.
export MSYS_NO_PATHCONV=1
if command -v cygpath > /dev/null 2>&1; then COMPOSE_FILE="$(cygpath -w "$COMPOSE_FILE")"; fi

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }
# SQL goes in through stdin (not as an argument): the registration-id list below can exceed the
# command-line length limits of Windows and Linux long before 10,000 registrations.
sql_registry() { printf '%s\n' "$1" | compose exec -T postgres psql -U postgres -d registry -tA | tr -d '[:space:]'; }
sql_compliance() { printf '%s\n' "$1" | compose exec -T postgres psql -U postgres -d compliance -tA | tr -d '[:space:]'; }
random_uuid() { printf '%08x-%04x-4%03x-%04x-%012x' $((RANDOM * RANDOM)) $RANDOM $((RANDOM % 4096)) $RANDOM $((RANDOM * RANDOM * RANDOM)); }

echo "run id: $RUN_ID  submitting $COUNT registrations to $BASE_URL"
accepted=0
other=0
keys=()
for i in $(seq 1 "$COUNT"); do
  key="$(random_uuid)"
  keys+=("$key")
  # The body is one line of JSON; the status code is appended on its own line and taken with tail.
  # A connection failure (curl exit code) is counted as "000" instead of aborting the script.
  code="$(curl -sS -w '\n%{http_code}' -X POST "$BASE_URL/registrations" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d "{\"fullName\":\"Smoke Test $i\",\"email\":\"smoke-$RUN_ID-$i@example.com\",\"countryCode\":\"NL\"}" 2>&1 | tail -n 1)" || code="000"
  case "$code" in
    202) accepted=$((accepted + 1)) ;;
    *) other=$((other + 1)); echo "  request $i -> HTTP $code" ;;
  esac
done
echo "accepted (202): $accepted   other: $other"
if [ "$accepted" -eq 0 ]; then
  echo "FAIL: no request was accepted; is the Registry service running and RabbitMQ reachable?"
  exit 1
fi

id_list="$(printf "'%s'," "${keys[@]}")"
id_list="${id_list%,}"

deadline=$((SECONDS + TIMEOUT_SECONDS))
while :; do
  users="$(sql_registry "SELECT count(*) FROM users WHERE email LIKE 'smoke-$RUN_ID-%'")"
  outbox="$(sql_registry "SELECT count(*) FROM outbox WHERE payload->>'email' LIKE 'smoke-$RUN_ID-%'")"
  processed="$(sql_compliance "SELECT count(*) FROM processed_events WHERE registration_id IN ($id_list)")"
  results="$(sql_compliance "SELECT count(*) FROM compliance_results WHERE registration_id IN ($id_list)")"
  printf '  users=%s outbox=%s processed=%s results=%s (want %s)\n' "$users" "$outbox" "$processed" "$results" "$accepted"
  if [ "$users" = "$accepted" ] && [ "$outbox" = "$accepted" ] && [ "$processed" = "$accepted" ] && [ "$results" = "$accepted" ]; then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "FAIL: counts did not converge within ${TIMEOUT_SECONDS}s"
    exit 1
  fi
  sleep 2
done

echo "compliance consumer group (lag should be 0):"
compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group compliance 2>/dev/null | awk 'NR<=2 || $1=="compliance" {print "  " $0}' | cut -c1-140 || true

echo "OK: $accepted accepted, $users users, $outbox outbox events, $processed processed, $results compliance results"
