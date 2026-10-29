# Demo script

The presentation sequence of design section 8, as commands. Run from the repository root in Git Bash
(Windows), macOS or Linux. Before starting: Docker Desktop running, Java 25 on `JAVA_HOME`. Every
command below has a `make` shorthand (`make up`, `make registry`, `make compliance`, `make register`,
`make smoke`, `make experiment-a` ... `make lag`); the long forms are shown so nothing is hidden.

## 0. Bring everything up (about two minutes)

```bash
docker compose -f infra/compose.yml up -d --wait                                   # make up
docker compose -f infra/compose.yml --profile init up --force-recreate kafka-init connect-init   # (part of make up)
user_registry/mvnw -f user_registry/pom.xml -DskipTests package                    # make build
compliance/mvnw -f compliance/pom.xml -DskipTests package
```

Terminal 2 (Compliance stays up for the whole demo):

```bash
java -jar compliance/target/compliance-0.0.1-SNAPSHOT.jar
```

Terminal 3 (Registry; the experiment script starts its own instance later, so stop this one before
experiments):

```bash
java -jar user_registry/target/user_registry-0.0.1-SNAPSHOT.jar
```

Open Grafana: <http://localhost:3000/d/registration-case> (no login needed locally). Open the RabbitMQ
UI: <http://localhost:15672> (guest / guest). Check the connector: `curl -s localhost:8083/connectors/registry-outbox/status`.

## 1. Acceptance is not completion

```bash
curl -i -X POST localhost:8080/registrations -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 11111111-1111-1111-1111-111111111111' \
  -d '{"fullName":"Ada Lovelace","email":"ada@example.com","countryCode":"NL"}'
```

Point at: `202` with `status: ACCEPTED`. Then the same registration reaching each stage:

```bash
MSYS_NO_PATHCONV=1 docker compose -f infra/compose.yml exec postgres psql -U registry -d registry \
  -c "SELECT email, registration_id, created_at FROM users WHERE email='ada@example.com'"
MSYS_NO_PATHCONV=1 docker compose -f infra/compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic outbox.event.user --from-beginning \
  --property print.key=true --property print.headers=true --timeout-ms 10000 | grep ada@example.com
MSYS_NO_PATHCONV=1 docker compose -f infra/compose.yml exec postgres psql -U compliance -d compliance \
  -c "SELECT registration_id, decision, reason FROM compliance_results WHERE registration_id='11111111-1111-1111-1111-111111111111'"
```

Dashboard panel 1 shows three counters rising one after another: accepted, committed, compliance.
Send the same request again: `202` again, Registry log line `outcome=DUPLICATE_IGNORED`, no new rows.

## 2. Experiment A: one consumer

Stop the Registry in terminal 3 (Ctrl+C). The experiment script starts its own instance with the
requested consumer count and stops it afterwards.

```bash
load-tests/experiment.sh A 1
```

Point at: panel 3 (ready messages pile up and drain slowly), panel 2 (committed/s flat at what one
consumer can do), panel 4 (active connections 1 of 4), panel 5 (registration completion p95 grows with
queue waiting while HTTP p95 stays flat). The script appends the numbers to `docs/benchmark-results.md`.

## 3. Experiment B: four consumers

```bash
load-tests/experiment.sh B 4
```

Point at: committed/s up, drain time down, active connections up to 4, no pending.

## 4. Experiment C: sixteen consumers, pool still four

```bash
load-tests/experiment.sh C 16
```

Point at: panel 4 `pending` above zero (twelve consumers waiting for four connections), committed/s
about the same as B. Explain the measured result instead of assuming more workers are faster.

## 5. Compliance outage and catch-up

With a Registry running (terminal 3 again, or during an experiment): stop Compliance in terminal 2
(Ctrl+C). Submit registrations (`COUNT=200 infra/smoke-test.sh` will time out on the Compliance
counts, that is the point) and show:

```bash
MSYS_NO_PATHCONV=1 docker compose -f infra/compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group compliance
```

Lag per partition grows; Registry keeps committing. Start Compliance again; lag returns to 0 and
`compliance_results` catches up. The dashboard's Kafka lag panel explains why lag is read from the
CLI and not from a consumer metric.

## 6. Redelivery is harmless

Reset the Compliance group to the beginning while Compliance is stopped, then start it:

```bash
MSYS_NO_PATHCONV=1 docker compose -f infra/compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group compliance --topic outbox.event.user --reset-offsets --to-earliest --execute
```

Point at: Compliance logs `compliance duplicate ignored` for every record, panel 6 shows the
"compliance duplicate ignored" rate, and the row counts do not change:

```bash
MSYS_NO_PATHCONV=1 docker compose -f infra/compose.yml exec postgres psql -U compliance -d compliance \
  -c "SELECT count(*) AS ledger FROM processed_events" -c "SELECT count(*) AS results FROM compliance_results"
```

## 7. A permanent failure in the DLQ

Send the Ada registration again with the same key but a different name:

```bash
curl -s -X POST localhost:8080/registrations -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 11111111-1111-1111-1111-111111111111' \
  -d '{"fullName":"Ada Byron","email":"ada@example.com","countryCode":"NL"}'
```

Point at: still `202` (the API only promises queuing), Registry log `registration rejected permanently`,
one message in `registration.commands.dlq` in the RabbitMQ UI with headers `x-exception-message`,
`x-exception-stacktrace`, `x-original-exchange` and `x-original-routingKey`, panel 6 "registry
dead-lettered". Note that the conflict took one attempt and no retries. Delete the DLQ message from the
UI afterwards if you want a clean queue.

The Kafka side has the same shape: a record Compliance cannot process ends up on
`outbox.event.user.DLT` with `kafka_dlt-exception-message`, `kafka_dlt-exception-fqcn` and
`kafka_dlt-original-topic/-partition/-offset` headers (an unparseable record goes there without retries;
a transient failure after three retries). Parked work is never replayed automatically; the README's
"Failure handling" section describes how to replay it safely.

## 8. Optional: experiment D, platform threads

Same as B with virtual threads off; everything else identical:

```bash
load-tests/experiment.sh D 4 platform
```

Point at: the context row shows "off (platform)", the diagnostics row shows the platform-thread count
and heap for B versus D. A small or negligible difference is a valid result; the point is that the
bounds (consumers, pool) decide throughput, not the thread type.

## Tear down

```bash
docker compose -f infra/compose.yml down        # keep data
docker compose -f infra/compose.yml down -v     # wipe everything
```
