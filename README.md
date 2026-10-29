# User Registration Burst Processing

A small, explainable backend case: accept a burst of 10,000 user registrations through an asynchronous
`202 Accepted` API, process them with bounded concurrency, and hand each completed registration to a
Compliance service through a transactional outbox, Debezium change data capture and Kafka.

- The design and the reasoning behind every choice: [docs/design.md](docs/design.md)
- What was built and what it guarantees: [docs/architecture.md](docs/architecture.md)
- The presentation, step by step: [docs/demo-script.md](docs/demo-script.md)
- Measured results of the 10,000-request experiments: [docs/benchmark-results.md](docs/benchmark-results.md)

```
client --POST /registrations--> Registry API --publish (confirmed)--> RabbitMQ
                                     |                                   |
                                 202 Accepted              4 bounded consumers, prefetch 8
                                                                         |
                                                       one transaction: users + outbox (PostgreSQL)
                                                                         |
                                            Debezium (CDC) --> Kafka outbox.event.user --> Compliance
                                                                                             |
                                                                 one transaction: processed_events + compliance_results
```

## Status

| Phase | Content | State |
|-------|---------|-------|
| 1. Foundation | Java 25, Spring Boot 4.1.2, two layered services, pinned Compose infrastructure, Flyway | done |
| 2. Reliable Registry | confirmed `202` intake, bounded publishing and consumers, atomic user + outbox, idempotency, virtual-thread verification | done |
| 3. CDC and Compliance | Debezium connector, Kafka topic, idempotent Compliance consumer | done |
| 4. Failure verification | finite retries, DLQ with reason headers, dead-letter topic, restart and redelivery demonstrations | done |
| 5. Measurement | Micrometer, Prometheus, Grafana dashboard, k6 load test, experiment runner and results | done |
| 6. Handoff | architecture notes, demo script, measured results | done |

## Prerequisites

- **Java 25** (Temurin 25 or any JDK 25). The build refuses to run on another major version.
- **Docker Desktop** (or any Docker engine with Compose v2). Used for the local infrastructure, the k6
  load generator, and by the integration tests, which start real PostgreSQL, RabbitMQ and Kafka containers.
- **GNU make** (optional, for the short commands below; on Windows: `winget install ezwinports.make`;
  Git for Windows provides the bash the recipes run in). Everything also works without make: each
  service ships its own Maven Wrapper and every script is plain bash.

Check: `make check`, or `java -version` prints `25.x` and `docker info` succeeds.

## Run it

| Step | Command | Without make |
|------|---------|--------------|
| List all commands | `make` | |
| Start the infrastructure and run the one-shot init containers | `make up` | `docker compose -f infra/compose.yml up -d --wait` then `docker compose -f infra/compose.yml --profile init up --force-recreate kafka-init connect-init` |
| Build and test both services | `make verify` | `user_registry/mvnw -f user_registry/pom.xml verify` and `compliance/mvnw -f compliance/pom.xml verify` |
| Run the Registry service (port 8080), terminal 2 | `make registry` | `user_registry/mvnw -f user_registry/pom.xml spring-boot:run` |
| Run the Compliance service (port 8081), terminal 3 | `make compliance` | `compliance/mvnw -f compliance/pom.xml spring-boot:run` |
| Submit one registration | `make register` | the curl below |
| End-to-end check through every stage | `make smoke` (`COUNT=200 make smoke`) | `COUNT=50 infra/smoke-test.sh` |
| Consumer-group lag | `make lag` | see "Where to look" |
| Stop, keep data / stop and wipe | `make down` / `make clean` | `docker compose -f infra/compose.yml down [-v]` |

On Windows PowerShell use `mvnw.cmd` instead of `mvnw`; the bash scripts need Git Bash.

The init containers must both end with `exited (0)`. Check the connector: both the connector and its
task must be `RUNNING` (the first line alone can be `RUNNING` while the task has failed):

```bash
curl -s localhost:8083/connectors/registry-outbox/status | grep -o '"state":"[A-Z]*"'
# "state":"RUNNING"
# "state":"RUNNING"
```

If the task shows `FAILED`, read the `trace` in the same JSON and restart it with
`curl -X POST 'localhost:8083/connectors/registry-outbox/restart?includeTasks=true&onlyFailed=true'`.
The order in which Registry and the connector start does not matter: the PostgreSQL init script creates
the publication the connector reads before any table exists.

**Submit a registration.** The `Idempotency-Key` header is required and becomes the registration id:

```bash
curl -i -X POST http://localhost:8080/registrations \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: 11111111-1111-1111-1111-111111111111" \
  -d '{"fullName":"Ada Lovelace","email":"ada@example.com","countryCode":"NL"}'
```

Expected: `HTTP/1.1 202` with a receipt such as `{"registrationId":"1111...","status":"ACCEPTED","acceptedAt":"..."}`.
The receipt means the command was confirmed by the broker, not that the user exists yet.

**Watch it complete, hop by hop** (on Git Bash prefix docker commands that mention container paths
with `MSYS_NO_PATHCONV=1`):

```bash
# Registry database: the user and its outbox event
docker compose -f infra/compose.yml exec postgres psql -U registry -d registry \
  -c "SELECT email, registration_id, created_at FROM users" \
  -c "SELECT aggregatetype, aggregateid, type FROM outbox"

# Kafka: the event Debezium published (key = user id, headers id + eventType, value = payload JSON)
docker compose -f infra/compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic outbox.event.user --from-beginning \
  --property print.key=true --property print.headers=true --timeout-ms 10000

# Compliance database: the ledger row and the verdict
docker compose -f infra/compose.yml exec postgres psql -U compliance -d compliance \
  -c "SELECT registration_id, decision, reason, checked_at FROM compliance_results"
```

## Where to look

| What | Where |
|------|-------|
| Grafana dashboard "Registration Case" | <http://localhost:3000/d/registration-case> (no login; provisioned from `infra/monitoring/grafana`) |
| Prometheus | <http://localhost:9090> (targets: registry, compliance, rabbitmq, prometheus) |
| Registry health / metrics | <http://localhost:8080/actuator/health>, `/actuator/prometheus` |
| Compliance health / metrics | <http://localhost:8081/actuator/health>, `/actuator/prometheus` |
| RabbitMQ management UI | <http://localhost:15672> (guest / guest): `registration.commands`, `registration.commands.dlq` |
| Kafka (host) | `localhost:29092`; topics `outbox.event.user`, `outbox.event.user.DLT` |
| Debezium Connect REST | <http://localhost:8083/connectors/registry-outbox/status> |
| Consumer lag | `make lag`, or `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group compliance` inside the kafka container |

## Monitoring

Prometheus scrapes both services (`/actuator/prometheus`, through `host.docker.internal` because they
run on the host) and RabbitMQ's built-in plugin every 5 seconds; Grafana refreshes every 5 seconds and
is provisioned entirely from files in `infra/monitoring` (data source, dashboard). No plugins, no
internet access, no cloud account.

The dashboard follows design section 8: a context row (configured consumers, prefetch, Hikari max,
virtual-thread mode, Java version), then six panels: observed progress, throughput per second (30 s
windows), RabbitMQ ready/unacknowledged/DLQ depth per queue, the Registry database pool (active,
pending, max, acquisition timeouts), p95 latencies from histogram buckets (HTTP 202, registration
completion, compliance completion, both completions measured from the original submission time), and
errors and recovery (503 reasons, failed attempts, dead letters, deduplicated redeliveries), plus a
diagnostics row (platform threads, heap) for the threading-mode comparison. Every panel description
states its measurement boundary. Kafka consumer-group lag is deliberately not graphed: the consumer's
own lag metric disappears when Compliance is stopped, which is exactly when lag matters, so the
dashboard points to the CLI instead.

Metric names and labels are pinned by `RegistryMetricsTest` and `ComplianceMetricsTest`; labels are
bounded (`application`, `outcome`, `queue`), never ids or emails. Application counters reset when a
service restarts; the database counts are the ledger.

## Load test and experiments

The k6 scenario (`load-tests/registrations.js`, run from the pinned `grafana/k6` image, nothing to
install) submits exactly one registration per iteration, 10,000 iterations shared among 50 virtual
users, with keys and emails unique per run and no retries. It is a closed loop: a slower server lowers
the send rate, so it is not a fixed arrival-rate benchmark.

| Command | What it does |
|---------|--------------|
| `make k6` (`ITERATIONS=500 make k6`) | the burst only; prints and saves a summary |
| `make experiment-a` | A: 1 consumer. Starts Registry itself (stop yours first), fires the burst, waits for drain, measures, appends a row to `docs/benchmark-results.md` |
| `make experiment-b` / `make experiment-c` | B: 4 consumers; C: 16 consumers, pool still 4 |
| `make experiment-d` | D (optional): B on platform threads (`platform` profile), same Java 25 |
| `make experiments` | A, B and C back to back |

Each row records submission time, 202 count, HTTP p50/p95 (k6), drain time and committed/s (database
timestamps), registration and compliance completion p95 (Prometheus histograms), Hikari pending max and
acquisition timeouts (Prometheus), and the final distinct counts. A run that did not complete 10,000
iterations is marked `INCOMPLETE`.

### Measured results (this machine, 2026-10-29; full table and method in [docs/benchmark-results.md](docs/benchmark-results.md))

| Experiment | Registry consumers | Virtual threads | Submitted 10,000 in | HTTP p95 | Drain (last commit) | Committed/s | Registration p95 | Compliance done | Hikari pending max |
|------------|-------------------:|-----------------|--------------------:|---------:|--------------------:|------------:|-----------------:|----------------:|-------------------:|
| A | 1 | on | 2.7 s | 27 ms | 51.0 s | 201 | 43.3 s | 52.0 s | 0 |
| B | 4 | on | 2.8 s | 27 ms | 16.4 s | 644 | 12.4 s | 50.7 s | 0 |
| C | 16 | on | 3.3 s | 33 ms | 35.4 s | 297 | 32.7 s | 69.5 s | 12 |
| D | 4 | off (platform) | 2.9 s | 28 ms | 17.2 s | 641 | 12.4 s | 51.5 s | 0 |

What the numbers say: acceptance and completion are different stages (all 10,000 receipts in under 3 s,
completion seconds later); four consumers on four connections are over three times faster than one;
sixteen consumers on the same four connections are slower than four, with twelve of them waiting for a
connection at peak, so the pool, not the worker count, is the bound; and the threading mode changes
nothing measurable at this scale, because the limits are set by intent, not by thread exhaustion.
Compliance completes in about 50 s regardless, bounded by its single consumer committing one offset per
record; that is the next thing to tune, not the Registry.

## API contract

`POST /registrations`

| Input | Rule |
|-------|------|
| header `Idempotency-Key` | required, UUID; reused as the registration id |
| body `fullName` | required, at most 100 characters; internal whitespace collapsed |
| body `email` | required, valid email, at most 254 characters; stored lower-cased |
| body `countryCode` | required, two letters; stored upper-cased |

| Response | Meaning |
|----------|---------|
| `202` + receipt | the command is durably queued (publisher-confirmed and routed) |
| `400` problem detail | malformed input, including unknown JSON fields |
| `503` `reason=overloaded` | admission limit reached; retry shortly |
| `503` `reason=broker_rejected` / `unroutable` / `broker_unavailable` | definitely not queued; retry |
| `503` `reason=confirm_timeout` | outcome unknown; retry with the same `Idempotency-Key` |

Resending the same key with the same data is harmless. The same key with different data, or a new key
with an already registered email, is accepted with `202` and then rejected by the consumer into the
dead-letter queue; the original record is preserved. There is intentionally no status or lookup endpoint.

## Failure handling

| Stage | Transient failure | Permanent failure | Where it ends up |
|-------|-------------------|-------------------|------------------|
| Registry consumer (RabbitMQ) | retried 3 times with backoff 500ms, 1s, 2s | not retried (`AmqpRejectAndDontRequeueException`, unconvertible message) | `registration.commands.dlq`, republished with `x-exception-message`, `x-exception-stacktrace` and `x-original-*` headers; the original delivery is acknowledged |
| Compliance consumer (Kafka) | retried 3 times with backoff 500ms, 1s, 2s | unparseable JSON not retried | `outbox.event.user.DLT`, same partition number, `kafka_dlt-*` headers |

What is guaranteed and what is not: delivery is at-least-once on both brokers; business effects are
idempotent (database constraints in Registry, a ledger with `ON CONFLICT DO NOTHING` in Compliance), so a
redelivery after a crash between commit and acknowledgement changes nothing. There is no global
transaction across database and broker and no exactly-once delivery; there is exactly-once effect.

Retries are finite on purpose, so an outage that outlasts the retry window parks work instead of
losing it. With PostgreSQL down, each Registry delivery is attempted four times, each attempt waiting
up to the Hikari connection timeout (30 s), and is then republished to the DLQ with
`x-exception-message`, `x-exception-stacktrace`, `x-original-exchange` and `x-original-routingKey`
headers. Compliance behaves the same way into the DLT, with headers `kafka_dlt-exception-message`,
`kafka_dlt-exception-fqcn`, `kafka_dlt-original-topic`, `kafka_dlt-original-partition` and
`kafka_dlt-original-offset`; while one record retries, its consumer thread is busy, so with one consumer
all four partitions pause. Nothing replays parked work automatically. To replay, move the DLQ messages
back to `registration.commands` (RabbitMQ management UI "Move messages", or a shovel) or re-produce the
DLT records onto `outbox.event.user`; the fingerprint check and the `processed_events` ledger make a
replay safe even if some of it was already applied.

### Restart and redelivery demonstration

1. Stop the Compliance service (Ctrl+C). Submit registrations: Registry keeps working, users and outbox
   rows appear, `make lag` shows growing lag, `processed_events` stays unchanged.
2. Start Compliance again: it resumes from its committed offsets, lag returns to 0, and
   `processed_events` catches up by exactly the number of registrations submitted meanwhile.
3. Redeliver: with Compliance stopped (the group must have no active member), rewind its offsets

   ```bash
   docker compose -f infra/compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --group compliance --topic outbox.event.user \
     --reset-offsets --to-earliest --execute
   ```

   then start Compliance and observe `compliance duplicate ignored` log lines with unchanged row counts.

The same holds on the Registry side: a crash after the database commit and before the RabbitMQ
acknowledgement redelivers the command, which the fingerprint check turns into `DUPLICATE_IGNORED`.
Kafka, Connect and the replication slot keep their state in named volumes: `docker compose restart
kafka connect` with records in flight resumes without loss.

### Operational notes

- RabbitMQ does not allow changing the arguments of an existing queue. If you change
  `registry.rabbit.queue-max-length` or the dead-letter exchange, delete the queue in the management UI
  (or `make clean`) before restarting, otherwise the declaration fails with `PRECONDITION_FAILED` and the
  old arguments stay in effect.
- Outbox rows are never deleted by the application (design section 5: documented, not built). Debezium
  only needs the rows until it has read them, so a periodic `DELETE FROM outbox WHERE created_at < ...`
  is the production answer; the outbox router ignores deletes, so cleanup produces no events. The
  replication slot `registry_outbox` retains WAL until Debezium confirms it has processed it: while
  Connect is down, and also while the `registry` database is idle (WAL is server-wide, and the
  `compliance` database keeps writing to it). The connector's `heartbeat.interval.ms` makes Debezium
  confirm its position every 10 s even without outbox traffic, which keeps the retained WAL small.
- The publication `registry_outbox_pub` is created by the PostgreSQL init script for every table in the
  Registry schema, including ones Flyway creates later; the connector only emits `public.outbox`.

## Project layout

Two independent Spring Boot applications, each package-by-layer:

```
user_registry/   com.example.registry      Registry API + RabbitMQ consumer (port 8080)
compliance/      com.example.compliance    Compliance Kafka consumer (port 8081)
infra/           compose.yml, PostgreSQL init SQL, RabbitMQ config, Debezium connector, Prometheus + Grafana files, smoke test
load-tests/      k6 scenario, Docker runner, experiment runner, results
docs/            design, architecture, demo script, benchmark results, implementation plans
Makefile         short names for every command above
```

| Package | Responsibility | Registry examples | Compliance examples |
|---------|----------------|-------------------|---------------------|
| `controllers` | HTTP mapping, validation, error boundary | `RegistrationController` | none (no HTTP API) |
| `services` | orchestration, business rules, transaction ownership | `RegistrationIntakeService`, `RegistrationService` | `ComplianceService`, `DemoComplianceRule` |
| `repositories` | plain JDBC SQL | `UserRepository`, `OutboxRepository` | `ProcessedEventRepository`, `ComplianceResultRepository` |
| `models` | persisted records | `User`, `OutboxEvent` | `ProcessedEvent`, `ComplianceResult` |
| `dtos` | HTTP request/response contracts | `RegistrationRequest`, `RegistrationAcceptedResponse` | none |
| `listeners` | thin broker entry points | `RegistrationCommandListener` | `UserRegisteredEventListener` |
| `messaging` | message contracts and publishing | `RegisterUserCommand`, `RegistrationCommandPublisher` | `UserRegisteredEvent` |
| `config` | typed properties, broker topology, metrics | `RabbitConfig`, `RegistryRabbitProperties`, `RegistryMetrics` | `KafkaConfig`, `ComplianceProperties`, `ComplianceMetrics` |
| `exceptions` | explicit failures and HTTP mapping | `ApiExceptionHandler` | none |

## Tests

`make test` (or `mvnw test` in each module) runs everything. Unit tests cover the decision logic
(normalization, fingerprinting, admission and outcome mapping, broker verdict mapping, the demo rule).
Integration tests run the real applications against Testcontainers PostgreSQL, RabbitMQ and Kafka and
cover: the HTTP contract, confirmed publishing, the atomic user + outbox transaction with rollback, all
duplicate and conflict rules including a forced two-consumer race, the end-to-end flow into the database
and the dead-letter queue, finite retries with attempt counts and the DLQ reason headers, the
Compliance ledger + result transaction with rollback, Kafka consumption with a verified redelivery and
a poison record, the metric names the dashboard depends on, and proof that HTTP handlers, Rabbit
consumers and Kafka listeners execute on virtual threads (run with `-Dspring.profiles.active=platform`
to see those proofs fail on platform threads). The real Debezium path is verified by `infra/smoke-test.sh`.

## Key configuration

Registry, `user_registry/src/main/resources/application.yml`:

| Setting | Default | Why |
|---------|---------|-----|
| `spring.threads.virtual.enabled` | `true` | blocking code, cheap waits; `platform` profile turns it off for comparison |
| `spring.datasource.hikari.maximum-pool-size` | `4` | the real write bound; experiment baseline |
| `spring.rabbitmq.listener.simple.concurrency` | `4` | simultaneous registrations in progress (experiments A/B/C: 1/4/16) |
| `spring.rabbitmq.listener.simple.prefetch` | `8` | unacknowledged deliveries per consumer, not parallelism |
| `spring.rabbitmq.listener.simple.retry.*` | 3 retries, 500ms x2, max 5s | finite retry for transient consumer failures |
| `registry.intake.max-in-flight` | `64` | requests allowed to wait for a broker confirmation at once |
| `registry.rabbit.queue-max-length` | `20000` | queue bound; overflow rejects new publishes instead of dropping accepted work |
| `registry.rabbit.confirm-timeout` | `5s` | finite publisher-confirm wait |

Compliance, `compliance/src/main/resources/application.yml`:

| Setting | Default | Why |
|---------|---------|-----|
| `spring.kafka.listener.concurrency` | `1` | one consumer; at most 4 are useful on a 4-partition topic |
| `spring.kafka.listener.ack-mode` | `record` | the offset moves only after each record's transaction committed |
| `compliance.demo-rule.flagged-countries` | `XX,ZZ` | the one deterministic demo rule (user-assigned ISO codes) |

Override any of them with environment variables, for example `SPRING_RABBITMQ_LISTENER_SIMPLE_CONCURRENCY=16`.
