# Architecture

How the registration burst is processed, which resource bounds the system, and what is and is not
guaranteed. The reasoning behind the choices is in [design.md](design.md); this document describes what
was built.

## The flow

```
                 1 HTTP POST /registrations            2 publish + confirm
  client  ───────────────────────────────►  Registry API  ─────────────────►  RabbitMQ  registration.commands
    ▲                                           │                                   │
    │ 3  202 Accepted {registrationId}          │ admission semaphore (64)          │ 4 consumers, prefetch 8
    └───────────────────────────────────────────┘                                   ▼
                                                                          Registry consumer
                                                                                    │ 4 one transaction
                                                                                    ▼
                                                               PostgreSQL registry: users + outbox
                                                                                    │ 5 logical replication (WAL)
                                                                                    ▼
                                                         Debezium Connect: outbox event router
                                                                                    │ 6 key = user id, 4 partitions
                                                                                    ▼
                                                               Kafka outbox.event.user
                                                                                    │ 7 one consumer group
                                                                                    ▼
                                                                   Compliance consumer
                                                                                    │ 8 one transaction
                                                                                    ▼
                                                 PostgreSQL compliance: processed_events + compliance_results
```

1. The client sends `fullName`, `email`, `countryCode` and an `Idempotency-Key` (UUID). Validation
   and normalization happen here; a malformed request is a `400` and never reaches a broker.
2. The API publishes a `RegisterUserCommand` (registration id, payload fingerprint, normalized payload,
   submission time) and blocks until RabbitMQ confirms it. The wait happens on a virtual thread, so it
   costs no platform thread. At most 64 requests may be waiting for a confirmation at once; the 65th
   waits 200 ms for a slot and is then refused with `503 overloaded`.
3. `202` means exactly "the command is durably queued". A nack is `503 broker_rejected`, an unroutable
   message `503 unroutable`, an unreachable broker `503 broker_unavailable`, and a missing confirmation
   `503 confirm_timeout` with the instruction to retry with the same key, because the outcome is unknown.
4. Four consumers (configurable) each take one command at a time, call the transactional service and
   acknowledge only after the commit. The transaction inserts the user and its `UserRegistered` outbox
   row. Duplicates are decided by the database: the same registration id with the same fingerprint is
   acknowledged and ignored, the same id with different data or a second id with an existing email is
   dead-lettered with a reason header; transient errors are retried three times with backoff first.
5. Debezium reads the committed outbox inserts from the PostgreSQL write-ahead log through a
   replication slot. A rolled-back transaction never reaches the log, so it never becomes an event.
6. The outbox event router turns each row into a Kafka record: topic from `aggregatetype`, key from
   `aggregateid` (the user id), the JSON payload as the value, the event id as a header.
7. Compliance consumes with one consumer (configurable up to the four partitions), parses the JSON,
   and commits its Kafka offset only after step 8 succeeded.
8. One transaction inserts the ledger row (`ON CONFLICT DO NOTHING` on the event id) and, if the event
   is new, the demo verdict. A redelivered event finds its ledger row and changes nothing.

`POST /registrations/sync` is a second entry point: validation and normalization → shared processing
permit → the same user + outbox transaction → committed user in `201`, or the existing user in `200`
for a same-key/same-payload replay. Business conflicts return `409` during the HTTP request. It does
not publish a Rabbit command; Debezium and Compliance continue from the same outbox normally.

`RegistrationProcessor` acquires the shared execution permit before invoking the transactional bean,
and holds it until commit and any fresh-transaction duplicate resolution finish. Rabbit consumers
wait for a permit; synchronous HTTP has at most 64 waiters and a 200 ms admission timeout, then `503`
with `reason=sync_overloaded` and `Retry-After: 1`. The limit is per application instance; replicas need
a combined database connection budget. See [the synchronous experiments](synchronous-registration.md).

## Services and packages

Both services are Spring Boot 4.1.2 on Java 25 with the same package-by-layer layout
(`controllers`, `services`, `repositories`, `models`, `dtos`, `listeners`, `messaging`, `config`,
`exceptions`); see the table in the [README](../README.md#project-layout).

| Concern | Registry (`user_registry`) | Compliance (`compliance`) |
|---------|----------------------------|---------------------------|
| Entry points | `RegistrationController`, `SynchronousRegistrationController` (HTTP), `RegistrationCommandListener` (RabbitMQ) | `UserRegisteredEventListener` (Kafka) |
| Orchestration | `RegistrationIntakeService` (publish), `SynchronousRegistrationService`, shared `RegistrationProcessor` (capacity, duplicate resolution, metrics) | none needed |
| Transactions | `RegistrationService.registerWithResult` / `resolveDuplicateWithResult` (legacy outcome-only wrappers retained) | `ComplianceService.process` |
| Persistence | `UserRepository`, `OutboxRepository` (JdbcTemplate) | `ProcessedEventRepository`, `ComplianceResultRepository` |
| Contracts | `RegisterUserCommand`, `UserRegisteredEvent` (JSON) | `UserRegisteredEvent` (same JSON, own record) |
| Topology | `RabbitConfig` (exchange, queue, DLX/DLQ, retry, recoverer) | `KafkaConfig` (topics, error handler, DLT) |
| Schema | Flyway `V1__users_and_outbox.sql` | Flyway `V1__compliance.sql` |

Data access is plain SQL through `JdbcTemplate` on purpose: every statement in a transaction is visible
in the code, which is what a throughput demonstration needs to explain. There is no JPA, no shared
library between the services (the event JSON is the contract), and no interface/implementation pairs.

## What bounds the system

| Bound | Default | What it limits | What it does not limit |
|-------|---------|----------------|------------------------|
| Intake admission semaphore | 64 | requests simultaneously waiting for a publisher confirmation | queued commands, database work |
| Shared processing permits | 4 | simultaneous Registry executions across Rabbit and HTTP, through commit | connections available from the pool |
| HTTP waiting slots + admission timeout | 64 / 200 ms | number and duration of HTTP requests waiting for a processing permit | durable storage; overload returns `503` |
| RabbitMQ queue `x-max-length` + `reject-publish` | 20,000 | buffered commands; beyond it the broker nacks and the API answers `503` | nothing already accepted is dropped |
| Consumer concurrency | 4 | registrations being processed at the same time | the number of connections they can get |
| Prefetch | 8 per consumer | delivered-but-unacknowledged messages per consumer | processing parallelism |
| HikariCP maximum pool size | 4 | database connections of the instance; the real write bound | HTTP connections, queue depth |
| Kafka partitions | 4 | useful Compliance consumers (one per partition at most) | Registry throughput |
| Compliance listener concurrency | 1 | events processed at the same time | Registry throughput |

Virtual threads are an execution mechanism, not a bound: they make waiting (for a confirmation, a
connection, a lock) cheap, so the limits above can be set by intent instead of by thread exhaustion.
Experiments A/B/C set both consumer and processing limits to 1/4/16 with a pool of 4, showing the difference between adding workers and
adding capacity: sixteen consumers on four connections mostly wait, which the Hikari `pending` series
makes visible.

## Guarantees

| Property | Status | Mechanism |
|----------|--------|-----------|
| A `202` is backed by a durable, routed message | yes | publisher confirms + mandatory returns; durable queue, persistent messages |
| User and outbox event are created together or not at all | yes | one database transaction |
| At most one user per registration id and per email | yes | unique constraints enforced by PostgreSQL, also under concurrent consumers |
| A redelivered command has no second effect | yes | fingerprint comparison; `DUPLICATE_IGNORED` |
| Every committed outbox row becomes a Kafka record | yes, at least once | Debezium reads committed WAL; Connect offsets are stored in Kafka |
| A redelivered event has no second effect | yes | ledger row on the event id, `ON CONFLICT DO NOTHING` |
| Exactly-once delivery | no | both brokers are at-least-once; the effect is idempotent instead |
| A global transaction across database and broker | no | commit-then-acknowledge on both sides; the gap is covered by idempotency |
| Ordering | per user id on Kafka (same key, same partition) | not across users; not relevant for registration |
| High availability | no | single-node infrastructure for the local case |

## Failure paths

| Failure | Behaviour |
|---------|-----------|
| Broker down at request time | `503 broker_unavailable` after a 2 s connection timeout; the admission slot is released |
| Confirmation timeout | `503 confirm_timeout`; the message may be queued; the client retries with the same key safely |
| Queue full | broker nack, `503 broker_rejected`; accepted work is never evicted |
| Transient consumer error (database hiccup) | retried 3 times (500 ms, 1 s, 2 s), then republished to the DLQ with `x-exception-message` |
| Permanent conflict | one attempt, then the DLQ with the reason; original record preserved |
| Crash between commit and acknowledgement | redelivery, recognized as duplicate |
| Compliance down | lag accumulates in Kafka; Registry unaffected; catch-up on restart from the committed offsets |
| Poison event (unparseable) | straight to `outbox.event.user.DLT`, same partition, no retries |
| Debezium Connect down | WAL retained by the replication slot; events published when it returns |

## Alternatives that were considered

- **Synchronous `201 Created`**: now available as `/registrations/sync` for comparison. A shared
  processing gate and bounded HTTP admission protect the database; excess synchronous demand is
  rejected instead of stored durably. The original async route still demonstrates burst buffering.
- **JPA instead of JdbcTemplate**: less code for a larger domain model, but hidden SQL and extra
  caching behaviour in a throughput demonstration; two tables and five queries do not justify it.
- **An application Kafka producer or a polling outbox publisher** instead of Debezium: simpler to run,
  but either a dual write (not atomic) or a second component polling the table. CDC reads what was
  committed, nothing else.
- **Kafka for the command queue too**: possible, but the command stage needs per-message
  acknowledgement, dead-lettering and bounded prefetch, which is what a classic queue does natively.

## Production considerations (documented, not built)

- Outbox rows are never deleted by the application; a periodic delete of rows older than the
  connector's position is the usual answer. The replication slot retains WAL while Connect is down.
- Across replicas, total database connections are every instance's pool plus other services; the pool
  size here is a demonstration value.
- Secrets are plain values in `application.yml` and the compose file for the local case.
- No authentication, no status/lookup API, no client application: out of scope by design.
