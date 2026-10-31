# Design decisions

A small Java 25 / Spring Boot case for exploring queues, connection limits and virtual threads with
10,000 registration requests. The [README](../README.md) describes current operation.

- **One business flow:** async Rabbit workers, batched Rabbit workers and direct sync HTTP requests
  share registration rules, user/outbox atomicity and downstream Compliance. Async commits one
  registration per transaction; batched uses JDBC batches and commits groups. Sync bypasses RabbitMQ.
- **Bounded batching:** a separate queue on the same broker lets the third endpoint collect up to
  100 commands with a 100 ms collection target and partial-batch timeout. Both values are properties.
  The 10,000-request workload is independent of batch size and queue capacity. Batching trades
  collection delay and larger rollback scope for fewer database round trips and commits.
- **Visible controls:** async consumer count, prefetch, shared pool size and virtual-thread mode are at the top of
  `application.properties`. Test scripts do not override application settings.
- **One-command startup:** `make up` builds and runs both applications and their infrastructure in
  Docker. Topics initialize before Compliance; connector registration follows Registry migrations.
  Host Java 25 is needed only for optional host development and Maven tests.
- **Plain JDBC:** repositories expose the SQL used in each transaction.
- **Transactional outbox:** user and event commit atomically; Debezium reads committed changes and
  publishes to Kafka, avoiding an application database/broker dual write.
- **Idempotency:** registration ids, normalized payload fingerprints, unique emails and the Compliance
  event ledger protect redelivery. Duplicate resolution runs in a fresh transaction after rollback.
- **Bounded resources:** HTTP admission, async queue depth, Rabbit workers, connections and broker
  retries have finite limits. Sync holds its HTTP intake slot through commit and shares the pool;
  it is not limited by Rabbit consumer count. Virtual threads change waiting cost, not these limits.
- **One dashboard:** settings, five core graphs (speed, backlog, pool, HTTP response latency, Registry
  processing latency), and small progress/error summaries. Processing is measured from shared
  processor entry through commit for new successful registrations, including connection waiting and
  excluding admission, Rabbit queue waiting and downstream work. The modes share the timing boundary
  but can have different concurrency. Prometheus scrapes every second, while RabbitMQ queue statistics
  update every five seconds by default; short queue spikes can be missed. Database counts verify durable effects.
  Operational checks follow the core graphs; scrape reachability and Kafka fetch-position lag do not
  establish end-to-end completion. Collapsed JVM diagnostics reuse existing application metrics.
- **One load-test script:** `make k6`, `make k6-sync` and `make k6-batched` default to 10,000 closed-loop requests.
  Adding `cont 200` instead schedules 200 arrivals/s until Ctrl+C, with dropped arrivals and
  interruption state included in the same report. Correctness tests cover rollback, duplicates,
  redelivery and failures.

The local stack is single-node PostgreSQL, RabbitMQ, Kafka, Debezium Connect, Prometheus and Grafana.
Authentication, external compliance providers, high availability and outbox cleanup are outside this
demo. See [architecture](architecture.md) and [the sync flow](synchronous-registration.md).
