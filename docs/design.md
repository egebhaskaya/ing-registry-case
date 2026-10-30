# Design decisions

A small Java 25 / Spring Boot case for exploring queues, connection limits and virtual threads with
10,000 registration requests. The [README](../README.md) describes current operation.

- **One processing flow:** both HTTP routes share RabbitMQ, workers, the registration transaction and
  downstream Compliance. Only HTTP response timing differs.
- **Visible controls:** consumer count, prefetch, pool size and virtual-thread mode are at the top of
  `application.properties`. Test scripts do not override application settings.
- **Plain JDBC:** repositories expose the SQL used in each transaction.
- **Transactional outbox:** user and event commit atomically; Debezium reads committed changes and
  publishes to Kafka, avoiding an application database/broker dual write.
- **Idempotency:** registration ids, normalized payload fingerprints, unique emails and the Compliance
  event ledger protect redelivery. Duplicate resolution runs in a fresh transaction after rollback.
- **Bounded resources:** HTTP intake, queue depth, workers, connections, retries and sync waits have
  finite limits. Virtual threads change waiting cost, not these limits.
- **One dashboard:** settings, four graphs (speed, backlog, pool, latency), and small progress/error
  summaries. Five-second samples can miss short events; database counts verify durable effects.
- **One load-test scenario:** `make k6` and `make k6-sync` each send 10,000 requests with the same
  closed-loop client model. Correctness tests cover rollback, duplicates, redelivery and failures.

The local stack is single-node PostgreSQL, RabbitMQ, Kafka, Debezium Connect, Prometheus and Grafana.
Authentication, external compliance providers, high availability and outbox cleanup are outside this
demo. See [architecture](architecture.md) and [the sync flow](synchronous-registration.md).
