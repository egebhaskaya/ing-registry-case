# Benchmark results

Measured behaviour of the 10,000-registration burst on one machine (docs/design.md sections 6 and 7).
These are demonstration numbers for the consumer-concurrency comparison, not capacity claims and not
recommended production sizes.

## Environment

| Item | Value |
|------|-------|
| CPU | Intel Core i7-12700F, 12 cores / 20 threads |
| RAM | 32 GB |
| OS | Windows 11 Pro |
| Java | Temurin 25.0.5+8 (both services, same for every run) |
| Spring Boot | 4.1.2 |
| Containers | Docker Desktop 29.6.1 / Compose v5.3.0: postgres:17.11, rabbitmq:4.3.2-management, apache/kafka:4.2.2, quay.io/debezium/connect:3.7.0.Final, prom/prometheus:v3.15.1, grafana/grafana:13.3.0 |
| Load generator | grafana/k6:2.3.0 in Docker, `shared-iterations`, 10,000 iterations, 50 VUs, no retries; reaches the API via host.docker.internal |
| Registry | one instance from `java -jar`, Hikari max 4, prefetch 8, intake limit 64, publisher-confirm timeout 5s |
| Compliance | one instance, one consumer, 4 partitions |
| Everything | on the same machine: load generator, both services, all containers. CPU is shared; the numbers include that interference. |

## How each column is measured

| Column | Source | Boundary |
|--------|--------|----------|
| iterations / accepted / 503 / other | k6 summary | one POST per iteration; `accepted` = HTTP 202 |
| submit s | k6 | wall time from first to last request |
| http p50 / p95 ms | k6 `http_req_duration` | request sent to 202 received (includes publisher confirm) |
| drain s | Registry DB | `max(users.created_at)` minus the moment k6 started: last registration committed |
| committed/s | Registry DB | accepted / (`max(created_at)` - `min(created_at)`) |
| registration p95 | Prometheus histogram | `registry_registration_completion_seconds`: submittedAt to user+outbox commit, 95th percentile over the run window |
| compliance s | Compliance DB | `max(compliance_results.checked_at)` minus k6 start: last compliance result committed |
| compliance p95 | Prometheus histogram | `compliance_completion_seconds`: submittedAt to result commit |
| hikari pending max | Prometheus | `max_over_time(hikaricp_connections_pending)` sampled every 5s: waits shorter than a scrape can be missed |
| hikari timeouts | Prometheus | `increase(hikaricp_connections_timeout_total)` over the run |
| users / outbox / results | both DBs | final distinct counts for the run (the ledger; counters are not) |

The run is closed-loop: 50 VUs each wait for a response before sending the next request, so a slower
server lowers the send rate. Do not read `submit s` as a fixed arrival rate.

## Runs

Rows are appended by `load-tests/experiment.sh`.

| Exp | Run id | Consumers | Pool max | Prefetch | Virtual threads | Java | Iterations | Accepted | 503 | Other | Submit s | HTTP p50 ms | HTTP p95 ms | Drain s | Committed/s | Registration p95 s | Compliance s | Compliance p95 s | Hikari pending max | Hikari timeouts | Users / outbox / results | Status |
|-----|--------|-----------|----------|----------|-----------------|------|------------|----------|-----|-------|----------|-------------|-------------|---------|-------------|--------------------|--------------|------------------|--------------------|-----------------|--------------------------|--------|
| A | A-20261029-163144 | 1 | 4 | 8 | on | 25.0.5 | 10000 | 10000 | 0 | 0 | 2.7 | 10.6 | 26.6 | 51.0 | 201.3 | 43.3 | 52.0 | 43.4 | 0.0 | 0.0 | 10000 / 10000 / 10000 | COMPLETE |
| B | B-20261029-163305 | 4 | 4 | 8 | on | 25.0.5 | 10000 | 10000 | 0 | 0 | 2.8 | 11.2 | 26.9 | 16.4 | 644.0 | 12.4 | 50.7 | 45.1 | 0.0 | 0.0 | 10000 / 10000 / 10000 | COMPLETE |
| C | C-20261029-163416 | 16 | 4 | 8 | on | 25.0.5 | 10000 | 10000 | 0 | 0 | 3.3 | 13.2 | 33.0 | 35.4 | 296.6 | 32.7 | 69.5 | 60.6 | 12.0 | 0.0 | 10000 / 10000 / 10000 | COMPLETE |
| D | D-20261029-163549 | 4 | 4 | 8 | off | 25.0.5 | 10000 | 10000 | 0 | 0 | 2.9 | 11.8 | 28.2 | 17.2 | 640.6 | 12.4 | 51.5 | 43.3 | 0.0 | 0.0 | 10000 / 10000 / 10000 | COMPLETE |

## Per-run JVM diagnostics (Registry, from Prometheus over each run's window)

| Exp | Threading | Peak live platform threads | Peak heap used |
|-----|-----------|---------------------------:|---------------:|
| A | virtual | 125 | 204 MB (first run after start; includes warm-up garbage) |
| B | virtual | 123 | 63 MB |
| C | virtual | 123 | 77 MB |
| D | platform | 148 | 88 MB |

`jvm_threads_live_threads` counts platform threads only. On virtual threads the HTTP handlers and the
Rabbit consumers do not appear in it; in D they do, hence the higher count. Sixteen consumers (C) did
not raise the platform-thread count: each consumer is a virtual thread.

## Observations

1. **Acceptance and completion are different stages.** Every run accepted all 10,000 registrations in
   under 3.5 s with an HTTP p95 under 35 ms, while the last registration was committed 16 to 51 s later
   and the last compliance result 51 to 70 s later. The receipt is fast because it only promises that the
   command is queued; the work is bounded by the consumers and the pool, not by the API.

2. **A to B: four consumers on four connections are 3.1 times faster than one** (201 to 644 committed/s,
   drain 51 to 16 s). The registration p95 drops from 43 s to 12 s because the queue drains faster; the
   HTTP p95 does not move, because the API never waits for the database.

3. **B to C: sixteen consumers on the same four connections are slower than four** (644 to 297
   committed/s, drain 16 to 35 s), and `hikaricp_connections_pending` peaked at 12: twelve consumers
   waiting for a connection at any time. Twelve extra workers add contention (connection hand-offs, lock
   waits on the unique indexes) and no capacity. More workers are not faster when the bound is the pool.
   Prefetch also matters here: 16 consumers x 8 prefetch = 128 deliveries parked in memory while four
   can actually run.

4. **B to D: the threading mode changed nothing measurable** (644 versus 641 committed/s, HTTP p95 27
   versus 28 ms). The design predicted this: virtual threads make waiting cheap, they do not add
   connections or make SQL faster. The visible difference is in the diagnostics row: about 25 more live
   platform threads in D. At 50 virtual users this stays far from the point where platform threads run
   out; a negligible difference is a valid result (design section 6).

5. **Compliance is the slowest stage in every run** (about 50 s for 10,000 events in A, B and D; 70 s in
   C where Registry commits were spread out longer). Its single consumer commits one Kafka offset per
   record (`ack-mode: record`) and runs one small transaction per event, roughly 200 events/s. It is
   independent of Registry's consumer count by design (design section 5: more Compliance consumers do
   not speed up Registry writes, and vice versa). The next tuning step would be `spring.kafka.listener.concurrency`
   up to the four partitions, or batch acknowledgement, both measured rather than assumed.

6. **No errors anywhere:** no 503s, no retries, no dead letters, no Hikari acquisition timeouts in any run.
   The intake limit (64) was never reached with 50 virtual users; a higher VU count or a lower limit would
   be needed to demonstrate the `overloaded` path under load.

7. **Caveats.** Everything ran on one machine: k6 inside Docker, both services, PostgreSQL, RabbitMQ,
   Kafka, Debezium, Prometheus and Grafana share 12 cores. The first run (A) also carried JVM warm-up
   (visible in its heap peak). Hikari `pending` is sampled every 5 s and can miss shorter waits. k6 is
   closed-loop: 50 virtual users each wait for a reply, so a slower API would lower the send rate.
