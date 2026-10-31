# Registration case

Three HTTP endpoints share registration rules and the transactional outbox:

| Endpoint | HTTP returns when | Success |
|---|---|---|
| `POST /registrations` | RabbitMQ confirms the command was accepted and routed | `202 Accepted` |
| `POST /registrations/batched` | RabbitMQ confirms the command was accepted into the batch queue | `202 Accepted` |
| `POST /registrations/sync` | The HTTP handler directly commits the user and outbox event | `201 Created`, or `200 OK` for an identical replay |

All accept one registration per HTTP request and use the same validation and idempotency rules.
Async commits one registration at a time; batched workers commit groups of registrations together.
Both queues use the same RabbitMQ broker. Sync calls the processor directly and waits for Registry.
Each new user has its own outbox event, and all three paths use the same outbox -> Debezium -> Kafka
-> Compliance flow. No HTTP endpoint waits for Compliance.

## Start

Install Java 25, Docker with Compose, GNU make, Bash (Git for Windows supplies Bash on Windows), and
Node.js for generating the local HTML run report (no npm packages needed).
Run from the repository root:

```text
make up          # Start infrastructure and register the Debezium connector
make registry    # Leave running in its own terminal (port 8080)
make compliance  # Leave running in another terminal (port 8081)
```

The service commands use each project's Maven Wrapper; no separate Maven installation or packaging
step is needed. `make build` creates runnable JARs when needed. Init containers should finish with exit
code 0. `make connector` shows the Debezium connector and task; both should be `RUNNING`.

## Change concurrency

Edit [Registry application.properties](user_registry/src/main/resources/application.properties):

```properties
spring.rabbitmq.listener.simple.concurrency=4
spring.rabbitmq.listener.simple.max-concurrency=${spring.rabbitmq.listener.simple.concurrency}
spring.rabbitmq.listener.simple.prefetch=8
spring.datasource.hikari.maximum-pool-size=4
spring.threads.virtual.enabled=true
```

- **Concurrency:** Rabbit workers per queue. Maximum concurrency follows the first value.
- **Prefetch:** unfinished async messages delivered per Rabbit worker, including the one being processed.
- **Pool size:** database connections shared by Rabbit workers and direct synchronous requests.
- **Virtual threads:** `true` uses virtual threads; `false` uses platform threads. Rabbit worker count is unchanged.

Rabbit concurrency and prefetch do not limit sync requests. All routes share the HTTP intake limit
(`registry.intake.max-in-flight=64`): queued requests hold a slot until broker confirmation, sync until its
transaction completes. Sync processing is limited by HTTP admission and the shared database pool.

For batched processing, edit these values in the same properties file:

```properties
registry.batch.size=100
registry.batch.max-wait=100ms
```

Each batch worker collects up to 100 registrations with a 100 ms collection target. Partial batches
flush on a receive or gathering timeout. Spring checks gathering time between receive polls, so polling
can add one extra receive wait (another 100 ms with these defaults). This is a collection target,
not a database completion deadline. The worker writes users and their outbox events together with JDBC
batching in one transaction; the queue can keep receiving work while previous batches are processed.
The batch size is separate from queue capacity and Rabbit prefetch.
The batch queue uses an effective prefetch of `max(configured prefetch, batch size)` so a worker can
collect a full batch: prefetch 8 and batch size 100 gives batch workers a prefetch of 100. Ordinary
async workers keep the configured prefetch of 8.

The configured Rabbit concurrency applies to each queue: 4 means up to 4 ordinary workers and 4 batch
workers if both receive traffic. They share the Registry connection pool. Compare one mode at a time.
If a batch write fails, its transaction rolls back. The worker isolates conflicts; commands needing
ordinary retries or dead-letter handling are forwarded with broker confirmation to the original queue.
Redelivery uses the same registration keys, preserving idempotency.

For a 10,000-registration test, this means many small batches; the worker does not wait for 10,000
messages to accumulate. Larger batches can reduce database round trips and commits, but add collection
delay and increase the amount of work rolled back on failure. Start with 100, then compare 10 and 250
at the same arrival rate, worker count and pool size. Batching can improve throughput without reducing
each registration's total waiting time. One bad registration must not discard the other registrations.

Compliance has its own [application.properties](compliance/src/main/resources/application.properties),
with `spring.kafka.listener.concurrency`, pool size and virtual-thread mode at the top. Its topic has
four partitions, so at most four consumers can process records concurrently.

Restart the affected `make registry` or `make compliance` command after editing. Previously packaged
JARs need rebuilding to include changed resource files. Load tests never override application settings.

## Run load tests

Keep both services running. Run one load test at a time:

```text
make k6          # /registrations: accept into the queue; individual DB commits
make k6-sync     # /registrations/sync: wait for Registry commits
make k6-batched  # /registrations/batched: accept into the queue; grouped DB commits
```

After either queued run, let its Rabbit queue drain and Compliance catch up before comparing another run.

By default, all three send 10,000 requests shared by 50 simulated clients, with unique keys and emails per
run, and no retries. Each client waits for a response before sending its next request. This is 10,000
requests total, not a fixed requests-per-second rate. If the duration cap stops a run early, its summary
shows fewer than 10,000 completed iterations.

For steady traffic until you stop it, add `cont` and your desired requests per second:

```text
make k6 cont 200
make k6-sync cont 200
make k6-batched cont 200
```

The number is changeable: `cont 50` targets 50 requests/s; `cont 1000` targets 1,000 requests/s,
independent of response time. The longer `CONTINUOUS=1 RATE=200` form also works.
Run one endpoint at a time. Press **Ctrl+C once** to stop and allow the JSON
summary and shared HTML report to be written. An interrupted exit status from k6/make is expected;
closing the terminal abruptly or pressing Ctrl+C again can prevent report generation. Already accepted
queued registrations continue processing after the load generator stops.

Continuous mode preallocates 50 k6 VUs and can use up to 200. `VUS` and `MAX_VUS` adjust those client
limits. If they cannot sustain the requested rate, `dropped_iterations` reports scheduled requests
that were never sent; these are separate from HTTP failures. Both the terminal and HTML report show
the target rate, observed HTTP rate and dropped iterations. These settings do not change application
threads, consumers or connections. k6 requires a finite duration, so continuous mode has a one-year
ceiling; optional `DURATION=30s` makes a short rate-controlled run.

The terminal prints status counts and HTTP latency. Each run saves `<RUN_ID>.summary.json` in the
ignored `load-tests/results/` directory and rebuilds **one `index.html` containing all saved runs**.
Open that HTML directly in your browser and refresh after a run finishes. Compare requests, failures,
latency, duration and client concurrency; expand a run for outcomes, checks and its full JSON summary.
No web server, internet or npm packages are needed. To rebuild the page manually from existing JSON,
run `node load-tests/render-report.mjs`. No extra application processes are started and no benchmark report is
modified. Optional `ITERATIONS` and `VUS` settings adjust the default burst test.

Async and batched summaries measure HTTP acceptance, not database or batch completion. Use the
dashboard for processing throughput and wait for both queues and Compliance to catch up. `make smoke`
checks the original async path end to end; a `202` alone does not establish that the user or Compliance
result exists. Compare modes with the same total registrations or the same continuous arrival rate.

## Watch one dashboard

Open [Registration Case](http://localhost:3000/d/registration-case): **Last 5 minutes**, refresh **1s**.
No login is required for this local demo. A settings strip shows async Rabbit workers, prefetch, pool maximum,
virtual-thread mode and Compliance workers. The five core graphs show:

1. **Processing speed:** queued acceptance, sync completion decisions, Registry commits and Compliance
   completions per second, averaged over 10 seconds. These are server decisions; k6 records client responses.
2. **Queued RabbitMQ backlog:** ready messages and delivered-but-unfinished (`unacked`) messages, with current
   and peak values in the legend. Missing queue telemetry stays missing instead of looking empty.
3. **Processing and database:** workers in progress across direct HTTP and Rabbit, borrowed connections,
   pool maximum and callers waiting for a connection. One batch worker handles a group of registrations.
   Processing includes database waiting; this is not CPU parallelism.
4. **HTTP response p95 (milliseconds):** async and batched return after Rabbit acceptance; sync returns
   after Registry commit. These measure different response promises.
5. **Registry processing p95 (milliseconds):** direct sync, async workers and batched workers use the same boundary:
   processor entry to committed user/outbox result, including database connection waiting. Only new
   successful registrations are timed. HTTP admission/normalization, Rabbit queue waiting, retry
   backoff and downstream Kafka/Compliance are excluded. Batch collection waiting is also excluded.
   Successful bulk commits record the full group-processing duration for each new registration,
   without dividing by batch size. The batched line also includes individual fallback commits;
   commands forwarded to the ordinary queue for retries can appear under async processing.
   HTTP threads and Rabbit workers can still have different concurrency, so compare settings too.

The processing graph uses rolling 30-second histogram estimates, not whole-run duration. It becomes
available after the updated service starts and receives work; earlier runs are not backfilled. The
existing end-to-end Registry and Compliance timers remain available in Prometheus.

Small summaries show progress and errors. Service counters reset when that service restarts; they
are not authoritative per-run totals. Application counters and completion timers exist from startup,
including their zero baseline. Prometheus scrapes every second, but RabbitMQ updates queue statistics
every five seconds by default: a short test can finish between broker updates and show no queue spike.
Queue depth measures unfinished work, not how many messages passed through the queue. Use the HTML
report for exact k6 counts and HTTP statistics. Both ready and unacked must reach zero for each Rabbit queue
to be fully drained. A missing latency estimate means no observations in that window, not zero latency.
Before the first load burst after startup, let the dashboard load its configuration and initial zero
samples. This gives rate calculations a baseline before requests arrive.

Below the core graphs, **Operational checks** shows Registry/Compliance/RabbitMQ scrape reachability,
current Registry DLQ depth (`ready + unacked`, red when nonzero), Kafka fetch-position lag for
`outbox.event.user`, and recent HTTP 5xx / Registry failed-attempt rates. UP means the metrics endpoint
is reachable, not that the full pipeline works. Missing telemetry is unknown, not zero. Error rates
use a recent window and account for counter resets; the lifetime summaries still reset on service restart.

Kafka fetch-position lag is not committed-offset backlog or proof that Compliance finished. Use
`make lag` for committed consumer-group lag and `make connector` for CDC status. DLQ depth is the
current message count, distinct from lifetime dead-letter publications. Expand **JVM diagnostics**
when needed for heap utilization and GC pause time as percentages; these use existing application
metrics and require no extra exporter.

For example, increase async Rabbit workers from 4 to 16 with pool 4 during `make k6`. Watch connection waiting and throughput.
Next keep workers fixed and change the pool. Change one setting at a time and repeat measurements;
more workers or connections do not guarantee more throughput.

## Request contract

All three endpoints accept the same single-registration JSON and require an `Idempotency-Key` UUID:

```json
{"fullName":"Ada Lovelace","email":"ada@example.com","countryCode":"NL"}
```

`make register` sends one example to async. Invalid input returns `400`. An identical key and normalized
body reuse the original registration. Conflicting key/body or email is a business conflict: sync
returns `409`, while async and batched conflicts go through the worker's dead-letter path.

`POST /registrations/batched` returns the same acceptance response as async after broker confirmation;
`202` does not mean its database batch has committed. It does not accept an array. Retry a lost or
uncertain response with the same key and body: identical replays create no additional user or event.

Sync publishes no Rabbit message and has no broker reply timeout. If a response is lost after a
commit, retry with the **same key and body** to retrieve the original registration safely.
See [the detailed sync flow](docs/synchronous-registration.md).

## Useful commands

| Command | Purpose |
|---|---|
| `make help` | List commands |
| `make test` / `make verify` | Correctness tests / complete Maven verification |
| `make smoke` | Verify the real Rabbit -> Registry -> CDC -> Kafka -> Compliance path |
| `make ps` | Show containers |
| `make connector` / `make lag` | Debezium status / Compliance Kafka lag |
| `make down` | Stop infrastructure, preserve data |
| `make clean` | Stop infrastructure and delete its data volumes |

RabbitMQ management: [localhost:15672](http://localhost:15672) (`guest` / `guest`). Prometheus:
[localhost:9090](http://localhost:9090). Application health: ports 8080/8081, `/actuator/health`.
This is a local demo with development credentials and single-node infrastructure.

`user_registry` contains HTTP, Rabbit workers and user/outbox persistence; `compliance` contains Kafka
consumption and checks; `infra` contains Compose/monitoring; `load-tests` contains the shared k6 scenario.
See [architecture](docs/architecture.md) for transaction and failure boundaries.
