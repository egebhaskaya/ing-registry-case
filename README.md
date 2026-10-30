# Registration case

Two HTTP endpoints use one RabbitMQ queue and the same registration workers:

| Endpoint | HTTP returns when | Success |
|---|---|---|
| `POST /registrations` | RabbitMQ confirms the command was accepted and routed | `202 Accepted` |
| `POST /registrations/sync` | A worker commits the user and outbox event, then replies | `201 Created`, or `200 OK` for an identical replay |

Both use the same validation, idempotency rules and database transaction. Compliance follows the same
outbox -> Debezium -> Kafka path for either endpoint. Sync waits for Registry, not Compliance.

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

- **Concurrency:** workers processing messages. Change the first value; maximum concurrency follows it.
- **Prefetch:** unfinished messages delivered per worker, including the one being processed.
- **Pool size:** database connections available to the workers.
- **Virtual threads:** `true` uses virtual threads; `false` uses platform threads. Worker count is unchanged.

Compliance has its own [application.properties](compliance/src/main/resources/application.properties),
with `spring.kafka.listener.concurrency`, pool size and virtual-thread mode at the top. Its topic has
four partitions, so at most four consumers can process records concurrently.

Restart the affected `make registry` or `make compliance` command after editing. Previously packaged
JARs need rebuilding to include changed resource files. Load tests never override application settings.

## Send 10,000 requests

Keep both services running. Run one load test at a time:

```text
make k6       # /registrations: measure acceptance into the queue
make k6-sync  # /registrations/sync: wait for Registry commits
```

After an async run, let the Rabbit queue drain and Compliance catch up before comparing another run.

Both use the same scenario: 10,000 requests shared by 50 simulated clients, unique keys and emails per
run, and no retries. Each client waits for a response before sending its next request. This is 10,000
requests total, not a fixed requests-per-second rate. If the duration cap stops a run early, its summary
shows fewer than 10,000 completed iterations.

The terminal prints status counts and HTTP latency. Each run saves `<RUN_ID>.summary.json` in the
ignored `load-tests/results/` directory and rebuilds **one `index.html` containing all saved runs**.
Open that HTML directly in your browser and refresh after a run finishes. Compare requests, failures,
latency, duration and client concurrency; expand a run for outcomes, checks and its full JSON summary.
No web server, internet or npm packages are needed. To rebuild the page manually from existing JSON,
run `node load-tests/render-report.mjs`. No extra application processes are started and no benchmark report is
modified. Optional `ITERATIONS` and `VUS` environment variables change only the load generator.

An async summary measures HTTP acceptance. Use the dashboard and `make smoke` to check downstream
processing; a `202` alone does not establish that the user or Compliance result exists.

## Watch one dashboard

Open [Registration Case](http://localhost:3000/d/registration-case): **Last 15 minutes**, refresh **5s**.
No login is required for this local demo. A settings strip shows workers, prefetch, pool maximum,
virtual-thread mode and Compliance workers. Four graphs show:

1. **Processing speed:** HTTP successes, Registry commits and Compliance completions per second.
2. **RabbitMQ backlog:** ready messages and delivered-but-unfinished (`unacked`) messages.
3. **Database connections:** active connections, maximum and workers waiting for one.
4. **Response and completion p95:** HTTP response and end-to-end Registry/Compliance times.

Small summaries show progress and errors. Service counters reset when that service restarts; they
are not authoritative per-run totals. Samples every five seconds can miss short spikes. Both ready
and unacked must reach zero for the Rabbit queue to be fully drained.

For example, increase workers from 4 to 16 with pool 4. Watch connection waiting and throughput.
Next keep workers fixed and change the pool. Change one setting at a time and repeat measurements;
more workers or connections do not guarantee more throughput.

## Request contract

Both endpoints accept the same JSON and require an `Idempotency-Key` UUID:

```json
{"fullName":"Ada Lovelace","email":"ada@example.com","countryCode":"NL"}
```

`make register` sends one example to async. Invalid input returns `400`. An identical key and normalized
body reuse the original registration. Conflicting key/body or email is a business conflict: sync
returns `409`, while async conflicts go through the worker's dead-letter path.

Sync has a finite reply timeout (`spring.rabbitmq.template.reply-timeout=30s`). A timeout means the
outcome is unknown: the queued command may still commit. Retry with the **same key and body**.
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
