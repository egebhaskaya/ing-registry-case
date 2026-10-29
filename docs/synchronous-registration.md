# Synchronous registration and bounded parallelism

The second endpoint, `POST /registrations/sync`, performs the same user + outbox transaction as the
Rabbit consumer. It returns after the transaction commits. Compliance still runs later through CDC
and Kafka. The original `POST /registrations` remains a publisher-confirmed `202` intake endpoint.

| Outcome | Synchronous HTTP response |
|---|---|
| New user + outbox committed | `201`, persisted user data and `status=REGISTERED` |
| Same key and normalized body already committed | `200`, the same persisted user; no extra outbox event |
| Same key with different normalized data | `409` |
| Existing email submitted under another key | `409` |
| Invalid input | `400` |
| No execution slot within bounded admission | `503`, `reason=sync_overloaded`, `Retry-After` |

Both routes use the same idempotency scope: the `Idempotency-Key` is the registration id, the normalized
payload fingerprint detects conflicting reuse, and database uniqueness resolves concurrent insert
races. Retrying with the same key is safe even if an HTTP response was lost after commit. An earlier
async `202` does not reserve a key/body in the database: if different payloads race across routes,
whichever transaction commits first establishes the registration, and the other conflicts.

## Capacity controls

| Setting | Default | Meaning |
|---|---|---|
| `registry.processing.max-concurrency` | `4` | Shared execution permits for synchronous HTTP and Rabbit processing, per Registry instance |
| `registry.processing.max-waiting-requests` | `64` | Maximum HTTP requests waiting for a permit |
| `registry.processing.admission-wait` | `200ms` | Maximum HTTP wait for a permit |
| `spring.datasource.hikari.maximum-pool-size` | `4` | Maximum database connections per Registry instance |
| k6 `VUS` | `50` | Concurrent client users; each waits for a response before its next request |

The admission gate runs before acquiring a database connection and holds capacity through commit
and duplicate resolution. A rejected synchronous request is not published to RabbitMQ. Rabbit
consumers wait for a shared permit; an already accepted command is not rejected just because the
synchronous endpoint is busy. No additional executor or detached task is needed with virtual threads.
The 200ms admission timeout limits waiting for an execution permit; transaction and response time are
additional, so successful HTTP latency can exceed 200ms.

The execution limit and database pool are different bounds. With processing parallelism `16` and a
pool of `4`, at most four executions hold connections; some other permitted executions can wait for
connections. The HTTP waiting limit is an additional bound outside those executions. Changing the
processing limit does not create more database capacity. Multiple Registry instances each have their
own admission gate and pool; size their combined connection budgets separately.

## Reproducible comparison

Build the Registry with Java 25 and start the existing infrastructure and Compliance. The runner
starts a temporary Registry process, checks run-specific database counts, and stops only the process
it started. It never purges a queue or truncates a table. Commands below use Git Bash on Windows or
Bash on Linux/macOS.
Finish `make verify` before benchmarking: concurrent verification competes for resources and rebuilds
the target JAR. `REGISTRY_JAR` can point to an immutable JAR copy for a series of experiments.

```bash
user_registry/mvnw -f user_registry/pom.xml -DskipTests package
# If another Registry is on 8080, leave it idle and run the comparison on 8082.
PORT=8082 ITERATIONS=2000 VUS=50 bash load-tests/sync-experiment.sh S1 1
PORT=8082 ITERATIONS=2000 VUS=50 bash load-tests/sync-experiment.sh S4 4
PORT=8082 ITERATIONS=2000 VUS=50 bash load-tests/sync-experiment.sh S16 16
```

To bypass Docker Desktop host forwarding, use a native k6 2.3.0 executable from the
[official release](https://github.com/grafana/k6/releases/tag/v2.3.0), with the same script and workload:

```bash
K6_BIN=C:/tools/k6.exe BASE_URL=http://127.0.0.1:8082 PORT=8082 ITERATIONS=2000 VUS=50 \
  bash load-tests/sync-experiment.sh HostS1 1
# Repeat with processing limits 4 and 16, keeping K6_BIN and the other settings fixed.
```

Native mode defaults to `localhost:$PORT`, writes summaries directly to `RESULTS_DIR`, and does not
run a load-generator container. The databases remain in Docker. Record which generator path was
used; do not combine native and container runs into one performance comparison.

Each run fixes the Hikari pool at `4`, disables Rabbit listeners on its temporary instance, and sets
`registry.processing.max-concurrency` to its second argument. The runner refuses existing ready,
unacknowledged or dead-letter registration messages. Do not send traffic to another Registry while
benchmarking: an idle process can coexist on a different port, but its database work would contend
with the experiment. If a DLQ is nonempty, inspect its failures before running a comparison.

Keep client concurrency fixed when comparing server limits. A useful server matrix is:

| Run | Client VUs | Processing limit | DB pool | HTTP waiting limit | Admission wait |
|---|---:|---:|---:|---:|---|
| S1 | 50 | 1 | 4 | 64 | 200ms |
| S4 | 50 | 4 | 4 | 64 | 200ms |
| S16 | 50 | 16 | 4 | 64 | 200ms |

Then hold server parallelism at `4` and sweep client pressure separately:

```bash
PORT=8082 ITERATIONS=2000 VUS=4 bash load-tests/sync-experiment.sh C4 4
PORT=8082 ITERATIONS=2000 VUS=50 bash load-tests/sync-experiment.sh C50 4
PORT=8082 ITERATIONS=2000 VUS=200 bash load-tests/sync-experiment.sh C200 4
# Later overload experiment; keep 25,000 total distinct from 25,000 simultaneous connections.
PORT=8082 ITERATIONS=25000 VUS=200 bash load-tests/sync-experiment.sh C25K 4
```

The default 200ms wait may allow all requests through at moderate pressure. To demonstrate immediate
backpressure independently, run an explicitly labeled zero-wait experiment:

```bash
PORT=8082 ITERATIONS=2000 VUS=200 MAX_WAITING_REQUESTS=0 ADMISSION_WAIT=0ms \
  bash load-tests/sync-experiment.sh RejectExcess 4
```

Set `WAIT_FOR_COMPLIANCE=0` to check only the Registry commit boundary when Compliance is not running.
Otherwise the runner requires healthy Compliance and waits up to `COMPLIANCE_TIMEOUT=900` seconds
for its results. A successful synchronous response never means Compliance is already complete.

The same k6 scenario can target an already running Registry directly:

```bash
MODE=async ITERATIONS=10000 VUS=50 bash load-tests/run-k6.sh
MODE=sync BASE_URL=http://host.docker.internal:8082 ITERATIONS=2000 VUS=50 bash load-tests/run-k6.sh
```

The async `experiment.sh` continues to measure consumer concurrency, and now sets the shared
processing limit to that consumer count. Run async and sync comparisons separately against an idle
system, with the same total, client concurrency, pool size, hardware and JVM configuration. If an
8080 Registry is already running, the existing async runner still requires stopping it first; use the
new sync runner's `PORT` option to keep that process intact.

## Reading the results

Each iteration sends one unique key/email and performs no retry. The primary report counts `201`,
`200`, `202`, `409`, `503` and other outcomes separately. Fresh runs expect only `201` and controlled
`503`; a replay or conflict indicates accidental reuse or interference. k6 checks successful sync
bodies for a user UUID, matching registration id/email and `status=REGISTERED`.

Results are appended to `load-tests/results/sync-experiments.tsv`, with raw k6 summary JSON, Registry
logs, before/after Prometheus snapshots and sampled gauges alongside it. The runner checks
`users = distinct registration ids = outbox rows = 201 count` for the run; when enabled it also checks
Compliance result count. A count mismatch or unexpected response marks the run incomplete and exits
nonzero. A valid experiment with overload responses is labeled `COMPLETE_WITH_REJECTIONS`.
Set `RESULTS_DIR` to use another writable artifact directory for both the runner and k6.
The Docker runner uses Docker Desktop's built-in host DNS, and adds `host-gateway` on native Linux.
`DOCKER_HOST_GATEWAY=0` or `1` overrides that detection. An injected unreachable IPv6 address can
produce transport errors; a run with such errors is incomplete and must not be treated as overload.

HTTP latency across all responses includes potentially fast rejection responses. Use the separate
successful-response p50/p95 together with the rejection count. In sync mode that successful latency
includes admission, database work, commit and response transfer. In async mode it ends at queue
acceptance, so compare it with async registration-completion latency before discussing user-visible
completion. The reported created/s is `201 count / k6 run duration`, not the number of attempted requests
per second and not the database's maximum capacity.

The runner samples the **target instance's** processing-active, HTTP-waiting and Hikari-pending gauges
with a 500ms delay between scrapes. These are observed sample maxima; shorter peaks can be missed,
and request/shell overhead increases the actual interval. Before/after snapshots are also retained.
An existing Prometheus target on 8080 does not describe a comparison running on 8082. No claims about
exact peaks or p95 are derived from those sparse gauge samples; HTTP percentiles come from k6.

`shared-iterations` is a closed-loop workload: slower responses reduce the request rate. It demonstrates
parallelism and backpressure at bounded client concurrency; it does not establish a fixed-arrival-rate
capacity claim. Repeat runs and record the environment before drawing a performance conclusion.

## Measured example: 2026-10-29

Three sequential native Windows k6 2.3.0 runs, each with 2,000 distinct requests, 50 VUs, no retries,
Java 25 virtual threads, pool 4, HTTP waiting limit 64 and admission wait 200ms. Rabbit listeners were
disabled on the temporary Registry at 8082. Compliance remained running; all successful requests were
verified through its eventual result. Hardware and infrastructure match the [environment table](benchmark-results.md#environment).
Verification/build processes were no longer running during these final runs. Each application was
newly started; there was no separate transaction warm-up or repeated statistical sampling.

| Execution limit | Created `201` | Overloaded `503` | Other / invalid bodies | Created/s | Successful HTTP p95 ms | Sampled execution max | Sampled Hikari pending max | Users / outbox / Compliance |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 231 | 1,769 | 0 / 0 | 22.4 | 321.8 | 1 | 0 | 231 / 231 / 231 |
| 4 | 607 | 1,393 | 0 / 0 | 49.8 | 466.6 | 4 | 0 | 607 / 607 / 607 |
| 16 | 716 | 1,284 | 0 / 0 | 42.9 | 1,242.2 | 16 | 12 | 716 / 716 / 716 |

The four-permit run committed more per second than the one-permit run. Sixteen permits completed
more requests over a longer run, but committed fewer per second than four and had higher successful
latency. Its sampled twelve pool waiters show how extra executions can wait behind the same four
connections. These are single local demonstration runs, not a production sizing conclusion.

The high rejection counts are explicit backpressure under the short 200ms admission policy. They do
not indicate lost accepted work: every `201` matched one user, one outbox event and one Compliance
result. Increasing the admission wait changes that latency/rejection tradeoff and should be tested
separately. The default execution limit remains four.

Run IDs, in table order:

- `QuietS1-sync-20261029-204952-645853700-11587`
- `QuietS4-sync-20261029-205042-429009900-30262`
- `QuietS16-sync-20261029-205141-147830000-4441`

The immutable Registry JAR SHA-256 was
`e48bbe1a29bbf5e01092ad765933c2719ba23eecb6c90e8137bdcc75bad38bfa`.
Raw summaries and the TSV are in `load-tests/results/`. Earlier interrupted runs, transport errors,
startup failures and runs potentially overlapping `make verify` are excluded from this table.

Validation: the full Registry suite passed 59 tests, then the five flow tests passed after adding the
cross-route replay check (60 distinct tests overall). These cover commit-before-response, stable
replays, conflicts, rollback, concurrent duplicate races, shared limits, bounded waiting, timeout and
interrupt cleanup, and the original Rabbit retry/processing behavior. Shell parsing and k6 initialization
were checked as well.
