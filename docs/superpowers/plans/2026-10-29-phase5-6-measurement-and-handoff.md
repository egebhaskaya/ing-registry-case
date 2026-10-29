# Phase 5-6: Measurement, Dashboard, Load Test and Handoff — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **For this project:** Claude implements, Ege reviews; code carries explanatory comments; no git commits by Claude.

**Goal:** Instrument both services with Micrometer, scrape them with a local Prometheus, show one provisioned Grafana dashboard, run the 10,000-request k6 experiments A/B/C (and optional D), record measured results, and write the handoff documents.

**Architecture:** Counters and timers with bounded labels in the services (intake outcomes, registration outcomes, end-to-end completion timers measured from the original submission timestamp, dead-letter counts, configuration gauges). Prometheus (`prom/prometheus:v3.15.1`) scrapes both apps on the host through `host.docker.internal` and RabbitMQ's plugin with per-object metrics. Grafana OSS (`grafana/grafana:13.3.0`) is provisioned from files: one data source, one dashboard with the six panels of design section 8 plus a context row. k6 runs from the pinned `grafana/k6:2.3.0` image with a `shared-iterations` scenario; an experiment script starts Registry with a given consumer count, runs k6, waits for drain, derives throughput and completion from database timestamps and Prometheus, and appends a results row.

**Tech Stack:** Micrometer 1.17 (via Boot 4.1.2), `micrometer-registry-prometheus` (already in both poms), Prometheus v3.15.1, Grafana 13.3.0, k6 2.3.0 (Docker), bash scripts.

**Spec:** `docs/design.md` sections 6 (experiments), 7 (load test and measurements), 8 (Prometheus/Grafana, metric correctness, presentation sequence), 9 (phases 5-6), 11 (checklist).

## Global Constraints

- Bounded metric labels only: `outcome`, `application`, `queue`, `status`, `uri`. Never registration ids, event ids, emails or thread ids.
- Completion timers measure from `submittedAt` (carried in the command/event) to the successful commit; a listener execution timer is not an end-to-end timer.
- p95 comes from histogram buckets via `histogram_quantile`, never from averages.
- Scrape interval 5s, dashboard refresh 5s, rate windows 30s for throughput and 1m for latency, documented on the panels.
- Queue panels use per-queue series (`rabbitmq_queue_messages_ready{queue="registration.commands"}`), enabled by `prometheus.return_per_object_metrics = true`.
- Kafka committed-group lag has no verified exporter: the panel states "unavailable; use kafka-consumer-groups.sh" rather than showing `records-lag-max`.
- Grafana runs offline: analytics, update checks and news feed disabled; anonymous Admin access for the local demo; no plugins.
- k6: `shared-iterations`, 10,000 iterations total, VUs configurable (default 50), one registration per iteration, unique key and email per iteration derived from a run id and the iteration index, no automatic retries, `maxDuration` 10m; an incomplete run is reported as incomplete.
- Experiments A/B/C: consumers 1/4/16, Hikari max 4, prefetch 8, intake limit 64, one Registry instance, virtual threads on; D optional: B with the `platform` profile. Same k6 profile for every run. Report submission duration, 202 count, failures, HTTP p50/p95 (from k6), registration drain time and committed/s (from `users.created_at`), Compliance completion time (from `compliance_results.checked_at`), Hikari pending max and acquisition timeouts (from Prometheus), hardware context.
- No performance target is invented; results are whatever the machine measures.

## Review Focus

1. A counter label with unbounded values (for example the exception message) → cardinality explosion in Prometheus. Pinned by code review of every `Counter.builder(...).tag(...)` call and `RegistryMetricsTest` asserting the exact label sets.
2. The completion timer recorded on duplicates or failures → p95 lies. Pinned in Task 1 (timer only on `CREATED`) and Task 2 (only on `PROCESSED`).
3. Prometheus cannot reach the apps from the container → empty dashboard with "No data" presented as zero. Pinned in Task 3 by asserting every target is `up` and by panels configured to show "No data" (no `null as zero`).
4. k6 keys collide across runs → duplicates counted as conflicts and the 10,000 figure is wrong. Pinned in Task 4 by deriving keys from the run id and the global iteration index, and verified by the experiment script's distinct-count check.
5. An experiment reported as 10,000 when k6 stopped early → the design forbids it. Pinned in Task 5: the script reads k6's iteration count and marks the run incomplete when it is below 10,000.

---

## File structure

```text
user_registry/src/main/java/com/example/registry/config/RegistryMetrics.java      gauges for config + virtual-thread mode; counter/timer factories
user_registry/src/main/java/com/example/registry/services/RegistrationIntakeService.java   + intake counter
user_registry/src/main/java/com/example/registry/listeners/RegistrationCommandListener.java + outcome counter, completion timer, failure counter
user_registry/src/main/java/com/example/registry/config/RabbitConfig.java                + dead-letter counter around the recoverer
user_registry/src/main/resources/application.yml                                         + histogram settings, application tag
user_registry/src/test/java/com/example/registry/RegistryMetricsTest.java
compliance/src/main/java/com/example/compliance/config/ComplianceMetrics.java
compliance/src/main/java/com/example/compliance/listeners/UserRegisteredEventListener.java + counter, completion timer
compliance/src/main/java/com/example/compliance/config/KafkaConfig.java                   + dead-letter counter
compliance/src/main/resources/application.yml                                            + histogram settings, application tag
compliance/src/test/java/com/example/compliance/ComplianceMetricsTest.java
infra/compose.yml                                   + prometheus, grafana; rabbitmq conf mount
infra/rabbitmq/20-prometheus.conf                   prometheus.return_per_object_metrics = true
infra/monitoring/prometheus.yml
infra/monitoring/grafana/provisioning/datasources/prometheus.yml
infra/monitoring/grafana/provisioning/dashboards/dashboards.yml
infra/monitoring/grafana/dashboards/registration-case.json
load-tests/registrations.js                         k6 scenario
load-tests/run-k6.sh                                runs k6 in Docker, writes results/<run>.summary.json
load-tests/experiment.sh                            one experiment: start Registry with N consumers, k6, drain, measure, append row
load-tests/results/.gitkeep
docs/architecture.md
docs/demo-script.md
docs/benchmark-results.md
README.md                                           status, monitoring, load test, results summary
```

Metric names (Micrometer form → Prometheus form):

| Micrometer | Prometheus | Type / labels |
|------------|------------|---------------|
| `registry.intake.requests` | `registry_intake_requests_total` | counter, `outcome` ∈ accepted, overloaded, broker_rejected, unroutable, broker_unavailable, confirm_timeout |
| `registry.registrations` | `registry_registrations_total` | counter, `outcome` ∈ created, duplicate_ignored, conflict |
| `registry.registrations.failures` | `registry_registrations_failures_total` | counter (listener attempt threw; includes retried attempts) |
| `registry.registrations.dead_lettered` | `registry_registrations_dead_lettered_total` | counter |
| `registry.registration.completion` | `registry_registration_completion_seconds_*` | timer with histogram, submittedAt → commit |
| `registry.config.consumers` / `registry.config.prefetch` | `registry_config_consumers` / `registry_config_prefetch` | gauges |
| `registry.threads.virtual.enabled` | `registry_threads_virtual_enabled` | gauge 1/0 |
| `compliance.events` | `compliance_events_total` | counter, `outcome` ∈ processed, duplicate_ignored |
| `compliance.events.dead_lettered` | `compliance_events_dead_lettered_total` | counter |
| `compliance.completion` | `compliance_completion_seconds_*` | timer with histogram, submittedAt → result commit |
| `compliance.config.concurrency` | `compliance_config_concurrency` | gauge |
| built-in | `http_server_requests_seconds_*`, `hikaricp_connections_*`, `spring_rabbitmq_listener_seconds_*`, `spring_kafka_listener_seconds_*`, `jvm_threads_*`, `jvm_memory_used_bytes`, `jvm_info` | provided by Boot/Micrometer |

---

### Task 1: Registry metrics

**Files:** create `config/RegistryMetrics.java`; modify `RegistrationIntakeService`, `RegistrationCommandListener`, `RabbitConfig`, `application.yml`; test `RegistryMetricsTest`.

**Interfaces:**
- `@Component class RegistryMetrics` constructor `(MeterRegistry, RabbitProperties, Environment)`: registers the gauges; methods `void intake(String outcome)`, `void registration(String outcome)`, `void registrationFailed()`, `void deadLettered()`, `void completion(Duration)`.
- `application.yml`: `management.metrics.tags.application: registry-service`; `management.metrics.distribution.percentiles-histogram.http.server.requests: true`, `.registry.registration.completion: true`; `minimum-expected-value.http.server.requests: 1ms`, `maximum-expected-value.http.server.requests: 10s`; `minimum-expected-value.registry.registration.completion: 10ms`, `maximum-expected-value.registry.registration.completion: 5m`.

- [ ] **Step 1: Failing test** `RegistryMetricsTest` (extends `AbstractIntegrationTest`, `@AutoConfigureRestTestClient`): POST one valid registration, await users == 1, then GET `/actuator/prometheus` and assert the body contains `registry_intake_requests_total{application="registry-service",outcome="accepted"}`, `registry_registrations_total{application="registry-service",outcome="created"}`, `registry_registration_completion_seconds_bucket`, `http_server_requests_seconds_bucket{`, `hikaricp_connections_max{`, `registry_config_consumers{application="registry-service"} 4.0`, `registry_threads_virtual_enabled{application="registry-service"} 1.0`, `jvm_info{`.
- [ ] **Step 2: Run** → FAIL (metric names absent).
- [ ] **Step 3: Implement** `RegistryMetrics`; call it from the intake service (every outcome branch, including the exception paths), the listener (counter per outcome; timer only on CREATED; failure counter in a catch-rethrow), and wrap the recoverer in `RabbitConfig` to count dead letters. YAML settings.
- [ ] **Step 4: Run** → PASS; full Registry suite → PASS.

### Task 2: Compliance metrics

**Files:** create `config/ComplianceMetrics.java`; modify `UserRegisteredEventListener`, `KafkaConfig`, `application.yml`; test `ComplianceMetricsTest`.

- [ ] **Step 1: Failing test**: send one event via `KafkaTemplate`, await results == 1, GET `/actuator/prometheus`: contains `compliance_events_total{application="compliance-service",outcome="processed"}`, `compliance_completion_seconds_bucket`, `compliance_config_concurrency{application="compliance-service"} 1.0`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** (`ComplianceMetrics` with `events(String outcome)`, `completion(Duration)`, `deadLettered()`; the recoverer in `KafkaConfig` subclassed to count). **Step 4: Run** → PASS; full Compliance suite → PASS.

### Task 3: Prometheus, Grafana and per-queue RabbitMQ metrics

**Files:** `infra/rabbitmq/20-prometheus.conf`, `infra/monitoring/prometheus.yml`, Grafana provisioning files and dashboard JSON, `infra/compose.yml`.

- `prometheus.yml`: `global.scrape_interval: 5s`; jobs `registry` (`host.docker.internal:8080`, `metrics_path: /actuator/prometheus`), `compliance` (`host.docker.internal:8081`), `rabbitmq` (`rabbitmq:15692`, path `/metrics`), `prometheus` (self).
- Compose: `prometheus` (`prom/prometheus:v3.15.1`, port 9090, `extra_hosts: ["host.docker.internal:host-gateway"]`, volume `prometheus-data:/prometheus`, command with `--config.file` and `--storage.tsdb.retention.time=7d`); `grafana` (`grafana/grafana:13.3.0`, port 3000, env `GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Admin`, `GF_ANALYTICS_REPORTING_ENABLED=false`, `GF_ANALYTICS_CHECK_FOR_UPDATES=false`, `GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES=false`, `GF_NEWS_NEWS_FEED_ENABLED=false`, volumes for provisioning, dashboards and `grafana-data:/var/lib/grafana`); rabbitmq gets `./rabbitmq/20-prometheus.conf:/etc/rabbitmq/conf.d/20-prometheus.conf:ro`.
- Dashboard `Registration Case`: rows: Context (stats: consumers, prefetch, Hikari max, virtual threads, JVM version); 1 Observed progress; 2 Throughput; 3 Rabbit work; 4 Database pool; 5 Latency p95; 6 Errors and recovery; Diagnostics (JVM live threads, heap used); a text panel "Kafka group lag: unavailable without an exporter; use kafka-consumer-groups.sh". Each panel has a description naming the measurement boundary and the rate window. Data source uid `prometheus`.

- [ ] **Step 1: Write files; `docker compose up -d --wait`** → prometheus and grafana healthy.
- [ ] **Step 2: Verify targets**: with both apps running, `curl -s localhost:9090/api/v1/targets` → every target `"health":"up"`. `curl -s 'localhost:9090/api/v1/query?query=rabbitmq_queue_messages_ready{queue="registration.commands"}'` returns a series.
- [ ] **Step 3: Verify Grafana**: `curl -s localhost:3000/api/health` ok; `curl -s 'localhost:3000/api/search?query=Registration'` lists the dashboard; `curl -s localhost:3000/api/dashboards/uid/registration-case` returns the panels. Run the smoke test and query each panel's PromQL through Prometheus's API to confirm a non-empty result.

### Task 4: k6 scenario and runner

**Files:** `load-tests/registrations.js`, `load-tests/run-k6.sh`, `load-tests/results/.gitkeep`.

- `registrations.js`: `scenarios.burst = { executor: 'shared-iterations', vus: __ENV.VUS || 50, iterations: __ENV.ITERATIONS || 10000, maxDuration: '10m' }`; per iteration: `n = exec.scenario.iterationInTest`, key = `${runHex}-${nHex}` shaped as a UUID, email `load-${RUN_ID}-${n}@example.com`, POST, `check` for 202, custom counters `accepted`, `rejected_503`, `other`; `handleSummary` writes `/results/${RUN_ID}.summary.json`; thresholds none (report, do not fail).
- `run-k6.sh`: `RUN_ID=${RUN_ID:-$(date +%Y%m%d-%H%M%S)}`; `docker run --rm -i --add-host=host.docker.internal:host-gateway -e BASE_URL=${BASE_URL:-http://host.docker.internal:8080} -e RUN_ID -e VUS -e ITERATIONS -v <abs load-tests>:/scripts -v <abs results>:/results grafana/k6:2.3.0 run /scripts/registrations.js`; prints the run id and the summary path.

- [ ] **Step 1:** `ITERATIONS=100 load-tests/run-k6.sh` against the running stack → 100 iterations, 100 checks passed, summary file written; `users` for the run id == 100.

### Task 5: Experiment runner and the A/B/C(/D) results

**Files:** `load-tests/experiment.sh`, `docs/benchmark-results.md`.

- `experiment.sh NAME CONSUMERS [PROFILE]`: requires the stack and Compliance running and no Registry running; starts Registry from its jar with `SPRING_RABBITMQ_LISTENER_SIMPLE_CONCURRENCY=CONSUMERS`, `SPRING_RABBITMQ_LISTENER_SIMPLE_MAX_CONCURRENCY=CONSUMERS` (+ `--spring.profiles.active=platform` for D), waits for health; records `t0`; runs `run-k6.sh` with `RUN_ID=NAME-<timestamp>`; reads from the summary: iterations, 202 count, failures, http p50/p95, submission duration; polls Registry DB until `count(users where email like 'load-RUN_ID-%') == accepted` (timeout 15m) and takes `min/max(created_at)` → drain time (`max(created_at) - t0`) and committed/s (`accepted / (max - min)`); polls Compliance until results for the run's registration ids (join via `users.registration_id`) == accepted → compliance completion (`max(checked_at) - t0`); queries Prometheus (`max_over_time(hikaricp_connections_pending{application="registry-service"}[window])`, `increase(hikaricp_connections_timeout_total[window])`, `histogram_quantile(0.95, sum(rate(registry_registration_completion_seconds_bucket[window])) by (le))` evaluated at the end time with window = drain duration); marks the run `INCOMPLETE` when iterations < 10000; appends a row to `docs/benchmark-results.md`; stops Registry.
- `benchmark-results.md`: hardware/JVM context block (CPU, RAM, Java 25, Docker Desktop, image versions), the experiment table, the measurement boundaries (what each column means and what it does not), and observations.

- [ ] **Step 1:** dry run with `ITERATIONS=500 experiment.sh smoke 4` → a row appears, numbers plausible.
- [ ] **Step 2:** run A (1), B (4), C (16) with 10,000 each; then D (4, `platform`) if time allows. Record results and observations (consumers vs. pool waiting, where the stages diverge).

### Task 6: Handoff documents

**Files:** `docs/architecture.md`, `docs/demo-script.md`, `README.md`; `docs/benchmark-results.md` from Task 5.

- `architecture.md`: the flow diagram, component responsibilities per service, the bounded resources table (admission 64, consumers, prefetch, Hikari 4, partitions 4, Compliance concurrency), the guarantees table (at-least-once vs exactly-once effect, what the 202 means, DLQ/DLT), tradeoffs and alternatives from the design (sync 201 alternative, JdbcTemplate vs JPA, Debezium vs polling publisher, virtual threads are not an admission limit), production considerations (outbox cleanup, WAL retention, HA out of scope).
- `demo-script.md`: the presentation sequence of design section 8 as numbered steps with the exact commands and what to point at on the dashboard, including the failure demonstrations (stop Compliance, redelivery, DLQ).
- `README.md`: status table all done, monitoring section (URLs, dashboard name, scrape/refresh), load test and experiment instructions, results summary table copied from `benchmark-results.md`, links to the three docs.

- [ ] **Step 1:** write the three documents and the README update. **Step 2:** full `verify` on both modules → BUILD SUCCESS. **Step 3:** walk the demo script once against the running stack and fix any command that does not work as written.

---

## Self-review notes

- Spec section 7 measurements covered: submitted/202/failures/unknown (k6 summary + intake counter), HTTP p50/p95 (k6 + histogram), registration and Compliance completion (timers + DB timestamps), committed/s and drain time (DB timestamps), Rabbit ready/unacked/DLQ (per-object metrics), Hikari active/idle/pending/max/timeouts (built-in), Java/virtual-thread mode (gauges + `jvm_info`), CDC/Kafka lag (declared unavailable, CLI documented), final distinct counts (experiment script).
- Spec section 8 metric-correctness rules are encoded as Global Constraints and panel descriptions.
- Checklist section 11: every line maps to a plan task or to Plan 1/2 evidence.
