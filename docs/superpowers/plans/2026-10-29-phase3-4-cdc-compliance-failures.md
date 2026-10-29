# Phase 3-4: CDC, Compliance and Failure Verification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **For this project:** Claude implements, Ege reviews; code carries explanatory comments (syntax + design reason); no git commits by Claude.

**Goal:** Committed outbox rows reach Kafka through Debezium only, Compliance consumes them exactly-once-in-effect, and Registry failures are retried finitely then dead-lettered with a recorded reason.

**Architecture:** Debezium Kafka Connect (PostgreSQL `pgoutput` connector + Outbox Event Router) publishes each committed `outbox` insert to topic `outbox.event.user` (key = user id, value = the JSON payload, 4 partitions). Compliance (`compliance/`, package `com.example.compliance`) consumes with a `@KafkaListener` on a virtual thread, parses the JSON, and in one transaction inserts a `processed_events` row (idempotency ledger, `ON CONFLICT DO NOTHING`) and a `compliance_results` row; the offset is committed per record after the listener returns. Registry gets finite listener retries with backoff for transient failures and a republishing recoverer that dead-letters with `x-exception-message` headers; permanent conflicts skip retries.

**Tech Stack:** adds Kafka 4.2.2 (`apache/kafka:4.2.2`, KRaft single node), Debezium Connect 3.7.0.Final (`quay.io/debezium/connect:3.7.0.Final`), `curlimages/curl:8.23.0`, Spring Kafka 4.1.2 (`spring-boot-starter-kafka`, already in the Compliance pom), Testcontainers 2.0.5 `org.testcontainers.kafka.KafkaContainer`, Spring AMQP 4.1.2 retry on Spring Framework 7 core retry.

**Spec:** `docs/design.md` sections 4 (Compliance data), 5 (CDC and Compliance, failure rules), 9 (phases 3-4), 10. Metrics, dashboards, k6 and the final handoff docs remain for the next plan.

**Execution order:** Task 1, then Task 6 (Registry retries) before any Kafka work, then Tasks 2, 3, 4, 5, 7. The Phase 1-2 reviewer asked for finite retry to land before anything that generates bursts, because today every consumer error dead-letters immediately.

## Global Constraints

- Everything from Plan 1 still holds (Java 25, Boot 4.1.2, package-by-layer, JdbcTemplate, no detached async work, pinned images).
- Only Debezium publishes the outbox stream. No application Kafka producer for business events, no polling publisher. (Compliance's producer exists solely for its dead-letter topic.)
- Kafka key = user id (`aggregateid`); stable event id for deduplication; 4 partitions; one Compliance consumer by default, concurrency configurable via `spring.kafka.listener.concurrency`.
- Compliance commits result + deduplication state before the offset advances: `spring.kafka.listener.ack-mode: record`, `enable-auto-commit: false`.
- Exactly one deterministic demo rule, labelled as demo logic: a country code on the configurable list `compliance.demo-rule.flagged-countries` (default `XX,ZZ`, ISO user-assigned codes) is `FLAGGED`, anything else `APPROVED`.
- Keep transactions short; never hold a DB connection while waiting on a broker.
- Registry retries: `spring.rabbitmq.listener.simple.retry` enabled, `max-retries: 3`, `initial-interval: 500ms`, `multiplier: 2`, `max-interval: 5s`, stateless. Permanent conflicts (`AmqpRejectAndDontRequeueException`) are excluded from retry. Exhausted or permanent failures are republished to the dead-letter exchange with exception headers; the original delivery is then acknowledged.
- Compliance retries: `DefaultErrorHandler` with `ExponentialBackOffWithMaxRetries(3)` (500ms, x2, max 5s) and a `DeadLetterPublishingRecoverer` to `outbox.event.user.DLT`; unparseable records are not retried.
- Retain broker data, connector offsets and the replication slot across restarts (named volumes). Outbox cleanup is documented, not built.
- Compose file is `infra/compose.yml`; host-facing Kafka listener on `localhost:29092`; Connect REST on `localhost:8083`.

## Review Focus

1. The same Kafka record delivered twice (consumer restart after commit, before offset) → one `processed_events` row, one `compliance_results` row. Pinned in Task 4 (`ComplianceServiceTest`) and Task 5 (`UserRegisteredEventListenerTest`).
2. An unparseable record on the topic → it goes to the dead-letter topic without blocking the partition forever, and no rows are written. Pinned in Task 5.
3. A transient Registry failure (database hiccup) → the delivery is retried with backoff and eventually succeeds; nothing in the DLQ. Pinned in Task 6 (`RegistrationRetryTest`).
4. A permanent Registry conflict → exactly one processing attempt, then the DLQ message carries the reason header. Pinned in Task 6.
5. A rolled-back Registry transaction → no Kafka record (Debezium only sees committed WAL). Pinned by the end-to-end smoke test in Task 7 (rolled-back inserts are not visible to logical decoding by construction; the smoke test checks counts match users, not outbox attempts).

---

## File structure

```text
infra/compose.yml                                    add kafka, kafka-init, connect, connect-init; volume kafka-data
infra/debezium/registry-outbox-connector.json        connector config (PUT body)
infra/smoke-test.sh                                  end-to-end check against the Compose stack
user_registry/src/main/java/com/example/registry/messaging/UserRegisteredEvent.java   add eventId
user_registry/src/main/java/com/example/registry/services/RegistrationService.java    eventId generated once
user_registry/src/main/java/com/example/registry/config/RabbitConfig.java             + recoverer, retry customizer
user_registry/src/main/resources/application.yml                                      + retry block
user_registry/src/test/java/com/example/registry/listeners/RegistrationRetryTest.java
compliance/src/main/resources/application.yml                                         + kafka consumer/listener, compliance.*
compliance/src/main/resources/db/migration/V1__compliance.sql
compliance/src/main/java/com/example/compliance/
  config/ComplianceProperties.java        compliance.kafka.*, compliance.demo-rule.*
  config/KafkaConfig.java                 topics (NewTopic), error handler with DLT recoverer
  messaging/UserRegisteredEvent.java      the consumed contract (same fields as Registry's)
  models/ProcessedEvent.java
  models/ComplianceResult.java
  repositories/ProcessedEventRepository.java
  repositories/ComplianceResultRepository.java
  services/ComplianceDecision.java        APPROVED | FLAGGED
  services/Verdict.java                   decision + reason
  services/DemoComplianceRule.java
  services/ComplianceOutcome.java         PROCESSED | DUPLICATE_IGNORED
  services/ComplianceService.java         the transaction
  listeners/UserRegisteredEventListener.java
compliance/src/test/java/com/example/compliance/
  TestcontainersConfiguration.java        + KafkaContainer
  AbstractIntegrationTest.java
  ComplianceApplicationTests.java         health + tables
  services/DemoComplianceRuleTest.java
  services/ComplianceServiceTest.java
  listeners/UserRegisteredEventListenerTest.java
README.md                                            phases 3-4 status, ports, inspection commands, failure demos
```

Build commands: `./user_registry/mvnw -f user_registry/pom.xml ...` and `./compliance/mvnw -f compliance/pom.xml ...` with `JAVA_HOME` on JDK 25.

---

### Task 1: Event id travels inside the outbox payload (Registry)

**Files:**
- Modify: `user_registry/src/main/java/com/example/registry/messaging/UserRegisteredEvent.java`, `services/RegistrationService.java`
- Modify test: `user_registry/src/test/java/com/example/registry/services/RegistrationServiceTest.java`

**Interfaces:**
- Produces: `record UserRegisteredEvent(UUID eventId, UUID userId, UUID registrationId, String email, String fullName, String countryCode, Instant submittedAt, Instant registeredAt)`; the outbox row `id` equals `eventId`. Compliance (Task 4) deduplicates on this field, independent of Kafka headers.

- [ ] **Step 1: Extend the test** `createsOneUserAndOneOutboxEvent`: add `assertThat(event.payload()).contains("\"eventId\":\"" + event.id() + "\"")`.
- [ ] **Step 2: Run** `-Dtest=RegistrationServiceTest` → that test FAILS (payload has no eventId).
- [ ] **Step 3: Implement**: add the component; in `register` compute `UUID eventId = UUID.randomUUID()` before building the event and pass it to both the event and the `OutboxEvent`.
- [ ] **Step 4: Run** → PASS. Run the full Registry suite → 29 pass.

---

### Task 2: Kafka, Debezium Connect and connector registration in Compose

**Files:**
- Modify: `infra/compose.yml`
- Create: `infra/debezium/registry-outbox-connector.json`

**Interfaces:**
- Produces: Kafka on `kafka:9092` (compose network) and `localhost:29092` (host); topics `outbox.event.user` and `outbox.event.user.DLT` with 4 partitions; Connect REST on `localhost:8083`; connector `registry-outbox` RUNNING; records on `outbox.event.user` with key = user id (string), headers `id` (event id) and `eventType` (`UserRegistered`), value = the payload JSON object.

- [ ] **Step 1: Add services to `compose.yml`**

```yaml
  kafka:
    image: apache/kafka:4.2.2
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 4
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    ports: ["29092:29092"]
    volumes: ["kafka-data:/var/lib/kafka/data"]
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 12
      start_period: 20s

  kafka-init:            # one-shot: creates the event topic and its dead-letter topic with 4 partitions
    image: apache/kafka:4.2.2
    depends_on: { kafka: { condition: service_healthy } }
    entrypoint: ["/bin/sh", "-c"]
    command: >
      "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic outbox.event.user --partitions 4 --replication-factor 1
       && /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic outbox.event.user.DLT --partitions 4 --replication-factor 1"

  connect:
    image: quay.io/debezium/connect:3.7.0.Final
    depends_on:
      kafka: { condition: service_healthy }
      postgres: { condition: service_healthy }
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: 1
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
    ports: ["8083:8083"]
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8083/connectors || exit 1"]   # verify curl exists in the image; fall back to bash /dev/tcp
      interval: 10s
      timeout: 5s
      retries: 18
      start_period: 30s

  connect-init:          # one-shot, idempotent: PUT the connector config
    image: curlimages/curl:8.23.0
    depends_on: { connect: { condition: service_healthy } }
    volumes: ["./debezium:/debezium:ro"]
    command: ["sh", "-c", "curl -sf -X PUT -H 'Content-Type: application/json' --data @/debezium/registry-outbox-connector.json http://connect:8083/connectors/registry-outbox/config"]
```

Add `kafka-data:` under `volumes:`.

- [ ] **Step 2: Write the connector config** (`PUT /connectors/registry-outbox/config` body: the config map only)

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "postgres",
  "database.dbname": "registry",
  "topic.prefix": "registry",
  "plugin.name": "pgoutput",
  "slot.name": "registry_outbox",
  "publication.name": "registry_outbox_pub",
  "publication.autocreate.mode": "filtered",
  "table.include.list": "public.outbox",
  "snapshot.mode": "initial",
  "tombstones.on.delete": "false",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.expand.json.payload": "true",
  "transforms.outbox.table.fields.additional.placement": "type:header:eventType",
  "transforms.outbox.route.topic.replacement": "outbox.event.${routedByValue}",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
```

- [ ] **Step 3: Bring it up and verify**
Run: `docker compose -f infra/compose.yml up -d --wait` → all long-running services healthy, both init containers exited 0.
Run: `curl -s localhost:8083/connectors/registry-outbox/status` → `"state":"RUNNING"` for connector and task.
Run: `docker compose -f infra/compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic outbox.event.user` → `PartitionCount: 4`.
Start Registry (`spring-boot:run`), POST one registration, then
`docker compose -f infra/compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic outbox.event.user --from-beginning --max-messages 1 --property print.key=true --property print.headers=true --timeout-ms 15000`
Expected: one line with key = the user id, headers `id:<eventId>,eventType:UserRegistered`, value = the JSON payload containing `"registrationId"`.
Run: `docker compose -f infra/compose.yml exec postgres psql -U postgres -d registry -c "SELECT slot_name, active FROM pg_replication_slots"` → `registry_outbox | t`.

---

### Task 3: Compliance schema, configuration and test harness

**Files:**
- Modify: `compliance/src/main/resources/application.yml`
- Create: `compliance/src/main/resources/db/migration/V1__compliance.sql` (delete `.gitkeep`)
- Create: `compliance/src/main/java/com/example/compliance/config/ComplianceProperties.java`
- Modify test: `TestcontainersConfiguration.java` (+ Kafka); Create: `AbstractIntegrationTest.java`; Modify: `ComplianceApplicationTests.java`

**Interfaces:**
- Produces: tables `processed_events(event_id PK, user_id, registration_id, submitted_at, processed_at)` and `compliance_results(event_id PK → processed_events, user_id, registration_id, decision VARCHAR(16), reason VARCHAR(255), checked_at)`; `@ConfigurationProperties("compliance") record ComplianceProperties(Kafka kafka, DemoRule demoRule)` with nested `record Kafka(String topic, int partitions)` and `record DemoRule(List<String> flaggedCountries)`; `AbstractIntegrationTest` with `jdbcTemplate` and `truncateTables()` (`TRUNCATE TABLE compliance_results, processed_events`).

- [ ] **Step 1: application.yml additions**

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:29092
    consumer:
      group-id: compliance
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    listener:
      ack-mode: record
      concurrency: 1
compliance:
  kafka:
    topic: outbox.event.user
    partitions: 4
  demo-rule:
    flagged-countries: XX,ZZ
```

- [ ] **Step 2: Write `V1__compliance.sql`** (two tables as above; `decision` and `reason` NOT NULL).
- [ ] **Step 3: Write the failing test** `ComplianceApplicationTests.flywayCreatedTheTables` (counts of both tables are zero) next to `healthIsUp`; add `KafkaContainer kafkaContainer()` (`apache/kafka:4.2.2`, `@ServiceConnection`) to `TestcontainersConfiguration`; add `AbstractIntegrationTest`.
- [ ] **Step 4: Run** `compliance ... test -Dtest=ComplianceApplicationTests` → FAIL on missing tables, then write the SQL (if not yet) and `ComplianceProperties` → PASS.

---

### Task 4: Demo rule and the Compliance transaction

**Files:**
- Create: `messaging/UserRegisteredEvent.java`, `models/ProcessedEvent.java`, `models/ComplianceResult.java`, `repositories/ProcessedEventRepository.java`, `repositories/ComplianceResultRepository.java`, `services/ComplianceDecision.java`, `services/Verdict.java`, `services/DemoComplianceRule.java`, `services/ComplianceOutcome.java`, `services/ComplianceService.java`
- Test: `services/DemoComplianceRuleTest.java` (unit), `services/ComplianceServiceTest.java` (Postgres)

**Interfaces:**
- `record UserRegisteredEvent(UUID eventId, UUID userId, UUID registrationId, String email, String fullName, String countryCode, Instant submittedAt, Instant registeredAt)`
- `record ProcessedEvent(UUID eventId, UUID userId, UUID registrationId, Instant submittedAt, Instant processedAt)`
- `record ComplianceResult(UUID eventId, UUID userId, UUID registrationId, ComplianceDecision decision, String reason, Instant checkedAt)`
- `@Repository ProcessedEventRepository { boolean insertIfAbsent(ProcessedEvent); long count(); }` — `INSERT ... ON CONFLICT (event_id) DO NOTHING`, returns `updateCount == 1`.
- `@Repository ComplianceResultRepository { void insert(ComplianceResult); Optional<ComplianceResult> findByEventId(UUID); long count(); }`
- `enum ComplianceDecision { APPROVED, FLAGGED }`, `record Verdict(ComplianceDecision decision, String reason)`
- `@Service DemoComplianceRule { Verdict decide(UserRegisteredEvent event); }` constructor `(ComplianceProperties)`; reason strings: `"demo rule: country code <CC> is on the demo watch list"` / `"demo rule: no match"`.
- `enum ComplianceOutcome { PROCESSED, DUPLICATE_IGNORED }`
- `@Service ComplianceService { @Transactional ComplianceOutcome process(UserRegisteredEvent event); }` constructor `(ProcessedEventRepository, ComplianceResultRepository, DemoComplianceRule)`: insertIfAbsent → false → `DUPLICATE_IGNORED`; true → decide, insert result → `PROCESSED`.

- [ ] **Step 1: Unit tests** `DemoComplianceRuleTest`: `flagsCountriesOnTheDemoList` (XX → FLAGGED, reason contains "XX"), `approvesEverythingElse` (NL → APPROVED). Run → compile error.
- [ ] **Step 2: Implement** rule, enums, records → PASS.
- [ ] **Step 3: Integration tests** `ComplianceServiceTest` (`@MockitoSpyBean ComplianceResultRepository results`): `processesAnEventOnce` (PROCESSED; counts 1/1; result decision APPROVED), `sameEventTwiceIsIgnored` (second DUPLICATE_IGNORED; counts 1/1), `flaggedCountryIsRecordedAsFlagged`, `resultInsertFailureRollsBackTheLedgerRow` (doThrow on `results.insert`; process throws; `processedEvents.count()` is 0). Run → compile error.
- [ ] **Step 4: Implement** repositories and service → PASS.

---

### Task 5: Kafka listener, error handling and the Kafka end-to-end test

**Files:**
- Create: `config/KafkaConfig.java`, `listeners/UserRegisteredEventListener.java`
- Test: `listeners/UserRegisteredEventListenerTest.java`

**Interfaces:**
- `KafkaConfig` (`@EnableConfigurationProperties(ComplianceProperties.class)`): `NewTopic userRegisteredTopic` (name from props, partitions from props, replicas 1), `NewTopic userRegisteredDeadLetterTopic` (`<topic>.DLT`, same partitions), `CommonErrorHandler kafkaErrorHandler(KafkaTemplate<?, ?> template)` → `new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backOff)` with `ExponentialBackOffWithMaxRetries(3)` (initial 500, multiplier 2, max 5000) and `addNotRetryableExceptions(tools.jackson.core.JacksonException.class)`.
- `@Component UserRegisteredEventListener { @KafkaListener(topics = "${compliance.kafka.topic}") void onUserRegistered(ConsumerRecord<String, String> record); }` constructor `(ComplianceService, ObjectMapper)`: parse `record.value()` into `UserRegisteredEvent`, call `process`, log `eventId`, `registrationId`, `outcome`, partition and offset at INFO.

- [ ] **Step 1: Write the failing tests** (extends `AbstractIntegrationTest`; `@Autowired KafkaTemplate<String, String> kafkaTemplate` — if Boot's bean is `KafkaTemplate<?, ?>`, inject that and cast; `ComplianceProperties props`; nested `@TestConfiguration` with a `DeadLetterProbe` bean holding `CompletableFuture<String>` and `@KafkaListener(topics = "${compliance.kafka.topic}.DLT", groupId = "dlt-probe")`):

```java
@Test void consumedEventBecomesOneLedgerRowAndOneResult()   // send JSON of a sample event, key = userId; await().atMost(20s) until processed==1 && results==1
@Test void sameEventTwiceCreatesOneResult()                 // send same JSON twice; await processed==1; pollDelay 1s; counts stay 1/1
@Test void unparseableRecordGoesToDeadLetterTopic()         // send "not json"; DLT probe future completes within 20s with "not json"; counts 0/0
```
The JSON is built with the injected `ObjectMapper` from a `UserRegisteredEvent` so the field names cannot drift.

- [ ] **Step 2: Run** `-Dtest=UserRegisteredEventListenerTest` → compile error / timeouts.
- [ ] **Step 3: Implement** `KafkaConfig` and the listener → PASS. Run the whole Compliance suite → PASS.

---

### Task 6: Registry finite retries and dead-letter reasons

**Files:**
- Modify: `user_registry/src/main/resources/application.yml` (retry block), `config/RabbitConfig.java`
- Test: `user_registry/src/test/java/com/example/registry/listeners/RegistrationRetryTest.java`

**Interfaces:**
- YAML: `spring.rabbitmq.listener.simple.retry: { enabled: true, max-retries: 3, initial-interval: 500ms, multiplier: 2.0, max-interval: 5s, stateless: true }`.
- `RabbitConfig` beans: `MessageRecoverer deadLetterRecoverer(RabbitTemplate, RegistryRabbitProperties)` → `new RepublishMessageRecoverer(rabbitTemplate, props.deadLetterExchange(), props.routingKey())`; `RabbitListenerRetrySettingsCustomizer skipRetryForPermanentFailures()` → `settings.setExceptionExcludes(List.of(AmqpRejectAndDontRequeueException.class))` (Spring's retry policy matches the exception and its causes, so the `ListenerExecutionFailedException` wrapper is fine).

- [ ] **Step 1: Write the failing tests** (extends `AbstractIntegrationTest`, `@AutoConfigureRestTestClient`, `@MockitoSpyBean RegistrationService registrationService`, purge queues + truncate before each):

```java
@Test void transientFailureIsRetriedThenSucceeds()      // doThrow(new TransientDataAccessResourceException("db hiccup")).doThrow(same).doCallRealMethod().when(registrationService).register(any()); POST → 202; await users==1 within 15s; DLQ empty (receive 500ms null); verify(registrationService, times(3)).register(any())
@Test void exhaustedRetriesEndInDlqWithReason()         // doThrow(always).when(...).register(any()); POST; DLQ message within 20s; header "x-exception-message" contains "db hiccup"; users==0
@Test void permanentConflictIsNotRetried()              // real first registration; POST same key different name; DLQ message within 10s; verify(registrationService, times(2)).register(any()) (one real create + one conflict attempt), header x-exception-message contains "different payload"
```

- [ ] **Step 2: Run** → the retry tests fail (no retry: first test ends in DLQ; third test's header missing).
- [ ] **Step 3: Implement** YAML + beans → PASS. Run the whole Registry suite (`RegistrationFlowTest` DLQ assertions must still hold with the republished message) → PASS.

---

### Task 7: Smoke test script, documentation, full verification

**Files:**
- Create: `infra/smoke-test.sh`
- Modify: `README.md`

- [ ] **Step 1: Write `infra/smoke-test.sh`** (bash; runs on Git Bash, macOS, Linux). Inputs: `COUNT` (default 20), `BASE_URL` (default `http://localhost:8080`). Posts `COUNT` registrations with unique keys/emails (`smoke-<runid>-<i>@example.com`), counts 202s, then polls every 2s for up to 60s until `users`, `outbox`, `processed_events` and `compliance_results` counts for this run id all equal the number of 202s (queries via `docker compose exec postgres psql`, filtering on the email prefix / registration ids). Prints a one-line verdict and exits 1 on mismatch. Also prints `kafka-consumer-groups.sh --describe --group compliance` lag.
- [ ] **Step 2: Run it** against the live stack with both apps running → `OK: 20 accepted, 20 users, 20 outbox, 20 processed, 20 results`.
- [ ] **Step 3: Demonstrate restart recovery manually and record in README**: stop Compliance (Ctrl+C), run the smoke script (it will time out on compliance counts), start Compliance, rerun the count query → caught up; `kafka-consumer-groups.sh` lag back to 0.
- [ ] **Step 4: README**: phases 3-4 done; new ports (29092 Kafka, 8083 Connect); connector status/registration commands; topic inspection; DLQ (Rabbit) vs DLT (Kafka) and the reason headers; retry settings table; restart/redelivery demo; outbox cleanup + WAL retention note (design section 5); smoke test usage.
- [ ] **Step 5: Full verification**: `verify` on both modules → BUILD SUCCESS.

---

## Self-review notes

- Spec coverage Phase 3: Debezium-only publishing (Task 2), key/event id (Tasks 1-2), partitions/concurrency (Tasks 2-3), idempotent Compliance with commit-before-offset (Tasks 4-5), deterministic demo rule (Task 4), correlation fields preserved (Task 3 schema), retained slot/offsets/data (Task 2 volumes), cleanup documented (Task 7).
- Spec coverage Phase 4: finite retries with backoff, DLQ with reason, commit/ACK boundary (Task 6), result/offset boundary (Task 5), restart/redelivery with persisted state (Task 7).
- Deferred to the next plan: metrics, Prometheus/Grafana, k6, experiment report, architecture/demo-script docs.
