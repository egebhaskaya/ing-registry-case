# Phase 1-2: Foundation and Reliable Registry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **For this project:** the human partner (Ege) types the code. The executor supplies exact code plus a short explanation of the Java syntax and the design reason for each step, then runs the verification command.

**Goal:** A Registry service that returns `202 Accepted` only after RabbitMQ confirms a registration command, and a bounded consumer that writes the user and its outbox event in one PostgreSQL transaction, with a Compliance service skeleton and pinned local infrastructure.

**Architecture:** One Maven build with a root pom and two Spring Boot modules, `registry-service` and `compliance-service`, each using package-by-layer. In Registry, HTTP intake flows `controllers -> services -> messaging` (publish with publisher confirms, admission bounded by a semaphore). Processing flows `listeners -> services -> repositories` (a `@RabbitListener` calls a `@Transactional` service that uses `JdbcTemplate`; the delivery is acknowledged only after that transaction commits). Integration tests run against real PostgreSQL and RabbitMQ containers via Testcontainers `@ServiceConnection`.

**Tech Stack:** Java 25 (Temurin 25.0.5+8), Spring Boot 4.1.2 (Spring Framework 7.0.10, Spring AMQP 4.1.2, Jackson 3.1.7, HikariCP 7.0.2, Flyway 12.4.0), PostgreSQL 17.11, RabbitMQ 4.3.2, Testcontainers 2.0.5, JUnit 6, AssertJ, Awaitility, Mockito, Docker Compose v5.

**Spec:** `docs/design.md` (sections 1-6, 9, 10). Kafka, Debezium, Compliance processing, retries/DLQ refinements, metrics, Grafana, k6 and the README handoff are later plans.

## Global Constraints

- Java 25 for compile, test and run. `<java.version>25</java.version>`; the Enforcer plugin fails the build on any other JDK. Never fall back to Java 21.
- Spring Boot `4.1.2` (Maven coordinate has no `.RELEASE` suffix).
- Base packages `com.example.registry` and `com.example.compliance`; layer packages exactly: `controllers`, `services`, `repositories`, `models`, `dtos`, `listeners`, `messaging`, `config`, `exceptions`. Create a package only when a class goes in it.
- Plain `JdbcTemplate` SQL. No JPA. No interface/implementation pairs.
- No `@Async`, `CompletableFuture` fan-out, `parallelStream` or extra executors in listeners or services. Listener concurrency supplies parallelism.
- `spring.threads.virtual.enabled=true` and `spring.main.keep-alive=true` in the default profile. A `platform` profile sets virtual threads to `false` for the optional comparison.
- Defaults: Hikari `maximum-pool-size: 4`; Rabbit `concurrency: 4`, `prefetch: 8`; intake `max-in-flight: 64`, `admission-wait: 200ms`, `confirm-timeout: 5s`; queue `x-max-length: 20000` with `x-overflow: reject-publish`.
- `POST /registrations`: header `Idempotency-Key` (UUID, required) is the `registrationId`; body has only `fullName`, `email`, `countryCode`. `202` only after a positive publisher confirm with no returned message. `400` for malformed input. `503` with a distinct `reason` for `overloaded`, `broker_rejected`, `unroutable`, `broker_unavailable` and `confirm_timeout`. A timeout is reported as unknown outcome, never as a definite failure.
- Durable queue, persistent messages, publisher confirms (`correlated`) and returns (`mandatory`), dead-letter exchange and queue. `default-requeue-rejected: false`.
- Consumer path is synchronous: listener → transactional service → commit → return → container ACK.
- Duplicate rules: same `registrationId` + same fingerprint → ACK, no new rows; same `registrationId` + different fingerprint → DLQ, original preserved; different `registrationId` + existing email → DLQ, no new rows. Uniqueness enforced by DB constraints; a rolled-back transaction is never reused.
- No status/lookup/polling endpoints, no client app, no authentication.
- Pinned image tags only: `postgres:17.11`, `rabbitmq:4.3.2-management`.
- Logs carry `registrationId` for correlation. Metrics come in a later plan.

## Review Focus

1. `Idempotency-Key` missing or not a UUID → `400` problem detail, never a `500`. Pinned in Task 6 (`RegistrationControllerTest`).
2. A JSON body with an extra unknown field → `400`, because the contract accepts only three fields. Pinned in Task 6 via `spring.jackson.deserialization.fail-on-unknown-properties: true`.
3. Two submissions with different registration IDs whose emails differ only by case or whitespace → exactly one user; the second delivery goes to the DLQ. Pinned in Task 8 (`RegistrationFlowTest`).
4. RabbitMQ unreachable at request time → prompt `503 broker_unavailable`, and the admission permit is released so later requests are not starved. Pinned in Task 6 (`RegistrationIntakeServiceTest`).
5. Redelivery after a crash between DB commit and ACK → the duplicate delivery is acknowledged without a second user or outbox row. Pinned in Task 7 (`RegistrationServiceTest`) and Task 8 (`RegistrationFlowTest`).

---

## File structure

```text
pom.xml                                   root: Boot parent 4.1.2, modules, Enforcer (Java 25)
mvnw, mvnw.cmd, .mvn/wrapper/maven-wrapper.properties   Maven Wrapper (copied from Initializr output)
.gitignore
README.md                                 quickstart: prerequisites, start infra, build, run, stop
infra/compose.yaml                        postgres + rabbitmq, pinned, healthchecks, volumes
infra/postgres-init/01-create-databases.sql   roles + databases `registry` and `compliance`
infra/rabbitmq/enabled_plugins            management + prometheus plugins
user_registry/pom.xml
user_registry/src/main/java/com/example/registry/
  RegistryApplication.java
  config/RegistryRabbitProperties.java    exchange/queue names, max length, confirm timeout
  config/IntakeProperties.java            max in-flight, admission wait
  config/RabbitConfig.java                exchanges, queues, bindings, JSON converter
  controllers/RegistrationController.java
  dtos/RegistrationRequest.java           validated HTTP body
  dtos/RegistrationAcceptedResponse.java  the 202 receipt
  exceptions/IntakeOverloadedException.java
  exceptions/IntakeRejectedException.java
  exceptions/IntakeOutcomeUnknownException.java
  exceptions/ConflictingPayloadException.java
  exceptions/EmailAlreadyRegisteredException.java
  exceptions/ApiExceptionHandler.java     maps intake exceptions to 503 ProblemDetail
  listeners/RegistrationCommandListener.java
  messaging/RegistrationPayload.java      normalized fields carried in the command
  messaging/RegisterUserCommand.java      Rabbit message contract
  messaging/UserRegisteredEvent.java      outbox payload contract
  messaging/PublishOutcome.java           CONFIRMED | REJECTED | UNROUTABLE | UNKNOWN
  messaging/RegistrationCommandPublisher.java
  models/User.java
  models/OutboxEvent.java
  repositories/UserRepository.java
  repositories/OutboxRepository.java
  services/RegistrationNormalizer.java    normalize + fingerprint
  services/RegistrationIntakeService.java admission + publish + outcome mapping
  services/RegistrationService.java       the transaction: user + outbox, idempotency
  services/RegistrationOutcome.java       CREATED | DUPLICATE_IGNORED
user_registry/src/main/resources/application.yml
user_registry/src/main/resources/application-platform.yml
user_registry/src/main/resources/db/migration/V1__users_and_outbox.sql
user_registry/src/test/java/com/example/registry/
  TestcontainersConfiguration.java        Postgres + RabbitMQ @ServiceConnection beans
  AbstractIntegrationTest.java            @SpringBootTest(RANDOM_PORT) + cleanup helpers
  RegistryApplicationTests.java           context loads, Flyway applied
  VirtualThreadVerificationTest.java      real HTTP + Rabbit handler threads are virtual
  controllers/RegistrationControllerTest.java
  listeners/RegistrationFlowTest.java     HTTP → Rabbit → DB end to end
  messaging/RegistrationCommandPublisherTest.java
  services/RegistrationNormalizerTest.java
  services/RegistrationIntakeServiceTest.java
  services/RegistrationServiceTest.java
compliance/pom.xml
compliance/src/main/java/com/example/compliance/ComplianceApplication.java
compliance/src/main/resources/application.yml
compliance/src/main/resources/db/migration/.gitkeep
compliance/src/test/java/com/example/compliance/TestcontainersConfiguration.java
compliance/src/test/java/com/example/compliance/ComplianceApplicationTests.java
```

Test naming: every test class ends in `Test` or `Tests` so `mvnw test` runs all of them, including the container-backed ones. Docker Desktop must be running.

Build command convention (until VS Code is restarted and picks up the new `JAVA_HOME`): run from the repo root in PowerShell with

```powershell
$env:JAVA_HOME="C:\Users\egebh\AppData\Local\Programs\Eclipse Adoptium\jdk-25.0.5+8"; $env:Path="$env:JAVA_HOME\bin;$env:Path"; .\mvnw.cmd -q <goals>
```

---

### Task 1: Maven skeleton for both modules

**Files:**
- Create: `pom.xml`, `.gitignore`, `user_registry/pom.xml`, `compliance/pom.xml`
- Create: `user_registry/src/main/java/com/example/registry/RegistryApplication.java`
- Create: `compliance/src/main/java/com/example/compliance/ComplianceApplication.java`
- Copy: `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties` from the Initializr probe at `C:\Users\egebh\AppData\Local\Temp\claude\c--Users-egebh-OneDrive-Belgeler-GitHub-ing-registry-case\049d397b-d7d6-4b35-822d-c5d3826a3239\scratchpad\initializr-probe\probe\`

**Interfaces:**
- Produces: a multi-module build that compiles on Java 25 and refuses other JDKs.

- [ ] **Step 1: Copy the Maven Wrapper files** (`mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties`) from the probe directory to the repo root. The properties file pins Maven 3.10.0 with `distributionType=only-script`.

- [ ] **Step 2: Write the root `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>4.1.2</version>
    <relativePath/>
  </parent>

  <groupId>com.example</groupId>
  <artifactId>registration-case</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>pom</packaging>
  <name>registration-case</name>

  <modules>
    <module>registry-service</module>
    <module>compliance-service</module>
  </modules>

  <properties>
    <java.version>25</java.version>
  </properties>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-enforcer-plugin</artifactId>
        <executions>
          <execution>
            <id>require-java-25</id>
            <goals>
              <goal>enforce</goal>
            </goals>
            <configuration>
              <rules>
                <requireJavaVersion>
                  <version>[25,26)</version>
                </requireJavaVersion>
              </rules>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 3: Write `user_registry/pom.xml`**

Parent is `com.example:registration-case:0.1.0-SNAPSHOT` with `<relativePath>../pom.xml</relativePath>`; artifactId `registry-service`. Dependencies (versions managed by the Boot parent):

| Scope   | Dependency |
|---------|------------|
| compile | `org.springframework.boot:spring-boot-starter-webmvc`, `spring-boot-starter-validation`, `spring-boot-starter-amqp`, `spring-boot-starter-jdbc`, `spring-boot-starter-flyway`, `spring-boot-starter-actuator` |
| compile | `org.flywaydb:flyway-database-postgresql` |
| runtime | `org.postgresql:postgresql`, `io.micrometer:micrometer-registry-prometheus` |
| test    | `org.springframework.boot:spring-boot-starter-webmvc-test`, `spring-boot-starter-amqp-test`, `spring-boot-starter-jdbc-test`, `spring-boot-testcontainers` |
| test    | `org.testcontainers:testcontainers-postgresql`, `org.testcontainers:testcontainers-rabbitmq` |

Build plugin: `org.springframework.boot:spring-boot-maven-plugin`.

- [ ] **Step 4: Write `compliance/pom.xml`**

Same parent and plugin; artifactId `compliance-service`. Dependencies: the registry list minus everything AMQP and minus `testcontainers-rabbitmq`. Kafka is added in the next plan when it is first used.

- [ ] **Step 5: Write the two application classes**

`RegistryApplication` and `ComplianceApplication`: a `@SpringBootApplication` class with the standard `main` calling `SpringApplication.run`.

- [ ] **Step 6: Write `.gitignore`**: `target/`, `.mvn/wrapper/maven-wrapper.jar`, `.idea/`, `*.iml`, `.vscode/`, `.classpath`, `.project`, `.settings/`, `HELP.md`.

- [ ] **Step 7: Verify the build compiles on Java 25**

Run: `.\mvnw.cmd -q -DskipTests package` (with the Java 25 env prefix).
Expected: exit code 0, `user_registry/target/registry-service-0.1.0-SNAPSHOT.jar` and the compliance jar exist.

- [ ] **Step 8: Verify the Enforcer rejects Java 21**

Run the same command with `JAVA_HOME` pointed at `...\jdk-21.0.12.1+1`.
Expected: build FAILS with a `requireJavaVersion` message.

- [ ] **Step 9: Commit**

```bash
git add pom.xml .gitignore mvnw mvnw.cmd .mvn registry-service compliance-service
git commit -m "build: multi-module Spring Boot 4.1.2 skeleton on Java 25

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Local infrastructure with Docker Compose

**Files:**
- Create: `infra/compose.yaml`, `infra/postgres-init/01-create-databases.sql`, `infra/rabbitmq/enabled_plugins`

**Interfaces:**
- Produces: PostgreSQL on `localhost:5432` with databases `registry` (owner `registry`/`registry`) and `compliance` (owner `compliance`/`compliance`), superuser `postgres`/`postgres`, `wal_level=logical`. RabbitMQ on `5672` (AMQP), `15672` (management UI), `15692` (Prometheus), user `guest`/`guest`.

- [ ] **Step 1: Write `infra/postgres-init/01-create-databases.sql`**

```sql
CREATE ROLE registry LOGIN PASSWORD 'registry';
CREATE DATABASE registry OWNER registry;
CREATE ROLE compliance LOGIN PASSWORD 'compliance';
CREATE DATABASE compliance OWNER compliance;
```

- [ ] **Step 2: Write `infra/rabbitmq/enabled_plugins`**

```erlang
[rabbitmq_management,rabbitmq_prometheus].
```

- [ ] **Step 3: Write `infra/compose.yaml`**

```yaml
name: registration-case

services:
  postgres:
    image: postgres:17.11
    command: ["postgres", "-c", "wal_level=logical"]
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./postgres-init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 3s
      retries: 10

  rabbitmq:
    image: rabbitmq:4.3.2-management
    ports:
      - "5672:5672"
      - "15672:15672"
      - "15692:15692"
    volumes:
      - rabbitmq-data:/var/lib/rabbitmq
      - ./rabbitmq/enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "check_port_connectivity"]
      interval: 10s
      timeout: 5s
      retries: 10

volumes:
  postgres-data:
  rabbitmq-data:
```

`wal_level=logical` is set now so the next plan's Debezium connector does not require wiping the volume.

- [ ] **Step 4: Start and verify**

Run: `docker compose -f infra/compose.yaml up -d --wait`
Expected: both services reported healthy.
Run: `docker compose -f infra/compose.yaml exec postgres psql -U postgres -c "\l"`
Expected: `registry` and `compliance` databases listed.
Run: `curl -s -u guest:guest http://localhost:15672/api/overview | head -c 200`
Expected: JSON starting with `{"management_version"`.
Run: `curl -s http://localhost:15692/metrics | head -3`
Expected: Prometheus text lines beginning with `# TYPE` or `# HELP`.

- [ ] **Step 5: Commit**

```bash
git add infra
git commit -m "infra: pinned PostgreSQL and RabbitMQ compose stack with healthchecks

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Registry configuration, schema and integration-test harness

**Files:**
- Create: `user_registry/src/main/resources/application.yml`, `application-platform.yml`
- Create: `user_registry/src/main/resources/db/migration/V1__users_and_outbox.sql`
- Create: `user_registry/src/test/java/com/example/registry/TestcontainersConfiguration.java`, `AbstractIntegrationTest.java`, `RegistryApplicationTests.java`

**Interfaces:**
- Produces: tables `users` and `outbox`; property prefixes `registry.intake.*` and `registry.rabbit.*` (bound in Task 5); `AbstractIntegrationTest` with `protected JdbcTemplate jdbcTemplate` and `protected void truncateTables()`.

- [ ] **Step 1: Write `application.yml`**

```yaml
spring:
  application:
    name: registry-service
  threads:
    virtual:
      enabled: true
  main:
    keep-alive: true
  datasource:
    url: jdbc:postgresql://localhost:5432/registry
    username: registry
    password: registry
    hikari:
      maximum-pool-size: 4
  flyway:
    enabled: true
  rabbitmq:
    host: localhost
    port: 5672
    username: guest
    password: guest
    publisher-confirm-type: correlated
    publisher-returns: true
    template:
      mandatory: true
    listener:
      simple:
        concurrency: 4
        max-concurrency: 4
        prefetch: 8
        acknowledge-mode: auto
        default-requeue-rejected: false
  mvc:
    problemdetails:
      enabled: true
  jackson:
    deserialization:
      fail-on-unknown-properties: true

server:
  port: 8080

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      show-details: always

registry:
  intake:
    max-in-flight: 64
    admission-wait: 200ms
  rabbit:
    exchange: registration
    routing-key: registration.register
    queue: registration.commands
    dead-letter-exchange: registration.dlx
    dead-letter-queue: registration.commands.dlq
    queue-max-length: 20000
    confirm-timeout: 5s
```

`application-platform.yml` contains only `spring.threads.virtual.enabled: false`.

- [ ] **Step 2: Write `V1__users_and_outbox.sql`**

```sql
CREATE TABLE users (
    id                  UUID PRIMARY KEY,
    registration_id     UUID NOT NULL,
    payload_fingerprint VARCHAR(64) NOT NULL,
    email               VARCHAR(254) NOT NULL,
    full_name           VARCHAR(100) NOT NULL,
    country_code        CHAR(2) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    CONSTRAINT users_registration_id_key UNIQUE (registration_id),
    CONSTRAINT users_email_key UNIQUE (email)
);

CREATE TABLE outbox (
    id            UUID PRIMARY KEY,
    aggregatetype VARCHAR(255) NOT NULL,
    aggregateid   VARCHAR(255) NOT NULL,
    type          VARCHAR(255) NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);
```

Outbox column names match the Debezium Outbox Event Router defaults (`id`, `aggregatetype`, `aggregateid`, `type`, `payload`) so the next plan needs no field remapping.

- [ ] **Step 3: Write `TestcontainersConfiguration`**

`@TestConfiguration(proxyBeanMethods = false)` with two `@Bean @ServiceConnection` methods: `PostgreSQLContainer postgresContainer()` using `DockerImageName.parse("postgres:17.11")` and `RabbitMQContainer rabbitContainer()` using `"rabbitmq:4.3.2-management"`. Imports come from `org.testcontainers.postgresql` and `org.testcontainers.rabbitmq` (Testcontainers 2 package names).

- [ ] **Step 4: Write `AbstractIntegrationTest`**

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
public abstract class AbstractIntegrationTest {
    @Autowired protected JdbcTemplate jdbcTemplate;
    protected void truncateTables() { jdbcTemplate.execute("TRUNCATE TABLE users, outbox"); }
}
```

If `@Import` on the superclass is not picked up (context fails to find a datasource), move both annotations onto each concrete test class.

- [ ] **Step 5: Write the failing test `RegistryApplicationTests`**

```java
class RegistryApplicationTests extends AbstractIntegrationTest {
    @Test
    void flywayCreatedTheTables() {
        Integer users = jdbcTemplate.queryForObject("SELECT count(*) FROM users", Integer.class);
        Integer outbox = jdbcTemplate.queryForObject("SELECT count(*) FROM outbox", Integer.class);
        assertThat(users).isZero();
        assertThat(outbox).isZero();
    }
}
```

- [ ] **Step 6: Run it**

Run: `.\mvnw.cmd -q -f user_registry/pom.xml test -Dtest=RegistryApplicationTests`
Expected: PASS. Console shows Flyway `Successfully applied 1 migration` and two containers starting.

- [ ] **Step 7: Verify the real application starts against Compose**

Run: `.\mvnw.cmd -q -f user_registry/pom.xml spring-boot:run` in one terminal; `curl -s http://localhost:8080/actuator/health` in another.
Expected: `{"status":"UP", ...}` with `db` and `rabbit` components UP. Stop with Ctrl+C.

- [ ] **Step 8: Commit**

```bash
git add registry-service
git commit -m "feat(registry): configuration, Flyway schema and container test harness

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Request contract, normalization and fingerprint

**Files:**
- Create: `dtos/RegistrationRequest.java`, `messaging/RegistrationPayload.java`, `services/RegistrationNormalizer.java`
- Test: `services/RegistrationNormalizerTest.java` (pure unit test, no Spring)

**Interfaces:**
- Produces:
  - `record RegistrationRequest(@NotBlank @Size(max = 100) String fullName, @NotBlank @Email @Size(max = 254) String email, @NotBlank @Pattern(regexp = "^[A-Za-z]{2}$") String countryCode)`
  - `record RegistrationPayload(String fullName, String email, String countryCode)`
  - `@Service class RegistrationNormalizer { RegistrationPayload normalize(RegistrationRequest request); String fingerprint(RegistrationPayload payload); }`

Normalization rules (pinned): `fullName` trimmed and internal whitespace collapsed to one space, case preserved; `email` trimmed and lowercased; `countryCode` trimmed and uppercased.

- [ ] **Step 1: Write the failing tests**

```java
class RegistrationNormalizerTest {
    private final RegistrationNormalizer normalizer = new RegistrationNormalizer();

    @Test
    void normalizesWhitespaceAndCase() {
        var payload = normalizer.normalize(new RegistrationRequest("  Ada   Lovelace ", " Ada.Lovelace@Example.COM ", " nl "));
        assertThat(payload).isEqualTo(new RegistrationPayload("Ada Lovelace", "ada.lovelace@example.com", "NL"));
    }

    @Test
    void fingerprintIsStableAcrossCosmeticDifferences() {
        var a = normalizer.normalize(new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL"));
        var b = normalizer.normalize(new RegistrationRequest(" Ada  Lovelace", "ADA@example.com", "nl"));
        assertThat(normalizer.fingerprint(a)).isEqualTo(normalizer.fingerprint(b));
    }

    @Test
    void fingerprintChangesWhenContentChanges() { /* different fullName → different fingerprint */ }

    @Test
    void fingerprintIsSha256Hex() { /* matches "^[0-9a-f]{64}$" */ }
}
```

- [ ] **Step 2: Run to verify failure**: `.\mvnw.cmd -q -f user_registry/pom.xml test -Dtest=RegistrationNormalizerTest` → compilation error, classes missing.

- [ ] **Step 3: Implement** `RegistrationRequest`, `RegistrationPayload`, `RegistrationNormalizer`. Fingerprint = SHA-256 of `fullName + "\n" + email + "\n" + countryCode` (UTF-8) rendered as lowercase hex via `HexFormat.of().formatHex(...)`.

- [ ] **Step 4: Run to verify pass**: same command → PASS.

- [ ] **Step 5: Commit** `feat(registry): request contract, normalization and payload fingerprint`.

---

### Task 5: Rabbit topology and confirmed publisher

**Files:**
- Create: `config/RegistryRabbitProperties.java`, `config/IntakeProperties.java`, `config/RabbitConfig.java`
- Create: `messaging/RegisterUserCommand.java`, `messaging/PublishOutcome.java`, `messaging/RegistrationCommandPublisher.java`
- Test: `messaging/RegistrationCommandPublisherTest.java`

**Interfaces:**
- Consumes: `RegistrationPayload` (Task 4).
- Produces:
  - `@ConfigurationProperties("registry.rabbit") record RegistryRabbitProperties(String exchange, String routingKey, String queue, String deadLetterExchange, String deadLetterQueue, int queueMaxLength, Duration confirmTimeout)`
  - `@ConfigurationProperties("registry.intake") record IntakeProperties(int maxInFlight, Duration admissionWait)`
  - `record RegisterUserCommand(UUID registrationId, String payloadFingerprint, RegistrationPayload payload, Instant submittedAt)`
  - `enum PublishOutcome { CONFIRMED, REJECTED, UNROUTABLE, UNKNOWN }`
  - `@Component class RegistrationCommandPublisher { PublishOutcome publish(RegisterUserCommand command); }` — public constructor `(RabbitTemplate, RegistryRabbitProperties)` so a test can build one with a bad routing key.
  - Beans in `RabbitConfig`: `DirectExchange registrationExchange`, `FanoutExchange registrationDeadLetterExchange`, `Queue registrationQueue` (durable; args `x-dead-letter-exchange`, `x-max-length`, `x-overflow=reject-publish`), `Queue registrationDeadLetterQueue` (durable), two `Binding`s, `MessageConverter jsonMessageConverter()` returning `new JacksonJsonMessageConverter()`. Annotate the class `@EnableConfigurationProperties({RegistryRabbitProperties.class, IntakeProperties.class})`.

- [ ] **Step 1: Write the failing tests** (extends `AbstractIntegrationTest`; `@Autowired RegistrationCommandPublisher publisher; RabbitTemplate rabbitTemplate; RegistryRabbitProperties props; AmqpAdmin amqpAdmin`; `@BeforeEach` purges `props.queue()` and `props.deadLetterQueue()`).

```java
@Test
void confirmedPublishLandsInTheRegistrationQueue() {
    var command = sampleCommand();                       // helper building a RegisterUserCommand
    assertThat(publisher.publish(command)).isEqualTo(PublishOutcome.CONFIRMED);
    Message message = rabbitTemplate.receive(props.queue(), 5000);
    assertThat(message).isNotNull();
    assertThat(new String(message.getBody(), StandardCharsets.UTF_8)).contains(command.registrationId().toString());
    assertThat(message.getMessageProperties().getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
}

@Test
void unroutableMessageIsReportedNotConfirmed() {
    var badProps = new RegistryRabbitProperties(props.exchange(), "no.such.key", props.queue(), props.deadLetterExchange(), props.deadLetterQueue(), props.queueMaxLength(), props.confirmTimeout());
    var badPublisher = new RegistrationCommandPublisher(rabbitTemplate, badProps);
    assertThat(badPublisher.publish(sampleCommand())).isEqualTo(PublishOutcome.UNROUTABLE);
    assertThat(rabbitTemplate.receive(props.queue(), 1000)).isNull();
}
```

- [ ] **Step 2: Run to verify failure**: `-Dtest=RegistrationCommandPublisherTest` → compilation error.

- [ ] **Step 3: Implement the properties records, `RabbitConfig`, the command record, the enum and the publisher.**

Publisher algorithm (the one place the signature does not determine the body):

```java
var correlation = new CorrelationData(command.registrationId().toString());
rabbitTemplate.convertAndSend(props.exchange(), props.routingKey(), command, correlation);
try {
    CorrelationData.Confirm confirm = correlation.getFuture().get(props.confirmTimeout().toMillis(), TimeUnit.MILLISECONDS);
    if (!confirm.isAck()) return PublishOutcome.REJECTED;
    if (correlation.getReturned() != null) return PublishOutcome.UNROUTABLE;
    return PublishOutcome.CONFIRMED;
} catch (TimeoutException | ExecutionException e) {
    return PublishOutcome.UNKNOWN;
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    return PublishOutcome.UNKNOWN;
}
```

A connection failure thrown by `convertAndSend` (`AmqpException`) propagates; Task 6 maps it.

- [ ] **Step 4: Run to verify pass** → PASS. Then open `http://localhost:15672` (guest/guest) against the Compose broker after a `spring-boot:run` and confirm `registration.commands` and `registration.commands.dlq` exist with the expected arguments.

- [ ] **Step 5: Commit** `feat(registry): durable Rabbit topology and confirmed command publisher`.

---

### Task 6: Intake service, controller and error mapping

**Files:**
- Create: `dtos/RegistrationAcceptedResponse.java`, `services/RegistrationIntakeService.java`, `controllers/RegistrationController.java`
- Create: `exceptions/IntakeOverloadedException.java`, `IntakeRejectedException.java`, `IntakeOutcomeUnknownException.java`, `ApiExceptionHandler.java`
- Test: `services/RegistrationIntakeServiceTest.java` (Mockito unit test), `controllers/RegistrationControllerTest.java` (integration, real server)

**Interfaces:**
- Consumes: `RegistrationNormalizer`, `RegistrationCommandPublisher`, `IntakeProperties`, `PublishOutcome`.
- Produces:
  - `record RegistrationAcceptedResponse(UUID registrationId, String status, Instant acceptedAt)` with `status` always `"ACCEPTED"`.
  - `@Service class RegistrationIntakeService { RegistrationAcceptedResponse accept(UUID registrationId, RegistrationRequest request); int availablePermits(); }` constructor `(RegistrationNormalizer, RegistrationCommandPublisher, IntakeProperties)`.
  - Exceptions (all `extends RuntimeException`): `IntakeOverloadedException()`, `IntakeRejectedException(String reason)` with accessor `reason()`, `IntakeOutcomeUnknownException()`.
  - `@RestController class RegistrationController` with `@PostMapping("/registrations") @ResponseStatus(HttpStatus.ACCEPTED) RegistrationAcceptedResponse register(@RequestHeader("Idempotency-Key") UUID idempotencyKey, @Valid @RequestBody RegistrationRequest request)`.
  - `@RestControllerAdvice class ApiExceptionHandler` returning `ProblemDetail` with status 503 and a `reason` property: `overloaded`, the exception's reason, or `confirm_timeout`. The unknown-outcome detail text: `"Publication outcome unknown; retry with the same Idempotency-Key"`.

- [ ] **Step 1: Write the failing unit tests** (`@ExtendWith(MockitoExtension.class)`, `@Mock RegistrationCommandPublisher publisher`, real `RegistrationNormalizer`, `IntakeProperties(2, Duration.ofMillis(100))`).

```java
@Test void confirmedPublishReturnsReceiptWithSameRegistrationId()      // also captures the command: payload normalized, fingerprint 64 hex, submittedAt not null
@Test void brokerRejectionBecomes503Rejected()                         // REJECTED → IntakeRejectedException with reason "broker_rejected"
@Test void unroutableBecomes503Rejected()                              // UNROUTABLE → reason "unroutable"
@Test void unknownOutcomeBecomesOutcomeUnknown()                       // UNKNOWN → IntakeOutcomeUnknownException
@Test void brokerConnectionFailureReleasesThePermit()                  // publisher throws new AmqpConnectException(new RuntimeException("down")) → IntakeRejectedException "broker_unavailable"; availablePermits() == 2
@Test void overloadedWhenAllPermitsAreHeld()                           // IntakeProperties(1, 100ms); publisher blocks on a CountDownLatch; second call on another thread → IntakeOverloadedException within ~100ms; after release, availablePermits() == 1
```

- [ ] **Step 2: Run** `-Dtest=RegistrationIntakeServiceTest` → compilation error.

- [ ] **Step 3: Implement the DTO, exceptions and `RegistrationIntakeService`.** Admission is a `java.util.concurrent.Semaphore(maxInFlight)`; `tryAcquire(admissionWait)`; release in `finally`. Map `AmqpException` from the publisher to `IntakeRejectedException("broker_unavailable")`. Log at INFO with `registrationId` and outcome.

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Write the failing controller tests** (extends `AbstractIntegrationTest`, annotated `@AutoConfigureRestTestClient`, `@Autowired RestTestClient client`).

```java
@Test void acceptsValidRegistrationWith202()        // header UUID, body valid → isAccepted(); jsonPath $.registrationId == header; $.status == "ACCEPTED"
@Test void missingIdempotencyKeyIs400()
@Test void nonUuidIdempotencyKeyIs400()
@Test void invalidEmailIs400()
@Test void unknownJsonFieldIs400()                   // body {"fullName":"A","email":"a@b.c","countryCode":"NL","nickname":"x"}
```

- [ ] **Step 6: Run** `-Dtest=RegistrationControllerTest` → failures (404 / missing classes).

- [ ] **Step 7: Implement `RegistrationController` and `ApiExceptionHandler`.** Spring's own problem-details support already turns missing header, bad UUID, bean-validation and unknown-field errors into 400.

- [ ] **Step 8: Run** → PASS.

- [ ] **Step 9: Manual check against Compose**: `spring-boot:run`, then

```powershell
curl.exe -i -X POST http://localhost:8080/registrations -H "Content-Type: application/json" -H "Idempotency-Key: 11111111-1111-1111-1111-111111111111" -d '{"fullName":"Ada Lovelace","email":"ada@example.com","countryCode":"NL"}'
```

Expected: `HTTP/1.1 202` with a JSON receipt, and one ready message visible on `registration.commands` in the management UI (nothing consumes it yet).

- [ ] **Step 10: Commit** `feat(registry): bounded 202 intake with confirmed publish and problem-detail errors`.

---

### Task 7: The registration transaction: user + outbox, idempotent

**Files:**
- Create: `models/User.java`, `models/OutboxEvent.java`, `messaging/UserRegisteredEvent.java`, `services/RegistrationOutcome.java`
- Create: `repositories/UserRepository.java`, `repositories/OutboxRepository.java`, `services/RegistrationService.java`
- Create: `exceptions/ConflictingPayloadException.java`, `exceptions/EmailAlreadyRegisteredException.java`
- Test: `services/RegistrationServiceTest.java`

**Interfaces:**
- Consumes: `RegisterUserCommand`, `RegistrationPayload`.
- Produces:
  - `record User(UUID id, UUID registrationId, String payloadFingerprint, String email, String fullName, String countryCode, Instant createdAt)`
  - `record OutboxEvent(UUID id, String aggregateType, String aggregateId, String type, String payload, Instant createdAt)`
  - `record UserRegisteredEvent(UUID userId, UUID registrationId, String email, String fullName, String countryCode, Instant submittedAt, Instant registeredAt)`
  - `enum RegistrationOutcome { CREATED, DUPLICATE_IGNORED }`
  - `@Repository class UserRepository { Optional<User> findByRegistrationId(UUID); void insert(User); boolean existsByEmail(String); long count(); }`
  - `@Repository class OutboxRepository { void insert(OutboxEvent); List<OutboxEvent> findAll(); long count(); }` — insert uses `?::jsonb` for `payload`.
  - `@Service class RegistrationService { @Transactional RegistrationOutcome register(RegisterUserCommand); @Transactional(readOnly = true) RegistrationOutcome resolveDuplicate(RegisterUserCommand); }` constructor `(UserRepository, OutboxRepository, tools.jackson.databind.ObjectMapper)`. Aggregate type `"user"`, event type `"UserRegistered"`, `aggregateId = userId.toString()`.
  - Exceptions (`extends RuntimeException`, message carries the registrationId): `ConflictingPayloadException`, `EmailAlreadyRegisteredException`.

Decision rules:
- `register`: look up by `registrationId`. Found + same fingerprint → `DUPLICATE_IGNORED`. Found + different fingerprint → throw `ConflictingPayloadException`. Not found → insert user (new `UUID.randomUUID()`, `createdAt = Instant.now()`), insert outbox row with the serialized `UserRegisteredEvent` → `CREATED`. A `DuplicateKeyException` from either insert propagates and rolls the transaction back.
- `resolveDuplicate` (called by the listener after a `DuplicateKeyException`, in a fresh transaction): found by `registrationId` → same rules as above; not found → `existsByEmail` true → throw `EmailAlreadyRegisteredException`; otherwise throw `IllegalStateException("Unexplained uniqueness violation")`.

- [ ] **Step 1: Write the failing tests** (extends `AbstractIntegrationTest`; `@Autowired RegistrationService service, UserRepository users`; `@MockitoSpyBean OutboxRepository outbox`; `@BeforeEach truncateTables()`).

```java
@Test void createsOneUserAndOneOutboxEvent()           // CREATED; users.count()==1; outbox.findAll() has one row: type "UserRegistered", aggregateType "user", aggregateId == stored user id, payload contains registrationId
@Test void sameCommandTwiceIsIgnoredTheSecondTime()     // second call DUPLICATE_IGNORED; counts stay 1 and 1
@Test void sameRegistrationIdDifferentPayloadConflicts()// ConflictingPayloadException; counts stay 1 and 1
@Test void sameEmailUnderNewRegistrationIdIsPermanentConflict() // register(second) throws DuplicateKeyException; resolveDuplicate(second) throws EmailAlreadyRegisteredException; counts 1 and 1
@Test void outboxFailureRollsBackTheUser()              // doThrow(new RuntimeException("boom")).when(outbox).insert(any()); register throws; users.count()==0
```

- [ ] **Step 2: Run** `-Dtest=RegistrationServiceTest` → compilation error.

- [ ] **Step 3: Implement models, event record, repositories, service and the two exceptions.** Row mapping reads UUIDs with `rs.getObject("id", UUID.class)` and timestamps with `rs.getObject("created_at", OffsetDateTime.class).toInstant()`.

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit** `feat(registry): atomic user+outbox registration with idempotency rules`.

---

### Task 8: Bounded Rabbit listener and the end-to-end flow

**Files:**
- Create: `listeners/RegistrationCommandListener.java`
- Test: `listeners/RegistrationFlowTest.java`

**Interfaces:**
- Consumes: `RegistrationService`, `RegisterUserCommand`, `RegistryRabbitProperties`.
- Produces: `@Component class RegistrationCommandListener { @RabbitListener(queues = "${registry.rabbit.queue}") void onRegisterUser(RegisterUserCommand command); }`

Listener rules: call `register`; on `DuplicateKeyException` call `resolveDuplicate`; on `ConflictingPayloadException` or `EmailAlreadyRegisteredException` log at WARN with `registrationId` and throw `AmqpRejectAndDontRequeueException(reason, cause)`; any other exception propagates (with `default-requeue-rejected=false` it dead-letters; the next plan adds finite retry for transient failures). Log the outcome at INFO with `registrationId` and `outcome`.

- [ ] **Step 1: Write the failing tests** (extends `AbstractIntegrationTest`, `@AutoConfigureRestTestClient`; `@BeforeEach` truncates tables and purges both queues; helper `post(UUID key, String fullName, String email, String country)` returning the status).

```java
@Test void acceptedRequestBecomesOneUserAndOneOutboxRow()   // 202; await().atMost(10s) until users.count()==1 and outbox.count()==1
@Test void sameRequestTwiceCreatesOnlyOneUser()             // two 202s with the same key; await users==1; then await().pollDelay(1s).atMost(3s).untilAsserted(users==1, outbox==1); DLQ empty
@Test void sameKeyDifferentPayloadGoesToDlq()               // second 202 with same key, different name; DLQ receives a message within 10s; users==1
@Test void sameEmailDifferentCaseUnderNewKeyGoesToDlq()     // second key, email " ADA@Example.com "; DLQ message; users==1
```

- [ ] **Step 2: Run** `-Dtest=RegistrationFlowTest` → first test fails (rows never appear).

- [ ] **Step 3: Implement `RegistrationCommandListener`.**

- [ ] **Step 4: Run** → PASS. Run the whole module: `.\mvnw.cmd -q -f user_registry/pom.xml test` → PASS.

- [ ] **Step 5: Manual check**: `spring-boot:run`; repeat the Task 6 curl; `psql -U registry -d registry -c "SELECT email, registration_id FROM users"` through Compose shows the row; log line shows `outcome=CREATED`.

- [ ] **Step 6: Commit** `feat(registry): bounded listener completes registrations after commit`.

---

### Task 9: Verify virtual threads on real HTTP and Rabbit handlers

**Files:**
- Test: `VirtualThreadVerificationTest.java` (test-only controller and listener declared in a nested `@TestConfiguration`)

**Interfaces:**
- Consumes: `RabbitTemplate`, the auto-configured listener container factory.

- [ ] **Step 1: Write the test**

```java
@AutoConfigureRestTestClient
class VirtualThreadVerificationTest extends AbstractIntegrationTest {

    @TestConfiguration
    static class Probes {
        @RestController
        static class ThreadProbeController {
            @GetMapping("/__test/thread")
            Map<String, Object> thread() {
                var t = Thread.currentThread();
                return Map.of("virtual", t.isVirtual(), "name", t.getName());
            }
        }
        @Component
        static class ThreadProbeListener {
            final CompletableFuture<Boolean> observed = new CompletableFuture<>();
            @RabbitListener(queuesToDeclare = @Queue("test.thread-probe"))
            void on(String ignored) { observed.complete(Thread.currentThread().isVirtual()); }
        }
    }

    @Test void httpHandlerRunsOnAVirtualThread()   // GET /__test/thread → jsonPath $.virtual == true
    @Test void rabbitListenerRunsOnAVirtualThread() // convertAndSend("test.thread-probe", "ping"); observed.get(10s) == true
}
```

Expected on first run: PASS already if Task 3's YAML is right. This task's value is the evidence. If `virtual` is `false`, fix the YAML, do not change the test.

- [ ] **Step 2: Run** `-Dtest=VirtualThreadVerificationTest` → PASS. Also run once with `-Dspring.profiles.active=platform` → both tests FAIL (proves the switch works), then run without the profile again → PASS.

- [ ] **Step 3: Commit** `test(registry): prove HTTP and Rabbit handlers execute on virtual threads`.

---

### Task 10: Compliance skeleton, README quickstart, full build

**Files:**
- Create: `compliance/src/main/resources/application.yml`, `db/migration/.gitkeep`
- Create: `compliance/src/test/java/com/example/compliance/TestcontainersConfiguration.java`, `ComplianceApplicationTests.java`
- Create: `README.md`

**Interfaces:**
- Produces: Compliance on port `8081` with datasource `jdbc:postgresql://localhost:5432/compliance` (`compliance`/`compliance`), Flyway enabled with no migrations yet, virtual threads + keep-alive, actuator `health,info,metrics,prometheus`.

- [ ] **Step 1: Write `application.yml`** mirroring Registry's structure without the Rabbit and intake blocks; `server.port: 8081`.

- [ ] **Step 2: Write the failing test**: `@SpringBootTest(RANDOM_PORT) @Import(TestcontainersConfiguration.class) @AutoConfigureRestTestClient class ComplianceApplicationTests` with `healthIsUp()` → `GET /actuator/health` is 200 and `$.status == "UP"`. `TestcontainersConfiguration` has only the Postgres container.

- [ ] **Step 3: Run** `.\mvnw.cmd -q -f compliance/pom.xml test` → PASS.

- [ ] **Step 4: Write `README.md`** (English): prerequisites (Java 25, Docker Desktop), `docker compose -f infra/compose.yaml up -d --wait`, build `.\mvnw.cmd -q verify`, run each service with `.\mvnw.cmd -f <folder>/pom.xml spring-boot:run`, the curl from Task 6, management UI and actuator URLs, stop with `docker compose -f infra/compose.yaml down` (add `-v` to wipe data), and the package-by-layer table from the design.

- [ ] **Step 5: Full verification**: `.\mvnw.cmd -q verify` from the root → BUILD SUCCESS, all tests green.

- [ ] **Step 6: Commit** `feat(compliance): service skeleton with health check; docs: quickstart README`.

---

## Self-review notes

- Spec coverage for Phases 1-2: Java 25 everywhere (Task 1 Enforcer), two layered apps (Tasks 1, 10), Maven Wrapper (Task 1), pinned Compose with health checks (Task 2), Flyway (Task 3), 202 contract and error rules (Tasks 5-6), bounded publishing (Task 6 semaphore + confirm timeout), bounded consumers and prefetch (Task 3 YAML, Task 8), atomic user/outbox with idempotency and race handling (Task 7), commit-before-ACK (Task 8), virtual-thread verification on real handlers (Task 9), startup/shutdown documentation (Task 10).
- Deliberately deferred to the next plans: Kafka, Debezium connector, Compliance processing and schema, finite retry with backoff, DLQ reason headers, metrics and dashboards, k6.
- Deviation from the design's folder sketch: the project sits at the repository root rather than in a nested `registration-case/` directory.
