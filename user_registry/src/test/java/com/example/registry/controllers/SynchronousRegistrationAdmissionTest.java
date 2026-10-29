package com.example.registry.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationPayload;
import com.example.registry.models.User;
import com.example.registry.repositories.OutboxRepository;
import com.example.registry.repositories.UserRepository;
import com.example.registry.services.RegistrationNormalizer;
import com.example.registry.services.RegistrationProcessor;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Holds real transactions open to verify the execution limit and bounded HTTP waiting room. */
@AutoConfigureRestTestClient
@TestPropertySource(properties = {
        "spring.rabbitmq.listener.simple.auto-startup=false",
        "registry.processing.max-concurrency=2",
        "registry.processing.max-waiting-requests=1",
        "registry.processing.admission-wait=2s"
})
class SynchronousRegistrationAdmissionTest extends AbstractIntegrationTest {

    @Autowired
    RestTestClient client;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    RegistrationProcessor processor;
    @Autowired
    RegistrationNormalizer normalizer;
    @Autowired
    OutboxRepository outbox;
    @MockitoSpyBean
    UserRepository users;

    @BeforeEach
    void cleanDatabase() {
        truncateTables();
    }

    @Test
    void twoExecuteOneWaitsAndAnAdditionalRequestIsRejectedWithoutDatabaseWork() throws Exception {
        BlockedTransactions blocked = blockUserInserts();
        UUID rejectedKey = UUID.randomUUID();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> post(UUID.randomUUID()));
            var second = executor.submit(() -> post(UUID.randomUUID()));
            try {
                assertThat(blocked.entered().await(10, TimeUnit.SECONDS)).isTrue();
                var waiting = executor.submit(() -> post(UUID.randomUUID()));
                await().atMost(Duration.ofSeconds(1)).until(() -> processor.waitingRequests() == 1);

                Reply rejected = post(rejectedKey);

                assertOverloaded(rejected);
                assertThat(processor.availablePermits()).isZero();
                assertThat(blocked.maximum().get()).isEqualTo(2);
                blocked.release().countDown();
                assertThat(first.get(10, TimeUnit.SECONDS).status()).isEqualTo(201);
                assertThat(second.get(10, TimeUnit.SECONDS).status()).isEqualTo(201);
                assertThat(waiting.get(10, TimeUnit.SECONDS).status()).isEqualTo(201);
            } finally {
                blocked.release().countDown();
            }
        }
        assertThat(users.findByRegistrationId(rejectedKey)).isEmpty();
        assertThat(users.count()).isEqualTo(3);
        assertThat(outbox.count()).isEqualTo(3);
        assertThat(processor.availablePermits()).isEqualTo(2);
        assertThat(processor.waitingRequests()).isZero();
        assertThat(blocked.maximum().get()).isEqualTo(2);
    }

    @Test
    void consumerAndHttpShareCapacityAndAWaitingHttpRequestTimesOutSafely() throws Exception {
        BlockedTransactions blocked = blockUserInserts();
        UUID rejectedKey = UUID.randomUUID();
        RegistrationPayload payload = new RegistrationPayload("Consumer User", "consumer@example.com", "NL");
        RegisterUserCommand command = new RegisterUserCommand(UUID.randomUUID(), normalizer.fingerprint(payload),
                payload, Instant.now());
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var consumer = executor.submit(() -> processor.processConsumer(command));
            var http = executor.submit(() -> post(UUID.randomUUID()));
            try {
                assertThat(blocked.entered().await(10, TimeUnit.SECONDS)).isTrue();

                Reply rejected = post(rejectedKey);

                assertOverloaded(rejected);
                assertThat(processor.waitingRequests()).isZero();
                assertThat(blocked.maximum().get()).isEqualTo(2);
                blocked.release().countDown();
                consumer.get(10, TimeUnit.SECONDS);
                assertThat(http.get(10, TimeUnit.SECONDS).status()).isEqualTo(201);
            } finally {
                blocked.release().countDown();
            }
        }
        assertThat(users.findByRegistrationId(rejectedKey)).isEmpty();
        assertThat(users.count()).isEqualTo(2);
        assertThat(outbox.count()).isEqualTo(2);
        // A rejected request is safe to retry; its waiting slot and all execution permits were released.
        assertThat(post(rejectedKey).status()).isEqualTo(201);
        assertThat(processor.availablePermits()).isEqualTo(2);
        assertThat(processor.waitingRequests()).isZero();
        assertThat(users.count()).isEqualTo(3);
        assertThat(outbox.count()).isEqualTo(3);
    }

    private BlockedTransactions blockUserInserts() {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        doAnswer(invocation -> {
            int current = active.incrementAndGet();
            maximum.accumulateAndGet(current, Math::max);
            entered.countDown();
            try {
                if (!release.await(15, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Test did not release the blocked transactions");
                }
                return invocation.callRealMethod();
            } finally {
                active.decrementAndGet();
            }
        }).when(users).insert(any(User.class));
        return new BlockedTransactions(entered, release, maximum);
    }

    private Reply post(UUID key) {
        var result = client.post().uri("/registrations/sync")
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("HTTP User", "sync-" + key + "@example.com", "NL"))
                .exchange().expectBody(String.class).returnResult();
        return new Reply(result.getStatus().value(), objectMapper.readTree(result.getResponseBody()),
                result.getResponseHeaders());
    }

    private static void assertOverloaded(Reply reply) {
        assertThat(reply.status()).isEqualTo(503);
        assertThat(reply.body().path("reason").asText()).isEqualTo("sync_overloaded");
        assertThat(reply.headers().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");
    }

    private record Reply(int status, JsonNode body, HttpHeaders headers) {
    }

    private record BlockedTransactions(CountDownLatch entered, CountDownLatch release, AtomicInteger maximum) {
    }
}
