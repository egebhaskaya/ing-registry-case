package com.example.registry.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.config.RegistryRabbitProperties;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.repositories.OutboxRepository;
import com.example.registry.repositories.UserRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Stopped Rabbit consumers distinguish queued async work from direct synchronous transactions. */
@AutoConfigureRestTestClient
class SharedQueueRegistrationTest extends AbstractIntegrationTest {
    @Autowired RestTestClient client;
    @Autowired ObjectMapper objectMapper;
    @Autowired RabbitListenerEndpointRegistry listeners;
    @Autowired AmqpAdmin admin;
    @Autowired RegistryRabbitProperties rabbit;
    @Autowired UserRepository users;
    @Autowired MeterRegistry meters;
    @MockitoSpyBean OutboxRepository outbox;

    @BeforeEach
    void pauseAndClean() {
        listeners.stop();
        admin.purgeQueue(rabbit.queue(), false);
        admin.purgeQueue(rabbit.deadLetterQueue(), false);
        truncateTables();
    }

    @AfterEach
    void resume() {
        listeners.start();
    }

    @Test
    void syncCommitsWithoutQueueingWhileAsyncWaitsForRabbitConsumers() {
        UUID asyncKey = UUID.randomUUID();
        UUID syncKey = UUID.randomUUID();

        assertThat(post("/registrations", asyncKey).status()).isEqualTo(202);
        assertThat(queuedMessages()).isEqualTo(1);
        assertThat(users.count()).isZero();

        Reply completed = post("/registrations/sync", syncKey);

        assertThat(completed.status()).isEqualTo(201);
        assertThat(users.findByRegistrationId(syncKey)).isPresent();
        assertThat(users.findByRegistrationId(asyncKey)).isEmpty();
        assertThat(outbox.count()).isEqualTo(1);
        assertThat(queuedMessages()).isEqualTo(1);

        Reply replay = post("/registrations/sync", syncKey);
        assertThat(replay.status()).isEqualTo(200);
        assertThat(replay.body()).isEqualTo(completed.body());
        assertThat(outbox.count()).isEqualTo(1);
        assertThat(queuedMessages()).isEqualTo(1);

        listeners.start();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(users.findByRegistrationId(asyncKey)).isPresent();
            assertThat(users.count()).isEqualTo(2);
            assertThat(outbox.count()).isEqualTo(2);
            assertThat(meters.get("registry.workers.active").gauge().value()).isZero();
        });
    }

    @Test
    void syncCannotReturnBeforeTheDatabaseTransactionCommits() throws Exception {
        var processing = meters.get("registry.registration.processing").tag("mode", "sync").timer();
        long processingBefore = processing.count();
        UUID syncKey = UUID.randomUUID();
        CountDownLatch beforeCommit = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void beforeCommit(boolean readOnly) {
                    beforeCommit.countDown();
                    try {
                        if (!allowCommit.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Test did not release the transaction");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
            });
            return result;
        }).when(outbox).insert(any());

        try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
            var sync = requests.submit(() -> post("/registrations/sync", syncKey));
            try {
                assertThat(beforeCommit.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(sync.isDone()).isFalse();
                assertThat(users.findByRegistrationId(syncKey)).isEmpty();
                assertThat(outbox.count()).isZero();
                assertThat(queuedMessages()).isZero();
                assertThat(meters.get("registry.workers.active").gauge().value()).isEqualTo(1);
                assertThat(processing.count()).isEqualTo(processingBefore);
            } finally {
                allowCommit.countDown();
            }

            assertThat(sync.get(5, TimeUnit.SECONDS).status()).isEqualTo(201);
            assertThat(users.findByRegistrationId(syncKey)).isPresent();
            assertThat(outbox.count()).isEqualTo(1);
            assertThat(queuedMessages()).isZero();
            assertThat(meters.get("registry.workers.active").gauge().value()).isZero();
            assertThat(processing.count()).isEqualTo(processingBefore + 1);
        }
    }

    private long queuedMessages() {
        return admin.getQueueInfo(rabbit.queue()).getMessageCount();
    }

    private Reply post(String path, UUID key) {
        var result = client.post().uri(path)
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("Shared User", "shared-" + key + "@example.com", "NL"))
                .exchange().expectBody(String.class).returnResult();
        return new Reply(result.getStatus().value(), objectMapper.readTree(result.getResponseBody()));
    }

    private record Reply(int status, JsonNode body) { }
}
