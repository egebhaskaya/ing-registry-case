package com.example.registry.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.config.RegistryBatchProperties;
import com.example.registry.config.RegistryRabbitProperties;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.messaging.PublishOutcome;
import com.example.registry.messaging.RegistrationCommandPublisher;
import com.example.registry.models.OutboxEvent;
import com.example.registry.repositories.OutboxRepository;
import com.example.registry.repositories.UserRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Real HTTP, RabbitMQ and PostgreSQL checks for grouped writes and per-registration failures. */
@AutoConfigureRestTestClient
@TestPropertySource(properties = {
        "registry.batch.size=4",
        "registry.batch.max-wait=100ms",
        "spring.rabbitmq.listener.simple.concurrency=1",
        "spring.rabbitmq.listener.simple.max-concurrency=1",
        "spring.datasource.hikari.maximum-pool-size=4"
})
class BatchedRegistrationControllerTest extends AbstractIntegrationTest {
    @Autowired RestTestClient client;
    @Autowired UserRepository users;
    @Autowired AmqpAdmin admin;
    @Autowired RabbitTemplate rabbitTemplate;
    @Autowired RabbitListenerEndpointRegistry listeners;
    @Autowired RegistryRabbitProperties rabbit;
    @Autowired RegistryBatchProperties batch;
    @Autowired MeterRegistry meters;
    @MockitoSpyBean OutboxRepository outbox;
    @MockitoSpyBean RegistrationCommandPublisher publisher;

    @BeforeEach
    void pauseBatchConsumerAndClean() {
        listeners.getListenerContainer("registration-batched").stop();
        admin.purgeQueue(batch.queue(), false);
        admin.purgeQueue(rabbit.queue(), false);
        admin.purgeQueue(rabbit.deadLetterQueue(), false);
        truncateTables();
    }

    @AfterEach
    void stopBatchConsumer() {
        listeners.getListenerContainer("registration-batched").stop();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(meters.get("registry.workers.active").gauge().value()).isZero());
    }

    @Test
    void acceptancePrecedesCommitAndOneRequestFlushesWithoutFillingTheBatch() {
        UUID key = UUID.randomUUID();
        long processingBefore = processingCount();

        post(key, "Ada Lovelace", email(key))
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.registrationId").isEqualTo(key.toString())
                .jsonPath("$.status").isEqualTo("ACCEPTED");

        assertThat(admin.getQueueInfo(batch.queue()).getMessageCount()).isEqualTo(1);
        assertThat(admin.getQueueInfo(rabbit.queue()).getMessageCount()).isZero();
        assertThat(users.count()).isZero();
        assertThat(outbox.count()).isZero();

        listeners.getListenerContainer("registration-batched").start();

        // Only one of the configured four slots is filled: the collection timeout must release it.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(users.findByRegistrationId(key)).isPresent();
            assertThat(outbox.count()).isEqualTo(1);
            assertThat(processingCount()).isEqualTo(processingBefore + 1);
        });
    }

    @Test
    void fullBatchMakesAllUsersAndOutboxEventsVisibleInOneCommit() throws Exception {
        CountDownLatch beforeCommit = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        AtomicInteger observedBatchSize = new AtomicInteger();
        long processingBefore = processingCount();
        doAnswer(invocation -> {
            List<OutboxEvent> events = invocation.getArgument(0);
            observedBatchSize.set(events.size());
            Object result = invocation.callRealMethod();
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void beforeCommit(boolean readOnly) {
                    beforeCommit.countDown();
                    try {
                        if (!allowCommit.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Test did not release the batch commit");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
            });
            return result;
        }).when(outbox).insertBatch(anyList());

        for (int i = 0; i < 4; i++) {
            UUID key = UUID.randomUUID();
            post(key, "Batch User " + i, email(key)).expectStatus().isAccepted();
        }
        assertThat(admin.getQueueInfo(batch.queue()).getMessageCount()).isEqualTo(4);
        listeners.getListenerContainer("registration-batched").start();

        try {
            assertThat(beforeCommit.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(observedBatchSize.get()).isEqualTo(4);
            assertThat(users.count()).isZero();
            assertThat(outbox.count()).isZero();
            assertThat(processingCount()).isEqualTo(processingBefore);
        } finally {
            allowCommit.countDown();
        }

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(users.count()).isEqualTo(4);
            assertThat(outbox.count()).isEqualTo(4);
            assertThat(processingCount()).isEqualTo(processingBefore + 4);
        });
    }

    @Test
    void invalidRequestsAreRejectedBeforeAnythingIsQueued() {
        RegistrationRequest valid = new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL");
        client.post().uri("/registrations/batched").contentType(MediaType.APPLICATION_JSON)
                .body(valid).exchange().expectStatus().isBadRequest();
        client.post().uri("/registrations/batched").header("Idempotency-Key", "not-a-uuid")
                .contentType(MediaType.APPLICATION_JSON).body(valid).exchange().expectStatus().isBadRequest();
        post(UUID.randomUUID(), "Ada Lovelace", "not-an-email").expectStatus().isBadRequest();
        client.post().uri("/registrations/batched").header("Idempotency-Key", UUID.randomUUID().toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"fullName\":\"Ada\",\"email\":\"ada@example.com\",\"countryCode\":\"NL\",\"extra\":true}")
                .exchange().expectStatus().isBadRequest();
        // This endpoint batches separate commands at the consumer; its HTTP body remains one registration.
        client.post().uri("/registrations/batched").header("Idempotency-Key", UUID.randomUUID().toString())
                .contentType(MediaType.APPLICATION_JSON).body(List.of(valid))
                .exchange().expectStatus().isBadRequest();

        assertThat(admin.getQueueInfo(batch.queue()).getMessageCount()).isZero();
        assertThat(users.count()).isZero();
        assertThat(outbox.count()).isZero();
    }

    @Test
    void replayAndPayloadConflictAreIsolatedFromOtherRegistrationsInTheBatch() {
        UUID original = UUID.randomUUID();
        String originalEmail = email(original);
        client.post().uri("/registrations/sync").header("Idempotency-Key", original.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("Original User", originalEmail, "NL"))
                .exchange().expectStatus().isCreated();
        double replaysBefore = meters.get("registry.registrations").tag("outcome", "duplicate_ignored")
                .counter().count();

        post(original, "Original User", originalEmail).expectStatus().isAccepted();
        post(original, "Changed User", originalEmail).expectStatus().isAccepted();
        UUID firstGood = UUID.randomUUID();
        UUID secondGood = UUID.randomUUID();
        post(firstGood, "First Good User", email(firstGood)).expectStatus().isAccepted();
        post(secondGood, "Second Good User", email(secondGood)).expectStatus().isAccepted();
        listeners.getListenerContainer("registration-batched").start();

        Message dead = rabbitTemplate.receive(rabbit.deadLetterQueue(), 15_000);
        assertThat(dead).isNotNull();
        assertThat(new String(dead.getBody(), StandardCharsets.UTF_8)).contains(original.toString(), "Changed User");
        assertThat(String.valueOf(dead.getMessageProperties().getHeaders().get("x-exception-message")))
                .contains("different payload");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(users.count()).isEqualTo(3);
            assertThat(outbox.count()).isEqualTo(3);
            assertThat(users.findByRegistrationId(original).orElseThrow().fullName()).isEqualTo("Original User");
            assertThat(users.findByRegistrationId(firstGood)).isPresent();
            assertThat(users.findByRegistrationId(secondGood)).isPresent();
            assertThat(meters.get("registry.registrations").tag("outcome", "duplicate_ignored")
                    .counter().count()).isEqualTo(replaysBefore + 1);
        });
        assertThat(admin.getQueueInfo(rabbit.deadLetterQueue()).getMessageCount()).isZero();
    }

    @Test
    void sameEmailWithinABatchRollsBackThenOnlyTheConflictingRegistrationGoesToDlq() {
        UUID first = UUID.randomUUID();
        UUID conflicting = UUID.randomUUID();
        UUID third = UUID.randomUUID();
        UUID fourth = UUID.randomUUID();
        String sharedEmail = email(first);
        post(first, "First User", sharedEmail).expectStatus().isAccepted();
        post(conflicting, "Conflicting User", sharedEmail).expectStatus().isAccepted();
        post(third, "Third User", email(third)).expectStatus().isAccepted();
        post(fourth, "Fourth User", email(fourth)).expectStatus().isAccepted();
        listeners.getListenerContainer("registration-batched").start();

        Message dead = rabbitTemplate.receive(rabbit.deadLetterQueue(), 15_000);
        assertThat(dead).isNotNull();
        assertThat(new String(dead.getBody(), StandardCharsets.UTF_8)).contains(conflicting.toString());
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(users.count()).isEqualTo(3);
            assertThat(outbox.count()).isEqualTo(3);
            assertThat(users.findByRegistrationId(first)).isPresent();
            assertThat(users.findByRegistrationId(conflicting)).isEmpty();
            assertThat(users.findByRegistrationId(third)).isPresent();
            assertThat(users.findByRegistrationId(fourth)).isPresent();
        });
        assertThat(admin.getQueueInfo(rabbit.deadLetterQueue()).getMessageCount()).isZero();
    }

    @Test
    void malformedMessageDoesNotSendValidBatchMembersToDlq() {
        MessageProperties malformedProperties = new MessageProperties();
        malformedProperties.setContentType("application/json");
        malformedProperties.setHeader("__TypeId__", "com.example.registry.messaging.RegisterUserCommand");
        rabbitTemplate.send(rabbit.exchange(), batch.routingKey(),
                new Message("{broken-json".getBytes(StandardCharsets.UTF_8), malformedProperties));
        for (int i = 0; i < 3; i++) {
            UUID key = UUID.randomUUID();
            post(key, "Valid User " + i, email(key)).expectStatus().isAccepted();
        }
        listeners.getListenerContainer("registration-batched").start();

        Message dead = rabbitTemplate.receive(rabbit.deadLetterQueue(), 15_000);
        assertThat(dead).isNotNull();
        assertThat(new String(dead.getBody(), StandardCharsets.UTF_8)).isEqualTo("{broken-json");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(users.count()).isEqualTo(3);
            assertThat(outbox.count()).isEqualTo(3);
        });
        assertThat(admin.getQueueInfo(rabbit.deadLetterQueue()).getMessageCount()).isZero();
    }

    @Test
    void technicalBatchFailureRollsBackAndExistingIndividualRetriesPreserveAllRegistrations() {
        AtomicBoolean failFirst = new AtomicBoolean(true);
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            if (failFirst.compareAndSet(true, false)) {
                // Both SQL batches ran, but this transaction must roll back before normal processing retries.
                throw new TransientDataAccessResourceException("batch database hiccup");
            }
            return result;
        }).when(outbox).insertBatch(anyList());
        for (int i = 0; i < 4; i++) {
            UUID key = UUID.randomUUID();
            post(key, "Recoverable User " + i, email(key)).expectStatus().isAccepted();
        }
        listeners.getListenerContainer("registration-batched").start();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(failFirst.get()).isFalse();
            assertThat(users.count()).isEqualTo(4);
            assertThat(outbox.count()).isEqualTo(4);
            assertThat(meters.get("registry.workers.active").gauge().value()).isZero();
        });
        assertThat(admin.getQueueInfo(rabbit.deadLetterQueue()).getMessageCount()).isZero();
    }

    @Test
    void uncertainRetryForwardingRedeliversTheBatchWithoutDuplicatingCommittedNeighbors() {
        UUID original = UUID.randomUUID();
        String originalEmail = email(original);
        client.post().uri("/registrations/sync").header("Idempotency-Key", original.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("Original User", originalEmail, "NL"))
                .exchange().expectStatus().isCreated();
        double replaysBefore = meters.get("registry.registrations").tag("outcome", "duplicate_ignored")
                .counter().count();
        AtomicInteger forwardAttempts = new AtomicInteger();
        AtomicLong usersBeforeUncertainForward = new AtomicLong();
        AtomicLong outboxBeforeUncertainForward = new AtomicLong();
        AtomicBoolean sawRedelivery = new AtomicBoolean();
        doAnswer(invocation -> {
            if (forwardAttempts.incrementAndGet() == 1) {
                usersBeforeUncertainForward.set(users.count());
                outboxBeforeUncertainForward.set(outbox.count());
                // Simulate a missing publisher confirmation: the original batch must remain retryable.
                return PublishOutcome.UNKNOWN;
            }
            Message redelivered = invocation.getArgument(0);
            sawRedelivery.set(Boolean.TRUE.equals(redelivered.getMessageProperties().isRedelivered()));
            return invocation.callRealMethod();
        }).when(publisher).retryIndividually(org.mockito.ArgumentMatchers.any(Message.class));

        post(original, "Changed User", originalEmail).expectStatus().isAccepted();
        for (int i = 0; i < 3; i++) {
            UUID key = UUID.randomUUID();
            post(key, "Committed Neighbor " + i, email(key)).expectStatus().isAccepted();
        }
        listeners.getListenerContainer("registration-batched").start();

        Message dead = rabbitTemplate.receive(rabbit.deadLetterQueue(), 15_000);
        assertThat(dead).isNotNull();
        assertThat(new String(dead.getBody(), StandardCharsets.UTF_8)).contains(original.toString(), "Changed User");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(forwardAttempts.get()).isGreaterThanOrEqualTo(2);
            assertThat(sawRedelivery.get()).isTrue();
            assertThat(usersBeforeUncertainForward.get()).isEqualTo(4);
            assertThat(outboxBeforeUncertainForward.get()).isEqualTo(4);
            assertThat(users.count()).isEqualTo(4);
            assertThat(outbox.count()).isEqualTo(4);
            assertThat(users.findByRegistrationId(original).orElseThrow().fullName()).isEqualTo("Original User");
            assertThat(meters.get("registry.registrations").tag("outcome", "duplicate_ignored")
                    .counter().count()).isGreaterThanOrEqualTo(replaysBefore + 3);
        });
        assertThat(admin.getQueueInfo(rabbit.deadLetterQueue()).getMessageCount()).isZero();
    }

    private RestTestClient.ResponseSpec post(UUID key, String name, String email) {
        return client.post().uri("/registrations/batched").header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON).body(new RegistrationRequest(name, email, "NL"))
                .exchange();
    }

    private long processingCount() {
        return meters.get("registry.registration.processing").tag("mode", "batched").timer().count();
    }

    private static String email(UUID key) {
        return "batch-" + key + "@example.com";
    }
}
