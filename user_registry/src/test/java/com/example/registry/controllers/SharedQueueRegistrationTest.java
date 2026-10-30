package com.example.registry.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.config.RegistryRabbitProperties;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.models.OutboxEvent;
import com.example.registry.repositories.OutboxRepository;
import com.example.registry.repositories.UserRepository;
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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Pausing the actual workers distinguishes broker acceptance from a committed worker reply. */
@AutoConfigureRestTestClient
@TestPropertySource(properties = "spring.rabbitmq.template.reply-timeout=5s")
class SharedQueueRegistrationTest extends AbstractIntegrationTest {
    @Autowired RestTestClient client;
    @Autowired ObjectMapper objectMapper;
    @Autowired RabbitListenerEndpointRegistry listeners;
    @Autowired AmqpAdmin admin;
    @Autowired RegistryRabbitProperties rabbit;
    @Autowired UserRepository users;
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
    void bothRoutesQueueTheSameWorkAndSyncCannotReplyBeforeCommit() throws Exception {
        UUID asyncKey = UUID.randomUUID();
        UUID syncKey = UUID.randomUUID();
        CountDownLatch beforeCommit = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        doAnswer(invocation -> {
            OutboxEvent event = invocation.getArgument(0);
            Object result = invocation.callRealMethod();
            if (event.payload().contains(syncKey.toString())) {
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
            }
            return result;
        }).when(outbox).insert(any());

        assertThat(post("/registrations", asyncKey).status()).isEqualTo(202);
        try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
            var sync = requests.submit(() -> post("/registrations/sync", syncKey));
            try {
                await().atMost(Duration.ofSeconds(2)).until(() -> queuedMessages() == 2);
                assertThat(sync.isDone()).isFalse();
                assertThat(users.count()).isZero();

                listeners.start();
                assertThat(beforeCommit.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(sync.isDone()).isFalse();
                assertThat(users.findByRegistrationId(syncKey)).isEmpty();
                allowCommit.countDown();

                Reply completed = sync.get(5, TimeUnit.SECONDS);
                assertThat(completed.status()).isEqualTo(201);
                assertThat(users.findByRegistrationId(syncKey)).isPresent();
                assertThat(outbox.findAll()).anyMatch(event -> event.payload().contains(syncKey.toString()));
                await().atMost(Duration.ofSeconds(5)).until(() -> users.findByRegistrationId(asyncKey).isPresent());
                assertThat(users.count()).isEqualTo(2);
                assertThat(outbox.count()).isEqualTo(2);
            } finally {
                allowCommit.countDown();
                listeners.start();
            }
        }
    }

    @Test
    void replyTimeoutLeavesQueuedWorkAndSameKeyRetryReturnsItsCommittedUser() {
        UUID key = UUID.randomUUID();

        Reply timedOut = post("/registrations/sync", key);

        assertThat(timedOut.status()).isEqualTo(503);
        assertThat(timedOut.body().path("reason").asText()).isEqualTo("reply_timeout");
        assertThat(queuedMessages()).isEqualTo(1);
        assertThat(users.count()).isZero();
        listeners.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> users.findByRegistrationId(key).isPresent());

        Reply replay = post("/registrations/sync", key);

        assertThat(replay.status()).isEqualTo(200);
        assertThat(replay.body().path("userId").asText())
                .isEqualTo(users.findByRegistrationId(key).orElseThrow().id().toString());
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
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
