package com.example.registry.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.models.User;
import com.example.registry.repositories.OutboxRepository;
import com.example.registry.repositories.UserRepository;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** The synchronous HTTP response describes committed Registry data, including on retries. */
@AutoConfigureRestTestClient
@TestPropertySource(properties = "spring.rabbitmq.template.reply-timeout=5s")
class SynchronousRegistrationControllerTest extends AbstractIntegrationTest {

    @Autowired
    RestTestClient client;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    RabbitTemplate rabbitTemplate;
    @MockitoSpyBean
    UserRepository users;
    @MockitoSpyBean
    OutboxRepository outbox;

    @BeforeEach
    void cleanDatabase() {
        truncateTables();
    }

    @Test
    void createdResponseContainsTheCommittedUserAndAnOutboxEventAlreadyExists() {
        UUID key = UUID.randomUUID();

        Reply reply = post(key, request("  Ada   Lovelace  ", "ADA@EXAMPLE.COM", "nl"));

        assertThat(reply.status()).isEqualTo(201);
        User stored = users.findByRegistrationId(key).orElseThrow();
        assertStoredUser(reply.body(), stored);
        assertThat(stored.fullName()).isEqualTo("Ada Lovelace");
        assertThat(stored.email()).isEqualTo("ada@example.com");
        assertThat(stored.countryCode()).isEqualTo("NL");
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.findAll()).singleElement().satisfies(event -> {
            assertThat(event.aggregateId()).isEqualTo(stored.id().toString());
            assertThat(event.payload()).contains(key.toString());
        });
    }

    @Test
    void sameKeyAndNormalizedPayloadReturnsTheSameUserWithoutAnotherEvent() {
        UUID key = UUID.randomUUID();
        Reply created = post(key, request("  Ada   Lovelace  ", "ADA@EXAMPLE.COM", "nl"));

        Reply replay = post(key, request("Ada Lovelace", "ada@example.com", "NL"));

        assertThat(created.status()).isEqualTo(201);
        assertThat(replay.status()).isEqualTo(200);
        assertThat(replay.body()).isEqualTo(created.body());
        assertStoredUser(replay.body(), users.findByRegistrationId(key).orElseThrow());
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void reusingAKeyForDifferentDataReturnsConflictAndPreservesTheOriginal() {
        UUID key = UUID.randomUUID();
        assertThat(post(key, request("Ada Lovelace", "ada@example.com", "NL")).status()).isEqualTo(201);

        Reply conflict = post(key, request("Ada Byron", "ada@example.com", "NL"));

        assertThat(conflict.status()).isEqualTo(409);
        assertThat(conflict.body().path("reason").asText()).isEqualTo("idempotency_conflict");
        assertThat(users.findByRegistrationId(key).orElseThrow().fullName()).isEqualTo("Ada Lovelace");
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void existingEmailUnderANewKeyReturnsConflictWithoutAnotherEvent() {
        assertThat(post(UUID.randomUUID(), request("Ada Lovelace", "ada@example.com", "NL")).status())
                .isEqualTo(201);

        Reply conflict = post(UUID.randomUUID(), request("Ada Byron", "ADA@EXAMPLE.COM", "NL"));

        assertThat(conflict.status()).isEqualTo(409);
        assertThat(conflict.body().path("reason").asText()).isEqualTo("email_already_registered");
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void concurrentRequestsForTheSameKeyCommitOnceAndReturnTheSameUser() throws Exception {
        UUID key = UUID.randomUUID();
        RegistrationRequest request = request("Ada Lovelace", "ada@example.com", "NL");
        CyclicBarrier bothLookedUp = new CyclicBarrier(2);
        AtomicInteger lookups = new AtomicInteger();
        // Force both transactions past the initial absence check so the database constraint wins the race.
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            if (lookups.incrementAndGet() <= 2) {
                bothLookedUp.await(10, TimeUnit.SECONDS);
            }
            return result;
        }).when(users).findByRegistrationId(key);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> post(key, request));
            var second = executor.submit(() -> post(key, request));
            Reply firstReply = first.get(15, TimeUnit.SECONDS);
            Reply secondReply = second.get(15, TimeUnit.SECONDS);

            assertThat(new int[] { firstReply.status(), secondReply.status() })
                    .containsExactlyInAnyOrder(201, 200);
            assertThat(firstReply.body()).isEqualTo(secondReply.body());
        }
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void outboxFailureReturnsNoSuccessAndRollsBackTheUser() {
        doThrow(new IllegalStateException("outbox unavailable")).when(outbox).insert(any());

        Reply reply = post(UUID.randomUUID(), request("Ada Lovelace", "ada@example.com", "NL"));

        assertThat(reply.status()).isEqualTo(503);
        assertThat(reply.body().path("reason").asText()).isEqualTo("reply_timeout");
        assertThat(rabbitTemplate.receive("registration.commands.dlq", 10_000)).isNotNull();
        assertThat(users.count()).isZero();
        assertThat(outbox.count()).isZero();
    }

    @Test
    void invalidInputDoesNotWriteToTheDatabase() {
        Reply reply = post(UUID.randomUUID(), request("Ada Lovelace", "not-an-email", "NL"));

        assertThat(reply.status()).isEqualTo(400);
        assertThat(users.count()).isZero();
        assertThat(outbox.count()).isZero();
    }

    @Test
    void idempotencyKeyIsRequiredAndMustBeAUuid() {
        client.post().uri("/registrations/sync")
                .contentType(MediaType.APPLICATION_JSON)
                .body(request("Ada Lovelace", "ada@example.com", "NL"))
                .exchange().expectStatus().isBadRequest();
        client.post().uri("/registrations/sync")
                .header("Idempotency-Key", "not-a-uuid")
                .contentType(MediaType.APPLICATION_JSON)
                .body(request("Ada Lovelace", "ada@example.com", "NL"))
                .exchange().expectStatus().isBadRequest();
        assertThat(users.count()).isZero();
        assertThat(outbox.count()).isZero();
    }

    private Reply post(UUID key, RegistrationRequest request) {
        var result = client.post().uri("/registrations/sync")
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .exchange().expectBody(String.class).returnResult();
        return new Reply(result.getStatus().value(), objectMapper.readTree(result.getResponseBody()));
    }

    private static RegistrationRequest request(String fullName, String email, String countryCode) {
        return new RegistrationRequest(fullName, email, countryCode);
    }

    private static void assertStoredUser(JsonNode body, User user) {
        assertThat(body.path("userId").asText()).isEqualTo(user.id().toString());
        assertThat(body.path("registrationId").asText()).isEqualTo(user.registrationId().toString());
        assertThat(body.path("fullName").asText()).isEqualTo(user.fullName());
        assertThat(body.path("email").asText()).isEqualTo(user.email());
        assertThat(body.path("countryCode").asText()).isEqualTo(user.countryCode());
        assertThat(Instant.parse(body.path("createdAt").asText())).isEqualTo(user.createdAt());
        assertThat(body.path("status").asText()).isEqualTo("REGISTERED");
    }

    private record Reply(int status, JsonNode body) {
    }
}
