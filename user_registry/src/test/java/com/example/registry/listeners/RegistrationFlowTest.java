package com.example.registry.listeners;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.config.RegistryRabbitProperties;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.repositories.OutboxRepository;
import com.example.registry.repositories.UserRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * End to end through the real stack: HTTP 202 -> RabbitMQ -> listener ->
 * transaction -> rows in PostgreSQL.
 * Awaitility polls because the work is asynchronous: a 202 means "queued", not
 * "done".
 */
@AutoConfigureRestTestClient
class RegistrationFlowTest extends AbstractIntegrationTest {

    @Autowired
    RestTestClient client;
    @Autowired
    UserRepository users;
    @Autowired
    OutboxRepository outbox;
    @Autowired
    RabbitTemplate rabbitTemplate;
    @Autowired
    AmqpAdmin amqpAdmin;
    @Autowired
    RegistryRabbitProperties props;
    @Autowired
    MeterRegistry meterRegistry;

    @BeforeEach
    void cleanState() {
        truncateTables();
        amqpAdmin.purgeQueue(props.queue(), false);
        amqpAdmin.purgeQueue(props.deadLetterQueue(), false);
    }

    @Test
    void acceptedRequestBecomesOneUserAndOneOutboxRow() {
        post(UUID.randomUUID(), "Ada Lovelace", "ada@example.com").expectStatus().isAccepted();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(users.count()).isEqualTo(1);
            assertThat(outbox.count()).isEqualTo(1);
        });
    }

    @Test
    void sameRequestTwiceCreatesOnlyOneUser() {
        UUID key = UUID.randomUUID();
        post(key, "Ada Lovelace", "ada@example.com").expectStatus().isAccepted();
        post(key, "Ada Lovelace", "ada@example.com").expectStatus().isAccepted();

        await().atMost(Duration.ofSeconds(10)).until(() -> users.count() == 1);
        // Give the second delivery time to be processed, then prove it changed nothing.
        await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            assertThat(users.count()).isEqualTo(1);
            assertThat(outbox.count()).isEqualTo(1);
        });
        assertThat(rabbitTemplate.receive(props.deadLetterQueue(), 500)).isNull();
    }

    @Test
    void synchronousCreationAndAsynchronousReplayShareIdempotency() {
        UUID key = UUID.randomUUID();
        RegistrationRequest request = new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL");
        client.post().uri("/registrations/sync")
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .exchange().expectStatus().isCreated();
        var stored = users.findByRegistrationId(key).orElseThrow();
        var duplicates = meterRegistry.counter("registry.registrations", "outcome", "duplicate_ignored");
        double duplicatesBefore = duplicates.count();

        post(key, request.fullName(), request.email()).expectStatus().isAccepted();

        // Wait for the real consumer to finish the duplicate transaction, rather than only checking
        // rows that already existed before the asynchronous request was submitted.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(duplicates.count()).isGreaterThan(duplicatesBefore);
            assertThat(users.count()).isEqualTo(1);
            assertThat(outbox.count()).isEqualTo(1);
        });
        assertThat(rabbitTemplate.receive(props.deadLetterQueue(), 500)).isNull();
        client.post().uri("/registrations/sync")
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.userId").isEqualTo(stored.id().toString());
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void sameKeyDifferentPayloadGoesToDlq() {
        UUID key = UUID.randomUUID();
        post(key, "Ada Lovelace", "ada@example.com").expectStatus().isAccepted();
        await().atMost(Duration.ofSeconds(10)).until(() -> users.count() == 1);

        // The API still says 202: it only promises the command was queued (design
        // section 4).
        post(key, "Ada Byron", "ada@example.com").expectStatus().isAccepted();

        Message dead = rabbitTemplate.receive(props.deadLetterQueue(), 10_000);
        assertThat(dead).isNotNull();
        assertThat(new String(dead.getBody(), StandardCharsets.UTF_8)).contains(key.toString());
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void sameEmailDifferentCaseUnderNewKeyGoesToDlq() {
        // Review focus 3: the email is normalized before the uniqueness rule, so case
        // cannot bypass it.
        post(UUID.randomUUID(), "Ada Lovelace", "ada@example.com").expectStatus().isAccepted();
        await().atMost(Duration.ofSeconds(10)).until(() -> users.count() == 1);

        UUID secondKey = UUID.randomUUID();
        post(secondKey, "Ada Byron", "ADA@Example.com").expectStatus().isAccepted();

        Message dead = rabbitTemplate.receive(props.deadLetterQueue(), 10_000);
        assertThat(dead).isNotNull();
        assertThat(new String(dead.getBody(), StandardCharsets.UTF_8)).contains(secondKey.toString());
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    private RestTestClient.ResponseSpec post(UUID key, String fullName, String email) {
        return client.post().uri("/registrations")
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest(fullName, email, "NL"))
                .exchange();
    }
}
