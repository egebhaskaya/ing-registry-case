package com.example.registry.listeners;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.config.RegistryRabbitProperties;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.repositories.UserRepository;
import com.example.registry.services.RegistrationService;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * Design section 5 failure rules: transient failures are retried a finite
 * number of times with backoff,
 * exhausted or permanent failures end in the DLQ with a recorded reason, and a
 * permanent conflict is not
 * retried at all. The transactional service is spied so failures can be
 * injected on demand.
 */
@AutoConfigureRestTestClient
class RegistrationRetryTest extends AbstractIntegrationTest {

    @Autowired
    RestTestClient client;
    @Autowired
    UserRepository users;
    @Autowired
    RabbitTemplate rabbitTemplate;
    @Autowired
    AmqpAdmin amqpAdmin;
    @Autowired
    RegistryRabbitProperties props;

    @MockitoSpyBean
    RegistrationService registrationService;

    @BeforeEach
    void cleanState() {
        truncateTables();
        amqpAdmin.purgeQueue(props.queue(), false);
        amqpAdmin.purgeQueue(props.deadLetterQueue(), false);
    }

    @Test
    void transientFailureIsRetriedThenSucceeds() {
        // Review focus 3: two database hiccups, then the real method runs.
        doThrow(new TransientDataAccessResourceException("db hiccup"))
                .doThrow(new TransientDataAccessResourceException("db hiccup"))
                .doCallRealMethod()
                .when(registrationService).registerWithResult(any());

        post(UUID.randomUUID(), "Ada Lovelace", "retry-" + UUID.randomUUID() + "@example.com");

        await().atMost(Duration.ofSeconds(15)).until(() -> users.count() == 1);
        assertThat(rabbitTemplate.receive(props.deadLetterQueue(), 500)).isNull();
        verify(registrationService, times(3)).registerWithResult(any());
    }

    @Test
    void exhaustedRetriesEndInDlqWithReason() {
        doThrow(new TransientDataAccessResourceException("db hiccup")).when(registrationService).registerWithResult(any());

        post(UUID.randomUUID(), "Ada Lovelace", "exhausted-" + UUID.randomUUID() + "@example.com");

        Message dead = rabbitTemplate.receive(props.deadLetterQueue(), 20_000);
        assertThat(dead).isNotNull();
        // The recoverer records the failure reason, the stack trace and where the message came from.
        String reason = dead.getMessageProperties().getHeader("x-exception-message");
        assertThat(reason).contains("db hiccup");
        // Header values longer than 1 KB arrive as the client's LongString type, hence toString().
        Object trace = dead.getMessageProperties().getHeaders().get("x-exception-stacktrace");
        assertThat(String.valueOf(trace)).contains("db hiccup");
        assertThat(dead.getMessageProperties().getHeaders())
                .containsEntry("x-original-exchange", props.exchange())
                .containsEntry("x-original-routingKey", props.routingKey());
        assertThat(users.count()).isZero();
        // initial attempt + max-retries (3)
        verify(registrationService, times(4)).registerWithResult(any());
    }

    @Test
    void permanentConflictIsNotRetried() {
        // Review focus 4: a conflict can never succeed on retry, so it must take
        // exactly one attempt.
        doCallRealMethod().when(registrationService).registerWithResult(any());
        UUID key = UUID.randomUUID();
        String email = "conflict-" + key + "@example.com";
        post(key, "Ada Lovelace", email);
        await().atMost(Duration.ofSeconds(10)).until(() -> users.count() == 1);

        post(key, "Ada Byron", email);

        Message dead = rabbitTemplate.receive(props.deadLetterQueue(), 10_000);
        assertThat(dead).isNotNull();
        String reason = dead.getMessageProperties().getHeader("x-exception-message");
        assertThat(reason).contains("different payload");
        assertThat(users.count()).isEqualTo(1);
        // one real creation + one conflicting attempt, no retries of the conflict
        verify(registrationService, times(2)).registerWithResult(any());
    }

    private void post(UUID key, String fullName, String email) {
        client.post().uri("/registrations")
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest(fullName, email, "NL"))
                .exchange()
                .expectStatus().isAccepted();
    }
}
