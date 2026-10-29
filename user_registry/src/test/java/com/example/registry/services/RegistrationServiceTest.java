package com.example.registry.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationPayload;
import com.example.registry.models.OutboxEvent;
import com.example.registry.models.User;
import com.example.registry.repositories.OutboxRepository;
import com.example.registry.repositories.UserRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * The registration transaction against a real PostgreSQL (design section 5
 * rules, section 9 gate:
 * "single registration, rollback, duplicate and conflict behavior pass").
 */
class RegistrationServiceTest extends AbstractIntegrationTest {

    @Autowired
    RegistrationService service;

    /**
     * Spies wrap the real repositories: every call goes through to the database
     * unless a test says
     * otherwise. One test makes the outbox insert throw to prove the user insert is
     * rolled back with it;
     * another pauses the user lookup to force two transactions into the same race.
     */
    @MockitoSpyBean
    UserRepository users;
    @MockitoSpyBean
    OutboxRepository outbox;

    private final RegistrationNormalizer normalizer = new RegistrationNormalizer();

    @BeforeEach
    void cleanDatabase() {
        truncateTables();
    }

    @Test
    void createsOneUserAndOneOutboxEvent() {
        RegisterUserCommand command = command(UUID.randomUUID(), "Ada Lovelace", "ada@example.com");

        RegistrationOutcome outcome = service.register(command);

        assertThat(outcome).isEqualTo(RegistrationOutcome.CREATED);
        assertThat(users.count()).isEqualTo(1);
        User user = users.findByRegistrationId(command.registrationId()).orElseThrow();
        assertThat(user.email()).isEqualTo("ada@example.com");

        List<OutboxEvent> events = outbox.findAll();
        assertThat(events).hasSize(1);
        OutboxEvent event = events.get(0);
        assertThat(event.type()).isEqualTo("UserRegistered");
        assertThat(event.aggregateType()).isEqualTo("user");
        assertThat(event.aggregateId()).isEqualTo(user.id().toString());
        assertThat(event.payload()).contains(command.registrationId().toString());
        // The event id travels inside the payload too, so Compliance can deduplicate
        // without Kafka headers.
        assertThat(event.payload()).contains("\"eventId\": \"" + event.id() + "\"");
    }

    @Test
    void sameCommandTwiceIsIgnoredTheSecondTime() {
        // Review focus 5: a redelivery after commit-but-before-ack must be harmless.
        RegisterUserCommand command = command(UUID.randomUUID(), "Ada Lovelace", "ada@example.com");
        service.register(command);

        RegistrationOutcome second = service.register(command);

        assertThat(second).isEqualTo(RegistrationOutcome.DUPLICATE_IGNORED);
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void sameRegistrationIdDifferentPayloadConflicts() {
        UUID registrationId = UUID.randomUUID();
        service.register(command(registrationId, "Ada Lovelace", "ada@example.com"));

        assertThatThrownBy(() -> service.register(command(registrationId, "Ada Byron", "ada@example.com")))
                .isInstanceOf(ConflictingPayloadException.class);
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void sameEmailUnderNewRegistrationIdIsPermanentConflict() {
        service.register(command(UUID.randomUUID(), "Ada Lovelace", "ada@example.com"));
        RegisterUserCommand second = command(UUID.randomUUID(), "Ada Byron", "ada@example.com");

        // The unique constraint fires inside the transaction and rolls it back...
        assertThatThrownBy(() -> service.register(second)).isInstanceOf(DuplicateKeyException.class);
        // ...and a fresh read-only transaction classifies it as a permanent business
        // conflict.
        assertThatThrownBy(() -> service.resolveDuplicate(second)).isInstanceOf(EmailAlreadyRegisteredException.class);
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void resolveDuplicateRecognizesTheSameRegistration() {
        RegisterUserCommand command = command(UUID.randomUUID(), "Ada Lovelace", "ada@example.com");
        service.register(command);

        assertThat(service.resolveDuplicate(command)).isEqualTo(RegistrationOutcome.DUPLICATE_IGNORED);
    }

    @Test
    void resolveDuplicateRejectsADifferentPayloadForTheSameId() {
        UUID registrationId = UUID.randomUUID();
        service.register(command(registrationId, "Ada Lovelace", "ada@example.com"));

        assertThatThrownBy(() -> service.resolveDuplicate(command(registrationId, "Ada Byron", "ada@example.com")))
                .isInstanceOf(ConflictingPayloadException.class);
    }

    @Test
    void concurrentDeliveriesOfTheSameCommandEndInOneUserAndOneHarmlessDuplicate() throws Exception {
        RegisterUserCommand command = command(UUID.randomUUID(), "Ada Lovelace", "ada@example.com");

        // Force the race: both transactions must finish their "not found" lookup before
        // either inserts.
        // The barrier holds the first two lookups until both arrived; later lookups
        // pass straight through.
        CyclicBarrier bothLookedUp = new CyclicBarrier(2);
        AtomicInteger lookups = new AtomicInteger();
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            if (lookups.incrementAndGet() <= 2) {
                bothLookedUp.await(10, TimeUnit.SECONDS);
            }
            return result;
        }).when(users).findByRegistrationId(command.registrationId());

        List<RegistrationOutcome> outcomes = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean uniqueViolationSeen = new AtomicBoolean(false);
        Runnable delivery = () -> {
            try {
                outcomes.add(service.register(command));
            } catch (DuplicateKeyException e) {
                // Exactly what the listener does: the losing transaction is gone; classify in a
                // fresh one.
                uniqueViolationSeen.set(true);
                outcomes.add(service.resolveDuplicate(command));
            }
        };
        Thread first = Thread.ofVirtual().start(delivery);
        Thread second = Thread.ofVirtual().start(delivery);
        first.join();
        second.join();

        assertThat(uniqueViolationSeen).isTrue();
        assertThat(outcomes).containsExactlyInAnyOrder(RegistrationOutcome.CREATED,
                RegistrationOutcome.DUPLICATE_IGNORED);
        assertThat(users.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test
    void outboxFailureRollsBackTheUser() {
        doThrow(new RuntimeException("boom")).when(outbox).insert(any());

        assertThatThrownBy(() -> service.register(command(UUID.randomUUID(), "Ada Lovelace", "ada@example.com")))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("boom");
        // Design section 5: "roll back user and outbox together".
        assertThat(users.count()).isZero();
    }

    private RegisterUserCommand command(UUID registrationId, String fullName, String email) {
        RegistrationPayload payload = new RegistrationPayload(fullName, email, "NL");
        return new RegisterUserCommand(registrationId, normalizer.fingerprint(payload), payload, Instant.now());
    }
}
