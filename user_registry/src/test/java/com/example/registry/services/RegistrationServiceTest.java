package com.example.registry.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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
    @Autowired
    RegistrationProcessor processor;

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

        RegistrationResult result = service.register(command);

        assertThat(result.outcome()).isEqualTo(RegistrationOutcome.CREATED);
        assertThat(users.count()).isEqualTo(1);
        User user = users.findByRegistrationId(command.registrationId()).orElseThrow();
        assertThat(result.user()).isEqualTo(user);
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

        RegistrationResult second = service.register(command);

        assertThat(second.outcome()).isEqualTo(RegistrationOutcome.DUPLICATE_IGNORED);
        assertThat(second.user()).isEqualTo(users.findByRegistrationId(command.registrationId()).orElseThrow());
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

        assertThat(service.resolveDuplicate(command).outcome()).isEqualTo(RegistrationOutcome.DUPLICATE_IGNORED);
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
                outcomes.add(service.register(command).outcome());
            } catch (DuplicateKeyException e) {
                // Exactly what the listener does: the losing transaction is gone; classify in a
                // fresh one.
                uniqueViolationSeen.set(true);
                outcomes.add(service.resolveDuplicate(command).outcome());
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

    @Test
    void jdbcBatchPreservesStoredAndInBatchReplaysWithOneOutboxPerNewUser() {
        RegisterUserCommand stored = command(UUID.randomUUID(), "Stored User", "stored@example.com");
        service.register(stored);
        RegisterUserCommand first = command(UUID.randomUUID(), "First User", "first@example.com");
        RegisterUserCommand second = command(UUID.randomUUID(), "Second User", "second@example.com");
        List<RegisterUserCommand> batch = List.of(stored, first, first, second);
        clearInvocations(users, outbox);

        List<RegistrationResult> results = service.registerBatch(batch);

        verify(users).findByRegistrationIds(anyCollection());
        verify(users, never()).findByRegistrationId(any());
        verify(users).insertBatch(anyList());
        verify(outbox).insertBatch(anyList());
        assertThat(results).extracting(RegistrationResult::outcome).containsExactly(
                RegistrationOutcome.DUPLICATE_IGNORED, RegistrationOutcome.CREATED,
                RegistrationOutcome.DUPLICATE_IGNORED, RegistrationOutcome.CREATED);
        assertThat(results.get(1).user()).isEqualTo(results.get(2).user());
        assertThat(users.count()).isEqualTo(3);
        assertThat(outbox.count()).isEqualTo(3);
        for (int index : new int[] { 1, 3 }) {
            User user = results.get(index).user();
            assertThat(users.findByRegistrationId(batch.get(index).registrationId())).contains(user);
            assertThat(outbox.findAll()).filteredOn(event -> event.aggregateId().equals(user.id().toString()))
                    .singleElement().satisfies(event -> assertThat(event.payload())
                            .contains(user.registrationId().toString()).contains(event.id().toString()));
        }

        assertThat(service.registerBatch(batch)).extracting(RegistrationResult::outcome)
                .containsOnly(RegistrationOutcome.DUPLICATE_IGNORED);
        assertThat(users.count()).isEqualTo(3);
        assertThat(outbox.count()).isEqualTo(3);
    }

    @Test
    void technicalFailureAfterBothJdbcBatchesRollsBackEverythingBeforeRetry() {
        List<RegisterUserCommand> batch = List.of(
                command(UUID.randomUUID(), "First User", "first@example.com"),
                command(UUID.randomUUID(), "Second User", "second@example.com"));
        doAnswer(invocation -> {
            invocation.callRealMethod();
            throw new IllegalStateException("outbox batch unavailable");
        }).when(outbox).insertBatch(anyList());

        assertThatThrownBy(() -> processor.processBatch(batch)).isInstanceOf(IllegalStateException.class)
                .hasMessage("outbox batch unavailable");

        assertThat(users.count()).isZero();
        assertThat(outbox.count()).isZero();
        verify(users, never()).insert(any());
        doCallRealMethod().when(outbox).insertBatch(anyList());

        assertThat(processor.processBatch(batch)).isEmpty();
        assertThat(users.count()).isEqualTo(2);
        assertThat(outbox.count()).isEqualTo(2);
    }

    @Test
    void emailConflictRollsBackTheBulkWriteThenIsolatesOnlyTheBadItem() {
        RegisterUserCommand first = command(UUID.randomUUID(), "First User", "shared@example.com");
        RegisterUserCommand conflict = command(UUID.randomUUID(), "Conflicting User", "shared@example.com");
        RegisterUserCommand second = command(UUID.randomUUID(), "Second User", "second@example.com");
        List<RegisterUserCommand> batch = List.of(first, conflict, second);

        assertThatThrownBy(() -> service.registerBatch(batch)).isInstanceOf(DuplicateKeyException.class);
        assertThat(users.count()).isZero();
        assertThat(outbox.count()).isZero();

        List<RegistrationProcessor.BatchFailure> failures = processor.processBatch(batch);

        assertThat(failures).singleElement().satisfies(failure -> {
            assertThat(failure.index()).isEqualTo(1);
            assertThat(failure.cause()).isInstanceOf(EmailAlreadyRegisteredException.class);
        });
        assertThat(users.findByRegistrationId(first.registrationId())).isPresent();
        assertThat(users.findByRegistrationId(second.registrationId())).isPresent();
        assertThat(users.findByRegistrationId(conflict.registrationId())).isEmpty();
        assertThat(users.count()).isEqualTo(2);
        assertThat(outbox.count()).isEqualTo(2);

        assertThat(processor.processBatch(batch)).hasSize(1);
        assertThat(users.count()).isEqualTo(2);
        assertThat(outbox.count()).isEqualTo(2);
    }

    @Test
    void conflictingIdInBatchDoesNotDiscardOtherValidRegistrations() {
        RegisterUserCommand stored = command(UUID.randomUUID(), "Stored User", "stored@example.com");
        service.register(stored);
        RegisterUserCommand conflict = command(stored.registrationId(), "Changed User", "stored@example.com");
        List<RegisterUserCommand> batch = List.of(
                command(UUID.randomUUID(), "First User", "first@example.com"), conflict,
                command(UUID.randomUUID(), "Second User", "second@example.com"));

        List<RegistrationProcessor.BatchFailure> failures = processor.processBatch(batch);

        assertThat(failures).singleElement().satisfies(failure -> {
            assertThat(failure.index()).isEqualTo(1);
            assertThat(failure.cause()).isInstanceOf(ConflictingPayloadException.class);
        });
        assertThat(users.findByRegistrationId(stored.registrationId()).orElseThrow().fullName()).isEqualTo("Stored User");
        assertThat(users.count()).isEqualTo(3);
        assertThat(outbox.count()).isEqualTo(3);
    }

    private RegisterUserCommand command(UUID registrationId, String fullName, String email) {
        RegistrationPayload payload = new RegistrationPayload(fullName, email, "NL");
        return new RegisterUserCommand(registrationId, normalizer.fingerprint(payload), payload, Instant.now());
    }
}
