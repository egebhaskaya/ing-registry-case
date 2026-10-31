package com.example.registry.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.registry.config.RegistryMetrics;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationPayload;
import com.example.registry.models.User;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.dao.DuplicateKeyException;

class RegistrationProcessorTest {
    private final RegistrationService transactions = mock(RegistrationService.class);
    private final RegistryMetrics metrics = mock(RegistryMetrics.class);
    private RegistrationProcessor processor;
    private final RegisterUserCommand command = new RegisterUserCommand(UUID.randomUUID(), "fingerprint",
            new RegistrationPayload("Ada Lovelace", "ada@example.com", "NL"), Instant.now());
    private final RegistrationResult created = new RegistrationResult(RegistrationOutcome.CREATED,
            new User(UUID.randomUUID(), command.registrationId(), command.payloadFingerprint(),
                    command.payload().email(), command.payload().fullName(), command.payload().countryCode(),
                    Instant.now()));

    @BeforeEach
    void setUp() {
        processor = new RegistrationProcessor(transactions, metrics);
    }

    @Test
    void recordsCompletionOnlyAfterTheTransactionReturns() {
        when(transactions.register(command)).thenAnswer(invocation -> {
            verify(metrics, never()).completion(any());
            return created;
        });

        assertThat(processor.process(command, RegistrationMode.ASYNC)).isEqualTo(created);

        verify(metrics).registration("created");
        verify(metrics).completion(any());
        verify(metrics, never()).registrationFailed();
    }

    @Test
    void resolvesAUniqueViolationAfterTheFailedTransactionReturns() {
        RegistrationResult duplicate = new RegistrationResult(RegistrationOutcome.DUPLICATE_IGNORED, created.user());
        when(transactions.register(command)).thenThrow(new DuplicateKeyException("concurrent duplicate"));
        when(transactions.resolveDuplicate(command)).thenReturn(duplicate);

        assertThat(processor.process(command, RegistrationMode.ASYNC)).isEqualTo(duplicate);

        InOrder order = inOrder(transactions);
        order.verify(transactions).register(command);
        order.verify(transactions).resolveDuplicate(command);
        verify(metrics).registration("duplicate_ignored");
        verify(metrics, never()).completion(any());
        verify(metrics, never()).registrationFailed();
    }

    @Test
    void propagatesBusinessConflictsWithoutCountingATransientFailure() {
        when(transactions.register(command)).thenThrow(new ConflictingPayloadException(command.registrationId()));

        assertThatThrownBy(() -> processor.process(command, RegistrationMode.ASYNC)).isInstanceOf(ConflictingPayloadException.class);

        verify(metrics).registration("conflict");
        verify(metrics, never()).registrationFailed();
        verify(metrics, never()).completion(any());
    }

    @Test
    void propagatesTransientFailuresForTheListenerRetryPolicy() {
        when(transactions.register(command)).thenThrow(new IllegalStateException("database failed"));

        assertThatThrownBy(() -> processor.process(command, RegistrationMode.ASYNC)).isInstanceOf(IllegalStateException.class)
                .hasMessage("database failed");

        verify(metrics).registrationFailed();
        verify(metrics, never()).completion(any());
    }

    @Test
    void bulkMetricsFollowTheCommittedProxyAndCountNewItemsAndTransactionsSeparately() {
        RegisterUserCommand second = new RegisterUserCommand(UUID.randomUUID(), command.payloadFingerprint(),
                new RegistrationPayload("Second User", "second@example.com", "NL"), Instant.now());
        User secondUser = new User(UUID.randomUUID(), second.registrationId(), second.payloadFingerprint(),
                second.payload().email(), second.payload().fullName(), second.payload().countryCode(), Instant.now());
        List<RegisterUserCommand> batch = List.of(command, command, second);
        when(transactions.registerBatch(batch)).thenAnswer(invocation -> {
            verify(metrics, never()).processing(any(), any());
            verify(metrics, never()).batchCommitted(org.mockito.ArgumentMatchers.anyInt(), any());
            return List.of(created, new RegistrationResult(RegistrationOutcome.DUPLICATE_IGNORED, created.user()),
                    new RegistrationResult(RegistrationOutcome.CREATED, secondUser));
        });

        assertThat(processor.processBatch(batch)).isEmpty();

        verify(metrics, times(2)).registration("created");
        verify(metrics).registration("duplicate_ignored");
        verify(metrics, times(2)).processing(eq(RegistrationMode.BATCHED), any());
        verify(metrics, times(2)).completion(any());
        verify(metrics).batchCommitted(eq(2), any());
        verify(metrics).workerStarted();
        verify(metrics).workerFinished();
        verify(transactions, never()).register(any());
    }

    @Test
    void conflictFallbackClosesBulkWorkBeforeProcessingAndReturnsFailedItemIndices() {
        RegisterUserCommand conflicting = new RegisterUserCommand(UUID.randomUUID(), "other",
                command.payload(), Instant.now());
        List<RegisterUserCommand> batch = List.of(command, conflicting);
        when(transactions.registerBatch(batch)).thenThrow(new DuplicateKeyException("bulk conflict"));
        when(transactions.register(command)).thenReturn(created);
        when(transactions.register(conflicting)).thenThrow(new ConflictingPayloadException(conflicting.registrationId()));

        var failures = processor.processBatch(batch);

        assertThat(failures).singleElement().satisfies(failure -> {
            assertThat(failure.index()).isEqualTo(1);
            assertThat(failure.cause()).isInstanceOf(ConflictingPayloadException.class);
        });
        InOrder order = inOrder(transactions, metrics);
        order.verify(transactions).registerBatch(batch);
        order.verify(metrics).workerFinished();
        order.verify(transactions).register(command);
        verify(metrics, times(3)).workerStarted();
        verify(metrics, times(3)).workerFinished();
        verify(metrics, never()).batchCommitted(org.mockito.ArgumentMatchers.anyInt(), any());
    }

    @Test
    void bulkTechnicalFailurePropagatesWithoutTryingIndividualTransactions() {
        List<RegisterUserCommand> batch = List.of(command);
        when(transactions.registerBatch(batch)).thenThrow(new IllegalStateException("database unavailable"));

        assertThatThrownBy(() -> processor.processBatch(batch)).isInstanceOf(IllegalStateException.class)
                .hasMessage("database unavailable");

        verify(transactions, never()).register(any());
        verify(metrics).registrationFailed();
        verify(metrics).workerFinished();
        verify(metrics, never()).processing(any(), any());
    }
}
