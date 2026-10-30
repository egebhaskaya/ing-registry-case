package com.example.registry.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.registry.config.RegistryMetrics;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationPayload;
import com.example.registry.models.User;
import java.time.Instant;
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

        assertThat(processor.process(command)).isEqualTo(created);

        verify(metrics).registration("created");
        verify(metrics).completion(any());
        verify(metrics, never()).registrationFailed();
    }

    @Test
    void resolvesAUniqueViolationAfterTheFailedTransactionReturns() {
        RegistrationResult duplicate = new RegistrationResult(RegistrationOutcome.DUPLICATE_IGNORED, created.user());
        when(transactions.register(command)).thenThrow(new DuplicateKeyException("concurrent duplicate"));
        when(transactions.resolveDuplicate(command)).thenReturn(duplicate);

        assertThat(processor.process(command)).isEqualTo(duplicate);

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

        assertThatThrownBy(() -> processor.process(command)).isInstanceOf(ConflictingPayloadException.class);

        verify(metrics).registration("conflict");
        verify(metrics, never()).registrationFailed();
        verify(metrics, never()).completion(any());
    }

    @Test
    void propagatesTransientFailuresForTheListenerRetryPolicy() {
        when(transactions.register(command)).thenThrow(new IllegalStateException("database failed"));

        assertThatThrownBy(() -> processor.process(command)).isInstanceOf(IllegalStateException.class)
                .hasMessage("database failed");

        verify(metrics).registrationFailed();
        verify(metrics, never()).completion(any());
    }
}
