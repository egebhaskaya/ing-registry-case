package com.example.registry.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

import com.example.registry.config.RegistryRabbitProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Unit test of the confirm-to-outcome mapping with a mocked RabbitTemplate, so
 * each broker verdict can
 * be produced exactly, including the one a live broker cannot be asked for on
 * demand: a nack that Spring
 * AMQP synthesizes itself when the channel dies before the broker answered.
 * That nack carries a reason
 * text; a real broker nack (for example queue overflow with reject-publish)
 * carries none.
 */
@ExtendWith(MockitoExtension.class)
class RegistrationCommandPublisherNackTest {

    @Mock
    RabbitTemplate rabbitTemplate;

    private final RegistryRabbitProperties props = new RegistryRabbitProperties(
            "registration", "registration.register", "registration.commands",
            "registration.dlx", "registration.commands.dlq", 20_000, Duration.ofMillis(300));

    @Test
    void ackWithoutReturnIsConfirmed() {
        completeConfirmWith(new CorrelationData.Confirm(true, null));

        assertThat(publisher().publish(sampleCommand())).isEqualTo(PublishOutcome.CONFIRMED);
    }

    @Test
    void brokerNackWithoutReasonIsADefiniteRejection() {
        completeConfirmWith(new CorrelationData.Confirm(false, null));

        assertThat(publisher().publish(sampleCommand())).isEqualTo(PublishOutcome.REJECTED);
    }

    @Test
    void nackSynthesizedOnChannelShutdownIsUnknown() {
        // Design section 4: the message may already be queued; never report it as a
        // definite failure.
        completeConfirmWith(new CorrelationData.Confirm(false, "Channel closed by application"));

        assertThat(publisher().publish(sampleCommand())).isEqualTo(PublishOutcome.UNKNOWN);
    }

    @Test
    void noConfirmWithinTimeoutIsUnknown() {
        // No stubbing: the mock accepts the publish and nothing ever completes the
        // confirm future.
        assertThat(publisher().publish(sampleCommand())).isEqualTo(PublishOutcome.UNKNOWN);
    }

    private RegistrationCommandPublisher publisher() {
        return new RegistrationCommandPublisher(rabbitTemplate, props);
    }

    /**
     * Makes the mocked template complete the CorrelationData future the publisher
     * handed it.
     */
    private void completeConfirmWith(CorrelationData.Confirm confirm) {
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            correlation.getFuture().complete(confirm);
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(Object.class), any(CorrelationData.class));
    }

    private static RegisterUserCommand sampleCommand() {
        return new RegisterUserCommand(UUID.randomUUID(), "f".repeat(64),
                new RegistrationPayload("Ada Lovelace", "ada@example.com", "NL"), Instant.now());
    }
}
