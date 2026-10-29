package com.example.registry.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.config.RegistryRabbitProperties;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Publishes against a real RabbitMQ and reads the queue back directly, proving
 * that
 * "CONFIRMED" really means "the broker stored the message in the intended
 * queue".
 */
class RegistrationCommandPublisherTest extends AbstractIntegrationTest {

    @Autowired
    RegistrationCommandPublisher publisher;
    @Autowired
    RabbitTemplate rabbitTemplate;
    @Autowired
    RegistryRabbitProperties props;
    @Autowired
    AmqpAdmin amqpAdmin;
    @Autowired
    RabbitListenerEndpointRegistry listenerRegistry;

    @BeforeEach
    void stopConsumersAndPurge() {
        // The application's own consumer must not take the message before this test can
        // read it,
        // so pause every @RabbitListener for the duration of the test.
        listenerRegistry.stop();
        amqpAdmin.purgeQueue(props.queue(), false);
        amqpAdmin.purgeQueue(props.deadLetterQueue(), false);
    }

    @AfterEach
    void restartConsumers() {
        listenerRegistry.start();
    }

    @Test
    void confirmedPublishLandsInTheRegistrationQueue() {
        RegisterUserCommand command = sampleCommand();

        PublishOutcome outcome = publisher.publish(command);

        assertThat(outcome).isEqualTo(PublishOutcome.CONFIRMED);
        Message message = rabbitTemplate.receive(props.queue(), 5000);
        assertThat(message).isNotNull();
        assertThat(new String(message.getBody(), StandardCharsets.UTF_8)).contains(command.registrationId().toString());
        // design §5: persistent commands, so a broker restart does not lose accepted
        // work
        assertThat(message.getMessageProperties().getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
    }

    @Test
    void unroutableMessageIsReportedNotConfirmed() {
        // Same exchange, a routing key no queue is bound to: the broker accepts it but
        // returns it.
        RegistryRabbitProperties badProps = new RegistryRabbitProperties(
                props.exchange(), "no.such.key", props.queue(), props.deadLetterExchange(),
                props.deadLetterQueue(), props.queueMaxLength(), props.confirmTimeout());
        RegistrationCommandPublisher badPublisher = new RegistrationCommandPublisher(rabbitTemplate, badProps);

        PublishOutcome outcome = badPublisher.publish(sampleCommand());

        assertThat(outcome).isEqualTo(PublishOutcome.UNROUTABLE);
        assertThat(rabbitTemplate.receive(props.queue(), 1000)).isNull();
    }

    private static RegisterUserCommand sampleCommand() {
        return new RegisterUserCommand(
                UUID.randomUUID(),
                "f".repeat(64),
                new RegistrationPayload("Ada Lovelace", "ada@example.com", "NL"),
                Instant.now());
    }
}
