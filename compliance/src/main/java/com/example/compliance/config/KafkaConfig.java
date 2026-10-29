package com.example.compliance.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import tools.jackson.core.JacksonException;

/**
 * Kafka topology as seen by Compliance (design section 5: four partitions, one consumer by default).
 *
 * <p>Spring Boot's KafkaAdmin creates every {@link NewTopic} bean on startup if it does not exist yet
 * and leaves it alone if it does. The compose stack creates the same topics with the same partition
 * count (kafka-init), so whichever starts first wins and the other is a no-op. Debezium produces into
 * the event topic; Compliance only consumes it, and produces only into the dead-letter topic.
 */
@Configuration
@EnableConfigurationProperties(ComplianceProperties.class)
public class KafkaConfig {

    @Bean
    NewTopic userRegisteredTopic(ComplianceProperties props) {
        return TopicBuilder.name(props.kafka().topic())
                .partitions(props.kafka().partitions())
                .replicas(1)
                .build();
    }

    /** Where records that keep failing end up; same partition count so a record keeps its partition. */
    @Bean
    NewTopic userRegisteredDeadLetterTopic(ComplianceProperties props) {
        return TopicBuilder.name(props.kafka().deadLetterTopic())
                .partitions(props.kafka().partitions())
                .replicas(1)
                .build();
    }

    /**
     * What happens when the listener throws (design section 5 failure rules, mirrored from Registry):
     * retry the record up to 3 times with growing delays (500ms, 1s, 2s), then publish it to the
     * dead-letter topic and move on, so one bad record cannot block its partition forever.
     * Unparseable JSON is excluded from retries: it will never parse better the fourth time.
     * Spring Boot applies a {@link CommonErrorHandler} bean to every listener container.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<?, ?> kafkaTemplate, ComplianceMetrics metrics) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5_000);

        // Explicit destination: Spring Kafka's default would be "<topic>-dlt"; we name it "<topic>.DLT"
        // everywhere (compose, properties, tests). Derived from the failed record's own topic, so a
        // future listener on another topic gets its own dead-letter topic. Same partition number.
        // The anonymous subclass counts each publication for the dashboard after the parent did its work.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition())) {
            @Override
            public void accept(ConsumerRecord<?, ?> record, Exception exception) {
                super.accept(record, exception);
                metrics.deadLettered();
            }
        };

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(JacksonException.class);
        return handler;
    }
}
