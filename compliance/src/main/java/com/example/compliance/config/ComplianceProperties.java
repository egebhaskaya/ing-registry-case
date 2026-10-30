package com.example.compliance.config;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The {@code compliance.*} settings in application.properties. Nested records bind the
 * nested YAML blocks:
 * {@code compliance.kafka.topic} becomes {@code kafka().topic()}.
 *
 * @param kafka    the topic Compliance consumes and how many partitions it has
 * @param demoRule the one deterministic rule (design section 4: explicitly demo
 *                 logic)
 */
@ConfigurationProperties("compliance")
public record ComplianceProperties(Kafka kafka, DemoRule demoRule) {

    public record Kafka(String topic, int partitions) {

        /**
         * Spring Kafka's convention for a dead-letter topic: the source topic name plus
         * ".DLT".
         */
        public String deadLetterTopic() {
            return topic + ".DLT";
        }
    }

    public record DemoRule(List<String> flaggedCountries) {
    }
}
