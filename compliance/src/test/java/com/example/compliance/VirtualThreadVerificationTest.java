package com.example.compliance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Design section 6: do not assume that a client library's threads become virtual because the Boot
 * setting is on; verify it. This is the Kafka counterpart of the Registry's HTTP and Rabbit probes.
 * Run with {@code -Dspring.profiles.active=platform} and the test fails, which proves it measures the
 * switch and not a constant.
 */
class VirtualThreadVerificationTest extends AbstractIntegrationTest {

    static final String PROBE_TOPIC = "test.thread-probe";

    @TestConfiguration(proxyBeanMethods = false)
    static class Probes {

        @Bean
        NewTopic threadProbeTopic() {
            return TopicBuilder.name(PROBE_TOPIC).partitions(1).replicas(1).build();
        }

        @Bean
        ThreadProbeListener threadProbeListener() {
            return new ThreadProbeListener();
        }
    }

    /** Records the kind of thread the auto-configured listener container used for one record. */
    static class ThreadProbeListener {

        final CompletableFuture<Boolean> observedVirtual = new CompletableFuture<>();

        @KafkaListener(topics = PROBE_TOPIC, groupId = "thread-probe")
        void on(String ignored) {
            observedVirtual.complete(Thread.currentThread().isVirtual());
        }
    }

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ThreadProbeListener probeListener;

    @Test
    void kafkaListenerRunsOnAVirtualThread() throws Exception {
        kafkaTemplate.send(PROBE_TOPIC, "ping");

        assertThat(probeListener.observedVirtual.get(30, TimeUnit.SECONDS)).isTrue();
    }
}
