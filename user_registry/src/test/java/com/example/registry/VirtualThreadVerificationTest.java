package com.example.registry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Design section 6: "verify actual HTTP handler and Simple Rabbit consumer
 * execution with
 * Thread.isVirtual()", through the real embedded server and a real listener
 * container, not a mock.
 *
 * <p>
 * The probes live only in this test class (a nested
 * {@code @TestConfiguration}), so production code
 * carries no test-only endpoint. Run with
 * {@code -Dspring.profiles.active=platform} and both tests fail,
 * which is the proof that the test measures the switch and not a constant.
 */
@AutoConfigureRestTestClient
class VirtualThreadVerificationTest extends AbstractIntegrationTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class Probes {

        @Bean
        ThreadProbeController threadProbeController() {
            return new ThreadProbeController();
        }

        @Bean
        ThreadProbeListener threadProbeListener() {
            return new ThreadProbeListener();
        }
    }

    /** Answers with the kind of thread Tomcat ran this request on. */
    @RestController
    static class ThreadProbeController {

        @GetMapping("/__test/thread")
        Map<String, Object> thread() {
            Thread current = Thread.currentThread();
            return Map.of("virtual", current.isVirtual(), "name", current.getName());
        }
    }

    /**
     * Records the kind of thread the auto-configured listener container used for
     * one message.
     */
    static class ThreadProbeListener {

        final CompletableFuture<Boolean> observedVirtual = new CompletableFuture<>();

        @RabbitListener(queuesToDeclare = @Queue("test.thread-probe"))
        void on(String ignored) {
            observedVirtual.complete(Thread.currentThread().isVirtual());
        }
    }

    @Autowired
    RestTestClient client;
    @Autowired
    ThreadProbeListener probeListener;
    @Autowired
    RabbitTemplate rabbitTemplate;

    @Test
    void httpHandlerRunsOnAVirtualThread() {
        client.get().uri("/__test/thread")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.virtual").isEqualTo(true);
    }

    @Test
    void rabbitListenerRunsOnAVirtualThread() throws Exception {
        rabbitTemplate.convertAndSend("test.thread-probe", "ping");

        assertThat(probeListener.observedVirtual.get(10, TimeUnit.SECONDS)).isTrue();
    }
}
