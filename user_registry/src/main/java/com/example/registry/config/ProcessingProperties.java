package com.example.registry.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Per-instance registration capacity, shared by HTTP and Rabbit consumers before opening a transaction. */
@ConfigurationProperties("registry.processing")
public record ProcessingProperties(int maxConcurrency, int maxWaitingRequests, Duration admissionWait) {
    public ProcessingProperties {
        if (maxConcurrency < 1 || maxWaitingRequests < 0 || admissionWait == null || admissionWait.isNegative()) {
            throw new IllegalArgumentException(
                    "Processing requires max-concurrency >= 1, max-waiting-requests >= 0 and admission-wait >= 0");
        }
    }
}
