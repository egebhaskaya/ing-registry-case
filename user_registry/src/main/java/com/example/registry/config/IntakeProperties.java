package com.example.registry.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Bounds HTTP requests waiting for an async publisher confirmation or a direct synchronous commit. */
@ConfigurationProperties("registry.intake")
public record IntakeProperties(int maxInFlight, Duration admissionWait) {
}
