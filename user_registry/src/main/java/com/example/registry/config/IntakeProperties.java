package com.example.registry.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Bounds HTTP requests waiting for a publisher confirmation or a synchronous worker reply. */
@ConfigurationProperties("registry.intake")
public record IntakeProperties(int maxInFlight, Duration admissionWait) {
}
