package com.example.registry.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The {@code registry.intake.*} block of application.yml: the HTTP admission
 * limit.
 *
 * <p>
 * Design sections 4 and 6: virtual threads remove the cost of waiting, but they
 * are not an admission
 * limit. This is the explicit one: at most {@code maxInFlight} requests may be
 * waiting on a broker
 * confirmation at any moment; the next request waits up to
 * {@code admissionWait} for a slot and is then
 * refused with a 503 rather than queued in memory without bound.
 */
@ConfigurationProperties("registry.intake")
public record IntakeProperties(int maxInFlight, Duration admissionWait) {
}
