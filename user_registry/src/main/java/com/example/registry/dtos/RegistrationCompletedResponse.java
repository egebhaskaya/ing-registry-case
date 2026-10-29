package com.example.registry.dtos;

import com.example.registry.models.User;
import java.time.Instant;
import java.util.UUID;

/** A committed Registry user; downstream Compliance may still be pending. */
public record RegistrationCompletedResponse(UUID userId, UUID registrationId, String fullName,
        String email, String countryCode, Instant createdAt, String status) {
    public static RegistrationCompletedResponse from(User user) {
        return new RegistrationCompletedResponse(user.id(), user.registrationId(), user.fullName(),
                user.email(), user.countryCode(), user.createdAt(), "REGISTERED");
    }
}
