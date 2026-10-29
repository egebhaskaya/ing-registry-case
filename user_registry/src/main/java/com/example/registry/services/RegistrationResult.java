package com.example.registry.services;

import com.example.registry.models.User;

/** The outcome and persisted user, returned only after the transactional proxy has committed. */
public record RegistrationResult(RegistrationOutcome outcome, User user) {
}
