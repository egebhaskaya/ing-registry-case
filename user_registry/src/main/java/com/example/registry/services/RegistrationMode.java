package com.example.registry.services;

/** Identifies the caller for processing metrics; both modes execute the same transaction. */
public enum RegistrationMode {
    SYNC,
    ASYNC
}
