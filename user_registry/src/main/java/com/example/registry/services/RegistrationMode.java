package com.example.registry.services;

/** Identifies how work enters processing; batched registrations share a database commit. */
public enum RegistrationMode {
    SYNC,
    ASYNC,
    BATCHED
}
