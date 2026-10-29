package com.example.registry.dtos;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * The HTTP request body of POST /registrations. Design §4: exactly these three
 * fields.
 *
 * <p>
 * A Java {@code record} is an immutable data class: the compiler generates the
 * constructor,
 * accessors ({@code fullName()}, not {@code getFullName()}), {@code equals},
 * {@code hashCode} and
 * {@code toString}. Jackson builds it from JSON; the constraint annotations are
 * checked by Bean
 * Validation when the controller parameter is marked {@code @Valid}, and any
 * violation becomes a 400.
 */
public record RegistrationRequest(
                @NotBlank @Size(max = 100) String fullName,
                @NotBlank @Email @Size(max = 254) String email,
                @NotBlank @Pattern(regexp = "^[A-Za-z]{2}$", message = "must be a two-letter country code") String countryCode) {
}
