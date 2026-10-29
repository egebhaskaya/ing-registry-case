package com.example.registry.services;

import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationPayload;
import com.example.registry.messaging.UserRegisteredEvent;
import com.example.registry.models.OutboxEvent;
import com.example.registry.models.User;
import com.example.registry.repositories.OutboxRepository;
import com.example.registry.repositories.UserRepository;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * The registration transaction (design section 5): one user plus one outbox
 * event, committed together.
 *
 * <p>
 * {@code @Transactional} makes Spring wrap the method in a database
 * transaction: a connection is
 * taken from the Hikari pool, every SQL statement inside runs on it, and the
 * transaction commits when
 * the method returns, or rolls back when it throws a {@code RuntimeException}
 * (Spring's default; a
 * checked exception would commit, which is why nothing here throws one). That
 * is what makes "user and
 * outbox together" true: if the outbox insert fails, the user insert is undone
 * with it.
 *
 * <p>
 * This works because the listener calls this bean from the outside. Spring
 * applies
 * {@code @Transactional} through a proxy around the bean, so a method calling
 * another method of the
 * same class would bypass it.
 */
@Service
public class RegistrationService {

    private static final Logger log = LoggerFactory.getLogger(RegistrationService.class);

    static final String AGGREGATE_TYPE = "user";
    static final String EVENT_TYPE = "UserRegistered";

    private final UserRepository users;
    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;

    public RegistrationService(UserRepository users, OutboxRepository outbox, ObjectMapper objectMapper) {
        this.users = users;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    /**
     * Registers the user, or recognizes a redelivery.
     *
     * <p>
     * A {@code DuplicateKeyException} from an INSERT is deliberately NOT caught
     * here: PostgreSQL has
     * already marked this transaction as failed, so no further statement can run on
     * it (design section 5:
     * "do not continue using a transaction already marked for rollback"). The
     * exception rolls the
     * transaction back, and the listener then calls {@link #resolveDuplicate} in a
     * fresh one.
     */
    @Transactional
    public RegistrationOutcome register(RegisterUserCommand command) {
        return registerWithResult(command).outcome();
    }

    /** Both public entry points have a transaction; the HTTP path also needs the stored user. */
    @Transactional
    public RegistrationResult registerWithResult(RegisterUserCommand command) {
        Optional<User> existing = users.findByRegistrationId(command.registrationId());
        if (existing.isPresent()) {
            return outcomeForExisting(existing.get(), command);
        }

        // PostgreSQL stores microseconds: creation and later idempotent replays return identical data.
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        RegistrationPayload payload = command.payload();
        User user = new User(UUID.randomUUID(), command.registrationId(), command.payloadFingerprint(),
                payload.email(), payload.fullName(), payload.countryCode(), now);
        users.insert(user);

        // One id for the outbox row and for the payload inside it: the stable event id
        // (design section 5).
        UUID eventId = UUID.randomUUID();
        UserRegisteredEvent event = new UserRegisteredEvent(eventId, user.id(), command.registrationId(),
                user.email(), user.fullName(), user.countryCode(), command.submittedAt(), now);
        outbox.insert(new OutboxEvent(eventId, AGGREGATE_TYPE, user.id().toString(), EVENT_TYPE,
                objectMapper.writeValueAsString(event), now));

        log.info("registration committed registrationId={} userId={}", command.registrationId(), user.id());
        return new RegistrationResult(RegistrationOutcome.CREATED, user);
    }

    /**
     * Called after {@link #register} failed on a uniqueness violation, in a new
     * read-only transaction,
     * to decide which rule was hit: a concurrent duplicate of the same registration
     * (harmless), the same
     * key with other data, or a second registration for an existing email.
     */
    @Transactional(readOnly = true)
    public RegistrationOutcome resolveDuplicate(RegisterUserCommand command) {
        return resolveDuplicateWithResult(command).outcome();
    }

    @Transactional(readOnly = true)
    public RegistrationResult resolveDuplicateWithResult(RegisterUserCommand command) {
        Optional<User> existing = users.findByRegistrationId(command.registrationId());
        if (existing.isPresent()) {
            return outcomeForExisting(existing.get(), command);
        }
        if (users.existsByEmail(command.payload().email())) {
            throw new EmailAlreadyRegisteredException(command.registrationId());
        }
        throw new IllegalStateException(
                "Unexplained uniqueness violation for registrationId=" + command.registrationId());
    }

    private RegistrationResult outcomeForExisting(User existing, RegisterUserCommand command) {
        if (existing.payloadFingerprint().equals(command.payloadFingerprint())) {
            log.info("registration duplicate ignored registrationId={}", command.registrationId());
            return new RegistrationResult(RegistrationOutcome.DUPLICATE_IGNORED, existing);
        }
        throw new ConflictingPayloadException(command.registrationId());
    }
}
