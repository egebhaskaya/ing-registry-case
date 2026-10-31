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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/** User and outbox writes share one transaction; the caller receives a result only after its proxy commits. */
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

    /** A uniqueness failure must roll back here before the processor starts duplicate resolution. */
    @Transactional
    public RegistrationResult register(RegisterUserCommand command) {
        Optional<User> existing = users.findByRegistrationId(command.registrationId());
        if (existing.isPresent()) {
            return outcomeForExisting(existing.get(), command);
        }

        PreparedRegistration prepared = prepare(command);
        users.insert(prepared.user());
        outbox.insert(prepared.event());
        log.info("registration prepared registrationId={} userId={}", command.registrationId(), prepared.user().id());
        return new RegistrationResult(RegistrationOutcome.CREATED, prepared.user());
    }

    /** Two JDBC batches and one commit; any conflict or write failure rolls the entire transaction back. */
    @Transactional
    public List<RegistrationResult> registerBatch(List<RegisterUserCommand> commands) {
        if (commands.isEmpty()) {
            return List.of();
        }
        List<UUID> registrationIds = commands.stream().map(RegisterUserCommand::registrationId).distinct().toList();
        Map<UUID, User> knownUsers = new HashMap<>();
        for (User existing : users.findByRegistrationIds(registrationIds)) {
            knownUsers.put(existing.registrationId(), existing);
        }
        List<User> newUsers = new ArrayList<>();
        List<OutboxEvent> events = new ArrayList<>();
        List<RegistrationResult> results = new ArrayList<>(commands.size());
        for (RegisterUserCommand command : commands) {
            User existing = knownUsers.get(command.registrationId());
            if (existing != null) {
                results.add(outcomeForExisting(existing, command));
                continue;
            }
            PreparedRegistration prepared = prepare(command);
            newUsers.add(prepared.user());
            events.add(prepared.event());
            knownUsers.put(command.registrationId(), prepared.user());
            results.add(new RegistrationResult(RegistrationOutcome.CREATED, prepared.user()));
        }
        users.insertBatch(newUsers);
        outbox.insertBatch(events);
        return List.copyOf(results);
    }

    private PreparedRegistration prepare(RegisterUserCommand command) {
        // PostgreSQL stores microseconds: creation and later idempotent replays return identical data.
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        RegistrationPayload payload = command.payload();
        User user = new User(UUID.randomUUID(), command.registrationId(), command.payloadFingerprint(),
                payload.email(), payload.fullName(), payload.countryCode(), now);

        // One id for the outbox row and for the payload inside it: the stable event id
        // (design section 5).
        UUID eventId = UUID.randomUUID();
        UserRegisteredEvent event = new UserRegisteredEvent(eventId, user.id(), command.registrationId(),
                user.email(), user.fullName(), user.countryCode(), command.submittedAt(), now);
        return new PreparedRegistration(user, new OutboxEvent(eventId, AGGREGATE_TYPE, user.id().toString(), EVENT_TYPE,
                objectMapper.writeValueAsString(event), now));
    }

    /** Classifies a rolled-back uniqueness violation using a fresh read-only transaction. */
    @Transactional(readOnly = true)
    public RegistrationResult resolveDuplicate(RegisterUserCommand command) {
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

    private record PreparedRegistration(User user, OutboxEvent event) {
    }
}
