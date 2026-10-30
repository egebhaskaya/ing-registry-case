# How /sync works

Both routes submit the same `RegisterUserCommand` to the same `registration.commands` queue.
The difference is the milestone that releases the HTTP response.

## Async: accepted and queued

1. Validate and normalize the request, including its `Idempotency-Key`.
2. Publish a command and wait for RabbitMQ's publisher confirmation.
3. If accepted and routed, return `202 Accepted` with its registration id.
4. A worker later commits the user and outbox event.

The response establishes queue acceptance. A business conflict found later is handled by the
worker's dead-letter path.

## Sync: queued, committed, then returned

1. Validate and normalize the same request.
2. Publish the same command with a reply address using Spring AMQP's built-in request/reply support.
3. Keep HTTP waiting. It holds an intake slot, but no database connection.
4. One of the same Rabbit workers takes the command and obtains a Hikari connection.
5. The shared registration service commits the user and outbox event together, then returns the connection.
6. After that transaction completes, the worker replies with the stored user or a business conflict.
7. HTTP returns `201 Created`, `200 OK` for an identical replay, or `409 Conflict`.

There is no second registration queue, worker pool or database implementation. Reply messages return
results to waiting HTTP handlers; they are not a separate registration pipeline.

## Two acknowledgements

- **Broker confirmation:** RabbitMQ accepted the publication. Enough for async `202`.
- **Worker result:** the registration transaction completed. Required for sync success.

Sync success contains the committed user id, registration id, normalized data and `status=REGISTERED`.
It does not establish that Compliance finished. After Registry commits, Debezium, Kafka and the same
Compliance consumer continue independently for either endpoint.

## Timeouts and retries

`spring.rabbitmq.template.reply-timeout` bounds the worker reply wait (default 30 seconds). If it
expires, HTTP returns `503` with `reason=reply_timeout`. The command may still be queued, executing,
or already committed with its reply lost. A timeout does not cancel it.

Retry using the **same idempotency key and body**. A completed command returns the stored user without
another outbox event. Concurrent identical retries are resolved through database uniqueness. Reusing
the key with different normalized data produces a conflict.

The common intake limit bounds HTTP confirmation/reply waiters. A request that cannot obtain a slot
is rejected before publishing. Worker processing continues independently of the HTTP connection.

## Comparing the endpoints

`make k6` measures HTTP response after queue acceptance; `make k6-sync` measures response after Registry
commit. Both use the same workers, prefetch, pool, threading settings and Compliance pipeline.

Each simulated client waits for its response before submitting another request. Sync can therefore
submit more slowly and build a smaller backlog. Compare queue depth, database waiting and end-to-end
completion alongside HTTP latency; acceptance speed alone is not database throughput.
