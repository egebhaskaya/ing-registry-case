# Demo

1. Start Docker, then run `make up` from the repository root.
2. Run `make registry` and `make compliance` in separate terminals.
3. Open [Registration Case](http://localhost:3000/d/registration-case): Last 5 minutes, refresh 1s.
4. Run `make k6`. Watch HTTP acceptance, Rabbit backlog, Registry commits and Compliance catching up.
   Wait for the queue to drain before the next run.
5. Run `make k6-sync`. The HTTP handler calls the shared processor directly and returns after the
   Registry commit. Rabbit queue activity is not expected from this run. Compliance still runs
   independently. Response timing also affects the closed-loop submission rate.
6. Stop Registry, edit its `application.properties`, and restart with `make registry`. Change one
   setting at a time: async Rabbit workers/prefetch, shared pool size or virtual-thread mode. Rabbit
   worker settings affect `make k6`; pool and HTTP admission also govern `make k6-sync`.
7. Use `make smoke` for a real end-to-end count check, `make connector` for CDC status and `make lag`
   for Compliance's Kafka backlog. `make test` runs automated correctness checks.

For a sustained comparison, run `make k6 cont 200`, stop with Ctrl+C once and wait for
the async backlog to drain. Then run `make k6-sync cont 200` at the same target rate. Replace 200
with your desired requests per second.
The shared HTML report includes each stopped run and any arrivals dropped by the load generator.

Keep client settings fixed when comparing application settings. Repeat measurements; all components
share one machine. Retry a lost HTTP response with the same key and body to preserve idempotency.
