-- Runs once, when the PostgreSQL data volume is first created (docker-entrypoint-initdb.d).
-- Design section 3: one server, one database per service, each owned by its own role so that
-- Registry physically cannot touch Compliance's tables and vice versa.

CREATE ROLE registry LOGIN PASSWORD 'registry';
CREATE DATABASE registry OWNER registry;
CREATE ROLE compliance LOGIN PASSWORD 'compliance';
CREATE DATABASE compliance OWNER compliance;

-- The logical-replication publication Debezium reads (design section 5). It is created here, before
-- any table exists, and covers every table of the schema, including ones created later by Flyway.
-- The connector itself only emits changes of public.outbox (table.include.list); the other tables
-- are decoded and discarded, which is negligible locally. Doing it at init time means the startup
-- order of Registry and the connector no longer matters: with Debezium's "filtered" auto-create the
-- connector task fails permanently when the outbox table does not exist yet.
-- UPDATE/DELETE on published tables need a replica identity; all Registry tables have primary keys.
\connect registry
CREATE PUBLICATION registry_outbox_pub FOR TABLES IN SCHEMA public;
