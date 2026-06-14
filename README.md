# Concord

A transactional outbox relay. Writes an event row in the same DB transaction as
the business row it describes, then relays it to a stream at least once, with
retries and dead-letter handling for the ones that never make it.

Early days. Core relay logic + in-memory adapters exist and are tested. Real
Postgres/Kafka/Redis/MinIO adapters and docs are next.
