# Architecture

Concord is a hexagonal (ports-and-adapters) application. The relay logic depends
only on four small interfaces; production and test wiring supply different
implementations. The code path under load is the code path in production, which
is why the benchmark numbers mean something.

## Data flow

```
  Producer service (your code)
  ┌──────────────────────────────────────────┐
  │  BEGIN;                                     │
  │    INSERT INTO orders (...);   business row │
  │    INSERT INTO outbox (...);   event row    │   one transaction:
  │  COMMIT;                                     │   both or neither
  └───────────────────────┬─────────────────────┘
                          │  (committed)
                          v
                 ┌──────────────────┐
                 │  outbox table     │   PENDING rows wait here durably
                 │  (Postgres)       │
                 └────────┬──────────┘
                          │  SELECT ... FOR UPDATE SKIP LOCKED
                          v
             ┌──────────────────────────┐
             │  Concord relay (N workers)│
             │   claim -> publish -> ack │
             └───┬───────────┬───────────┘
        success  │           │  failure (retry w/ backoff; then dead-letter)
                 v           v
        ┌────────────┐  ┌──────────────────┐
        │  Kafka /    │  │  MinIO / S3       │
        │  Redpanda   │  │  dead-letter      │
        └─────┬───────┘  └──────────────────┘
              │  consumer checks Redis dedupe key -> effectively-once
              v
        Downstream consumers
```

## Ports (in `concord/ports.py`)

| Port | Responsibility | Prod adapter | Test/bench adapter |
|------|----------------|--------------|--------------------|
| `OutboxStore` | durable rows + atomic claim | `PostgresStore` | `MemoryStore` |
| `Broker` | publish to the stream, raise on failure | `KafkaBroker` | `MemoryBroker` / `FlakyBroker` / `MeasuringBroker` |
| `DedupeCache` | consumer-side idempotency | `RedisDedupe` | `MemoryDedupe` |
| `DeadLetterArchive` | park poison records for forensics | `MinioArchive` | `MemoryArchive` |

## Delivery guarantee, stated precisely

Concord provides **at-least-once** delivery from database to stream, upgraded to
**effectively-once** for a consumer that honours the dedupe key. It does not claim
exactly-once end-to-end, because across two systems without a shared transaction
that phrase is marketing, not engineering. The ordering guarantee is per key: all
events sharing a partition key preserve their commit order.

The one invariant the whole design protects: **a committed business change can
never be silently absent from the stream.** Worst case, it is delivered more than
once (deduped downstream) or parked in the dead-letter store for a human. It is
never lost.
