"""
Concord: a transactional outbox relay that keeps a relational source of truth
and an event stream in agreement, with backoff retries and dead-letter archival.

The relay logic depends only on the ports defined in `concord.ports`. Production
adapters (Postgres, Kafka/Redpanda, Redis, MinIO) and in-memory adapters used by
tests and the benchmark implement the same interfaces, so the code path under
load is the code path in production.
"""

__version__ = "0.2.5"
__all__ = ["__version__"]
