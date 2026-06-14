"""
Ports (hexagonal-architecture style). The relay depends on these Protocols and
nothing concrete. Postgres/Kafka/Redis/MinIO adapters implement them for
production; in-memory adapters implement them for tests and the benchmark. Same
relay code, different plumbing.

Kept deliberately small. A port with fifteen methods is a class wearing an
interface as a disguise.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from .model import OutboxRecord


@runtime_checkable
class OutboxStore(Protocol):
    """The relational source of truth plus its outbox table."""

    def enqueue(self, record: OutboxRecord) -> None:
        """Persist a PENDING record. In prod this shares the caller's transaction."""

    def claim_batch(self, limit: int, now: float) -> Sequence[OutboxRecord]:
        """
        Atomically claim up to `limit` due PENDING records for this worker.
        Prod uses SELECT ... FOR UPDATE SKIP LOCKED so N relay workers never
        fight over the same row. Returns records whose next_attempt_at <= now.
        """

    def mark_published(self, record_id: str) -> None:
        ...

    def mark_retry(self, record_id: str, attempts: int,
                   next_attempt_at: float, error: str) -> None:
        ...

    def mark_dead(self, record_id: str, error: str) -> None:
        ...

    def pending_count(self) -> int:
        """For lag/observability. Cheap-ish; used by metrics, not the hot path."""


@runtime_checkable
class Broker(Protocol):
    """The destination event stream (Kafka / Redpanda)."""

    def publish(self, topic: str, key: str | None, value: bytes) -> None:
        """Synchronously publish or raise. Raising triggers the retry path."""


@runtime_checkable
class DedupeCache(Protocol):
    """Consumer-side idempotency (Redis in prod)."""

    def seen_before(self, dedupe_key: str) -> bool:
        """Return True if this key was already processed; record it if not."""


@runtime_checkable
class DeadLetterArchive(Protocol):
    """Where poison records go to be examined by a human later (MinIO in prod)."""

    def archive(self, record: OutboxRecord) -> str:
        """Persist the full record for forensics. Returns a locator (URI/key)."""
