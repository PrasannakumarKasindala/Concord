"""
In-memory adapters. These implement the exact same ports as the production
Postgres/Kafka/Redis/MinIO adapters, so the relay under test and under benchmark
runs the identical code path it runs in production. The only thing that changes
is where the bytes physically go.

They are also intentionally *imperfect* in useful ways: FlakyBroker fails a
configurable fraction of publishes so the retry and dead-letter paths get real
exercise instead of being decorative.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Sequence

from .model import OutboxRecord, Status
from .ports import Broker, DeadLetterArchive, DedupeCache, OutboxStore


class MemoryStore(OutboxStore):
    """A dict standing in for Postgres + the outbox table. Thread-safe."""

    def __init__(self) -> None:
        self._rows: dict[str, OutboxRecord] = {}
        self._claimed: set[str] = set()
        self._lock = threading.Lock()

    def enqueue(self, record: OutboxRecord) -> None:
        with self._lock:
            self._rows[record.id] = record

    def claim_batch(self, limit: int, now: float) -> Sequence[OutboxRecord]:
        with self._lock:
            due = [
                r for r in self._rows.values()
                if r.status == Status.PENDING
                and r.next_attempt_at <= now
                and r.id not in self._claimed
            ]
            due.sort(key=lambda r: r.next_attempt_at)
            batch = due[:limit]
            # Model SKIP LOCKED: claimed rows are invisible to other claimers
            # until released by a mark_* call.
            for r in batch:
                self._claimed.add(r.id)
            return list(batch)

    def mark_published(self, record_id: str) -> None:
        with self._lock:
            r = self._rows.get(record_id)
            if r:
                r.status = Status.PUBLISHED
            self._claimed.discard(record_id)

    def mark_retry(self, record_id: str, attempts: int,
                   next_attempt_at: float, error: str) -> None:
        with self._lock:
            r = self._rows.get(record_id)
            if r:
                r.attempts = attempts
                r.next_attempt_at = next_attempt_at
                r.last_error = error
                r.status = Status.PENDING
            self._claimed.discard(record_id)

    def mark_dead(self, record_id: str, error: str) -> None:
        with self._lock:
            r = self._rows.get(record_id)
            if r:
                r.status = Status.DEAD
                r.last_error = error
            self._claimed.discard(record_id)

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for r in self._rows.values() if r.status == Status.PENDING)

    # test/inspection helpers (not part of the port)
    def snapshot(self) -> dict[str, int]:
        with self._lock:
            out = {"PENDING": 0, "PUBLISHED": 0, "DEAD": 0}
            for r in self._rows.values():
                out[r.status.value] += 1
            return out


class MemoryBroker(Broker):
    """A perfectly reliable broker. Records everything it receives."""

    def __init__(self, latency_s: float = 0.0) -> None:
        self.messages: list[tuple[str, str | None, bytes]] = []
        self.latency_s = latency_s
        self._lock = threading.Lock()

    def publish(self, topic: str, key: str | None, value: bytes) -> None:
        if self.latency_s:
            time.sleep(self.latency_s)
        with self._lock:
            self.messages.append((topic, key, value))


class FlakyBroker(Broker):
    """Fails a fraction of publishes to exercise retry and dead-letter paths."""

    def __init__(self, fail_rate: float = 0.15, latency_s: float = 0.0,
                 seed: int | None = None) -> None:
        self.fail_rate = fail_rate
        self.latency_s = latency_s
        self.messages: list[tuple[str, str | None, bytes]] = []
        self._rng = random.Random(seed)
        self._lock = threading.Lock()

    def publish(self, topic: str, key: str | None, value: bytes) -> None:
        if self.latency_s:
            time.sleep(self.latency_s)
        with self._lock:
            roll = self._rng.random()
        if roll < self.fail_rate:
            raise ConnectionError("broker temporarily unavailable (simulated)")
        with self._lock:
            self.messages.append((topic, key, value))


class MemoryDedupe(DedupeCache):
    """Consumer-side idempotency, standing in for Redis SETNX."""

    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def seen_before(self, dedupe_key: str) -> bool:
        with self._lock:
            if dedupe_key in self._seen:
                return True
            self._seen.add(dedupe_key)
            return False


class MemoryArchive(DeadLetterArchive):
    """Dead-letter archive standing in for a MinIO/S3 bucket."""

    def __init__(self) -> None:
        self.parked: dict[str, OutboxRecord] = {}
        self._lock = threading.Lock()

    def archive(self, record: OutboxRecord) -> str:
        with self._lock:
            self.parked[record.id] = record
        return f"memory://deadletter/{record.aggregate}/{record.id}.json"
