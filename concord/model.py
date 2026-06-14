"""
The one datum this whole system exists to move safely: an OutboxRecord.

A record is written in the *same database transaction* as the business state
change that produced it. That single fact is the entire point of the pattern:
if the business row commits, the outbox row commits with it, atomically. There
is no window in which the database and the stream can disagree about whether an
event happened. The relay's job is then the comparatively boring one of getting
a committed record onto the stream at least once, and marking it done.
"""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field


class Status(str, enum.Enum):
    PENDING = "PENDING"        # committed, not yet published
    PUBLISHED = "PUBLISHED"    # confirmed on the stream, terminal-happy
    DEAD = "DEAD"              # exhausted retries, parked in the dead-letter store


@dataclass
class OutboxRecord:
    aggregate: str                       # e.g. "order", "payment"; becomes the topic
    payload: bytes                       # opaque event bytes (JSON, Avro, protobuf...)
    key: str | None = None               # partition key; ordering is per-key
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: Status = Status.PENDING
    attempts: int = 0
    created_at: float = field(default_factory=time.time)
    next_attempt_at: float = field(default_factory=time.time)
    last_error: str | None = None

    def dedupe_key(self) -> str:
        """
        Stable, content-independent identity used by the consumer side to drop
        duplicates. The relay guarantees at-least-once; this key is what turns
        that into effectively-once for a well-behaved consumer.
        """
        return f"{self.aggregate}:{self.id}"
