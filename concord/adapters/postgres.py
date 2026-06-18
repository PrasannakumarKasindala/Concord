"""
Postgres adapter. The outbox table lives in the same database as your business
tables, which is the whole point: a producer writes its business row and the
outbox row in one transaction (see `enqueue_in_tx`), so they commit or roll back
together.

The claim query is the load-bearing line of the entire system:

    SELECT ... FROM outbox
    WHERE status = 'PENDING' AND next_attempt_at <= now()
    ORDER BY next_attempt_at
    FOR UPDATE SKIP LOCKED
    LIMIT :n

FOR UPDATE locks the claimed rows; SKIP LOCKED tells other relay workers to walk
past rows already locked by a peer instead of blocking on them. That one clause
is what lets you run twenty relay pods against one table with zero coordination
and zero double-publishing of the same row within a tick.
"""

from __future__ import annotations

from typing import Sequence

from ..model import OutboxRecord, Status
from ..ports import OutboxStore

DDL = """
CREATE TABLE IF NOT EXISTS outbox (
    id              UUID PRIMARY KEY,
    aggregate       TEXT NOT NULL,
    partition_key   TEXT,
    payload         BYTEA NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    attempts        INT  NOT NULL DEFAULT 0,
    created_at      DOUBLE PRECISION NOT NULL,
    next_attempt_at DOUBLE PRECISION NOT NULL,
    last_error      TEXT
);
-- Partial index: the relay only ever scans PENDING rows, so we only index those.
-- On a hot table this keeps the claim query off the PUBLISHED tombstones.
CREATE INDEX IF NOT EXISTS outbox_due_idx
    ON outbox (next_attempt_at)
    WHERE status = 'PENDING';
"""


class PostgresStore(OutboxStore):
    def __init__(self, dsn: str) -> None:
        self._psycopg = _import_psycopg()
        self._dsn = dsn
        self._conn = self._psycopg.connect(dsn, autocommit=False)

    def init_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(DDL)
        self._conn.commit()

    def enqueue(self, record: OutboxRecord) -> None:
        """
        Standalone enqueue (its own transaction). In real producer code you would
        instead call `enqueue_in_tx(cur, record)` inside the same transaction that
        writes your business row. This method exists for tooling and tests.
        """
        with self._conn.cursor() as cur:
            self.enqueue_in_tx(cur, record)
        self._conn.commit()

    @staticmethod
    def enqueue_in_tx(cur, record: OutboxRecord) -> None:
        cur.execute(
            """INSERT INTO outbox
               (id, aggregate, partition_key, payload, status, attempts,
                created_at, next_attempt_at, last_error)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (record.id, record.aggregate, record.key, record.payload,
             record.status.value, record.attempts, record.created_at,
             record.next_attempt_at, record.last_error),
        )

    def claim_batch(self, limit: int, now: float) -> Sequence[OutboxRecord]:
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT id, aggregate, partition_key, payload, status,
                          attempts, created_at, next_attempt_at, last_error
                   FROM outbox
                   WHERE status = 'PENDING' AND next_attempt_at <= %s
                   ORDER BY next_attempt_at
                   FOR UPDATE SKIP LOCKED
                   LIMIT %s""",
                (now, limit),
            )
            rows = cur.fetchall()
        # We hold the row locks until the transaction ends. The relay marks each
        # row and we commit per tick, releasing the locks.
        return [self._row(r) for r in rows]

    def mark_published(self, record_id: str) -> None:
        self._update("UPDATE outbox SET status='PUBLISHED' WHERE id=%s",
                     (record_id,))

    def mark_retry(self, record_id, attempts, next_attempt_at, error) -> None:
        self._update(
            """UPDATE outbox SET attempts=%s, next_attempt_at=%s,
               last_error=%s, status='PENDING' WHERE id=%s""",
            (attempts, next_attempt_at, error, record_id),
        )

    def mark_dead(self, record_id: str, error: str) -> None:
        self._update("UPDATE outbox SET status='DEAD', last_error=%s WHERE id=%s",
                     (error, record_id))

    def pending_count(self) -> int:
        with self._conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM outbox WHERE status='PENDING'")
            (n,) = cur.fetchone()
        self._conn.commit()
        return int(n)

    def _update(self, sql: str, args) -> None:
        with self._conn.cursor() as cur:
            cur.execute(sql, args)
        self._conn.commit()

    @staticmethod
    def _row(r) -> OutboxRecord:
        return OutboxRecord(
            id=str(r[0]), aggregate=r[1], key=r[2], payload=bytes(r[3]),
            status=Status(r[4]), attempts=r[5], created_at=r[6],
            next_attempt_at=r[7], last_error=r[8],
        )


def _import_psycopg():
    try:
        import psycopg  # psycopg 3
        return psycopg
    except ImportError as e:
        raise RuntimeError(
            "Postgres adapter needs psycopg. Install with: pip install 'concord[prod]'"
        ) from e
