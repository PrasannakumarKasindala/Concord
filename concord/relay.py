"""
The relay. This is the whole product in one file, and it is deliberately small,
because the interesting engineering is in the *guarantees*, not the line count.

One tick of the relay:

    1. Claim a batch of due PENDING records (FOR UPDATE SKIP LOCKED in prod, so
       many workers scale horizontally without stepping on each other).
    2. For each record, publish to the broker.
         - success -> mark PUBLISHED.
         - failure -> increment attempts. If under the cap, schedule a retry
           with jittered backoff. If at the cap, archive to the dead-letter
           store and mark DEAD so it stops consuming the hot path.
    3. Return how many were processed, so the caller can back off polling when idle.

Delivery is at-least-once by construction: we publish *before* we mark published,
so a crash between the two re-delivers rather than loses. The consumer-side
DedupeCache is what upgrades at-least-once to effectively-once. We never claim
exactly-once end to end, because across two systems without a shared transaction
that is a marketing word, not an engineering one.
"""

from __future__ import annotations

import time

from .backoff import next_delay
from .config import Config
from .logging_ import get_logger
from .metrics import Metrics
from .model import OutboxRecord
from .ports import Broker, DeadLetterArchive, DedupeCache, OutboxStore

log = get_logger("concord.relay")


class Relay:
    def __init__(
        self,
        store: OutboxStore,
        broker: Broker,
        archive: DeadLetterArchive,
        config: Config,
        dedupe: DedupeCache | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self.store = store
        self.broker = broker
        self.archive = archive
        self.cfg = config
        self.dedupe = dedupe
        self.metrics = metrics or Metrics()

    def tick(self, now: float | None = None) -> int:
        """Process one batch. Returns the number of records handled."""
        now = time.time() if now is None else now
        batch = self.store.claim_batch(self.cfg.batch_size, now)
        for record in batch:
            self._handle(record)
        return len(batch)

    def _handle(self, record: OutboxRecord) -> None:
        # Consumer-side idempotency guard. If we already confirmed a publish for
        # this key (e.g. a crash re-delivered it), drop it cheaply. We check here
        # and only record AFTER a successful publish below, never before.
        if self.dedupe is not None and self.dedupe.seen_before(record.dedupe_key()):
            self.store.mark_published(record.id)
            self.metrics.record_duplicate()
            log.info("duplicate.dropped", extra={"record_id": record.id,
                                                 "aggregate": record.aggregate})
            return

        started = time.perf_counter()
        try:
            self.broker.publish(record.aggregate, record.key, record.payload)
        except Exception as exc:  # broker down, timeout, serialization, etc.
            self._on_failure(record, exc)
            return

        # Publish confirmed: NOW it is safe to record the dedupe key.
        if self.dedupe is not None:
            self.dedupe.mark_seen(record.dedupe_key())
        self.store.mark_published(record.id)
        latency_ms = (time.perf_counter() - started) * 1000.0
        self.metrics.record_publish(latency_ms)
        log.info("published", extra={"record_id": record.id,
                                     "aggregate": record.aggregate,
                                     "attempts": record.attempts + 1,
                                     "latency_ms": round(latency_ms, 2)})

    def _on_failure(self, record: OutboxRecord, exc: Exception) -> None:
        attempts = record.attempts + 1
        err = f"{type(exc).__name__}: {exc}"
        if attempts >= self.cfg.max_attempts:
            locator = self.archive.archive(record)
            self.store.mark_dead(record.id, err)
            self.metrics.record_dead()
            log.error("dead_lettered", extra={"record_id": record.id,
                                               "aggregate": record.aggregate,
                                               "attempts": attempts,
                                               "archive": locator,
                                               "error": err})
            return

        delay = next_delay(attempts, self.cfg.backoff_base_s, self.cfg.backoff_cap_s)
        self.store.mark_retry(record.id, attempts, time.time() + delay, err)
        self.metrics.record_retry()
        log.warning("retry_scheduled", extra={"record_id": record.id,
                                              "aggregate": record.aggregate,
                                              "attempts": attempts,
                                              "retry_in_s": round(delay, 3),
                                              "error": err})

    def run_forever(self, stop: "callable[[], bool] | None" = None) -> None:
        """Poll loop. Sleeps the poll interval only when a tick found nothing,
        so a busy relay stays hot and an idle one stays cheap."""
        idle_sleep = self.cfg.poll_interval_ms / 1000.0
        while not (stop and stop()):
            handled = self.tick()
            if handled == 0:
                time.sleep(idle_sleep)
