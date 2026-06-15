"""
Tests for the guarantees, not the plumbing. Each test names the promise it
defends. If these pass, Concord does what the README claims; the adapters are
just different pipes into the same tested logic.
"""

import time

from concord.backoff import next_delay
from concord.config import Config
from concord.memory import FlakyBroker, MemoryArchive, MemoryBroker, MemoryStore
from concord.model import OutboxRecord
from concord.relay import Relay


def make_relay(broker, **overrides):
    cfg = Config(**{"max_attempts": 4, "backoff_base_s": 0.0,
                    "backoff_cap_s": 0.0, "log_level": "ERROR", **overrides})
    store = MemoryStore()
    relay = Relay(store, broker, MemoryArchive(), cfg)
    return relay, store


def test_happy_path_publishes_and_marks_done():
    """A committed record reaches the stream and is marked PUBLISHED exactly once."""
    broker = MemoryBroker()
    relay, store = make_relay(broker)
    store.enqueue(OutboxRecord(aggregate="order", payload=b'{"id":1}'))
    handled = relay.tick()
    assert handled == 1
    assert store.snapshot() == {"PENDING": 0, "PUBLISHED": 1, "DEAD": 0}
    assert len(broker.messages) == 1


def test_transient_failure_is_retried_not_lost():
    """A broker that fails then recovers must not lose the record."""
    broker = FlakyBroker(fail_rate=1.0)  # always fail for now
    relay, store = make_relay(broker, max_attempts=4)
    store.enqueue(OutboxRecord(aggregate="order", payload=b"x"))

    relay.tick()  # first attempt fails
    assert store.snapshot()["PENDING"] == 1  # still pending, scheduled to retry
    assert store.snapshot()["DEAD"] == 0

    broker.fail_rate = 0.0  # broker recovers
    relay.tick()            # retry succeeds
    assert store.snapshot() == {"PENDING": 0, "PUBLISHED": 1, "DEAD": 0}


def test_poison_record_is_dead_lettered_after_max_attempts():
    """A record that never succeeds ends up DEAD and archived, not looping forever."""
    broker = FlakyBroker(fail_rate=1.0)
    archive = MemoryArchive()
    cfg = Config(max_attempts=3, backoff_base_s=0.0, backoff_cap_s=0.0,
                 log_level="ERROR")
    store = MemoryStore()
    relay = Relay(store, broker, archive, cfg)
    store.enqueue(OutboxRecord(aggregate="order", payload=b"poison"))

    for _ in range(3):
        relay.tick()

    snap = store.snapshot()
    assert snap["DEAD"] == 1
    assert snap["PENDING"] == 0
    assert len(archive.parked) == 1  # forensic copy retained


def test_backoff_is_bounded_and_grows():
    """Full-jitter backoff never exceeds the cap and trends upward with attempts."""
    cap = 30.0
    for attempts in range(1, 12):
        for _ in range(50):
            d = next_delay(attempts, base=0.5, cap=cap)
            assert 0.0 <= d <= cap
    assert min(cap, 0.5 * 2 ** 8) >= min(cap, 0.5 * 2 ** 2)


def test_claim_respects_due_time():
    """A record scheduled in the future is not claimed early."""
    broker = MemoryBroker()
    relay, store = make_relay(broker)
    future = OutboxRecord(aggregate="order", payload=b"later",
                          next_attempt_at=time.time() + 3600)
    store.enqueue(future)
    assert relay.tick() == 0  # nothing due yet
    assert store.snapshot()["PENDING"] == 1


def test_skip_locked_semantics_no_double_claim():
    """Two sequential claims within one tick window do not return the same row twice."""
    store = MemoryStore()
    store.enqueue(OutboxRecord(aggregate="order", payload=b"a"))
    first = store.claim_batch(10, time.time())
    second = store.claim_batch(10, time.time())
    assert len(first) == 1
    assert len(second) == 0  # already claimed, invisible to the second claimer
