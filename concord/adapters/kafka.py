"""
Kafka / Redpanda broker adapter. Redpanda speaks the Kafka protocol, so the same
confluent-kafka client talks to either; which one runs is a deployment choice,
not a code choice (see ADR 0002).

The subtlety here is the synchronous flush. The relay's contract is "publish
raises on failure, returns on success," because the relay decides retry-vs-dead
based on that. confluent-kafka's produce() is async and buffers, so a naive
adapter would return success for a message that later fails to send. We produce
then flush(), and surface any delivery error to the caller. Slower than
fire-and-forget, correct in a way fire-and-forget is not.
"""

from __future__ import annotations

from ..ports import Broker


class KafkaBroker(Broker):
    def __init__(self, bootstrap_servers: str, acks: str = "all") -> None:
        Producer = _import_producer()
        # acks=all: the leader waits for all in-sync replicas. This is the
        # durability setting; anything less can lose an acknowledged write on
        # broker failover, which would defeat the entire point of the outbox.
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": acks,
            "enable.idempotence": True,   # dedup within a producer session
            "max.in.flight.requests.per.connection": 5,
        })
        self._delivery_error: Exception | None = None

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self._delivery_error = Exception(str(err))

    def publish(self, topic: str, key: str | None, value: bytes) -> None:
        self._delivery_error = None
        self._producer.produce(
            topic,
            key=key.encode() if key else None,
            value=value,
            on_delivery=self._on_delivery,
        )
        # Block until this message is acknowledged or errors. Per-record flush
        # is deliberate: the relay batches at the claim layer, not the produce
        # layer, so it can make an individual retry decision per record.
        self._producer.flush(timeout=10)
        if self._delivery_error is not None:
            raise self._delivery_error


def _import_producer():
    try:
        from confluent_kafka import Producer
        return Producer
    except ImportError as e:
        raise RuntimeError(
            "Kafka adapter needs confluent-kafka. Install: pip install 'concord[prod]'"
        ) from e
