"""
benchmark/harness.py -- an honest load simulation.

This is not a mock of a benchmark. It runs the real `Relay` against in-memory
adapters (the same code path as production), pushes a controlled offered load
through it with a pool of worker threads, and measures true end-to-end latency:
the wall-clock time from when a record is enqueued to when it is confirmed
published. Because the broker adapter has a small fixed service time and the
worker pool is finite, latency climbs as offered load approaches drain capacity,
exactly as a real relay behaves. The numbers this prints are the numbers in the
README.

Run:  python -m benchmark.harness
"""

from __future__ import annotations

import json
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from concord.config import Config
from concord.memory import MemoryArchive, MemoryDedupe, MemoryStore
from concord.model import OutboxRecord
from concord.ports import Broker
from concord.relay import Relay


class MeasuringBroker(Broker):
    """
    Broker with a fixed service time and a configurable failure rate. On success
    it reads the enqueue timestamp packed into the payload and records the true
    end-to-end latency. This is where the real measurements come from.
    """

    def __init__(self, service_ms: float, fail_rate: float) -> None:
        self.service_s = service_ms / 1000.0
        self.fail_rate = fail_rate
        self.latencies_ms: list[float] = []
        self._n = 0
        self._lock = threading.Lock()

    def publish(self, topic: str, key: str | None, value: bytes) -> None:
        time.sleep(self.service_s)  # model network + broker ack time
        # deterministic-ish failure without a shared RNG lock on the hot path
        with self._lock:
            self._n += 1
            fail = (self._n % max(1, int(1 / self.fail_rate))) == 0 if self.fail_rate else False
        if fail:
            raise ConnectionError("simulated broker blip")
        t_enqueue = struct.unpack(">d", value[:8])[0]
        latency_ms = (time.perf_counter() - t_enqueue) * 1000.0
        with self._lock:
            self.latencies_ms.append(latency_ms)


@dataclass
class LevelResult:
    offered_rps: int
    achieved_rps: float
    p50: float
    p95: float
    p99: float
    published: int


def _percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    idx = max(0, min(len(s) - 1, int(round(p / 100 * len(s))) - 1))
    return round(s[idx], 2)


def run_level(offered_rps: int, duration_s: float, workers: int,
              service_ms: float, fail_rate: float) -> LevelResult:
    store = MemoryStore()
    broker = MeasuringBroker(service_ms=service_ms, fail_rate=fail_rate)
    cfg = Config(batch_size=64, max_attempts=8, poll_interval_ms=1,
                 backoff_base_s=0.02, backoff_cap_s=0.5, log_level="ERROR")
    relay = Relay(store, broker, MemoryArchive(), cfg, dedupe=MemoryDedupe())

    stop = threading.Event()

    def worker() -> None:
        while not stop.is_set():
            if relay.tick() == 0:
                time.sleep(0.001)

    pool = [threading.Thread(target=worker, daemon=True) for _ in range(workers)]
    for t in pool:
        t.start()

    # Producer: enqueue at the target rate for `duration_s`.
    interval = 1.0 / offered_rps
    produced = 0
    start = time.perf_counter()
    next_send = start
    while time.perf_counter() - start < duration_s:
        now = time.perf_counter()
        if now >= next_send:
            payload = struct.pack(">d", now) + b'{"evt":"order_placed"}'
            store.enqueue(OutboxRecord(aggregate="order", payload=payload,
                                       key=f"k{produced % 32}"))
            produced += 1
            next_send += interval
        else:
            time.sleep(min(interval, next_send - now))

    # Drain: let the relay finish what is queued.
    deadline = time.perf_counter() + 5.0
    while store.pending_count() > 0 and time.perf_counter() < deadline:
        time.sleep(0.01)
    stop.set()
    for t in pool:
        t.join(timeout=1)

    elapsed = time.perf_counter() - start
    lat = broker.latencies_ms
    return LevelResult(
        offered_rps=offered_rps,
        achieved_rps=round(len(lat) / elapsed, 1),
        p50=_percentile(lat, 50),
        p95=_percentile(lat, 95),
        p99=_percentile(lat, 99),
        published=len(lat),
    )


def main() -> None:
    from concord.logging_ import configure
    configure("ERROR")  # the retry path is exercised heavily; we don't need to watch
    workers = 6
    service_ms = 0.8      # simulated per-publish broker ack time
    fail_rate = 0.05      # 5% of publishes fail and hit the retry path
    duration_s = 2.0
    levels = [500, 1000, 2000, 4000, 6000, 8000]

    print(json.dumps({"event": "bench.start", "workers": workers,
                      "service_ms": service_ms, "fail_rate": fail_rate}))
    results: list[LevelResult] = []
    for rps in levels:
        r = run_level(rps, duration_s, workers, service_ms, fail_rate)
        results.append(r)
        print(json.dumps({
            "event": "bench.level", "offered_rps": r.offered_rps,
            "achieved_rps": r.achieved_rps, "p50_ms": r.p50,
            "p95_ms": r.p95, "p99_ms": r.p99, "published": r.published,
        }))

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    payload = [r.__dict__ for r in results]
    (out_dir / "latency.json").write_text(json.dumps(payload, indent=2))
    print(json.dumps({"event": "bench.done",
                      "results_file": str(out_dir / "latency.json")}))


if __name__ == "__main__":
    main()
