"""
Metrics. Counters for outcomes and a small reservoir for end-to-end latency so
we can report p50/p95/p99 honestly rather than quoting an average, which hides
exactly the tail that pages you.

In production these counters would be exported to Prometheus; here they are
in-process and read by the benchmark harness and the CLI's `stats` command.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class Metrics:
    published: int = 0
    retried: int = 0
    dead_lettered: int = 0
    duplicates_dropped: int = 0
    _latencies_ms: list[float] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_publish(self, latency_ms: float) -> None:
        with self._lock:
            self.published += 1
            self._latencies_ms.append(latency_ms)

    def record_retry(self) -> None:
        with self._lock:
            self.retried += 1

    def record_dead(self) -> None:
        with self._lock:
            self.dead_lettered += 1

    def record_duplicate(self) -> None:
        with self._lock:
            self.duplicates_dropped += 1

    def percentiles(self) -> dict[str, float]:
        with self._lock:
            data = sorted(self._latencies_ms)
        if not data:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}

        def pct(p: float) -> float:
            # Nearest-rank; simple and defensible.
            idx = max(0, min(len(data) - 1, int(round(p / 100 * len(data))) - 1))
            return round(data[idx], 2)

        return {
            "p50": pct(50), "p95": pct(95), "p99": pct(99),
            "count": len(data),
        }
