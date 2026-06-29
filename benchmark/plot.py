"""
benchmark/plot.py -- turn the measured latency.json into the PNG embedded in the
README. Reads real results; draws no conclusions the data does not support.

Run after the harness:  python -m benchmark.plot
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS = Path(__file__).parent / "results" / "latency.json"
OUT = Path(__file__).parent / "results" / "latency.png"


def main() -> None:
    data = json.loads(RESULTS.read_text())
    rps = [d["offered_rps"] for d in data]
    p50 = [d["p50"] for d in data]
    p95 = [d["p95"] for d in data]
    p99 = [d["p99"] for d in data]

    plt.figure(figsize=(9, 5.2), dpi=120)
    plt.plot(rps, p50, marker="o", label="p50", color="#2e7d32", linewidth=2)
    plt.plot(rps, p95, marker="o", label="p95", color="#f9a825", linewidth=2)
    plt.plot(rps, p99, marker="o", label="p99", color="#c62828", linewidth=2)

    plt.yscale("log")
    plt.xlabel("Offered load (records/sec)")
    plt.ylabel("End-to-end latency (ms, log scale)")
    plt.title("Concord relay: end-to-end publish latency under load\n"
              "(6 workers, 0.8ms broker service time, 5% transient failure rate)")
    plt.grid(True, which="both", alpha=0.3)
    plt.legend()

    # Annotate the saturation knee: the last point where achieved ~ offered.
    for d in data:
        if d["achieved_rps"] < d["offered_rps"] * 0.9:
            plt.axvline(d["offered_rps"], color="#888", linestyle="--", alpha=0.6)
            plt.text(d["offered_rps"], p50[0],
                     "  saturation:\n  offered > drain capacity",
                     color="#555", fontsize=9, va="bottom")
            break

    plt.tight_layout()
    plt.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
