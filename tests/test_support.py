"""Unit tests for the small support pieces: metrics percentiles and config."""

import os

from concord.config import Config
from concord.metrics import Metrics


def test_percentiles_empty_is_zeroed():
    m = Metrics()
    assert m.percentiles() == {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}


def test_percentiles_are_ordered_and_counted():
    m = Metrics()
    for i in range(1, 101):        # 1..100 ms
        m.record_publish(float(i))
    pct = m.percentiles()
    assert pct["count"] == 100
    assert pct["p50"] <= pct["p95"] <= pct["p99"]
    assert pct["p99"] >= 99  # the tail reflects the largest samples


def test_metrics_counters_increment():
    m = Metrics()
    m.record_retry()
    m.record_retry()
    m.record_dead()
    m.record_duplicate()
    assert m.retried == 2
    assert m.dead_lettered == 1
    assert m.duplicates_dropped == 1


def test_config_reads_environment(monkeypatch=None):
    os.environ["CONCORD_BATCH_SIZE"] = "17"
    os.environ["CONCORD_MAX_ATTEMPTS"] = "3"
    try:
        cfg = Config.from_env()
        assert cfg.batch_size == 17
        assert cfg.max_attempts == 3
    finally:
        del os.environ["CONCORD_BATCH_SIZE"]
        del os.environ["CONCORD_MAX_ATTEMPTS"]


def test_config_defaults_are_sane():
    cfg = Config()
    assert cfg.batch_size > 0
    assert cfg.max_attempts >= 1
    assert cfg.backoff_cap_s >= cfg.backoff_base_s
