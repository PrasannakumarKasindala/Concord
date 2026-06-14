"""
Configuration, read from the environment. No secrets in code, no secrets in the
repo. `.env.example` documents every knob; `.env` (gitignored) holds real values
locally; in production these come from the orchestrator's secret store.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    # relay behavior
    batch_size: int = 200
    max_attempts: int = 8
    poll_interval_ms: int = 50
    backoff_base_s: float = 0.5
    backoff_cap_s: float = 30.0
    log_level: str = "INFO"

    # infra (only read by production adapters)
    database_url: str = "postgresql://concord:concord@localhost:5432/concord"
    kafka_bootstrap: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379/0"
    minio_endpoint: str = "localhost:9000"
    minio_bucket: str = "concord-deadletter"

    @classmethod
    def from_env(cls) -> "Config":
        g = os.environ.get
        return cls(
            batch_size=int(g("CONCORD_BATCH_SIZE", 200)),
            max_attempts=int(g("CONCORD_MAX_ATTEMPTS", 8)),
            poll_interval_ms=int(g("CONCORD_POLL_INTERVAL_MS", 50)),
            backoff_base_s=float(g("CONCORD_BACKOFF_BASE_S", 0.5)),
            backoff_cap_s=float(g("CONCORD_BACKOFF_CAP_S", 30.0)),
            log_level=g("CONCORD_LOG_LEVEL", "INFO"),
            database_url=g("CONCORD_DATABASE_URL",
                           "postgresql://concord:concord@localhost:5432/concord"),
            kafka_bootstrap=g("CONCORD_KAFKA_BOOTSTRAP", "localhost:9092"),
            redis_url=g("CONCORD_REDIS_URL", "redis://localhost:6379/0"),
            minio_endpoint=g("CONCORD_MINIO_ENDPOINT", "localhost:9000"),
            minio_bucket=g("CONCORD_MINIO_BUCKET", "concord-deadletter"),
        )
