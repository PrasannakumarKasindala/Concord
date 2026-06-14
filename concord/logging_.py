"""
Structured JSON logging. One event per line, machine-parseable, no f-string
soup in a `print`. This is the difference between grepping logs at 3am and
running a real query over them in Loki or CloudWatch Insights.

We keep it dependency-free: the stdlib `logging` module with a small JSON
formatter. Adding structlog would be reasonable in a larger service; here it
would be a dependency earning its keep by saving forty lines.
"""

from __future__ import annotations

import json
import logging
import sys


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": round(record.created, 3),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Anything passed via `extra=` that isn't a standard LogRecord attr.
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                base[key] = value
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base, default=str, separators=(",", ":"))


_RESERVED = set(vars(logging.makeLogRecord({})).keys()) | {
    "message", "asctime", "taskName",
}


def configure(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
