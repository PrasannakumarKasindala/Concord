"""The structured logging claim deserves a test: output must be one JSON object
per line with our fields, not free-form text."""

import json
import logging

from concord.logging_ import JsonFormatter


def test_formatter_emits_valid_json_with_extra_fields():
    fmt = JsonFormatter()
    rec = logging.makeLogRecord({
        "name": "concord.relay", "levelname": "INFO",
        "msg": "published", "record_id": "abc", "latency_ms": 1.5,
    })
    line = fmt.format(rec)
    obj = json.loads(line)          # must parse
    assert obj["msg"] == "published"
    assert obj["record_id"] == "abc"
    assert obj["latency_ms"] == 1.5
    assert obj["level"] == "INFO"
    assert "ts" in obj
