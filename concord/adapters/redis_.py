"""
Redis dedupe adapter. `SET key 1 NX EX <ttl>` is an atomic "claim this key if it
does not exist." If it returns truthy, we are the first to see this dedupe key
and should process the record; if falsy, someone already did and this is a
duplicate to drop.

The TTL matters: dedupe keys are not kept forever, they are kept long enough to
cover the retry window. A key that outlives any possible in-flight retry is just
memory you are renting for no reason. Default 24h comfortably exceeds the maximum
backoff schedule.
"""

from __future__ import annotations

from ..ports import DedupeCache


class RedisDedupe(DedupeCache):
    def __init__(self, url: str, ttl_seconds: int = 86_400) -> None:
        redis = _import_redis()
        self._r = redis.from_url(url)
        self._ttl = ttl_seconds

    def seen_before(self, dedupe_key: str) -> bool:
        return bool(self._r.exists(f"cc:dedupe:{dedupe_key}"))

    def mark_seen(self, dedupe_key: str) -> None:
        # Recorded only after a confirmed publish. TTL covers the retry window;
        # keeping keys longer than any possible in-flight retry is rented memory.
        self._r.set(name=f"cc:dedupe:{dedupe_key}", value=1, ex=self._ttl)


def _import_redis():
    try:
        import redis
        return redis
    except ImportError as e:
        raise RuntimeError(
            "Redis adapter needs redis. Install: pip install 'concord[prod]'"
        ) from e
