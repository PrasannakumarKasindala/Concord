"""
Exponential backoff with full jitter. When the broker is having a bad day, the
worst thing a fleet of relay workers can do is retry in lockstep and turn a
brief blip into a synchronized thundering herd. Full jitter (AWS's
recommendation) spreads retries across the whole window, which flattens the
retry spike at the cost of a little unpredictability per record. That trade is
correct here: we care about the broker recovering, not about any single record's
exact retry time.
"""

from __future__ import annotations

import random


def next_delay(attempts: int, base: float = 0.5, cap: float = 30.0) -> float:
    """
    Delay in seconds before the next attempt, given how many have already failed.

    Full jitter: delay = random(0, min(cap, base * 2**attempts)).
    `attempts` is 1-based here (we call it after incrementing).
    """
    ceiling = min(cap, base * (2 ** attempts))
    return random.uniform(0, ceiling)
