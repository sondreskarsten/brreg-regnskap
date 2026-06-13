"""Adaptive rate limiter for the BRREG kopi PDF service.

The sustained ceiling of the PDF copy endpoint is not a fixed number: it
depends on the egress IP and recent load history (a Cloud Run NAT IP that
has been pushed hard gets a lower allowance than a cold one), and it drifts
down over a long run.  A static rate either wastes throughput or generates a
429 storm.

This is a strict-interval pacer (one acquisition per ``1/rate`` seconds,
burst of one — unlike a windowed limiter, which would release a burst of
``rate * window`` requests at once and trip the endpoint) with
additive-increase / multiplicative-decrease control of the rate:

* every observed 429 multiplies the rate by ``decrease_factor`` (down to
  ``min_rate``) and starts a cooldown;
* the rate recovers by ``increase_step`` only after ``recover_after`` clean
  acquisitions *and* ``recover_cooldown`` seconds since the last 429, up to
  ``max_rate``.

Slow, cooldown-gated recovery is what stops the rate from ratcheting back
up into the throttle zone over a multi-hour run.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


class AdaptiveLimiter:
    def __init__(
        self,
        max_rate: float,
        *,
        start_rate: float | None = None,
        min_rate: float = 0.5,
        decrease_factor: float = 0.7,
        increase_step: float = 0.1,
        recover_after: int = 120,
        recover_cooldown: float = 45.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_rate = max_rate
        self._min_rate = min_rate
        self._decrease_factor = decrease_factor
        self._increase_step = increase_step
        self._recover_after = recover_after
        self._recover_cooldown = recover_cooldown
        self._clock = clock
        self._rate = max(min_rate, start_rate if start_rate is not None else max_rate)
        self._next_ok = 0.0
        self._clean_streak = 0
        self._last_throttle = float("-inf")
        self._lock = asyncio.Lock()

    @property
    def rate(self) -> float:
        return self._rate

    @property
    def _interval(self) -> float:
        return 1.0 / self._rate

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = self._clock()
                if now >= self._next_ok:
                    self._next_ok = now + self._interval
                    return
                wait = self._next_ok - now
            await asyncio.sleep(wait)

    async def on_throttle(self) -> None:
        """Record a 429; cut the rate multiplicatively and start cooldown."""
        async with self._lock:
            self._clean_streak = 0
            self._last_throttle = self._clock()
            self._rate = max(self._min_rate, self._rate * self._decrease_factor)

    async def on_success(self) -> None:
        """Record a clean response; recover additively, slowly, gated by cooldown."""
        async with self._lock:
            self._clean_streak += 1
            if self._rate >= self._max_rate:
                return
            if self._clean_streak < self._recover_after:
                return
            if (self._clock() - self._last_throttle) < self._recover_cooldown:
                return
            self._clean_streak = 0
            self._rate = min(self._max_rate, self._rate + self._increase_step)
