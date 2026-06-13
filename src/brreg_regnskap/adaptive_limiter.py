"""Adaptive rate limiter for the BRREG kopi PDF service.

The sustained ceiling of the PDF copy endpoint is not a fixed number: it
depends on the egress IP and recent load history (a Cloud Run NAT IP that
has been pushed hard gets a lower allowance than a cold one).  A static
``requests_per_second`` therefore either wastes throughput (too low) or
generates a 429 storm that backoff must absorb (too high).

This wraps :class:`aiolimiter.AsyncLimiter` with additive-increase /
multiplicative-decrease control:

* every observed 429 multiplies the current rate by ``decrease_factor``
  (down to ``min_rate``);
* every ``recover_after`` consecutive clean acquisitions adds
  ``increase_step`` back (up to the configured ceiling).

The conceptual core is unchanged — requests are still politeness-paced;
only the pace now tracks what the endpoint actually tolerates.
"""

from __future__ import annotations

import asyncio

from aiolimiter import AsyncLimiter


class AdaptiveLimiter:
    def __init__(
        self,
        max_rate: float,
        *,
        start_rate: float | None = None,
        min_rate: float = 0.5,
        decrease_factor: float = 0.7,
        increase_step: float = 0.25,
        recover_after: int = 40,
    ) -> None:
        self._max_rate = max_rate
        self._min_rate = min_rate
        self._decrease_factor = decrease_factor
        self._increase_step = increase_step
        self._recover_after = recover_after
        self._rate = max(min_rate, start_rate if start_rate is not None else max_rate)
        self._clean_streak = 0
        self._lock = asyncio.Lock()
        self._limiter = AsyncLimiter(self._rate, 1)

    @property
    def rate(self) -> float:
        return self._rate

    async def acquire(self) -> None:
        await self._limiter.acquire()

    async def on_throttle(self) -> None:
        """Record a 429; cut the rate multiplicatively."""
        async with self._lock:
            self._clean_streak = 0
            new_rate = max(self._min_rate, self._rate * self._decrease_factor)
            if new_rate != self._rate:
                self._rate = new_rate
                self._limiter = AsyncLimiter(self._rate, 1)

    async def on_success(self) -> None:
        """Record a clean response; recover additively after a streak."""
        async with self._lock:
            self._clean_streak += 1
            if self._clean_streak >= self._recover_after and self._rate < self._max_rate:
                self._clean_streak = 0
                self._rate = min(self._max_rate, self._rate + self._increase_step)
                self._limiter = AsyncLimiter(self._rate, 1)
