"""AdaptiveLimiter: strict-interval pacing + cooldown-gated AIMD."""

from __future__ import annotations

import pytest

from brreg_regnskap.adaptive_limiter import AdaptiveLimiter


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.mark.asyncio
async def test_throttle_decreases_rate_multiplicatively() -> None:
    lim = AdaptiveLimiter(4.0, decrease_factor=0.5, min_rate=0.5)
    assert lim.rate == 4.0
    await lim.on_throttle()
    assert lim.rate == 2.0
    await lim.on_throttle()
    assert lim.rate == 1.0


@pytest.mark.asyncio
async def test_rate_floored_at_min() -> None:
    lim = AdaptiveLimiter(4.0, decrease_factor=0.5, min_rate=1.0)
    for _ in range(10):
        await lim.on_throttle()
    assert lim.rate == 1.0


@pytest.mark.asyncio
async def test_recovery_requires_streak_and_cooldown() -> None:
    clk = _Clock()
    lim = AdaptiveLimiter(
        4.0,
        decrease_factor=0.5,
        increase_step=0.5,
        recover_after=3,
        recover_cooldown=45.0,
        clock=clk,
    )
    await lim.on_throttle()  # rate 2.0, last_throttle=0
    assert lim.rate == 2.0

    # streak reached but cooldown not elapsed -> no recovery
    for _ in range(3):
        await lim.on_success()
    assert lim.rate == 2.0

    # advance past cooldown; next success crossing the streak recovers
    clk.advance(50)
    for _ in range(3):
        await lim.on_success()
    assert lim.rate == 2.5


@pytest.mark.asyncio
async def test_throttle_resets_streak_and_restarts_cooldown() -> None:
    clk = _Clock()
    lim = AdaptiveLimiter(
        4.0,
        decrease_factor=0.5,
        increase_step=0.5,
        recover_after=3,
        recover_cooldown=45.0,
        clock=clk,
    )
    await lim.on_throttle()  # 2.0
    clk.advance(50)
    await lim.on_success()
    await lim.on_success()
    await lim.on_throttle()  # 1.0, streak reset, cooldown restarts at t=50
    clk.advance(10)  # only 10s since last throttle
    for _ in range(3):
        await lim.on_success()
    assert lim.rate == 1.0  # cooldown not elapsed


@pytest.mark.asyncio
async def test_recovery_capped_at_max() -> None:
    clk = _Clock()
    lim = AdaptiveLimiter(
        2.0, increase_step=1.0, recover_after=1, recover_cooldown=0.0, min_rate=0.5, clock=clk
    )
    await lim.on_throttle()  # 1.4
    for _ in range(10):
        clk.advance(1)
        await lim.on_success()
    assert lim.rate == 2.0


@pytest.mark.asyncio
async def test_start_rate_below_max() -> None:
    lim = AdaptiveLimiter(2.0, start_rate=1.5)
    assert lim.rate == 1.5


@pytest.mark.asyncio
async def test_acquire_paces_at_interval() -> None:
    clk = _Clock()
    lim = AdaptiveLimiter(2.0, start_rate=2.0, clock=clk)
    # First acquire is immediate (now >= next_ok=0)
    await lim.acquire()
    # next_ok is now 0 + 0.5; a second acquire at the same clock would need to wait,
    # but since the clock doesn't advance on its own here we just assert the schedule.
    assert lim._next_ok == pytest.approx(0.5)
    clk.advance(0.5)
    await lim.acquire()
    assert lim._next_ok == pytest.approx(1.0)
