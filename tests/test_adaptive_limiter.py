"""AdaptiveLimiter AIMD behaviour."""

from __future__ import annotations

import pytest

from brreg_regnskap.adaptive_limiter import AdaptiveLimiter


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
async def test_recovery_is_additive_after_streak() -> None:
    lim = AdaptiveLimiter(
        4.0, decrease_factor=0.5, increase_step=0.5, recover_after=3, min_rate=0.5
    )
    await lim.on_throttle()  # 2.0
    assert lim.rate == 2.0
    for _ in range(2):
        await lim.on_success()
    assert lim.rate == 2.0  # streak not yet reached
    await lim.on_success()  # streak hits 3
    assert lim.rate == 2.5


@pytest.mark.asyncio
async def test_recovery_capped_at_max() -> None:
    lim = AdaptiveLimiter(2.0, increase_step=1.0, recover_after=1, min_rate=0.5)
    await lim.on_throttle()  # 1.4
    for _ in range(10):
        await lim.on_success()
    assert lim.rate == 2.0


@pytest.mark.asyncio
async def test_throttle_resets_clean_streak() -> None:
    lim = AdaptiveLimiter(
        4.0, decrease_factor=0.5, increase_step=0.5, recover_after=3, min_rate=0.5
    )
    await lim.on_throttle()  # 2.0
    await lim.on_success()
    await lim.on_success()
    await lim.on_throttle()  # 1.0, streak reset
    await lim.on_success()
    await lim.on_success()
    assert lim.rate == 1.0  # streak was reset, no recovery yet
