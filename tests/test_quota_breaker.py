"""Quota circuit breaker: end the run on PDF throttle saturation."""

from __future__ import annotations

import time

import pytest

from brreg_regnskap import sync_engine
from brreg_regnskap.config import Settings
from brreg_regnskap.sync_engine import SyncEngine


def _engine(tmp_path) -> SyncEngine:
    settings = Settings(storage_path=str(tmp_path), max_runtime_minutes=0)
    return SyncEngine(settings)


def test_not_saturated_when_few_throttles(tmp_path) -> None:
    eng = _engine(tmp_path)
    now = time.monotonic()
    for _ in range(sync_engine._THROTTLE_SATURATION - 1):
        eng._pdf_throttle_times.append(now)
    assert eng._pdf_quota_saturated() is False
    assert eng._should_shutdown() is False


def test_saturated_trips_shutdown(tmp_path) -> None:
    eng = _engine(tmp_path)
    now = time.monotonic()
    for _ in range(sync_engine._THROTTLE_SATURATION):
        eng._pdf_throttle_times.append(now)
    assert eng._pdf_quota_saturated() is True
    assert eng._should_shutdown() is True
    # latches
    assert eng._shutdown is True
    assert eng._should_shutdown() is True


def test_old_throttles_outside_window_do_not_count(tmp_path) -> None:
    eng = _engine(tmp_path)
    old = time.monotonic() - sync_engine._THROTTLE_WINDOW - 5
    for _ in range(sync_engine._THROTTLE_SATURATION + 10):
        eng._pdf_throttle_times.append(old)
    assert eng._pdf_quota_saturated() is False
