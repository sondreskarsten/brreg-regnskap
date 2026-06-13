"""Lock-file behaviour for the daily command.

Two concurrent daily executions share one global manifest.parquet and
checkpoint.json (single-worker topology), so a second run started while
the first is alive corrupts progress via last-write-wins.  Observed
2026-06-13: scheduler fired at 03:00 while a manual run begun 22:24 was
still draining the catch-up backlog.  A lock file gates startup; a stale
lock (older than max_runtime + margin) is overridden so a crashed run
cannot block forever.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from typer.testing import CliRunner

from brreg_regnskap.cli import app
from brreg_regnskap.config import Settings
from brreg_regnskap.storage import StorageBackend

runner = CliRunner()


def _lock_path(tmp_path) -> str:
    settings = Settings(storage_path=str(tmp_path))
    return f"{settings.storage_path}/metadata/daily.lock"


def test_daily_exits_when_fresh_lock_held(tmp_path) -> None:
    settings = Settings(storage_path=str(tmp_path))
    storage = StorageBackend.from_settings(settings)
    storage.write_bytes(
        _lock_path(tmp_path),
        json.dumps(
            {"execution": "other", "acquired_at": datetime.now(UTC).isoformat()}
        ).encode(),
    )

    result = runner.invoke(app, ["daily", str(tmp_path)])

    assert result.exit_code == 0
    assert "daily.lock held" in result.stdout
    assert storage.exists(_lock_path(tmp_path))


def test_daily_overrides_stale_lock(tmp_path, monkeypatch) -> None:
    settings = Settings(storage_path=str(tmp_path))
    storage = StorageBackend.from_settings(settings)
    stale = (datetime.now(UTC) - timedelta(hours=48)).isoformat()
    storage.write_bytes(
        _lock_path(tmp_path),
        json.dumps({"execution": "crashed", "acquired_at": stale}).encode(),
    )

    calls: list[str] = []

    async def _noop_patch(settings, storage):
        calls.append("patch")

    class _FakeEngine:
        def __init__(self, settings):
            pass

        async def run(self):
            calls.append("run")
            return {}

    monkeypatch.setattr("brreg_regnskap.cli._patch_async", _noop_patch)
    monkeypatch.setattr("brreg_regnskap.sync_engine.SyncEngine", _FakeEngine)

    result = runner.invoke(app, ["daily", str(tmp_path)])

    assert result.exit_code == 0
    assert "stale" in result.stdout
    assert calls == ["patch", "run"]
    assert not storage.exists(_lock_path(tmp_path))


def test_daily_releases_lock_on_completion(tmp_path, monkeypatch) -> None:
    async def _noop_patch(settings, storage):
        pass

    class _FakeEngine:
        def __init__(self, settings):
            pass

        async def run(self):
            return {}

    monkeypatch.setattr("brreg_regnskap.cli._patch_async", _noop_patch)
    monkeypatch.setattr("brreg_regnskap.sync_engine.SyncEngine", _FakeEngine)

    result = runner.invoke(app, ["daily", str(tmp_path)])

    assert result.exit_code == 0
    settings = Settings(storage_path=str(tmp_path))
    storage = StorageBackend.from_settings(settings)
    assert not storage.exists(_lock_path(tmp_path))
