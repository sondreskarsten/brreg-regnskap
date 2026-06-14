"""Lean collector: cursor walking, holding dumps, no manifest."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from brreg_regnskap import collector as collector_mod
from brreg_regnskap.collector import Collector
from brreg_regnskap.config import Settings
from brreg_regnskap.storage import StorageBackend


def _write_work_list(storage: StorageBackend, settings: Settings, pairs: list[tuple[str, int]]) -> None:
    table = pa.table(
        {
            "orgnr": pa.array([p[0] for p in pairs], type=pa.string()),
            "year": pa.array([p[1] for p in pairs], type=pa.int32()),
        }
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    storage.write_bytes(settings.work_list_path, sink.getvalue().to_pybytes())


class _FakeClient:
    def __init__(self, session=None) -> None:
        pass

    async def download_pdf(self, orgnr: str, year: int):
        return f"PDF:{orgnr}:{year}".encode()

    async def fetch_regnskap_raw(self, orgnr: str):
        return f"JSON:{orgnr}".encode()


@pytest.mark.asyncio
async def test_collector_walks_and_dumps_to_holding(tmp_path, monkeypatch) -> None:
    settings = Settings(storage_path=str(tmp_path), max_runtime_minutes=0)
    storage = StorageBackend.from_settings(settings)
    _write_work_list(storage, settings, [("910000001", 2024), ("910000002", 2024)])
    monkeypatch.setattr(collector_mod, "RegnskapsregisteretClient", _FakeClient)

    stats = await Collector(settings).run()

    assert stats["pdfs"] == 2
    cursor = json.loads(storage.read_bytes(settings.collect_cursor_path))
    assert cursor["position"] == 2
    assert (
        storage.read_bytes(f"{settings.holding_prefix}/pdf/910000001_2024.pdf")
        == b"PDF:910000001:2024"
    )


@pytest.mark.asyncio
async def test_collector_resumes_from_cursor(tmp_path, monkeypatch) -> None:
    settings = Settings(storage_path=str(tmp_path), max_runtime_minutes=0)
    storage = StorageBackend.from_settings(settings)
    _write_work_list(storage, settings, [("a", 2024), ("b", 2024), ("c", 2024)])
    storage.write_bytes(settings.collect_cursor_path, json.dumps({"position": 2}).encode())
    monkeypatch.setattr(collector_mod, "RegnskapsregisteretClient", _FakeClient)

    stats = await Collector(settings).run()

    assert stats["pdfs"] == 1  # only the last entry
    cursor = json.loads(storage.read_bytes(settings.collect_cursor_path))
    assert cursor["position"] == 3


@pytest.mark.asyncio
async def test_collector_no_work_list_is_noop(tmp_path) -> None:
    settings = Settings(storage_path=str(tmp_path), max_runtime_minutes=0)
    stats = await Collector(settings).run()
    assert stats == {"pdfs": 0, "missing": 0, "failed": 0}


@pytest.mark.asyncio
async def test_collector_missing_pdf_counts(tmp_path, monkeypatch) -> None:
    class _NoPdf(_FakeClient):
        async def download_pdf(self, orgnr, year):
            return None

    settings = Settings(storage_path=str(tmp_path), max_runtime_minutes=0)
    storage = StorageBackend.from_settings(settings)
    _write_work_list(storage, settings, [("x", 2024)])
    monkeypatch.setattr(collector_mod, "RegnskapsregisteretClient", _NoPdf)

    stats = await Collector(settings).run()
    assert stats["missing"] == 1
    assert stats["pdfs"] == 0
