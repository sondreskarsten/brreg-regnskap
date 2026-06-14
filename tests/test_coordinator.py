"""Coordinator: holding drain + work-list build."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from brreg_regnskap.config import Settings
from brreg_regnskap.coordinator import Coordinator
from brreg_regnskap.storage import StorageBackend


def test_drain_holding_moves_files_and_writes_manifest(tmp_path, monkeypatch) -> None:
    settings = Settings(storage_path=str(tmp_path))
    storage = StorageBackend.from_settings(settings)

    storage.write_bytes(f"{settings.holding_prefix}/pdf/910000001_2024.pdf", b"PDFBYTES")

    async def fake_batch(self, orgnrs):
        return {"910000001": b'{"a":1}'}

    monkeypatch.setattr(Coordinator, "_fetch_json_batch", fake_batch)

    coord = Coordinator(settings)
    drained = coord.drain_holding()

    assert drained == 1
    assert storage.exists(settings.regnskap_pdf_path("910000001", 2024))
    assert storage.exists(settings.regnskap_json_path("910000001", 2024))
    assert not storage.exists(f"{settings.holding_prefix}/pdf/910000001_2024.pdf")

    table = pq.read_table(pa.BufferReader(storage.read_bytes(settings.manifest_path)))
    assert table.num_rows == 1
    assert table.column("status")[0].as_py() == "success"
    assert table.column("json_path")[0].as_py() is not None


def test_drain_holding_empty_is_noop(tmp_path) -> None:
    settings = Settings(storage_path=str(tmp_path))
    coord = Coordinator(settings)
    assert coord.drain_holding() == 0
