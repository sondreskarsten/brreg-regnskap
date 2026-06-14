"""Coordinator: holding drain + work-list build."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from brreg_regnskap.config import Settings
from brreg_regnskap.coordinator import Coordinator
from brreg_regnskap.storage import StorageBackend


def test_drain_holding_moves_files_and_writes_manifest(tmp_path) -> None:
    settings = Settings(storage_path=str(tmp_path))
    storage = StorageBackend.from_settings(settings)

    storage.write_bytes(f"{settings.holding_prefix}/pdf/910000001_2024.pdf", b"PDFBYTES")
    storage.write_bytes(f"{settings.holding_prefix}/json/910000001.json", b'{"a":1}')
    storage.write_bytes(
        f"{settings.holding_prefix}/meta/910000001_2024.json",
        json.dumps({"orgnr": "910000001", "year": 2024, "pdf_hash": "abc", "pdf_size": 8}).encode(),
    )

    coord = Coordinator(settings)
    drained = coord.drain_holding()

    assert drained == 1
    assert storage.exists(settings.regnskap_pdf_path("910000001", 2024))
    assert storage.exists(settings.regnskap_json_path("910000001", 2024))
    assert not storage.exists(f"{settings.holding_prefix}/meta/910000001_2024.json")

    table = pq.read_table(pa.BufferReader(storage.read_bytes(settings.manifest_path)))
    assert table.num_rows == 1
    assert table.column("status")[0].as_py() == "success"
    assert table.column("pdf_hash")[0].as_py() == "abc"


def test_drain_holding_empty_is_noop(tmp_path) -> None:
    settings = Settings(storage_path=str(tmp_path))
    coord = Coordinator(settings)
    assert coord.drain_holding() == 0
