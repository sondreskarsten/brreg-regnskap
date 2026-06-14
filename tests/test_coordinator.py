"""Coordinator: holding drain (PDF + verified raw JSON) + work-list build."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq

from brreg_regnskap.config import Settings
from brreg_regnskap.coordinator import Coordinator, _json_year_and_journalnr
from brreg_regnskap.storage import StorageBackend


def _json_for(year: int, journalnr: str = "JNR1") -> bytes:
    return json.dumps(
        [{"journalnr": journalnr, "regnskapsperiode": {"fraDato": f"{year}-01-01", "tilDato": f"{year}-12-31"}}]
    ).encode()


def test_json_year_and_journalnr_extracts_from_tildato() -> None:
    year, jnr = _json_year_and_journalnr(_json_for(2024, "ABC"))
    assert year == 2024
    assert jnr == "ABC"
    assert _json_year_and_journalnr(b"not json") == (None, None)


def test_drain_pairs_json_when_year_matches(tmp_path) -> None:
    settings = Settings(storage_path=str(tmp_path))
    storage = StorageBackend.from_settings(settings)
    storage.write_bytes(f"{settings.holding_prefix}/pdf/910000001_2024.pdf", b"PDFBYTES")
    storage.write_bytes(f"{settings.holding_prefix}/json/910000001.json", _json_for(2024, "J9"))

    drained = Coordinator(settings).drain_holding()

    assert drained == 1
    assert storage.exists(settings.regnskap_pdf_path("910000001", 2024))
    assert storage.exists(settings.regnskap_json_path("910000001", 2024))
    table = pq.read_table(pa.BufferReader(storage.read_bytes(settings.manifest_path)))
    assert table.num_rows == 1
    assert table.column("json_path")[0].as_py() is not None
    assert table.column("journalnr")[0].as_py() == "J9"


def test_drain_does_not_pair_json_on_year_mismatch(tmp_path) -> None:
    settings = Settings(storage_path=str(tmp_path))
    storage = StorageBackend.from_settings(settings)
    storage.write_bytes(f"{settings.holding_prefix}/pdf/910000001_2023.pdf", b"PDFBYTES")
    # JSON endpoint only has max year 2024; PDF is an older 2023 -> must NOT pair
    storage.write_bytes(f"{settings.holding_prefix}/json/910000001.json", _json_for(2024))

    drained = Coordinator(settings).drain_holding()

    assert drained == 1
    assert storage.exists(settings.regnskap_pdf_path("910000001", 2023))
    assert not storage.exists(settings.regnskap_json_path("910000001", 2023))
    table = pq.read_table(pa.BufferReader(storage.read_bytes(settings.manifest_path)))
    assert table.column("json_path")[0].as_py() is None


def test_drain_holding_empty_is_noop(tmp_path) -> None:
    settings = Settings(storage_path=str(tmp_path))
    assert Coordinator(settings).drain_holding() == 0
