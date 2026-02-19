"""Tests for the orderflow-driven sync engine."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from aioresponses import aioresponses

from brreg_regnskap.api.regnskapsregisteret import RegnskapsregisteretClient
from brreg_regnskap.config import Settings
from brreg_regnskap.orderflow import OrderflowManager
from brreg_regnskap.sync_engine import SyncEngine

BASE = "https://data.brreg.no/regnskapsregisteret"


@pytest.fixture
def local_settings(tmp_path: Path) -> Settings:
    return Settings(
        storage_path=str(tmp_path / "store"),
        max_concurrent=5,
        requests_per_second=100.0,
        max_retries=1,
        checkpoint_interval=10,
    )


@pytest.fixture
def engine(local_settings: Settings) -> SyncEngine:
    e = SyncEngine(local_settings)
    e._storage.check_credentials()
    return e


@pytest.fixture
def regnskap_json_fixture(fixtures_dir: Path) -> list:
    return json.loads((fixtures_dir / "regnskap_964118191.json").read_text())


class TestDownloadEntity:
    @pytest.mark.asyncio
    async def test_downloads_pdf_and_json(
        self,
        engine: SyncEngine,
        local_settings: Settings,
        regnskap_json_fixture: list,
    ) -> None:
        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/2024", body=b"%PDF-fake")
            m.get(f"{BASE}/regnskap/964118191", payload=regnskap_json_fixture)

            async with RegnskapsregisteretClient() as client:
                records = await engine._download_entity(
                    "964118191", 2024, client, json_too=True,
                )

        assert len(records) == 1
        assert records[0].pdf_path is not None
        assert records[0].json_path is not None
        assert records[0].status == "success"
        assert engine._storage.exists(local_settings.regnskap_pdf_path("964118191", 2024))
        assert engine._storage.exists(local_settings.regnskap_json_path("964118191", 2024))

    @pytest.mark.asyncio
    async def test_pdf_only_no_json(
        self,
        engine: SyncEngine,
        local_settings: Settings,
    ) -> None:
        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/2020", body=b"%PDF-old")

            async with RegnskapsregisteretClient() as client:
                records = await engine._download_entity(
                    "964118191", 2020, client, json_too=False,
                )

        assert len(records) == 1
        assert records[0].pdf_path is not None
        assert records[0].json_path is None
        assert records[0].status == "success"

    @pytest.mark.asyncio
    async def test_pdf_missing(self, engine: SyncEngine) -> None:
        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/2024", status=404)

            async with RegnskapsregisteretClient() as client:
                records = await engine._download_entity(
                    "964118191", 2024, client, json_too=True,
                )

        assert len(records) == 1
        assert records[0].status == "pdf_missing"

    @pytest.mark.asyncio
    async def test_pdf_error(self, engine: SyncEngine) -> None:
        with aioresponses() as m:
            m.get(
                f"{BASE}/regnskap/aarsregnskap/kopi/964118191/2024",
                exception=ConnectionError("boom"),
            )

            async with RegnskapsregisteretClient() as client:
                records = await engine._download_entity(
                    "964118191", 2024, client, json_too=True,
                )

        assert len(records) == 1
        assert records[0].status == "pdf_failed"

    @pytest.mark.asyncio
    async def test_duplicate_pdf_hash_skipped(
        self,
        engine: SyncEngine,
    ) -> None:
        """If we already have the exact same PDF content, skip it."""
        from brreg_regnskap.api.models import ManifestRecord

        pdf_data = b"%PDF-fake"
        pdf_hash = engine._hash(pdf_data)
        engine._manifest.upsert([
            ManifestRecord(
                orgnr="964118191",
                year=2024,
                download_timestamp="2025-01-01T00:00:00Z",
                pdf_hash=pdf_hash,
                pdf_path="some/path.pdf",
                status="success",
            )
        ])

        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/2024", body=pdf_data)

            async with RegnskapsregisteretClient() as client:
                records = await engine._download_entity(
                    "964118191", 2024, client, json_too=True,
                )

        assert len(records) == 0
        assert engine._stats["skipped"] == 1


class TestFullRun:
    @pytest.mark.asyncio
    async def test_processes_fast_lane(
        self,
        engine: SyncEngine,
        local_settings: Settings,
        regnskap_json_fixture: list,
    ) -> None:
        """A full run processes fast-lane entries."""
        from brreg_regnskap.orderflow import OrderflowManager

        of = OrderflowManager(engine._storage, local_settings)
        of.enqueue_fast([("964118191", 2024)], source="bulk_dump")

        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/2024", body=b"%PDF-fake")
            m.get(f"{BASE}/regnskap/964118191", payload=regnskap_json_fixture)

            stats = await engine.run()

        assert stats["success"] == 1
        assert stats["pdfs"] == 1
        assert stats["jsons"] == 1

        table = engine._manifest.load()
        assert table.num_rows == 1
        assert table.column("orgnr")[0].as_py() == "964118191"
        assert table.column("year")[0].as_py() == 2024
