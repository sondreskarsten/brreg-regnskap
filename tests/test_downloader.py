"""Tests for the async download engine using aioresponses mocking."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from aioresponses import aioresponses

from brreg_regnskap.api.models import ManifestRecord
from brreg_regnskap.api.regnskapsregisteret import RegnskapsregisteretClient
from brreg_regnskap.config import Settings
from brreg_regnskap.downloader import SyncEngine

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


@pytest.fixture
def years_json_fixture(fixtures_dir: Path) -> list:
    return json.loads((fixtures_dir / "years_964118191.json").read_text())


def _mock_full_entity(m, orgnr: str, regnskap_payload: list, years: list) -> None:
    """Register all mock URLs for a full entity download."""
    m.get(f"{BASE}/regnskap/{orgnr}", payload=regnskap_payload)
    m.get(f"{BASE}/regnskap/aarsregnskap/kopi/{orgnr}/aar", payload=years)
    for year in years:
        m.get(f"{BASE}/regnskap/aarsregnskap/kopi/{orgnr}/{year}", body=b"%PDF-fake")


class TestProcessEntity:
    @pytest.mark.asyncio
    async def test_process_entity_json_and_pdf(
        self,
        engine: SyncEngine,
        local_settings: Settings,
        regnskap_json_fixture: list,
    ) -> None:
        with aioresponses() as m:
            _mock_full_entity(m, "964118191", regnskap_json_fixture, ["2023", "2024"])

            async with RegnskapsregisteretClient() as client:
                records = await engine._process_entity("964118191", client)

        assert len(records) > 0
        json_records = [r for r in records if r.json_path]
        assert len(json_records) >= 1
        assert json_records[0].orgnr == "964118191"
        assert json_records[0].year == 2024
        assert json_records[0].journalnr == "2025741982"
        assert json_records[0].status == "success"
        assert json_records[0].file_hash is not None
        assert engine._storage.exists(local_settings.regnskap_json_path("964118191", 2024))

        pdf_records = [r for r in records if r.pdf_path]
        assert len(pdf_records) >= 1

    @pytest.mark.asyncio
    async def test_process_entity_no_regnskap(self, engine: SyncEngine) -> None:
        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/964118191", status=404)

            async with RegnskapsregisteretClient() as client:
                records = await engine._process_entity("964118191", client)

        assert len(records) == 0

    @pytest.mark.asyncio
    async def test_process_entity_pdf_missing(
        self,
        engine: SyncEngine,
        regnskap_json_fixture: list,
    ) -> None:
        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/964118191", payload=regnskap_json_fixture)
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/aar", payload=["2024"])
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/2024", status=404)

            async with RegnskapsregisteretClient() as client:
                records = await engine._process_entity("964118191", client)

        assert len(records) == 1
        assert records[0].status == "pdf_missing"
        assert records[0].json_path is None
        assert records[0].pdf_path is None

    @pytest.mark.asyncio
    async def test_process_entity_no_years(
        self,
        engine: SyncEngine,
        regnskap_json_fixture: list,
    ) -> None:
        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/964118191", payload=regnskap_json_fixture)
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/aar", payload=[])

            async with RegnskapsregisteretClient() as client:
                records = await engine._process_entity("964118191", client)

        assert len(records) == 0

    @pytest.mark.asyncio
    async def test_skips_already_downloaded_pdf(
        self,
        engine: SyncEngine,
        local_settings: Settings,
        regnskap_json_fixture: list,
    ) -> None:
        engine._manifest.upsert([
            ManifestRecord(
                orgnr="964118191",
                year=2023,
                download_timestamp="2025-01-01T00:00:00Z",
                journalnr="2025741982",
                pdf_path=local_settings.regnskap_pdf_path("964118191", 2023),
                status="success",
            )
        ])
        engine._storage.write_bytes(
            local_settings.regnskap_pdf_path("964118191", 2023), b"%PDF-old"
        )

        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/964118191", payload=regnskap_json_fixture)
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/aar", payload=["2023", "2024"])
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/2024", body=b"%PDF-new")

            async with RegnskapsregisteretClient() as client:
                records = await engine._process_entity("964118191", client)

        pdf_records = [r for r in records if r.pdf_path]
        downloaded_years = {r.year for r in pdf_records}
        assert 2024 in downloaded_years
        assert 2023 not in downloaded_years


class TestCorrectionDetection:
    @pytest.mark.asyncio
    async def test_correction_archives_old_files(
        self,
        engine: SyncEngine,
        local_settings: Settings,
        regnskap_json_fixture: list,
    ) -> None:
        old_json_path = local_settings.regnskap_json_path("964118191", 2024)
        engine._storage.write_bytes(old_json_path, b'{"old": true}')
        engine._manifest.upsert([
            ManifestRecord(
                orgnr="964118191",
                year=2024,
                download_timestamp="2025-01-01T00:00:00Z",
                json_path=old_json_path,
                journalnr="OLD_JOURNALNR",
                status="success",
            )
        ])

        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/964118191", payload=regnskap_json_fixture)
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/aar", payload=["2024"])
            m.get(f"{BASE}/regnskap/aarsregnskap/kopi/964118191/2024", body=b"%PDF-new")

            async with RegnskapsregisteretClient() as client:
                records = await engine._process_entity("964118191", client)

        json_records = [r for r in records if r.json_path and r.year == 2024]
        assert len(json_records) == 1
        assert json_records[0].is_correction is True
        assert json_records[0].journalnr == "2025741982"

        corrections = engine._storage.list_dir(f"{local_settings.storage_path}/corrections")
        assert len(corrections) > 0


class TestProcessEntitySafe:
    @pytest.mark.asyncio
    async def test_exception_returns_failed_record(self, engine: SyncEngine) -> None:
        with aioresponses() as m:
            m.get(f"{BASE}/regnskap/964118191", exception=ConnectionError("boom"))

            async with RegnskapsregisteretClient() as client:
                records = await engine._process_entity_safe("964118191", client)

        assert len(records) == 1
        assert records[0].status == "failed"
        assert records[0].orgnr == "964118191"
