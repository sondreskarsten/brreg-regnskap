"""CLI smoke tests using typer.testing.CliRunner."""

from __future__ import annotations

import re
from pathlib import Path

from typer.testing import CliRunner

from brreg_regnskap.api.models import ManifestRecord
from brreg_regnskap.cli import app
from brreg_regnskap.config import Settings
from brreg_regnskap.manifest import ManifestManager
from brreg_regnskap.storage import StorageBackend


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)

runner = CliRunner()


class TestStatusCommand:
    def test_status_empty(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["status", str(tmp_path / "store")])
        assert result.exit_code == 0
        assert "Total records: 0" in result.stdout

    def test_status_with_records(self, tmp_path: Path) -> None:
        store = str(tmp_path / "store")
        settings = Settings(storage_path=store)
        storage = StorageBackend.from_settings(settings)
        storage.check_credentials()
        manifest = ManifestManager(storage, settings.manifest_path)
        manifest.upsert(
            [
                ManifestRecord(
                    orgnr="964118191",
                    year=2024,
                    download_timestamp="2025-01-01T00:00:00Z",
                    status="success",
                )
            ]
        )

        result = runner.invoke(app, ["status", store])
        assert result.exit_code == 0
        assert "Total records: 1" in result.stdout
        assert "success" in result.stdout


class TestVerifyCommand:
    def test_verify_empty(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["verify", str(tmp_path / "store")])
        assert result.exit_code == 0
        assert "empty" in result.stdout.lower()

    def test_verify_all_present(self, tmp_path: Path) -> None:
        store = str(tmp_path / "store")
        settings = Settings(storage_path=store)
        storage = StorageBackend.from_settings(settings)
        storage.check_credentials()

        json_path = settings.regnskap_json_path("964118191", 2024)
        storage.write_bytes(json_path, b'{"data": true}')

        manifest = ManifestManager(storage, settings.manifest_path)
        manifest.upsert(
            [
                ManifestRecord(
                    orgnr="964118191",
                    year=2024,
                    download_timestamp="2025-01-01T00:00:00Z",
                    json_path=json_path,
                    status="success",
                )
            ]
        )

        result = runner.invoke(app, ["verify", store])
        assert result.exit_code == 0
        assert "Missing JSON files: 0" in result.stdout

    def test_verify_missing_file(self, tmp_path: Path) -> None:
        store = str(tmp_path / "store")
        settings = Settings(storage_path=store)
        storage = StorageBackend.from_settings(settings)
        storage.check_credentials()

        manifest = ManifestManager(storage, settings.manifest_path)
        manifest.upsert(
            [
                ManifestRecord(
                    orgnr="964118191",
                    year=2024,
                    download_timestamp="2025-01-01T00:00:00Z",
                    json_path=settings.regnskap_json_path("964118191", 2024),
                    status="success",
                )
            ]
        )

        result = runner.invoke(app, ["verify", store])
        assert result.exit_code == 0
        assert "Missing JSON files: 1" in result.stdout


class TestMergeManifestsCommand:
    def test_merge_no_shards(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["merge-manifests", str(tmp_path / "store")])
        assert result.exit_code == 0
        assert "No shard" in result.stdout

    def test_merge_with_shards(self, tmp_path: Path) -> None:
        store = str(tmp_path / "store")
        settings = Settings(storage_path=store)
        storage = StorageBackend.from_settings(settings)
        storage.check_credentials()

        shard_path = settings.shard_manifest_path("800000000", "850000000")
        m = ManifestManager(storage, shard_path)
        m.upsert(
            [
                ManifestRecord(
                    orgnr="811111111",
                    year=2024,
                    download_timestamp="2025-01-01T00:00:00Z",
                    status="success",
                )
            ]
        )

        result = runner.invoke(app, ["merge-manifests", store])
        assert result.exit_code == 0
        assert "1 shard" in result.stdout
        assert "1 records" in result.stdout


class TestSyncCommandHelp:
    def test_sync_help(self) -> None:
        result = runner.invoke(app, ["sync", "--help"])
        assert result.exit_code == 0
        out = _strip_ansi(result.stdout)
        assert "--max-runtime" in out
        assert "--shard" in out


class TestSetupCommandHelp:
    def test_setup_help(self) -> None:
        result = runner.invoke(app, ["setup", "--help"])
        assert result.exit_code == 0
        assert "bucket" in result.stdout.lower() or "dump" in result.stdout.lower()


class TestPatchCommandHelp:
    def test_patch_help(self) -> None:
        result = runner.invoke(app, ["patch", "--help"])
        assert result.exit_code == 0
        assert "BRREG" in result.stdout or "update" in result.stdout.lower()
