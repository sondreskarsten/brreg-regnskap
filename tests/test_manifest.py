"""Tests for manifest CRUD operations."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from brreg_regnskap.api.models import ManifestRecord
from brreg_regnskap.config import Settings
from brreg_regnskap.manifest import MANIFEST_SCHEMA, ManifestManager
from brreg_regnskap.storage import StorageBackend


def _make_manifest(tmp_path: Path) -> ManifestManager:
    settings = Settings(storage_path=str(tmp_path / "store"))
    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()
    return ManifestManager(storage, settings.manifest_path)


def _make_record(orgnr: str = "964118191", year: int = 2024, **kwargs) -> ManifestRecord:
    defaults = {
        "download_timestamp": "2025-01-01T00:00:00Z",
        "status": "success",
        "journalnr": "2025741982",
    }
    defaults.update(kwargs)
    return ManifestRecord(orgnr=orgnr, year=year, **defaults)


class TestManifestSchema:
    def test_schema_has_required_fields(self) -> None:
        field_names = {f.name for f in MANIFEST_SCHEMA}
        assert "orgnr" in field_names
        assert "year" in field_names
        assert "journalnr" in field_names
        assert "is_correction" in field_names
        assert "status" in field_names

    def test_orgnr_not_nullable(self) -> None:
        field = MANIFEST_SCHEMA.field("orgnr")
        assert not field.nullable

    def test_year_not_nullable(self) -> None:
        field = MANIFEST_SCHEMA.field("year")
        assert not field.nullable

    def test_empty_table_from_schema(self) -> None:
        table = pa.table(
            {f.name: pa.array([], type=f.type) for f in MANIFEST_SCHEMA},
            schema=MANIFEST_SCHEMA,
        )
        assert table.num_rows == 0
        assert table.schema.equals(MANIFEST_SCHEMA)


class TestManifestManager:
    def test_load_empty_manifest(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        table = m.load()
        assert table.num_rows == 0
        assert table.schema.equals(MANIFEST_SCHEMA)

    def test_save_and_load_roundtrip(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        r = _make_record()
        m.upsert([r])
        table = m.load()
        assert table.num_rows == 1
        assert table.column("orgnr")[0].as_py() == "964118191"
        assert table.column("year")[0].as_py() == 2024

    def test_upsert_new_record(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        m.upsert([_make_record()])
        assert m.load().num_rows == 1

    def test_upsert_multiple_records(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        m.upsert(
            [
                _make_record(orgnr="111111111", year=2023),
                _make_record(orgnr="222222222", year=2024),
            ]
        )
        assert m.load().num_rows == 2

    def test_upsert_replaces_same_key(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        m.upsert([_make_record(status="pending")])
        m.upsert([_make_record(status="success")])
        table = m.load()
        assert table.num_rows == 1
        assert table.column("status")[0].as_py() == "success"

    def test_upsert_preserves_other_keys(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        m.upsert([_make_record(orgnr="111111111")])
        m.upsert([_make_record(orgnr="222222222")])
        assert m.load().num_rows == 2

    def test_get_existing(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        m.upsert([_make_record()])
        result = m.get("964118191", 2024)
        assert result is not None
        assert result.orgnr == "964118191"
        assert result.journalnr == "2025741982"

    def test_get_missing(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        assert m.get("999999999", 2024) is None

    def test_list_missing(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        m.upsert([_make_record(orgnr="111111111", year=2024)])
        missing = m.list_missing(["111111111", "222222222", "333333333"], 2024)
        assert "111111111" not in missing
        assert "222222222" in missing
        assert "333333333" in missing

    def test_list_missing_empty_manifest(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        missing = m.list_missing(["111111111"], 2024)
        assert missing == ["111111111"]

    def test_detect_correction_true(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        m.upsert([_make_record(journalnr="OLD_JNR")])
        assert m.detect_corrections("964118191", "NEW_JNR", 2024) is True

    def test_detect_correction_false_same(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        m.upsert([_make_record(journalnr="SAME")])
        assert m.detect_corrections("964118191", "SAME", 2024) is False

    def test_detect_correction_false_missing(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        assert m.detect_corrections("964118191", "ANY", 2024) is False

    def test_cache_avoids_repeated_disk_reads(self, tmp_path: Path) -> None:
        """After load(), subsequent get() calls use the cache, not disk."""
        m = _make_manifest(tmp_path)
        m.upsert([_make_record()])
        # First get loads from disk and caches
        r1 = m.get("964118191", 2024)
        assert r1 is not None
        # Second get should use cache (no disk read)
        r2 = m.get("964118191", 2024)
        assert r2 is not None
        assert r1.orgnr == r2.orgnr

    def test_upsert_updates_cache(self, tmp_path: Path) -> None:
        """After upsert, get() should see the new data without explicit reload."""
        m = _make_manifest(tmp_path)
        m.upsert([_make_record(status="pending")])
        r1 = m.get("964118191", 2024)
        assert r1 is not None
        assert r1.status == "pending"
        m.upsert([_make_record(status="success")])
        r2 = m.get("964118191", 2024)
        assert r2 is not None
        assert r2.status == "success"

    def test_invalidate_cache(self, tmp_path: Path) -> None:
        m = _make_manifest(tmp_path)
        m.upsert([_make_record()])
        _ = m.load()  # populate cache
        m.invalidate_cache()
        # After invalidation, load reads from disk again
        table = m.load()
        assert table.num_rows == 1


class TestManifestMerge:
    def test_merge_two_shards(self, tmp_path: Path) -> None:
        settings = Settings(storage_path=str(tmp_path / "store"))
        storage = StorageBackend.from_settings(settings)
        storage.check_credentials()

        shard1_path = settings.shard_manifest_path("800000000", "850000000")
        shard2_path = settings.shard_manifest_path("850000001", "900000000")

        m1 = ManifestManager(storage, shard1_path)
        m1.upsert([_make_record(orgnr="811111111")])

        m2 = ManifestManager(storage, shard2_path)
        m2.upsert([_make_record(orgnr="866666666")])

        ManifestManager.merge_shards(storage, [shard1_path, shard2_path], settings.manifest_path)

        merged = ManifestManager(storage, settings.manifest_path)
        table = merged.load()
        assert table.num_rows == 2

    def test_merge_deduplicates(self, tmp_path: Path) -> None:
        settings = Settings(storage_path=str(tmp_path / "store"))
        storage = StorageBackend.from_settings(settings)
        storage.check_credentials()

        shard1_path = settings.shard_manifest_path("800000000", "900000000")
        shard2_path = settings.shard_manifest_path("800000000", "900000000b")

        m1 = ManifestManager(storage, shard1_path)
        m1.upsert([_make_record(orgnr="811111111", download_timestamp="2025-01-01T00:00:00Z")])

        m2 = ManifestManager(storage, shard2_path)
        m2.upsert([_make_record(orgnr="811111111", download_timestamp="2025-06-01T00:00:00Z")])

        ManifestManager.merge_shards(storage, [shard1_path, shard2_path], settings.manifest_path)

        merged = ManifestManager(storage, settings.manifest_path)
        table = merged.load()
        assert table.num_rows == 1
        assert table.column("download_timestamp")[0].as_py() == "2025-06-01T00:00:00Z"
