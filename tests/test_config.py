"""Tests for configuration loading and validation."""

from __future__ import annotations

import pytest

from brreg_regnskap.config import Settings, StorageBackendType


class TestSettings:
    def test_default_settings(self) -> None:
        s = Settings()
        assert s.storage_path == "./data"
        assert s.backend_type == StorageBackendType.LOCAL
        assert s.max_concurrent == 5
        assert s.requests_per_second == 4.0

    def test_s3_backend_detection(self) -> None:
        s = Settings(storage_path="s3://my-bucket/prefix")
        assert s.backend_type == StorageBackendType.S3

    def test_gcs_backend_detection(self) -> None:
        s = Settings(storage_path="gs://my-bucket/prefix")
        assert s.backend_type == StorageBackendType.GCS

    def test_local_backend_detection(self) -> None:
        s = Settings(storage_path="/tmp/brreg")
        assert s.backend_type == StorageBackendType.LOCAL

    def test_trailing_slash_stripped(self) -> None:
        s = Settings(storage_path="s3://bucket/prefix/")
        assert s.storage_path == "s3://bucket/prefix"

    def test_empty_storage_path_raises(self) -> None:
        with pytest.raises(ValueError):
            Settings(storage_path="")

    def test_manifest_path(self) -> None:
        s = Settings(storage_path="s3://bucket/prefix")
        assert s.manifest_path == "s3://bucket/prefix/manifest.parquet"

    def test_checkpoint_path(self) -> None:
        s = Settings(storage_path="./data")
        assert s.checkpoint_path == "./data/checkpoint.json"

    def test_regnskap_json_path(self) -> None:
        s = Settings(storage_path="s3://b/p")
        assert (
            s.regnskap_json_path("964118191", 2024)
            == "s3://b/p/regnskap/964118191/regnskap_2024.json"
        )

    def test_regnskap_pdf_path(self) -> None:
        s = Settings(storage_path="gs://b/p")
        assert (
            s.regnskap_pdf_path("964118191", 2024)
            == "gs://b/p/regnskap/964118191/aarsregnskap_2024.pdf"
        )

    def test_shard_manifest_path(self) -> None:
        s = Settings(storage_path="s3://b/p")
        assert (
            s.shard_manifest_path("800000000", "850000000")
            == "s3://b/p/manifest-800000000-850000000.parquet"
        )

    def test_orderflow_shard_path(self) -> None:
        s = Settings(storage_path="gs://b/p")
        assert s.orderflow_shard_path(3) == "gs://b/p/orderflow/shard_3.parquet"

    def test_etag_path(self) -> None:
        s = Settings(storage_path="s3://b/p")
        assert s.etag_path == "s3://b/p/metadata/etag.json"

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRREG_STORAGE_PATH", "s3://from-env/data")
        monkeypatch.setenv("BRREG_MAX_CONCURRENT", "100")
        s = Settings()
        assert s.storage_path == "s3://from-env/data"
        assert s.max_concurrent == 100
