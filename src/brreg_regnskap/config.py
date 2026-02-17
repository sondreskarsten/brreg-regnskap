"""Configuration via environment variables and .env files.

Settings are loaded with this priority: CLI args > env vars > .env file > defaults.
The storage_path prefix determines the backend: s3://, gs://, or local filesystem.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageBackendType(StrEnum):
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"


class SyncMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"


class Settings(BaseSettings):
    """All configuration for brreg-regnskap.

    Required:
        storage_path: Root path for all stored data.
            - Local: ./data or /abs/path
            - S3: s3://bucket-name/prefix
            - GCS: gs://bucket-name/prefix

    Optional:
        max_concurrent: Max simultaneous HTTP connections (default 50).
        requests_per_second: Rate limit for BRREG API calls (default 10.0).
        max_retries: Max retry attempts per failed request (default 5).
        checkpoint_interval: Save checkpoint every N entities processed (default 1000).
        max_runtime_minutes: Graceful shutdown after N minutes. 0 = unlimited (default 0).
        log_level: Logging verbosity (default INFO).
    """

    model_config = SettingsConfigDict(
        env_prefix="BRREG_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    storage_path: str = "./data"
    max_concurrent: int = 5
    requests_per_second: float = 3.0
    max_retries: int = 5
    checkpoint_interval: int = 1000
    max_runtime_minutes: int = 0
    log_level: str = "INFO"
    orgnr_range_start: str | None = None
    orgnr_range_end: str | None = None

    @field_validator("storage_path")
    @classmethod
    def validate_storage_path(cls, v: str) -> str:
        """Ensure storage_path is a valid prefix."""
        v = v.rstrip("/")
        if not v:
            raise ValueError("storage_path cannot be empty")
        return v

    @property
    def backend_type(self) -> StorageBackendType:
        """Infer storage backend from path prefix."""
        if self.storage_path.startswith("s3://"):
            return StorageBackendType.S3
        if self.storage_path.startswith("gs://"):
            return StorageBackendType.GCS
        return StorageBackendType.LOCAL

    @property
    def manifest_path(self) -> str:
        return f"{self.storage_path}/manifest.parquet"

    @property
    def checkpoint_path(self) -> str:
        return f"{self.storage_path}/checkpoint.json"

    def shard_manifest_path(self, range_start: str, range_end: str) -> str:
        return f"{self.storage_path}/manifest-{range_start}-{range_end}.parquet"

    def regnskap_json_path(self, orgnr: str, year: int) -> str:
        return f"{self.storage_path}/regnskap/{orgnr}/regnskap_{year}.json"

    def regnskap_pdf_path(self, orgnr: str, year: int) -> str:
        return f"{self.storage_path}/regnskap/{orgnr}/aarsregnskap_{year}.pdf"

    def correction_json_path(self, orgnr: str, year: int, journalnr: str, ts: str) -> str:
        return f"{self.storage_path}/corrections/{orgnr}/regnskap_{year}_{journalnr}_{ts}.json"

    def correction_pdf_path(self, orgnr: str, year: int, ts: str) -> str:
        return f"{self.storage_path}/corrections/{orgnr}/aarsregnskap_{year}_{ts}.pdf"

    def entity_dump_path(self, date: str) -> str:
        return f"{self.storage_path}/entities/enheter_dump_{date}.json.gz"

    @property
    def backfill_db_path(self) -> str:
        return f"{self.storage_path}/metadata/backfill_years.json"
