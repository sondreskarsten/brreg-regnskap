"""Configuration via environment variables and .env files.

Settings are loaded with this priority: CLI args > env vars > .env file > defaults.
The storage_path prefix determines the backend: s3://, gs://, or local filesystem.

Storage layout:
    orderflow/shard_{0-9}.parquet    - two-lane work queue (fast + slow)
    metadata/etag.json               - bulk dump ETag for conditional fetches
    metadata/checkpoint.json         - sync cursor state
    regnskap/{orgnr}/...             - downloaded JSON and PDF files
    manifest.parquet                 - source of truth for completed downloads
    entities/enheter_dump_{date}.json.gz - cached bulk dumps
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageBackendType(StrEnum):
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"


class Settings(BaseSettings):
    """All configuration for brreg-regnskap.

    Required:
        storage_path: Root path for all stored data.
            - Local: ./data or /abs/path
            - S3: s3://bucket-name/prefix
            - GCS: gs://bucket-name/prefix

    Optional:
        max_concurrent: Max simultaneous HTTP connections (default 5).
        requests_per_second: Rate limit for BRREG API calls (default 4.0; 5/s burst-clean but sustained Cloud Run load saw frequent 429).
        max_retries: Max retry attempts per failed request (default 5).
        checkpoint_interval: Save checkpoint every N entities processed (default 1000).
        max_runtime_minutes: Graceful shutdown after N minutes. 0 = unlimited (default 0).
        log_level: Logging verbosity (default INFO).
        shard: Shard digit 0-9 for parallel workers (default None = all shards).
    """

    model_config = SettingsConfigDict(
        env_prefix="BRREG_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    storage_path: str = "./data"
    max_concurrent: int = 5
    requests_per_second: float = 4.0
    max_retries: int = 5
    checkpoint_interval: int = 1000
    max_runtime_minutes: int = 0
    log_level: str = "INFO"
    shard: int | None = None

    @field_validator("storage_path")
    @classmethod
    def validate_storage_path(cls, v: str) -> str:
        """Ensure storage_path is a valid prefix."""
        v = v.rstrip("/")
        if not v:
            raise ValueError("storage_path cannot be empty")
        return v

    @field_validator("shard")
    @classmethod
    def validate_shard(cls, v: int | None) -> int | None:
        if v is not None and not (0 <= v <= 9):
            raise ValueError("shard must be 0-9")
        return v

    @property
    def _shard_suffix(self) -> str:
        if self.shard is None:
            return ""
        return f"_shard_{self.shard}"

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
        return f"{self.storage_path}/manifest{self._shard_suffix}.parquet"

    @property
    def checkpoint_path(self) -> str:
        return f"{self.storage_path}/checkpoint{self._shard_suffix}.json"

    def shard_manifest_path(self, range_start: str, range_end: str) -> str:
        return f"{self.storage_path}/manifest-{range_start}-{range_end}.parquet"

    def regnskap_json_path(self, orgnr: str, year: int, version: int = 1) -> str:
        suffix = f"_v{version}" if version > 1 else ""
        return f"{self.storage_path}/regnskap/{orgnr}/regnskap_{year}{suffix}.json"

    def regnskap_pdf_path(self, orgnr: str, year: int, version: int = 1) -> str:
        suffix = f"_v{version}" if version > 1 else ""
        return f"{self.storage_path}/regnskap/{orgnr}/aarsregnskap_{year}{suffix}.pdf"

    def entity_dump_path(self, date: str) -> str:
        return f"{self.storage_path}/entities/enheter_dump_{date}.json.gz"

    # ── Note extraction paths ────────────────────────────────────────

    def notes_json_path(self, orgnr: str, year: int) -> str:
        return f"{self.storage_path}/notes/{orgnr}/notes_{year}.json"

    def regnskap_ocr_path(self, orgnr: str, year: int) -> str:
        return f"{self.storage_path}/notes/{orgnr}/ocr_{year}.txt"

    def regnskap_items_path(self, orgnr: str, year: int) -> str:
        return f"{self.storage_path}/notes/{orgnr}/regnskap_{year}.json"

    @property
    def notes_consolidated_path(self) -> str:
        return f"{self.storage_path}/notes/extractions.parquet"

    @property
    def regnskap_consolidated_path(self) -> str:
        return f"{self.storage_path}/notes/regnskap_items.parquet"

    # ── Orderflow paths ──────────────────────────────────────────────

    def orderflow_shard_path(self, digit: int) -> str:
        """Parquet file for one orderflow shard (digit 0-9)."""
        return f"{self.storage_path}/orderflow/shard_{digit}.parquet"

    @property
    def etag_path(self) -> str:
        """JSON file storing the bulk-dump ETag and last-processed date."""
        return f"{self.storage_path}/metadata/etag.json"

    @property
    def patch_cursor_path(self) -> str:
        """JSON file storing the date of the last successful patch run."""
        return f"{self.storage_path}/metadata/patch_cursor.json"
