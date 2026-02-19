"""Parquet manifest for tracking downloaded regnskap files.

The manifest is the source of truth for what has been downloaded. Each row represents
a (orgnr, year) pair with its download status, file paths, hash, and journal number.

Implementation notes:
    - Schema is defined as MANIFEST_SCHEMA using pyarrow types.
    - The manifest is stored as a single Parquet file with zstd compression.
    - For GitHub Actions matrix jobs, each job writes a shard manifest. The merge
      operation combines all shards into the global manifest.
    - Upsert logic: filter out existing (orgnr, year) pairs, append new records.
    - Correction detection: compare journalnr for existing (orgnr, year) pairs.
      If different, the existing record should be marked and the old file archived.
    - The manifest is small (~500K rows, ~10-20MB compressed) — full read-modify-write
      is acceptable. No need for incremental append or partitioning.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from brreg_regnskap.storage import StorageBackend

from brreg_regnskap.api.models import ManifestRecord

MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("orgnr", pa.string(), nullable=False),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("version", pa.int32(), nullable=False),
        pa.field("download_timestamp", pa.string()),
        pa.field("file_hash", pa.string()),
        pa.field("pdf_hash", pa.string()),
        pa.field("json_path", pa.string()),
        pa.field("pdf_path", pa.string()),
        pa.field("file_size_bytes", pa.int64()),
        pa.field("is_correction", pa.bool_()),
        pa.field("journalnr", pa.string()),
        pa.field("source_url", pa.string()),
        pa.field("status", pa.string()),
        pa.field("error_detail", pa.string()),
    ]
)


def _empty_table() -> pa.Table:
    return pa.table(
        {f.name: pa.array([], type=f.type) for f in MANIFEST_SCHEMA},
        schema=MANIFEST_SCHEMA,
    )


def _record_to_dict(r: ManifestRecord) -> dict:
    return {
        "orgnr": r.orgnr,
        "year": r.year,
        "version": r.version,
        "download_timestamp": r.download_timestamp,
        "file_hash": r.file_hash,
        "pdf_hash": r.pdf_hash,
        "json_path": r.json_path,
        "pdf_path": r.pdf_path,
        "file_size_bytes": r.file_size_bytes,
        "is_correction": r.is_correction,
        "journalnr": r.journalnr,
        "source_url": r.source_url,
        "status": r.status,
        "error_detail": r.error_detail,
    }


def _row_to_record(table: pa.Table, idx: int) -> ManifestRecord:
    row = {col: table.column(col)[idx].as_py() for col in table.column_names}
    return ManifestRecord(**row)


class ManifestManager:
    """Manages the Parquet manifest tracking all downloaded regnskap files.

    Primary key: (orgnr, year, version). Multiple versions per (orgnr, year)
    represent corrections — each version is immutable once written.

    Usage:
        manifest = ManifestManager(storage, settings.manifest_path)
        table = manifest.load()
        manifest.upsert([record1, record2])
        existing = manifest.get("964118191", 2024, 1)
        versions = manifest.get_versions("964118191", 2024)
    """

    def __init__(self, storage: StorageBackend, manifest_path: str) -> None:
        self._storage = storage
        self._manifest_path = manifest_path
        self._cache: pa.Table | None = None

    def load(self) -> pa.Table:
        """Load the manifest from storage. Returns empty table if not found.

        Uses an in-memory cache to avoid repeated Parquet deserialization.
        Returns a pyarrow Table with MANIFEST_SCHEMA.
        """
        if self._cache is not None:
            return self._cache
        if not self._storage.exists(self._manifest_path):
            return _empty_table()
        raw = self._storage.read_bytes(self._manifest_path)
        buf = pa.BufferReader(raw)
        table = pq.read_table(buf)
        if "version" not in table.column_names:
            version_col = pa.array([1] * table.num_rows, type=pa.int32())
            version_field = pa.field("version", pa.int32(), nullable=False)
            table = table.append_column(version_field, version_col)
        if "pdf_hash" not in table.column_names:
            pdf_hash_col = pa.array([None] * table.num_rows, type=pa.string())
            table = table.append_column(pa.field("pdf_hash", pa.string()), pdf_hash_col)
        table = table.select([f.name for f in MANIFEST_SCHEMA])
        self._cache = table.cast(MANIFEST_SCHEMA)
        return self._cache

    def save(self, table: pa.Table) -> None:
        """Write the manifest table to storage atomically.

        Uses zstd compression. Overwrites the existing manifest.
        Updates the in-memory cache.
        """
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="zstd")
        self._storage.write_bytes(self._manifest_path, sink.getvalue())
        self._cache = table

    def invalidate_cache(self) -> None:
        """Clear the in-memory cache, forcing next load() to read from storage."""
        self._cache = None

    def upsert(self, records: list[ManifestRecord]) -> None:
        """Insert or update records in the manifest.

        Key: (orgnr, year, version). Records with matching keys are replaced.
        Records with new keys are inserted.
        """
        if not records:
            return

        existing = self.load()

        new_keys = {(r.orgnr, r.year, r.version) for r in records}

        if existing.num_rows > 0:
            orgnr_col = existing.column("orgnr")
            year_col = existing.column("year")
            version_col = existing.column("version")
            keep_mask = pa.array([
                (
                    orgnr_col[i].as_py(),
                    year_col[i].as_py(),
                    version_col[i].as_py(),
                ) not in new_keys
                for i in range(existing.num_rows)
            ])
            existing = existing.filter(keep_mask)

        new_rows = {f.name: [] for f in MANIFEST_SCHEMA}
        for r in records:
            d = _record_to_dict(r)
            for col in new_rows:
                new_rows[col].append(d[col])

        new_arrays = {}
        for f in MANIFEST_SCHEMA:
            new_arrays[f.name] = pa.array(new_rows[f.name], type=f.type)
        new_table = pa.table(new_arrays, schema=MANIFEST_SCHEMA)

        merged = pa.concat_tables([existing, new_table], promote_options="none")
        self.save(merged)

    def get(self, orgnr: str, year: int, version: int = 1) -> ManifestRecord | None:
        """Look up a single manifest entry by (orgnr, year, version).

        Uses the in-memory cache. Returns None if not found.
        """
        table = self.load()
        if table.num_rows == 0:
            return None

        mask = pc.and_(
            pc.and_(
                pc.equal(table.column("orgnr"), orgnr),
                pc.equal(table.column("year"), year),
            ),
            pc.equal(table.column("version"), version),
        )
        filtered = table.filter(mask)
        if filtered.num_rows == 0:
            return None
        return _row_to_record(filtered, 0)

    def get_versions(self, orgnr: str, year: int) -> list[ManifestRecord]:
        """Return all version records for a given (orgnr, year), sorted by version."""
        table = self.load()
        if table.num_rows == 0:
            return []

        mask = pc.and_(
            pc.equal(table.column("orgnr"), orgnr),
            pc.equal(table.column("year"), year),
        )
        filtered = table.filter(mask)
        if filtered.num_rows == 0:
            return []

        records = [_row_to_record(filtered, i) for i in range(filtered.num_rows)]
        records.sort(key=lambda r: r.version)
        return records

    def max_version(self, orgnr: str, year: int) -> int:
        """Return the highest version number for (orgnr, year), or 0 if none."""
        versions = self.get_versions(orgnr, year)
        if not versions:
            return 0
        return max(r.version for r in versions)

    def has_hash(self, orgnr: str, year: int, file_hash: str | None, pdf_hash: str | None) -> bool:
        """Check if any version of (orgnr, year) already has this content.

        Returns True if either file_hash or pdf_hash matches an existing version.
        """
        versions = self.get_versions(orgnr, year)
        for v in versions:
            if file_hash and v.file_hash and v.file_hash == file_hash:
                return True
            if pdf_hash and v.pdf_hash and v.pdf_hash == pdf_hash:
                return True
        return False

    def list_missing(self, orgnr_list: list[str], year: int) -> list[str]:
        """Return orgnr values from the input list that are NOT in the manifest for the given year.

        Used to determine which entities need downloading.
        """
        table = self.load()
        if table.num_rows == 0:
            return list(orgnr_list)

        year_mask = pc.equal(table.column("year"), year)
        year_rows = table.filter(year_mask)
        existing_orgnr = set(year_rows.column("orgnr").to_pylist())
        return [o for o in orgnr_list if o not in existing_orgnr]

    def detect_corrections(self, orgnr: str, new_journalnr: str, year: int) -> bool:
        """Check if the given orgnr+year has a different journalnr in the manifest.

        Returns True if a correction is detected (any existing version has a different journalnr).
        Returns False if no existing entry or all journalnr values match.
        """
        versions = self.get_versions(orgnr, year)
        if not versions:
            return False
        for v in versions:
            if v.journalnr and v.journalnr != new_journalnr:
                return True
        return False

    @staticmethod
    def merge_shards(storage: StorageBackend, shard_paths: list[str], output_path: str) -> None:
        """Merge multiple shard manifest files into a single global manifest.

        Used after GitHub Actions matrix jobs complete. Concatenates all shard tables,
        deduplicates by (orgnr, year, version) keeping the most recent download_timestamp,
        and writes the merged result.
        """
        tables = []

        output_mgr = ManifestManager(storage, output_path)
        existing = output_mgr.load()
        if existing.num_rows > 0:
            tables.append(existing)

        for path in shard_paths:
            if not storage.exists(path):
                continue
            raw = storage.read_bytes(path)
            buf = pa.BufferReader(raw)
            shard = pq.read_table(buf, schema=MANIFEST_SCHEMA)
            if shard.num_rows > 0:
                tables.append(shard)

        if not tables:
            output_mgr.save(_empty_table())
            return

        combined = pa.concat_tables(tables, promote_options="none")

        if combined.num_rows == 0:
            output_mgr.save(combined)
            return

        orgnr_col = combined.column("orgnr")
        year_col = combined.column("year")
        version_col = combined.column("version")
        ts_col = combined.column("download_timestamp")

        seen: dict[tuple[str, int, int], int] = {}
        for i in range(combined.num_rows):
            key = (orgnr_col[i].as_py(), year_col[i].as_py(), version_col[i].as_py())
            ts = ts_col[i].as_py() or ""
            if key not in seen:
                seen[key] = i
            else:
                existing_ts = ts_col[seen[key]].as_py() or ""
                if ts > existing_ts:
                    seen[key] = i

        keep_indices = sorted(seen.values())
        deduped = combined.take(keep_indices)
        output_mgr.save(deduped)
