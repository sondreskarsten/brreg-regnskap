"""Daily coordinator (hefty).

Owns the manifest. Runs once a day and absorbs all the heavy, full-population
memory work so the collector can stay lean:

1. Drain the holding area the collector filled: server-side copy each PDF/JSON
   to its final ``regnskap/{orgnr}/...`` path, fold a success row into the
   manifest from the tiny sidecar, then delete the holding object.
2. Poll the updates API for new filings (delegated to the existing patch).
3. Recompute pending = orderflow anti-join manifest and write it as a flat work
   list, resetting the collector cursor to 0.

Only this process loads the 4M-row manifest, so it keeps the larger memory
allocation; the collector never touches it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from brreg_regnskap.api.models import ManifestRecord
from brreg_regnskap.config import Settings
from brreg_regnskap.duckgcs import connect, to_gcs
from brreg_regnskap.manifest import MANIFEST_SCHEMA
from brreg_regnskap.orderflow import OrderflowManager
from brreg_regnskap.storage import StorageBackend

logger = structlog.get_logger()


class Coordinator:
    def __init__(self, settings: Settings, key_file: str | None = None) -> None:
        self._settings = settings
        self._storage = StorageBackend.from_settings(settings)
        self._orderflow = OrderflowManager(self._storage, settings)
        self._key_file = key_file
        self._is_gcs = settings.storage_path.startswith("gs://")

    def _con(self):
        return connect(self._key_file, gcs=self._is_gcs)

    def drain_holding(self) -> int:
        """Move collected files to final paths and fold them into the manifest."""
        meta_prefix = f"{self._settings.holding_prefix}/meta/"
        meta_keys = [k for k in self._storage.list_dir(meta_prefix) if k.endswith(".json")]
        if not meta_keys:
            logger.info("holding_empty")
            return 0

        records: list[ManifestRecord] = []
        now_iso = datetime.now(UTC).isoformat()
        drained = 0

        for meta_key in meta_keys:
            meta = json.loads(self._storage.read_bytes(meta_key))
            orgnr = meta["orgnr"]
            year = int(meta["year"])

            holding_pdf = f"{self._settings.holding_prefix}/pdf/{orgnr}_{year}.pdf"
            holding_json = f"{self._settings.holding_prefix}/json/{orgnr}.json"
            final_pdf = self._settings.regnskap_pdf_path(orgnr, year)
            final_json = self._settings.regnskap_json_path(orgnr, year)

            if not self._storage.exists(holding_pdf):
                continue
            self._storage.copy(holding_pdf, final_pdf)

            json_final_path = None
            file_hash = None
            if self._storage.exists(holding_json):
                raw = self._storage.read_bytes(holding_json)
                self._storage.write_bytes(final_json, raw)
                json_final_path = final_json
                import hashlib

                file_hash = hashlib.sha256(raw).hexdigest()

            records.append(
                ManifestRecord(
                    orgnr=orgnr,
                    year=year,
                    download_timestamp=now_iso,
                    pdf_hash=meta.get("pdf_hash"),
                    file_hash=file_hash,
                    pdf_path=final_pdf,
                    json_path=json_final_path,
                    file_size_bytes=meta.get("pdf_size"),
                    status="success",
                )
            )

            self._storage.delete(holding_pdf)
            if self._storage.exists(holding_json):
                self._storage.delete(holding_json)
            self._storage.delete(meta_key)
            drained += 1

        if records:
            self._merge_into_manifest(records)
        logger.info("holding_drained", count=drained)
        return drained

    def _merge_into_manifest(self, records: list[ManifestRecord]) -> None:
        """Streaming manifest rewrite: existing minus replaced keys, plus new rows.

        DuckDB streams the 4M-row manifest under a memory cap rather than loading
        it into Arrow (which costs ~2.8 GB). New keys (orgnr, year, version)
        replace any existing rows with the same key.
        """
        new_table = _records_to_table(records)
        con = self._con()
        try:
            con.register("new_records", new_table)
            mpath = to_gcs(self._settings.manifest_path)
            tmp = to_gcs(self._settings.manifest_path.replace(".parquet", ".tmp.parquet"))
            exists = self._storage.exists(self._settings.manifest_path)
            if exists:
                con.execute(
                    f"""
                    COPY (
                      SELECT * FROM read_parquet('{mpath}') m
                      WHERE NOT EXISTS (
                        SELECT 1 FROM new_records n
                        WHERE n.orgnr = m.orgnr AND n.year = m.year AND n.version = m.version
                      )
                      UNION ALL BY NAME
                      SELECT * FROM new_records
                    ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)
                    """
                )
            else:
                con.execute(
                    f"COPY (SELECT * FROM new_records) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)"
                )
        finally:
            con.close()
        # Atomic-ish swap: copy tmp over the manifest, delete tmp.
        self._storage.copy(
            self._settings.manifest_path.replace(".parquet", ".tmp.parquet"),
            self._settings.manifest_path,
        )
        self._storage.delete(self._settings.manifest_path.replace(".parquet", ".tmp.parquet"))

    def build_work_list(self) -> int:
        """Streaming anti-join: write pending (orgnr, year) and reset the cursor.

        Pending = fast-lane orderflow entries with no success row downloaded at or
        after their create_time. DuckDB streams the manifest under a memory cap.
        """
        con = self._con()
        try:
            of = to_gcs(f"{self._settings.storage_path}/orderflow/shard_*.parquet")
            out = to_gcs(self._settings.work_list_path)
            if self._storage.exists(self._settings.manifest_path):
                mpath = to_gcs(self._settings.manifest_path)
                anti = f"""
                  AND NOT EXISTS (
                    SELECT 1 FROM read_parquet('{mpath}') m
                    WHERE m.orgnr = o.orgnr AND m.year = o.year
                      AND m.status = 'success' AND m.pdf_path IS NOT NULL
                      AND epoch(CAST(m.download_timestamp AS TIMESTAMP)) >= o.create_time
                  )
                """
            else:
                anti = ""
            con.execute(
                f"""
                COPY (
                  SELECT o.orgnr, CAST(o.year AS INTEGER) AS year
                  FROM read_parquet('{of}') o
                  WHERE o.year IS NOT NULL
                    AND o.processing_priority = o.create_time
                    {anti}
                  ORDER BY o.processing_priority DESC
                ) TO '{out}' (FORMAT parquet, COMPRESSION zstd)
                """
            )
            entries = con.execute(
                f"SELECT count(*) FROM read_parquet('{out}')"
            ).fetchone()[0]
        finally:
            con.close()

        self._storage.write_bytes(
            self._settings.collect_cursor_path, json.dumps({"position": 0}).encode()
        )
        logger.info("work_list_built", entries=entries)
        return int(entries)


def _records_to_table(records: list[ManifestRecord]) -> pa.Table:
    cols: dict[str, list[object]] = {f.name: [] for f in MANIFEST_SCHEMA}
    for r in records:
        d = r.model_dump()
        for f in MANIFEST_SCHEMA:
            cols[f.name].append(d.get(f.name))
    arrays = {f.name: pa.array(cols[f.name], type=f.type) for f in MANIFEST_SCHEMA}
    return pa.table(arrays, schema=MANIFEST_SCHEMA)
