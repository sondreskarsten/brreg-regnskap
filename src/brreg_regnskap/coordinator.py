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
from brreg_regnskap.manifest import ManifestManager
from brreg_regnskap.orderflow import OrderflowManager
from brreg_regnskap.storage import StorageBackend

logger = structlog.get_logger()


class Coordinator:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._storage = StorageBackend.from_settings(settings)
        self._manifest = ManifestManager(self._storage, settings.manifest_path)
        self._orderflow = OrderflowManager(self._storage, settings)

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
            self._manifest.upsert(records)
        logger.info("holding_drained", count=drained)
        return drained

    def build_work_list(self) -> int:
        """Write pending (orgnr, year) as a flat work list and reset the cursor."""
        manifest_ts = self._load_manifest_ts()
        rows_orgnr: list[str] = []
        rows_year: list[int] = []
        for digit in range(10):
            pending = self._orderflow.fast_lane_pending(digit, manifest_ts)
            if pending.num_rows == 0:
                continue
            rows_orgnr.extend(pending.column("orgnr").to_pylist())
            rows_year.extend(pending.column("year").to_pylist())

        table = pa.table(
            {
                "orgnr": pa.array(rows_orgnr, type=pa.string()),
                "year": pa.array(rows_year, type=pa.int32()),
            }
        )
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd")
        self._storage.write_bytes(self._settings.work_list_path, sink.getvalue().to_pybytes())
        self._storage.write_bytes(
            self._settings.collect_cursor_path, json.dumps({"position": 0}).encode()
        )
        logger.info("work_list_built", entries=table.num_rows)
        return table.num_rows

    def _load_manifest_ts(self) -> dict[tuple[str, int], int]:
        table = self._manifest.load()
        ts: dict[tuple[str, int], int] = {}
        if table.num_rows == 0:
            return ts
        orgnr_col = table.column("orgnr").to_pylist()
        year_col = table.column("year").to_pylist()
        status_col = table.column("status").to_pylist()
        pdf_col = table.column("pdf_path").to_pylist()
        for o, y, s, p in zip(orgnr_col, year_col, status_col, pdf_col, strict=False):
            if s == "success" and p:
                ts[(o, y)] = 1
        return ts
