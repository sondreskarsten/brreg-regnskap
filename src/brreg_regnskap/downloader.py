"""Async download engine orchestrating the sync pipeline.

Pipeline order on every run:
    1. FRESH PASS — For each entity in enhetsregisteret where
       sisteInnsendteAarsregnskap is set and we don't already have that
       orgnr+year in the manifest with a PDF: download PDF, then regnskap JSON.
       JSON only exists for sisteInnsendteAarsregnskap year.
    2. BACKFILL SCAN — For every orgnr with at least one PDF already
       downloaded, call /aar to discover all available years.
       Store {orgnr: [year, ...]} in backfill_years.json (the backfill DB).
       Skip orgnr already present in the DB.
    3. BACKFILL DOWNLOAD — Year-by-year, newest first, across ALL entities.
       Download PDFs only (no JSON). Do not start year N-1 until year N is
       fully collected for every entity that has it.

Rate limiting:
    - Serial requests with 0.5s sleep for /regnskap endpoint
    - Burst 30 + pause 30s for PDF endpoint in backfill
    - tenacity retry: wait_exponential_jitter(initial=1, max=60, jitter=2)

Storage layout:
    regnskap/{orgnr}/regnskap_{year}.json       - raw JSON (fresh pass only)
    regnskap/{orgnr}/aarsregnskap_{year}.pdf     - PDF annual report
    regnskap/{orgnr}/regnskap_{year}_v2.json     - correction (version 2+)
    regnskap/{orgnr}/aarsregnskap_{year}_v2.pdf  - correction (version 2+)
    metadata/backfill_years.json                 - {orgnr: [2024, 2023, ...]}
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp
import structlog
from aiolimiter import AsyncLimiter
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from brreg_regnskap.api.enhetsregisteret import EnhetsregisteretClient
from brreg_regnskap.api.models import ManifestRecord, Regnskap
from brreg_regnskap.api.regnskapsregisteret import BrregRateLimitError, RegnskapsregisteretClient
from brreg_regnskap.checkpoint import CheckpointManager, CheckpointState
from brreg_regnskap.config import Settings, SyncMode
from brreg_regnskap.manifest import ManifestManager
from brreg_regnskap.storage import StorageBackend

logger = structlog.get_logger()

RETRYABLE_ERRORS = (
    aiohttp.ServerDisconnectedError,
    asyncio.TimeoutError,
    ConnectionError,
    BrregRateLimitError,
)

RETRYABLE_HTTP_STATUSES = {429, 502, 503, 504}


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, RETRYABLE_ERRORS):
        return True
    if isinstance(exc, aiohttp.ClientResponseError) and exc.status in RETRYABLE_HTTP_STATUSES:
        return True
    return False


def _before_retry_log(retry_state: RetryCallState) -> None:
    logger.warning(
        "retrying_request",
        attempt=retry_state.attempt_number,
        exception=str(retry_state.outcome.exception()) if retry_state.outcome else None,
    )


class BackfillDB:
    """Persistent {orgnr: [year, ...]} stored as JSON in GCS/S3/local."""

    def __init__(self, storage: StorageBackend, path: str) -> None:
        self._storage = storage
        self._path = path
        self._data: dict[str, list[int]] = {}
        self._dirty = False

    def load(self) -> None:
        if self._storage.exists(self._path):
            raw = self._storage.read_bytes(self._path)
            self._data = json.loads(raw)
        else:
            self._data = {}
        self._dirty = False

    def save(self) -> None:
        if not self._dirty:
            return
        raw = json.dumps(self._data, separators=(",", ":")).encode("utf-8")
        self._storage.write_bytes(self._path, raw)
        self._dirty = False

    def __contains__(self, orgnr: str) -> bool:
        return orgnr in self._data

    def get_years(self, orgnr: str) -> list[int]:
        return self._data.get(orgnr, [])

    def set_years(self, orgnr: str, years: list[int]) -> None:
        self._data[orgnr] = sorted(years, reverse=True)
        self._dirty = True

    def all_orgnr(self) -> list[str]:
        return list(self._data.keys())

    def all_years(self) -> set[int]:
        result: set[int] = set()
        for years in self._data.values():
            result.update(years)
        return result

    def orgnr_for_year(self, year: int) -> list[str]:
        return [o for o, years in self._data.items() if year in years]

    def __len__(self) -> int:
        return len(self._data)


class SyncEngine:
    """Orchestrates the fresh + backfill sync pipeline.

    Usage:
        settings = Settings()
        engine = SyncEngine(settings)
        await engine.run(mode=SyncMode.FULL)
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._storage = StorageBackend.from_settings(settings)
        self._manifest = ManifestManager(self._storage, settings.manifest_path)
        self._checkpoint_mgr = CheckpointManager(self._storage, settings.checkpoint_path)
        self._backfill_db = BackfillDB(self._storage, settings.backfill_db_path)
        self._semaphore = asyncio.Semaphore(settings.max_concurrent)
        self._limiter = AsyncLimiter(settings.requests_per_second, 1)
        self._start_time = time.monotonic()
        self._shutdown_requested = False
        self._stats = {"processed": 0, "success": 0, "failed": 0, "skipped": 0, "pdfs": 0, "jsons": 0}

    def _time_remaining(self) -> float | None:
        if self._settings.max_runtime_minutes <= 0:
            return None
        elapsed = time.monotonic() - self._start_time
        limit = self._settings.max_runtime_minutes * 60
        return max(0.0, limit - elapsed)

    def _should_shutdown(self) -> bool:
        remaining = self._time_remaining()
        if remaining is not None and remaining < 120:
            return True
        return self._shutdown_requested

    async def run(self, mode: SyncMode = SyncMode.FULL) -> None:
        self._storage.check_credentials()
        state = self._checkpoint_mgr.load()
        state.mode = mode.value
        state.run_started_at = self._now_iso()

        logger.info("sync_started", mode=mode.value, storage=self._settings.storage_path)

        if mode == SyncMode.FULL:
            await self._run_full(state)
        else:
            await self._run_incremental(state)

        logger.info("sync_finished", **self._stats)

    # ── FULL SYNC ──────────────────────────────────────────────────────

    async def _run_full(self, state: CheckpointState) -> None:
        connector = aiohttp.TCPConnector(limit=self._settings.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=300)

        async with (
            EnhetsregisteretClient() as enhet_client,
            aiohttp.ClientSession(connector=connector, timeout=timeout) as session,
        ):
            regnskap_client = RegnskapsregisteretClient(session=session)

            raw_dump = self._load_existing_dump()
            if raw_dump is not None:
                logger.info("reusing_existing_dump")
            else:
                logger.info("downloading_bulk_dump")
                raw_dump = await self._throttled_request(enhet_client.download_bulk_dump)
                dump_date = datetime.now(UTC).strftime("%Y%m%d")
                dump_path = self._settings.entity_dump_path(dump_date)
                self._storage.write_bytes(dump_path, raw_dump)

            logger.info("parsing_bulk_dump")
            entities = enhet_client.iter_entities_from_dump(raw_dump)
            entities.sort(key=lambda e: e.organisasjonsnummer)
            logger.info("bulk_dump_parsed", entities=len(entities))

            if self._settings.orgnr_range_start:
                entities = [
                    e for e in entities
                    if e.organisasjonsnummer >= self._settings.orgnr_range_start
                ]
            if self._settings.orgnr_range_end:
                entities = [
                    e for e in entities
                    if e.organisasjonsnummer <= self._settings.orgnr_range_end
                ]

            orgnr_to_year: dict[str, int] = {}
            for e in entities:
                if e.sisteInnsendteAarsregnskap:
                    orgnr_to_year[e.organisasjonsnummer] = int(e.sisteInnsendteAarsregnskap)

            logger.info("entities_with_regnskap", count=len(orgnr_to_year))

            if state.phase == "fresh":
                await self._run_fresh(orgnr_to_year, regnskap_client, state)
                if self._should_shutdown():
                    return

            if state.phase == "backfill_scan":
                await self._run_backfill_scan(regnskap_client, state)
                if self._should_shutdown():
                    return

            if state.phase == "backfill_download":
                await self._run_backfill_download(regnskap_client, state)

    # ── FRESH PASS ─────────────────────────────────────────────────────

    async def _run_fresh(
        self,
        orgnr_to_year: dict[str, int],
        regnskap_client: RegnskapsregisteretClient,
        state: CheckpointState,
    ) -> None:
        table = self._manifest.load()
        existing_keys: set[tuple[str, int]] = set()
        if table.num_rows > 0:
            orgnr_col = table.column("orgnr").to_pylist()
            year_col = table.column("year").to_pylist()
            status_col = table.column("status").to_pylist()
            pdf_col = table.column("pdf_path").to_pylist()
            for o, y, s, p in zip(orgnr_col, year_col, status_col, pdf_col):
                if s == "success" and p:
                    existing_keys.add((o, y))

        todo = [
            (orgnr, year)
            for orgnr, year in sorted(orgnr_to_year.items())
            if (orgnr, year) not in existing_keys
        ]

        if state.last_orgnr_processed and state.phase == "fresh":
            todo = [(o, y) for o, y in todo if o > state.last_orgnr_processed]

        logger.info("fresh_pass_start", total=len(todo), already_done=len(existing_keys))

        checkpoint_every = self._settings.checkpoint_interval
        records_buf: list[ManifestRecord] = []

        for idx, (orgnr, year) in enumerate(todo):
            if self._should_shutdown():
                logger.info("graceful_shutdown", phase="fresh", orgnr=orgnr)
                if records_buf:
                    self._manifest.upsert(records_buf)
                self._checkpoint_mgr.save(state)
                return

            records = await self._download_fresh_entity(orgnr, year, regnskap_client)
            records_buf.extend(records)
            state.last_orgnr_processed = orgnr
            state.entities_processed += 1

            if (idx + 1) % checkpoint_every == 0 or idx + 1 == len(todo):
                if records_buf:
                    self._manifest.upsert(records_buf)
                    records_buf = []
                self._checkpoint_mgr.save(state)
                logger.info(
                    "fresh_checkpoint",
                    entities_processed=state.entities_processed,
                    total=len(todo),
                    **self._stats,
                )

            await asyncio.sleep(0.5)

        logger.info("fresh_pass_complete", **self._stats)
        state.phase = "backfill_scan"
        state.last_orgnr_processed = None
        state.entities_processed = 0
        self._checkpoint_mgr.save(state)

    async def _download_fresh_entity(
        self,
        orgnr: str,
        year: int,
        regnskap_client: RegnskapsregisteretClient,
    ) -> list[ManifestRecord]:
        self._stats["processed"] += 1
        now = self._now_iso()

        try:
            pdf_data = await self._throttled_request(regnskap_client.download_pdf, orgnr, year)
        except Exception as exc:
            logger.warning("pdf_download_failed", orgnr=orgnr, year=year, error=str(exc))
            self._stats["failed"] += 1
            return [ManifestRecord(
                orgnr=orgnr, year=year, download_timestamp=now,
                status="pdf_failed", error_detail=str(exc)[:500],
            )]

        if pdf_data is None:
            self._stats["skipped"] += 1
            return [ManifestRecord(
                orgnr=orgnr, year=year, download_timestamp=now,
                status="pdf_missing",
            )]

        pdf_hash_val = self._hash_content(pdf_data)

        if self._manifest.has_hash(orgnr, year, file_hash=None, pdf_hash=pdf_hash_val):
            logger.info("pdf_duplicate_skipped", orgnr=orgnr, year=year)
            self._stats["skipped"] += 1
            return []

        version = self._manifest.max_version(orgnr, year) + 1
        is_correction = version > 1

        pdf_path = self._settings.regnskap_pdf_path(orgnr, year, version)
        self._storage.write_bytes(pdf_path, pdf_data)
        self._stats["pdfs"] += 1

        json_path = None
        file_hash = None
        journalnr = None
        json_error = None

        try:
            raw_json = await self._throttled_request(regnskap_client.fetch_regnskap_raw, orgnr)
        except Exception as exc:
            logger.warning("regnskap_json_failed", orgnr=orgnr, error=str(exc))
            raw_json = None
            json_error = str(exc)[:500]

        if raw_json is not None:
            parsed_items = json.loads(raw_json)
            regnskap = None
            if isinstance(parsed_items, list) and parsed_items:
                regnskap = Regnskap.model_validate(parsed_items[0])
            elif isinstance(parsed_items, dict):
                regnskap = Regnskap.model_validate(parsed_items)

            if regnskap and regnskap.journalnr:
                journalnr = str(regnskap.journalnr)

            json_path = self._settings.regnskap_json_path(orgnr, year, version)
            file_hash = self._hash_content(raw_json)
            self._storage.write_bytes(json_path, raw_json)
            self._stats["jsons"] += 1

        self._stats["success"] += 1
        return [ManifestRecord(
            orgnr=orgnr, year=year, version=version, download_timestamp=now,
            file_hash=file_hash, pdf_hash=pdf_hash_val,
            json_path=json_path, pdf_path=pdf_path,
            file_size_bytes=len(pdf_data), is_correction=is_correction,
            journalnr=journalnr,
            source_url=f"https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}",
            status="success",
            error_detail=json_error,
        )]

    # ── BACKFILL SCAN ──────────────────────────────────────────────────

    async def _run_backfill_scan(
        self,
        regnskap_client: RegnskapsregisteretClient,
        state: CheckpointState,
    ) -> None:
        self._backfill_db.load()

        table = self._manifest.load()
        orgnr_with_pdfs: set[str] = set()
        if table.num_rows > 0:
            orgnr_col = table.column("orgnr").to_pylist()
            pdf_col = table.column("pdf_path").to_pylist()
            status_col = table.column("status").to_pylist()
            for o, p, s in zip(orgnr_col, pdf_col, status_col):
                if s == "success" and p:
                    orgnr_with_pdfs.add(o)

        todo = sorted(o for o in orgnr_with_pdfs if o not in self._backfill_db)

        if state.last_orgnr_processed and state.phase == "backfill_scan":
            todo = [o for o in todo if o > state.last_orgnr_processed]

        logger.info(
            "backfill_scan_start",
            entities_to_scan=len(todo),
            already_in_db=len(self._backfill_db),
        )

        checkpoint_every = self._settings.checkpoint_interval
        burst_count = 0
        BURST_SIZE = 30
        BURST_PAUSE = 30

        for idx, orgnr in enumerate(todo):
            if self._should_shutdown():
                logger.info("graceful_shutdown", phase="backfill_scan", orgnr=orgnr)
                self._backfill_db.save()
                self._checkpoint_mgr.save(state)
                return

            years = await self._fetch_years_safe(orgnr, regnskap_client)
            if years:
                self._backfill_db.set_years(orgnr, years)

            state.last_orgnr_processed = orgnr
            state.entities_processed += 1
            burst_count += 1

            if (idx + 1) % checkpoint_every == 0 or idx + 1 == len(todo):
                self._backfill_db.save()
                self._checkpoint_mgr.save(state)
                logger.info(
                    "backfill_scan_checkpoint",
                    entities_processed=state.entities_processed,
                    total=len(todo),
                    db_size=len(self._backfill_db),
                )

            if burst_count >= BURST_SIZE and idx + 1 < len(todo):
                burst_count = 0
                await asyncio.sleep(BURST_PAUSE)

        self._backfill_db.save()
        logger.info("backfill_scan_complete", db_size=len(self._backfill_db))
        state.phase = "backfill_download"
        state.last_orgnr_processed = None
        state.entities_processed = 0
        state.current_year = None
        self._checkpoint_mgr.save(state)

    async def _fetch_years_safe(
        self,
        orgnr: str,
        regnskap_client: RegnskapsregisteretClient,
    ) -> list[int]:
        try:
            years = await self._throttled_request(regnskap_client.fetch_years, orgnr)
            return sorted(years, reverse=True)
        except Exception as exc:
            logger.warning("backfill_scan_failed", orgnr=orgnr, error=str(exc))
            return []

    async def _extract_years_from_regnskap(
        self,
        orgnr: str,
        regnskap_client: RegnskapsregisteretClient,
    ) -> list[int]:
        try:
            raw = await self._throttled_request(regnskap_client.fetch_regnskap_raw, orgnr)
        except Exception as exc:
            logger.warning("backfill_scan_failed", orgnr=orgnr, error=str(exc))
            return []

        if raw is None:
            return []

        parsed = json.loads(raw)
        years: set[int] = set()
        items = parsed if isinstance(parsed, list) else [parsed] if isinstance(parsed, dict) else []
        for rec in items:
            til = rec.get("regnskapsperiode", {}).get("tilDato")
            if til and len(til) >= 4:
                years.add(int(til[:4]))
        return sorted(years, reverse=True)

    # ── BACKFILL DOWNLOAD ──────────────────────────────────────────────

    async def _run_backfill_download(
        self,
        regnskap_client: RegnskapsregisteretClient,
        state: CheckpointState,
    ) -> None:
        self._backfill_db.load()

        table = self._manifest.load()
        completed_keys: set[tuple[str, int]] = set()
        if table.num_rows > 0:
            orgnr_col = table.column("orgnr").to_pylist()
            year_col = table.column("year").to_pylist()
            status_col = table.column("status").to_pylist()
            pdf_col = table.column("pdf_path").to_pylist()
            for o, y, s, p in zip(orgnr_col, year_col, status_col, pdf_col):
                if s == "success" and p:
                    completed_keys.add((o, y))

        all_years = sorted(self._backfill_db.all_years(), reverse=True)
        logger.info("backfill_download_start", years=all_years, db_size=len(self._backfill_db))

        if state.current_year:
            all_years = [y for y in all_years if y <= state.current_year]

        for year in all_years:
            if self._should_shutdown():
                logger.info("graceful_shutdown", phase="backfill_download", year=year)
                self._checkpoint_mgr.save(state)
                return

            candidates = self._backfill_db.orgnr_for_year(year)
            todo = sorted(o for o in candidates if (o, year) not in completed_keys)

            if state.current_year == year and state.last_orgnr_processed:
                todo = [o for o in todo if o > state.last_orgnr_processed]

            if not todo:
                logger.info("backfill_year_complete", year=year, already_done=len(candidates))
                continue

            state.current_year = year
            state.entities_processed = 0
            logger.info("backfill_year_start", year=year, todo=len(todo), already_done=len(candidates) - len(todo))

            await self._download_backfill_year(todo, year, regnskap_client, state, completed_keys)

            if self._should_shutdown():
                return

        self._checkpoint_mgr.clear()
        logger.info("backfill_complete", **self._stats)

    async def _download_backfill_year(
        self,
        orgnr_list: list[str],
        year: int,
        regnskap_client: RegnskapsregisteretClient,
        state: CheckpointState,
        completed_keys: set[tuple[str, int]],
    ) -> None:
        checkpoint_every = self._settings.checkpoint_interval
        records_buf: list[ManifestRecord] = []
        burst_count = 0
        BURST_SIZE = 30
        BURST_PAUSE = 30

        for idx, orgnr in enumerate(orgnr_list):
            if self._should_shutdown():
                logger.info("graceful_shutdown", phase="backfill_download", year=year, orgnr=orgnr)
                if records_buf:
                    self._manifest.upsert(records_buf)
                self._checkpoint_mgr.save(state)
                return

            record = await self._download_backfill_pdf(orgnr, year, regnskap_client)
            if record:
                records_buf.append(record)
                if record.status == "success" and record.pdf_path:
                    completed_keys.add((orgnr, year))

            state.last_orgnr_processed = orgnr
            state.entities_processed += 1
            burst_count += 1

            if (idx + 1) % checkpoint_every == 0 or idx + 1 == len(orgnr_list):
                if records_buf:
                    self._manifest.upsert(records_buf)
                    records_buf = []
                self._checkpoint_mgr.save(state)
                logger.info(
                    "backfill_checkpoint",
                    year=year,
                    entities_processed=state.entities_processed,
                    total=len(orgnr_list),
                    **self._stats,
                )

            if burst_count >= BURST_SIZE and idx + 1 < len(orgnr_list):
                burst_count = 0
                await asyncio.sleep(BURST_PAUSE)

        if records_buf:
            self._manifest.upsert(records_buf)

        logger.info("backfill_year_done", year=year, **self._stats)
        state.last_orgnr_processed = None

    async def _download_backfill_pdf(
        self,
        orgnr: str,
        year: int,
        regnskap_client: RegnskapsregisteretClient,
    ) -> ManifestRecord | None:
        self._stats["processed"] += 1
        now = self._now_iso()

        try:
            pdf_data = await self._throttled_request(regnskap_client.download_pdf, orgnr, year)
        except Exception as exc:
            logger.warning("backfill_pdf_failed", orgnr=orgnr, year=year, error=str(exc))
            self._stats["failed"] += 1
            return ManifestRecord(
                orgnr=orgnr, year=year, download_timestamp=now,
                status="pdf_failed", error_detail=str(exc)[:500],
            )

        if pdf_data is None:
            self._stats["skipped"] += 1
            return ManifestRecord(
                orgnr=orgnr, year=year, download_timestamp=now,
                status="pdf_missing",
            )

        pdf_hash_val = self._hash_content(pdf_data)

        if self._manifest.has_hash(orgnr, year, file_hash=None, pdf_hash=pdf_hash_val):
            logger.info("backfill_pdf_duplicate_skipped", orgnr=orgnr, year=year)
            self._stats["skipped"] += 1
            return None

        version = self._manifest.max_version(orgnr, year) + 1

        pdf_path = self._settings.regnskap_pdf_path(orgnr, year, version)
        self._storage.write_bytes(pdf_path, pdf_data)
        self._stats["pdfs"] += 1
        self._stats["success"] += 1

        return ManifestRecord(
            orgnr=orgnr, year=year, version=version, download_timestamp=now,
            pdf_hash=pdf_hash_val,
            pdf_path=pdf_path, file_size_bytes=len(pdf_data),
            is_correction=version > 1,
            status="success",
        )

    # ── INCREMENTAL SYNC ───────────────────────────────────────────────

    async def _run_incremental(self, state: CheckpointState) -> None:
        connector = aiohttp.TCPConnector(limit=self._settings.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=300)

        async with (
            EnhetsregisteretClient() as enhet_client,
            aiohttp.ClientSession(connector=connector, timeout=timeout) as session,
        ):
            regnskap_client = RegnskapsregisteretClient(session=session)

            logger.info("polling_updates", since_id=state.last_oppdateringsid)

            changed_orgnr: list[str] = []
            max_id = state.last_oppdateringsid

            async for update in enhet_client.poll_regnskap_updates(state.last_oppdateringsid):
                changed_orgnr.append(update.organisasjonsnummer)
                max_id = max(max_id, update.oppdateringsid)
                if len(changed_orgnr) >= 50_000:
                    break

            changed_orgnr = list(dict.fromkeys(changed_orgnr))
            logger.info("updates_collected", count=len(changed_orgnr), max_id=max_id)

            if changed_orgnr:
                await self._process_batch(changed_orgnr, regnskap_client, state)

            state.last_oppdateringsid = max_id
            self._checkpoint_mgr.save(state)

    async def _process_batch(
        self,
        orgnr_list: list[str],
        regnskap_client: RegnskapsregisteretClient,
        state: CheckpointState,
    ) -> None:
        batch_size = self._settings.checkpoint_interval
        total = len(orgnr_list)

        for i in range(0, total, batch_size):
            if self._should_shutdown():
                logger.info("graceful_shutdown", reason="time_limit", entities_processed=state.entities_processed)
                self._checkpoint_mgr.save(state)
                return

            batch = orgnr_list[i : i + batch_size]
            tasks = [self._process_entity_safe(orgnr, regnskap_client) for orgnr in batch]
            results = await asyncio.gather(*tasks)

            all_records: list[ManifestRecord] = []
            for orgnr, records in zip(batch, results, strict=True):
                all_records.extend(records)
                state.last_orgnr_processed = orgnr
                state.entities_processed += 1

            if all_records:
                self._manifest.upsert(all_records)

            self._checkpoint_mgr.save(state)
            logger.info(
                "batch_checkpointed",
                batch=i // batch_size + 1,
                checkpoint_orgnr=state.last_orgnr_processed,
                entities_processed=state.entities_processed,
                total=total,
                **self._stats,
            )

        self._checkpoint_mgr.clear()
        logger.info("sync_complete", entities_processed=state.entities_processed)

    async def _process_entity_safe(
        self,
        orgnr: str,
        regnskap_client: RegnskapsregisteretClient,
    ) -> list[ManifestRecord]:
        try:
            return await self._process_entity(orgnr, regnskap_client)
        except Exception as exc:
            logger.error("entity_failed", orgnr=orgnr, error=str(exc))
            self._stats["failed"] += 1
            return [ManifestRecord(
                orgnr=orgnr, year=0, download_timestamp=self._now_iso(),
                status="failed", error_detail=str(exc)[:500],
            )]

    async def _process_entity(
        self,
        orgnr: str,
        regnskap_client: RegnskapsregisteretClient,
    ) -> list[ManifestRecord]:
        self._stats["processed"] += 1
        records: list[ManifestRecord] = []
        now = self._now_iso()

        try:
            raw_json = await self._throttled_request(regnskap_client.fetch_regnskap_raw, orgnr)
        except aiohttp.ClientResponseError as exc:
            error_detail = f"HTTP {exc.status}: {exc.message} url={exc.request_info.real_url}"
            logger.warning("regnskap_server_error", orgnr=orgnr, status=exc.status, error=error_detail)
            self._stats["failed"] += 1
            return [ManifestRecord(
                orgnr=orgnr, year=0, download_timestamp=now,
                source_url=str(exc.request_info.real_url),
                status="server_error", error_detail=error_detail,
            )]

        if raw_json is None:
            self._stats["skipped"] += 1
            return records

        parsed_items = json.loads(raw_json)
        if isinstance(parsed_items, list) and parsed_items:
            regnskap = Regnskap.model_validate(parsed_items[0])
        elif isinstance(parsed_items, dict):
            regnskap = Regnskap.model_validate(parsed_items)
        else:
            self._stats["skipped"] += 1
            return records

        journalnr = str(regnskap.journalnr) if regnskap.journalnr else None
        regnskap_year = None
        if regnskap.regnskapsperiode and regnskap.regnskapsperiode.tilDato:
            regnskap_year = int(regnskap.regnskapsperiode.tilDato[:4])

        if regnskap_year and journalnr:
            file_hash = self._hash_content(raw_json)
            is_correction = self._manifest.detect_corrections(orgnr, journalnr, regnskap_year)

            if self._manifest.has_hash(orgnr, regnskap_year, file_hash=file_hash, pdf_hash=None):
                self._stats["skipped"] += 1
                return records

            version = self._manifest.max_version(orgnr, regnskap_year) + 1
            json_path = self._settings.regnskap_json_path(orgnr, regnskap_year, version)
            self._storage.write_bytes(json_path, raw_json)

            records.append(ManifestRecord(
                orgnr=orgnr, year=regnskap_year, version=version,
                download_timestamp=now,
                file_hash=file_hash, json_path=json_path, pdf_path=None,
                file_size_bytes=len(raw_json), is_correction=is_correction,
                journalnr=journalnr,
                source_url=f"https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}",
                status="success",
            ))

        available_years = []
        try:
            available_years = await self._throttled_request(regnskap_client.fetch_years, orgnr)
        except Exception as exc:
            logger.warning("fetch_years_failed", orgnr=orgnr, error=str(exc))

        available_years.sort(reverse=True)

        for year in available_years:
            versions = self._manifest.get_versions(orgnr, year)
            if any(v.pdf_path and v.status == "success" for v in versions):
                continue

            try:
                pdf_data = await self._throttled_request(regnskap_client.download_pdf, orgnr, year)
            except Exception as exc:
                logger.warning("pdf_download_failed", orgnr=orgnr, year=year, error=str(exc))
                if not any(r.year == year for r in records):
                    records.append(ManifestRecord(
                        orgnr=orgnr, year=year, download_timestamp=now,
                        status="pdf_failed", error_detail=str(exc)[:500],
                    ))
                continue

            if pdf_data is None:
                if not any(r.year == year for r in records):
                    records.append(ManifestRecord(
                        orgnr=orgnr, year=year, download_timestamp=now,
                        status="pdf_missing",
                    ))
                continue

            pdf_hash_val = self._hash_content(pdf_data)
            if self._manifest.has_hash(orgnr, year, file_hash=None, pdf_hash=pdf_hash_val):
                continue

            pdf_version = self._manifest.max_version(orgnr, year) + 1
            pdf_path = self._settings.regnskap_pdf_path(orgnr, year, pdf_version)
            self._storage.write_bytes(pdf_path, pdf_data)
            self._stats["pdfs"] += 1

            existing_record = next((r for r in records if r.year == year), None)
            if existing_record:
                existing_record.pdf_path = pdf_path
                existing_record.pdf_hash = pdf_hash_val
                existing_record.version = pdf_version
            else:
                records.append(ManifestRecord(
                    orgnr=orgnr, year=year, version=pdf_version,
                    download_timestamp=now,
                    pdf_hash=pdf_hash_val,
                    pdf_path=pdf_path, file_size_bytes=len(pdf_data),
                    status="success",
                ))

        if records:
            self._stats["success"] += 1
        else:
            self._stats["skipped"] += 1

        return records

    # ── SHARED HELPERS ─────────────────────────────────────────────────

    def _load_existing_dump(self) -> bytes | None:
        for days_ago in range(0, 3):
            dt = datetime.now(UTC)
            if days_ago:
                dt = dt - timedelta(days=days_ago)
            date_str = dt.strftime("%Y%m%d")
            path = self._settings.entity_dump_path(date_str)
            if self._storage.exists(path):
                logger.info("found_existing_dump", path=path, age_days=days_ago)
                return self._storage.read_bytes(path)
        return None

    async def _throttled_request(self, coro_fn: Any, *args: Any, **kwargs: Any) -> Any:
        @retry(
            retry=retry_if_exception(_is_retryable),
            wait=wait_exponential_jitter(initial=1, max=60, jitter=2),
            stop=stop_after_attempt(self._settings.max_retries),
            before_sleep=_before_retry_log,
            reraise=True,
        )
        async def _inner() -> Any:
            async with self._semaphore:
                await self._limiter.acquire()
                return await coro_fn(*args, **kwargs)

        return await _inner()

    @staticmethod
    def _hash_content(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()
