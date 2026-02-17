"""Async download engine orchestrating the full sync pipeline.

This is the core module that ties everything together:
    1. Determines which entities need processing (full dump or incremental updates)
    2. For each entity: fetches regnskap JSON, available years, and PDFs
    3. Detects corrections by comparing journalnr against manifest
    4. Archives old files when corrections are detected
    5. Writes new files to storage
    6. Updates manifest
    7. Checkpoints every N entities for resume safety

Rate limiting strategy:
    - Semaphore(max_concurrent) for connection pool size
    - AsyncLimiter(requests_per_second, 1) for throughput
    - On HTTP 429 or 503: reduce limiter to 2 req/s for 60 seconds, then restore
    - tenacity retry: wait_exponential_jitter(initial=1, max=60, jitter=2), max 5 attempts

Storage layout per entity:
    regnskap/{orgnr}/regnskap_{year}.json     - raw JSON from regnskapsregisteret
    regnskap/{orgnr}/aarsregnskap_{year}.pdf  - PDF annual report
    corrections/{orgnr}/regnskap_{year}_{journalnr}_{timestamp}.json - archived corrections
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


class SyncEngine:
    """Orchestrates the full/incremental sync pipeline.

    Usage:
        settings = Settings()
        engine = SyncEngine(settings)
        await engine.run(mode=SyncMode.FULL)

    The engine is designed to be called from the CLI layer.
    It handles its own aiohttp session lifecycle.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._storage = StorageBackend.from_settings(settings)
        self._manifest = ManifestManager(self._storage, settings.manifest_path)
        self._checkpoint_mgr = CheckpointManager(self._storage, settings.checkpoint_path)
        self._semaphore = asyncio.Semaphore(settings.max_concurrent)
        self._limiter = AsyncLimiter(settings.requests_per_second, 1)
        self._start_time = time.monotonic()
        self._shutdown_requested = False
        self._stats = {"processed": 0, "success": 0, "failed": 0, "skipped": 0, "pdfs": 0}

    def _time_remaining(self) -> float | None:
        """Seconds remaining before max_runtime. None if unlimited."""
        if self._settings.max_runtime_minutes <= 0:
            return None
        elapsed = time.monotonic() - self._start_time
        limit = self._settings.max_runtime_minutes * 60
        return max(0.0, limit - elapsed)

    def _should_shutdown(self) -> bool:
        """Check if we should gracefully stop due to time limit."""
        remaining = self._time_remaining()
        if remaining is not None and remaining < 120:
            return True
        return self._shutdown_requested

    async def run(self, mode: SyncMode = SyncMode.FULL) -> None:
        """Execute the sync pipeline."""
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

    async def _run_full(self, state: CheckpointState) -> None:
        """Full sync: two-phase pipeline.

        Phase 1 (metadata): Build available_years.json by calling fetch_years for each entity.
        Phase 2 (download): Year-by-year, newest first. For each year, process only
            entities whose sisteInnsendteAarsregnskap matches that year.
            Per entity: download regnskap JSON + PDF for that year.
        """
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

            if state.phase == "metadata" or not self._storage.exists(self._settings.available_years_path):
                await self._build_available_years(
                    list(orgnr_to_year.keys()), regnskap_client, state,
                )
                if self._should_shutdown():
                    return

            state.phase = "download"
            self._checkpoint_mgr.save(state)

            year_groups: dict[int, list[str]] = {}
            for orgnr, year in orgnr_to_year.items():
                year_groups.setdefault(year, []).append(orgnr)

            all_years = sorted(year_groups.keys(), reverse=True)
            logger.info("year_groups", years=all_years, counts={y: len(v) for y, v in year_groups.items()})

            if state.current_year:
                all_years = [y for y in all_years if y <= state.current_year]

            for year in all_years:
                if self._should_shutdown():
                    logger.info("graceful_shutdown", reason="time_limit", current_year=year)
                    self._checkpoint_mgr.save(state)
                    return

                orgnr_list = sorted(year_groups[year])

                if state.current_year == year and state.last_orgnr_processed:
                    orgnr_list = [o for o in orgnr_list if o > state.last_orgnr_processed]

                state.current_year = year
                state.entities_total = len(orgnr_list)
                state.entities_processed = 0
                logger.info("year_pass_start", year=year, entities=len(orgnr_list))

                await self._process_year_batch(orgnr_list, year, regnskap_client, state)

            self._checkpoint_mgr.clear()
            logger.info("sync_complete", **{k: v for k, v in self._stats.items()})

    async def _build_available_years(
        self,
        orgnr_list: list[str],
        regnskap_client: RegnskapsregisteretClient,
        state: CheckpointState,
    ) -> None:
        """Phase 1: fetch available PDF years for all entities, save as one JSON.

        Processes entities serially to avoid thundering-herd 429s on the /aar endpoint.
        """
        if self._storage.exists(self._settings.available_years_path):
            logger.info("reusing_available_years", path=self._settings.available_years_path)
            return

        logger.info("building_available_years", entities=len(orgnr_list))
        result: dict[str, list[int]] = {}

        if state.last_orgnr_processed and state.phase == "metadata":
            orgnr_list = [o for o in orgnr_list if o > state.last_orgnr_processed]

        total = len(orgnr_list)
        checkpoint_every = self._settings.checkpoint_interval

        for idx, orgnr in enumerate(orgnr_list):
            if self._should_shutdown():
                logger.info("graceful_shutdown", reason="time_limit", phase="metadata")
                self._checkpoint_mgr.save(state)
                return

            years = await self._fetch_years_safe(orgnr, regnskap_client)
            if years:
                result[orgnr] = years

            state.last_orgnr_processed = orgnr
            state.entities_processed += 1

            if (idx + 1) % checkpoint_every == 0:
                self._checkpoint_mgr.save(state)
                logger.info(
                    "metadata_checkpoint",
                    entities_processed=state.entities_processed,
                    total=total,
                    years_found=len(result),
                )

            await asyncio.sleep(0.35)

        data = json.dumps(result, separators=(",", ":")).encode("utf-8")
        self._storage.write_bytes(self._settings.available_years_path, data)
        logger.info("available_years_saved", path=self._settings.available_years_path, entities=len(result))

        state.last_orgnr_processed = None
        state.phase = "download"

    async def _fetch_years_safe(
        self, orgnr: str, regnskap_client: RegnskapsregisteretClient
    ) -> list[int]:
        """Fetch available years for one entity, returning [] on failure."""
        try:
            return await self._throttled_request(regnskap_client.fetch_years, orgnr)
        except Exception as exc:
            logger.warning("fetch_years_failed", orgnr=orgnr, error=str(exc))
            return []

    async def _process_year_batch(
        self,
        orgnr_list: list[str],
        year: int,
        regnskap_client: RegnskapsregisteretClient,
        state: CheckpointState,
    ) -> None:
        """Process all entities for a single year in small sub-batches."""
        sub_batch_size = self._settings.max_concurrent
        checkpoint_every = self._settings.checkpoint_interval
        total = len(orgnr_list)
        records_since_checkpoint: list[ManifestRecord] = []

        for i in range(0, total, sub_batch_size):
            if self._should_shutdown():
                logger.info("graceful_shutdown", reason="time_limit", year=year)
                if records_since_checkpoint:
                    self._manifest.upsert(records_since_checkpoint)
                self._checkpoint_mgr.save(state)
                return

            batch = orgnr_list[i : i + sub_batch_size]
            tasks = [
                self._process_entity_year_safe(orgnr, year, regnskap_client)
                for orgnr in batch
            ]
            results = await asyncio.gather(*tasks)

            for orgnr, records in zip(batch, results, strict=True):
                records_since_checkpoint.extend(records)
                state.last_orgnr_processed = orgnr
                state.entities_processed += 1

            if state.entities_processed % checkpoint_every == 0 or i + sub_batch_size >= total:
                if records_since_checkpoint:
                    self._manifest.upsert(records_since_checkpoint)
                    records_since_checkpoint = []
                self._checkpoint_mgr.save(state)
                logger.info(
                    "batch_checkpointed",
                    year=year,
                    entities_processed=state.entities_processed,
                    total=total,
                    **self._stats,
                )

        logger.info("year_pass_complete", year=year, **self._stats)

    def _load_existing_dump(self) -> bytes | None:
        """Return the most recent bulk dump from storage if it exists.

        Checks today's dump first, then yesterday's. Returns None if
        no recent dump is available.
        """
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

    async def _run_incremental(self, state: CheckpointState) -> None:
        """Incremental sync: poll updates API for changed entities."""
        connector = aiohttp.TCPConnector(limit=self._settings.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=300)

        async with (
            EnhetsregisteretClient() as enhet_client,
            aiohttp.ClientSession(connector=connector, timeout=timeout) as session,
        ):
            regnskap_client = RegnskapsregisteretClient(session=session)

            logger.info(
                "polling_updates",
                since_id=state.last_oppdateringsid,
            )

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
        """Process a list of orgnr in batches with checkpointing."""
        batch_size = self._settings.checkpoint_interval
        total = len(orgnr_list)

        for i in range(0, total, batch_size):
            if self._should_shutdown():
                logger.info(
                    "graceful_shutdown",
                    reason="time_limit",
                    processed=state.entities_processed,
                )
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
        logger.info("sync_complete", processed=state.entities_processed)

    async def _process_entity_year_safe(
        self,
        orgnr: str,
        year: int,
        regnskap_client: RegnskapsregisteretClient,
    ) -> list[ManifestRecord]:
        """Wrapper that catches all exceptions per entity/year."""
        try:
            return await self._process_entity_year(orgnr, year, regnskap_client)
        except Exception as exc:
            logger.error("entity_year_failed", orgnr=orgnr, year=year, error=str(exc))
            self._stats["failed"] += 1
            return [
                ManifestRecord(
                    orgnr=orgnr,
                    year=year,
                    download_timestamp=self._now_iso(),
                    status="failed",
                    error_detail=str(exc)[:500],
                )
            ]

    async def _process_entity_year(
        self,
        orgnr: str,
        year: int,
        regnskap_client: RegnskapsregisteretClient,
    ) -> list[ManifestRecord]:
        """Process one entity for one year: download regnskap JSON + PDF.

        Returns manifest records for the year processed.
        """
        self._stats["processed"] += 1
        now = self._now_iso()

        existing = self._manifest.get(orgnr, year)
        if existing and existing.pdf_path and existing.json_path and existing.status == "success":
            self._stats["skipped"] += 1
            return []

        records: list[ManifestRecord] = []

        raw_json = None
        json_path = None
        file_hash = None
        journalnr = None
        is_correction = False

        try:
            raw_json = await self._throttled_request(regnskap_client.fetch_regnskap_raw, orgnr)
        except aiohttp.ClientResponseError as exc:
            logger.warning("regnskap_json_failed", orgnr=orgnr, status=exc.status)
        except Exception as exc:
            logger.warning("regnskap_json_failed", orgnr=orgnr, error=str(exc))

        if raw_json is not None:
            parsed_items = json.loads(raw_json)
            if isinstance(parsed_items, list) and parsed_items:
                regnskap = Regnskap.model_validate(parsed_items[0])
            elif isinstance(parsed_items, dict):
                regnskap = Regnskap.model_validate(parsed_items)
            else:
                regnskap = None

            if regnskap:
                journalnr = str(regnskap.journalnr) if regnskap.journalnr else None
                if journalnr:
                    is_correction = self._manifest.detect_corrections(orgnr, journalnr, year)
                    if is_correction:
                        await self._archive_correction(orgnr, year, journalnr)

                json_path = self._settings.regnskap_json_path(orgnr, year)
                file_hash = self._hash_content(raw_json)
                self._storage.write_bytes(json_path, raw_json)

        pdf_path = None
        pdf_size = 0
        try:
            pdf_data = await self._throttled_request(regnskap_client.download_pdf, orgnr, year)
        except Exception as exc:
            logger.warning("pdf_download_failed", orgnr=orgnr, year=year, error=str(exc))
            pdf_data = None

        if pdf_data is not None:
            pdf_path = self._settings.regnskap_pdf_path(orgnr, year)
            self._storage.write_bytes(pdf_path, pdf_data)
            pdf_size = len(pdf_data)
            self._stats["pdfs"] += 1

        if json_path or pdf_path:
            records.append(
                ManifestRecord(
                    orgnr=orgnr,
                    year=year,
                    download_timestamp=now,
                    file_hash=file_hash,
                    json_path=json_path,
                    pdf_path=pdf_path,
                    file_size_bytes=(len(raw_json) if raw_json else 0) + pdf_size,
                    is_correction=is_correction,
                    journalnr=journalnr,
                    source_url=f"https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}",
                    status="success",
                )
            )
            self._stats["success"] += 1
        else:
            self._stats["skipped"] += 1

        return records

    async def _process_entity_safe(
        self,
        orgnr: str,
        regnskap_client: RegnskapsregisteretClient,
    ) -> list[ManifestRecord]:
        """Wrapper that catches all exceptions per entity."""
        try:
            return await self._process_entity(orgnr, regnskap_client)
        except Exception as exc:
            logger.error("entity_failed", orgnr=orgnr, error=str(exc))
            self._stats["failed"] += 1
            return [
                ManifestRecord(
                    orgnr=orgnr,
                    year=0,
                    download_timestamp=self._now_iso(),
                    status="failed",
                    error_detail=str(exc)[:500],
                )
            ]

    async def _process_entity(
        self,
        orgnr: str,
        regnskap_client: RegnskapsregisteretClient,
    ) -> list[ManifestRecord]:
        """Process a single entity: fetch regnskap JSON + PDFs for all available years.

        Returns a list of ManifestRecord entries (one per year).
        """
        self._stats["processed"] += 1
        records: list[ManifestRecord] = []
        now = self._now_iso()

        try:
            raw_json = await self._throttled_request(regnskap_client.fetch_regnskap_raw, orgnr)
        except aiohttp.ClientResponseError as exc:
            error_detail = f"HTTP {exc.status}: {exc.message} url={exc.request_info.real_url}"
            logger.warning("regnskap_server_error", orgnr=orgnr, status=exc.status, error=error_detail)
            self._stats["failed"] += 1
            return [
                ManifestRecord(
                    orgnr=orgnr,
                    year=0,
                    download_timestamp=now,
                    source_url=str(exc.request_info.real_url),
                    status="server_error",
                    error_detail=error_detail,
                )
            ]

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
            is_correction = self._manifest.detect_corrections(orgnr, journalnr, regnskap_year)
            if is_correction:
                await self._archive_correction(orgnr, regnskap_year, journalnr)

            json_path = self._settings.regnskap_json_path(orgnr, regnskap_year)
            file_hash = self._hash_content(raw_json)
            self._storage.write_bytes(json_path, raw_json)

            records.append(
                ManifestRecord(
                    orgnr=orgnr,
                    year=regnskap_year,
                    download_timestamp=now,
                    file_hash=file_hash,
                    json_path=json_path,
                    pdf_path=None,
                    file_size_bytes=len(raw_json),
                    is_correction=is_correction,
                    journalnr=journalnr,
                    source_url=f"https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}",
                    status="success",
                )
            )

        available_years = []
        try:
            available_years = await self._throttled_request(regnskap_client.fetch_years, orgnr)
        except Exception as exc:
            logger.warning("fetch_years_failed", orgnr=orgnr, error=str(exc))

        available_years.sort(reverse=True)

        for year in available_years:
            existing = self._manifest.get(orgnr, year)
            if existing and existing.pdf_path and existing.status == "success":
                continue

            try:
                pdf_data = await self._throttled_request(regnskap_client.download_pdf, orgnr, year)
            except Exception as exc:
                logger.warning("pdf_download_failed", orgnr=orgnr, year=year, error=str(exc))
                if not any(r.year == year for r in records):
                    records.append(
                        ManifestRecord(
                            orgnr=orgnr,
                            year=year,
                            download_timestamp=now,
                            status="pdf_failed",
                            error_detail=str(exc)[:500],
                        )
                    )
                continue

            if pdf_data is None:
                if not any(r.year == year for r in records):
                    records.append(
                        ManifestRecord(
                            orgnr=orgnr,
                            year=year,
                            download_timestamp=now,
                            status="pdf_missing",
                        )
                    )
                continue

            pdf_path = self._settings.regnskap_pdf_path(orgnr, year)
            self._storage.write_bytes(pdf_path, pdf_data)
            self._stats["pdfs"] += 1

            existing_record = next((r for r in records if r.year == year), None)
            if existing_record:
                existing_record.pdf_path = pdf_path
            else:
                records.append(
                    ManifestRecord(
                        orgnr=orgnr,
                        year=year,
                        download_timestamp=now,
                        pdf_path=pdf_path,
                        file_size_bytes=len(pdf_data),
                        status="success",
                    )
                )

        if records:
            self._stats["success"] += 1
        else:
            self._stats["skipped"] += 1

        return records

    async def _archive_correction(self, orgnr: str, year: int, old_journalnr: str) -> None:
        """Move existing regnskap files to corrections/ before overwriting."""
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")

        existing_json = self._settings.regnskap_json_path(orgnr, year)
        if self._storage.exists(existing_json):
            old_record = self._manifest.get(orgnr, year)
            jnr = old_record.journalnr if old_record and old_record.journalnr else "unknown"
            correction_json = self._settings.correction_json_path(orgnr, year, jnr, ts)
            self._storage.rename(existing_json, correction_json)
            logger.info("archived_correction", orgnr=orgnr, year=year, type="json")

        existing_pdf = self._settings.regnskap_pdf_path(orgnr, year)
        if self._storage.exists(existing_pdf):
            correction_pdf = self._settings.correction_pdf_path(orgnr, year, ts)
            self._storage.rename(existing_pdf, correction_pdf)
            logger.info("archived_correction", orgnr=orgnr, year=year, type="pdf")

    async def _throttled_request(self, coro_fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute an async request with semaphore + rate limiter + retry."""

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
