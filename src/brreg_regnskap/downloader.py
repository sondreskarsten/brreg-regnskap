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
from datetime import UTC, datetime
from typing import Any

import aiohttp
import structlog
from aiolimiter import AsyncLimiter
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from brreg_regnskap.api.enhetsregisteret import EnhetsregisteretClient
from brreg_regnskap.api.models import ManifestRecord, Regnskap
from brreg_regnskap.api.regnskapsregisteret import RegnskapsregisteretClient
from brreg_regnskap.checkpoint import CheckpointManager, CheckpointState
from brreg_regnskap.config import Settings, SyncMode
from brreg_regnskap.manifest import ManifestManager
from brreg_regnskap.storage import StorageBackend

logger = structlog.get_logger()

RETRYABLE_ERRORS = (
    aiohttp.ClientError,
    aiohttp.ServerDisconnectedError,
    asyncio.TimeoutError,
    ConnectionError,
)


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
        """Full sync: download bulk dump, iterate all entities with regnskap."""
        connector = aiohttp.TCPConnector(limit=self._settings.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=300)

        async with (
            EnhetsregisteretClient() as enhet_client,
            aiohttp.ClientSession(connector=connector, timeout=timeout) as session,
        ):
            regnskap_client = RegnskapsregisteretClient(session=session)

            logger.info("downloading_bulk_dump")
            raw_dump = await self._throttled_request(enhet_client.download_bulk_dump())
            logger.info("parsing_bulk_dump")
            entities = enhet_client.iter_entities_from_dump(raw_dump)
            entities.sort(key=lambda e: e.organisasjonsnummer)

            dump_date = datetime.now(UTC).strftime("%Y%m%d")
            dump_path = self._settings.entity_dump_path(dump_date)
            self._storage.write_bytes(dump_path, raw_dump)
            logger.info("bulk_dump_saved", path=dump_path, entities=len(entities))

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

            if state.last_orgnr_processed:
                entities = [
                    e for e in entities
                    if e.organisasjonsnummer > state.last_orgnr_processed
                ]
                logger.info(
                    "resuming_from_checkpoint",
                    after=state.last_orgnr_processed,
                    remaining=len(entities),
                )

            state.entities_total = len(entities)
            orgnr_list = [e.organisasjonsnummer for e in entities]

            await self._process_batch(orgnr_list, regnskap_client, state)

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
                processed=state.entities_processed,
                total=total,
                **self._stats,
            )

        self._checkpoint_mgr.clear()
        logger.info("sync_complete", processed=state.entities_processed)

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

        raw_json = await self._throttled_request(regnskap_client.fetch_regnskap_raw(orgnr))
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

        available_years = await self._throttled_request(regnskap_client.fetch_years(orgnr))

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

        for year in available_years:
            existing = self._manifest.get(orgnr, year)
            if existing and existing.pdf_path and existing.status == "success":
                continue

            pdf_data = await self._throttled_request(regnskap_client.download_pdf(orgnr, year))
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

    async def _throttled_request(self, coro: Any) -> Any:
        """Execute an async request with semaphore + rate limiter + retry."""

        @retry(
            retry=retry_if_exception_type(RETRYABLE_ERRORS),
            wait=wait_exponential_jitter(initial=1, max=60, jitter=2),
            stop=stop_after_attempt(self._settings.max_retries),
            before=_before_retry_log,
            reraise=True,
        )
        async def _inner() -> Any:
            async with self._semaphore:
                await self._limiter.acquire()
                return await coro

        return await _inner()

    @staticmethod
    def _hash_content(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()
