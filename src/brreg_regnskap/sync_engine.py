"""Orderflow-driven sync engine.

Replaces the monolithic downloader.py with a two-lane architecture:

1. **Fast lane** — ``(orgnr, year)`` pairs from the bulk dump or BRREG
   update patches.  Downloads JSON + PDF.  Processed first.
2. **Slow lane** — historical years discovered via the ``/aar`` API.
   Downloads PDF only.  Processed when the fast lane is empty for a shard.

Processing order within each shard (by ``orgnr % 10``):

    fast-lane entries   (priority = unix-now, newest first)
    slow-lane entries   (priority = unix(year-01-01), newest years first)

Completed downloads are recorded in the manifest.  The orderflow is
never mutated on completion — the manifest anti-join determines what
work remains.
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
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from brreg_regnskap.api.models import ManifestRecord, Regnskap
from brreg_regnskap.api.regnskapsregisteret import BrregRateLimitError, RegnskapsregisteretClient
from brreg_regnskap.checkpoint import CheckpointManager, CheckpointState
from brreg_regnskap.config import Settings
from brreg_regnskap.manifest import ManifestManager
from brreg_regnskap.orderflow import OrderflowManager
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
    return isinstance(exc, aiohttp.ClientResponseError) and exc.status in RETRYABLE_HTTP_STATUSES


def _before_retry_log(retry_state: RetryCallState) -> None:
    logger.warning(
        "retrying_request",
        attempt=retry_state.attempt_number,
        exception=str(retry_state.outcome.exception()) if retry_state.outcome else None,
    )


class SyncEngine:
    """Orderflow-driven sync engine.

    Usage::

        engine = SyncEngine(settings)
        await engine.run()
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._storage = StorageBackend.from_settings(settings)
        self._manifest = ManifestManager(self._storage, settings.manifest_path)
        self._orderflow = OrderflowManager(self._storage, settings)
        self._checkpoint_mgr = CheckpointManager(self._storage, settings.checkpoint_path)
        self._semaphore = asyncio.Semaphore(settings.max_concurrent)
        self._limiter = AsyncLimiter(settings.requests_per_second, 1)
        self._start_time = time.monotonic()
        self._shutdown = False
        self._stats = {
            "processed": 0, "success": 0, "failed": 0,
            "skipped": 0, "pdfs": 0, "jsons": 0,
        }

    # ── Public entry point ────────────────────────────────────────

    async def run(self) -> dict[str, int]:
        """Process the orderflow: fast lane first, then slow lane.

        Iterates over shards (0-9) or only the configured ``--shard``.
        Returns the stats dict.
        """
        self._storage.check_credentials()

        state = self._checkpoint_mgr.load()
        state.run_started_at = self._now_iso()

        shards = [self._settings.shard] if self._settings.shard is not None else list(range(10))
        manifest_keys = self._load_manifest_keys()

        connector = aiohttp.TCPConnector(limit=self._settings.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=300)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            regnskap_client = RegnskapsregisteretClient(session=session)

            for digit in shards:
                if self._should_shutdown():
                    break

                logger.info("processing_shard", shard=digit)

                # ── Fast lane ────────────────────────────────────
                await self._process_fast_lane(
                    digit, regnskap_client, manifest_keys, state,
                )
                if self._should_shutdown():
                    break

                # ── Slow lane discovery ──────────────────────────
                fast_remaining = self._orderflow.fast_lane_pending(digit, manifest_keys)
                if fast_remaining.num_rows == 0:
                    await self._discover_slow_lane(
                        digit, regnskap_client, manifest_keys,
                    )
                    if self._should_shutdown():
                        break

                    # ── Slow lane download ────────────────────────
                    await self._process_slow_lane(
                        digit, regnskap_client, manifest_keys, state,
                    )

        self._checkpoint_mgr.save(state)
        logger.info("sync_finished", **self._stats)
        return dict(self._stats)

    # ── Fast lane processing ──────────────────────────────────────

    async def _process_fast_lane(
        self,
        digit: int,
        client: RegnskapsregisteretClient,
        manifest_keys: set[tuple[str, int]],
        state: CheckpointState,
    ) -> None:
        pending = self._orderflow.fast_lane_pending(digit, manifest_keys)
        if pending.num_rows == 0:
            logger.info("fast_lane_empty", shard=digit)
            return

        logger.info("fast_lane_start", shard=digit, count=pending.num_rows)
        records_buf: list[ManifestRecord] = []
        checkpoint_every = self._settings.checkpoint_interval

        for idx in range(pending.num_rows):
            if self._should_shutdown():
                break

            orgnr = pending.column("orgnr")[idx].as_py()
            year = pending.column("year")[idx].as_py()

            records = await self._download_entity(orgnr, year, client, json_too=True)
            records_buf.extend(records)
            for r in records:
                if r.status == "success" and r.pdf_path:
                    manifest_keys.add((r.orgnr, r.year))

            state.last_orgnr_processed = orgnr
            state.entities_processed += 1

            if (idx + 1) % checkpoint_every == 0 or idx + 1 == pending.num_rows:
                if records_buf:
                    self._manifest.upsert(records_buf)
                    records_buf = []
                self._checkpoint_mgr.save(state)
                logger.info(
                    "fast_checkpoint", shard=digit,
                    progress=idx + 1, total=pending.num_rows, **self._stats,
                )

            await asyncio.sleep(0.5)

        if records_buf:
            self._manifest.upsert(records_buf)
        logger.info("fast_lane_done", shard=digit, **self._stats)

    # ── Slow lane discovery ───────────────────────────────────────

    async def _discover_slow_lane(
        self,
        digit: int,
        client: RegnskapsregisteretClient,
        manifest_keys: set[tuple[str, int]],
    ) -> None:
        stubs = self._orderflow.discovery_stubs(digit)
        if not stubs:
            return

        logger.info("slow_discovery_start", shard=digit, orgnrs=len(stubs))
        discovered: set[str] = set()
        burst_count = 0

        for orgnr in stubs:
            if self._should_shutdown():
                break

            try:
                years = await self._throttled_request(client.fetch_years, orgnr)
            except Exception as exc:
                logger.warning("years_api_failed", orgnr=orgnr, error=str(exc))
                continue

            if years:
                self._orderflow.enqueue_slow(orgnr, years, manifest_keys)
            discovered.add(orgnr)
            burst_count += 1

            if burst_count >= 30:
                burst_count = 0
                await asyncio.sleep(30)

        if discovered:
            self._orderflow.remove_discovery_stubs(digit, discovered)
        logger.info("slow_discovery_done", shard=digit, discovered=len(discovered))

    # ── Slow lane download ────────────────────────────────────────

    async def _process_slow_lane(
        self,
        digit: int,
        client: RegnskapsregisteretClient,
        manifest_keys: set[tuple[str, int]],
        state: CheckpointState,
    ) -> None:
        pending = self._orderflow.pending(digit, manifest_keys)
        # After fast lane is removed, remaining are slow lane
        slow = pending
        if slow.num_rows == 0:
            logger.info("slow_lane_empty", shard=digit)
            return

        logger.info("slow_lane_start", shard=digit, count=slow.num_rows)
        records_buf: list[ManifestRecord] = []
        checkpoint_every = self._settings.checkpoint_interval
        burst_count = 0

        for idx in range(slow.num_rows):
            if self._should_shutdown():
                break

            orgnr = slow.column("orgnr")[idx].as_py()
            year = slow.column("year")[idx].as_py()

            records = await self._download_entity(orgnr, year, client, json_too=False)
            records_buf.extend(records)
            for r in records:
                if r.status == "success" and r.pdf_path:
                    manifest_keys.add((r.orgnr, r.year))

            state.entities_processed += 1
            burst_count += 1

            if (idx + 1) % checkpoint_every == 0 or idx + 1 == slow.num_rows:
                if records_buf:
                    self._manifest.upsert(records_buf)
                    records_buf = []
                self._checkpoint_mgr.save(state)
                logger.info(
                    "slow_checkpoint", shard=digit,
                    progress=idx + 1, total=slow.num_rows, **self._stats,
                )

            if burst_count >= 30:
                burst_count = 0
                await asyncio.sleep(30)

        if records_buf:
            self._manifest.upsert(records_buf)
        logger.info("slow_lane_done", shard=digit, **self._stats)

    # ── Download a single (orgnr, year) ──────────────────────────

    async def _download_entity(
        self,
        orgnr: str,
        year: int,
        client: RegnskapsregisteretClient,
        *,
        json_too: bool,
    ) -> list[ManifestRecord]:
        self._stats["processed"] += 1
        now = self._now_iso()

        # ── PDF ──────────────────────────────────────────────────
        try:
            pdf_data = await self._throttled_request(client.download_pdf, orgnr, year)
        except Exception as exc:
            logger.warning("pdf_failed", orgnr=orgnr, year=year, error=str(exc))
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

        pdf_hash = self._hash(pdf_data)
        if self._manifest.has_hash(orgnr, year, file_hash=None, pdf_hash=pdf_hash):
            self._stats["skipped"] += 1
            return []

        version = self._manifest.max_version(orgnr, year) + 1
        pdf_path = self._settings.regnskap_pdf_path(orgnr, year, version)
        self._storage.write_bytes(pdf_path, pdf_data)
        self._stats["pdfs"] += 1

        # ── JSON (fast lane only) ────────────────────────────────
        json_path = None
        file_hash = None
        journalnr = None
        json_error = None

        if json_too:
            try:
                raw_json = await self._throttled_request(client.fetch_regnskap_raw, orgnr)
            except Exception as exc:
                logger.warning("json_failed", orgnr=orgnr, error=str(exc))
                raw_json = None
                json_error = str(exc)[:500]

            if raw_json is not None:
                parsed = json.loads(raw_json)
                regnskap = None
                if isinstance(parsed, list) and parsed:
                    regnskap = Regnskap.model_validate(parsed[0])
                elif isinstance(parsed, dict):
                    regnskap = Regnskap.model_validate(parsed)

                if regnskap and regnskap.journalnr:
                    journalnr = str(regnskap.journalnr)

                json_path = self._settings.regnskap_json_path(orgnr, year, version)
                file_hash = self._hash(raw_json)
                self._storage.write_bytes(json_path, raw_json)
                self._stats["jsons"] += 1

        self._stats["success"] += 1
        return [ManifestRecord(
            orgnr=orgnr, year=year, version=version,
            download_timestamp=now,
            file_hash=file_hash, pdf_hash=pdf_hash,
            json_path=json_path, pdf_path=pdf_path,
            file_size_bytes=len(pdf_data),
            is_correction=version > 1,
            journalnr=journalnr,
            source_url=f"https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}",
            status="success",
            error_detail=json_error,
        )]

    # ── Helpers ───────────────────────────────────────────────────

    def _load_manifest_keys(self) -> set[tuple[str, int]]:
        table = self._manifest.load()
        keys: set[tuple[str, int]] = set()
        if table.num_rows == 0:
            return keys
        orgnr_col = table.column("orgnr").to_pylist()
        year_col = table.column("year").to_pylist()
        status_col = table.column("status").to_pylist()
        pdf_col = table.column("pdf_path").to_pylist()
        for o, y, s, p in zip(orgnr_col, year_col, status_col, pdf_col):
            if s == "success" and p:
                keys.add((o, y))
        return keys

    def _should_shutdown(self) -> bool:
        if self._shutdown:
            return True
        remaining = self._time_remaining()
        if remaining is not None and remaining < 120:
            return True
        return False

    def _time_remaining(self) -> float | None:
        if self._settings.max_runtime_minutes <= 0:
            return None
        elapsed = time.monotonic() - self._start_time
        limit = self._settings.max_runtime_minutes * 60
        return max(0.0, limit - elapsed)

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
    def _hash(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()
