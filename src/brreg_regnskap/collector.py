"""Lean collector.

A dumb cursor-walker over the coordinator's work list. It holds no manifest
and does no dedup — that is all the coordinator's job. Per batch it downloads
each (orgnr, year) PDF (+ the orgnr's JSON) and dumps the raw bytes plus a tiny
sidecar into the holding area, then advances the cursor. It stops when the kopi
quota saturates (circuit breaker) or max-runtime is reached, and the next
scheduled run resumes from the cursor.

Memory footprint is a batch of bytes plus a slice of the work list — there is
no 4M-row structure anywhere, so it runs comfortably in ~1 GiB.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque

import aiohttp
import pyarrow.parquet as pq
import structlog
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from brreg_regnskap.adaptive_limiter import AdaptiveLimiter
from brreg_regnskap.api.regnskapsregisteret import (
    BrregRateLimitError,
    RegnskapsregisteretClient,
)
from brreg_regnskap.config import Settings
from brreg_regnskap.storage import StorageBackend

logger = structlog.get_logger()

_THROTTLE_WINDOW = 90.0
_THROTTLE_SATURATION = 18
_THROTTLE_WINDOW_MAX = 1000


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, BrregRateLimitError):
        return True
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status == 429 or exc.status >= 500
    return isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError))


class Collector:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._storage = StorageBackend.from_settings(settings)
        self._pdf_limiter = AdaptiveLimiter(settings.requests_per_second, start_rate=1.5)
        self._json_limiter = AdaptiveLimiter(20.0, start_rate=20.0, min_rate=5.0)
        self._semaphore = asyncio.Semaphore(settings.max_concurrent)
        self._start_time = time.monotonic()
        self._shutdown = False
        self._pdf_throttle_times: deque[float] = deque(maxlen=_THROTTLE_WINDOW_MAX)
        self._stats = {"pdfs": 0, "missing": 0, "failed": 0, "jsons": 0}
        self._collected_orgnrs: set[str] = set()

    async def run(self) -> dict[str, int]:
        self._storage.check_credentials()

        if not self._storage.exists(self._settings.work_list_path):
            logger.warning("no_work_list", path=self._settings.work_list_path)
            return dict(self._stats)

        work = self._load_work_list()
        total = work.num_rows
        cursor = self._load_cursor()
        if cursor >= total:
            logger.info("work_list_exhausted", cursor=cursor, total=total)
            return dict(self._stats)

        orgnr_col = work.column("orgnr")
        year_col = work.column("year")

        connector = aiohttp.TCPConnector(limit=self._settings.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=300)
        batch = self._settings.checkpoint_interval

        logger.info("collect_start", cursor=cursor, total=total)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            client = RegnskapsregisteretClient(session=session)
            idx = cursor
            pipeline = self._settings.max_concurrent
            while idx < total:
                if self._should_shutdown():
                    break
                window = min(pipeline, total - idx)
                tasks = []
                for k in range(window):
                    orgnr = orgnr_col[idx + k].as_py()
                    year = year_col[idx + k].as_py()
                    tasks.append(self._collect_one(client, orgnr, year))
                await asyncio.gather(*tasks)
                idx += window
                if (idx - cursor) % batch < pipeline:
                    self._save_cursor(idx)
                    logger.info("collect_progress", cursor=idx, total=total, **self._stats)

            # ── Phase 2: raw-JSON pass over orgnrs collected this run ──
            # The JSON endpoint takes no year and returns only the max (latest
            # delivered) year — which equals the fast-lane year. Unlimited
            # endpoint, so it runs even if the PDF burst hit the quota wall.
            await self._json_pass(client)

        self._save_cursor(idx)
        logger.info("collect_finished", cursor=idx, total=total, **self._stats)
        return dict(self._stats)

    async def _json_pass(self, client: RegnskapsregisteretClient) -> None:
        orgnrs = sorted(self._collected_orgnrs)
        if not orgnrs:
            return
        logger.info("json_pass_start", orgnrs=len(orgnrs))
        sem = asyncio.Semaphore(self._settings.max_concurrent)

        async def one(orgnr: str) -> None:
            async with sem:
                await self._json_limiter.acquire()
                try:
                    raw = await client.fetch_regnskap_raw(orgnr)
                except Exception as exc:
                    logger.warning("json_failed", orgnr=orgnr, error=str(exc))
                    return
            if raw is not None:
                await asyncio.to_thread(self._put, f"json/{orgnr}.json", raw)
                self._stats["jsons"] += 1

        await asyncio.gather(*(one(o) for o in orgnrs))
        logger.info("json_pass_done", **self._stats)

    async def _collect_one(self, client: RegnskapsregisteretClient, orgnr: str, year: int) -> None:
        """Download one PDF and stream it to the holding blob — nothing else.

        PDF is the only quota-scarce resource, so the collector does the bare
        minimum during the clean window: one fetch, one write, advance. JSON,
        hashing, and the manifest are the coordinator's job. orgnr/year travel
        in the blob name, so no sidecar is needed.
        """
        try:
            pdf = await self._throttled(client.download_pdf, orgnr, year, limiter=self._pdf_limiter)
        except Exception as exc:
            logger.warning("pdf_failed", orgnr=orgnr, year=year, error=str(exc))
            self._stats["failed"] += 1
            return

        if pdf is None:
            self._stats["missing"] += 1
            return

        await asyncio.to_thread(self._put, f"pdf/{orgnr}_{year}.pdf", pdf)
        self._stats["pdfs"] += 1
        self._collected_orgnrs.add(orgnr)

    async def _throttled(self, coro_fn, *args, limiter: AdaptiveLimiter, **kwargs):
        @retry(
            retry=retry_if_exception(_is_retryable),
            wait=wait_exponential_jitter(initial=1, max=60, jitter=2),
            stop=stop_after_attempt(self._settings.max_retries),
            reraise=True,
        )
        async def _inner():
            async with self._semaphore:
                await limiter.acquire()
                try:
                    result = await coro_fn(*args, **kwargs)
                except (aiohttp.ClientResponseError, BrregRateLimitError) as exc:
                    if isinstance(exc, BrregRateLimitError) or (
                        isinstance(exc, aiohttp.ClientResponseError) and exc.status == 429
                    ):
                        await limiter.on_throttle()
                        if limiter is self._pdf_limiter:
                            self._pdf_throttle_times.append(time.monotonic())
                    raise
                await limiter.on_success()
                return result

        return await _inner()

    def _put(self, rel: str, data: bytes) -> None:
        self._storage.write_bytes(f"{self._settings.holding_prefix}/{rel}", data)

    def _load_work_list(self):
        raw = self._storage.read_bytes(self._settings.work_list_path)
        import pyarrow as pa

        return pq.read_table(pa.BufferReader(raw))

    def _load_cursor(self) -> int:
        if not self._storage.exists(self._settings.collect_cursor_path):
            return 0
        data = json.loads(self._storage.read_bytes(self._settings.collect_cursor_path))
        return int(data.get("position", 0))

    def _save_cursor(self, position: int) -> None:
        self._storage.write_bytes(
            self._settings.collect_cursor_path,
            json.dumps({"position": position}).encode(),
        )

    def _should_shutdown(self) -> bool:
        if self._shutdown:
            return True
        if self._pdf_quota_saturated():
            if not self._shutdown:
                logger.warning(
                    "pdf_quota_saturated",
                    throttles_in_window=len(self._pdf_throttle_times),
                    action="ending collect for quota recharge",
                )
            self._shutdown = True
            return True
        if self._settings.max_runtime_minutes > 0:
            elapsed = time.monotonic() - self._start_time
            if elapsed > self._settings.max_runtime_minutes * 60 - 120:
                return True
        return False

    def _pdf_quota_saturated(self) -> bool:
        now = time.monotonic()
        recent = sum(1 for t in self._pdf_throttle_times if now - t <= _THROTTLE_WINDOW)
        return recent >= _THROTTLE_SATURATION
