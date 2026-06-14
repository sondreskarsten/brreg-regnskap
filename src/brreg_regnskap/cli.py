"""CLI interface for brreg-regnskap.

Commands:
    setup       One-time bucket initialisation (downloads bulk dump, seeds orderflow)
    sync        Process the orderflow queue (fast lane then slow lane)
    patch       Fetch BRREG updates since last run and add to fast lane
    status      Show manifest + orderflow statistics
    verify      Check manifest entries against actual files in storage
    merge       Merge shard manifests from matrix jobs into global manifest

All commands accept a STORAGE_PATH positional argument which can be:
    - A local path: ./data, /tmp/brreg
    - An S3 path: s3://my-bucket/brreg
    - A GCS path: gs://my-bucket/brreg

Environment variables (BRREG_*) override defaults. CLI flags override env vars.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import TYPE_CHECKING

import structlog
import typer
from rich.console import Console

from brreg_regnskap.config import Settings

if TYPE_CHECKING:
    from brreg_regnskap.storage import StorageBackend

app = typer.Typer(
    name="brreg-regnskap",
    help="Bulk mirror of Norwegian annual financial statements from BRREG.",
    no_args_is_help=True,
)
console = Console()


def _configure_logging(level: str) -> None:
    """Set up structlog with the given level."""
    import logging

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
    )


def _build_settings(
    storage_path: str,
    log_level: str = "INFO",
    shard: str | None = None,
    max_concurrent: int | None = None,
    requests_per_second: float | None = None,
    max_runtime: int | None = None,
    checkpoint_interval: int | None = None,
) -> Settings:
    """Build Settings from CLI args, falling back to env vars / defaults."""
    shard_digit: int | None = None
    if shard is not None:
        if shard == "auto":
            from brreg_regnskap.shard import claim_shard
            from brreg_regnskap.storage import StorageBackend

            tmp_settings = Settings(storage_path=storage_path)
            tmp_storage = StorageBackend.from_settings(tmp_settings)
            shard_digit = claim_shard(tmp_storage, storage_path.rstrip("/"))
        else:
            shard_digit = int(shard)
    elif (cr_idx := os.environ.get("CLOUD_RUN_TASK_INDEX")) is not None:
        shard_digit = int(cr_idx)

    overrides: dict[str, object] = {
        "storage_path": storage_path,
        "log_level": log_level,
        "shard": shard_digit,
    }
    if max_concurrent is not None:
        overrides["max_concurrent"] = max_concurrent
    if requests_per_second is not None:
        overrides["requests_per_second"] = requests_per_second
    if max_runtime is not None:
        overrides["max_runtime_minutes"] = max_runtime
    if checkpoint_interval is not None:
        overrides["checkpoint_interval"] = checkpoint_interval
    return Settings(**overrides)  # type: ignore[arg-type]


# ── setup ─────────────────────────────────────────────────────────


@app.command()
def setup(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """One-time bucket initialisation.

    Downloads the BRREG bulk entity dump, stores the ETag, and seeds
    the orderflow with fast-lane entries for every entity that has
    a sisteInnsendteAarsregnskap.  Also creates slow-lane discovery
    stubs so historical years can be back-filled later.
    """
    _configure_logging(log_level)
    settings = Settings(storage_path=storage_path, log_level=log_level)

    from brreg_regnskap.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    asyncio.run(_setup_async(settings, storage))


async def _setup_async(settings: Settings, storage: StorageBackend) -> None:
    from brreg_regnskap.api.enhetsregisteret import EnhetsregisteretClient
    from brreg_regnskap.orderflow import OrderflowManager

    orderflow = OrderflowManager(storage, settings)

    # ── Download bulk dump ────────────────────────────────────────
    async with EnhetsregisteretClient() as client:
        console.print("Downloading bulk entity dump...")
        raw_dump, etag = await client.download_bulk_dump()

    if raw_dump is None:
        console.print("[red]Bulk dump download returned no data.[/red]")
        raise typer.Exit(1)

    # Save dump
    from datetime import UTC, datetime

    dump_date = datetime.now(UTC).strftime("%Y%m%d")
    dump_path = settings.entity_dump_path(dump_date)
    storage.write_bytes(dump_path, raw_dump)
    console.print(f"  Saved dump: {dump_path}")

    # Save ETag
    etag_data = json.dumps({"etag": etag, "dump_date": dump_date}).encode()
    storage.write_bytes(settings.etag_path, etag_data)
    console.print(f"  Saved ETag: {etag}")

    # ── Parse entities ────────────────────────────────────────────
    from brreg_regnskap.api.enhetsregisteret import EnhetsregisteretClient

    entities = EnhetsregisteretClient().iter_entities_from_dump(raw_dump)
    console.print(f"  Entities with regnskap: {len(entities)}")

    # ── Seed orderflow ────────────────────────────────────────────
    fast_entries: list[tuple[str, int]] = []
    slow_orgnrs: list[str] = []

    for e in entities:
        orgnr = e.organisasjonsnummer
        year = int(e.sisteInnsendteAarsregnskap)  # type: ignore[arg-type]
        fast_entries.append((orgnr, year))
        slow_orgnrs.append(orgnr)

    added_fast = orderflow.enqueue_fast(fast_entries, source="bulk_dump")
    added_slow = orderflow.enqueue_slow_discovery(slow_orgnrs)

    console.print("\n[green]Orderflow seeded:[/green]")
    console.print(f"  Fast-lane entries: {added_fast}")
    console.print(f"  Slow-lane discovery stubs: {added_slow}")
    console.print("\nRun [bold]brreg-regnskap sync[/bold] to start processing.")


# ── sync ──────────────────────────────────────────────────────────


@app.command()
def sync(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
    max_concurrent: int | None = typer.Option(
        None, "--max-concurrent", "-c", help="Max simultaneous HTTP connections"
    ),
    requests_per_second: float | None = typer.Option(
        None, "--rps", help="Rate limit (requests per second)"
    ),
    max_runtime: int | None = typer.Option(
        None, "--max-runtime", help="Max runtime in minutes (0=unlimited)"
    ),
    shard: str | None = typer.Option(
        None, "--shard", "-s", help="Shard digit 0-9, or 'auto' to claim a free shard"
    ),
    checkpoint_interval: int | None = typer.Option(
        None, "--checkpoint-interval", help="Save checkpoint every N entities"
    ),
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """Process the orderflow queue (fast lane first, then slow lane).

    Requires ``setup`` to have been run at least once.
    """
    _configure_logging(log_level)

    settings = _build_settings(
        storage_path,
        log_level,
        shard,
        max_concurrent,
        requests_per_second,
        max_runtime,
        checkpoint_interval,
    )

    from brreg_regnskap.sync_engine import SyncEngine

    engine = SyncEngine(settings)
    asyncio.run(engine.run())


# ── patch ─────────────────────────────────────────────────────────


@app.command()
def patch(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """Fetch BRREG updates since last ETag and add changed entities to the fast lane.

    Uses the bulk-dump ETag for conditional fetch.  If the dump has changed,
    diffs the new dump against the existing orderflow.  Also polls the
    updates API for any entity whose sisteInnsendteAarsregnskap changed.
    """
    _configure_logging(log_level)
    settings = Settings(storage_path=storage_path, log_level=log_level)

    from brreg_regnskap.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()
    asyncio.run(_patch_async(settings, storage))


async def _patch_async(settings: Settings, storage: StorageBackend) -> None:
    from datetime import UTC, datetime

    from brreg_regnskap.api.enhetsregisteret import EnhetsregisteretClient
    from brreg_regnskap.checkpoint import CheckpointManager
    from brreg_regnskap.orderflow import OrderflowManager

    orderflow = OrderflowManager(storage, settings)
    checkpoint_mgr = CheckpointManager(storage, settings.checkpoint_path)
    state = checkpoint_mgr.load()

    patch_started = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # ── Poll updates API ──────────────────────────────────────────
    console.print("Polling BRREG updates API...")
    added = 0

    async with EnhetsregisteretClient() as client:
        max_id = state.last_oppdateringsid
        entries: list[tuple[str, int]] = []
        slow_orgnrs: list[str] = []

        async for update, year in client.poll_regnskap_updates_since_date(
            _last_patch_date(storage, settings)
        ):
            orgnr = update.organisasjonsnummer
            if year is not None:
                entries.append((orgnr, int(year)))
                slow_orgnrs.append(orgnr)
            max_id = max(max_id, update.oppdateringsid)

        if entries:
            added = orderflow.enqueue_fast(entries, source="patch")
            orderflow.enqueue_slow_discovery(slow_orgnrs)

        state.last_oppdateringsid = max_id
        checkpoint_mgr.save(state)

    storage.write_bytes(
        settings.patch_cursor_path,
        json.dumps({"last_patch_date": patch_started}).encode(),
    )

    console.print(f"  Updates processed, fast-lane entries added: {added}")
    console.print("Run [bold]brreg-regnskap sync[/bold] to download.")


# ── collect ───────────────────────────────────────────────────────


@app.command()
def collect(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
    requests_per_second: float | None = typer.Option(None, "--rps"),
    max_runtime: int | None = typer.Option(None, "--max-runtime"),
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """Lean collector: walk the work list, dump downloads to holding.

    Holds no manifest. Reads the cursor, downloads the next batch of PDFs+JSON to
    the holding area, advances the cursor, stops on quota saturation or
    max-runtime. The daily coordinator drains holding into final locations.
    """
    _configure_logging(log_level)
    overrides: dict[str, object] = {"storage_path": storage_path, "log_level": log_level}
    if requests_per_second is not None:
        overrides["requests_per_second"] = requests_per_second
    if max_runtime is not None:
        overrides["max_runtime_minutes"] = max_runtime
    settings = Settings(**overrides)  # type: ignore[arg-type]

    from brreg_regnskap.collector import Collector

    stats = asyncio.run(Collector(settings).run())
    console.print(f"collect done: {stats}")


# ── coordinate ────────────────────────────────────────────────────


@app.command()
def coordinate(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """Daily coordinator: drain holding, patch updates, rebuild the work list.

    Owns the manifest (the only process that loads it fully). Folds the
    collector's holding area into final paths + manifest, polls the updates API
    for new filings, then writes pending (orgnr, year) as a flat work list and
    resets the collector cursor.
    """
    _configure_logging(log_level)
    settings = Settings(storage_path=storage_path, log_level=log_level)  # type: ignore[arg-type]

    from brreg_regnskap.coordinator import Coordinator
    from brreg_regnskap.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    coord = Coordinator(settings)
    drained = coord.drain_holding()
    asyncio.run(_patch_async(settings, storage))
    entries = coord.build_work_list()
    console.print(f"coordinate done: drained={drained} work_list={entries}")


# ── daily ─────────────────────────────────────────────────────────


@app.command()
def daily(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
    requests_per_second: float | None = typer.Option(
        None, "--rps", help="Rate limit (requests per second)"
    ),
    max_runtime: int | None = typer.Option(
        None, "--max-runtime", help="Max runtime in minutes (0=unlimited)"
    ),
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """Patch then sync the full orderflow in one process.

    Single-worker topology: processes all shards' lanes sequentially and
    writes the global manifest directly, so no merge step is needed.
    Ignores CLOUD_RUN_TASK_INDEX deliberately — on Cloud Run the env var is
    always set and would otherwise silently pin the run to shard 0.
    """
    _configure_logging(log_level)

    overrides: dict[str, object] = {
        "storage_path": storage_path,
        "log_level": log_level,
        "shard": None,
    }
    if requests_per_second is not None:
        overrides["requests_per_second"] = requests_per_second
    if max_runtime is not None:
        overrides["max_runtime_minutes"] = max_runtime
    settings = Settings(**overrides)  # type: ignore[arg-type]

    from brreg_regnskap.storage import StorageBackend
    from brreg_regnskap.sync_engine import SyncEngine

    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    lock_path = f"{settings.storage_path}/metadata/daily.lock"
    lock_max_age_s = (settings.max_runtime_minutes + 120) * 60

    if storage.exists(lock_path):
        from datetime import UTC, datetime

        held = json.loads(storage.read_bytes(lock_path))
        acquired = datetime.fromisoformat(held["acquired_at"])
        age_s = (datetime.now(UTC) - acquired).total_seconds()
        if age_s < lock_max_age_s:
            console.print(
                f"[yellow]daily.lock held by execution {held.get('execution')} "
                f"since {held['acquired_at']} ({age_s/60:.0f} min ago); exiting.[/yellow]"
            )
            return
        console.print(
            f"[yellow]daily.lock is stale ({age_s/60:.0f} min > "
            f"{lock_max_age_s/60:.0f} min limit); overriding.[/yellow]"
        )

    from datetime import UTC, datetime

    execution = os.environ.get("CLOUD_RUN_EXECUTION", "local")
    storage.write_bytes(
        lock_path,
        json.dumps(
            {"execution": execution, "acquired_at": datetime.now(UTC).isoformat()}
        ).encode(),
    )

    try:
        asyncio.run(_patch_async(settings, storage))
        engine = SyncEngine(settings)
        asyncio.run(engine.run())
    finally:
        storage.delete(lock_path)


def _last_patch_date(storage: StorageBackend, settings: Settings) -> str:
    """Determine the date to poll updates from.

    Reads metadata/patch_cursor.json (written after each successful patch),
    falling back to the bulk-dump date in etag.json, then to now.

    BRREG requires ISO format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    """
    from datetime import UTC, datetime

    if storage.exists(settings.patch_cursor_path):
        raw = storage.read_bytes(settings.patch_cursor_path)
        data = json.loads(raw)
        cursor = data.get("last_patch_date", "")
        if cursor:
            return cursor
    if storage.exists(settings.etag_path):
        raw = storage.read_bytes(settings.etag_path)
        data = json.loads(raw)
        dump_date = data.get("dump_date", "")
        if dump_date:
            # dump_date is YYYYMMDD format from setup
            try:
                dt = datetime.strptime(dump_date, "%Y%m%d").replace(tzinfo=UTC)
                return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            except ValueError:
                pass
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ── status ────────────────────────────────────────────────────────


@app.command()
def status(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
) -> None:
    """Show manifest and orderflow statistics."""
    settings = Settings(storage_path=storage_path)
    _configure_logging(settings.log_level)

    from brreg_regnskap.checkpoint import CheckpointManager
    from brreg_regnskap.manifest import ManifestManager
    from brreg_regnskap.orderflow import OrderflowManager
    from brreg_regnskap.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    manifest = ManifestManager(storage, settings.manifest_path)
    checkpoint = CheckpointManager(storage, settings.checkpoint_path)
    orderflow = OrderflowManager(storage, settings)

    table = manifest.load()
    state = checkpoint.load()

    import pyarrow.compute as pc  # type: ignore[import-untyped]

    # Manifest
    console.print(f"[bold]Manifest:[/bold] {settings.manifest_path}")
    console.print(f"  Total records: {table.num_rows}")
    if table.num_rows > 0:
        status_col = table.column("status")
        for s in ["success", "failed", "pending", "pdf_missing", "pdf_failed"]:
            count = pc.sum(pc.equal(status_col, s)).as_py()
            if count:
                console.print(f"  {s}: {count}")

    # Build manifest timestamps for orderflow anti-join
    from brreg_regnskap.sync_engine import _iso_to_unix

    manifest_ts: dict[tuple[str, int], int] = {}
    if table.num_rows > 0:
        orgnr_col = table.column("orgnr").to_pylist()
        year_col = table.column("year").to_pylist()
        status_list = table.column("status").to_pylist()
        pdf_col = table.column("pdf_path").to_pylist()
        dl_col = table.column("download_timestamp").to_pylist()
        for o, y, s, p, dl in zip(orgnr_col, year_col, status_list, pdf_col, dl_col, strict=False):
            if s == "success" and p:
                key = (o, y)
                manifest_ts[key] = max(manifest_ts.get(key, 0), _iso_to_unix(dl))

    # Orderflow per shard
    console.print("\n[bold]Orderflow:[/bold]")
    total_fast = 0
    total_slow = 0
    total_discovery = 0
    for digit in range(10):
        stats = orderflow.shard_stats(digit, manifest_ts)
        if stats["total_entries"] > 0:
            total_fast += stats["fast_lane_pending"]
            total_slow += stats["slow_lane_pending"]
            total_discovery += stats["discovery_stubs"]
    console.print(f"  Fast-lane pending: {total_fast}")
    console.print(f"  Slow-lane pending: {total_slow}")
    console.print(f"  Discovery stubs:   {total_discovery}")

    # Checkpoint
    console.print(f"\n[bold]Checkpoint:[/bold] {settings.checkpoint_path}")
    console.print(f"  Last oppdateringsid: {state.last_oppdateringsid}")
    console.print(f"  Last orgnr processed: {state.last_orgnr_processed}")
    console.print(f"  Entities processed: {state.entities_processed}")

    # ETag
    if storage.exists(settings.etag_path):
        raw = storage.read_bytes(settings.etag_path)
        etag_info = json.loads(raw)
        console.print("\n[bold]ETag:[/bold]")
        console.print(f"  Dump date: {etag_info.get('dump_date', 'unknown')}")
        console.print(f"  ETag: {etag_info.get('etag', 'none')}")


# ── verify ────────────────────────────────────────────────────────


@app.command()
def verify(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
) -> None:
    """Verify manifest entries against actual files in storage."""
    settings = Settings(storage_path=storage_path)
    _configure_logging(settings.log_level)

    from brreg_regnskap.manifest import ManifestManager
    from brreg_regnskap.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()
    manifest = ManifestManager(storage, settings.manifest_path)
    table = manifest.load()

    if table.num_rows == 0:
        console.print("[yellow]Manifest is empty -- nothing to verify.[/yellow]")
        return

    missing_json = 0
    missing_pdf = 0
    verified = 0

    json_paths = table.column("json_path").to_pylist()
    pdf_paths = table.column("pdf_path").to_pylist()
    statuses = table.column("status").to_pylist()

    for i in range(table.num_rows):
        if statuses[i] == "failed":
            continue
        jp = json_paths[i]
        if jp and not storage.exists(jp):
            missing_json += 1
        pp = pdf_paths[i]
        if pp and not storage.exists(pp):
            missing_pdf += 1
        verified += 1

    console.print(f"[bold]Verified:[/bold] {verified} records")
    console.print(f"  Missing JSON files: {missing_json}")
    console.print(f"  Missing PDF files: {missing_pdf}")
    if missing_json == 0 and missing_pdf == 0:
        console.print("[green]All manifest entries have matching files.[/green]")


# ── merge-manifests ───────────────────────────────────────────────


@app.command(name="merge-manifests")
def merge_manifests(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
) -> None:
    """Merge shard manifests from matrix jobs into the global manifest."""
    settings = Settings(storage_path=storage_path)
    _configure_logging(settings.log_level)

    from brreg_regnskap.manifest import ManifestManager
    from brreg_regnskap.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    all_files = storage.list_dir(storage_path)
    shard_paths = [
        f
        for f in all_files
        if f.rsplit("/", 1)[-1].startswith("manifest")
        and f.endswith(".parquet")
        and f != settings.manifest_path
    ]

    if not shard_paths:
        console.print("[yellow]No shard manifests found.[/yellow]")
        return

    console.print(f"Found {len(shard_paths)} shard manifest(s)")
    for sp in shard_paths:
        console.print(f"  {sp}")

    ManifestManager.merge_shards(storage, shard_paths, settings.manifest_path)

    manifest = ManifestManager(storage, settings.manifest_path)
    table = manifest.load()
    console.print(f"\n[green]Merged manifest: {table.num_rows} records[/green]")

    for sp in shard_paths:
        storage.delete(sp)
        console.print(f"  Deleted shard: {sp}")


# ── compact ────────────────────────────────────────────────────


@app.command()
def compact(
    storage_path: str = typer.Argument(..., help="Root storage path (local, s3://, or gs://)"),
    shard: str | None = typer.Option(
        None, "--shard", "-s", help="Compact only this shard digit (0-9). Default: all shards."
    ),
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """Remove completed entries from orderflow shards.

    Entries are removed when their (orgnr, year) appears in the manifest
    with a download_timestamp newer than the entry's create_time.
    Discovery stubs (year=null) are kept.
    """
    _configure_logging(log_level)
    settings = Settings(storage_path=storage_path, log_level=log_level)

    from brreg_regnskap.orderflow import OrderflowManager
    from brreg_regnskap.storage import StorageBackend
    from brreg_regnskap.sync_engine import _iso_to_unix

    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    from brreg_regnskap.manifest import ManifestManager

    manifest = ManifestManager(storage, settings.manifest_path)
    table = manifest.load()

    manifest_ts: dict[tuple[str, int], int] = {}
    if table.num_rows > 0:
        orgnr_col = table.column("orgnr").to_pylist()
        year_col = table.column("year").to_pylist()
        status_list = table.column("status").to_pylist()
        pdf_col = table.column("pdf_path").to_pylist()
        dl_col = table.column("download_timestamp").to_pylist()
        for o, y, s, p, dl in zip(orgnr_col, year_col, status_list, pdf_col, dl_col, strict=False):
            if s == "success" and p:
                key = (o, y)
                manifest_ts[key] = max(manifest_ts.get(key, 0), _iso_to_unix(dl))

    orderflow = OrderflowManager(storage, settings)
    digits = [int(shard)] if shard is not None else list(range(10))
    total_removed = 0

    for digit in digits:
        removed = orderflow.compact(digit, manifest_ts)
        if removed:
            console.print(f"  Shard {digit}: removed {removed} entries")
            total_removed += removed

    if total_removed == 0:
        console.print("[yellow]No entries to compact.[/yellow]")
    else:
        console.print(f"\n[green]Compacted {total_removed} entries total.[/green]")


if __name__ == "__main__":
    app()
