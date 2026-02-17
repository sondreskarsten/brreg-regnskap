"""CLI interface for brreg-regnskap.

Commands:
    sync        Run a full or incremental sync
    status      Show manifest statistics and checkpoint state
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

import structlog
import typer
from rich.console import Console

from brreg_regnskap.config import Settings, SyncMode

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


@app.command()
def sync(
    storage_path: str = typer.Argument(
        ..., help="Root storage path (local, s3://, or gs://)"
    ),
    mode: SyncMode = typer.Option(
        SyncMode.FULL, "--mode", "-m", help="Sync mode: full or incremental"
    ),
    max_concurrent: int = typer.Option(50, "--max-concurrent", "-c"),
    requests_per_second: float = typer.Option(10.0, "--rps"),
    max_runtime: int = typer.Option(
        0, "--max-runtime", help="Max runtime in minutes (0=unlimited)"
    ),
    range_start: str | None = typer.Option(
        None, "--range-start", help="Start of orgnr range (for matrix jobs)"
    ),
    range_end: str | None = typer.Option(
        None, "--range-end", help="End of orgnr range (for matrix jobs)"
    ),
    shard: str | None = typer.Option(
        None, "--shard", "-s",
        help="Shard digit 0-9, or 'auto' to claim a free shard"
    ),
    checkpoint_interval: int = typer.Option(1000, "--checkpoint-interval"),
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """Run a full or incremental sync of BRREG regnskap data."""
    _configure_logging(log_level)

    shard_digit: int | None = None
    if shard is not None:
        if shard == "auto":
            from brreg_regnskap.shard import claim_shard
            from brreg_regnskap.storage import StorageBackend

            tmp_settings = Settings(storage_path=storage_path)
            tmp_storage = StorageBackend.from_settings(tmp_settings)
            tmp_storage.check_credentials()
            shard_digit = claim_shard(tmp_storage, storage_path.rstrip("/"))
        else:
            shard_digit = int(shard)

    settings = Settings(
        storage_path=storage_path,
        max_concurrent=max_concurrent,
        requests_per_second=requests_per_second,
        max_runtime_minutes=max_runtime,
        checkpoint_interval=checkpoint_interval,
        log_level=log_level,
        orgnr_range_start=range_start,
        orgnr_range_end=range_end,
        shard=shard_digit,
    )

    from brreg_regnskap.downloader import SyncEngine

    engine = SyncEngine(settings)
    asyncio.run(engine.run(mode=mode))


@app.command()
def status(
    storage_path: str = typer.Argument(
        ..., help="Root storage path (local, s3://, or gs://)"
    ),
) -> None:
    """Show manifest statistics and checkpoint state."""
    settings = Settings(storage_path=storage_path)
    _configure_logging(settings.log_level)

    from brreg_regnskap.checkpoint import CheckpointManager
    from brreg_regnskap.manifest import ManifestManager
    from brreg_regnskap.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    manifest = ManifestManager(storage, settings.manifest_path)
    checkpoint = CheckpointManager(storage, settings.checkpoint_path)

    table = manifest.load()
    state = checkpoint.load()

    console.print(f"[bold]Manifest:[/bold] {settings.manifest_path}")
    console.print(f"  Total records: {table.num_rows}")
    if table.num_rows > 0:
        status_col = table.column("status")
        for s in ["success", "failed", "pending", "pdf_missing"]:
            import pyarrow.compute as pc

            count = pc.sum(pc.equal(status_col, s)).as_py()
            if count:
                console.print(f"  {s}: {count}")

    console.print(f"\n[bold]Checkpoint:[/bold] {settings.checkpoint_path}")
    console.print(f"  Mode: {state.mode}")
    console.print(f"  Last oppdateringsid: {state.last_oppdateringsid}")
    console.print(f"  Last orgnr processed: {state.last_orgnr_processed}")
    console.print(f"  Entities processed: {state.entities_processed}")
    console.print(f"  Errors: {state.errors}")


@app.command()
def verify(
    storage_path: str = typer.Argument(
        ..., help="Root storage path (local, s3://, or gs://)"
    ),
) -> None:
    """Verify manifest entries against actual files in storage.

    Reports orphaned files (in storage but not manifest) and missing files
    (in manifest but not storage).
    """
    settings = Settings(storage_path=storage_path)
    _configure_logging(settings.log_level)

    from brreg_regnskap.manifest import ManifestManager
    from brreg_regnskap.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()
    manifest = ManifestManager(storage, settings.manifest_path)
    table = manifest.load()

    if table.num_rows == 0:
        console.print("[yellow]Manifest is empty — nothing to verify.[/yellow]")
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


@app.command(name="merge-manifests")
def merge_manifests(
    storage_path: str = typer.Argument(
        ..., help="Root storage path (local, s3://, or gs://)"
    ),
) -> None:
    """Merge shard manifests from matrix jobs into the global manifest.

    Finds all manifest-*.parquet files in the storage root, merges them
    with the existing global manifest, deduplicates by (orgnr, year),
    and writes the result.
    """
    settings = Settings(storage_path=storage_path)
    _configure_logging(settings.log_level)

    from brreg_regnskap.manifest import ManifestManager
    from brreg_regnskap.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    all_files = storage.list_dir(storage_path)
    shard_paths = [
        f for f in all_files
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


if __name__ == "__main__":
    app()
