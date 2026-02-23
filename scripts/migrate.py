#!/usr/bin/env python3
"""Combined setup + migration: initialise gs://brreg-regnskap and migrate old PDFs.

Run from Google Cloud Shell with:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
    python migrate.py

Prerequisites:
    pip install 'brreg-regnskap[gcs]' google-cloud-storage

What this does (in order):
    Phase 1 — Setup (equivalent to `brreg-regnskap setup gs://brreg-regnskap`)
        - Downloads BRREG bulk entity dump (~200 MB gz)
        - Saves to gs://brreg-regnskap/entities/enheter_dump_{date}.json.gz
        - Saves ETag to gs://brreg-regnskap/metadata/etag.json
        - Parses entities with sisteInnsendteAarsregnskap
        - Seeds orderflow fast lane: (orgnr, latest_year) per entity
        - Seeds orderflow slow lane: discovery stubs (year=null) per orgnr

    Phase 2 — Migration
        - Reads PDFs from gs://sondre_brreg_data (regnskap/, temp-chunks/, archives/)
        - Writes to gs://brreg-regnskap/regnskap/{orgnr}/aarsregnskap_{year}.pdf
        - Creates manifest.parquet with status="success" for all entries
        - Deduplicates by (orgnr, year) + pdf_hash across all sources
"""

import asyncio
import hashlib
import io
import json
import re
import zipfile
from datetime import UTC, datetime

from google.cloud import storage as gcs

from brreg_regnskap.api.enhetsregisteret import EnhetsregisteretClient
from brreg_regnskap.api.models import ManifestRecord
from brreg_regnskap.config import Settings
from brreg_regnskap.manifest import ManifestManager
from brreg_regnskap.orderflow import OrderflowManager
from brreg_regnskap.storage import StorageBackend

# ── Configuration ────────────────────────────────────────────────────

SOURCE_BUCKET = "sondre_brreg_data"
TARGET_STORAGE_PATH = "gs://brreg-regnskap"
BATCH_SIZE = 500

PATTERN_STANDARD = re.compile(r"aarsregnskap_(\d{4})_(\d{9})\.pdf$", re.IGNORECASE)
PATTERN_KOPI = re.compile(r"KopiAvAarsregnskap_(\d{9})_(\d{4})\.pdf$", re.IGNORECASE)


def parse_filename(filename: str) -> tuple[str, int] | None:
    basename = filename.rsplit("/", 1)[-1] if "/" in filename else filename
    m = PATTERN_STANDARD.search(basename)
    if m:
        return m.group(2), int(m.group(1))
    m = PATTERN_KOPI.search(basename)
    if m:
        return m.group(1), int(m.group(2))
    return None


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ═════════════════════════════════════════════════════════════════════
# Phase 1: Setup
# ═════════════════════════════════════════════════════════════════════


async def run_setup(settings: Settings, storage: StorageBackend) -> None:
    """Equivalent to `brreg-regnskap setup gs://brreg-regnskap`."""
    print("\n" + "=" * 60)
    print("PHASE 1: SETUP")
    print("=" * 60)

    orderflow = OrderflowManager(storage, settings)

    # ── Download bulk dump ────────────────────────────────────────
    print("\nDownloading bulk entity dump from BRREG...")
    async with EnhetsregisteretClient() as client:
        raw_dump, etag = await client.download_bulk_dump()

    if raw_dump is None:
        raise RuntimeError("Bulk dump download returned no data")

    # Save dump
    dump_date = datetime.now(UTC).strftime("%Y%m%d")
    dump_path = settings.entity_dump_path(dump_date)
    storage.write_bytes(dump_path, raw_dump)
    print(f"  Saved dump: {dump_path} ({len(raw_dump) / 1e6:.1f} MB)")

    # Save ETag
    etag_data = json.dumps({"etag": etag, "dump_date": dump_date}).encode()
    storage.write_bytes(settings.etag_path, etag_data)
    print(f"  Saved ETag: {etag}")

    # ── Parse entities ────────────────────────────────────────────
    print("\nParsing entities...")
    entities = EnhetsregisteretClient().iter_entities_from_dump(raw_dump)
    print(f"  Entities with sisteInnsendteAarsregnskap: {len(entities)}")

    # ── Seed orderflow ────────────────────────────────────────────
    print("\nSeeding orderflow...")
    fast_entries: list[tuple[str, int]] = []
    slow_orgnrs: list[str] = []

    for e in entities:
        orgnr = e.organisasjonsnummer
        year = int(e.sisteInnsendteAarsregnskap)  # type: ignore[arg-type]
        fast_entries.append((orgnr, year))
        slow_orgnrs.append(orgnr)

    added_fast = orderflow.enqueue_fast(fast_entries, source="bulk_dump")
    added_slow = orderflow.enqueue_slow_discovery(slow_orgnrs)

    print(f"  Fast-lane entries: {added_fast}")
    print(f"  Slow-lane discovery stubs: {added_slow}")
    print("\nSetup complete.")


# ═════════════════════════════════════════════════════════════════════
# Phase 2: Migration
# ═════════════════════════════════════════════════════════════════════


class Migrator:
    def __init__(self, settings: Settings, storage: StorageBackend) -> None:
        self.settings = settings
        self.backend = storage
        self.manifest = ManifestManager(self.backend, self.settings.manifest_path)
        self.gcs_client = gcs.Client()
        self.source_bucket = self.gcs_client.bucket(SOURCE_BUCKET)

        self.seen: dict[tuple[str, int], str] = {}
        self.records_buf: list[ManifestRecord] = []
        self.stats = {
            "uploaded": 0,
            "skipped_duplicate": 0,
            "skipped_parse_fail": 0,
            "corrections": 0,
        }

    def _load_existing(self) -> None:
        table = self.manifest.load()
        if table.num_rows == 0:
            return
        orgnr_col = table.column("orgnr").to_pylist()
        year_col = table.column("year").to_pylist()
        hash_col = table.column("pdf_hash").to_pylist()
        for o, y, h in zip(orgnr_col, year_col, hash_col):
            if h:
                self.seen[(o, y)] = h
        print(f"  Loaded {len(self.seen)} existing entries from manifest")

    def _flush_records(self, force: bool = False) -> None:
        if not self.records_buf:
            return
        if not force and len(self.records_buf) < BATCH_SIZE:
            return
        self.manifest.upsert(self.records_buf)
        self.records_buf = []

    def _process_pdf(self, pdf_data: bytes, orgnr: str, year: int, source_desc: str) -> None:
        pdf_hash = sha256_bytes(pdf_data)
        key = (orgnr, year)

        if key in self.seen:
            if self.seen[key] == pdf_hash:
                self.stats["skipped_duplicate"] += 1
                return
            version = self.manifest.max_version(orgnr, year) + 1
            if version <= 1:
                version = 2
            self.stats["corrections"] += 1
        else:
            version = 1

        pdf_path = self.settings.regnskap_pdf_path(orgnr, year, version)
        self.backend.write_bytes(pdf_path, pdf_data)
        self.seen[key] = pdf_hash

        now = datetime.now(UTC).isoformat()
        self.records_buf.append(
            ManifestRecord(
                orgnr=orgnr,
                year=year,
                version=version,
                download_timestamp=now,
                file_hash=None,
                pdf_hash=pdf_hash,
                json_path=None,
                pdf_path=pdf_path,
                file_size_bytes=len(pdf_data),
                is_correction=version > 1,
                journalnr=None,
                source_url=f"migrated:{source_desc}",
                status="success",
                error_detail=None,
            )
        )
        self.stats["uploaded"] += 1
        self._flush_records()

        if self.stats["uploaded"] % 1000 == 0:
            print(f"  Progress: {self.stats}")

    def migrate_flat_files(self) -> None:
        print("\n── Phase 2a: regnskap/ flat files ──")
        blobs = [b for b in self.source_bucket.list_blobs(prefix="regnskap/") if b.name.endswith(".pdf")]
        print(f"  Found {len(blobs)} PDFs")

        for blob in blobs:
            parsed = parse_filename(blob.name)
            if parsed is None:
                self.stats["skipped_parse_fail"] += 1
                continue
            orgnr, year = parsed
            pdf_data = blob.download_as_bytes()
            self._process_pdf(pdf_data, orgnr, year, f"gs://{SOURCE_BUCKET}/{blob.name}")

        self._flush_records(force=True)
        print(f"  Phase 2a done: {self.stats}")

    def migrate_temp_chunks(self) -> None:
        print("\n── Phase 2b: temp-chunks/ + temp_chunks/ ──")
        for prefix in ("temp-chunks/", "temp_chunks/"):
            blobs = [b for b in self.source_bucket.list_blobs(prefix=prefix) if b.name.endswith(".pdf")]
            print(f"  {prefix}: {len(blobs)} PDFs")
            for blob in blobs:
                parsed = parse_filename(blob.name)
                if parsed is None:
                    self.stats["skipped_parse_fail"] += 1
                    continue
                orgnr, year = parsed
                pdf_data = blob.download_as_bytes()
                self._process_pdf(pdf_data, orgnr, year, f"gs://{SOURCE_BUCKET}/{blob.name}")

        self._flush_records(force=True)
        print(f"  Phase 2b done: {self.stats}")

    def migrate_archives(self) -> None:
        print("\n── Phase 2c: archives/ (646 zips, ~703 GB) ──")
        archive_blobs = sorted(
            [b for b in self.source_bucket.list_blobs(prefix="archives/") if b.name.endswith(".zip")],
            key=lambda b: b.name,
        )
        print(f"  Found {len(archive_blobs)} zip archives")

        for i, blob in enumerate(archive_blobs):
            print(f"\n  [{i+1}/{len(archive_blobs)}] {blob.name} ({blob.size / 1e9:.2f} GB)")
            zip_bytes = blob.download_as_bytes()
            buf = io.BytesIO(zip_bytes)

            try:
                with zipfile.ZipFile(buf) as zf:
                    pdf_names = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
                    for pdf_name in pdf_names:
                        parsed = parse_filename(pdf_name)
                        if parsed is None:
                            self.stats["skipped_parse_fail"] += 1
                            continue
                        orgnr, year = parsed
                        pdf_data = zf.read(pdf_name)
                        self._process_pdf(
                            pdf_data, orgnr, year,
                            f"gs://{SOURCE_BUCKET}/{blob.name}!{pdf_name}",
                        )
            except zipfile.BadZipFile:
                print(f"    WARN: Bad zip file: {blob.name}")
                continue

            self._flush_records(force=True)
            del zip_bytes, buf

        self._flush_records(force=True)
        print(f"\n  Phase 2c done: {self.stats}")

    def run(self) -> None:
        print("\n" + "=" * 60)
        print("PHASE 2: MIGRATION")
        print("=" * 60)

        self._load_existing()

        self.migrate_flat_files()
        self.migrate_temp_chunks()
        self.migrate_archives()

        self._flush_records(force=True)

        table = self.manifest.load()
        print("\n" + "-" * 40)
        print(f"  Stats: {self.stats}")
        print(f"  Manifest rows: {table.num_rows}")
        print(f"  Unique (orgnr, year): {len(self.seen)}")


# ═════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════


def merge_existing_shards(settings: Settings, storage: StorageBackend) -> None:
    """Phase 0: merge any existing shard manifests into global manifest.

    Shards 5/6/7 already exist from a prior GHA matrix run (5,197 rows).
    Must merge before migration so _load_existing() sees them.
    """
    print("\n" + "=" * 60)
    print("PHASE 0: MERGE EXISTING SHARD MANIFESTS")
    print("=" * 60)

    shard_paths = []
    for s in range(10):
        path = f"{TARGET_STORAGE_PATH}/manifest_shard_{s}.parquet"
        if storage.exists(path):
            shard_paths.append(path)
            print(f"  Found: {path}")

    if not shard_paths:
        print("  No existing shard manifests — skipping merge.")
        return

    ManifestManager.merge_shards(storage, shard_paths, settings.manifest_path)

    manifest = ManifestManager(storage, settings.manifest_path)
    table = manifest.load()
    print(f"  Merged into global manifest: {table.num_rows} rows")

    for sp in shard_paths:
        storage.delete(sp)
        print(f"  Deleted shard: {sp}")


def main() -> None:
    print("=" * 60)
    print("brreg-regnskap: setup + migration")
    print(f"  Source: gs://{SOURCE_BUCKET}")
    print(f"  Target: {TARGET_STORAGE_PATH}")
    print("=" * 60)

    settings = Settings(storage_path=TARGET_STORAGE_PATH)
    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    # Phase 0: Merge existing shard manifests
    merge_existing_shards(settings, storage)

    # Phase 1: Setup
    asyncio.run(run_setup(settings, storage))

    # Phase 2: Migration
    migrator = Migrator(settings, storage)
    migrator.run()

    print("\n" + "=" * 60)
    print("ALL DONE")
    print("=" * 60)
    print("\nNext steps:")
    print("  brreg-regnskap status gs://brreg-regnskap")
    print("  brreg-regnskap verify gs://brreg-regnskap")
    print("  brreg-regnskap sync gs://brreg-regnskap    # downloads JSON, new filings")


if __name__ == "__main__":
    main()
