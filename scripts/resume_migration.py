"""Resume migration from Colab with 50GB RAM.

Run in Colab after:
    from google.colab import auth
    auth.authenticate_user()
    !pip install brreg-regnskap[gcs] google-cloud-storage

State as of 2026-02-24:
    - manifest: 19,846 rows (5,197 API + 14,649 migrated)
    - regnskap/ flat files: DONE (1,183)
    - temp-chunks/: DONE (854)
    - archives/: 5 of 646 zips done (chunk_10, chunk_100-103)
    - 884 orphan PDFs in bucket but not in manifest
    - Crashed mid-chunk_104.zip (3.67 GB, largest zip)

This script:
    Phase A: Recover 884 orphan PDFs into manifest
    Phase B: Resume archives from chunk_104 onward (641 zips, ~749 GB)
"""

import hashlib
import io
import re
import zipfile
from datetime import UTC, datetime

from google.cloud import storage as gcs

from brreg_regnskap.api.models import ManifestRecord
from brreg_regnskap.config import Settings
from brreg_regnskap.manifest import ManifestManager
from brreg_regnskap.storage import StorageBackend

# ── Configuration ────────────────────────────────────────────────────

SOURCE_BUCKET = "sondre_brreg_data"
TARGET_STORAGE_PATH = "gs://brreg-regnskap"
BATCH_SIZE = 500

PROCESSED_ZIPS = {"chunk_10.zip", "chunk_100.zip", "chunk_101.zip", "chunk_102.zip", "chunk_103.zip"}

PATTERN_STANDARD = re.compile(r"aarsregnskap_(\d{4})_(\d{9})\.pdf$", re.IGNORECASE)
PATTERN_KOPI = re.compile(r"KopiAvAarsregnskap_(\d{9})_(\d{4})\.pdf$", re.IGNORECASE)
PATTERN_TARGET = re.compile(r"regnskap/(\d{9})/aarsregnskap_(\d{4})(?:_v(\d+))?\.pdf$")


def parse_source_filename(filename: str) -> tuple[str, int] | None:
    basename = filename.rsplit("/", 1)[-1] if "/" in filename else filename
    m = PATTERN_STANDARD.search(basename)
    if m:
        return m.group(2), int(m.group(1))
    m = PATTERN_KOPI.search(basename)
    if m:
        return m.group(1), int(m.group(2))
    return None


def parse_target_path(path: str) -> tuple[str, int] | None:
    m = PATTERN_TARGET.search(path)
    if m:
        return m.group(1), int(m.group(2))
    return None


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ═════════════════════════════════════════════════════════════════════
# Phase A: Recover orphan PDFs into manifest
# ═════════════════════════════════════════════════════════════════════


def recover_orphans(settings: Settings, storage: StorageBackend) -> None:
    print("\n" + "=" * 60)
    print("PHASE A: RECOVER ORPHAN PDFs")
    print("=" * 60)

    manifest = ManifestManager(storage, settings.manifest_path)
    table = manifest.load()

    manifest_pdfs = set(p for p in table.column("pdf_path").to_pylist() if p)
    print(f"  Manifest PDF paths: {len(manifest_pdfs)}")

    gcs_client = gcs.Client()
    target_bucket = gcs_client.bucket("brreg-regnskap")

    orphans = []
    for blob in target_bucket.list_blobs(prefix="regnskap/"):
        if not blob.name.endswith(".pdf"):
            continue
        full_path = f"gs://brreg-regnskap/{blob.name}"
        if full_path not in manifest_pdfs:
            orphans.append(blob)

    print(f"  Orphan PDFs found: {len(orphans)}")
    if not orphans:
        return

    records = []
    skipped = 0
    for i, blob in enumerate(orphans):
        parsed = parse_target_path(blob.name)
        if parsed is None:
            skipped += 1
            continue
        orgnr, year = parsed

        pdf_data = blob.download_as_bytes()
        pdf_hash = sha256_bytes(pdf_data)

        if manifest.has_hash(orgnr, year, None, pdf_hash):
            skipped += 1
            continue

        max_v = manifest.max_version(orgnr, year)
        version = max_v + 1 if max_v > 0 else 1

        now = datetime.now(UTC).isoformat()
        records.append(
            ManifestRecord(
                orgnr=orgnr,
                year=year,
                version=version,
                download_timestamp=now,
                file_hash=None,
                pdf_hash=pdf_hash,
                json_path=None,
                pdf_path=f"gs://brreg-regnskap/{blob.name}",
                file_size_bytes=len(pdf_data),
                is_correction=version > 1,
                journalnr=None,
                source_url="recovered:orphan",
                status="success",
                error_detail=None,
            )
        )

        if len(records) >= BATCH_SIZE:
            manifest.upsert(records)
            print(f"  Flushed {len(records)} records ({i+1}/{len(orphans)} orphans processed)")
            records = []

    if records:
        manifest.upsert(records)

    print(f"  Recovered: {len(orphans) - skipped}, Skipped (dup/unparseable): {skipped}")


# ═════════════════════════════════════════════════════════════════════
# Phase B: Resume archive migration
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
            "zips_processed": 0,
            "zips_skipped": 0,
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

        if self.stats["uploaded"] % 2000 == 0:
            print(f"    Progress: {self.stats}")

    def run(self) -> None:
        print("\n" + "=" * 60)
        print("PHASE B: RESUME ARCHIVE MIGRATION")
        print("=" * 60)

        self._load_existing()

        archive_blobs = sorted(
            [b for b in self.source_bucket.list_blobs(prefix="archives/") if b.name.endswith(".zip")],
            key=lambda b: b.name,
        )
        print(f"  Total archive zips: {len(archive_blobs)}")

        for i, blob in enumerate(archive_blobs):
            zip_name = blob.name.split("archives/")[1]

            if zip_name in PROCESSED_ZIPS:
                self.stats["zips_skipped"] += 1
                continue

            print(f"\n  [{i+1}/{len(archive_blobs)}] {blob.name} ({blob.size / 1e9:.2f} GB)")
            zip_bytes = blob.download_as_bytes()
            buf = io.BytesIO(zip_bytes)

            try:
                with zipfile.ZipFile(buf) as zf:
                    pdf_names = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
                    for pdf_name in pdf_names:
                        parsed = parse_source_filename(pdf_name)
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
            self.stats["zips_processed"] += 1
            del zip_bytes, buf

            PROCESSED_ZIPS.add(zip_name)
            print(f"    Done. Stats: uploaded={self.stats['uploaded']}, dupes={self.stats['skipped_duplicate']}, zips={self.stats['zips_processed']}")

        self._flush_records(force=True)

        table = self.manifest.load()
        print("\n" + "-" * 40)
        print(f"  Final stats: {self.stats}")
        print(f"  Manifest rows: {table.num_rows}")
        print(f"  Unique (orgnr, year): {len(self.seen)}")


# ═════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════


def main() -> None:
    print("=" * 60)
    print("brreg-regnskap: resume migration (Colab)")
    print(f"  Source: gs://{SOURCE_BUCKET}")
    print(f"  Target: {TARGET_STORAGE_PATH}")
    print("=" * 60)

    settings = Settings(storage_path=TARGET_STORAGE_PATH)
    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    # Phase A: Recover orphans
    recover_orphans(settings, storage)

    # Phase B: Resume archives
    migrator = Migrator(settings, storage)
    migrator.run()

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)
    print("\nNext steps:")
    print("  brreg-regnskap status gs://brreg-regnskap")
    print("  brreg-regnskap sync gs://brreg-regnskap")


if __name__ == "__main__":
    main()
