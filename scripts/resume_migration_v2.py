"""Resume migration — parallelized GCS uploads.

Run in Colab after:
    from google.colab import auth
    auth.authenticate_user()
    !pip install 'git+https://<PAT>@github.com/sondreskarsten/brreg-regnskap.git#egg=brreg-regnskap[gcs]' google-cloud-storage -q

Bottleneck in v1: sequential write_bytes (~200ms each) + manifest upsert every 500 rows.
Fix: ThreadPoolExecutor for GCS uploads + single manifest upsert per zip.
"""

import hashlib
import io
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

from google.cloud import storage as gcs

from brreg_regnskap.api.models import ManifestRecord
from brreg_regnskap.config import Settings
from brreg_regnskap.manifest import ManifestManager
from brreg_regnskap.storage import StorageBackend

# ── Configuration ────────────────────────────────────────────────────

SOURCE_BUCKET = "sondre_brreg_data"
TARGET_STORAGE_PATH = "gs://brreg-regnskap"
UPLOAD_WORKERS = 64

PATTERN_STANDARD = re.compile(r"aarsregnskap_(\d{4})_(\d{9})\.pdf$", re.IGNORECASE)
PATTERN_KOPI = re.compile(r"KopiAvAarsregnskap_(\d{9})_(\d{4})\.pdf$", re.IGNORECASE)


def parse_source_filename(filename: str) -> tuple[str, int] | None:
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


def _upload_one(target_bucket: gcs.Bucket, gcs_path: str, pdf_data: bytes) -> None:
    blob = target_bucket.blob(gcs_path)
    blob.upload_from_file(io.BytesIO(pdf_data), content_type="application/pdf")


class Migrator:
    def __init__(self, settings: Settings, storage: StorageBackend) -> None:
        self.settings = settings
        self.backend = storage
        self.manifest = ManifestManager(self.backend, self.settings.manifest_path)
        self.gcs_client = gcs.Client()
        self.source_bucket = self.gcs_client.bucket(SOURCE_BUCKET)
        self.target_bucket = self.gcs_client.bucket("brreg-regnskap")

        self.seen: dict[tuple[str, int], str] = {}
        self.processed_zips: set[str] = set()
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
        source_col = table.column("source_url").to_pylist()

        for o, y, h, s in zip(orgnr_col, year_col, hash_col, source_col):
            if h:
                self.seen[(o, y)] = h
            if s and "archives/" in s:
                zip_name = s.split("archives/")[1].split("!")[0]
                self.processed_zips.add(zip_name)

        print(f"  Loaded {len(self.seen)} existing entries from manifest")
        print(f"  Already-processed archive zips: {len(self.processed_zips)}")

    def _process_zip(self, blob: gcs.Blob, zip_index: int, total_zips: int) -> None:
        zip_name = blob.name.split("archives/")[1]

        if zip_name in self.processed_zips:
            self.stats["zips_skipped"] += 1
            return

        t0 = time.time()
        print(f"\n  [{zip_index}/{total_zips}] {blob.name} ({blob.size / 1e9:.2f} GB)")

        # Step 1: Download zip
        t_dl = time.time()
        zip_bytes = blob.download_as_bytes()
        buf = io.BytesIO(zip_bytes)
        print(f"    Download: {time.time() - t_dl:.1f}s")

        # Step 2: Extract + hash + dedup (sequential, CPU-bound)
        t_extract = time.time()
        to_upload: list[tuple[str, str, int, int, bytes, str]] = []  # (orgnr, year, version, size, data, hash)
        parse_fail = 0

        try:
            with zipfile.ZipFile(buf) as zf:
                pdf_names = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
                for pdf_name in pdf_names:
                    parsed = parse_source_filename(pdf_name)
                    if parsed is None:
                        parse_fail += 1
                        continue
                    orgnr, year = parsed
                    pdf_data = zf.read(pdf_name)
                    pdf_hash = sha256_bytes(pdf_data)
                    key = (orgnr, year)

                    if key in self.seen and self.seen[key] == pdf_hash:
                        self.stats["skipped_duplicate"] += 1
                        continue

                    if key in self.seen:
                        version = 2
                        self.stats["corrections"] += 1
                    else:
                        version = 1

                    self.seen[key] = pdf_hash
                    to_upload.append((orgnr, year, version, len(pdf_data), pdf_data, pdf_hash))
        except zipfile.BadZipFile:
            print(f"    WARN: Bad zip file")
            return

        del zip_bytes, buf
        self.stats["skipped_parse_fail"] += parse_fail
        print(f"    Extract+hash: {time.time() - t_extract:.1f}s ({len(to_upload)} to upload, {parse_fail} parse fails)")

        if not to_upload:
            self.stats["zips_processed"] += 1
            self.processed_zips.add(zip_name)
            return

        # Step 3: Parallel upload to GCS
        t_upload = time.time()
        records: list[ManifestRecord] = []
        now = datetime.now(UTC).isoformat()
        upload_errors = 0

        with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as pool:
            futures = {}
            for orgnr, year, version, size, pdf_data, pdf_hash in to_upload:
                pdf_path = self.settings.regnskap_pdf_path(orgnr, year, version)
                gcs_path = pdf_path.replace("gs://brreg-regnskap/", "")

                future = pool.submit(_upload_one, self.target_bucket, gcs_path, pdf_data)
                futures[future] = (orgnr, year, version, size, pdf_hash, pdf_path)

            for future in as_completed(futures):
                orgnr, year, version, size, pdf_hash, pdf_path = futures[future]
                try:
                    future.result()
                    records.append(
                        ManifestRecord(
                            orgnr=orgnr,
                            year=year,
                            version=version,
                            download_timestamp=now,
                            file_hash=None,
                            pdf_hash=pdf_hash,
                            json_path=None,
                            pdf_path=pdf_path,
                            file_size_bytes=size,
                            is_correction=version > 1,
                            journalnr=None,
                            source_url=f"migrated:gs://{SOURCE_BUCKET}/{blob.name}",
                            status="success",
                            error_detail=None,
                        )
                    )
                except Exception as e:
                    upload_errors += 1
                    if upload_errors <= 3:
                        print(f"    Upload error: {e}")

        print(f"    Upload: {time.time() - t_upload:.1f}s ({len(records)} files, {UPLOAD_WORKERS} workers, {upload_errors} errors)")

        # Step 4: Single manifest upsert per zip
        t_manifest = time.time()
        if records:
            self.manifest.upsert(records)
        print(f"    Manifest upsert: {time.time() - t_manifest:.1f}s")

        self.stats["uploaded"] += len(records)
        self.stats["zips_processed"] += 1
        self.processed_zips.add(zip_name)

        total_time = time.time() - t0
        print(f"    Total: {total_time:.1f}s | Cumulative: uploaded={self.stats['uploaded']}, zips={self.stats['zips_processed']}")

    def run(self) -> None:
        print("\n" + "=" * 60)
        print("PHASE B: RESUME ARCHIVE MIGRATION (PARALLEL)")
        print("=" * 60)

        self._load_existing()

        archive_blobs = sorted(
            [b for b in self.source_bucket.list_blobs(prefix="archives/") if b.name.endswith(".zip")],
            key=lambda b: b.name,
        )
        print(f"  Total archive zips: {len(archive_blobs)}")
        remaining = sum(1 for b in archive_blobs if b.name.split("archives/")[1] not in self.processed_zips)
        print(f"  Remaining: {remaining}")

        for i, blob in enumerate(archive_blobs):
            self._process_zip(blob, i + 1, len(archive_blobs))

        table = self.manifest.load()
        print("\n" + "-" * 40)
        print(f"  Final stats: {self.stats}")
        print(f"  Manifest rows: {table.num_rows}")
        print(f"  Unique (orgnr, year): {len(self.seen)}")


def main() -> None:
    print("=" * 60)
    print("brreg-regnskap: resume migration v2 (parallel)")
    print(f"  Source: gs://{SOURCE_BUCKET}")
    print(f"  Target: {TARGET_STORAGE_PATH}")
    print(f"  Upload workers: {UPLOAD_WORKERS}")
    print("=" * 60)

    settings = Settings(storage_path=TARGET_STORAGE_PATH)
    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    migrator = Migrator(settings, storage)
    migrator.run()

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)


if __name__ == "__main__":
    main()
