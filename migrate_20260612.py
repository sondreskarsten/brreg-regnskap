"""One-time restructure migration, 2026-06-12.

1. Archive the 55-column merged manifest (enrichment columns all null;
   real extraction state lives under extraction/store/ and notes/).
2. Rewrite manifest.parquet as the 14-column MANIFEST_SCHEMA via
   ManifestManager.load()/save() so the file matches exactly what the
   sync engine reads and writes.
3. Seed metadata/patch_cursor.json at 2026-04-20 (last patch ran 2026-04-24;
   margin covers in-flight updates).
4. Archive the 10 per-shard checkpoints (single-worker topology uses the
   global checkpoint.json) and delete the originals.
"""

import json

from brreg_regnskap.config import Settings
from brreg_regnskap.manifest import ManifestManager
from brreg_regnskap.storage import StorageBackend

ROOT = "gs://brreg-regnskap"
ARCHIVE_MANIFEST = f"{ROOT}/metadata/manifest_pre_restructure_20260612.parquet"
CURSOR_SEED = "2026-04-20T00:00:00.000Z"


def main() -> None:
    settings = Settings(storage_path=ROOT)
    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()

    raw = storage.read_bytes(settings.manifest_path)
    storage.write_bytes(ARCHIVE_MANIFEST, raw)
    print(f"archived {len(raw):,} bytes -> {ARCHIVE_MANIFEST}")

    manifest = ManifestManager(storage, settings.manifest_path)
    table = manifest.load()
    manifest.save(table)
    print(f"rewrote manifest.parquet: {table.num_rows:,} rows, {table.num_columns} cols")

    storage.write_bytes(
        settings.patch_cursor_path,
        json.dumps({"last_patch_date": CURSOR_SEED}).encode(),
    )
    print(f"seeded patch cursor: {CURSOR_SEED}")

    for i in range(10):
        src = f"{ROOT}/checkpoint_shard_{i}.json"
        dst = f"{ROOT}/metadata/checkpoints_pre_restructure/checkpoint_shard_{i}.json"
        if storage.exists(src):
            storage.write_bytes(dst, storage.read_bytes(src))
            storage.delete(src)
            print(f"archived+removed checkpoint_shard_{i}.json")


main()
