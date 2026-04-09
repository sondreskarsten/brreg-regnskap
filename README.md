# brreg-regnskap

Bulk mirror of Norwegian annual financial statements (årsregnskap) from [Brønnøysundregistrene](https://www.brreg.no/) (BRREG).

Downloads structured JSON financial data and PDF annual reports for all Norwegian companies, storing them in S3, GCS, or local filesystem. Tracks state via a Parquet manifest for incremental updates and correction detection.

## Installation

```bash
# Using uv (recommended)
uv add brreg-regnskap

# With S3 support
uv add "brreg-regnskap[s3]"

# With GCS support
uv add "brreg-regnskap[gcs]"

# Development
git clone <repo-url>
cd brreg-regnskap
uv sync --all-extras
```

## Usage

```bash
# 1. One-time setup: download bulk dump, seed orderflow
brreg-regnskap setup ./data

# 2. Process the orderflow queue (fast lane then slow lane)
brreg-regnskap sync ./data

# 3. Fetch BRREG updates and add to fast lane
brreg-regnskap patch ./data

# Check status
brreg-regnskap status ./data

# Compact completed entries from orderflow
brreg-regnskap compact ./data

# Verify manifest files exist on disk
brreg-regnskap verify ./data

# Merge shard manifests after parallel GitHub Actions run
brreg-regnskap merge-manifests ./data
```

### Commands

**`setup STORAGE_PATH`** — One-time initialisation. Downloads the BRREG bulk entity dump, saves the ETag, and seeds the orderflow with fast-lane entries for every entity that has a `sisteInnsendteAarsregnskap`. Also creates slow-lane discovery stubs for historical year back-fill.

**`sync STORAGE_PATH [OPTIONS]`** — Process the orderflow queue. Fast lane (JSON+PDF) is processed first across all shards, then slow lane (PDF only) when the fast lane is empty.

```
  --shard, -s           Shard digit 0-9, or 'auto' to claim a free shard
  --max-concurrent, -c  Max simultaneous HTTP connections (default: 5)
  --rps                 Max requests per second (default: 3.0)
  --max-runtime         Max runtime in minutes, 0=unlimited (default: 0)
  --checkpoint-interval Save state every N entities (default: 1000)
  --log-level, -l       DEBUG | INFO | WARNING | ERROR (default: INFO)
```

**`patch STORAGE_PATH`** — Fetch BRREG updates since the last run and add changed entities to the fast lane.

**`compact STORAGE_PATH`** — Remove completed entries from orderflow shards. Entries are removed when their `(orgnr, year)` has been successfully downloaded since the entry was created.

**`status STORAGE_PATH`** — Show manifest and orderflow statistics.

**`verify STORAGE_PATH`** — Check that all files referenced in the manifest actually exist in storage.

**`merge-manifests STORAGE_PATH`** — Merge shard manifests from parallel workers into the global manifest.

## Configuration

All settings can be set via environment variables with the `BRREG_` prefix.

| Variable | Default | Description |
|----------|---------|-------------|
| `BRREG_STORAGE_PATH` | `./data` | Root storage path (local, `s3://`, or `gs://`) |
| `BRREG_MAX_CONCURRENT` | `5` | Max simultaneous HTTP connections |
| `BRREG_REQUESTS_PER_SECOND` | `3.0` | Rate limit for BRREG API calls |
| `BRREG_MAX_RETRIES` | `5` | Max retry attempts per failed request |
| `BRREG_CHECKPOINT_INTERVAL` | `1000` | Save checkpoint every N entities |
| `BRREG_MAX_RUNTIME_MINUTES` | `0` | Graceful shutdown timer (0=unlimited) |
| `BRREG_LOG_LEVEL` | `INFO` | Log verbosity |
| `BRREG_SHARD` | — | Shard digit 0-9 (for parallel workers) |

## Architecture

```
CLI (typer)
  → setup     — downloads bulk dump, seeds orderflow
  → patch     — polls BRREG updates API, adds to fast lane
  → sync      — processes orderflow via SyncEngine
    → OrderflowManager      — two-lane parquet work queue (10 shards)
    → RegnskapsregisteretClient  — fetches JSON + PDF per entity
    → StorageBackend (fsspec)    — writes to S3/GCS/local
    → ManifestManager            — Parquet manifest tracking downloads
    → CheckpointManager          — resume state between runs
```

### Two-Lane Orderflow

The orderflow is a parquet-based work queue partitioned into 10 shards by `orgnr % 10`:

- **Fast lane**: `(orgnr, year)` pairs from the bulk dump or BRREG update patches. Priority = now. Downloads JSON + PDF. Processed first.
- **Slow lane**: Historical years discovered via the `/aar` API. Priority = `unix(year-01-01)`. Downloads PDF only. Processed when all fast lanes are empty.

### Storage Layout

```
{storage_path}/
├── manifest.parquet                    # source of truth for downloads
├── checkpoint.json                     # sync cursor state
├── orderflow/
│   └── shard_{0-9}.parquet             # two-lane work queue
├── metadata/
│   └── etag.json                       # bulk dump ETag
├── entities/
│   └── enheter_dump_{date}.json.gz     # cached bulk dumps
└── regnskap/
    └── {orgnr}/
        ├── regnskap_{year}.json        # JSON financial data
        ├── aarsregnskap_{year}.pdf     # PDF annual report
        ├── regnskap_{year}_v2.json     # correction (version 2+)
        └── aarsregnskap_{year}_v2.pdf  # correction (version 2+)
```

### Correction Handling

When BRREG receives a corrected regnskap, the `(orgnr, year)` re-enters the fast lane via `enqueue_fast` (upsert). The sync engine re-downloads and compares hashes. If the content differs, a new version is saved (`_v2`, `_v3`, etc.) and the manifest records `is_correction=True`.

## GitHub Actions

The included workflows support automated syncing:

- **ci.yml** — Lint + type check + test on push/PR
- **sync.yml** — Supports `setup`, `patch`, and `sync` commands. Sync runs in parallel across 10 shards.
- **gcs-sync.yml** — GCS bucket mirroring via `gsutil rsync`

### Setup

The workflow auto-detects the provider from `BRREG_STORAGE_PATH`: `s3://` uses AWS, `gs://` uses GCS.

**Required GitHub configuration by provider:**

| | AWS S3 (`s3://...`) | GCS (`gs://...`) |
|---|---|---|
| **Secrets** | `AWS_ROLE_ARN` | `GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_SERVICE_ACCOUNT` |
| **Variables** | `BRREG_STORAGE_PATH`, `AWS_REGION` | `BRREG_STORAGE_PATH` |

**Trigger a workflow:**
```bash
# First-time setup (downloads bulk dump, seeds orderflow)
gh workflow run sync.yml -f command=setup

# Process the queue
gh workflow run sync.yml -f command=sync

# Fetch updates and add to fast lane
gh workflow run sync.yml -f command=patch
```

## Cloud Run Deployment

```bash
# Build and push image
gcloud builds submit --tag europe-north1-docker.pkg.dev/sondreskarsten-d7d14/r-images/brreg-regnskap:latest

# Jobs reference this image:
#   regnskap-sync-{0-9}   — sharded sync workers (0,12 or 6,18 UTC)
#   regnskap-backfill      — backfill dissolved/historical entities
```

## Known Issues

### BRREG HTTP 406 on PDF download (March 2026+)

BRREG started returning HTTP 406 for `Accept: application/pdf` on the PDF copy endpoint. Fix: use `Accept: application/octet-stream` instead — response `content-type` is still `application/pdf`. Omitting the Accept header entirely also works. Fixed in `regnskapsregisteret.py` as of 2026-04-09.

## Note Extraction

The `note_extraction` module extracts structured disclosures from parsed annual accounts text (via ParseExtract API). Currently targets klientkonto/klientmidler identification but detects bundne midler, nettopresentasjon, inkasso forskrift, felleskostnader, and forretningsfører notes.

```bash
python -m brreg_regnskap.extract_notes --orgnrs 984272170 --years 2023,2024 --output notes.parquet
```

Requires `PARSEEXTRACT_API_KEY` environment variable. Note: `requests` package is an undeclared runtime dependency for this module.

## Data Source

All data is from [Brønnøysundregistrene](https://data.brreg.no/) open APIs under the [NLOD 2.0](https://data.norge.no/nlod/en/2.0) license. No authentication required. The package self-throttles to 3 req/s by default.

## Development

```bash
uv sync --all-extras
uv run pytest -v
uv run ruff check src/ tests/
uv run mypy src/
```

## License

MIT
