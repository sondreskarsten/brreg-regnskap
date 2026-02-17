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
# Full sync to local directory
brreg-regnskap sync ./data --mode full

# Full sync to S3
brreg-regnskap sync s3://my-bucket/brreg --mode full

# Incremental sync (only new/changed since last run)
brreg-regnskap sync s3://my-bucket/brreg --mode incremental

# Check status
brreg-regnskap status s3://my-bucket/brreg

# Merge shard manifests after parallel GitHub Actions run
brreg-regnskap merge-manifests s3://my-bucket/brreg
```

### CLI Options

```
brreg-regnskap sync STORAGE_PATH [OPTIONS]

  --mode              full | incremental (default: full)
  --max-concurrent    Max simultaneous HTTP connections (default: 50)
  --rps               Max requests per second (default: 10.0)
  --max-runtime       Max runtime in minutes, 0=unlimited (default: 0)
  --range-start       Start of orgnr range (for parallel jobs)
  --range-end         End of orgnr range (for parallel jobs)
  --checkpoint-interval  Save state every N entities (default: 1000)
  --log-level         DEBUG | INFO | WARNING | ERROR (default: INFO)
```

## Configuration

All settings can be set via environment variables with the `BRREG_` prefix. See [.env.example](.env.example).

| Variable | Default | Description |
|----------|---------|-------------|
| `BRREG_STORAGE_PATH` | `./data` | Root storage path (local, `s3://`, or `gs://`) |
| `BRREG_MAX_CONCURRENT` | `50` | Max simultaneous HTTP connections |
| `BRREG_REQUESTS_PER_SECOND` | `10.0` | Rate limit for BRREG API calls |
| `BRREG_MAX_RETRIES` | `5` | Max retry attempts per failed request |
| `BRREG_CHECKPOINT_INTERVAL` | `1000` | Save checkpoint every N entities |
| `BRREG_MAX_RUNTIME_MINUTES` | `0` | Graceful shutdown timer (0=unlimited) |
| `BRREG_LOG_LEVEL` | `INFO` | Log verbosity |

## Architecture

```
CLI (typer)
  → SyncEngine (downloader.py)
    → EnhetsregisteretClient    — discovers entities with regnskap
    → RegnskapsregisteretClient  — fetches JSON + PDF per entity
    → StorageBackend (fsspec)    — writes to S3/GCS/local
    → ManifestManager            — Parquet manifest tracking downloads
    → CheckpointManager          — resume state between runs
```

### Storage Layout

```
{storage_path}/
├── manifest.parquet
├── checkpoint.json
├── entities/
│   └── enheter_dump_{date}.json.gz
├── regnskap/
│   └── {orgnr}/
│       ├── regnskap_{year}.json
│       └── aarsregnskap_{year}.pdf
└── corrections/
    └── {orgnr}/
        ├── regnskap_{year}_{journalnr}_{timestamp}.json
        └── aarsregnskap_{year}_{timestamp}.pdf
```

### Sync Modes

**Full sync**: Downloads the nightly entity bulk dump, filters to entities with `sisteInnsendteAarsregnskap` set, and processes each one. Skips entities already in the manifest (unless a correction is detected).

**Incremental sync**: Polls the BRREG updates API from the last stored cursor (`oppdateringsid`), filters for entities where `sisteInnsendteAarsregnskap` changed, and processes only those.

### Correction Handling

When BRREG receives a corrected regnskap for a company, the `journalnr` changes for the same `(orgnr, year)`. The sync engine detects this by comparing against the manifest, archives the old files under `corrections/`, downloads the new version, and updates the manifest with `is_correction=True`.

## GitHub Actions

The included workflows support automated syncing:

- **ci.yml** — Lint + test on push/PR
- **sync.yml** — Scheduled sync with dynamic matrix parallelization

The sync workflow splits work across multiple parallel jobs (configurable shards), each with a 5.5-hour timeout and checkpointing. A merge job combines shard manifests after completion.

### Setup

1. Create an S3 bucket (or GCS equivalent)
2. Set up OIDC authentication between GitHub and AWS/GCP
3. Configure repository variables:
   - `BRREG_STORAGE_PATH`: e.g. `s3://brreg-regnskap/data`
   - `AWS_REGION`: e.g. `eu-north-1`
4. Configure repository secrets:
   - `AWS_ROLE_ARN`: IAM role ARN for OIDC

## Data Source

All data is from [Brønnøysundregistrene](https://data.brreg.no/) open APIs under the [NLOD 2.0](https://data.norge.no/nlod/en/2.0) license. No authentication required. No documented rate limits (the package self-throttles to 10 req/s by default).

The free API provides key financial figures from the most recent annual account per company. Full historical data with all line items requires a paid subscription (~NOK 400K/yr).

## Development

```bash
uv sync --all-extras
uv run pytest -v
uv run ruff check src/ tests/
uv run mypy src/
```

## License

MIT
