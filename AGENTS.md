# AGENTS.md — Instructions for AI Coding Agents

This file provides context for Codex, Claude Code, or any AI agent working on this codebase.

## Project Summary

`brreg-regnskap` is a Python package that bulk-downloads Norwegian annual financial statements from BRREG (Brønnøysundregistrene). It stores JSON financial data and PDF annual reports in cloud storage (S3 or GCS) and tracks state via a Parquet manifest.

## Architecture

```
CLI (typer)
  → setup     — downloads bulk dump, seeds orderflow
  → patch     — polls BRREG updates API, adds to fast lane
  → sync      — processes orderflow via SyncEngine
  → compact   — manual compaction of completed orderflow entries
  → status    — show manifest + orderflow stats
  → verify    — check manifest files exist in storage
  → merge-manifests — merge shard manifests from parallel workers

Modules:
  cli.py                   — typer CLI wiring
  sync_engine.py           — orderflow-driven download engine
  orderflow.py             — two-lane parquet work queue (10 shards)
  api/enhetsregisteret.py  — discovers which companies have regnskap
  api/regnskapsregisteret.py — fetches JSON + PDF per entity
  storage.py               — writes to S3/GCS/local via fsspec
  manifest.py              — Parquet manifest tracking downloads
  checkpoint.py            — resume state between runs
  shard.py                 — shard assignment for parallel workers
  config.py                — pydantic-settings configuration
```

## Key Concepts

### Two-Lane Orderflow

The orderflow is a parquet-based work queue partitioned into 10 shards by `int(orgnr) % 10`:

- **Fast lane**: `(orgnr, year)` pairs from bulk dump or BRREG update patches. Priority = now. Downloads JSON + PDF.
- **Slow lane**: Historical years from `/aar` API. Priority = `unix(year-01-01)`. Downloads PDF only.
- **Discovery stubs**: `year=null` entries marking orgnrs that need a years-API call.

Fast lane is always processed first across all shards before any slow lane work begins.

### Workflow

1. `brreg-regnskap setup gs://bucket/data` — One-time: download bulk dump, seed orderflow
2. `brreg-regnskap sync gs://bucket/data --shard N` — Process orderflow (parallel across 10 shards)
3. `brreg-regnskap merge-manifests gs://bucket/data` — Merge shard manifests
4. `brreg-regnskap patch gs://bucket/data` — Periodic: fetch updates, add to fast lane
5. `brreg-regnskap compact gs://bucket/data` — Periodic: remove completed orderflow entries

### Key Principles

1. **All HTTP calls are async** using `aiohttp`. The CLI bridges sync/async via `asyncio.run()`.
2. **Storage is abstracted** via fsspec. Never import `boto3` or `google.cloud.storage` directly.
3. **The manifest is the source of truth** for what has been downloaded.
4. **Corrections**: When `(orgnr, year)` re-enters the fast lane (via patch), the engine re-downloads and compares hashes. If different, saves as a new version (`_v2`, `_v3`).
5. **Compaction is manual**: Call `compact` to clean up completed orderflow entries. This is not done automatically during sync.
6. **Checkpointing**: The `SyncEngine` checkpoints every N items (configurable). On restart, it resumes from the checkpoint.

### Coding Standards

- Python 3.11+
- Type hints on all public functions
- `ruff` for linting, `mypy --strict` for type checking
- Tests use `pytest` with `pytest-asyncio` and `aioresponses` for HTTP mocking
- No `print()` — use `structlog` for all output

### Running

```bash
uv sync --frozen
uv run pytest
uv run brreg-regnskap --help
```

### BRREG API Quick Reference

No authentication required. Be conservative with rate limits (3 req/s default).

| What | URL |
|------|-----|
| Bulk entity dump | `GET https://data.brreg.no/enhetsregisteret/api/enheter/lastned` |
| Entity updates | `GET https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter?dato={date}&includeChanges=true` |
| Latest regnskap JSON | `GET https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}` |
| Available years | `GET https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/aar` |
| PDF annual report | `GET https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/{year}` |

### Test Fixtures

Place real BRREG API response samples in `tests/fixtures/`:
- `enhet_964118191.json` — Mowi ASA entity response
- `regnskap_964118191.json` — Mowi ASA regnskap response
- `years_964118191.json` — `["2011","2012",...,"2024"]`

### GitHub Actions

Three workflows:
- `ci.yml` — runs on PR/push, lints + type checks + tests
- `sync.yml` — supports `setup`, `patch`, and `sync` commands; sync runs in parallel across 10 shards
- `gcs-sync.yml` — GCS bucket mirroring via `gsutil rsync`
