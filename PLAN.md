# Implementation Plan: brreg-regnskap

## Overview

Python package to bulk-download and maintain a mirror of Norwegian annual financial statements (årsregnskap) from Brønnøysundregistrene (BRREG). Stores JSON regnskap data and PDF copies in S3 or GCS. Tracks state via a Parquet manifest. Runs locally or in GitHub Actions.

---

## Subtask Breakdown

### Phase 1: Project Scaffolding (COMPLETE — delivered in this PR)

| # | File | Purpose |
|---|------|---------|
| 1.1 | `pyproject.toml` | Build config, dependencies, extras `[s3]` `[gcs]`, CLI entrypoint |
| 1.2 | `README.md` | Usage, installation, configuration, architecture |
| 1.3 | `AGENTS.md` | Instructions for Codex / Claude Code agents |
| 1.4 | `.env.example` | All configurable env vars with comments |
| 1.5 | `.gitignore` | Python/uv/IDE ignores |
| 1.6 | `src/brreg_regnskap/__init__.py` | Package init |
| 1.7 | `src/brreg_regnskap/__about__.py` | `__version__` |
| 1.8 | `src/brreg_regnskap/config.py` | Pydantic settings model |
| 1.9 | `src/brreg_regnskap/cli.py` | Typer CLI with subcommands |
| 1.10 | `src/brreg_regnskap/api/__init__.py` | API subpackage |
| 1.11 | `src/brreg_regnskap/api/enhetsregisteret.py` | Entity registry client |
| 1.12 | `src/brreg_regnskap/api/regnskapsregisteret.py` | Accounts registry client |
| 1.13 | `src/brreg_regnskap/api/models.py` | Pydantic response models |
| 1.14 | `src/brreg_regnskap/storage.py` | fsspec storage abstraction |
| 1.15 | `src/brreg_regnskap/manifest.py` | Parquet manifest read/write/upsert |
| 1.16 | `src/brreg_regnskap/checkpoint.py` | Cursor/checkpoint persistence |
| 1.17 | `src/brreg_regnskap/downloader.py` | Async download engine |
| 1.18 | `tests/conftest.py` | Shared fixtures |
| 1.19 | `tests/test_manifest.py` | Manifest CRUD tests |
| 1.20 | `tests/test_config.py` | Config loading tests |
| 1.21 | `tests/test_api_models.py` | Model parsing tests |
| 1.22 | `.github/workflows/sync.yml` | GitHub Actions sync workflow |
| 1.23 | `.github/workflows/ci.yml` | GitHub Actions CI (lint + test) |
| 1.24 | `scripts/generate_matrix.py` | Matrix generator for parallel GHA jobs |

### Phase 2: Core Implementation (COMPLETE)

All modules fully implemented. Zero `NotImplementedError` stubs remaining.

| # | Module | What to implement | Depends on |
|---|--------|-------------------|------------|
| 2.1 | `config.py` | Validate storage_path prefix, detect backend, credential check | — |
| 2.2 | `api/models.py` | Parse real BRREG JSON responses into typed models | — |
| 2.3 | `api/enhetsregisteret.py` | `download_bulk_dump()`: GET `/api/enheter/lastned`, gunzip, parse JSON, yield `Enhet` models. `poll_updates(since_id)`: GET `/api/oppdateringer/enheter?oppdateringsid={since_id}&includeChanges=true`, paginate, yield updates where `sisteInnsendteAarsregnskap` changed | 2.2 |
| 2.4 | `api/regnskapsregisteret.py` | `fetch_regnskap(orgnr)`: GET `/regnskap/{orgnr}`, return parsed JSON. `fetch_years(orgnr)`: GET `/aarsregnskap/kopi/{orgnr}/aar`, return list[int]. `download_pdf(orgnr, year)`: GET `/aarsregnskap/kopi/{orgnr}/{year}`, return bytes | 2.2 |
| 2.5 | `storage.py` | `StorageBackend` class wrapping fsspec: `write_bytes(path, data)`, `read_bytes(path)`, `exists(path)`, `list_dir(prefix)`, `check_credentials()` | — |
| 2.6 | `manifest.py` | `ManifestManager`: load/save parquet via pyarrow, `upsert(records)`, `get(orgnr, year)`, `list_missing(orgnr_list, year)`, `list_corrections(orgnr, journalnr)` | 2.5 |
| 2.7 | `checkpoint.py` | `CheckpointManager`: stores JSON `{last_oppdateringsid, last_orgnr_processed, run_started_at, mode}` in storage backend | 2.5 |
| 2.8 | `downloader.py` | `SyncEngine.run()`: orchestrates full/incremental sync. Uses aiohttp with semaphore + aiolimiter. Processes entities in batches. Calls regnskapsregisteret API. Writes files to storage. Updates manifest. Checkpoints every N items. Respects `--max-runtime` for GHA time limits | 2.3, 2.4, 2.5, 2.6, 2.7 |
| 2.9 | `cli.py` | Wire up typer commands to SyncEngine: `sync`, `status`, `verify`, `merge-manifests` | 2.8 |
| 2.10 | `scripts/generate_matrix.py` | Read checkpoint, determine mode, split org ranges, output JSON matrix for GHA | 2.6, 2.7 |

### Phase 3: Testing & Hardening (COMPLETE — 74 tests passing)

| # | What | Status |
|---|------|--------|
| 3.1 | Unit tests for manifest | ✅ 16 tests |
| 3.2 | Unit tests for API models | ✅ 13 tests |
| 3.3 | Integration test for storage | ✅ 9 tests |
| 3.4 | Mock-based test for downloader | ✅ 7 tests (aioresponses) |
| 3.5 | CLI smoke tests | ✅ 8 tests (typer CliRunner) |
| 3.6 | Config tests | ✅ 13 tests |
| 3.7 | Checkpoint tests | ✅ 5 tests |
| — | Ruff lint | ✅ 0 errors |

### Phase 4: Production Readiness

| # | What | Status |
|---|------|--------|
| 4.1 | Add structured logging (structlog) | ✅ Integrated in downloader + CLI |
| 4.2 | Add Prometheus-style metrics (optional) | — |
| 4.3 | Run first local full sync, tune rate limits | — |
| 4.4 | Enable GitHub Actions cron | — |

---

## Architecture Decisions

### Storage layout

```
{storage_path}/
├── manifest.parquet                    # Global manifest
├── manifest-{range}.parquet            # Shard manifests (GHA matrix jobs)
├── checkpoint.json                     # Sync cursor state
├── entities/
│   └── enheter_dump_{date}.json.gz     # Cached bulk entity dump
├── regnskap/
│   └── {orgnr}/
│       ├── regnskap_{year}.json        # Structured financial data
│       └── aarsregnskap_{year}.pdf     # PDF annual report
└── corrections/
    └── {orgnr}/
        ├── regnskap_{year}_{journalnr}_{timestamp}.json
        └── aarsregnskap_{year}_{timestamp}.pdf
```

### Correction handling

1. On each download, compare `journalnr` from new JSON against manifest
2. If manifest has existing entry for `(orgnr, year)` with different `journalnr`:
   - Move old files to `corrections/` with timestamp suffix
   - Download new versions to `regnskap/`
   - Upsert manifest with `is_correction=True`
3. Old versions are preserved, never deleted

### Rate limiting strategy

- `asyncio.Semaphore(50)` for max concurrent connections
- `aiolimiter.AsyncLimiter(10, 1)` for 10 req/s baseline
- Back off to 2 req/s on HTTP 429 or 503
- `tenacity` retry with exponential jitter, max 5 attempts

### GitHub Actions execution model

1. **Plan job**: downloads checkpoint, generates matrix of org-number ranges
2. **Process jobs** (matrix, max 20 parallel): each processes ~50K orgnr, writes shard manifest
3. **Merge job**: combines shard manifests into global manifest, updates checkpoint
4. **Time guard**: `--max-runtime` flag (default 300 min) causes graceful shutdown + checkpoint before GHA kills the job

---

## BRREG API Reference (for implementers)

### Enhetsregisteret

| Endpoint | Method | Returns |
|----------|--------|---------|
| `/api/enheter/lastned` | GET | gzip JSON of all entities |
| `/api/enheter/{orgnr}` | GET | Single entity JSON |
| `/api/oppdateringer/enheter` | GET | Change events, params: `dato`, `oppdateringsid`, `includeChanges` |

Accept header: `application/vnd.brreg.enhetsregisteret.enhet.v2+json`

### Regnskapsregisteret

| Endpoint | Method | Returns |
|----------|--------|---------|
| `/regnskap/{orgnr}` | GET | XML/JSON with financial figures |
| `/regnskap/aarsregnskap/kopi/{orgnr}/aar` | GET | JSON array of available years |
| `/regnskap/aarsregnskap/kopi/{orgnr}/{year}` | GET | PDF binary |

Base URL: `https://data.brreg.no/regnskapsregisteret`

No authentication required. No documented rate limits. NLOD 2.0 license.
