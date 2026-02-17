# AGENTS.md — Instructions for AI Coding Agents

This file provides context for Codex, Claude Code, or any AI agent working on this codebase.

## Project Summary

`brreg-regnskap` is a Python package that bulk-downloads Norwegian annual financial statements from BRREG (Brønnøysundregistrene). It stores JSON financial data and PDF annual reports in cloud storage (S3 or GCS) and tracks state via a Parquet manifest.

## Architecture

```
CLI (typer)
  → SyncEngine (downloader.py)
    → EnhetsregisteretClient (api/enhetsregisteret.py)   # discovers which companies have regnskap
    → RegnskapsregisteretClient (api/regnskapsregisteret.py)  # fetches JSON + PDF
    → StorageBackend (storage.py)                         # writes to S3/GCS/local via fsspec
    → ManifestManager (manifest.py)                       # tracks what's been downloaded
    → CheckpointManager (checkpoint.py)                   # resume state between runs
```

## Implementation Guide

Read `PLAN.md` for the full task breakdown. **Phases 1-3 are complete.** All modules are implemented, 73 tests pass, linting is clean.

### Current State (Phase 4 — Production Readiness)

All source modules have full implementations with no stubs. The package is ready for local testing against real BRREG APIs. Remaining work:

1. **Local full sync test** — run `brreg-regnskap sync ./local-test --mode full --rps 5` and tune rate limits based on real API behavior
2. **Structured logging polish** — structlog is wired up; review log output during real sync runs and adjust log levels/fields
3. **GitHub Actions deployment** — set up the S3 bucket, OIDC role, and repository secrets per the workflow files
4. **Contact opendata@brreg.no** — inform BRREG before large-scale operation (good API citizenship)
5. **Edge cases** — the async download engine handles 404s and retries, but real-world runs may reveal additional edge cases in BRREG's API responses

### Key Principles

1. **All HTTP calls are async** using `aiohttp`. The CLI bridges sync/async via `asyncio.run()`.
2. **Storage is abstracted** via fsspec. Never import `boto3` or `google.cloud.storage` directly — use `fsspec` and let `s3fs`/`gcsfs` handle the backends.
3. **The manifest is the source of truth** for what has been downloaded. If a file exists in storage but not in the manifest, it's orphaned. If it's in the manifest but not in storage, it needs re-downloading.
4. **Corrections**: When BRREG replaces a regnskap (new `journalnr` for same orgnr+year), preserve the old file under `corrections/` and download the new one. The manifest tracks both via `is_correction` and `journalnr`.
5. **Checkpointing**: The `SyncEngine` must checkpoint every N items (configurable, default 1000). On restart, it resumes from the checkpoint. This enables safe operation under GitHub Actions' 6-hour time limit.

### Coding Standards

- Python 3.11+
- Type hints on all public functions
- No comments in code — use descriptive names and docstrings
- `ruff` for linting, `mypy --strict` for type checking
- Tests use `pytest` with `pytest-asyncio` and `aioresponses` for HTTP mocking
- No `print()` — use `structlog` for all output

### Running

```bash
# Install with uv
uv sync --frozen

# Run tests
uv run pytest

# Run CLI
uv run brreg-regnskap --help
uv run brreg-regnskap sync ./local-mirror
uv run brreg-regnskap sync s3://my-bucket/brreg
uv run brreg-regnskap sync gs://my-bucket/brreg
uv run brreg-regnskap status s3://my-bucket/brreg
```

### BRREG API Quick Reference

No authentication required. No documented rate limits. Be conservative (10 req/s).

| What | URL |
|------|-----|
| Bulk entity dump | `GET https://data.brreg.no/enhetsregisteret/api/enheter/lastned` |
| Single entity | `GET https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}` |
| Entity updates | `GET https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter?oppdateringsid={id}&includeChanges=true` |
| Latest regnskap JSON | `GET https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}` |
| Available years | `GET https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/aar` |
| PDF annual report | `GET https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/{year}` |

Entity dump Accept header: `application/vnd.brreg.enhetsregisteret.enhet.v2+json`

### Test Fixtures

Place real BRREG API response samples in `tests/fixtures/`:
- `enhet_964118191.json` — Mowi ASA entity response
- `regnskap_964118191.json` — Mowi ASA regnskap response
- `years_964118191.json` — `["2011","2012",...,"2024"]`

Use these for model parsing tests. Do NOT make live API calls in tests.

### Environment Variables

See `.env.example` for all configurable values. The `BRREG_STORAGE_PATH` variable is required — everything else has sensible defaults.

### GitHub Actions

Two workflows:
- `ci.yml` — runs on PR/push, lints and tests
- `sync.yml` — runs on schedule or manual dispatch, executes the sync

The sync workflow uses a dynamic matrix strategy. See `scripts/generate_matrix.py` for the planning logic.
