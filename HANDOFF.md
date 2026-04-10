# Chat Manifest: Regnskap OCR Extraction Pipeline

**Date:** 2026-04-11
**Repo:** sondreskarsten/brreg-regnskap
**Last commit:** (pending — this commit)

---

## What was accomplished this session

### 1. NotebookLM schema iteration (6 documents → prompt v5)

Processed 6 NotebookLM response documents into the Gemini extraction prompt:

| Doc | Source | Key additions |
|---|---|---|
| 1 | Audit/klientmidler | Disponible midler exclusion, forskrift signals, klientmidler_forskrift field |
| 2 | Note catalog | Klientmidler on BS, pantstillelser table structure, fortsatt drift rules |
| 3 | Line item variations | dato_fastsettelse, lender-specific finansposter, sameie revenue mapping |
| 4 | Structural analysis | Extract everything (reverted skip instruction), konsern object, noter source field |
| 5 | Audit disclosures | regnskapsforer_nr, two fravalgt revisjon phrasings, dispute-as-going-concern |
| 6 | Structural anomalies | Three number formats, kontantstrømoppstilling, årsberetning extraction |

### 2. PDF vs image token cost benchmark

Sending PDF directly = same tokens as extracted images (3,367). Per-page calls 13x more expensive due to per-request overhead. **Module updated to send PDFs directly — fitz dependency only needed for page count.**

### 3. Silver layer architecture

Built three OCR modules:

| Module | Method | Cost/entity | Deterministic |
|---|---|---|---|
| `cloud_vision_ocr.py` | Cloud Vision REST API | $0.020 | Yes |
| `gemini_ocr.py` | Gemini Flash as OCR | $0.005 | No |
| `gemini_extraction.py` extract_text() | Gemini on cached OCR text | $0.004 | No |

Cloud Vision API enabled on project. Gemini OCR validated as 3.6x cheaper with comparable quality.

### 4. OCR benchmark (Gemini 2.0 Flash vs 2.5 Flash)

10 pages × 2 models × 4 entities. Results in `docs/ocr_format_experiments.md`.

Key findings: Norwegian chars identical across models. Zero dash runs with pipe-delimited prompt. 2.5 Flash produces more structured tables, has 65K output tokens (vs 8K for 2.0). Flash-Lite does NOT support image input. **Decision: Gemini 2.5 Flash for production.**

### 5. 20 format experiments

Full results in `docs/ocr_format_experiments.md`. Decisive findings:

| Test | Finding | Action |
|---|---|---|
| T1 | JSON eliminates ALL multiline breaks | Switch to JSON response_schema |
| T9 | 8192 tokens truncates 14p docs | maxOutputTokens=65536 mandatory |
| T15 | PDF 2.6x cheaper than PNG input | Send PDFs directly |
| T6,7,8,13,14 | Zero effect from priming, NO fmt, anti-dupe, few-shot, mime_type | Remove from prompt |
| T10 | thinkingBudget>0 costs 4.5x more, fewer chars | Keep at 0 |
| T16 | 768px sufficient for number accuracy | Don't upscale |
| T17 | Gemini ignores Markdown-KV format | Don't attempt |
| T20 | Cross-validation catches 50% P&L errors | Implement in production |

### 6. Manifest classification experiment

Gemini classifies pages (source + type + has_table) for $0.0003/entity. Results in `docs/manifest_classification_experiment.md`.

**Critical finding: 3/4 test entities have NO company P&L/BS. Without manifest, Gemini silently copies BRREG values into company section — undetectable by cross-validation because the numbers are arithmetically consistent.** Manifest prevents this at 25% cost increase.

`classify_pdf()` method added to GeminiExtractor.

### 7. Deep research reports (3 artifacts)

- "Store the Text" — Silver layer economics ($48/yr storage vs $14K-100K per re-extraction)
- "Gemini Flash as OCR Engine" — model comparison, failure modes, production architecture
- "Fixing Gemini OCR Table Output" — JSON vs pipe (11 format benchmark), DETR/Document AI for bounding boxes

---

## First job for next chat

**Install DETR layout detection model and run the experiment.**

Script ready at `experiments/run_detr_layout.py`. Steps:

```bash
# 1. Install dependencies (may take a few minutes for torch)
pip install transformers torch timm Pillow --break-system-packages

# 2. Run the experiment
GOOGLE_APPLICATION_CREDENTIALS=/mnt/project/sondreskarsten-d7d14-8486be2d085b.json \
  python experiments/run_detr_layout.py
```

The script will:
1. Download `cmarkea/detr-layout-detection` (~170MB)
2. Run on all pages of 5 test entities (Bonord, ECITLAW, Alliance, Silvercoin, Rødskifer)
3. Detect 11 element types with bounding boxes per page
4. Classify pages as: financial_table, mixed_table_text, narrative, cover_or_header, other
5. Compare against Gemini classification manifests
6. Save results to `experiments/results/`

The bounding box data from DETR can then be used to verify column assignment in Gemini's extraction — the spatial verification layer that Gemini cannot provide natively.

---

## Current repo structure

```
brreg-regnskap/
├── src/brreg_regnskap/
│   ├── gemini_extraction.py    — Gemini structured extraction (v5 prompt, classify_pdf + extract_pdf + extract_text)
│   ├── gemini_ocr.py           — Gemini Flash as OCR engine (Silver layer)
│   ├── cloud_vision_ocr.py     — Cloud Vision REST OCR (deterministic Silver layer)
│   ├── regnskap_extraction.py  — Regex table parser for OCR text
│   ├── extract_notes.py        — Note extraction with dedup + GCS backend
│   ├── parseextract.py         — ParseExtract OCR client
│   └── (other existing modules)
├── experiments/
│   ├── run_ocr_experiments.py  — Test harness for 20 OCR format experiments
│   └── run_detr_layout.py      — DETR bounding box experiment (NEXT JOB)
├── docs/
│   ├── ocr_format_experiments.md              — 20 test results
│   └── manifest_classification_experiment.md  — Page manifest findings
```

## GCS state

```
gs://brreg-regnskap/
├── regnskap/{orgnr}/aarsregnskap_2024.pdf  — Raw PDFs (Bronze)
├── notes/{orgnr}/
│   ├── cv_ocr_2024.txt      — Cloud Vision OCR text (Silver, Bonord only)
│   ├── gemini_ocr_2024.txt  — Gemini OCR text (Silver, Bonord only)
│   ├── gemini_v2.json       — Gemini structured extraction (Gold, 3 entities)
│   ├── ocr_2024.txt          — ParseExtract OCR text
│   ├── notes_2024.json       — Note flags from regex parser
│   └── regnskap_2024.json    — Line items from regex parser
├── notes/gemini_extractions.json  — Consolidated Gemini results (9 entities)
├── notes/benchmark/
│   ├── experiment_results.json    — Raw experiment data (28 tests)
│   ├── manifest_experiment.json   — Manifest comparison results
│   ├── page_manifests.json        — Gemini page classifications
│   ├── ocr_benchmark_results.json — Model comparison data
│   └── test*_*.{txt,json,html}    — Per-test output files
```

## Open items

- DETR experiment not yet run (model download timed out — first job next chat)
- Gemini 3 Flash not available on Vertex AI (404) — monitor
- Batch API (Vertex AI) not tested — reported 50% error rate on Flash-Lite
- finstat cross-validation shows 50% P&L failures — root cause is missing sum_driftskostnader in some BRREG wrappers
- ParseExtract jobs for Axactor and Veidekke still in_progress (likely expired)
- BRREG HTTP 406 regression for new PDF downloads not yet fixed
