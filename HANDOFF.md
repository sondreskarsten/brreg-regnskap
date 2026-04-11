# Chat Manifest: Regnskap OCR Extraction Pipeline

**Date:** 2026-04-11
**Repo:** sondreskarsten/brreg-regnskap
**Session:** Follow-up — DETR experiment → pixel classification → production page_classifier module

---

## What was accomplished this session

### 1. DETR layout detection experiment (FAILED)

Three attempts to run `cmarkea/detr-layout-detection` on 62 pages across 5 entities. OOM, weight mismatch, timeout. **DETR dropped from pipeline.**

### 2. Image dimension discovery (100% source classification)

BRREG pages: always `1728×2312`. Company pages: always width `1653`, heights `{2140-2341}`. Tested on 50 PDFs (808 pages): **100% source accuracy, $0, 2ms/page.** Eliminates `classify_pdf()` API call.

Company height `2140`/`2337` = revisjonsberetning (100% in 20-entity sample).

### 3. Silhouette experiments → 75% ceiling

48×48 thumbnail features cannot distinguish BRREG from company when company uses identical visual template (Silvercoin). Image dimensions bypass this entirely.

### 4. BRREG positional type assignment (96%)

Fixed order: p1=generell_info, p2=resultatregnskap, p3=balanse, p4=balanse if n≥5, rest=noter. 112/117 correct.

### 5. Footer perceptual hash for platform ID (70% classified)

Integrated 205-PDF taxonomy. 32% Fiken/Tripletex/Conta (blank footer), 26% Visma Finale, 12% brreg-only, 30% unknown.

### 6. Zone segmentation: table/title/text (88% has_table)

Horizontal projection → ink/whitespace blocks → column gap detection → zone labels. 2308 blocks across 243 pages. Perceptual hashing cannot distinguish table from text (separability 0.06) — column structure destroyed at 8×8.

### 7. Production module: `page_classifier.py`

`build_manifest(pdf_bytes)` consolidates all 4 layers. `segment_page_zones(gray)` for per-page zone segmentation.

---

## Production extraction architecture

```
1. page_classifier.build_manifest(pdf_bytes)        $0, 2ms/page
   → brreg_last_page (100%), BRREG types (96%), platform (70%), zones (88%)

2. gemini_extraction.extract_pdf(pdf_bytes, manifest)   $0.0012/entity
   → structured JSON, receives manifest for boundary + zone hints

3. Cross-validate P&L/BS sums                       $0
```

## Current repo structure

```
brreg-regnskap/
├── src/brreg_regnskap/
│   ├── page_classifier.py     — Zero-cost page classification (4 layers)
│   ├── gemini_extraction.py   — Gemini structured extraction (v5 prompt)
│   ├── gemini_ocr.py          — Gemini Flash as OCR engine (Silver layer)
│   ├── cloud_vision_ocr.py    — Cloud Vision REST OCR
│   ├── regnskap_extraction.py — Regex table parser for OCR text
│   ├── extract_notes.py       — Note extraction with dedup + GCS backend
│   ├── parseextract.py        — ParseExtract OCR client
│   └── (other existing modules)
├── experiments/
│   ├── pdfs/                  — 50 test PDFs
│   └── results/               — All experiment data (JSON)
├── docs/
│   ├── ocr_format_experiments.md
│   └── manifest_classification_experiment.md
```

## First job for next chat

Wire `page_classifier.build_manifest()` into extraction pipeline. Run on 50 test entities. Evaluate against 9 existing Gemini extractions. Fix BRREG HTTP 406.

## Open items

- Gemini 3 Flash not available on Vertex AI (404)
- Batch API not tested (50% error rate reported on Flash-Lite)
- finstat cross-validation: 50% P&L failures from missing sum_driftskostnader
- BRREG HTTP 406 regression for new PDF downloads
- 30% unknown platform hashes need expanding reference set
- Zone segmentation company FPs from indented margins (14 cases)
