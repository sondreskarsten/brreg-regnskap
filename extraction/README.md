# extraction/

OCR and structured data extraction from Norwegian årsregnskap PDFs.

## Directory Structure

```
extraction/
├── reference/                  # Versioned reference documents
│   ├── extraction_reference.md # Field patterns, terminology, validation rules (836 lines)
│   └── aarsregnskap_schema.py  # Pydantic v2 schema (41 definitions, 472 lines)
├── scripts/                    # Experimental extraction scripts
└── outputs/                    # Extraction run outputs (gitignored except samples)
```

## reference/

**`extraction_reference.md`** — Compiled from structural audit of 32+ entity note compilations across FY2022–2024. Covers:

- 15 table type archetypes mapped to document sections
- 19 note-by-note extraction patterns (regnskapsprinsipper → nærstående parter)
- Norwegian number format variants (6 patterns)
- Note title/number conventions with cross-reference index
- 9 parsing anomalies, NGAAP vs IFRS disclosure depth (dual notes, keyword false positives, chain-of-responsibility)
- 7 terminology sections (resultatregnskap, balanse, notes, årsberetning, revisjon, generalforsamling, labor)
- 14 validation signals (accounting equation, equity continuity, tax reconciliation, etc.)

**`aarsregnskap_schema.py`** — Pydantic v2 model defining the target extraction schema. 41 model definitions covering: generellInformasjon, resultatregnskap, balanse, noter (all 18 note types), revisjonsberetning, årsberetning, kontantstrømoppstilling. NoteTabellRad has 56 optional fields covering 13 observed table patterns.

## scripts/

Experimental extraction and evaluation scripts. Naming convention:

```
{tool}_{purpose}.py
```

Examples: `mistral_ocr_single.py`, `tesseract_baseline.py`, `evaluate_recall.py`

## outputs/

Extraction results from test runs. `.gitignore` excludes bulk outputs; commit only representative samples for comparison.

## Dependencies

Extraction scripts may require additional packages beyond the core `brreg-regnskap` install. These are tracked separately — extraction is experimental and not part of the deployed sync pipeline.
