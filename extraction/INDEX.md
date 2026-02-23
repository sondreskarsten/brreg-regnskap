# INDEX — extraction/

Machine-readable inventory of the extraction directory. Updated 2025-02-23.

---

## Directory Layout

```
extraction/
├── INDEX.md                            ← this file
├── README.md                           ← human-readable overview
├── reference/
│   ├── extraction_reference.md         ← field patterns, validation, terminology (751 lines)
│   └── aarsregnskap_schema.py          ← Pydantic v2 target schema (472 lines, 43 classes)
├── scripts/                            ← experimental extraction & evaluation scripts
└── outputs/                            ← run outputs (gitignored; sample_* committed)
```

---

## reference/extraction_reference.md

Compiled from structural audit of 32+ entities across FY2022–2024. Source of truth for what to expect in Norwegian årsregnskap PDFs and how to validate extracted data.

### Sections

| # | Section | Lines | Content |
|---|---------|-------|---------|
| 1 | Note Structure Classes | 7–19 | 4 structure types (A: Narrative, B: Key-Value, C: Standard Grid, D: Complex Matrix) |
| 1.1 | Table Type Taxonomy | 20–44 | 15 table archetypes mapped to 6 document zones |
| 2 | Note-by-Note Extraction Patterns | 46–521 | 18 note types with observed row labels, column variants, edge cases |
| 3 | Number Format Patterns | 523–543 | 11 Norwegian number formatting variants with examples |
| 4 | Note Title/Number Conventions | 545–595 | Note number → disclosure type mapping; no standard numbering exists |
| 4.1 | Note Cross-Reference Index | 576–595 | 10-row bidirectional lookup: note number → title → regnskapspost |
| 5 | Critical Parsing Anomalies | 597–643 | 9 anomaly types (dual notes, PII, liquidation, keyword false positives, etc.) |
| 6 | Comprehensive Terminology Reference | 645–715 | 7 sub-glossaries covering all document sections |
| 7 | Validation Signals from Note Content | 717–738 | 11 cross-checks derivable from extracted note data |
| 8 | Identified Data Quality Issues | 740–751 | 8 entity-specific data quality flags |

### Note Types Covered (Section 2)

| § | Note Type | Structure | Frequency |
|---|-----------|-----------|-----------|
| 2.1 | Regnskapsprinsipper (Accounting Principles) | A | High |
| 2.2 | Lønnskostnader (Payroll Costs) | C + narrative | High |
| 2.3 | Skatt (Tax) | D (multi-subtable) | High |
| 2.4 | Egenkapital (Equity Reconciliation) | C | High |
| 2.5 | Aksjonærer / Aksjekapital (Shareholders) | D (multi-subtable) | High |
| 2.6 | Anleggsmidler (Fixed Assets) | D (reconciliation matrix) | High |
| 2.7 | Gjeld og Pantstillelser (Debt and Pledges) | A/C mixed | High |
| 2.8 | Fordringer (Receivables) | A/C mixed | High |
| 2.9 | Verdipapirer / Investeringer (Securities) | C/D | Medium |
| 2.10 | Varelager (Inventory) | C | Medium |
| 2.11 | Bankinnskudd (Bank Deposits) | B | Medium |
| 2.12 | Konsernforhold (Group Relations) | A/C mixed | Medium |
| 2.13 | Fortsatt drift (Going Concern) | A | Low-medium |
| 2.14 | Fisjon/Fusjon (Demerger/Merger) | A | Low |
| 2.15 | Borettslag/Sameie-Specific | Mixed | Domain-specific |
| 2.16 | Salgsinntekt (Revenue) | A/B | Low-medium |
| 2.17 | Annen kortsiktig gjeld (Other Current Liabilities) | C | Medium |
| 2.18 | Fagforeninger og Tariffavtaler (Unions) | A | Low (NRS 8) |

---

## reference/aarsregnskap_schema.py

Pydantic v2 target schema defining the structured output that extraction scripts should produce. 43 classes total (2 enums, 41 models).

### Class Inventory

| Class | Type | Role |
|-------|------|------|
| Organisasjonsform | Enum | AS, ASA, ENK, ANS, DA, NUF, other |
| Revisjonskonklusjon | Enum | Audit opinion categories |
| AarsPar | Model | Reusable current/prior year amount pair |
| Forretningsadresse | Model | Street address |
| GenerellInformasjon | Model | Cover page: org number, fiscal year, accounting standard |
| Driftsinntekter | Model | Operating revenue lines |
| Driftskostnader | Model | Operating cost lines |
| Finansposter | Model | Financial income/expense lines |
| OverforingerOgDisponeringer | Model | Profit allocation |
| Resultatregnskap | Model | **Top-level income statement** |
| UtsattSkattefordel | Model | Deferred tax asset |
| VarigeDriftsmidler | Model | Tangible fixed assets |
| Anleggsmidler | Model | Non-current assets aggregate |
| Fordringer | Model | Receivables |
| Omlopsmidler | Model | Current assets aggregate |
| Eiendeler | Model | **Top-level assets** |
| InnskuttEgenkapital | Model | Paid-in equity |
| OpptjentEgenkapital | Model | Retained equity |
| Egenkapital | Model | **Top-level equity** |
| KortsiktigGjeld | Model | Current liabilities |
| Gjeld | Model | **Top-level liabilities** |
| EgenkapitalOgGjeld | Model | Equity + liabilities aggregate |
| Balanse | Model | **Top-level balance sheet** |
| NoteTabellRad | Model | Generic note table row (56 optional fields, 13 table patterns) |
| NoteTabell | Model | Table container with headers + rows |
| Noteopplysning | Model | Single note: number, title, narrative text, tables |
| LonnskostnaderSpesifisert | Model | Payroll breakdown |
| Ansattinformasjon | Model | FTE, headcount, OTP, remuneration |
| AnleggsmiddelKategori | Model | Fixed asset category reconciliation |
| AnleggsmidlerDetaljer | Model | Fixed assets note container |
| SkattekostnadSpesifisert | Model | Tax expense breakdown |
| BeregningAvSkattegrunnlag | Model | Taxable income calculation |
| MidlertidigForskjell | Model | Temporary difference line |
| UtsattSkattefordelEllerSkatt | Model | Deferred tax summary |
| Skattedetaljer | Model | **Tax note container** (all 4 sub-tables) |
| EgenkapitalBevegelse | Model | Single equity movement row |
| EgenkapitalSaldo | Model | Opening/closing equity balances |
| Egenkapitalendring | Model | **Equity note container** |
| BankinnskuddDetaljer | Model | Bank deposits + restricted amounts |
| Regnskapsprinsipper | Model | Accounting principles note |
| Revisjonsberetning | Model | Auditor's report |
| Signatur | Model | Signature block |
| Aarsregnskap | Model | **Root model** — entire annual accounts extraction |

### Key Structural Relationships

```
Aarsregnskap (root)
├── generellInformasjon: GenerellInformasjon
├── resultatregnskap: Resultatregnskap
│   ├── driftsinntekter: Driftsinntekter
│   ├── driftskostnader: Driftskostnader
│   ├── finansposter: Finansposter
│   └── overforinger: OverforingerOgDisponeringer
├── balanse: Balanse
│   ├── eiendeler: Eiendeler
│   └── egenkapitalOgGjeld: EgenkapitalOgGjeld
├── noter: list[Noteopplysning]       ← generic catch-all
├── ansattinformasjon: Ansattinformasjon
├── anleggsmidlerDetaljer: AnleggsmidlerDetaljer
├── skattedetaljer: Skattedetaljer
├── egenkapitalendring: Egenkapitalendring
├── bankinnskuddDetaljer: BankinnskuddDetaljer
├── regnskapsprinsipper: Regnskapsprinsipper
├── revisjonsberetning: Revisjonsberetning
└── signaturer: list[Signatur]
```

---

## scripts/

Empty. Naming convention: `{tool}_{purpose}.py`

Expected first scripts:
- `mistral_ocr_single.py` — single-PDF extraction via Mistral OCR API
- `tesseract_baseline.py` — baseline text extraction for comparison
- `evaluate_recall.py` — recall measurement against ground truth

---

## outputs/

Gitignored by default. Commit files prefixed `sample_*` for versioned comparison baselines.
