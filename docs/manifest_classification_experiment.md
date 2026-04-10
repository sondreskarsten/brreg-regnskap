# Page Manifest Classification Experiment

**Date:** 2026-04-10
**Method:** Gemini 2.5 Flash classifies each page (source + type + has_table), manifest sent with extraction prompt.
**Cost:** ~500 extra output tokens per entity for classification (~$0.0003). Same input tokens.

## Manifests

### Bonord (13p) — NO company P&L/BS

| Page | Source | Type | Table |
|---|---|---|---|
| 1 | brreg | generell_info | |
| 2 | brreg | resultatregnskap | 📊 |
| 3 | brreg | balanse | 📊 |
| 4 | brreg | balanse | 📊 |
| 5 | brreg | noter | 📊 |
| 6 | brreg | noter | |
| 7 | company | revisjonsberetning | |
| 8 | company | revisjonsberetning | |
| 9 | company | signatur | |
| 10 | company | noter | |
| 11 | company | noter | 📊 |
| 12 | company | noter | 📊 |
| 13 | company | noter | 📊 |

### ECITLAW (10p) — NO company P&L/BS

| Page | Source | Type | Table |
|---|---|---|---|
| 1 | brreg | generell_info | |
| 2 | brreg | resultatregnskap | 📊 |
| 3 | brreg | balanse | 📊 |
| 4 | brreg | balanse | 📊 |
| 5 | brreg | noter | 📊 |
| 6 | brreg | noter | 📊 |
| 7 | brreg | noter | |
| 8 | company | revisjonsberetning | |
| 9 | company | revisjonsberetning | |
| 10 | company | signatur | |

### Alliance (11p) — NO company P&L/BS

| Page | Source | Type | Table |
|---|---|---|---|
| 1 | brreg | generell_info | |
| 2 | brreg | resultatregnskap | 📊 |
| 3 | brreg | balanse | 📊 |
| 4 | brreg | noter | 📊 |
| 5 | brreg | noter | 📊 |
| 6 | company | revisjonsberetning | |
| 7 | company | revisjonsberetning | |
| 8 | company | signatur | |
| 9 | company | noter | 📊 |
| 10 | company | noter | 📊 |
| 11 | company | noter | 📊 |

### Silvercoin (14p) — HAS company P&L/BS

| Page | Source | Type | Table |
|---|---|---|---|
| 1 | brreg | generell_info | |
| 2 | brreg | resultatregnskap | 📊 |
| 3 | brreg | balanse | 📊 |
| 4 | brreg | balanse | 📊 |
| 5 | brreg | noter | |
| 6 | **company** | **resultatregnskap** | 📊 |
| 7 | **company** | **balanse** | 📊 |
| 8 | **company** | **balanse** | 📊 |
| 9 | company | noter | 📊 |
| 10 | company | noter | 📊 |
| 11 | company | noter | 📊 |
| 12 | company | signatur | |
| 13 | company | revisjonsberetning | |
| 14 | company | signatur | |

## Key finding: manifest prevents hallucinated company values

3 of 4 entities (Bonord, ECITLAW, Alliance) have **no separate company P&L/BS** — only the BRREG standardized wrapper plus company notes and revisjonsberetning.

| Entity | Company has own P&L/BS | Baseline extraction | Manifest extraction |
|---|---|---|---|
| Bonord | No | ✗ Copies BRREG values to company | ✓ company = null |
| ECITLAW | No | ✗ Copies BRREG values to company | ✓ company = null |
| Alliance | No | ✗ Copies BRREG values to company | ✓ company = null |
| Silvercoin | **Yes** (p6-8) | ✓ Extracts company values | ✓ Extracts company values |

Without the manifest, Gemini cannot distinguish between "this entity has one set of financials (BRREG only)" and "this entity has two sets (BRREG + company)". It defaults to populating both sections with the same numbers — a silent hallucination that downstream consumers would interpret as independent confirmation of the values.

With the manifest, Gemini sees that no `resultatregnskap` or `balanse` page exists in the company section, and correctly returns null.

## Cross-validation results (with manifest)

| Entity | BRREG P&L | BRREG BS | Company P&L | Company BS |
|---|---|---|---|---|
| Bonord | ✓ SDI-SDK=DR | ✓ EI=EK+GJ | null (correct) | null (correct) |
| Alliance | ✓ | ✓ | null (correct) | null (correct) |
| ECITLAW | — | — | null (correct) | null (correct) |
| Silvercoin | — | — | — | — |

## Cost

| Component | Output tokens | Cost per entity |
|---|---|---|
| Classification call | ~500 | $0.0003 |
| Extraction call (same with or without manifest) | ~2,000 | $0.0012 |
| **Total with manifest** | ~2,500 | **$0.0015** |
| **Total without manifest** | ~2,000 | **$0.0012** |

The manifest adds 25% to extraction cost ($0.0003/entity) but eliminates a category of hallucination that no cross-validation can catch — because the hallucinated company values are arithmetically consistent (they're copied from BRREG).

## Structural insight for the 8M document pipeline

The BRREG/company structure varies by entity size:

- **Small entities (NRS 8):** BRREG wrapper only. No separate company P&L/BS. Company section has only notes + revisjonsberetning. This is the **majority** of the 8M documents.
- **Larger entities:** Both BRREG wrapper AND company's own financial statements. Company may have different formatting, additional line items (konsernbidrag, renteinntekt konsern), and kontantstrømoppstilling.
- **IFRS entities:** English company section with Norwegian BRREG wrapper. Amounts may differ (BRREG in NOK, company in '000 NOK).

The manifest classification identifies which pattern applies at near-zero cost, enabling the extraction prompt to ask for the right things on each entity.

## Recommendation

Add the classification call as a standard first step in the production pipeline. The ~500 output tokens ($0.0003) prevent the most dangerous error type — silent value duplication that passes all arithmetic cross-validation checks.
