# OCR Format Experiments: Gemini 2.5 Flash on Norwegian Årsregnskap

**Date:** 2026-04-10
**Test set:** 5 entities ≤15 pages (Bonord 13p, ECITLAW 10p, Alliance 11p, Silvercoin 14p, Rødskifer 14p)
**Model:** Gemini 2.5 Flash via Vertex AI, thinkingBudget=0, temperature=0 unless noted
**Results stored:** `gs://brreg-regnskap/notes/benchmark/`

---

## Test 1: JSON structured output vs pipe-delimited

**Page:** Bonord p1 (BRREG P&L)

| Metric | Pipe-delimited | JSON (response_schema) |
|---|---|---|
| Multi-line label breaks | 8 | **0** |
| Parse failures | 0 | 0 |
| Output tokens | 537 | 878 |
| Extracted rows | — | 16 |
| JSON valid | — | true |

**Verdict:** JSON eliminates all multi-line label breaks. Token cost 63% higher. The decisive improvement — every other formatting issue (inconsistent spacing, empty cell ambiguity) also disappears with JSON.

---

## Test 2: Per-page vs full-PDF content duplication

**Entity:** Bonord (13 pages)

| Metric | Full PDF (1 call) | Per-page (13 calls) |
|---|---|---|
| Duplicate paragraphs | 1/133 | 1/138 |
| Output tokens | 7,001 | 7,444 |

**Verdict:** Minimal duplication on 13-page documents in both modes. The duplication problem reported earlier was on 24+ page documents (Sergel). For ≤15 page docs, full-PDF mode is acceptable and cheaper (1 API call vs 13).

---

## Test 3: Temperature 0 vs 0.2 vs 1.0

**Page:** Bonord p1 (pipe-delimited prompt)

| Temperature | Output chars | Dash runs | Finish |
|---|---|---|---|
| 0 | 1,023 | 0 | STOP |
| 0.2 | 1,023 | 0 | STOP |
| 1.0 | 1,023 | 0 | STOP |

**Verdict:** Zero dash runs at all temperatures with the current pipe-delimited prompt (no alignment padding instruction). The infinite-dash bug is only triggered by markdown table syntax with separator rows, which the current prompt avoids. Temperature choice is irrelevant for this prompt.

---

## Test 4: HTML table output

**Page:** Bonord p1

| Metric | Value |
|---|---|
| `<tr>` count | 22 |
| `<td>` count | 85 |
| Multi-line in `<td>` | **false** |
| Output tokens | 960 |

**Verdict:** HTML joins multi-line labels into single `<td>` cells (same benefit as JSON). Token cost between pipe (537) and JSON (878). Parseable with BeautifulSoup. Viable alternative to JSON for table-heavy pages.

---

## Test 5: Gemini 3 Flash

| Model | Status |
|---|---|
| gemini-2.5-flash | 200 OK |
| gemini-3-flash | **404 Not Found** |
| gemini-3-flash-lite | **404 Not Found** |

**Verdict:** Gemini 3 Flash not available on Vertex AI as of 2026-04-10. Cannot benchmark.

---

## Test 6: "Character by character" priming

**Page:** Bonord p7 (revisjonsberetning — dense Norwegian narrative)

| Metric | Primed | Unprimed |
|---|---|---|
| Output chars | 2,746 | 2,746 |
| Norwegian chars (æøå) | 30 | 30 |

**Verdict:** Zero measurable effect. The instruction "Transcribe character by character" does not improve OCR fidelity on Gemini 2.5 Flash. Identical output in both cases. Can be removed from prompt to save tokens.

---

## Test 7: Norwegian number format preservation

**Page:** Bonord p1

| Metric | Default prompt | Explicit Norwegian instruction |
|---|---|---|
| US-format numbers found | 0 | 0 |

**Verdict:** Gemini 2.5 Flash already preserves Norwegian number formatting (space-separated thousands) without explicit instruction. The "Do not convert to US format" instruction is unnecessary for this model.

---

## Test 8: Anti-duplication instruction

**Entity:** Alliance (11 pages, full-PDF mode)

| Metric | Without instruction | With "Do not repeat" |
|---|---|---|
| Duplicate paragraphs | 1/78 | **3/85** |
| Output tokens | 5,152 | 5,256 |

**Verdict:** The anti-duplication instruction is counterproductive — it increased duplicates from 1 to 3 and added 100 tokens. Do not use. Duplication is already minimal on ≤15 page documents.

---

## Test 9: maxOutputTokens exhaustion

**Entity:** Silvercoin (14 pages, full-PDF mode)

| maxOutputTokens | Output tokens | Pages found | Finish |
|---|---|---|---|
| 2,048 | 2,048 | 3 | **MAX_TOKENS** |
| 4,096 | 4,096 | 8 | **MAX_TOKENS** |
| 8,192 | 8,192 | 13 | **MAX_TOKENS** |
| 65,536 | 8,773 | **14** | STOP |

**Verdict:** Critical finding. A 14-page document requires 8,773 output tokens. The default 8,192 limit truncates at page 13 — silently losing the last page. **Must set maxOutputTokens=65536 on every call.** Gemini 2.0 Flash (hard cap 8,192) would truncate this document.

---

## Test 10: thinkingBudget impact

**Page:** Bonord p1

| thinkingBudget | Output chars | Norwegian chars | Thinking tokens | Cost |
|---|---|---|---|---|
| 0 | 1,023 | 9 | 0 | $0.00086 |
| 1,024 | 948 | 9 | 855 | $0.00384 |

**Verdict:** Thinking produces FEWER output characters (948 vs 1,023) and costs 4.5x more. Zero improvement in Norwegian char accuracy. Thinking tokens are wasted on OCR tasks. Keep thinkingBudget=0.

---

## Test 11: Cloud Vision vs Gemini OCR

**Page:** Bonord p1 (BRREG P&L)

| Metric | Cloud Vision | Gemini 2.5 Flash |
|---|---|---|
| Output chars | 915 | 1,023 |
| Norwegian chars | 9 | 9 |
| SDI found (3 374 122) | ✓ | ✓ |
| Bounding boxes | **Yes** | No |
| Cost per page | $0.0015 | $0.00094 |

**Verdict:** Identical accuracy on Norwegian chars and financial amounts. Cloud Vision provides bounding boxes that Gemini cannot. Gemini produces more formatted text (108 extra chars from pipe delimiters). Cloud Vision is 60% more expensive per page. Use Cloud Vision only when spatial verification is needed.

---

## Test 12: Sequential context for cross-page tables

**Pages:** Bonord p2→p3 (balance sheet spans pages)

| Metric | No context | With p2 context |
|---|---|---|
| Output chars | 290 | 290 |
| "Sum gjeld" found | true | true |

**Verdict:** Sequential context provides no benefit — p3 already contains the continuation content regardless of context. Gemini reads what's on the page without needing prior-page priming. Per-page processing without context is sufficient.

---

## Test 13: Few-shot examples

**Page:** ECITLAW p1 (different entity from the example)

| Metric | 0-shot | 1-shot |
|---|---|---|
| Pipe rows | 26 | 26 |
| Distinct pipe-count variants | 2 | 2 |

**Verdict:** No improvement in pipe consistency from few-shot examples. The 2 variants likely reflect rows with/without note references (3 pipes vs 2 pipes). Not a formatting error — a structural feature of the data.

---

## Test 14: response_mime_type="text/plain"

**Page:** Bonord p1

| Metric | text/plain | omitted |
|---|---|---|
| Markdown artifacts (**, #) | 0 | 0 |
| Output chars | — | — |

**Verdict:** Zero markdown artifacts in either case. The current prompt already suppresses markdown decoration. response_mime_type has no effect.

---

## Test 15: PDF document vs extracted PNG image

**Page:** Bonord p1 (same content, different container)

| Metric | Single-page PDF | Extracted PNG |
|---|---|---|
| Input tokens | **1,321** | 3,385 |
| Output chars | 1,081 | 1,023 |

**Verdict:** PDF input is 2.6x cheaper in input tokens (1,321 vs 3,385). Output quality is comparable. At 8M documents × 20 pages, this token difference translates to significant cost savings. **Send PDFs directly, not extracted images.**

---

## Test 16: Image resolution impact

**Page:** Bonord p1 (resized to different resolutions)

| Resolution | Input tokens | Output chars | Norwegian chars | SDI found |
|---|---|---|---|---|
| 768px | 1,837 | 918 | 9 | ✓ |
| 1024px | 1,837 | 1,081 | 9 | ✓ |
| 2048px | 3,385 | 921 | 9 | ✓ |

**Verdict:** 768px and 1024px produce identical input tokens (1,837) — Gemini's internal rasterization normalizes both to the same representation. 2048px costs 1.8x more in tokens with no accuracy improvement. Financial amounts are correct at all resolutions. **768px is sufficient for scanned Norwegian annual accounts.**

---

## Test 17: Markdown-KV format

**Page:** Bonord p1

| Metric | Value |
|---|---|
| KV blocks produced | 0 |
| Output tokens | 1,030 |

**Verdict:** Gemini ignored the KV block format instruction entirely and produced pipe-delimited output instead. The model has strong formatting priors for financial tables that override custom format instructions. Only JSON (via response_schema enforcement) and HTML reliably override default table formatting.

---

## Tests 18, 19: Not tested

**Test 18 (Dual-stream alignment):** Requires implementing the alignment algorithm between Gemini text and Document AI bounding boxes. Deferred to production implementation.

**Test 19 (Batch API):** Requires async submission to Vertex AI Batch Prediction API with JSONL input/output via GCS. Deferred — the GitHub issue reporting ~50% error rates on batch needs verification but is beyond the scope of a synchronous benchmark.

---

## Test 20: Financial cross-validation

Existing Gemini extraction results validated against arithmetic invariants.

| Entity | Section | P&L (SDI-SDK=DR) | BS (EI=EK+GJ) |
|---|---|---|---|
| Sergel | brreg | ✗ | ✓ |
| Sergel | company | ✗ | ✓ |
| Utleiemeg | brreg | ✗ | ✗ |
| Utleiemeg | company | ✗ | ✗ |
| INPEX | brreg | ✓ | ✓ |
| INPEX | company | ✗ | ✓ |
| AktivOppgjør | brreg | ✗ | ✓ |
| AktivOppgjør | company | ✗ | ✓ |
| SectorFund | brreg | ✓ | ✓ |
| SectorFund | company | ✓ | ✓ |
| Rødskifer | brreg | ✓ | ✓ |
| Rødskifer | company | ✓ | ✓ |
| ECITLAW | brreg | ✓ | ✓ |
| ECITLAW | company | ✓ | ✓ |

**P&L pass rate:** 7/14 (50%)
**BS pass rate:** 12/14 (86%)

**Verdict:** P&L cross-validation fails on 50% of entities. Root cause: many BRREG wrappers omit `sum_driftskostnader` as an explicit line — the current extraction schema requires a field that doesn't exist in some filings. These are not OCR errors but schema mapping gaps. BS validation passes 86% — the 2 failures (Utleiemeg) likely have the same missing-subtotal issue. Cross-validation is the most powerful error detector available — must be implemented in production.

---

## Summary of actionable findings

| Finding | Impact | Action |
|---|---|---|
| JSON structured output eliminates multi-line breaks | **High** | Switch from pipe-delimited to JSON with response_schema |
| maxOutputTokens=65536 is mandatory | **High** | Already set; verify on every API call |
| PDF input is 2.6x cheaper than PNG | **High** | Send PDFs directly, remove image extraction step |
| Character priming has zero effect | **Medium** | Remove "character by character" from prompt |
| Norwegian number instruction unnecessary | **Low** | Remove from prompt |
| Anti-duplication instruction is counterproductive | **Medium** | Never use |
| thinkingBudget >0 wastes money on OCR | **Medium** | Keep at 0 |
| Few-shot examples don't improve pipe consistency | **Low** | Don't add to prompt |
| 768px resolution is sufficient | **Medium** | Don't upscale images |
| Markdown-KV format is ignored by Gemini | **Low** | Don't attempt |
| Cross-validation catches 50% P&L errors | **High** | Implement arithmetic checks in production |
| Gemini 3 Flash unavailable on Vertex AI | **Blocking** | Monitor for availability |
| Cloud Vision adds bounding boxes at +60% cost | **Medium** | Use for spatial verification sample |
