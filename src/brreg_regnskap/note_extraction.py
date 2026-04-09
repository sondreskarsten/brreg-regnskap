"""Extract structured note disclosures from parsed annual accounts text.

Designed for klientkonto identification but extracts all detectable note types.
Input: raw text output from ParseExtract API (list of page strings or single string).
Output: list of NoteExtraction records with typed fields.

Usage:
    from brreg_regnskap.note_extraction import extract_notes, NoteExtraction
    pages = parseextract_response["text"]  # list[str] from API
    notes = extract_notes(orgnr="984272170", year=2024, pages=pages)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict


@dataclass
class NoteExtraction:
    orgnr: str
    year: int
    has_klientmidler: bool = False
    klientmidler_amount: float | None = None
    klientansvar_amount: float | None = None
    klientkonto_balance: float | None = None
    has_bundne_midler: bool = False
    bundne_midler_amount: float | None = None
    has_nettopresentasjon: bool = False
    has_inkasso_forskrift: bool = False
    has_felleskostnader: bool = False
    felleskostnader_amount: float | None = None
    has_forretningsforer: bool = False
    note_excerpts: list[str] = field(default_factory=list)


_SEPARATORS = re.compile(r"[\s\xa0]")
_TABLE_ROW_RE = re.compile(r"\|\s*(.+?)\s*\|\s*([\d\s]{4,})\s*\|")
_LARGE_NUM_RE = re.compile(r"(?<!\d)([\d]{4,}(?:[\s\xa0]\d{3})*)\b")


def _clean_amount(raw: str) -> float | None:
    cleaned = _SEPARATORS.sub("", raw.strip().lstrip("-"))
    if not cleaned or not cleaned.isdigit():
        return None
    val = float(cleaned)
    if "-" in raw.strip()[:2]:
        val = -val
    return val


def _find_table_amount(text: str, row_label: str) -> float | None:
    for m in _TABLE_ROW_RE.finditer(text):
        if row_label.lower() in m.group(1).lower():
            val = _clean_amount(m.group(2))
            if val is not None and abs(val) > 5000 and not (1990 <= abs(val) <= 2030):
                return abs(val)
    return None


def _find_amount_near(text: str, keyword: str, window: int = 300) -> float | None:
    idx = text.lower().find(keyword.lower())
    if idx == -1:
        return None
    chunk = text[idx:idx + window]
    table_val = _find_table_amount(chunk, keyword.split()[-1])
    if table_val:
        return table_val
    for m in _LARGE_NUM_RE.finditer(chunk):
        val = _clean_amount(m.group(1))
        if val is not None and abs(val) > 1000:
            return abs(val)
    return None


def _context_window(text: str, keyword: str, radius: int = 500) -> str | None:
    idx = text.lower().find(keyword.lower())
    if idx == -1:
        return None
    start = max(0, idx - radius)
    end = min(len(text), idx + radius)
    return text[start:end]


def extract_notes(orgnr: str, year: int, pages: list[str] | str) -> NoteExtraction:
    if isinstance(pages, list):
        full = "\n\n".join(pages)
    else:
        full = pages

    low = full.lower()
    result = NoteExtraction(orgnr=orgnr, year=year)

    if "klientmidl" in low or "klientinnskudd" in low or "klientkonto" in low or "klientansvar" in low:
        result.has_klientmidler = True
        result.klientmidler_amount = (
            _find_table_amount(full, "Klientinnskudd")
            or _find_amount_near(full, "Innestående på klientkonto")
            or _find_table_amount(full, "Klientmidler")
        )
        result.klientansvar_amount = (
            _find_table_amount(full, "Klientansvar")
            or _find_table_amount(full, "Klientgjeld")
        )
        if result.klientmidler_amount and result.klientansvar_amount:
            result.klientkonto_balance = result.klientmidler_amount - abs(result.klientansvar_amount)

        ctx = (
            _context_window(full, "klientmidl")
            or _context_window(full, "klientinnskudd")
            or _context_window(full, "klientkonto")
        )
        if ctx:
            result.note_excerpts.append(ctx.strip())

    if "betrodde midl" in low:
        result.has_klientmidler = True
        ctx = _context_window(full, "betrodde midl")
        if ctx:
            result.note_excerpts.append(ctx.strip())

    if "nettopresentasjon" in low or "regnskapsføres ikke i selskapets balanse" in low:
        result.has_nettopresentasjon = True

    if "bundne" in low:
        result.has_bundne_midler = True
        result.bundne_midler_amount = (
            _find_table_amount(full, "bundne bankinnskudd")
            or _find_amount_near(full, "bundne bankinnskudd")
            or _find_amount_near(full, "bundne skattetrekksmidler")
        )

    if "forskrift om årsregnskap" in low and "inkasso" in low:
        result.has_inkasso_forskrift = True

    if "felleskostnad" in low:
        result.has_felleskostnader = True
        result.felleskostnader_amount = (
            _find_table_amount(full, "felleskostnad")
            or _find_amount_near(full, "felleskostnad")
        )

    if "forretningsfører" in low or "forretningsførerhonorar" in low:
        result.has_forretningsforer = True

    return result


def extractions_to_rows(extractions: list[NoteExtraction]) -> list[dict]:
    rows = []
    for e in extractions:
        d = asdict(e)
        d["note_excerpts"] = "\n---\n".join(e.note_excerpts) if e.note_excerpts else None
        rows.append(d)
    return rows
