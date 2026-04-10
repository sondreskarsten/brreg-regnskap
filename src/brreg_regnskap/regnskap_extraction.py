"""Extract structured P&L and balance sheet line items from OCR text.

Parses the BRREG standard annual accounts format into a flat dict of
recognized line items with current-year and prior-year amounts.

The BRREG JSON API only provides sum-level fields (sumDriftsinntekter,
sumEiendeler, etc.). This module extracts sub-line items from OCR text
that are invisible to the JSON API, such as:
  - Salgsinntekt vs AnnenDriftsinntekt breakdown
  - Renteinntekt fra foretak i samme konsern
  - Kundefordringer, leverandørgjeld, kassekreditt
  - Aksjekapital, overkurs, annen egenkapital

Usage:
    from brreg_regnskap.regnskap_extraction import extract_regnskap
    result = extract_regnskap(orgnr, year, pages)
    result.line_items  # dict[str, float]
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_AMOUNT_RE = re.compile(r"(-?\d[\d\s\xa0]{2,})")
_SEPARATORS = re.compile(r"[\s\xa0]")

SECTION_MARKERS = {
    "pnl": ["RESULTATREGNSKAP"],
    "assets": ["BALANSE - EIENDELER", "BALANSE\nEIENDELER"],
    "equity_debt": ["BALANSE - EGENKAPITAL OG GJELD", "EGENKAPITAL OG GJELD"],
    "notes": ["NOTEOPPLYSNINGER", "Noter til", "Note 0", "Note 1"],
}

LINE_ITEM_MAP = {
    "salgsinntekt": "salgsinntekt",
    "salær- og provisjonsinntekt": "salgsinntekt",
    "salær og provisjonsinntekt": "salgsinntekt",
    "annen driftsinntekt": "annen_driftsinntekt",
    "sum inntekter": "sum_driftsinntekter",
    "sum driftsinntekter": "sum_driftsinntekter",
    "varekostnad": "varekostnad",
    "lønnskostnad": "lonnskostnad",
    "avskrivning": "avskrivning",
    "nedskrivning": "nedskrivning",
    "annen driftskostnad": "annen_driftskostnad",
    "sum kostnader": "sum_driftskostnader",
    "sum driftskostnader": "sum_driftskostnader",
    "driftsresultat": "driftsresultat",
    "renteinntekt fra foretak i samme konsern": "renteinntekt_konsern",
    "annen renteinntekt": "annen_renteinntekt",
    "annen finansinntekt": "annen_finansinntekt",
    "sum finansinntekter": "sum_finansinntekter",
    "rentekostnad til foretak i samme konsern": "rentekostnad_konsern",
    "annen rentekostnad": "annen_rentekostnad",
    "annen finanskostnad": "annen_finanskostnad",
    "sum finanskostnader": "sum_finanskostnader",
    "netto finans": "netto_finans",
    "resultat før skattekostnad": "resultat_for_skatt",
    "ordinært resultat før skattekostnad": "resultat_for_skatt",
    "skattekostnad": "skattekostnad",
    "skattekostnad på resultat": "skattekostnad",
    "skattekostnad på ordinært resultat": "skattekostnad",
    "årsresultat": "aarsresultat",
    "ordinært resultat etter skattekostnad": "ord_resultat_etter_skatt",
    "totalresultat": "totalresultat",
    "ordinært utbytte": "utbytte",
    "konsernbidrag": "konsernbidrag",
    "sum eiendeler": "sum_eiendeler",
    "sum anleggsmidler": "sum_anleggsmidler",
    "sum omløpsmidler": "sum_omlopsmidler",
    "kundefordringer": "kundefordringer",
    "andre fordringer": "andre_fordringer",
    "sum fordringer": "sum_fordringer",
    "bankinnskudd, kontanter og lignende": "bankinnskudd",
    "sum bankinnskudd, kontanter og lignende": "bankinnskudd",
    "sum egenkapital": "sum_egenkapital",
    "aksjekapital": "aksjekapital",
    "overkurs": "overkurs",
    "sum innskutt egenkapital": "sum_innskutt_egenkapital",
    "annen egenkapital": "annen_egenkapital",
    "sum opptjent egenkapital": "sum_opptjent_egenkapital",
    "sum langsiktig gjeld": "sum_langsiktig_gjeld",
    "leverandørgjeld": "leverandorgjeld",
    "skyldige offentlige avgifter": "skyldige_offentlige_avgifter",
    "annen kortsiktig gjeld": "annen_kortsiktig_gjeld",
    "sum kortsiktig gjeld": "sum_kortsiktig_gjeld",
    "sum gjeld": "sum_gjeld",
    "sum egenkapital og gjeld": "sum_egenkapital_gjeld",
    "pantstillelse": "pantstillelse",
    "kassekreditt": "kassekreditt",
    "kassekredittlimit": "kassekredittlimit",
    "innbetalt felleskostnader": "felleskostnader",
    "felleskostnader": "felleskostnader",
    "forretningsførerhonorar": "forretningsforerhonorar",
    "revisjonshonorar": "revisjonshonorar",
    "styrehonorar": "styrehonorar",
}


def _parse_amount(raw: str) -> float | None:
    cleaned = _SEPARATORS.sub("", raw.strip())
    neg = cleaned.startswith("-")
    digits = cleaned.lstrip("-")
    if not digits or not digits.isdigit():
        return None
    val = float(digits)
    if 1990 <= val <= 2030:
        return None
    return -val if neg else val


def _find_amounts_on_line(line: str) -> list[float]:
    amounts = []
    for m in _AMOUNT_RE.finditer(line):
        val = _parse_amount(m.group(1))
        if val is not None:
            amounts.append(val)
    return amounts


def _normalize_label(raw: str) -> str:
    raw = raw.strip().rstrip("|").strip()
    raw = re.sub(r"\|\s*[\d,\s]*\s*\|", "", raw)
    raw = re.sub(r"\s+\d+\s*$", "", raw)
    raw = raw.strip().rstrip("|").strip()
    return raw.lower()


@dataclass
class RegnskapExtraction:
    orgnr: str
    year: int
    line_items: dict[str, float] = field(default_factory=dict)
    prior_year_items: dict[str, float] = field(default_factory=dict)
    revenue_label: str | None = None
    sections_found: list[str] = field(default_factory=list)


def _find_section_boundaries(text: str) -> dict[str, int]:
    boundaries = {}
    low = text.lower()
    for section, markers in SECTION_MARKERS.items():
        for marker in markers:
            idx = low.find(marker.lower())
            if idx >= 0:
                if section not in boundaries or idx < boundaries[section]:
                    boundaries[section] = idx
    return boundaries


def _extract_section(text: str, start: int, end: int) -> dict[str, tuple[float | None, float | None]]:
    chunk = text[start:end]
    items: dict[str, tuple[float | None, float | None]] = {}

    for line in chunk.split("\n"):
        line = line.strip()
        if not line or line.startswith("---") or line.startswith("Beløp i"):
            continue
        if line.startswith("Utskriftsdato") or line.startswith("Organisasjonsnr"):
            continue

        label = _normalize_label(line.split("|")[1] if "|" in line else line)
        if not label or len(label) < 3:
            continue

        matched_key = None
        for pattern, key in LINE_ITEM_MAP.items():
            if pattern in label:
                matched_key = key
                break

        if matched_key is None:
            continue

        amounts = _find_amounts_on_line(line)
        current = amounts[0] if len(amounts) >= 1 else None
        prior = amounts[1] if len(amounts) >= 2 else None

        if matched_key not in items or (current is not None and items[matched_key][0] is None):
            items[matched_key] = (current, prior)

    return items


def extract_regnskap(orgnr: str, year: int, pages: list[str] | str) -> RegnskapExtraction:
    if isinstance(pages, list):
        full = "\n\n".join(pages)
    else:
        full = pages

    result = RegnskapExtraction(orgnr=orgnr, year=year)
    boundaries = _find_section_boundaries(full)
    result.sections_found = list(boundaries.keys())

    sorted_bounds = sorted(boundaries.items(), key=lambda x: x[1])

    for i, (section, start) in enumerate(sorted_bounds):
        if section == "notes":
            continue
        end = sorted_bounds[i + 1][1] if i + 1 < len(sorted_bounds) else len(full)
        items = _extract_section(full, start, end)
        for key, (current, prior) in items.items():
            if current is not None:
                result.line_items[key] = current
            if prior is not None:
                result.prior_year_items[key] = prior

    if "salgsinntekt" in result.line_items:
        low = full.lower()
        idx = low.find("salær")
        if idx >= 0 and idx < boundaries.get("pnl", len(full)) + 2000:
            result.revenue_label = "salær_provisjon"
        else:
            result.revenue_label = "salgsinntekt"
    elif "annen_driftsinntekt" in result.line_items and "salgsinntekt" not in result.line_items:
        result.revenue_label = "annen_driftsinntekt_only"

    return result


def regnskap_to_row(extraction: RegnskapExtraction) -> dict:
    row = {
        "orgnr": extraction.orgnr,
        "year": extraction.year,
        "revenue_label": extraction.revenue_label,
        "sections_found": ",".join(extraction.sections_found),
    }
    for key, val in extraction.line_items.items():
        row[key] = val
    for key, val in extraction.prior_year_items.items():
        row[f"{key}_prior"] = val
    return row
