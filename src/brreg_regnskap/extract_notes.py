"""Batch extract notes from annual accounts PDFs.

Downloads PDFs from {storage_path}/regnskap/{orgnr}/aarsregnskap_{year}.pdf,
sends to ParseExtract API for OCR, runs note_extraction pattern matching,
saves per-entity JSON and consolidated parquet.

Deduplication: checks for existing notes/{orgnr}/notes_{year}.json before
sending to ParseExtract. Pass --force to re-extract.

Usage:
    python -m brreg_regnskap.extract_notes --orgnrs 984272170,988054631 --year 2024
    python -m brreg_regnskap.extract_notes --orgnrs-file orgnrs.txt --years 2022,2023,2024
    python -m brreg_regnskap.extract_notes --force --orgnrs 984272170 --year 2024

Environment:
    PARSEEXTRACT_API_KEY  - API key for parseextract.com
    BRREG_STORAGE_PATH    - Storage root (default: gs://brreg-regnskap)
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from brreg_regnskap.config import Settings
from brreg_regnskap.note_extraction import NoteExtraction, extract_notes, extractions_to_rows
from brreg_regnskap.regnskap_extraction import RegnskapExtraction, extract_regnskap, regnskap_to_row
from brreg_regnskap.parseextract import ParseExtractClient, ParseExtractError
from brreg_regnskap.storage import StorageBackend


def _load_existing(storage: StorageBackend, settings: Settings, orgnr: str, year: int) -> NoteExtraction | None:
    path = settings.notes_json_path(orgnr, year)
    if not storage.exists(path):
        return None
    raw = storage.read_bytes(path)
    data = json.loads(raw)
    excerpts = data.pop("note_excerpts", None) or []
    if isinstance(excerpts, str):
        excerpts = [s for s in excerpts.split("\n---\n") if s.strip()]
    return NoteExtraction(**{**data, "note_excerpts": excerpts})


def _save_extraction(storage: StorageBackend, settings: Settings, extraction: NoteExtraction) -> None:
    path = settings.notes_json_path(extraction.orgnr, extraction.year)
    d = asdict(extraction)
    d["note_excerpts"] = "\n---\n".join(extraction.note_excerpts) if extraction.note_excerpts else None
    storage.write_bytes(path, json.dumps(d, ensure_ascii=False, indent=2, default=str).encode("utf-8"))


def _save_ocr(storage: StorageBackend, settings: Settings, orgnr: str, year: int, pages: list[str]) -> None:
    path = settings.regnskap_ocr_path(orgnr, year)
    text = "\n\n---PAGE BREAK---\n\n".join(pages)
    storage.write_bytes(path, text.encode("utf-8"))


def _save_regnskap(storage: StorageBackend, settings: Settings, extraction: RegnskapExtraction) -> None:
    path = settings.regnskap_items_path(extraction.orgnr, extraction.year)
    storage.write_bytes(path, json.dumps(regnskap_to_row(extraction), ensure_ascii=False, indent=2).encode("utf-8"))


def _load_regnskap(storage: StorageBackend, settings: Settings, orgnr: str, year: int) -> RegnskapExtraction | None:
    path = settings.regnskap_items_path(orgnr, year)
    if not storage.exists(path):
        return None
    raw = storage.read_bytes(path)
    data = json.loads(raw)
    line_items = {}
    prior_year_items = {}
    meta = {}
    for k, v in data.items():
        if k in ("orgnr", "year", "revenue_label", "sections_found"):
            meta[k] = v
        elif k.endswith("_prior"):
            prior_year_items[k.removesuffix("_prior")] = v
        else:
            line_items[k] = v
    sections = meta.get("sections_found", "").split(",") if meta.get("sections_found") else []
    return RegnskapExtraction(
        orgnr=orgnr, year=year,
        line_items=line_items, prior_year_items=prior_year_items,
        revenue_label=meta.get("revenue_label"), sections_found=sections,
    )


def _download_pdf(storage: StorageBackend, settings: Settings, orgnr: str, year: int) -> bytes | None:
    for version in [1, 2, 3]:
        path = settings.regnskap_pdf_path(orgnr, year, version)
        if storage.exists(path):
            return storage.read_bytes(path)
    return None


def _list_available_years(storage: StorageBackend, settings: Settings, orgnr: str) -> list[int]:
    prefix = f"{settings.storage_path}/regnskap/{orgnr}/"
    try:
        entries = storage.list_dir(prefix)
    except Exception:
        return []
    years = set()
    for entry in entries:
        name = entry.rsplit("/", 1)[-1]
        if name.startswith("aarsregnskap_") and name.endswith(".pdf"):
            yr_str = name.replace("aarsregnskap_", "").replace(".pdf", "").split("_")[0]
            try:
                years.add(int(yr_str))
            except ValueError:
                pass
    return sorted(years)


def _consolidate(storage: StorageBackend, settings: Settings,
                 note_results: list[NoteExtraction],
                 regnskap_results: list[RegnskapExtraction]) -> None:
    if note_results:
        _consolidate_one(storage, settings.notes_consolidated_path,
                         extractions_to_rows(note_results), key_cols=("orgnr", "year"))

    if regnskap_results:
        _consolidate_one(storage, settings.regnskap_consolidated_path,
                         [regnskap_to_row(r) for r in regnskap_results], key_cols=("orgnr", "year"))


def _consolidate_one(storage: StorageBackend, path: str, new_rows: list[dict],
                     key_cols: tuple[str, ...]) -> None:
    existing_rows: list[dict] = []
    if storage.exists(path):
        raw = storage.read_bytes(path)
        existing = pq.read_table(io.BytesIO(raw))
        existing_rows = existing.to_pylist()

    existing_keys = {tuple(r[k] for k in key_cols) for r in existing_rows}
    for row in new_rows:
        key = tuple(row[k] for k in key_cols)
        if key in existing_keys:
            existing_rows = [r for r in existing_rows if tuple(r[k] for k in key_cols) != key]
        existing_rows.append(row)

    existing_rows.sort(key=lambda r: tuple(r.get(k, "") for k in key_cols))
    table = pa.Table.from_pylist(existing_rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    storage.write_bytes(path, buf.getvalue())
    print(f"Consolidated {len(existing_rows)} rows → {path}", file=sys.stderr)


def run_extraction(
    orgnrs: list[str],
    years: list[int],
    api_key: str,
    settings: Settings | None = None,
    force: bool = False,
    delay: float = 2.0,
) -> tuple[list[NoteExtraction], list[RegnskapExtraction]]:

    if settings is None:
        settings = Settings()
    storage = StorageBackend.from_settings(settings)
    pe = ParseExtractClient(api_key=api_key)
    note_results: list[NoteExtraction] = []
    regnskap_results: list[RegnskapExtraction] = []

    total = len(orgnrs) * len(years)
    done = 0
    skipped = 0

    for orgnr in orgnrs:
        for year in years:
            done += 1

            if not force:
                existing_note = _load_existing(storage, settings, orgnr, year)
                existing_regn = _load_regnskap(storage, settings, orgnr, year)
                if existing_note is not None and existing_regn is not None:
                    note_results.append(existing_note)
                    regnskap_results.append(existing_regn)
                    skipped += 1
                    print(f"[{done}/{total}] {orgnr}/{year}: cached", file=sys.stderr)
                    continue

            pdf = _download_pdf(storage, settings, orgnr, year)
            if pdf is None:
                print(f"[{done}/{total}] {orgnr}/{year}: no PDF", file=sys.stderr)
                continue

            print(f"[{done}/{total}] {orgnr}/{year}: {len(pdf):,} bytes", end="", file=sys.stderr)
            try:
                pages = pe.extract_bytes(pdf, filename=f"{orgnr}_{year}.pdf")

                _save_ocr(storage, settings, orgnr, year, pages)

                note_ext = extract_notes(orgnr, year, pages)
                note_results.append(note_ext)
                _save_extraction(storage, settings, note_ext)

                regn_ext = extract_regnskap(orgnr, year, pages)
                regnskap_results.append(regn_ext)
                _save_regnskap(storage, settings, regn_ext)

                flags = []
                if note_ext.has_klientmidler:
                    flags.append("KLIENT")
                if note_ext.has_bundne_midler:
                    flags.append("BUNDNE")
                if note_ext.has_nettopresentasjon:
                    flags.append("NETTO")
                if note_ext.has_inkasso_forskrift:
                    flags.append("INKASSO")
                if note_ext.has_felleskostnader:
                    flags.append("FELLESK")
                if note_ext.has_forretningsforer:
                    flags.append("FORRF")
                n_items = len(regn_ext.line_items)
                flag_str = " [" + ",".join(flags) + "]" if flags else ""
                print(f" → {len(pages)} pages, {n_items} items{flag_str}", file=sys.stderr)
            except ParseExtractError as e:
                print(f" → ERROR: {e}", file=sys.stderr)
            except Exception as e:
                print(f" → ERROR: {type(e).__name__}: {e}", file=sys.stderr)

            time.sleep(delay)

    _consolidate(storage, settings, note_results, regnskap_results)

    print(f"\nDone: {done} total, {skipped} cached, {done - skipped} processed", file=sys.stderr)
    return note_results, regnskap_results


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract notes from annual accounts PDFs")
    parser.add_argument("--orgnrs", help="Comma-separated orgnrs")
    parser.add_argument("--orgnrs-file", help="File with one orgnr per line")
    parser.add_argument("--years", default="2024", help="Comma-separated years (default: 2024)")
    parser.add_argument("--force", action="store_true", help="Re-extract even if cached")
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--output", help="Additional output parquet path (local or gs://)")
    args = parser.parse_args()

    api_key = os.environ.get("PARSEEXTRACT_API_KEY", "")
    if not api_key:
        print("PARSEEXTRACT_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    settings = Settings()

    orgnrs: list[str] = []
    if args.orgnrs:
        orgnrs = [o.strip() for o in args.orgnrs.split(",")]
    elif args.orgnrs_file:
        orgnrs = [l.strip() for l in Path(args.orgnrs_file).read_text().strip().split("\n") if l.strip()]

    if not orgnrs:
        print("No orgnrs specified", file=sys.stderr)
        sys.exit(1)

    years = [int(y) for y in args.years.split(",")]

    note_results, regnskap_results = run_extraction(
        orgnrs=orgnrs,
        years=years,
        api_key=api_key,
        settings=settings,
        force=args.force,
        delay=args.delay,
    )

    if args.output:
        rows = extractions_to_rows(note_results)
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, args.output)
        print(f"Saved {len(rows)} rows to {args.output}", file=sys.stderr)

    combined = []
    for n, r in zip(note_results, regnskap_results):
        row = {**asdict(n), **regnskap_to_row(r)}
        row["note_excerpts"] = "\n---\n".join(n.note_excerpts) if n.note_excerpts else None
        combined.append(row)
    print(json.dumps(combined, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
