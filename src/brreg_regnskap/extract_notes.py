"""Batch extract notes from annual accounts PDFs.

Downloads PDFs from gs://brreg-regnskap/regnskap/{orgnr}/aarsregnskap_{year}.pdf,
sends to ParseExtract API, runs note_extraction, saves structured parquet.

Usage:
    python -m brreg_regnskap.extract_notes --orgnrs 984272170,988054631 --year 2024
    python -m brreg_regnskap.extract_notes --orgnrs-file orgnrs.txt --years 2022,2023,2024
    python -m brreg_regnskap.extract_notes --all-klientkonto --year 2024

Environment:
    PARSEEXTRACT_API_KEY  - API key for parseextract.com
    GOOGLE_APPLICATION_CREDENTIALS - GCS service account
    BRREG_STORAGE_PATH - GCS bucket (default: gs://brreg-regnskap)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from brreg_regnskap.note_extraction import NoteExtraction, extract_notes, extractions_to_rows
from brreg_regnskap.parseextract import ParseExtractClient, ParseExtractError


def _get_gcs_client():
    from google.cloud import storage
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(creds_path)
        return storage.Client(credentials=creds, project=creds.project_id)
    return storage.Client()


def _download_pdf(gcs_client, bucket_name: str, orgnr: str, year: int) -> bytes | None:
    bucket = gcs_client.bucket(bucket_name)
    for suffix in ["", "_v2", "_v3"]:
        blob = bucket.blob(f"regnskap/{orgnr}/aarsregnskap_{year}{suffix}.pdf")
        if blob.exists():
            return blob.download_as_bytes()
    return None


def _list_available_pdfs(gcs_client, bucket_name: str, orgnr: str) -> list[int]:
    bucket = gcs_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"regnskap/{orgnr}/aarsregnskap_"))
    years = set()
    for b in blobs:
        name = b.name.rsplit("/", 1)[-1]
        if name.startswith("aarsregnskap_") and name.endswith(".pdf"):
            yr_str = name.replace("aarsregnskap_", "").replace(".pdf", "").split("_")[0]
            try:
                years.add(int(yr_str))
            except ValueError:
                pass
    return sorted(years)


def run_extraction(
    orgnrs: list[str],
    years: list[int],
    api_key: str,
    bucket_name: str = "brreg-regnskap",
    output_path: str | None = None,
    delay: float = 2.0,
) -> list[NoteExtraction]:

    gcs = _get_gcs_client()
    pe = ParseExtractClient(api_key=api_key)
    results: list[NoteExtraction] = []

    total = len(orgnrs) * len(years)
    done = 0

    for orgnr in orgnrs:
        for year in years:
            done += 1
            pdf = _download_pdf(gcs, bucket_name, orgnr, year)
            if pdf is None:
                print(f"[{done}/{total}] {orgnr}/{year}: no PDF", file=sys.stderr)
                continue

            print(f"[{done}/{total}] {orgnr}/{year}: {len(pdf):,} bytes", end="", file=sys.stderr)
            try:
                pages = pe.extract_bytes(pdf, filename=f"{orgnr}_{year}.pdf")
                extraction = extract_notes(orgnr, year, pages)
                results.append(extraction)
                flags = []
                if extraction.has_klientmidler:
                    flags.append("KLIENT")
                if extraction.has_bundne_midler:
                    flags.append("BUNDNE")
                if extraction.has_nettopresentasjon:
                    flags.append("NETTO")
                if extraction.has_inkasso_forskrift:
                    flags.append("INKASSO")
                if extraction.has_felleskostnader:
                    flags.append("FELLESK")
                if extraction.has_forretningsforer:
                    flags.append("FORRF")
                flag_str = " [" + ",".join(flags) + "]" if flags else ""
                print(f" → {len(pages)} pages{flag_str}", file=sys.stderr)
            except ParseExtractError as e:
                print(f" → ERROR: {e}", file=sys.stderr)
                results.append(NoteExtraction(orgnr=orgnr, year=year))
            except Exception as e:
                print(f" → ERROR: {type(e).__name__}: {e}", file=sys.stderr)
                results.append(NoteExtraction(orgnr=orgnr, year=year))

            time.sleep(delay)

    if output_path:
        rows = extractions_to_rows(results)
        table = pa.Table.from_pylist(rows)
        if output_path.startswith("gs://"):
            import pyarrow.fs as pafs
            fs = pafs.GcsFileSystem()
            pq.write_table(table, output_path.replace("gs://", ""), filesystem=fs)
        else:
            pq.write_table(table, output_path)
        print(f"Saved {len(rows)} rows to {output_path}", file=sys.stderr)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract notes from annual accounts PDFs")
    parser.add_argument("--orgnrs", help="Comma-separated orgnrs")
    parser.add_argument("--orgnrs-file", help="File with one orgnr per line")
    parser.add_argument("--years", default="2024", help="Comma-separated years (default: 2024)")
    parser.add_argument("--bucket", default="brreg-regnskap")
    parser.add_argument("--output", help="Output parquet path (local or gs://)")
    parser.add_argument("--delay", type=float, default=2.0)
    args = parser.parse_args()

    api_key = os.environ.get("PARSEEXTRACT_API_KEY", "")
    if not api_key:
        print("PARSEEXTRACT_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    orgnrs = []
    if args.orgnrs:
        orgnrs = [o.strip() for o in args.orgnrs.split(",")]
    elif args.orgnrs_file:
        orgnrs = Path(args.orgnrs_file).read_text().strip().split("\n")

    if not orgnrs:
        print("No orgnrs specified", file=sys.stderr)
        sys.exit(1)

    years = [int(y) for y in args.years.split(",")]

    results = run_extraction(
        orgnrs=orgnrs,
        years=years,
        api_key=api_key,
        bucket_name=args.bucket,
        output_path=args.output,
        delay=args.delay,
    )

    print(json.dumps(extractions_to_rows(results), indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
