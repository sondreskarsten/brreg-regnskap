"""Extraction store for section-level årsregnskap data.

Stores Gemini extraction results as hive-partitioned parquet in GCS.
Each extraction run writes one batch file. Hive partitioning by
collection_date and collection_time prevents incidental overwrites —
a new run creates a new partition rather than clobbering existing data.

Storage layout:
    gs://brreg-regnskap/extraction/store/
    ├── manifests/
    │   └── collection_date={YYYY-MM-DD}/
    │       └── collection_time={HHMMSS}/
    │           └── manifests.parquet          # page_classifier output
    ├── brreg_financials/
    │   └── collection_date={YYYY-MM-DD}/
    │       └── collection_time={HHMMSS}/
    │           └── brreg_financials.parquet   # P&L + BS from BRREG wrapper
    ├── noter/
    │   └── collection_date={YYYY-MM-DD}/
    │       └── collection_time={HHMMSS}/
    │           └── noter.parquet              # note-level extraction
    ├── note_flags/
    │   └── collection_date={YYYY-MM-DD}/
    │       └── collection_time={HHMMSS}/
    │           └── note_flags.parquet         # entity-level flag signals
    └── revisjon/
        └── collection_date={YYYY-MM-DD}/
            └── collection_time={HHMMSS}/
                └── revisjon.parquet          # auditor report extraction

Primary key: (pdf_sha256_prefix, orgnr) — pdf_sha256_prefix is the
content-addressable identifier for the specific PDF version processed.
Same PDF processed twice produces identical pdf_sha256_prefix, making
deduplication trivial on read (latest collection_time wins).

Reading pattern:
    # Read all noter across all collection runs, dedup by pdf hash
    df = pq.read_table("gs://brreg-regnskap/extraction/store/noter/")
    df = df.sort_by("collection_time").group_by("pdf_sha256_prefix").aggregate(...)

Usage:
    from brreg_regnskap.extraction_store import ExtractionStore
    store = ExtractionStore("gs://brreg-regnskap/extraction/store")
    store.write_manifests(manifest_records)
    store.write_noter(noter_records)
    store.write_note_flags(flag_records)
    store.write_brreg_financials(brreg_records)
"""

from __future__ import annotations

import io
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage as gcs_storage


MANIFEST_SCHEMA = pa.schema([
    pa.field("pdf_sha256_prefix", pa.string(), nullable=False),
    pa.field("orgnr", pa.string(), nullable=False),
    pa.field("year", pa.int32()),
    pa.field("journalnr", pa.string()),
    pa.field("regnskapsaar", pa.int32()),
    pa.field("periode_start", pa.string()),
    pa.field("periode_slutt", pa.string()),
    pa.field("foretaksnavn", pa.string()),
    pa.field("organisasjonsform", pa.string()),
    pa.field("morselskap_i_konsern", pa.bool_()),
    pa.field("konsernregnskap_vedlagt", pa.bool_()),
    pa.field("regler_smaa_foretak", pa.bool_()),
    pa.field("regnskapsregler", pa.string()),
    pa.field("fravalgt_revisjon", pa.bool_()),
    pa.field("dato_fastsettelse", pa.string()),
    pa.field("regnskapsforer", pa.string()),
    pa.field("total_pages", pa.int32()),
    pa.field("brreg_last_page", pa.int32()),
    pa.field("n_brreg", pa.int32()),
    pa.field("n_company", pa.int32()),
    pa.field("platform", pa.string()),
    pa.field("footer_hash", pa.string()),
    pa.field("konsern_detected", pa.bool_()),
    pa.field("konsern_evidence_pages", pa.int32()),
    pa.field("classifier_version", pa.string()),
    pa.field("file_size_bytes", pa.int64()),
    pa.field("collection_date", pa.string(), nullable=False),
    pa.field("collection_time", pa.string(), nullable=False),
])

BRREG_FINANCIALS_SCHEMA = pa.schema([
    pa.field("pdf_sha256_prefix", pa.string(), nullable=False),
    pa.field("orgnr", pa.string(), nullable=False),
    pa.field("year", pa.int32()),
    pa.field("section", pa.string()),
    pa.field("salgsinntekt", pa.int64()),
    pa.field("annen_driftsinntekt", pa.int64()),
    pa.field("sum_driftsinntekter", pa.int64()),
    pa.field("lonnskostnad", pa.int64()),
    pa.field("avskrivning", pa.int64()),
    pa.field("annen_driftskostnad", pa.int64()),
    pa.field("sum_driftskostnader", pa.int64()),
    pa.field("driftsresultat", pa.int64()),
    pa.field("sum_finansinntekter", pa.int64()),
    pa.field("sum_finanskostnader", pa.int64()),
    pa.field("resultat_for_skatt", pa.int64()),
    pa.field("skattekostnad", pa.int64()),
    pa.field("aarsresultat", pa.int64()),
    pa.field("sum_anleggsmidler", pa.int64()),
    pa.field("sum_omlopsmidler", pa.int64()),
    pa.field("sum_eiendeler", pa.int64()),
    pa.field("sum_egenkapital", pa.int64()),
    pa.field("sum_langsiktig_gjeld", pa.int64()),
    pa.field("sum_kortsiktig_gjeld", pa.int64()),
    pa.field("sum_gjeld", pa.int64()),
    pa.field("extraction_model", pa.string()),
    pa.field("extraction_cost_usd", pa.float64()),
    pa.field("input_tokens", pa.int32()),
    pa.field("output_tokens", pa.int32()),
    pa.field("collection_date", pa.string(), nullable=False),
    pa.field("collection_time", pa.string(), nullable=False),
])

NOTER_SCHEMA = pa.schema([
    pa.field("pdf_sha256_prefix", pa.string(), nullable=False),
    pa.field("orgnr", pa.string(), nullable=False),
    pa.field("year", pa.int32()),
    pa.field("note_nr", pa.string()),
    pa.field("tittel", pa.string()),
    pa.field("note_type", pa.string()),
    pa.field("amounts_json", pa.string()),
    pa.field("n_amounts", pa.int32()),
    pa.field("extraction_model", pa.string()),
    pa.field("extraction_cost_usd", pa.float64()),
    pa.field("collection_date", pa.string(), nullable=False),
    pa.field("collection_time", pa.string(), nullable=False),
])

NOTE_FLAGS_SCHEMA = pa.schema([
    pa.field("pdf_sha256_prefix", pa.string(), nullable=False),
    pa.field("orgnr", pa.string(), nullable=False),
    pa.field("year", pa.int32()),
    pa.field("antall_ansatte", pa.int32()),
    pa.field("antall_aarsverk", pa.float64()),
    pa.field("otp_pliktig", pa.bool_()),
    pa.field("revisjonshonorar_revisjon", pa.int64()),
    pa.field("bundne_midler_amount", pa.int64()),
    pa.field("skattetrekkskonto", pa.int64()),
    pa.field("has_pantstillelser", pa.bool_()),
    pa.field("pantstillelser_gjeld", pa.int64()),
    pa.field("pantstillelser_bokfort", pa.int64()),
    pa.field("utbytte", pa.int64()),
    pa.field("konsernbidrag", pa.int64()),
    pa.field("fortsatt_drift_tvil", pa.bool_()),
    pa.field("kassekredittlimit", pa.int64()),
    pa.field("kassekreditt_benyttet", pa.int64()),
    pa.field("hendelser_etter_balansedagen", pa.string()),
    pa.field("extraction_model", pa.string()),
    pa.field("extraction_cost_usd", pa.float64()),
    pa.field("n_note_pages", pa.int32()),
    pa.field("collection_date", pa.string(), nullable=False),
    pa.field("collection_time", pa.string(), nullable=False),
])

REVISJON_SCHEMA = pa.schema([
    pa.field("pdf_sha256_prefix", pa.string(), nullable=False),
    pa.field("orgnr", pa.string(), nullable=False),
    pa.field("year", pa.int32()),
    pa.field("revisor", pa.string()),
    pa.field("firma", pa.string()),
    pa.field("konklusjon", pa.string()),
    pa.field("fravalgt", pa.bool_()),
    pa.field("presisering", pa.string()),
    pa.field("collection_date", pa.string(), nullable=False),
    pa.field("collection_time", pa.string(), nullable=False),
])


def _collection_partition() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d"), now.strftime("%H%M%S")


def _safe_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def _safe_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


class ExtractionStore:

    def __init__(self, base_path: str = "gs://brreg-regnskap/extraction/store"):
        self._base = base_path.rstrip("/")
        self._gcs = gcs_storage.Client()
        self._bucket_name = base_path.split("/")[2]
        self._prefix = "/".join(base_path.split("/")[3:])
        self._bucket = self._gcs.bucket(self._bucket_name)

    def _write_parquet(self, section: str, table: pa.Table, cdate: str, ctime: str):
        path = f"{self._prefix}/{section}/collection_date={cdate}/collection_time={ctime}/{section}.parquet"
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd")
        blob = self._bucket.blob(path)
        blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
        return f"gs://{self._bucket_name}/{path}"

    def write_manifests(self, records: list[dict], cdate: str | None = None, ctime: str | None = None) -> str:
        if cdate is None or ctime is None:
            cdate, ctime = _collection_partition()
        rows = []
        for r in records:
            rows.append({
                "pdf_sha256_prefix": r.get("pdf_sha256_prefix", ""),
                "orgnr": r.get("orgnr", ""),
                "year": r.get("year"),
                "journalnr": r.get("journalnr"),
                "regnskapsaar": _safe_int(r.get("regnskapsaar")),
                "periode_start": r.get("periode_start"),
                "periode_slutt": r.get("periode_slutt"),
                "foretaksnavn": r.get("foretaksnavn"),
                "organisasjonsform": r.get("organisasjonsform"),
                "morselskap_i_konsern": r.get("morselskap_i_konsern"),
                "konsernregnskap_vedlagt": r.get("konsernregnskap_vedlagt"),
                "regler_smaa_foretak": r.get("regler_smaa_foretak"),
                "regnskapsregler": r.get("regnskapsregler"),
                "fravalgt_revisjon": r.get("fravalgt_revisjon"),
                "dato_fastsettelse": r.get("dato_fastsettelse"),
                "regnskapsforer": r.get("regnskapsforer"),
                "total_pages": r.get("total_pages"),
                "brreg_last_page": r.get("brreg_last_page"),
                "n_brreg": r.get("n_brreg"),
                "n_company": r.get("n_company"),
                "platform": r.get("platform"),
                "footer_hash": r.get("footer_hash"),
                "konsern_detected": r.get("konsern_detected", False),
                "konsern_evidence_pages": r.get("konsern_evidence_pages", 0),
                "classifier_version": r.get("classifier_version", ""),
                "file_size_bytes": r.get("file_size_bytes"),
                "collection_date": cdate,
                "collection_time": ctime,
            })
        table = pa.Table.from_pylist(rows, schema=MANIFEST_SCHEMA)
        return self._write_parquet("manifests", table, cdate, ctime)

    def write_brreg_financials(self, records: list[dict], cdate: str | None = None, ctime: str | None = None) -> str:
        if cdate is None or ctime is None:
            cdate, ctime = _collection_partition()
        rows = []
        for r in records:
            row = {"collection_date": cdate, "collection_time": ctime}
            for field in BRREG_FINANCIALS_SCHEMA:
                name = field.name
                if name in ("collection_date", "collection_time"):
                    continue
                val = r.get(name)
                if pa.types.is_int64(field.type) or pa.types.is_int32(field.type):
                    row[name] = _safe_int(val)
                elif pa.types.is_float64(field.type):
                    row[name] = _safe_float(val)
                else:
                    row[name] = val
            rows.append(row)
        table = pa.Table.from_pylist(rows, schema=BRREG_FINANCIALS_SCHEMA)
        return self._write_parquet("brreg_financials", table, cdate, ctime)

    def write_noter(self, records: list[dict], cdate: str | None = None, ctime: str | None = None) -> str:
        if cdate is None or ctime is None:
            cdate, ctime = _collection_partition()
        import json as _json
        rows = []
        for r in records:
            amounts = r.get("amounts", {})
            rows.append({
                "pdf_sha256_prefix": r.get("pdf_sha256_prefix", ""),
                "orgnr": r.get("orgnr", ""),
                "year": r.get("year"),
                "note_nr": str(r.get("nr", r.get("note_nr", ""))) if r.get("nr") is not None or r.get("note_nr") is not None else None,
                "tittel": r.get("tittel"),
                "note_type": r.get("type", r.get("note_type")),
                "amounts_json": _json.dumps(amounts, ensure_ascii=False) if amounts else None,
                "n_amounts": len(amounts) if isinstance(amounts, dict) else 0,
                "extraction_model": r.get("extraction_model"),
                "extraction_cost_usd": _safe_float(r.get("extraction_cost_usd")),
                "collection_date": cdate,
                "collection_time": ctime,
            })
        table = pa.Table.from_pylist(rows, schema=NOTER_SCHEMA)
        return self._write_parquet("noter", table, cdate, ctime)

    def write_note_flags(self, records: list[dict], cdate: str | None = None, ctime: str | None = None) -> str:
        if cdate is None or ctime is None:
            cdate, ctime = _collection_partition()
        rows = []
        for r in records:
            row = {"collection_date": cdate, "collection_time": ctime}
            for field in NOTE_FLAGS_SCHEMA:
                name = field.name
                if name in ("collection_date", "collection_time"):
                    continue
                val = r.get(name)
                if pa.types.is_int64(field.type) or pa.types.is_int32(field.type):
                    row[name] = _safe_int(val)
                elif pa.types.is_float64(field.type):
                    row[name] = _safe_float(val)
                elif pa.types.is_boolean(field.type):
                    row[name] = bool(val) if val is not None else None
                else:
                    row[name] = val
            rows.append(row)
        table = pa.Table.from_pylist(rows, schema=NOTE_FLAGS_SCHEMA)
        return self._write_parquet("note_flags", table, cdate, ctime)

    def write_revisjon(self, records: list[dict], cdate: str | None = None, ctime: str | None = None) -> str:
        if cdate is None or ctime is None:
            cdate, ctime = _collection_partition()
        rows = []
        for r in records:
            rows.append({
                "pdf_sha256_prefix": r.get("pdf_sha256_prefix", ""),
                "orgnr": r.get("orgnr", ""),
                "year": r.get("year"),
                "revisor": r.get("revisor"),
                "firma": r.get("firma"),
                "konklusjon": r.get("konklusjon"),
                "fravalgt": r.get("fravalgt", False),
                "presisering": r.get("presisering"),
                "collection_date": cdate,
                "collection_time": ctime,
            })
        table = pa.Table.from_pylist(rows, schema=REVISJON_SCHEMA)
        return self._write_parquet("revisjon", table, cdate, ctime)

    def manifest_from_classifier(self, classifier_output: dict, generell_info: dict | None = None) -> dict:
        doc = classifier_output.get("document", {})
        split = classifier_output.get("split", {})
        platform = classifier_output.get("platform", {})
        konsern = classifier_output.get("konsern", {})
        clf = classifier_output.get("classifier", {})
        gi = generell_info or {}
        return {
            "pdf_sha256_prefix": doc.get("pdf_sha256_prefix"),
            "orgnr": doc.get("orgnr"),
            "year": doc.get("year"),
            "journalnr": gi.get("journalnr"),
            "regnskapsaar": gi.get("regnskapsaar"),
            "periode_start": gi.get("periode_start"),
            "periode_slutt": gi.get("periode_slutt"),
            "foretaksnavn": gi.get("foretaksnavn"),
            "organisasjonsform": gi.get("organisasjonsform"),
            "morselskap_i_konsern": gi.get("morselskap_i_konsern"),
            "konsernregnskap_vedlagt": gi.get("konsernregnskap_vedlagt"),
            "regler_smaa_foretak": gi.get("regler_smaa_foretak"),
            "regnskapsregler": gi.get("regnskapsregler"),
            "fravalgt_revisjon": gi.get("fravalgt_revisjon"),
            "dato_fastsettelse": gi.get("dato_fastsettelse"),
            "regnskapsforer": gi.get("regnskapsforer"),
            "total_pages": doc.get("total_pages"),
            "brreg_last_page": split.get("brreg_last_page"),
            "n_brreg": split.get("n_brreg"),
            "n_company": split.get("n_company"),
            "platform": platform.get("id"),
            "footer_hash": platform.get("footer_hash"),
            "konsern_detected": konsern.get("detected", False),
            "konsern_evidence_pages": len(konsern.get("evidence", [])),
            "classifier_version": clf.get("version"),
            "file_size_bytes": doc.get("file_size_bytes"),
        }


def parse_generell_info(ocr_text: str) -> dict:
    """Parse structured fields from BRREG generell_info page OCR text.

    Extracts: journalnr, regnskapsaar, periode_start/slutt, foretaksnavn,
    organisasjonsform, morselskap_i_konsern, konsernregnskap_vedlagt,
    regler_smaa_foretak, regnskapsregler, fravalgt_revisjon,
    dato_fastsettelse, regnskapsforer.
    """
    import re

    def _find_bool_after(label: str, text: str) -> bool | None:
        idx = text.find(label)
        if idx == -1:
            return None
        after = text[idx + len(label):idx + len(label) + 150]
        m = re.search(r'\b(Ja|Nei)\b', after)
        if m:
            return m.group(1) == "Ja"
        return None

    result = {}

    m = re.search(r'REGNSKAPSÅRET\s+(\d{4})', ocr_text)
    result["regnskapsaar"] = int(m.group(1)) if m else None

    m = re.search(r'(\d{4})\s+(\d{4,7})', ocr_text)
    result["journalnr"] = f"{m.group(1)}/{m.group(2)}" if m else None

    m = re.search(r'(\d{2}\.\d{2}\.\d{4})\s*[-–]?\s*\n?\s*(\d{2}\.\d{2}\.\d{4})', ocr_text)
    result["periode_start"] = m.group(1) if m else None
    result["periode_slutt"] = m.group(2) if m else None

    lines = ocr_text.split("\n")
    result["foretaksnavn"] = None
    for i, line in enumerate(lines):
        if line.strip().startswith("Foretaksnavn"):
            for j in range(i + 1, min(i + 5, len(lines))):
                candidate = lines[j].strip()
                if (candidate and
                    candidate not in ("Forretningsadresse:", "GENERELL INFORMASJON") and
                    not candidate.startswith("Organisasjon") and
                    not re.match(r'^\d{4}\s+\d{4,7}$', candidate) and
                    not re.match(r'^\d{3}\s+\d{3}\s+\d{3}$', candidate) and
                    len(candidate) > 3):
                    result["foretaksnavn"] = candidate
                    break
            break

    m = re.search(r'(Aksjeselskap|Norskregistrert utenlandsk foretak|Samvirkeforetak|'
                  r'Ansvarlig selskap|Allmennaksjeselskap|Enkeltpersonforetak|'
                  r'Borettslag|Stiftelse|Sameie)', ocr_text)
    result["organisasjonsform"] = m.group(1) if m else None

    result["morselskap_i_konsern"] = _find_bool_after("Morselskap i konsern", ocr_text)
    result["konsernregnskap_vedlagt"] = _find_bool_after("Konsernregnskap lagt ved", ocr_text)
    result["regler_smaa_foretak"] = _find_bool_after("Regler for små foretak benyttet", ocr_text)

    m = re.search(r'(Regnskapslovens alminnelige regler|Forenklet IFRS|IFRS)', ocr_text)
    result["regnskapsregler"] = m.group(1) if m else None

    result["fravalgt_revisjon"] = "ikke skal revideres" in ocr_text

    m = re.search(r'fastsettelse.*?(\d{2}\.\d{2}\.\d{4})', ocr_text, re.DOTALL)
    result["dato_fastsettelse"] = m.group(1) if m else None

    if result.get("fravalgt_revisjon"):
        m = re.search(r'regnskapsfører.*?\n\s*([A-ZÆØÅ][\wÆØÅæøå\s&.,-]+?)(?:\n|$)', ocr_text, re.IGNORECASE)
        result["regnskapsforer"] = m.group(1).strip() if m else None
    else:
        result["regnskapsforer"] = None

    return result
