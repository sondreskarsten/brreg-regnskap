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
    pa.field("klientmidler", pa.int64()),
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
                "amounts_json": _json.dumps(amounts, ensure_ascii=False) if isinstance(amounts, dict) else None,
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

    Uses regex patterns on full-page OCR text as a fallback method.
    For higher accuracy, use parse_generell_info_from_words() with
    Cloud Vision word-level bounding boxes.
    """
    import re

    result = {}

    m = re.search(r'REGNSKAPSÅRET\s+(\d{4})', ocr_text)
    result["regnskapsaar"] = int(m.group(1)) if m else None

    m = re.search(r'(\d{4})\s+(\d{4,7})', ocr_text)
    result["journalnr"] = f"{m.group(1)}/{m.group(2)}" if m else None

    m = re.search(r'(\d{2}\.\d{2}\.\d{4})\s*[-–]?\s*\n?\s*(\d{2}\.\d{2}\.\d{4})', ocr_text)
    result["periode_start"] = m.group(1) if m else None
    result["periode_slutt"] = m.group(2) if m else None

    result["foretaksnavn"] = None
    result["organisasjonsform"] = None
    result["morselskap_i_konsern"] = None
    result["konsernregnskap_vedlagt"] = None
    result["regler_smaa_foretak"] = None

    m = re.search(r'(Regnskapslovens alminnelige regler|Forenklet IFRS|IFRS)', ocr_text)
    result["regnskapsregler"] = m.group(1) if m else None

    result["fravalgt_revisjon"] = "ikke skal revideres" in ocr_text

    m = re.search(r'fastsettelse.*?(\d{2}\.\d{2}\.\d{4})', ocr_text, re.DOTALL)
    result["dato_fastsettelse"] = m.group(1) if m else None

    result["regnskapsforer"] = None

    return result


# Deterministic pixel positions for BRREG generell_info page (1728×2312).
# Calibrated on 5 entities via Cloud Vision word-level bounding boxes.
# All label positions are pixel-identical across entities.
# Value column starts at x≈920.
#
# The page has two layout variants:
#   Non-konsern: morselskap at y=903, smaa_foretak at y=1010
#   Konsern:     morselskap at y=933, konsernregnskap at y=961, smaa_foretak at y=1068
# The konsern variant is detected by checking for a Ja/Nei value at y≈933.

GENERELL_INFO_POSITIONS = {
    "stable": {
        "journalnr":          {"y": 469, "x_min": 920, "x_max": 1200},
        "orgnr":              {"y": 575, "x_min": 920, "x_max": 1100},
        "organisasjonsform":  {"y": 602, "x_min": 920, "x_max": 1300},
        "foretaksnavn":       {"y": 635, "x_min": 920, "x_max": 1600},
        "adresse_line1":      {"y": 663, "x_min": 920, "x_max": 1600},
        "adresse_line2":      {"y": 693, "x_min": 920, "x_max": 1200},
        "periode_start":      {"y": 799, "x_min": 920, "x_max": 1100},
        "periode_slutt":      {"y": 799, "x_min": 1100, "x_max": 1350},
    },
    "non_konsern": {
        "morselskap":         {"y": 903},
        "smaa_foretak":       {"y": 1010},
        "regnskapsregler":    {"y": 1066, "x_min": 920, "x_max": 1500},
        "representant":       {"y": 1170, "x_min": 920, "x_max": 1400},
        "dato_fastsettelse":  {"y": 1201, "x_min": 920, "x_max": 1200},
    },
    "konsern": {
        "morselskap":         {"y": 933},
        "konsernregnskap":    {"y": 961},
        "smaa_foretak":       {"y": 1068},
        "regnskapsregler":    {"y": 1124, "x_min": 920, "x_max": 1500},
        "representant":       {"y": 1232, "x_min": 920, "x_max": 1400},
        "dato_fastsettelse":  {"y": 1259, "x_min": 920, "x_max": 1200},
    },
}


def parse_generell_info_from_words(words: list[dict]) -> dict:
    """Parse generell_info using word-level bounding boxes from Cloud Vision.

    Args:
        words: list of {"text": str, "x0": int, "y0": int, "x1": int, "y1": int}
               from Cloud Vision DOCUMENT_TEXT_DETECTION word output.

    Returns:
        dict with parsed fields. All fields populated, None if not found.
    """
    def _words_at(y: int, x_min: int = 920, x_max: int = 1728, tol: int = 15) -> str:
        hits = [w for w in words if abs(w["y0"] - y) < tol and w["x0"] >= x_min and w["x0"] <= x_max]
        hits.sort(key=lambda w: w["x0"])
        return " ".join(w["text"] for w in hits).strip()

    def _ja_nei_at(y: int) -> bool | None:
        val = _words_at(y, 920, 1100, tol=15)
        if "Ja" in val:
            return True
        if "Nei" in val:
            return False
        return None

    result = {}

    # Stable zone
    jn = _words_at(469, 920, 1200)
    result["journalnr"] = jn.replace(" ", "/", 1) if jn else None

    orgnr_raw = _words_at(575, 920, 1100)
    result["orgnr_ocr"] = orgnr_raw.replace(" ", "") if orgnr_raw else None

    result["organisasjonsform"] = _words_at(602, 920, 1300) or None
    result["foretaksnavn"] = _words_at(635, 920, 1600) or None
    result["forretningsadresse"] = ((_words_at(663, 920, 1600) or "") + " " + (_words_at(693, 920, 1200) or "")).strip() or None
    result["periode_start"] = _words_at(799, 920, 1100) or None
    result["periode_slutt"] = _words_at(799, 1100, 1350) or None

    if not result["periode_start"]:
        result["periode_start"] = _words_at(828, 920, 1100) or None
        result["periode_slutt"] = _words_at(828, 1100, 1350) or None

    m = next((w for w in words if "REGNSKAPSÅRET" in w.get("text", "")), None)
    if m:
        year_words = [w for w in words if abs(w["y0"] - m["y0"]) < 10 and w["x0"] > m["x1"] and w["text"].isdigit()]
        result["regnskapsaar"] = int(year_words[0]["text"]) if year_words else None
    else:
        result["regnskapsaar"] = None

    # Detect konsern layout: check for value at y=933
    is_konsern_layout = _ja_nei_at(933) is not None

    if is_konsern_layout:
        pos = GENERELL_INFO_POSITIONS["konsern"]
        result["morselskap_i_konsern"] = _ja_nei_at(933)
        result["konsernregnskap_vedlagt"] = _ja_nei_at(961)
    else:
        pos = GENERELL_INFO_POSITIONS["non_konsern"]
        result["morselskap_i_konsern"] = _ja_nei_at(903)
        result["konsernregnskap_vedlagt"] = None

    result["regler_smaa_foretak"] = _ja_nei_at(pos["smaa_foretak"]["y"])
    result["regnskapsregler"] = _words_at(pos["regnskapsregler"]["y"],
                                           pos["regnskapsregler"].get("x_min", 920),
                                           pos["regnskapsregler"].get("x_max", 1500)) or None
    result["representant"] = _words_at(pos["representant"]["y"],
                                        pos["representant"].get("x_min", 920),
                                        pos["representant"].get("x_max", 1400)) or None
    result["dato_fastsettelse"] = _words_at(pos["dato_fastsettelse"]["y"],
                                             pos["dato_fastsettelse"].get("x_min", 920),
                                             pos["dato_fastsettelse"].get("x_max", 1200)) or None

    result["fravalgt_revisjon"] = any("revideres" in w.get("text", "") for w in words)

    return result


NOTER_PROMPT_V2 = """Extract notes (noter) from these Norwegian årsregnskap pages.

For each note:
- nr: note number as printed (string or null)
- tittel: exact Norwegian title
- type: narrative | table | mixed
- amounts: ALL key-value pairs of amounts found. snake_case keys. {} for narrative.

Extract these flags (AMOUNTS must be integers in NOK, not true/false):
- antall_ansatte: integer or null
- antall_aarsverk: number or null
- otp_pliktig: bool — "pliktig til å ha tjenestepensjonsordning" → true
- revisjonshonorar_revisjon: amount or null
- bundne_midler_amount: amount or null — "bundne bankinnskudd"
- skattetrekkskonto: amount or null — "har ikke" → 0
- has_pantstillelser: bool
- pantstillelser_gjeld: amount or null — "gjeld sikret ved pant"
- pantstillelser_bokfort: amount or null — "balanseført verdi av pantsatte eiendeler"
- utbytte: amount or null — "foreslått/avsatt utbytte"
- konsernbidrag: amount or null — the NOK amount, NOT true/false
- fortsatt_drift_tvil: bool
- kassekredittlimit: amount or null
- kassekreditt_benyttet: amount or null
- klientmidler: amount or null — "innestående på klientkonto" or "klientmidler" amount in NOK, NOT true/false

Do NOT split one note into multiple entries. Extract EVERY note.
Return JSON: {"noter": [...], "note_flags": {...}, "n_notes_found": <int>}"""


def extract_noter(
    pdf_path: str,
    orgnr: str,
    year: int = 2024,
    *,
    credentials_path: str = "/mnt/project/sondreskarsten-d7d14-8486be2d085b.json",
    gemini_location: str = "europe-west1",
) -> dict:
    import base64
    import hashlib
    import json
    import re
    import time

    import fitz
    import requests
    from PIL import Image
    from google.auth.transport.requests import Request
    from google.oauth2 import service_account

    from brreg_regnskap.page_classifier import build_manifest

    EXTRACTOR_VERSION = "noter_v2"
    MODEL = "gemini-2.5-flash"

    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    pdf_hash = hashlib.sha256(pdf_bytes).hexdigest()[:16]
    manifest = build_manifest(pdf_bytes, orgnr=orgnr, year=year)

    creds = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(Request())
    project_id = creds.project_id
    url = (f"https://{gemini_location}-aiplatform.googleapis.com/v1/"
           f"projects/{project_id}/locations/{gemini_location}/"
           f"publishers/google/models/{MODEL}:generateContent")

    def _gemini_call(parts, timeout=180):
        if not creds.valid:
            creds.refresh(Request())
        body = {"contents": [{"role": "user", "parts": parts}],
                "generationConfig": {"maxOutputTokens": 65536, "temperature": 0,
                                     "thinkingConfig": {"thinkingBudget": 0}}}
        r = requests.post(url, headers={"Authorization": f"Bearer {creds.token}",
                                         "Content-Type": "application/json"},
                          json=body, timeout=timeout)
        r.raise_for_status()
        d = r.json()
        u = d.get("usageMetadata", {})
        c = d["candidates"][0]
        return {"in_tok": u.get("promptTokenCount", 0), "out_tok": u.get("candidatesTokenCount", 0),
                "cost": u.get("promptTokenCount", 0)/1e6*0.15 + u.get("candidatesTokenCount", 0)/1e6*0.60,
                "raw": c["content"]["parts"][0]["text"], "finish_reason": c.get("finishReason", "")}

    def _img_part(img_bytes):
        return {"inlineData": {"mimeType": "image/png", "data": base64.b64encode(img_bytes).decode()}}

    def _crop_to_png(img_bytes, x0, y0, x1, y1):
        pil = Image.open(io.BytesIO(img_bytes))
        cropped = pil.crop((x0, y0, x1, y1))
        buf = io.BytesIO()
        cropped.save(buf, format="PNG")
        return buf.getvalue()

    # Extract journalnr from pixel crop of page 1
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    del pdf_bytes
    p1_imgs = doc[0].get_images()
    p1_image = doc.extract_image(p1_imgs[0][0])["image"] if p1_imgs else None

    journalnr = None
    regnskapsaar = year
    journalnr_cost = 0.0

    if p1_image:
        jn_crop = _crop_to_png(p1_image, 900, 455, 1250, 500)
        jn_result = _gemini_call([_img_part(jn_crop), {"text": "Read the numbers from this image. Return only the digits and spaces, nothing else."}], timeout=30)
        jn_raw = jn_result["raw"].strip()
        journalnr_cost = jn_result["cost"]
        m = re.search(r'(\d{4})\s*(\d{4,7})', jn_raw)
        if m:
            journalnr = f"{m.group(1)}/{m.group(2)}"

        yr_crop = _crop_to_png(p1_image, 780, 370, 870, 420)
        yr_result = _gemini_call([_img_part(yr_crop), {"text": "Read the 4-digit year. Return only the number."}], timeout=30)
        yr_raw = yr_result["raw"].strip()
        journalnr_cost += yr_result["cost"]
        m = re.search(r'(\d{4})', yr_raw)
        regnskapsaar = int(m.group(1)) if m else year

    record = {
        "pdf_sha256_prefix": pdf_hash,
        "orgnr": orgnr,
        "year": regnskapsaar,
        "journalnr": journalnr,
        "extractor_version": EXTRACTOR_VERSION,
        "extraction_model": MODEL,
        "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
        "classifier_version": manifest["classifier"]["version"],
        "platform": manifest["platform"]["id"],
        "konsern_detected": manifest["konsern"]["detected"],
        "n_brreg": manifest["split"]["n_brreg"],
        "n_company": manifest["split"]["n_company"],
        "total_pages": manifest["document"]["total_pages"],
    }

    if manifest["split"]["n_company"] == 0:
        record["status"] = "skipped_brreg_only"
        record["noter"] = []
        record["note_flags"] = {}
        record["cost_usd"] = journalnr_cost
        doc.close()
        return record

    note_pages = []
    for p in manifest["pages"]:
        if p["source"] != "company" or p["type"] == "revisjonsberetning":
            continue
        imgs = doc[p["page"] - 1].get_images()
        if imgs:
            info = doc.extract_image(imgs[0][0])
            note_pages.append({"page": p["page"], "image": info["image"]})
    doc.close()

    if not note_pages:
        record["status"] = "skipped_no_note_pages"
        record["noter"] = []
        record["note_flags"] = {}
        record["cost_usd"] = journalnr_cost
        return record

    record["n_note_pages"] = len(note_pages)
    record["note_page_numbers"] = [p["page"] for p in note_pages]

    parts = [_img_part(p["image"]) for p in note_pages]
    parts.append({"text": NOTER_PROMPT_V2})

    t0 = time.time()
    r = _gemini_call(parts)
    elapsed = time.time() - t0

    record["input_tokens"] = r["in_tok"]
    record["output_tokens"] = r["out_tok"]
    record["cost_usd"] = r["cost"] + journalnr_cost
    record["elapsed_seconds"] = round(elapsed, 1)
    record["finish_reason"] = r["finish_reason"]

    raw = r["raw"].strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]

    parsed = json.loads(raw.strip())
    record["noter"] = parsed.get("noter", [])
    record["note_flags"] = parsed.get("note_flags", {})
    record["n_notes_found"] = len(record["noter"])
    record["n_notes_with_amounts"] = sum(1 for n in record["noter"] if n.get("amounts"))
    record["n_flags_set"] = sum(
        1 for v in record["note_flags"].values()
        if v is not None and v is not False and v != 0
    )
    record["status"] = "ok"

    return record
