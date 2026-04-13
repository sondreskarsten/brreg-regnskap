"""Cloud Run job: extract noter from årsregnskap PDFs.

Reads PDF paths from the regnskap manifest, runs page_classifier +
Gemini noter extraction, writes results to GCS extraction store.

Env vars:
    GOOGLE_APPLICATION_CREDENTIALS: service account key (auto on Cloud Run)
    BUCKET: source PDF bucket (default: brreg-regnskap)
    STORE_BUCKET: extraction store bucket (default: brreg-regnskap)
    NACE_FILTER: comma-separated NACE codes (default: all)
    YEAR: fiscal year to extract (default: 2024)
    MANIFEST_SHARD: which manifest shard to read (default: all)
    BATCH_SIZE: max entities per run (default: 0 = unlimited)
"""

import io
import json
import hashlib
import logging
import os
import re
import time
import gc
import base64
from datetime import datetime, timezone

import duckdb
import fitz
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.cloud import storage as gcs_storage
from PIL import Image

from brreg_regnskap.page_classifier import build_manifest
from brreg_regnskap.extraction_store import (
    ExtractionStore, NOTER_PROMPT_V2, NOTER_SCHEMA, NOTE_FLAGS_SCHEMA,
    _safe_int, _safe_float,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-2.5-flash"
EXTRACTOR_VERSION = "noter_v2"


def _get_creds():
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if path and os.path.exists(path):
        creds = service_account.Credentials.from_service_account_file(
            path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    else:
        import google.auth
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(Request())
    return creds


def _gemini_url(creds, location="europe-west1"):
    project = getattr(creds, "project_id", None) or os.environ.get("GCP_PROJECT", "sondreskarsten-d7d14")
    return (f"https://{location}-aiplatform.googleapis.com/v1/"
            f"projects/{project}/locations/{location}/"
            f"publishers/google/models/{GEMINI_MODEL}:generateContent")


def _gemini_call(url, creds, parts, timeout=180):
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
    return {"in_tok": u.get("promptTokenCount", 0),
            "out_tok": u.get("candidatesTokenCount", 0),
            "cost": u.get("promptTokenCount", 0) / 1e6 * 0.15 + u.get("candidatesTokenCount", 0) / 1e6 * 0.60,
            "raw": c["content"]["parts"][0]["text"],
            "finish_reason": c.get("finishReason", "")}


def _img_part(img_bytes):
    return {"inlineData": {"mimeType": "image/png",
                           "data": base64.b64encode(img_bytes).decode()}}


def _crop_png(img_bytes, x0, y0, x1, y1):
    pil = Image.open(io.BytesIO(img_bytes))
    buf = io.BytesIO()
    pil.crop((x0, y0, x1, y1)).save(buf, format="PNG")
    return buf.getvalue()


def _parse_gemini_json(raw):
    s = raw.strip()
    if s.startswith("```"):
        s = s.split("\n", 1)[1]
    if s.endswith("```"):
        s = s.rsplit("```", 1)[0]
    return json.loads(s.strip())


def load_target_orgnrs(gcs, bucket_name, year, nace_filter, manifest_shard):
    bucket = gcs.bucket(bucket_name)

    if manifest_shard is not None:
        shard_blobs = [f"manifest_shard_{manifest_shard}.parquet"]
    else:
        shard_blobs = sorted(b.name for b in bucket.list_blobs(prefix="manifest_shard_")
                             if b.name.endswith(".parquet"))

    log.info("Manifest shards: %s", shard_blobs)

    con = duckdb.connect()
    frames = []
    for sb in shard_blobs:
        log.info("Downloading manifest shard: %s", sb)
        buf = bucket.blob(sb).download_as_bytes()
        log.info("Downloaded %d bytes, reading parquet", len(buf))
        t = pq.read_table(io.BytesIO(buf), columns=["orgnr", "year", "status", "pdf_path", "pdf_hash"])
        log.info("Shard %s: %d rows", sb, len(t))
        frames.append(t)
    combined = pa.concat_tables(frames)
    con.register("manifest", combined)
    log.info("Combined manifest: %d rows", len(combined))

    where = f"year = {year} AND status = 'success' AND pdf_path IS NOT NULL"

    if nace_filter:
        log.info("Loading enheter for NACE filter: %s", nace_filter)
        enheter_bucket = gcs.bucket("sondre_brreg_data")
        blobs = sorted(b.name for b in enheter_bucket.list_blobs(prefix="enheter/parsed/v1/state/")
                        if b.name.endswith(".parquet"))
        log.info("Latest enheter snapshot: %s", blobs[-1] if blobs else "NONE")
        buf = enheter_bucket.blob(blobs[-1]).download_as_bytes()
        log.info("Enheter downloaded: %d bytes", len(buf))
        enheter = pq.read_table(io.BytesIO(buf), columns=["org_nr", "nace_1"])
        con.register("enheter", enheter)
        nace_list = ",".join(f"'{n}'" for n in nace_filter)
        df = con.execute(f"""
            SELECT m.orgnr, m.pdf_path, m.pdf_hash
            FROM manifest m
            JOIN enheter e ON m.orgnr = e.org_nr
            WHERE {where} AND e.nace_1 IN ({nace_list})
        """).fetchdf()
    else:
        df = con.execute(f"SELECT orgnr, pdf_path, pdf_hash FROM manifest WHERE {where}").fetchdf()

    return list(df.itertuples(index=False, name=None))


def load_already_done(gcs, store_bucket):
    bucket = gcs.bucket(store_bucket)
    blobs = list(bucket.list_blobs(prefix="extraction/store/noter/"))
    if not blobs:
        return set()
    done = set()
    for b in blobs:
        if not b.name.endswith(".parquet"):
            continue
        buf = b.download_as_bytes()
        t = pq.read_table(io.BytesIO(buf), columns=["pdf_sha256_prefix"])
        done.update(t.column("pdf_sha256_prefix").to_pylist())
    return done


def extract_one(orgnr, pdf_path, gcs, creds, gemini_url):
    parts = pdf_path.replace("gs://", "").split("/", 1)
    pdf_bytes = gcs.bucket(parts[0]).blob(parts[1]).download_as_bytes()
    pdf_hash = hashlib.sha256(pdf_bytes).hexdigest()[:16]

    manifest = build_manifest(pdf_bytes, orgnr=orgnr, year=2024)

    record = {
        "pdf_sha256_prefix": pdf_hash,
        "orgnr": orgnr,
        "year": 2024,
        "extractor_version": EXTRACTOR_VERSION,
        "extraction_model": GEMINI_MODEL,
        "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
        "classifier_version": manifest["classifier"]["version"],
        "platform": manifest["platform"]["id"],
        "konsern_detected": manifest["konsern"]["detected"],
        "total_pages": manifest["document"]["total_pages"],
        "n_brreg": manifest["split"]["n_brreg"],
        "n_company": manifest["split"]["n_company"],
    }

    if manifest["split"]["n_company"] == 0:
        record.update(status="skipped_brreg_only", noter=[], note_flags={}, raw_response=None)
        return record

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    del pdf_bytes

    # Journalnr from pixel crop
    p1_imgs = doc[0].get_images()
    journalnr_cost = 0.0
    if p1_imgs:
        p1_img = doc.extract_image(p1_imgs[0][0])["image"]
        jn_crop = _crop_png(p1_img, 900, 455, 1250, 500)
        try:
            jn_r = _gemini_call(gemini_url, creds,
                                [_img_part(jn_crop), {"text": "Read the numbers. Return only digits and spaces."}],
                                timeout=30)
            m = re.search(r'(\d{4})\s*(\d{4,7})', jn_r["raw"])
            record["journalnr"] = f"{m.group(1)}/{m.group(2)}" if m else None
            journalnr_cost = jn_r["cost"]
        except Exception:
            record["journalnr"] = None
    else:
        record["journalnr"] = None

    note_pages = []
    for p in manifest["pages"]:
        if p["source"] != "company" or p["type"] == "revisjonsberetning":
            continue
        imgs = doc[p["page"] - 1].get_images()
        if imgs:
            note_pages.append({"page": p["page"], "image": doc.extract_image(imgs[0][0])["image"]})
    doc.close()

    if not note_pages:
        record.update(status="skipped_no_note_pages", noter=[], note_flags={},
                      cost_usd=journalnr_cost, raw_response=None)
        return record

    record["n_note_pages"] = len(note_pages)
    record["note_page_numbers"] = [p["page"] for p in note_pages]

    parts = [_img_part(p["image"]) for p in note_pages]
    parts.append({"text": NOTER_PROMPT_V2})

    t0 = time.time()
    r = _gemini_call(gemini_url, creds, parts)
    elapsed = time.time() - t0

    record["input_tokens"] = r["in_tok"]
    record["output_tokens"] = r["out_tok"]
    record["cost_usd"] = r["cost"] + journalnr_cost
    record["elapsed_seconds"] = round(elapsed, 1)
    record["finish_reason"] = r["finish_reason"]
    record["raw_response"] = r["raw"]

    try:
        parsed = _parse_gemini_json(r["raw"])
        record["noter"] = parsed.get("noter", [])
        record["note_flags"] = parsed.get("note_flags", {})
        record["n_notes_found"] = len(record["noter"])
        record["n_flags_set"] = sum(1 for v in record["note_flags"].values()
                                     if v is not None and v is not False and v != 0)
        record["status"] = "ok"
    except Exception as e:
        record["status"] = "parse_error"
        record["error"] = str(e)[:500]
        record["noter"] = []
        record["note_flags"] = {}

    return record


def write_batch(store, records, cdate, ctime):
    noter_rows = []
    flag_rows = []
    raw_rows = []

    for rec in records:
        if rec["status"] not in ("ok", "parse_error"):
            continue

        for n in rec.get("noter", []):
            noter_rows.append({
                "pdf_sha256_prefix": rec["pdf_sha256_prefix"],
                "orgnr": rec["orgnr"],
                "year": rec["year"],
                **n,
                "extraction_model": rec["extraction_model"],
                "extraction_cost_usd": rec.get("cost_usd", 0) / max(len(rec.get("noter", [])), 1),
            })

        flag_rows.append({
            "pdf_sha256_prefix": rec["pdf_sha256_prefix"],
            "orgnr": rec["orgnr"],
            "year": rec["year"],
            **rec.get("note_flags", {}),
            "extraction_model": rec["extraction_model"],
            "extraction_cost_usd": rec.get("cost_usd"),
            "n_note_pages": rec.get("n_note_pages"),
        })

        raw_rows.append({
            "pdf_sha256_prefix": rec["pdf_sha256_prefix"],
            "orgnr": rec["orgnr"],
            "year": rec["year"],
            "journalnr": rec.get("journalnr"),
            "raw_response": rec.get("raw_response"),
            "status": rec["status"],
            "input_tokens": rec.get("input_tokens"),
            "output_tokens": rec.get("output_tokens"),
            "cost_usd": rec.get("cost_usd"),
            "finish_reason": rec.get("finish_reason"),
            "extractor_version": rec["extractor_version"],
            "n_note_pages": rec.get("n_note_pages"),
            "n_notes_found": rec.get("n_notes_found"),
            "error": rec.get("error"),
        })

    if noter_rows:
        store.write_noter(noter_rows, cdate, ctime)
    if flag_rows:
        store.write_note_flags(flag_rows, cdate, ctime)

    # Write raw responses to separate partition
    if raw_rows:
        raw_schema = pa.schema([
            pa.field("pdf_sha256_prefix", pa.string(), nullable=False),
            pa.field("orgnr", pa.string(), nullable=False),
            pa.field("year", pa.int32()),
            pa.field("journalnr", pa.string()),
            pa.field("raw_response", pa.large_string()),
            pa.field("status", pa.string()),
            pa.field("input_tokens", pa.int32()),
            pa.field("output_tokens", pa.int32()),
            pa.field("cost_usd", pa.float64()),
            pa.field("finish_reason", pa.string()),
            pa.field("extractor_version", pa.string()),
            pa.field("n_note_pages", pa.int32()),
            pa.field("n_notes_found", pa.int32()),
            pa.field("error", pa.string()),
            pa.field("collection_date", pa.string(), nullable=False),
            pa.field("collection_time", pa.string(), nullable=False),
        ])
        for row in raw_rows:
            row["collection_date"] = cdate
            row["collection_time"] = ctime
        table = pa.Table.from_pylist(raw_rows, schema=raw_schema)
        store._write_parquet("raw_responses", table, cdate, ctime)

    return len(noter_rows), len(flag_rows), len(raw_rows)


def main():
    bucket_name = os.environ.get("BUCKET", "brreg-regnskap")
    store_bucket = os.environ.get("STORE_BUCKET", "brreg-regnskap")
    year = int(os.environ.get("YEAR", "2024"))
    batch_size = int(os.environ.get("BATCH_SIZE", "0"))
    nace_raw = os.environ.get("NACE_FILTER", "")
    nace_filter = [n.strip() for n in nace_raw.split(",") if n.strip()] or None
    manifest_shard = os.environ.get("MANIFEST_SHARD")
    if manifest_shard is not None:
        manifest_shard = int(manifest_shard)

    creds = _get_creds()
    gcs = gcs_storage.Client()
    gemini_url = _gemini_url(creds)
    store = ExtractionStore(f"gs://{store_bucket}/extraction/store")

    log.info("Loading targets bucket=%s year=%d nace=%s shard=%s",
             bucket_name, year, nace_filter, manifest_shard)
    targets = load_target_orgnrs(gcs, bucket_name, year, nace_filter, manifest_shard)
    log.info("Found %d target PDFs", len(targets))

    done_hashes = load_already_done(gcs, store_bucket)
    log.info("Already extracted: %d pdf hashes", len(done_hashes))

    cdate = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ctime = datetime.now(timezone.utc).strftime("%H%M%S")

    records = []
    total_cost = 0.0
    n_ok = n_skip = n_err = 0

    for i, (orgnr, pdf_path, pdf_hash_manifest) in enumerate(targets):
        if batch_size and i >= batch_size:
            break

        try:
            rec = extract_one(orgnr, pdf_path, gcs, creds, gemini_url)
        except Exception as e:
            log.warning("Failed %s: %s", orgnr, str(e)[:200])
            n_err += 1
            continue

        records.append(rec)
        total_cost += rec.get("cost_usd", 0)

        if rec["status"] == "ok":
            n_ok += 1
        elif "skip" in rec["status"]:
            n_skip += 1
        else:
            n_err += 1

        if len(records) % 50 == 0 and records:
            n_noter, n_flags, n_raw = write_batch(store, records, cdate, ctime)
            log.info("Batch written: %d noter, %d flags, %d raw | total: %d ok, %d skip, %d err, $%.2f",
                     n_noter, n_flags, n_raw, n_ok, n_skip, n_err, total_cost)
            records = []
            ctime = datetime.now(timezone.utc).strftime("%H%M%S")
            gc.collect()

    if records:
        n_noter, n_flags, n_raw = write_batch(store, records, cdate, ctime)
        log.info("Final batch: %d noter, %d flags, %d raw", n_noter, n_flags, n_raw)

    log.info("DONE: %d ok, %d skip, %d err, $%.2f total", n_ok, n_skip, n_err, total_cost)


if __name__ == "__main__":
    main()
