"""OCR format experiments for Norwegian årsregnskap PDFs.

Runs 20 tests comparing Gemini 2.5 Flash extraction approaches:
JSON vs pipe-delimited, per-page vs full-PDF, temperature, HTML,
model comparison, few-shot, resolution, cross-validation, and
manifest-guided extraction.

Results documented in docs/ocr_format_experiments.md

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=creds.json python experiments/run_ocr_experiments.py

Requires: google-cloud-storage, google-auth, PyMuPDF, Pillow, requests
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import time
from pathlib import Path

import fitz
import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account


ENTITIES = {
    "988054631": "Bonord",
    "981472470": "ECITLAW",
    "968613189": "Alliance",
    "987591102": "Silvercoin",
    "988775460": "Rødskifer",
}

PIPE_PROMPT = (
    "Transcribe ALL text. For tables: each row on one line using | separator. "
    "No alignment padding or extra dashes. Preserve Norwegian chars and numbers exactly."
)

JSON_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "page_type": {"type": "STRING"},
        "tables": {"type": "ARRAY", "items": {
            "type": "OBJECT",
            "properties": {
                "title": {"type": "STRING"},
                "headers": {"type": "ARRAY", "items": {"type": "STRING"}},
                "rows": {"type": "ARRAY", "items": {
                    "type": "OBJECT",
                    "properties": {
                        "label": {"type": "STRING"},
                        "note": {"type": "STRING"},
                        "current_year": {"type": "STRING"},
                        "prior_year": {"type": "STRING"},
                    },
                }},
            },
        }},
        "non_table_text": {"type": "STRING"},
    },
}

CLASSIFY_PROMPT = (
    'For each page in this PDF, return a JSON array:\n'
    '[{"page":1,"source":"brreg","type":"generell_info","has_table":false}]\n'
    'source: "brreg" (standardized Brønnøysundregistrene pages) or "company" '
    "(company's own attachment).\n"
    "type: generell_info | resultatregnskap | balanse | noter | "
    "revisjonsberetning | aarsberetning | kontantstrom | signatur | annet.\n"
    "has_table: true if page has a financial table with number columns."
)

EXTRACT_PROMPT = (
    "Extract ALL financial data from this Norwegian årsregnskap PDF.\n"
    "All amounts in NOK. Costs as positive. Return JSON:\n"
    '{"brreg":{"sum_driftsinntekter":null,"sum_driftskostnader":null,'
    '"driftsresultat":null,"aarsresultat":null,"sum_eiendeler":null,'
    '"sum_egenkapital":null,"sum_gjeld":null},\n'
    '"company":{"sum_driftsinntekter":null,"sum_driftskostnader":null,'
    '"driftsresultat":null,"aarsresultat":null,"sum_eiendeler":null,'
    '"sum_egenkapital":null,"sum_gjeld":null},\n'
    '"note_flags":{"has_klientmidler":false,"klientmidler_amount":null,'
    '"antall_ansatte":null},\n'
    '"revisjon":{"revisor":null,"konklusjon":null,"fravalgt":false}}\n'
    "If company has NO separate P&L/BS (only BRREG wrapper exists), "
    "set company fields to null."
)


class GeminiClient:

    def __init__(self, credentials_path: str, location: str = "us-central1"):
        self._creds = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self._creds.refresh(Request())
        self._project = self._creds.project_id
        self._location = location

    def _url(self, model: str = "gemini-2.5-flash") -> str:
        return (
            f"https://{self._location}-aiplatform.googleapis.com/v1/"
            f"projects/{self._project}/locations/{self._location}/"
            f"publishers/google/models/{model}:generateContent"
        )

    def call(self, parts, model="gemini-2.5-flash", max_tokens=65536,
             temperature=0, thinking=0, mime_type=None, schema=None):
        if not self._creds.valid:
            self._creds.refresh(Request())
        gen = {"maxOutputTokens": max_tokens, "temperature": temperature}
        if "2.5" in model:
            gen["thinkingConfig"] = {"thinkingBudget": thinking}
        if mime_type:
            gen["responseMimeType"] = mime_type
        if schema:
            gen["responseSchema"] = schema
        t0 = time.time()
        r = requests.post(
            self._url(model),
            headers={"Authorization": f"Bearer {self._creds.token}",
                     "Content-Type": "application/json"},
            json={"contents": [{"role": "user", "parts": parts}],
                  "generationConfig": gen},
            timeout=120,
        )
        elapsed = time.time() - t0
        if r.status_code != 200:
            return {"error": r.status_code, "body": r.text[:300], "elapsed": elapsed}
        data = r.json()
        cand = data["candidates"][0]
        usage = data.get("usageMetadata", {})
        return {
            "text": cand["content"]["parts"][0]["text"],
            "finish": cand.get("finishReason", "?"),
            "in_tok": usage.get("promptTokenCount", 0),
            "out_tok": usage.get("candidatesTokenCount", 0),
            "think_tok": usage.get("thoughtsTokenCount", 0),
            "elapsed": elapsed,
        }


def pdf_part(pdf_bytes: bytes) -> dict:
    return {"inlineData": {"mimeType": "application/pdf",
                           "data": base64.b64encode(pdf_bytes).decode()}}


def img_part(image_bytes: bytes) -> dict:
    return {"inlineData": {"mimeType": "image/png",
                           "data": base64.b64encode(image_bytes).decode()}}


def extract_page_image(pdf_bytes: bytes, page_idx: int) -> bytes | None:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    imgs = doc[page_idx].get_images()
    result = doc.extract_image(imgs[0][0])["image"] if imgs else None
    doc.close()
    return result


def count_dupes(text: str) -> tuple[int, int]:
    paras = [p.strip() for p in text.split("\n") if len(p.strip()) > 50]
    seen: dict[str, int] = {}
    dupes = 0
    for p in paras:
        h = hashlib.md5(p.encode()).hexdigest()[:16]
        seen[h] = seen.get(h, 0) + 1
        if seen[h] > 1:
            dupes += 1
    return dupes, len(paras)


def crossval(data: dict) -> dict:
    checks = {}
    for sec in ["brreg", "company"]:
        s = data.get(sec)
        if not s or not isinstance(s, dict):
            continue
        try:
            sdi = float(s["sum_driftsinntekter"]) if s.get("sum_driftsinntekter") is not None else None
            sdk = float(s["sum_driftskostnader"]) if s.get("sum_driftskostnader") is not None else None
            dr = float(s["driftsresultat"]) if s.get("driftsresultat") is not None else None
            if all(v is not None for v in [sdi, sdk, dr]):
                checks[f"{sec}_pnl"] = abs((sdi - sdk) - dr) < 2
            ei = float(s["sum_eiendeler"]) if s.get("sum_eiendeler") is not None else None
            ek = float(s["sum_egenkapital"]) if s.get("sum_egenkapital") is not None else None
            gj = float(s["sum_gjeld"]) if s.get("sum_gjeld") is not None else None
            if all(v is not None for v in [ei, ek, gj]):
                checks[f"{sec}_bs"] = abs(ei - (ek + gj)) < 2
        except (TypeError, ValueError):
            pass
    return checks
