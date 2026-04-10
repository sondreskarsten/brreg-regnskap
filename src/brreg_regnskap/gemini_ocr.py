"""Gemini Flash as OCR engine — the cheap Silver layer.

Sends PDF directly to Gemini 2.5 Flash and asks for full text transcription
with markdown table preservation. 3.6x cheaper than Cloud Vision, produces
pipe-delimited tables that the regnskap_extraction.py parser can handle.

Cost: ~$0.005 per entity. Non-deterministic (unlike Cloud Vision) but
written once and cached — non-determinism is a one-time risk.

Usage:
    from brreg_regnskap.gemini_ocr import GeminiOCR
    ocr = GeminiOCR()
    result = ocr.ocr_pdf(pdf_bytes)
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass

import fitz
import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

OCR_PROMPT = (
    "Return the full text content of every page in this PDF. "
    "Separate pages with '---PAGE BREAK---'. "
    "Preserve all numbers exactly as they appear. "
    "Preserve table structure using pipe-delimited markdown format. "
    "Do not summarize or interpret — transcribe everything visible on each page."
)


@dataclass
class OCRResult:
    pages: list[str]
    n_pages: int
    total_chars: int
    input_tokens: int
    output_tokens: int
    cost_usd: float

    @property
    def full_text(self) -> str:
        return "\n\n---PAGE BREAK---\n\n".join(self.pages)


class GeminiOCR:

    def __init__(
        self,
        credentials_path: str | None = None,
        project_id: str | None = None,
        location: str = "us-central1",
        model: str = "gemini-2.5-flash",
    ):
        if credentials_path:
            self._creds = service_account.Credentials.from_service_account_file(
                credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        else:
            import google.auth
            self._creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self._creds.refresh(Request())
        self._project = project_id or self._creds.project_id
        self._url = (
            f"https://{location}-aiplatform.googleapis.com/v1/"
            f"projects/{self._project}/locations/{location}/"
            f"publishers/google/models/{model}:generateContent"
        )

    def ocr_pdf(self, pdf_bytes: bytes) -> OCRResult:
        if not self._creds.valid:
            self._creds.refresh(Request())

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        n_pages = doc.page_count
        doc.close()

        parts = [
            {"inlineData": {"mimeType": "application/pdf", "data": base64.b64encode(pdf_bytes).decode()}},
            {"text": OCR_PROMPT},
        ]

        resp = requests.post(
            self._url,
            headers={"Authorization": f"Bearer {self._creds.token}", "Content-Type": "application/json"},
            json={
                "contents": [{"role": "user", "parts": parts}],
                "generationConfig": {"maxOutputTokens": 65536, "temperature": 0, "thinkingConfig": {"thinkingBudget": 0}},
            },
            timeout=180,
        )
        resp.raise_for_status()
        data = resp.json()

        raw = data["candidates"][0]["content"]["parts"][0]["text"]
        usage = data.get("usageMetadata", {})
        in_tok = usage.get("promptTokenCount", 0)
        out_tok = usage.get("candidatesTokenCount", 0)
        cost = in_tok / 1e6 * 0.15 + out_tok / 1e6 * 0.60

        pages = raw.split("---PAGE BREAK---")
        pages = [p.strip() for p in pages]

        return OCRResult(
            pages=pages, n_pages=n_pages, total_chars=len(raw),
            input_tokens=in_tok, output_tokens=out_tok, cost_usd=cost,
        )
