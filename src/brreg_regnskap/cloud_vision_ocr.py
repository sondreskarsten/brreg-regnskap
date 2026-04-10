"""Cloud Vision OCR for Norwegian årsregnskap PDFs.

Extracts text from scanned PDF pages via Google Cloud Vision REST API.
Produces deterministic OCR text (no hallucination) stored as the Silver
layer for downstream LLM extraction.

Cost: $1.50/1000 pages ($0.0015/page). 8M docs × 20 pages = $240K one-time.
Storage: ~50KB/doc → 400GB for 8M docs → $48-96/year on GCS.

Usage:
    from brreg_regnskap.cloud_vision_ocr import CloudVisionOCR
    ocr = CloudVisionOCR()
    pages = ocr.ocr_pdf(pdf_bytes)  # list[str], one per page
"""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass

import fitz
import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

VISION_URL = "https://vision.googleapis.com/v1/images:annotate"
BATCH_LIMIT = 16


@dataclass
class OCRResult:
    pages: list[str]
    n_pages: int
    total_chars: int
    cost_usd: float

    @property
    def full_text(self) -> str:
        return "\n\n---PAGE BREAK---\n\n".join(self.pages)


class CloudVisionOCR:

    def __init__(
        self,
        credentials_path: str | None = None,
    ):
        if credentials_path:
            self._creds = service_account.Credentials.from_service_account_file(
                credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        else:
            import google.auth
            self._creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self._creds.refresh(Request())

    def _refresh(self):
        if not self._creds.valid:
            self._creds.refresh(Request())

    def _ocr_batch(self, image_contents: list[bytes]) -> list[str]:
        self._refresh()
        body = {
            "requests": [
                {
                    "image": {"content": base64.b64encode(img).decode()},
                    "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
                    "imageContext": {"languageHints": ["no", "en"]},
                }
                for img in image_contents
            ]
        }
        resp = requests.post(
            VISION_URL,
            headers={"Authorization": f"Bearer {self._creds.token}", "Content-Type": "application/json"},
            json=body,
            timeout=120,
        )
        resp.raise_for_status()
        results = []
        for r in resp.json()["responses"]:
            text = r.get("fullTextAnnotation", {}).get("text", "")
            results.append(text)
        return results

    def ocr_pdf(self, pdf_bytes: bytes) -> OCRResult:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        n_pages = doc.page_count

        page_images: list[bytes | None] = []
        for i in range(n_pages):
            imgs = doc[i].get_images()
            if imgs:
                img_data = doc.extract_image(imgs[0][0])
                page_images.append(img_data["image"])
            else:
                page_images.append(None)
        doc.close()

        all_text: list[str] = [""] * n_pages
        batch_indices: list[int] = []
        batch_images: list[bytes] = []

        for i, img in enumerate(page_images):
            if img is None:
                continue
            batch_indices.append(i)
            batch_images.append(img)

            if len(batch_images) >= BATCH_LIMIT:
                results = self._ocr_batch(batch_images)
                for idx, text in zip(batch_indices, results):
                    all_text[idx] = text
                batch_indices = []
                batch_images = []

        if batch_images:
            results = self._ocr_batch(batch_images)
            for idx, text in zip(batch_indices, results):
                all_text[idx] = text

        total_chars = sum(len(t) for t in all_text)
        cost = n_pages * 0.0015

        return OCRResult(pages=all_text, n_pages=n_pages, total_chars=total_chars, cost_usd=cost)
