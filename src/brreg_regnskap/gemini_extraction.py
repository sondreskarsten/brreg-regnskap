"""Extract structured regnskap data from PDF page images via Gemini Flash.

Sends all pages as embedded PNG images to Gemini 2.5 Flash, returns
structured JSON with BRREG wrapper and company sections separated.

Cost: ~$0.0015 per entity at thinkingBudget=0.
Speed: ~3-5 seconds per entity.

Usage:
    from brreg_regnskap.gemini_extraction import GeminiExtractor
    extractor = GeminiExtractor()
    result = extractor.extract_pdf(pdf_bytes, orgnr="988054631", year=2024)
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field
from typing import Any

import fitz
import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

PROMPT = """Extract ALL financial data from this Norwegian årsregnskap PDF.

The PDF has two parts:
PART 1 (BRREG): First few pages with "Utskriftsdato" / "Brønnøysundregistrene" footers — standardized P&L and BS.
PART 2 (COMPANY): Company's own annual report — their own P&L, BS, notes, revisjonsberetning.

Extract BOTH parts. All amounts in NOK (not thousands). Costs as positive values. Use null for missing fields.

Return JSON:
{"split":{"brreg_last_page":4,"total_pages":13},
"brreg":{"salgsinntekt":0,"annen_driftsinntekt":0,"sum_driftsinntekter":0,"lonnskostnad":0,"avskrivning":0,"annen_driftskostnad":0,"sum_driftskostnader":0,"driftsresultat":0,"sum_finansinntekter":0,"sum_finanskostnader":0,"netto_finans":0,"resultat_for_skatt":0,"skattekostnad":0,"aarsresultat":0,"sum_anleggsmidler":0,"kundefordringer":0,"andre_fordringer":0,"bankinnskudd":0,"sum_omlopsmidler":0,"sum_eiendeler":0,"aksjekapital":0,"overkurs":0,"sum_egenkapital":0,"sum_langsiktig_gjeld":0,"leverandorgjeld":0,"sum_kortsiktig_gjeld":0,"sum_gjeld":0,"sum_egenkapital_gjeld":0},
"company":{"salgsinntekt":0,"annen_driftsinntekt":0,"sum_driftsinntekter":0,"lonnskostnad":0,"avskrivning":0,"annen_driftskostnad":0,"sum_driftskostnader":0,"driftsresultat":0,"renteinntekt_konsern":0,"annen_renteinntekt":0,"sum_finansinntekter":0,"rentekostnad_konsern":0,"annen_rentekostnad":0,"sum_finanskostnader":0,"netto_finans":0,"resultat_for_skatt":0,"skattekostnad":0,"aarsresultat":0,"sum_anleggsmidler":0,"kundefordringer":0,"andre_fordringer":0,"bankinnskudd":0,"sum_omlopsmidler":0,"sum_eiendeler":0,"aksjekapital":0,"overkurs":0,"sum_egenkapital":0,"sum_langsiktig_gjeld":0,"leverandorgjeld":0,"sum_kortsiktig_gjeld":0,"sum_gjeld":0,"sum_egenkapital_gjeld":0},
"notes":{"has_klientmidler":false,"klientmidler_amount":null,"klientansvar_amount":null,"has_bundne_midler":false,"bundne_midler_amount":null,"antall_ansatte":null,"antall_aarsverk":null,"revisjonshonorar":null,"pantstillelser_bokfort":null,"kassekredittlimit":null,"utbytte":null,"konsernbidrag":null},
"revisjon":{"revisor":"","firma":"","konklusjon":"uten_forbehold","fravalgt":false}}

If company section has no separate P&L/BS, set company to null.
For entities with KONSERN and SELSKAP statements, extract SELSKAP (not konsern)."""


@dataclass
class GeminiResult:
    orgnr: str
    year: int
    raw_json: dict
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    n_pages: int = 0

    @property
    def split_page(self) -> int:
        return self.raw_json.get("split", {}).get("brreg_last_page", 0)

    @property
    def brreg(self) -> dict | None:
        return self.raw_json.get("brreg")

    @property
    def company(self) -> dict | None:
        return self.raw_json.get("company")

    @property
    def notes(self) -> dict:
        return self.raw_json.get("notes", {})

    @property
    def revisjon(self) -> dict:
        return self.raw_json.get("revisjon", {})

    def to_flat_row(self, section: str = "company") -> dict:
        src = self.company if section == "company" else self.brreg
        row = {"orgnr": self.orgnr, "year": self.year, "section": section,
               "split_page": self.split_page, "n_pages": self.n_pages}
        if src:
            row.update(src)
        row.update({f"note_{k}": v for k, v in self.notes.items()})
        row.update({f"rev_{k}": v for k, v in self.revisjon.items()})
        row["gemini_cost_usd"] = self.cost_usd
        return row


def _extract_page_images(pdf_bytes: bytes) -> list[dict]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    images = []
    for i in range(doc.page_count):
        page_imgs = doc[i].get_images()
        if page_imgs:
            img = doc.extract_image(page_imgs[0][0])
            images.append({
                "inlineData": {
                    "mimeType": f"image/{img['ext']}",
                    "data": base64.b64encode(img["image"]).decode(),
                }
            })
    n_pages = doc.page_count
    doc.close()
    return images, n_pages


class GeminiExtractor:

    def __init__(
        self,
        credentials_path: str | None = None,
        project_id: str | None = None,
        location: str = "us-central1",
        model: str = "gemini-2.5-flash",
        thinking_budget: int = 0,
        max_output_tokens: int = 8192,
    ):
        if credentials_path:
            self._creds = service_account.Credentials.from_service_account_file(
                credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        else:
            import google.auth
            self._creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self._creds.refresh(Request())
        self._project = project_id or self._creds.project_id
        self._location = location
        self._model = model
        self._thinking_budget = thinking_budget
        self._max_output_tokens = max_output_tokens
        self._url = (
            f"https://{location}-aiplatform.googleapis.com/v1/"
            f"projects/{self._project}/locations/{location}/"
            f"publishers/google/models/{model}:generateContent"
        )

    def _refresh_if_needed(self):
        if not self._creds.valid:
            self._creds.refresh(Request())

    def extract_pdf(self, pdf_bytes: bytes, orgnr: str, year: int) -> GeminiResult:
        self._refresh_if_needed()
        images, n_pages = _extract_page_images(pdf_bytes)

        parts = images + [{"text": PROMPT}]
        body = {
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {
                "maxOutputTokens": self._max_output_tokens,
                "temperature": 0,
                "thinkingConfig": {"thinkingBudget": self._thinking_budget},
            },
        }

        resp = requests.post(
            self._url,
            headers={"Authorization": f"Bearer {self._creds.token}", "Content-Type": "application/json"},
            json=body,
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()

        raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
        usage = data.get("usageMetadata", {})
        in_tok = usage.get("promptTokenCount", 0)
        out_tok = usage.get("candidatesTokenCount", 0)
        cost = in_tok / 1e6 * 0.15 + out_tok / 1e6 * 0.60

        cleaned = raw_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
        if cleaned.endswith("```"):
            cleaned = cleaned.rsplit("```", 1)[0]

        parsed = json.loads(cleaned.strip())

        return GeminiResult(
            orgnr=orgnr, year=year, raw_json=parsed,
            input_tokens=in_tok, output_tokens=out_tok,
            cost_usd=cost, n_pages=n_pages,
        )
