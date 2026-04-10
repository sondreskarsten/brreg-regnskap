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

STRUCTURE: The PDF has two parts:
1. BRREG WRAPPER (first pages): Identified by "Utskriftsdato"/"Brønnøysundregistrene" footers and "Side X av Y" pagination. Contains Generell Informasjon, standardized P&L, BS, and sometimes basic notes.
2. COMPANY SECTION (remaining pages): Company's own annual report — their P&L, BS, detailed notes, and possibly revisjonsberetning/årsberetning. Company pages may have centered/bolded headers and different formatting.

CRITICAL RULES:
- Extract P&L and BS from BOTH sections separately. All amounts in NOK (not thousands).
- Check for "Beløp i: 1000 NOK" or "alle tall i tusen" — if found, multiply all amounts by 1000.
- NUMBER FORMATS: Three formats coexist in the same PDF. Tabular: "1 364 000" (space-delimited). Narrative: "128.689,-" (dot-delimited, ",- " suffix means zero øre — treat as integer). Textual: "1,4 millioner" or "ca 700 000" (word multipliers and approximations). Extract exact amounts from tables; for narrative/årsberetning amounts, extract the value and convert word multipliers (millioner=×1000000, tusen=×1000).
- KONTANTSTRØMOPPSTILLING: Large entities (regler_smaa_foretak=false) may include a cash flow statement. If present, extract operating/investing/financing cash flows into a note with type="table". NRS 8 entities are exempt — absence is expected, not an error.
- ÅRSBERETNING: The board report contains numerical data not in the financial statements: insurance premiums, dispute amounts, cost overruns, maintenance costs. Extract as a note with type="mixed" and amounts for any quantified items. These contextual figures explain P&L anomalies.
- Costs as POSITIVE values. Negative values appear as either minus signs (-5255) or parentheses ((5 255)) — treat both as negative.
- For entities with both KONSERN and SELSKAP statements, extract SELSKAP into the "company" object. If konsern statements exist, extract them into a separate "konsern" object with the same field structure. Check "Morselskap i konsern" on Generell Informasjon: if "Ja", look for "Noteopplysninger - SELSKAP" vs "Konsernregnskap"/"Konsernresultatregnskap"/"Konsernbalanse" headers to distinguish which is which.
- Extract notes from the COMPANY section. If BRREG wrapper also has notes, extract those too into the noter array with a flag indicating source (add "source":"brreg" or "source":"company" to each note object).
- The P&L and BS "Note" column contains integers linking to note numbers. The noter array captures the target notes. No need to extract the reference integers themselves.
- Some entities (Sameie, BRL) use "Fellesutgifter" instead of "Salgsinntekt" — map to annen_driftsinntekt.
- Some entities have NO company P&L/BS (only BRREG wrapper + notes). Set company P&L/BS to null.
- Some entities have "fravalgt revisjon" (opted out of audit) — no revisjonsberetning exists. Two phrasings: "Selskapet har besluttet at årsregnskapet ikke skal revideres" (AS entities) or "Selskapet har ikke revisor" (sameie/other). Both mean fravalgt_revisjon=true. When audit is opted out, extract the regnskapsfører (accountant) name and Regnskapsførernummer (license number) from Generell Informasjon or the cover page — this is the sole preparer quality signal.

Return JSON:
{"split":{"brreg_last_page":4,"total_pages":13},
"meta":{"morselskap_i_konsern":false,"regler_smaa_foretak":true,"fravalgt_revisjon":false,"amounts_unit":"NOK","regnskapsforer":null,"regnskapsforer_nr":null,"dato_fastsettelse":null},
"brreg":{"salgsinntekt":null,"annen_driftsinntekt":null,"sum_driftsinntekter":null,"lonnskostnad":null,"avskrivning":null,"annen_driftskostnad":null,"sum_driftskostnader":null,"driftsresultat":null,"sum_finansinntekter":null,"sum_finanskostnader":null,"resultat_for_skatt":null,"skattekostnad":null,"aarsresultat":null,"sum_anleggsmidler":null,"kundefordringer":null,"bankinnskudd":null,"sum_omlopsmidler":null,"sum_eiendeler":null,"aksjekapital":null,"sum_egenkapital":null,"sum_langsiktig_gjeld":null,"leverandorgjeld":null,"sum_kortsiktig_gjeld":null,"sum_gjeld":null},
"company":{"salgsinntekt":null,"annen_driftsinntekt":null,"sum_driftsinntekter":null,"lonnskostnad":null,"avskrivning":null,"annen_driftskostnad":null,"sum_driftskostnader":null,"driftsresultat":null,"renteinntekt_konsern":null,"annen_renteinntekt":null,"sum_finansinntekter":null,"rentekostnad_konsern":null,"annen_rentekostnad":null,"sum_finanskostnader":null,"resultat_for_skatt":null,"skattekostnad":null,"aarsresultat":null,"sum_anleggsmidler":null,"kundefordringer":null,"andre_fordringer":null,"bankinnskudd":null,"sum_omlopsmidler":null,"sum_eiendeler":null,"aksjekapital":null,"overkurs":null,"sum_innskutt_egenkapital":null,"annen_egenkapital":null,"sum_opptjent_egenkapital":null,"sum_egenkapital":null,"sum_langsiktig_gjeld":null,"leverandorgjeld":null,"betalbar_skatt":null,"skyldige_offentlige_avgifter":null,"annen_kortsiktig_gjeld":null,"sum_kortsiktig_gjeld":null,"sum_gjeld":null,"sum_egenkapital_gjeld":null},
"konsern":null,
"noter":[{"nr":1,"tittel":"...","type":"narrative|table|mixed","source":"company|brreg","amounts":{}}],
"note_flags":{"has_klientmidler":false,"klientmidler_amount":null,"klientansvar_amount":null,"klientmidler_forskrift":null,"has_bundne_midler":false,"bundne_midler_amount":null,"skattetrekkskonto":null,"has_pantstillelser":false,"pantstillelser_bokfort":null,"pantstillelser_gjeld":null,"has_kassekreditt":false,"kassekredittlimit":null,"kassekreditt_benyttet":null,"has_konsernmellomvaerende":false,"antall_ansatte":null,"antall_aarsverk":null,"revisjonshonorar_revisjon":null,"revisjonshonorar_andre":null,"utbytte":null,"konsernbidrag":null,"otp_pliktig":null,"fortsatt_drift_tvil":false,"hendelser_etter_balansedagen":null},
"revisjon":{"revisor":null,"firma":null,"konklusjon":null,"fravalgt":false,"presisering":null}}

NOTES RULES:
- "nr": note number as shown in the document
- "tittel": exact title from the document (keep original language, Norwegian or English)
- "type": "narrative" (free text only), "table" (has numerical data), or "mixed"
- "amounts": for table/mixed notes, extract key-value pairs of CURRENT YEAR amounts only. Use descriptive snake_case keys. Empty {} for narrative notes.
- Do NOT include text summaries — only amounts. This keeps output compact.
- FINANSPOSTER: Company P&L may use lender-specific labels ("Rentekostnad DNB", "Rentekostnad lån Fornebu SB") instead of generic "Annen rentekostnad". Map these to annen_rentekostnad/annen_renteinntekt in the fixed P&L schema, but preserve the lender-specific labels as amounts keys in the corresponding note (e.g., "rentekostnad_dnb": 331492). Year-over-year lender name changes signal refinancing.
- REVENUE LINES: Sameie/BRL entities may have 4+ revenue lines (Fellesutgifter ordinære, Fellesutgifter ekstra, Andel lån og rente, Andre driftsinntekter). Map their sum to sum_driftsinntekter. Set salgsinntekt=null for these entities — their revenue is not "sales".
- Klientmidler may be embedded within a "Bankinnskudd" note rather than its own note — scan all bank/deposit notes for klientmidler mentions.
- KLIENTMIDLER DISAMBIGUATION: "Disponible midler" in Sameie/BRL entities is collective owner liquidity, NOT klientmidler. Only flag has_klientmidler=true when entity holds funds in a statutory intermediary capacity (eiendomsmegling, inkasso, advokatvirksomhet). Look for regulatory references: "Meglerforskriften", "Inkassoforskriften", "Forskrift om årsregnskap m.m. for inkassovirksomhet", "Advokatforskriften" — these confirm fiduciary status.
- Klientmidler can appear in THREE places: (1) within a "Bankinnskudd" note, (2) as its own note, (3) as a separate balance sheet line item with corresponding "Klientansvar" liability. Check all three.
- PANTSTILLELSER: When extracting pantstillelser amounts, look for the standard table structure: "Gjeld som er pantsikret" (total secured debt), then "Balanseført verdi av pantsatte eiendeler" broken into sub-items (varige driftsmidler, kundefordringer, lager). Extract pantstillelser_gjeld as the secured debt amount and pantstillelser_bokfort as the total pledged asset book value.
- FORTSATT DRIFT: In healthy entities, the going concern statement is embedded in "Regnskapsprinsipper" (Note 1). In distressed entities, it becomes a standalone note. Set fortsatt_drift_tvil=true if: (1) the note expresses material uncertainty ("vesentlig usikkerhet knyttet til fortsatt drift"), OR (2) deeply negative equity combined with material disputes or litigation disclosed in notes (these function as going-concern substitutes even without explicit language). Negative equity justified by off-balance-sheet property values (common in Sameie) does NOT trigger fortsatt_drift_tvil.
- "Skattetrekkskonto (har ikke)" means entity has no employees — set skattetrekkskonto to 0 and antall_ansatte to 0.
- note_flags: derived signals scanned across ALL notes and årsberetning.
- revisjon.konklusjon: "uten_forbehold", "med_forbehold", "negativ", "kan_ikke_uttale_seg", or null if fravalgt.
- revisjon.presisering: any emphasis of matter text (e.g., "vesentlig usikkerhet knyttet til fortsatt drift"), null if none.
- note_flags.klientmidler_forskrift: if klientmidler found, set to the regulatory reference ("meglerforskriften", "inkassoforskriften", "advokatforskriften") or "unspecified" if no forskrift cited."""


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
    def meta(self) -> dict:
        return self.raw_json.get("meta", {})

    @property
    def brreg(self) -> dict | None:
        return self.raw_json.get("brreg")

    @property
    def company(self) -> dict | None:
        return self.raw_json.get("company")

    @property
    def notes(self) -> dict:
        return self.raw_json.get("note_flags", {})

    @property
    def noter(self) -> list[dict]:
        return self.raw_json.get("noter", [])

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
        row["n_noter"] = len(self.noter)
        row["note_titles"] = "|".join(n.get("tittel", "") for n in self.noter)
        row.update({f"rev_{k}": v for k, v in self.revisjon.items()})
        row["gemini_cost_usd"] = self.cost_usd
        return row

    def to_rows(self) -> list[dict]:
        rows = []
        for section in ["brreg", "company"]:
            src = self.brreg if section == "brreg" else self.company
            if src is None:
                continue
            row = {"orgnr": self.orgnr, "year": self.year, "section": section,
                   "split_page": self.split_page, "n_pages": self.n_pages}
            row.update(src)
            if section == "company":
                row.update({f"note_{k}": v for k, v in self.notes.items()})
                row["n_noter"] = len(self.noter)
                row.update({f"rev_{k}": v for k, v in self.revisjon.items()})
            row["gemini_cost_usd"] = self.cost_usd
            rows.append(row)
        return rows


def _prepare_pdf_content(pdf_bytes: bytes) -> tuple[list[dict], int]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    n_pages = doc.page_count
    doc.close()
    content = [{
        "inlineData": {
            "mimeType": "application/pdf",
            "data": base64.b64encode(pdf_bytes).decode(),
        }
    }]
    return content, n_pages


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
        content, n_pages = _prepare_pdf_content(pdf_bytes)

        parts = content + [{"text": PROMPT}]
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
