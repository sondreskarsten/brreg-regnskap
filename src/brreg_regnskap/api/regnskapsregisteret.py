"""Client for Brønnøysundregistrene Regnskapsregisteret (Accounts Registry) API.

Responsibilities:
    1. Fetch structured JSON regnskap data for a given orgnr
    2. Fetch the list of available years for PDF annual reports
    3. Download PDF copies of annual reports

Implementation notes:
    - Regnskap JSON: GET https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}
      Returns XML by default. Use Accept: application/json to get JSON.
      Response is an array (ArrayList) — take the first item for the most recent.
      The `journalnr` field identifies the specific submission version.
    - Available years: GET https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/aar
      Returns JSON array of year strings: ["2011","2012",...,"2024"]
    - PDF download: GET https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/{year}
      Returns binary PDF. May return 404 if the year is not available.
      Not all years listed in the years endpoint have PDFs available.
      NOTE: As of ~March 2026, BRREG returns HTTP 406 for Accept: application/pdf.
      Use Accept: application/octet-stream instead (response content-type is still application/pdf).
    - All methods are async using aiohttp.
    - HTTP 404 on PDF means the PDF is not (yet) available — return None, don't raise.
    - HTTP 404 on regnskap JSON means the company has no registered accounts — return None.
    - BRREG returns HTTP 200 with body {"message": "Too many requests!"} instead of
      HTTP 429. All methods must detect this and raise BrregRateLimitError for retry.
"""

from __future__ import annotations

import aiohttp

from brreg_regnskap.api.models import Regnskap

BASE_URL = "https://data.brreg.no/regnskapsregisteret"


class BrregRateLimitError(Exception):
    """BRREG returns HTTP 200 with JSON error body instead of HTTP 429."""


class RegnskapsregisteretClient:
    """Async client for the Regnskapsregisteret API.

    Usage:
        async with RegnskapsregisteretClient() as client:
            regnskap = await client.fetch_regnskap("964118191")
            years = await client.fetch_years("964118191")
            pdf = await client.download_pdf("964118191", 2024)
    """

    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> RegnskapsregisteretClient:
        if self._owns_session:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=120),
            )
        return self

    async def __aexit__(self, *exc) -> None:  # type: ignore[no-untyped-def]
        if self._owns_session and self._session:
            await self._session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        assert self._session is not None, "Client not initialized. Use async with."
        return self._session

    async def fetch_regnskap(self, orgnr: str) -> Regnskap | None:
        """Fetch the most recent regnskap JSON for an entity.

        Returns None if the entity has no registered accounts (HTTP 404).
        Returns the first (most recent) item from the response array.
        """
        url = f"{BASE_URL}/regnskap/{orgnr}"
        headers = {"Accept": "application/json"}
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 404:
                return None
            resp.raise_for_status()
            data = await resp.json()

        items = data if isinstance(data, list) else [data]
        if not items:
            return None
        return Regnskap.model_validate(items[0])

    async def fetch_regnskap_raw(self, orgnr: str) -> bytes | None:
        """Fetch the raw JSON bytes for storage (preserves full fidelity).

        Returns None if HTTP 404.
        """
        url = f"{BASE_URL}/regnskap/{orgnr}"
        headers = {"Accept": "application/json"}
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 404:
                return None
            resp.raise_for_status()
            raw = await resp.read()
            if b"Too many requests" in raw[:200]:
                raise BrregRateLimitError(f"Rate limited on regnskap/{orgnr}")
            return raw

    async def fetch_years(self, orgnr: str) -> list[int]:
        """Fetch the list of years with available PDF annual reports.

        Returns an empty list if the entity has no available PDFs.
        """
        url = f"{BASE_URL}/regnskap/aarsregnskap/kopi/{orgnr}/aar"
        headers = {"Accept": "application/json"}
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 404:
                return []
            resp.raise_for_status()
            data = await resp.json()
        if isinstance(data, dict) and "message" in data:
            raise BrregRateLimitError(f"Rate limited on years/{orgnr}: {data['message']}")
        return [int(y) for y in data] if isinstance(data, list) else []

    async def download_pdf(self, orgnr: str, year: int) -> bytes | None:
        """Download the PDF annual report for a specific year.

        Returns None if the PDF is not available (HTTP 404).
        Returns raw PDF bytes on success.
        """
        url = f"{BASE_URL}/regnskap/aarsregnskap/kopi/{orgnr}/{year}"
        headers = {"Accept": "application/octet-stream"}
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 404:
                return None
            resp.raise_for_status()
            data = await resp.read()
            if len(data) < 500 and b"Too many requests" in data:
                raise BrregRateLimitError(f"Rate limited on pdf/{orgnr}/{year}")
            return data
