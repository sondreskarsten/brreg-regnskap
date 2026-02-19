"""Client for Brønnøysundregistrene Enhetsregisteret (Entity Registry) API.

Responsibilities:
    1. Download the nightly bulk entity dump (gzip JSON of all entities)
    2. Parse entities and yield those with sisteInnsendteAarsregnskap set
    3. Poll the updates API for incremental changes since a given oppdateringsid
    4. Filter updates for entities where sisteInnsendteAarsregnskap changed

Implementation notes:
    - Bulk dump: GET https://data.brreg.no/enhetsregisteret/api/enheter/lastned
      Returns gzip-compressed JSON. Supports ETag/If-None-Match for caching.
      Response is an array of entity objects. ~200MB compressed, ~1.5GB uncompressed.
    - Updates: GET https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter
      Params: oppdateringsid (cursor), dato (ISO date filter), includeChanges=true
      Returns paginated results with _embedded.oppdaterteEnheter array.
      Page through using oppdateringsid of last item in each page.
    - Accept header for entities: application/vnd.brreg.enhetsregisteret.enhet.v2+json
    - All methods are async using aiohttp.
    - Rate limiting is handled externally by the SyncEngine, not here.
"""

from __future__ import annotations

import gzip
import json
from typing import TYPE_CHECKING

import aiohttp

from brreg_regnskap.api.models import Enhet, EnhetUpdate

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

BASE_URL = "https://data.brreg.no/enhetsregisteret/api"
ACCEPT_V2 = "application/vnd.brreg.enhetsregisteret.enhet.v2+json"


class EnhetsregisteretClient:
    """Async client for the Enhetsregisteret API.

    Usage:
        async with EnhetsregisteretClient() as client:
            async for enhet in client.iter_entities_with_regnskap():
                print(enhet.organisasjonsnummer)
    """

    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> EnhetsregisteretClient:
        if self._owns_session:
            self._session = aiohttp.ClientSession(
                headers={"Accept": ACCEPT_V2},
                timeout=aiohttp.ClientTimeout(total=600),
            )
        return self

    async def __aexit__(self, *exc) -> None:  # type: ignore[no-untyped-def]
        if self._owns_session and self._session:
            await self._session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        assert self._session is not None, "Client not initialized. Use async with."
        return self._session

    async def download_bulk_dump(self) -> bytes:
        """Download the full entity dump as gzip bytes.

        Returns raw gzip bytes. Caller is responsible for storage.
        Use iter_entities_from_dump() to parse.
        """
        url = f"{BASE_URL}/enheter/lastned"
        headers = {"Accept": "application/vnd.brreg.enhetsregisteret.enhet.v2+gzip;charset=UTF-8"}
        timeout = aiohttp.ClientTimeout(total=600)
        async with self.session.get(url, headers=headers, timeout=timeout) as resp:
            resp.raise_for_status()
            return await resp.read()

    def iter_entities_from_dump(self, raw_gzip: bytes) -> list[Enhet]:
        """Parse gzip bulk dump bytes into Enhet models.

        Filters to only entities where sisteInnsendteAarsregnskap is set.
        """
        decompressed = gzip.decompress(raw_gzip)
        entities_raw = json.loads(decompressed)
        results = []
        for e in entities_raw:
            if e.get("sisteInnsendteAarsregnskap"):
                results.append(Enhet.model_validate(e))
        return results

    async def poll_updates(
        self, since_id: int = 0, include_changes: bool = True
    ) -> AsyncIterator[EnhetUpdate]:
        """Yield entity updates since the given oppdateringsid.

        When include_changes=True, each update contains JSON Patch operations
        in the endringer field showing exactly which fields changed.

        Paginates automatically until no more results are returned.
        """
        cursor = since_id
        while True:
            params = {
                "oppdateringsid": str(cursor),
                "includeChanges": str(include_changes).lower(),
            }
            url = f"{BASE_URL}/oppdateringer/enheter"
            async with self.session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

            updates_raw = (
                data.get("_embedded", {}).get("oppdaterteEnheter", [])
            )
            if not updates_raw:
                break

            for u in updates_raw:
                update = EnhetUpdate.model_validate(u)
                cursor = update.oppdateringsid
                yield update

    async def poll_regnskap_updates(self, since_id: int = 0) -> AsyncIterator[EnhetUpdate]:
        """Yield only updates where sisteInnsendteAarsregnskap changed.

        Filters the full update stream for JSON Patch operations targeting
        the sisteInnsendteAarsregnskap field path.
        """
        async for update in self.poll_updates(since_id, include_changes=True):
            if update.endringstype == "Ny":
                yield update
                continue
            if update.endringer:
                for patch in update.endringer:
                    path = patch.get("path", "")
                    if "sisteInnsendteAarsregnskap" in path:
                        yield update
                        break

    async def poll_regnskap_updates_since_date(self, since_date: str) -> AsyncIterator[tuple[EnhetUpdate, int | None]]:
        cursor = 0
        while True:
            params: dict[str, str] = {
                "oppdateringsid": str(cursor),
                "dato": since_date,
                "includeChanges": "true",
            }
            url = f"{BASE_URL}/oppdateringer/enheter"
            async with self.session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

            updates_raw = data.get("_embedded", {}).get("oppdaterteEnheter", [])
            if not updates_raw:
                break

            for u in updates_raw:
                update = EnhetUpdate.model_validate(u)
                cursor = update.oppdateringsid

                if update.endringstype == "Ny":
                    yield update, None
                    continue
                if update.endringer:
                    for patch_op in update.endringer:
                        path = patch_op.get("path", "")
                        if "sisteInnsendteAarsregnskap" in path:
                            raw_year = patch_op.get("value")
                            year = int(raw_year) if raw_year and str(raw_year).isdigit() else None
                            yield update, year
                            break
