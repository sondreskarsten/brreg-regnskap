"""Regression test for end-of-stream termination in the updates poll.

The oppdateringer API is cursor-INCLUSIVE: requesting oppdateringsid=N
returns updates from N onward, including N itself.  Once the cursor sits
on the last update in the stream, every request returns that single item
again — the page is never empty, so an empty-page check alone loops
forever (observed 2026-06-12: ~25 req/s against BRREG until cancelled).
Termination must also fire when the cursor makes no progress.
"""

from __future__ import annotations

import re

import pytest
from aioresponses import aioresponses

from brreg_regnskap.api.enhetsregisteret import BASE_URL, EnhetsregisteretClient

URL_PATTERN = re.compile(rf"{re.escape(BASE_URL)}/oppdateringer/enheter.*")


def _update(oppdateringsid: int, year: str | None = "2025") -> dict:
    endringer = (
        [{"op": "replace", "path": "/sisteInnsendteAarsregnskap", "value": year}]
        if year
        else []
    )
    return {
        "oppdateringsid": oppdateringsid,
        "organisasjonsnummer": "999999999",
        "dato": "2026-05-01T00:00:00.000Z",
        "endringstype": "Endring",
        "endringer": endringer,
    }


def _page(updates: list[dict]) -> dict:
    return {"_embedded": {"oppdaterteEnheter": updates}}


@pytest.mark.asyncio
async def test_poll_since_date_terminates_on_inclusive_tail() -> None:
    with aioresponses() as m:
        m.get(URL_PATTERN, payload=_page([_update(100), _update(101)]))
        m.get(URL_PATTERN, payload=_page([_update(101)]))
        m.get(URL_PATTERN, payload=_page([_update(101)]))

        async with EnhetsregisteretClient() as client:
            seen = [
                u.oppdateringsid
                async for u, _year in client.poll_regnskap_updates_since_date(
                    "2026-04-20T00:00:00.000Z"
                )
            ]

    assert seen == [100, 101, 101]


@pytest.mark.asyncio
async def test_poll_updates_terminates_on_inclusive_tail() -> None:
    with aioresponses() as m:
        m.get(URL_PATTERN, payload=_page([_update(50), _update(51)]))
        m.get(URL_PATTERN, payload=_page([_update(51)]))
        m.get(URL_PATTERN, payload=_page([_update(51)]))

        async with EnhetsregisteretClient() as client:
            seen = [u.oppdateringsid async for u in client.poll_updates(since_id=0)]

    assert seen == [50, 51, 51]


@pytest.mark.asyncio
async def test_poll_since_date_terminates_on_empty_first_page() -> None:
    with aioresponses() as m:
        m.get(URL_PATTERN, payload=_page([]))

        async with EnhetsregisteretClient() as client:
            seen = [
                u.oppdateringsid
                async for u, _year in client.poll_regnskap_updates_since_date(
                    "2026-04-20T00:00:00.000Z"
                )
            ]

    assert seen == []
