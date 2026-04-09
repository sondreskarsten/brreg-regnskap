"""ParseExtract API client for PDF text extraction.

Handles sync/async submission, job polling for >5 page documents,
and rate limiting.

Usage:
    client = ParseExtractClient(api_key="...")
    pages = client.extract("path/to/file.pdf")
    # or from bytes:
    pages = client.extract_bytes(pdf_bytes, filename="doc.pdf")
"""

from __future__ import annotations

import time
from typing import BinaryIO

import requests

DEFAULT_TIMEOUT = (10, 300)
POLL_TIMEOUT = (10, 60)


class ParseExtractError(Exception):
    pass


class ParseExtractClient:

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.parseextract.com/v1",
        pdf_option: str = "option_a",
        poll_interval: int = 15,
        max_poll_attempts: int = 30,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._pdf_option = pdf_option
        self._poll_interval = poll_interval
        self._max_poll_attempts = max_poll_attempts
        self._headers = {"Authorization": f"Bearer {api_key}"}

    def extract_bytes(self, pdf_bytes: bytes, filename: str = "document.pdf") -> list[str]:
        files = {"file": (filename, pdf_bytes, "application/pdf")}
        payload = {"pdf_option": self._pdf_option, "inline_images": "False", "get_base64_images": "False"}
        resp = requests.post(
            f"{self._base_url}/pdf-parse",
            files=files,
            data=payload,
            headers=self._headers,
            timeout=DEFAULT_TIMEOUT,
        )
        if resp.status_code != 200:
            raise ParseExtractError(f"HTTP {resp.status_code}: {resp.text[:300]}")

        data = resp.json()
        job_id = data.get("job_id", "")
        text = data.get("text", "")

        if isinstance(text, list) and text and not job_id:
            return text

        if job_id:
            return self._poll_job(job_id)

        if isinstance(text, str) and len(text) > 200:
            return [text]

        return []

    def extract(self, path: str) -> list[str]:
        with open(path, "rb") as f:
            return self.extract_bytes(f.read(), filename=path.rsplit("/", 1)[-1])

    def _poll_job(self, job_id: str) -> list[str]:
        for attempt in range(self._max_poll_attempts):
            time.sleep(self._poll_interval)
            resp = requests.get(
                f"{self._base_url}/fetchoutput?job_id={job_id}",
                headers=self._headers,
                timeout=POLL_TIMEOUT,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            status = data.get("status", "")
            text = data.get("text", "")

            if status == "completed" or (isinstance(text, list) and text):
                return text if isinstance(text, list) else ([text] if text else [])
            if status == "failed":
                raise ParseExtractError(f"Job {job_id} failed")

        raise ParseExtractError(f"Job {job_id} timed out after {self._max_poll_attempts * self._poll_interval}s")
