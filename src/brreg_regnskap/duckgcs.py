"""DuckDB-over-GCS connection for streaming coordinator operations.

The coordinator's two heavy operations — building the work list (an anti-join
of orderflow against the manifest) and rewriting the manifest after a drain —
both touch the full 4M-row manifest. Loading it into Arrow/Python costs ~2.8-3.3
GB; streaming it through DuckDB with a memory limit keeps the peak under ~1 GB.

GCS access uses the ``gcs://`` scheme with a bearer token from the service
account (the ``gs://`` scheme fails over httpfs). Read paths must use
``gcs://`` — see :func:`to_gcs`.
"""

from __future__ import annotations

import duckdb
from google.auth.transport.requests import Request
from google.oauth2 import service_account


def to_gcs(path: str) -> str:
    """Rewrite a gs:// path to the gcs:// scheme DuckDB httpfs expects."""
    return path.replace("gs://", "gcs://", 1)


def connect(
    key_file: str | None,
    memory_limit: str = "1500MB",
    threads: int = 2,
    gcs: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection capped to ``memory_limit``.

    When ``gcs`` is True the connection is authenticated to GCS (``key_file`` is
    the service-account JSON path; None uses application-default credentials).
    When False — for local-filesystem paths — no GCS secret is created, so the
    connection works without any credentials.
    """
    con = duckdb.connect()
    con.execute("SET parquet_metadata_cache=true")
    con.execute("SET enable_progress_bar=false")
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    con.execute("SET preserve_insertion_order=false")

    if gcs:
        if key_file:
            creds = service_account.Credentials.from_service_account_file(
                key_file, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
        else:
            import google.auth

            creds, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
        creds.refresh(Request())
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"CREATE SECRET _gcs (TYPE gcs, BEARER_TOKEN '{creds.token}')")
    return con
