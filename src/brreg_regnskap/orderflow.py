"""Two-lane work queue for prioritised regnskap downloads.

The orderflow is a parquet-based queue partitioned into 10 shards by the
last digit of the orgnr (``int(orgnr) % 10``).  Each entry carries a
*processing_priority* and *create_time*:

Fast lane (high priority)
    ``processing_priority = create_time = int(time.time())``
    Both fields are equal.  Downloads JSON + PDF.

Slow lane (low priority / backlog)
    ``processing_priority = int(datetime(year, 1, 1, tzinfo=UTC).timestamp())``
    ``create_time = int(time.time())``
    Priority is less than create_time.  Downloads PDF only.

Lane identity is encoded structurally: ``priority == create_time`` means
fast lane, ``priority < create_time`` means slow lane.

Correction handling:
    When the same ``(orgnr, year)`` re-enters the fast lane via a patch,
    ``enqueue_fast`` **upserts** — it updates the entry's create_time and
    priority to *now*.  The ``pending()`` query uses a timestamp comparison
    against the manifest: an entry is pending if its ``create_time`` is
    greater than the manifest's ``download_timestamp`` for that key (or if
    the key is not in the manifest at all).  This lets corrections flow
    through naturally.  The download step then compares hashes and only
    saves a new version if the content actually differs.

Compaction:
    ``compact()`` removes entries whose ``create_time`` is at or before the
    manifest download timestamp.  Call periodically to keep shards small.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import structlog

if TYPE_CHECKING:
    from brreg_regnskap.config import Settings
    from brreg_regnskap.storage import StorageBackend

logger = structlog.get_logger()

# {(orgnr, year): unix_timestamp_of_latest_successful_download}
ManifestTimestamps = dict[tuple[str, int], int]

ORDERFLOW_SCHEMA = pa.schema(
    [
        pa.field("orgnr", pa.string(), nullable=False),
        pa.field("year", pa.int32()),  # nullable — null means "needs discovery"
        pa.field("processing_priority", pa.int64(), nullable=False),
        pa.field("create_time", pa.int64(), nullable=False),
        pa.field("source", pa.string()),
    ]
)


def _empty_table() -> pa.Table:
    return pa.table(
        {f.name: pa.array([], type=f.type) for f in ORDERFLOW_SCHEMA},
        schema=ORDERFLOW_SCHEMA,
    )


def _now_ts() -> int:
    return int(time.time())


def _year_priority(year: int) -> int:
    """Unix timestamp for Jan 1 of *year* — used as slow-lane priority."""
    return int(datetime(year, 1, 1, tzinfo=UTC).timestamp())


class OrderflowManager:
    """Manages the 10-shard orderflow queue.

    Usage::

        of = OrderflowManager(storage, settings)
        of.enqueue_fast([("964118191", 2024), ("987654321", 2023)])
        of.enqueue_slow("964118191", [2022, 2021, 2020])
        pending = of.pending(shard=1, manifest_ts={("987654321", 2023): 1700000000})
    """

    def __init__(self, storage: StorageBackend, settings: Settings) -> None:
        self._storage = storage
        self._settings = settings
        self._cache: dict[int, pa.Table] = {}

    # ── Load / save ───────────────────────────────────────────────

    def load_shard(self, digit: int) -> pa.Table:
        if digit in self._cache:
            return self._cache[digit]
        path = self._settings.orderflow_shard_path(digit)
        if not self._storage.exists(path):
            return _empty_table()
        raw = self._storage.read_bytes(path)
        table = pq.read_table(pa.BufferReader(raw), schema=ORDERFLOW_SCHEMA)
        self._cache[digit] = table
        return table

    def save_shard(self, digit: int, table: pa.Table) -> None:
        import io

        sink = io.BytesIO()
        pq.write_table(table, sink, compression="zstd")
        path = self._settings.orderflow_shard_path(digit)
        self._storage.write_bytes(path, sink.getvalue())
        self._cache[digit] = table

    def invalidate_cache(self) -> None:
        self._cache.clear()

    # ── Enqueue ───────────────────────────────────────────────────

    def enqueue_fast(
        self,
        entries: list[tuple[str, int]],
        source: str = "bulk_dump",
    ) -> int:
        """Add or update fast-lane entries: ``(orgnr, year)`` pairs.

        This is an **upsert**: if ``(orgnr, year)`` already exists in the
        shard, its ``create_time`` and ``processing_priority`` are updated
        to *now* so it will be re-processed (correction path).

        Deduplicates *within* the incoming batch.
        Returns the number of entries written (new + updated).
        """
        now = _now_ts()
        by_shard: dict[int, list[tuple[str, int]]] = {}
        for orgnr, year in entries:
            digit = int(orgnr) % 10
            by_shard.setdefault(digit, []).append((orgnr, year))

        written = 0
        for digit, pairs in by_shard.items():
            # Deduplicate incoming pairs (keep last occurrence)
            seen: set[tuple[str, int]] = set()
            unique: list[tuple[str, int]] = []
            for o, y in reversed(pairs):
                if (o, y) not in seen:
                    seen.add((o, y))
                    unique.append((o, y))
            unique.reverse()

            table = self.load_shard(digit)
            upsert_keys = seen  # all incoming keys

            # Remove old rows for keys being upserted
            if table.num_rows > 0 and upsert_keys:
                orgnr_col = table.column("orgnr")
                year_col = table.column("year")
                keep = pa.array(
                    [
                        (orgnr_col[i].as_py(), year_col[i].as_py()) not in upsert_keys
                        for i in range(table.num_rows)
                    ]
                )
                table = table.filter(keep)

            rows = {
                "orgnr": [o for o, _ in unique],
                "year": [y for _, y in unique],
                "processing_priority": [now] * len(unique),
                "create_time": [now] * len(unique),
                "source": [source] * len(unique),
            }
            new_table = pa.table(
                {f.name: pa.array(rows[f.name], type=f.type) for f in ORDERFLOW_SCHEMA},
                schema=ORDERFLOW_SCHEMA,
            )
            merged = pa.concat_tables([table, new_table], promote_options="none")
            self.save_shard(digit, merged)
            written += len(unique)

        return written

    def enqueue_slow(
        self,
        orgnr: str,
        years: list[int],
        manifest_ts: ManifestTimestamps | None = None,
    ) -> int:
        """Add slow-lane entries for discovered years.

        Each year gets ``processing_priority = unix(year-01-01)``.
        Skips years already queued or already successfully downloaded.
        Returns number of entries added.
        """
        digit = int(orgnr) % 10
        table = self.load_shard(digit)
        existing_keys = self._key_set(table)
        manifest_ts = manifest_ts or {}
        now = _now_ts()

        new_years = [
            y for y in years if (orgnr, y) not in existing_keys and (orgnr, y) not in manifest_ts
        ]
        if not new_years:
            return 0

        rows = {
            "orgnr": [orgnr] * len(new_years),
            "year": new_years,
            "processing_priority": [_year_priority(y) for y in new_years],
            "create_time": [now] * len(new_years),
            "source": ["years_api"] * len(new_years),
        }
        new_table = pa.table(
            {f.name: pa.array(rows[f.name], type=f.type) for f in ORDERFLOW_SCHEMA},
            schema=ORDERFLOW_SCHEMA,
        )
        merged = pa.concat_tables([table, new_table], promote_options="none")
        self.save_shard(digit, merged)
        return len(new_years)

    def enqueue_slow_discovery(self, orgnr_list: list[str]) -> int:
        """Add slow-lane discovery stubs (year=null, priority=0).

        These entries mark orgnrs that need a years-API call.
        Skips orgnrs that already have a discovery stub.
        Returns number of stubs added.
        """
        now = _now_ts()
        by_shard: dict[int, list[str]] = {}
        for orgnr in orgnr_list:
            digit = int(orgnr) % 10
            by_shard.setdefault(digit, []).append(orgnr)

        added = 0
        for digit, orgnrs in by_shard.items():
            table = self.load_shard(digit)
            existing_discovery = self._discovery_orgnrs(table)
            new_orgnrs = [o for o in orgnrs if o not in existing_discovery]
            if not new_orgnrs:
                continue

            rows = {
                "orgnr": new_orgnrs,
                "year": [None] * len(new_orgnrs),
                "processing_priority": [0] * len(new_orgnrs),
                "create_time": [now] * len(new_orgnrs),
                "source": ["bulk_dump"] * len(new_orgnrs),
            }
            new_table = pa.table(
                {f.name: pa.array(rows[f.name], type=f.type) for f in ORDERFLOW_SCHEMA},
                schema=ORDERFLOW_SCHEMA,
            )
            merged = pa.concat_tables([table, new_table], promote_options="none")
            self.save_shard(digit, merged)
            added += len(new_orgnrs)

        return added

    def remove_discovery_stubs(self, digit: int, orgnrs: set[str]) -> None:
        """Remove year=null discovery stubs for the given orgnrs.

        Called after years-API discovery has created year-specific entries.
        """
        table = self.load_shard(digit)
        if table.num_rows == 0:
            return

        year_col = table.column("year")
        orgnr_col = table.column("orgnr")
        keep = pa.array(
            [
                not (year_col[i].as_py() is None and orgnr_col[i].as_py() in orgnrs)
                for i in range(table.num_rows)
            ]
        )
        filtered = table.filter(keep)
        if filtered.num_rows != table.num_rows:
            self.save_shard(digit, filtered)

    # ── Query ─────────────────────────────────────────────────────

    def pending(
        self,
        digit: int,
        manifest_ts: ManifestTimestamps,
    ) -> pa.Table:
        """Return orderflow entries not yet completed, sorted by priority desc.

        An entry with ``year`` set is pending if:
        - ``(orgnr, year)`` is not in *manifest_ts*, **or**
        - ``entry.create_time > manifest_ts[(orgnr, year)]``
          (re-entered after last download — possible correction)

        Excludes year=null discovery stubs.
        """
        table = self.load_shard(digit)
        if table.num_rows == 0:
            return _empty_table()

        orgnr_col = table.column("orgnr")
        year_col = table.column("year")
        create_col = table.column("create_time")

        is_pending = pa.array(
            [
                year_col[i].as_py() is not None
                and (
                    (orgnr_col[i].as_py(), year_col[i].as_py()) not in manifest_ts
                    or create_col[i].as_py()
                    > manifest_ts[(orgnr_col[i].as_py(), year_col[i].as_py())]
                )
                for i in range(table.num_rows)
            ]
        )
        table = table.filter(is_pending)
        if table.num_rows == 0:
            return _empty_table()

        indices = pc.sort_indices(table, sort_keys=[("processing_priority", "descending")])
        return table.take(indices)

    def fast_lane_pending(
        self,
        digit: int,
        manifest_ts: ManifestTimestamps,
    ) -> pa.Table:
        """Return only fast-lane entries not yet completed.

        Fast lane = entries where ``processing_priority == create_time``
        (both set to *now* at enqueue time).
        """
        all_pending = self.pending(digit, manifest_ts)
        if all_pending.num_rows == 0:
            return _empty_table()
        prio_col = all_pending.column("processing_priority")
        create_col = all_pending.column("create_time")
        is_fast = pa.array(
            [prio_col[i].as_py() == create_col[i].as_py() for i in range(all_pending.num_rows)]
        )
        return all_pending.filter(is_fast)

    def discovery_stubs(self, digit: int) -> list[str]:
        """Return orgnrs with year=null discovery stubs in this shard."""
        table = self.load_shard(digit)
        return list(self._discovery_orgnrs(table))

    def shard_stats(self, digit: int, manifest_ts: ManifestTimestamps) -> dict[str, int]:
        """Return counts for this shard."""
        table = self.load_shard(digit)
        total = table.num_rows
        discovery = len(self._discovery_orgnrs(table))
        pend = self.pending(digit, manifest_ts)
        fast = self.fast_lane_pending(digit, manifest_ts)
        return {
            "total_entries": total,
            "discovery_stubs": discovery,
            "pending": pend.num_rows,
            "fast_lane_pending": fast.num_rows,
            "slow_lane_pending": pend.num_rows - fast.num_rows,
        }

    # ── Compaction ────────────────────────────────────────────────

    def compact(self, digit: int, manifest_ts: ManifestTimestamps) -> int:
        """Remove orderflow entries that are fully downloaded.

        An entry is compactable when:
        - It has a year (not a discovery stub), AND
        - ``(orgnr, year)`` is in *manifest_ts*, AND
        - ``entry.create_time <= manifest_ts[(orgnr, year)]``

        Returns the number of rows removed.
        """
        table = self.load_shard(digit)
        if table.num_rows == 0:
            return 0

        orgnr_col = table.column("orgnr")
        year_col = table.column("year")
        create_col = table.column("create_time")

        keep = pa.array(
            [
                year_col[i].as_py() is None  # keep discovery stubs
                or (orgnr_col[i].as_py(), year_col[i].as_py()) not in manifest_ts
                or create_col[i].as_py() > manifest_ts[(orgnr_col[i].as_py(), year_col[i].as_py())]
                for i in range(table.num_rows)
            ]
        )
        filtered = table.filter(keep)
        removed: int = table.num_rows - filtered.num_rows
        if removed > 0:
            self.save_shard(digit, filtered)
        return removed

    # ── Internals ─────────────────────────────────────────────────

    @staticmethod
    def _key_set(table: pa.Table) -> set[tuple[str, int]]:
        """Extract (orgnr, year) keys from a table, skipping null years."""
        if table.num_rows == 0:
            return set()
        orgnr_col = table.column("orgnr")
        year_col = table.column("year")
        keys = set()
        for i in range(table.num_rows):
            y = year_col[i].as_py()
            if y is not None:
                keys.add((orgnr_col[i].as_py(), y))
        return keys

    @staticmethod
    def _discovery_orgnrs(table: pa.Table) -> set[str]:
        """Return orgnrs that have year=null (discovery stub) entries."""
        if table.num_rows == 0:
            return set()
        orgnr_col = table.column("orgnr")
        year_col = table.column("year")
        return {orgnr_col[i].as_py() for i in range(table.num_rows) if year_col[i].as_py() is None}
