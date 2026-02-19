"""Tests for the two-lane orderflow queue."""

from __future__ import annotations

import time
from pathlib import Path

from brreg_regnskap.config import Settings
from brreg_regnskap.orderflow import ManifestTimestamps, OrderflowManager, _year_priority
from brreg_regnskap.storage import StorageBackend


def _make_orderflow(tmp_path: Path) -> OrderflowManager:
    settings = Settings(storage_path=str(tmp_path / "store"))
    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()
    return OrderflowManager(storage, settings)


class TestEnqueueFast:
    def test_adds_entries(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        added = of.enqueue_fast([("964118191", 2024), ("987654321", 2023)])
        assert added == 2

    def test_upserts_existing(self, tmp_path: Path) -> None:
        """Calling enqueue_fast again for the same key updates create_time."""
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        table_before = of.load_shard(1)
        ts_before = table_before.column("create_time")[0].as_py()

        # Wait a moment to guarantee different timestamp
        time.sleep(0.01)

        written = of.enqueue_fast([("964118191", 2024)], source="patch")
        assert written == 1  # upserted, not skipped

        of.invalidate_cache()
        table_after = of.load_shard(1)
        assert table_after.num_rows == 1  # still one row, not two
        ts_after = table_after.column("create_time")[0].as_py()
        assert ts_after >= ts_before

    def test_shards_by_last_digit(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        # 964118191 % 10 = 1, 964118190 % 10 = 0
        of.enqueue_fast([("964118191", 2024), ("964118190", 2023)])
        shard_0 = of.load_shard(0)
        shard_1 = of.load_shard(1)
        assert shard_0.num_rows == 1
        assert shard_1.num_rows == 1
        assert shard_0.column("orgnr")[0].as_py() == "964118190"
        assert shard_1.column("orgnr")[0].as_py() == "964118191"


class TestEnqueueSlow:
    def test_adds_year_entries(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        added = of.enqueue_slow("964118191", [2022, 2021, 2020])
        assert added == 3

    def test_uses_year_priority(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_slow("964118191", [2022, 2020])
        table = of.load_shard(1)  # 964118191 % 10 = 1
        priorities = table.column("processing_priority").to_pylist()
        assert _year_priority(2022) in priorities
        assert _year_priority(2020) in priorities

    def test_skips_manifest_keys(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        manifest_ts: ManifestTimestamps = {("964118191", 2022): int(time.time())}
        added = of.enqueue_slow("964118191", [2022, 2021], manifest_ts=manifest_ts)
        assert added == 1

    def test_skips_already_queued(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_slow("964118191", [2022])
        added = of.enqueue_slow("964118191", [2022, 2021])
        assert added == 1  # only 2021 is new


class TestDiscoveryStubs:
    def test_add_and_list(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        added = of.enqueue_slow_discovery(["964118191", "987654321"])
        assert added == 2
        stubs_1 = of.discovery_stubs(1)  # 964118191 % 10 = 1
        assert "964118191" in stubs_1

    def test_deduplicates(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_slow_discovery(["964118191"])
        added = of.enqueue_slow_discovery(["964118191"])
        assert added == 0

    def test_remove_stubs(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_slow_discovery(["964118191"])
        assert len(of.discovery_stubs(1)) == 1
        of.remove_discovery_stubs(1, {"964118191"})
        assert len(of.discovery_stubs(1)) == 0


class TestPending:
    def test_returns_entries_not_in_manifest(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024), ("964118191", 2023)])
        # 2023 is in manifest with a recent timestamp
        manifest_ts: ManifestTimestamps = {("964118191", 2023): int(time.time()) + 9999}
        pending = of.pending(1, manifest_ts=manifest_ts)
        assert pending.num_rows == 1
        assert pending.column("year")[0].as_py() == 2024

    def test_correction_reentry_is_pending(self, tmp_path: Path) -> None:
        """An entry re-enqueued AFTER the manifest download is pending."""
        of = _make_orderflow(tmp_path)
        old_ts = int(time.time()) - 1000  # downloaded 1000s ago
        manifest_ts: ManifestTimestamps = {("964118191", 2024): old_ts}

        # Enqueue now (create_time > old_ts) → should be pending
        of.enqueue_fast([("964118191", 2024)], source="patch")
        pending = of.pending(1, manifest_ts=manifest_ts)
        assert pending.num_rows == 1

    def test_old_entry_not_pending(self, tmp_path: Path) -> None:
        """An entry whose create_time <= manifest download is NOT pending."""
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])

        # Manifest says it was downloaded AFTER the enqueue
        future_ts = int(time.time()) + 9999
        manifest_ts: ManifestTimestamps = {("964118191", 2024): future_ts}
        pending = of.pending(1, manifest_ts=manifest_ts)
        assert pending.num_rows == 0

    def test_excludes_discovery_stubs(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        of.enqueue_slow_discovery(["964118191"])
        pending = of.pending(1, manifest_ts={})
        assert pending.num_rows == 1  # only the fast-lane entry, not the stub

    def test_sorts_by_priority_descending(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        of.enqueue_slow("964118191", [2020])
        pending = of.pending(1, manifest_ts={})
        assert pending.num_rows == 2
        p0 = pending.column("processing_priority")[0].as_py()
        p1 = pending.column("processing_priority")[1].as_py()
        assert p0 > p1


class TestFastLanePending:
    def test_uses_priority_equals_create_time(self, tmp_path: Path) -> None:
        """Fast lane identified by processing_priority == create_time."""
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        of.enqueue_slow("964118191", [2020])
        fast = of.fast_lane_pending(1, manifest_ts={})
        assert fast.num_rows == 1
        assert fast.column("year")[0].as_py() == 2024
        # Verify the structural identity: priority == create_time
        assert fast.column("processing_priority")[0].as_py() == fast.column("create_time")[0].as_py()


class TestCompact:
    def test_removes_completed_entries(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024), ("964118191", 2023)])
        # Manifest says 2023 was downloaded after enqueue
        manifest_ts: ManifestTimestamps = {("964118191", 2023): int(time.time()) + 9999}
        removed = of.compact(1, manifest_ts)
        assert removed == 1
        table = of.load_shard(1)
        assert table.num_rows == 1
        assert table.column("year")[0].as_py() == 2024

    def test_keeps_correction_entries(self, tmp_path: Path) -> None:
        """Entries with create_time > manifest ts are NOT compacted."""
        of = _make_orderflow(tmp_path)
        old_ts = int(time.time()) - 1000
        manifest_ts: ManifestTimestamps = {("964118191", 2024): old_ts}
        of.enqueue_fast([("964118191", 2024)], source="patch")
        removed = of.compact(1, manifest_ts)
        assert removed == 0

    def test_keeps_discovery_stubs(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_slow_discovery(["964118191"])
        removed = of.compact(1, {})
        assert removed == 0
        assert of.load_shard(1).num_rows == 1


class TestShardStats:
    def test_counts(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        of.enqueue_slow("964118191", [2020])
        of.enqueue_slow_discovery(["964118191"])
        stats = of.shard_stats(1, manifest_ts={})
        assert stats["total_entries"] == 3
        assert stats["fast_lane_pending"] == 1
        assert stats["slow_lane_pending"] == 1
        assert stats["discovery_stubs"] == 1
