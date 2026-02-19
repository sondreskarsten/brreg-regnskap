"""Tests for the two-lane orderflow queue."""

from __future__ import annotations

from pathlib import Path

from brreg_regnskap.config import Settings
from brreg_regnskap.orderflow import OrderflowManager, _year_priority
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

    def test_deduplicates(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        added = of.enqueue_fast([("964118191", 2024)])
        assert added == 0

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
        manifest_keys = {("964118191", 2022)}
        added = of.enqueue_slow("964118191", [2022, 2021], manifest_keys=manifest_keys)
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
    def test_returns_non_manifest_entries(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024), ("964118191", 2023)])
        pending = of.pending(1, manifest_keys={("964118191", 2023)})
        assert pending.num_rows == 1
        assert pending.column("year")[0].as_py() == 2024

    def test_excludes_discovery_stubs(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        of.enqueue_slow_discovery(["964118191"])
        pending = of.pending(1, manifest_keys=set())
        assert pending.num_rows == 1  # only the fast-lane entry, not the stub

    def test_sorts_by_priority_descending(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        # Add fast then slow entries
        of.enqueue_fast([("964118191", 2024)])
        of.enqueue_slow("964118191", [2020])
        pending = of.pending(1, manifest_keys=set())
        assert pending.num_rows == 2
        # Fast lane (higher priority) should come first
        p0 = pending.column("processing_priority")[0].as_py()
        p1 = pending.column("processing_priority")[1].as_py()
        assert p0 > p1

    def test_empty_when_all_done(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        pending = of.pending(1, manifest_keys={("964118191", 2024)})
        assert pending.num_rows == 0


class TestFastLanePending:
    def test_excludes_years_api_entries(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        of.enqueue_slow("964118191", [2020])
        fast = of.fast_lane_pending(1, manifest_keys=set())
        assert fast.num_rows == 1
        assert fast.column("year")[0].as_py() == 2024


class TestShardStats:
    def test_counts(self, tmp_path: Path) -> None:
        of = _make_orderflow(tmp_path)
        of.enqueue_fast([("964118191", 2024)])
        of.enqueue_slow("964118191", [2020])
        of.enqueue_slow_discovery(["964118191"])
        stats = of.shard_stats(1, manifest_keys=set())
        assert stats["total_entries"] == 3  # fast + slow + discovery
        assert stats["fast_lane_pending"] == 1
        assert stats["slow_lane_pending"] == 1
        assert stats["discovery_stubs"] == 1
