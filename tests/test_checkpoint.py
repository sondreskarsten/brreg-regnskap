"""Tests for checkpoint persistence."""

from __future__ import annotations

from pathlib import Path

from brreg_regnskap.checkpoint import CheckpointManager, CheckpointState
from brreg_regnskap.config import Settings
from brreg_regnskap.storage import StorageBackend


def _make_checkpoint(tmp_path: Path) -> CheckpointManager:
    settings = Settings(storage_path=str(tmp_path / "store"))
    storage = StorageBackend.from_settings(settings)
    storage.check_credentials()
    return CheckpointManager(storage, settings.checkpoint_path)


class TestCheckpointState:
    def test_default_state(self) -> None:
        s = CheckpointState()
        assert s.last_oppdateringsid == 0
        assert s.last_orgnr_processed is None
        assert s.mode == "full"

    def test_roundtrip_json(self) -> None:
        s = CheckpointState(
            last_oppdateringsid=12345,
            last_orgnr_processed="964118191",
            mode="incremental",
            entities_processed=500,
        )
        data = s.to_json()
        restored = CheckpointState.from_json(data)
        assert restored.last_oppdateringsid == 12345
        assert restored.last_orgnr_processed == "964118191"
        assert restored.mode == "incremental"
        assert restored.entities_processed == 500

    def test_from_json_ignores_unknown_fields(self) -> None:
        """Extra fields from a newer checkpoint version should not crash."""
        import json

        data = json.dumps(
            {
                "last_oppdateringsid": 42,
                "mode": "full",
                "phase": "download",
                "entities_processed": 0,
                "errors": 0,
                "future_field": "should_be_ignored",
                "another_new_field": 123,
            }
        ).encode("utf-8")
        restored = CheckpointState.from_json(data)
        assert restored.last_oppdateringsid == 42
        assert restored.mode == "full"


class TestCheckpointManager:
    def test_load_default_when_missing(self, tmp_path: Path) -> None:
        cp = _make_checkpoint(tmp_path)
        state = cp.load()
        assert state.last_oppdateringsid == 0
        assert state.mode == "full"

    def test_save_and_load(self, tmp_path: Path) -> None:
        cp = _make_checkpoint(tmp_path)
        state = CheckpointState(
            last_oppdateringsid=999,
            last_orgnr_processed="811111111",
            mode="full",
            entities_processed=42,
        )
        cp.save(state)
        loaded = cp.load()
        assert loaded.last_oppdateringsid == 999
        assert loaded.last_orgnr_processed == "811111111"
        assert loaded.entities_processed == 42

    def test_clear(self, tmp_path: Path) -> None:
        cp = _make_checkpoint(tmp_path)
        cp.save(CheckpointState(last_oppdateringsid=1))
        cp.clear()
        state = cp.load()
        assert state.last_oppdateringsid == 0
