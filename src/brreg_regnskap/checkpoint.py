"""Checkpoint persistence for resume-safe sync operations.

Stores a small JSON file in the storage backend tracking:
    - last_oppdateringsid: cursor for the Enhetsregisteret updates API
    - last_orgnr_processed: for resuming full syncs from where they stopped
    - run_started_at: ISO timestamp of current/last run start
    - mode: "full" or "incremental"
    - shard_range: optional (start, end) orgnr range for matrix jobs

Implementation notes:
    - The checkpoint file is tiny (<1KB) — read-modify-write is fine.
    - Save after every checkpoint_interval entities processed.
    - On startup, load checkpoint and resume from stored position.
    - For matrix jobs, each shard has its own checkpoint (derived from shard manifest path).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brreg_regnskap.storage import StorageBackend


@dataclass
class CheckpointState:
    """Serializable checkpoint state."""

    last_oppdateringsid: int = 0
    last_orgnr_processed: str | None = None
    run_started_at: str | None = None
    mode: str = "full"
    phase: str = "metadata"
    current_year: int | None = None
    shard_range_start: str | None = None
    shard_range_end: str | None = None
    entities_processed: int = 0
    entities_total: int | None = None
    errors: int = 0

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), indent=2).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> CheckpointState:
        raw = json.loads(data)
        # Filter out unknown fields for forward compatibility — a newer
        # version of the checkpoint may have added fields we don't know about.
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in raw.items() if k in known})


class CheckpointManager:
    """Manages checkpoint state for resumable sync operations.

    Usage:
        cp = CheckpointManager(storage, settings.checkpoint_path)
        state = cp.load()
        state.last_orgnr_processed = "964118191"
        state.entities_processed += 1
        cp.save(state)
    """

    def __init__(self, storage: StorageBackend, checkpoint_path: str) -> None:
        self._storage = storage
        self._checkpoint_path = checkpoint_path

    def load(self) -> CheckpointState:
        """Load checkpoint from storage. Returns default state if not found."""
        if not self._storage.exists(self._checkpoint_path):
            return CheckpointState()
        data = self._storage.read_bytes(self._checkpoint_path)
        return CheckpointState.from_json(data)

    def save(self, state: CheckpointState) -> None:
        """Persist checkpoint state to storage."""
        self._storage.write_bytes(self._checkpoint_path, state.to_json())

    def clear(self) -> None:
        """Delete the checkpoint file. Used when a sync completes successfully."""
        self._storage.delete(self._checkpoint_path)
