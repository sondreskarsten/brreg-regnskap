"""Shard assignment for parallel workers.

Each worker claims a digit 0-9 and processes only orgnr where
int(orgnr) % 10 == digit. Claim detection is based on manifest
file modification time — if a shard's manifest was updated in the
last 2 hours, another worker owns it.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import structlog

from brreg_regnskap.storage import StorageBackend

logger = structlog.get_logger()

SHARD_LOCK_HOURS = 2


def claim_shard(storage: StorageBackend, storage_path: str) -> int:
    """Find an unclaimed shard digit (0-9) and return it.

    A shard is "claimed" if its manifest_shard_{d}.parquet was modified
    within the last SHARD_LOCK_HOURS hours. Picks randomly from free shards.

    Raises RuntimeError if all 10 shards are claimed.
    """
    cutoff = datetime.now(UTC) - timedelta(hours=SHARD_LOCK_HOURS)
    free: list[int] = []

    for digit in range(10):
        path = f"{storage_path}/manifest_shard_{digit}.parquet"
        if not storage.exists(path):
            free.append(digit)
            continue
        mtime = storage.modified_time(path)
        if mtime is None or mtime < cutoff:
            free.append(digit)

    if not free:
        raise RuntimeError(
            f"All 10 shards claimed (manifests updated within {SHARD_LOCK_HOURS}h). "
            "Wait for a worker to finish or increase SHARD_LOCK_HOURS."
        )

    chosen = random.choice(free)
    logger.info(
        "shard_claimed",
        shard=chosen,
        free=free,
        claimed=[d for d in range(10) if d not in free],
    )
    return chosen
