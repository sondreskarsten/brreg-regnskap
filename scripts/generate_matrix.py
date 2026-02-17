"""Generate a dynamic GitHub Actions matrix for parallel sync jobs.

This script runs in the 'plan' job of the sync workflow. It determines the
sync mode (full or incremental), reads the checkpoint from storage, and
outputs a JSON matrix that splits the work across multiple parallel jobs.

For full sync: splits the 9-digit orgnr space into N ranges.
For incremental sync: may output a single job or split by changed entities.

Output format (written to GITHUB_OUTPUT):
    matrix={"include":[{"start":"800000000","end":"850000000"}, ...]}
    mode=full|incremental

Usage:
    python scripts/generate_matrix.py --storage-path s3://bucket/prefix --mode full
"""

from __future__ import annotations

import json
import os
from argparse import ArgumentParser


def generate_full_matrix(num_shards: int = 20) -> list[dict[str, str]]:
    """Split the 9-digit orgnr space into ranges.

    Norwegian org numbers are 9 digits (800000000-999999999 for most,
    but the full range is 000000000-999999999).
    """
    start = 800_000_000
    end = 999_999_999
    step = (end - start) // num_shards
    shards = []
    for i in range(num_shards):
        s = start + i * step
        e = start + (i + 1) * step - 1 if i < num_shards - 1 else end
        shards.append({"start": str(s), "end": str(e)})
    return shards


def generate_incremental_matrix() -> list[dict[str, str]]:
    """For incremental sync, typically a single job suffices.

    If the updates API returns many changes, this could be split further.
    For now, use the full orgnr range as a single shard.
    """
    return [{"start": "800000000", "end": "999999999"}]


def main() -> None:
    parser = ArgumentParser(description="Generate GHA matrix for BRREG sync")
    parser.add_argument("--storage-path", required=True)
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--num-shards", type=int, default=20)
    args = parser.parse_args()

    if args.mode == "full":
        shards = generate_full_matrix(args.num_shards)
    else:
        shards = generate_incremental_matrix()

    matrix = json.dumps({"include": shards})

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"matrix={matrix}\n")
            f.write(f"mode={args.mode}\n")
    else:
        print(f"matrix={matrix}")
        print(f"mode={args.mode}")


if __name__ == "__main__":
    main()
