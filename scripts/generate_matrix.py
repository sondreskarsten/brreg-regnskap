"""Generate a dynamic GitHub Actions matrix for parallel sync jobs.

This script runs in the 'plan' job of the sync workflow. It outputs a JSON
matrix that splits work across 10 shards (orgnr % 10 = 0..9).

Output format (written to GITHUB_OUTPUT):
    matrix={"include":[{"shard":"0"}, {"shard":"1"}, ...]}

Usage:
    python scripts/generate_matrix.py --storage-path gs://bucket/prefix
"""

from __future__ import annotations

import json
import os
from argparse import ArgumentParser


def generate_shard_matrix() -> list[dict[str, str]]:
    """Generate matrix entries for shards 0-9."""
    return [{"shard": str(d)} for d in range(10)]


def main() -> None:
    parser = ArgumentParser(description="Generate GHA matrix for BRREG sync")
    parser.add_argument("--storage-path", required=True)
    args = parser.parse_args()

    shards = generate_shard_matrix()
    matrix = json.dumps({"include": shards})

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"matrix={matrix}\n")
    else:
        print(f"matrix={matrix}")


if __name__ == "__main__":
    main()
