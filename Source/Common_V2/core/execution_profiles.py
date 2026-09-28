"""Shared three-tier Spark execution profiles for AllocationV2 SPs.

AQE is not part of this config. Databricks enables adaptive query execution
by default; orchestrators must not set or unset AQE from a profile.
"""

from __future__ import annotations

EXECUTION_PROFILES = {
    "low": {
        "shuffle_partitions": 32,
        "checkpoint_mode": 4,  # all localCheckpoint
        "max_threads": 4,
    },
    "medium": {
        "shuffle_partitions": 48,
        "checkpoint_mode": 2,  # hybrid: odd local / even Delta
        "max_threads": 4,
    },
    "big": {
        "shuffle_partitions": 64,
        "checkpoint_mode": 1,  # all stats-off Delta
        "max_threads": 4,
    },
}


def resolve_execution_profile(name: str = "low") -> dict:
    key = str(name or "low").strip().lower()
    if key not in EXECUTION_PROFILES:
        raise ValueError(
            "ExecutionProfile must be low, medium, or big; "
            f"got {name!r}"
        )
    return dict(EXECUTION_PROFILES[key])
