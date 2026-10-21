"""Spark execution profiles for the optimized AllocationV2 SPs.

``small`` uses localCheckpoint; ``large`` uses the checkpoint.py default
(Delta). Change
``DEFAULT_EXECUTION_PROFILE`` to switch every SP that is called without an
explicit ExecutionProfile.

AQE is not part of this config. Databricks enables adaptive query execution
by default; orchestrators must not set or unset AQE from a profile.
"""

from __future__ import annotations

import logging

from Common_V2.core.checkpoint import (
    DEFAULT_CHECKPOINT_MODE,
    DELTA_CHECKPOINT_MODE,
    LOCAL_CHECKPOINT_MODE,
)

logger = logging.getLogger(__name__)

DEFAULT_EXECUTION_PROFILE = "small"

EXECUTION_PROFILES = {
    "small": {
        "shuffle_partitions": 32,
        "checkpoint_mode": LOCAL_CHECKPOINT_MODE,
        "max_threads": 4,
    },
    "large": {
        "shuffle_partitions": 64,
        "checkpoint_mode": DEFAULT_CHECKPOINT_MODE,
        "max_threads": 4,
    },
}


def resolve_execution_profile(name: str = None, checkpoint_mode=None) -> dict:
    """Return the profile settings plus ``name``. Never raises.

    Blank or ``"default"`` selects ``DEFAULT_EXECUTION_PROFILE``; any other
    unknown name falls back to ``large`` (Delta). A non-blank
    ``checkpoint_mode`` overrides the profile's mode: 2 = local, anything
    else = Delta.
    """
    key = str(name or "").strip().lower()
    if key in ("", "default"):
        key = DEFAULT_EXECUTION_PROFILE
    if key not in EXECUTION_PROFILES:
        logger.warning(f"[profile] Unknown ExecutionProfile {name!r}; using 'large' (Delta)")
        key = "large"
    profile = dict(EXECUTION_PROFILES[key], name=key)
    if checkpoint_mode is not None and str(checkpoint_mode).strip() != "":
        try:
            mode = int(str(checkpoint_mode).strip())
        except ValueError:
            mode = None
        if mode != LOCAL_CHECKPOINT_MODE:
            if mode != DELTA_CHECKPOINT_MODE:
                logger.warning(f"[profile] Unknown CheckpointMode {checkpoint_mode!r}; using Delta")
            mode = DEFAULT_CHECKPOINT_MODE
        profile["checkpoint_mode"] = mode
    return profile
