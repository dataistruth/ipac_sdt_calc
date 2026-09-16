"""Profiled wrapper preserving Common_V2 checkpoint behavior."""

from __future__ import annotations

import logging
import time

from Common_V2.core.checkpoint import checkpoint as _checkpoint

from .plan_profiler import track_checkpoint_plan

logger = logging.getLogger(__name__)


def checkpoint(spark, df, name: str, cfg: dict):
    track_checkpoint_plan(name, df)
    started = time.time()
    try:
        return _checkpoint(spark, df, name, cfg)
    finally:
        elapsed = time.time() - started
        cfg.setdefault("_checkpoint_elapsed", []).append(
            {"name": name, "elapsed_seconds": elapsed}
        )
        logger.info("[CHECKPOINT TIME] %s elapsed=%.3fs", name, elapsed)
