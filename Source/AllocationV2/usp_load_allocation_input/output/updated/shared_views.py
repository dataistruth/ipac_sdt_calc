"""Parallel shared-view registration."""

import time

from Common_V2.core.helpers import log_section, log_timing

from .parallel_helpers import normalize_workers
from .shared_views_builders import register_all


def register_shared_views_parallel(spark, cfg, max_threads=None):
    """Public name used by the orchestrator."""
    workers = normalize_workers(
        max_threads if max_threads is not None else cfg.get("max_threads", 4),
        cfg.get("MaxThreads"),
    )
    log_section("register_shared_views")
    started = time.time()
    register_all(spark, cfg, workers)
    log_timing("register_shared_views", started)


register_shared_views = register_shared_views_parallel
