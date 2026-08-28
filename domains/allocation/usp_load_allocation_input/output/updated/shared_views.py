"""
Parallel register_shared_views — builders from ai_shared_views (DataFrame API).

Falls back to sequential ai_shared_views.register_shared_views when workers=1.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from .parent import output_module
from .parallel_config import parallel_workers
from .shared_views_builders import (
    INDEPENDENT_VIEW_REGISTRARS,
    register_shared_views_parallel_builders,
)

register_shared_views = output_module("ai_shared_views").register_shared_views

logger = logging.getLogger(__name__)


def register_shared_views_parallel(spark: SparkSession, cfg: dict) -> None:
    workers = parallel_workers(cfg)

    if workers <= 1:
        register_shared_views(spark, cfg)
        return

    print(
        f"[parallel_config] register_shared_views: "
        f"{len(INDEPENDENT_VIEW_REGISTRARS)} independent lookup(s) "
        f"+ reclass + lower_tier, max_workers={workers}"
    )
    register_shared_views_parallel_builders(spark, cfg, workers)
