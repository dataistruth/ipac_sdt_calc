"""Parallel shared-view registration."""

import time

from Common_V2.core.helpers import log_section, log_timing

from .shared_views_builders import register_all


def register_shared_views(spark, cfg, max_threads=4):
    log_section("register_shared_views")
    started = time.time()
    register_all(spark, cfg, max_threads)
    log_timing("register_shared_views", started)
