"""Compatibility re-export. Prefer importing from checkpoint."""

from .checkpoint import (
    cache_for_run,
    current_run,
    current_run_scoped,
    prune_to_lower_tier_runs,
    scoped,
    unpersist_cached,
)
