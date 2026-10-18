"""Bounded workers. Output/Input writes stay sequential."""

from __future__ import annotations

KNOWN_GROUPS = frozenset()


def normalize_workers(max_threads=4, MaxThreads=None):
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        return max(1, min(int(raw), 4))
    except (TypeError, ValueError):
        return 4


def parse_enabled_groups(parallel_groups="all", ParallelGroups=None):
    del parallel_groups, ParallelGroups
    return set()


__all__ = ["KNOWN_GROUPS", "normalize_workers", "parse_enabled_groups"]
