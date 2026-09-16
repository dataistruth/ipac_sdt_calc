"""Bounded parallel helpers for independent Spark planning and write tasks."""

from __future__ import annotations

import contextvars
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_THREADS = 4
logger = logging.getLogger(__name__)


def run_parallel(tasks, label: str):
    """Execute named callables with an invariant maximum of four threads."""
    if not tasks:
        return []
    workers = min(MAX_THREADS, len(tasks))
    started = time.time()
    values = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for name, task in tasks:
            context = contextvars.copy_context()
            futures[pool.submit(context.run, task)] = name
        for future in as_completed(futures):
            name = futures[future]
            values[name] = future.result()
    logger.info(
        "[parallel] %s: tasks=%d workers=%d wall=%.2fs",
        label,
        len(tasks),
        workers,
        time.time() - started,
    )
    return [(name, values[name]) for name, _ in tasks]


def isolated_collector_cfg(cfg: dict) -> dict:
    """Copy mutable collector state for one concurrent result builder."""
    local = dict(cfg)
    local["_parquet_results"] = {}
    local["_schema_cache"] = {}
    return local


def merge_collector_cfg(target: dict, local: dict) -> None:
    """Merge one task's collected DataFrames in deterministic caller order."""
    target_results = target.setdefault("_parquet_results", {})
    for table_name, df in local.get("_parquet_results", {}).items():
        if table_name in target_results:
            target_results[table_name] = target_results[table_name].unionByName(
                df, allowMissingColumns=True
            )
        else:
            target_results[table_name] = df
    target.setdefault("_schema_cache", {}).update(local.get("_schema_cache", {}))
