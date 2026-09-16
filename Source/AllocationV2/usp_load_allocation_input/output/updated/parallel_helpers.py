"""Small deterministic thread-pool helpers."""

from __future__ import annotations

import contextvars
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


def normalize_workers(max_threads=4, MaxThreads=None) -> int:
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 8))


def isolated_cfg(cfg: dict) -> dict:
    local = dict(cfg)
    local["_parquet_results"] = {}
    local["_schema_cache"] = {}
    return local


def run_parallel(tasks, max_threads: int, label: str):
    """Return ``[(name, value)]`` in task declaration order."""
    if not tasks:
        return []
    workers = min(normalize_workers(max_threads), len(tasks))
    if workers == 1:
        return [(name, fn()) for name, fn in tasks]

    started = time.time()
    results = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for name, fn in tasks:
            context = contextvars.copy_context()
            futures[pool.submit(context.run, fn)] = name
        for future in as_completed(futures):
            name = futures[future]
            task_started = time.time()
            results[name] = future.result()
            logger.info("[parallel] %s/%s completed at %.3fs", label, name, task_started)
    wall = time.time() - started
    line = f"[parallel] {label}: tasks={len(tasks)} workers={workers} wall={wall:.2f}s"
    print(line)
    logger.info(line)
    return [(name, results[name]) for name, _ in tasks]


def merge_collector_cfg(target: dict, local: dict) -> None:
    """Merge one isolated collector in deterministic caller order."""
    target_results = target.setdefault("_parquet_results", {})
    for table_name, df in local.get("_parquet_results", {}).items():
        if table_name in target_results:
            target_results[table_name] = target_results[table_name].unionByName(
                df, allowMissingColumns=True
            )
        else:
            target_results[table_name] = df
    target.setdefault("_schema_cache", {}).update(local.get("_schema_cache", {}))
