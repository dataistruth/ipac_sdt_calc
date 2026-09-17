"""Bounded parallel helpers for the updated allocation-input package."""

import contextvars
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from Common_V2.core import DEFAULT_MAX_THREADS, MAX_PARALLEL_THREADS
except ImportError:
    DEFAULT_MAX_THREADS = 4
    MAX_PARALLEL_THREADS = 4


def normalize_workers(value=None) -> int:
    try:
        workers = int(DEFAULT_MAX_THREADS if value is None else value)
    except (TypeError, ValueError):
        workers = DEFAULT_MAX_THREADS
    return max(1, min(workers, MAX_PARALLEL_THREADS))


def run_parallel(tasks, label: str, max_threads=None):
    """Execute named callables concurrently and preserve input result order."""
    if not tasks:
        print(f"[parallel:skip] {label}: no tasks", flush=True)
        return []
    workers = min(normalize_workers(max_threads), len(tasks))
    names = [name for name, _ in tasks]
    if workers == 1:
        print(f"[parallel:serial] {label}: names={names}", flush=True)
        return [(name, task()) for name, task in tasks]

    started = time.time()
    print(
        f"[PARALLEL] ▶ '{label}' RUNNING IN PARALLEL: {len(tasks)} tasks "
        f"across {workers} threads → {names}",
        flush=True,
    )

    def wrapped(name, task):
        thread = threading.current_thread().name
        task_started = time.time()
        result = task()
        return result, thread, time.time() - task_started

    values = {}
    prefix = f"par-{re.sub(r'[^A-Za-z0-9_]', '_', label)[:24]}"
    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix=prefix
    ) as pool:
        futures = {}
        for name, task in tasks:
            context = contextvars.copy_context()
            futures[pool.submit(context.run, wrapped, name, task)] = name
        for future in as_completed(futures):
            name = futures[future]
            result, thread, elapsed = future.result()
            values[name] = result
            print(
                f"[parallel:done] {label}/{name} thread={thread} "
                f"elapsed={elapsed:.2f}s",
                flush=True,
            )
    wall = time.time() - started
    print(
        f"[PARALLEL] ✔ '{label}' finished concurrently: wall={wall:.2f}s "
        f"(threads={workers}, tasks={len(tasks)})",
        flush=True,
    )
    return [(name, values[name]) for name, _ in tasks]


def isolated_collector_cfg(cfg: dict) -> dict:
    local = dict(cfg)
    local["_parquet_results"] = {}
    local["_schema_cache"] = {}
    return local


def merge_collector_cfg(target: dict, local: dict) -> None:
    results = target.setdefault("_parquet_results", {})
    for table_name, df in local.get("_parquet_results", {}).items():
        if table_name in results:
            results[table_name] = results[table_name].unionByName(
                df, allowMissingColumns=True
            )
        else:
            results[table_name] = df
    target.setdefault("_schema_cache", {}).update(local.get("_schema_cache", {}))


__all__ = [
    "isolated_collector_cfg",
    "merge_collector_cfg",
    "normalize_workers",
    "run_parallel",
]
