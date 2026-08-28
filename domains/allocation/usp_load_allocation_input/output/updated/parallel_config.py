"""
Parallel helpers for shared view registration (ThreadPoolExecutor).

Used by shared_views.py and shared_views_builders.py.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

TaskFn = Callable[[], dict[str, Any] | None]


def parallel_workers(cfg: dict | None, default: int = 3) -> int:
    if cfg is None:
        return default
    raw = cfg.get("parallel_config_workers", default)
    try:
        workers = int(raw or 0)
    except (TypeError, ValueError):
        workers = default
    return max(1, workers)


def merge_cfg_slices(base: dict, slices: list[tuple[str, dict[str, Any] | None]]) -> dict:
    out = dict(base)
    for name, piece in slices:
        if piece:
            out.update(piece)
    return out


def run_parallel_tasks(
    spark: SparkSession,
    label: str,
    tasks: list[tuple[str, TaskFn]],
    workers: int,
) -> list[tuple[str, dict[str, Any] | None]]:
    if not tasks:
        return []

    workers = max(1, min(workers, len(tasks)))
    if workers <= 1 or len(tasks) <= 1:
        results: list[tuple[str, dict[str, Any] | None]] = []
        for name, fn in tasks:
            t0 = time.time()
            try:
                piece = fn()
                elapsed = time.time() - t0
                print(f"[parallel_config] {label}.{name} sequential {elapsed:.2f}s")
                results.append((name, piece))
            except Exception as exc:
                raise RuntimeError(f"{label}.{name} failed: {exc}") from exc
        return results

    print(
        f"[parallel_config] {label}: {len(tasks)} task(s), max_workers={workers}"
    )
    results: list[tuple[str, dict[str, Any] | None]] = []
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(fn): name for name, fn in tasks}
        for fut in as_completed(future_map):
            name = future_map[fut]
            t0 = time.time()
            try:
                piece = fut.result()
                elapsed = time.time() - t0
                print(f"[parallel_config] {label}.{name} ok {elapsed:.2f}s")
                results.append((name, piece))
            except Exception as exc:
                errors.append(f"{name}: {exc}")

    if errors:
        raise RuntimeError(f"{label} parallel tasks failed: " + "; ".join(errors))

    return results


