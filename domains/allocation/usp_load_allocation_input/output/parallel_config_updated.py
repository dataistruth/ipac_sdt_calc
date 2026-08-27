"""
Parallel config helpers — ThreadPoolExecutor with cfg slice merge.

Used by config_loaders_updated.py and shared_views_updated.py.
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


def discover_module_tasks(
    module: Any,
    spark: SparkSession,
    cfg: dict,
    suffix: str = "_parallel_task",
    prefix: str = "",
) -> list[tuple[str, TaskFn]]:
    """Collect callables named *_parallel_task or load_*_parallel_task on a module."""
    tasks: list[tuple[str, TaskFn]] = []
    for attr in sorted(dir(module)):
        if not attr.endswith(suffix):
            continue
        fn = getattr(module, attr, None)
        if not callable(fn):
            continue
        name = attr
        if prefix and name.startswith(prefix):
            name = name[len(prefix):]
        name = name.replace(suffix, "")
        tasks.append((name, lambda f=fn: f(spark, cfg)))
    return tasks


def discover_load_config_tasks(spark: SparkSession, cfg: dict) -> list[tuple[str, TaskFn]]:
    """Known optional split functions in ai_config_service."""
    from . import ai_config_service as mod

    known = [
        "load_workflow_ids",
        "load_feature_flags",
        "load_schema_cache",
        "load_sp_scalars",
        "load_investment_tag_workflow",
        "load_output_schema_cache",
        "load_allocation_run_flags",
    ]
    tasks: list[tuple[str, TaskFn]] = []
    for name in known:
        fn = getattr(mod, name, None)
        if callable(fn):
            tasks.append((name, lambda f=fn: f(spark, cfg)))

    tasks.extend(discover_module_tasks(mod, spark, cfg))
    return tasks


def discover_load_common_config_tasks(
    spark: SparkSession,
    kwargs: dict[str, Any],
) -> list[tuple[str, TaskFn]]:
    """Optional split functions on Common_V2.core.config."""
    try:
        from Common_V2.core import config as mod
    except ImportError:
        return []

    known = [
        "load_run_context",
        "load_entity_context",
        "load_catalog_paths",
        "load_allocation_run_context",
        "load_client_entity_context",
        "load_tax_period_context",
    ]
    tasks: list[tuple[str, TaskFn]] = []
    for name in known:
        fn = getattr(mod, name, None)
        if callable(fn):
            tasks.append((name, lambda f=fn: f(spark, **kwargs)))

    # Module-level *_parallel_task hooks
    for attr in sorted(dir(mod)):
        if not attr.endswith("_parallel_task") or not attr.startswith("load_"):
            continue
        fn = getattr(mod, attr, None)
        if callable(fn):
            name = attr.replace("_parallel_task", "")
            tasks.append((name, lambda f=fn: f(spark, **kwargs)))

    return tasks
