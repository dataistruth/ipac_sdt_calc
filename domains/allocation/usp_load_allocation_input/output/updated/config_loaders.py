"""
Parallel wrappers for load_common_config and load_config.

Steps remain sequential; each step may fan out to parallel_config_workers tasks.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import SparkSession

from Common_V2.core.config import load_common_config

from . import config_parallel_hooks
from ..ai_config_service import load_config
from .parallel_config import (
    discover_load_common_config_tasks,
    discover_load_config_tasks,
    merge_cfg_slices,
    parallel_workers,
    run_parallel_tasks,
)

logger = logging.getLogger(__name__)


def _normalize_task_result(result: Any) -> dict[str, Any] | None:
    if result is None:
        return None
    if isinstance(result, dict):
        return result
    raise TypeError(f"parallel config task must return dict or None, got {type(result)}")


def _resolve_common_config_tasks(
    spark: SparkSession,
    kwargs: dict[str, Any],
) -> list[tuple[str, Any]]:
    explicit = config_parallel_hooks.load_common_config_tasks(spark, **kwargs)
    if explicit:
        return explicit
    return discover_load_common_config_tasks(spark, kwargs)


def _resolve_load_config_tasks(spark: SparkSession, cfg: dict) -> list[tuple[str, Any]]:
    explicit = config_parallel_hooks.load_config_tasks(spark, cfg)
    if explicit:
        return explicit
    return discover_load_config_tasks(spark, cfg)


def load_common_config_parallel(
    spark: SparkSession,
    cfg: dict | None = None,
    entity_id: int | None = None,
    client_id: int | None = None,
    tax_period_id: int | None = None,
    run_id: int | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    call_from: str | None = None,
    **kwargs: Any,
) -> dict:
    """
    Build cfg via load_common_config, optionally prefetching with parallel tasks.

    When parallel tasks are discovered (hooks or Common_V2), they run first and
    merge into kwargs; then load_common_config runs once (correct final cfg).
    """
    workers = parallel_workers(cfg)
    kw = {
        "entity_id": entity_id,
        "client_id": client_id,
        "tax_period_id": tax_period_id,
        "run_id": run_id,
        "catalog": catalog,
        "schema": schema,
        "call_from": call_from,
        **kwargs,
    }

    tasks = _resolve_common_config_tasks(spark, kw)
    if workers > 1 and len(tasks) >= 2:
        wrapped = [
            (name, lambda f=fn: _normalize_task_result(f()))
            for name, fn in tasks
        ]
        slices = run_parallel_tasks(spark, "load_common_config", wrapped, workers)
        prefetch = merge_cfg_slices({}, slices)
        for key, value in prefetch.items():
            if key not in kw or kw[key] is None:
                kw[key] = value
        print(
            f"[parallel_config] load_common_config prefetch merged "
            f"{len(prefetch)} key(s); running load_common_config"
        )

    return load_common_config(
        spark,
        entity_id=kw.get("entity_id"),
        client_id=kw.get("client_id"),
        tax_period_id=kw.get("tax_period_id"),
        run_id=kw.get("run_id"),
        catalog=kw.get("catalog"),
        schema=kw.get("schema"),
        call_from=kw.get("call_from"),
    )


def load_config_parallel(spark: SparkSession, cfg: dict) -> dict:
    """
    Parallel load_config when split tasks exist; otherwise sequential load_config.
    """
    workers = parallel_workers(cfg)
    tasks = _resolve_load_config_tasks(spark, cfg)

    if workers <= 1 or len(tasks) < 2:
        if workers > 1 and len(tasks) < 2:
            print(
                "[parallel_config] load_config: no splittable tasks found; "
                "sequential load_config"
            )
        return load_config(spark, cfg)

    wrapped = [
        (name, lambda f=fn: _normalize_task_result(f()))
        for name, fn in tasks
    ]
    slices = run_parallel_tasks(spark, "load_config", wrapped, workers)
    return merge_cfg_slices(cfg, slices)
