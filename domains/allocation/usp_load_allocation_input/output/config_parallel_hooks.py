"""
Hooks for parallel config — imports local shared_view_sql_map (no ai_* edits).
"""

from __future__ import annotations

from typing import Any, Callable

from pyspark.sql import SparkSession

from .shared_view_sql_map import get_shared_view_sql_map


def _prefetch_describe(spark: SparkSession, fqn: str) -> dict[str, Any]:
    spark.sql(f"DESCRIBE TABLE {fqn}")
    return {}


def load_config_tasks(
    spark: SparkSession,
    cfg: dict,
) -> list[tuple[str, Callable[[], dict[str, Any] | None]]] | None:
    """Optional explicit parallel tasks for load_config."""
    return None


def load_common_config_tasks(
    spark: SparkSession,
    **kwargs: Any,
) -> list[tuple[str, Callable[[], dict[str, Any] | None]]] | None:
    """Parallel UC metadata prefetch before load_common_config."""
    catalog = kwargs.get("catalog")
    schema = kwargs.get("schema")
    if not catalog or not schema:
        return None

    prefix = f"`{catalog}`.`{schema}`"
    warm_tables = ["AllocationRun", "Entity", "TaxPeriod"]
    return [
        (
            f"prefetch_{table}",
            lambda t=table: _prefetch_describe(spark, f"{prefix}.`{t}`"),
        )
        for table in warm_tables
    ]
