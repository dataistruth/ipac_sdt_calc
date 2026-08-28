"""
Optional hooks for parallel config loading.

Edit get_shared_view_sql_map with your view SQL for parallel register_shared_views.

load_common_config_tasks defaults to UC metadata prefetch (DESCRIBE) when catalog+schema set.
"""

from __future__ import annotations

from typing import Any, Callable

from pyspark.sql import SparkSession


def _prefetch_describe(spark: SparkSession, fqn: str) -> dict[str, Any]:
    spark.sql(f"DESCRIBE TABLE {fqn}")
    return {}


def get_shared_view_sql_map(cfg: dict) -> dict[str, str] | None:
    """
    Return view_name → SQL body (SELECT …) for parallel temp view registration.

    Populate from your ai_shared_views.py loop. Example:

        from Common_V2.core.helpers import table_prefix
        p = table_prefix(cfg)
        rid = cfg["run_id"]
        return {
            "_example_view": f"SELECT * FROM {p}.SomeTable WHERE RunID = {rid}",
        }
    """
    return None


def load_config_tasks(
    spark: SparkSession,
    cfg: dict,
) -> list[tuple[str, Callable[[], dict[str, Any] | None]]] | None:
    """
    Optional explicit parallel tasks for load_config.

    Each task returns a cfg dict slice. Example:

        return [
            ("workflows", lambda: load_workflow_ids(spark, cfg)),
            ("flags", lambda: load_feature_flags(spark, cfg)),
            ("schema_cache", lambda: load_schema_cache(spark, cfg)),
        ]
    """
    return None


def load_common_config_tasks(
    spark: SparkSession,
    **kwargs: Any,
) -> list[tuple[str, Callable[[], dict[str, Any] | None]]] | None:
    """
    Parallel metadata prefetch before load_common_config (default).

    Warms UC table metadata with DESCRIBE in 3 threads — does not replace
    load_common_config. Returns empty dict slices.
    """
    catalog = kwargs.get("catalog")
    schema = kwargs.get("schema")
    if not catalog or not schema:
        return None

    prefix = f"`{catalog}`.`{schema}`"
    warm_tables = [
        "AllocationRun",
        "Entity",
        "TaxPeriod",
    ]
    return [
        (
            f"prefetch_{table}",
            lambda t=table: _prefetch_describe(spark, f"{prefix}.`{t}`"),
        )
        for table in warm_tables
    ]
