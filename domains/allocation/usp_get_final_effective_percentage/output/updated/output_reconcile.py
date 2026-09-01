# updated-package sync marker v2 (2026-09-01): resync the ENTIRE
# output/updated/ folder as one set.
"""Output purge and deterministic metrics for original/updated A/B runs."""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

OUTPUT_TABLES = (
    "FinalEffectivePercentages",
    "FNFinalEffectivePercentages",
    "SM_FinalEffectivePercentages",
)

MEASURE_COLUMNS = (
    "EffPercentage",
    "EffAmount",
)


def _quoted(name: str):
    return F.col(f"`{name.replace('`', '``')}`")


def purge_output_partitions_for_run(
    spark: SparkSession,
    catalog: str,
    schema: str,
    run_id: int,
) -> list[str]:
    """Delete the benchmark RunID from all FEP output tables."""
    purged: list[str] = []
    for table in OUTPUT_TABLES:
        fqn = f"{catalog}.{schema}.{table}"
        if not spark.catalog.tableExists(fqn):
            continue
        spark.sql(f"DELETE FROM {fqn} WHERE RunID = {int(run_id)}")
        purged.append(table)
    print(
        f"[reconcile] pre-run purge RunID={run_id}: "
        f"{len(purged)} table(s)"
    )
    return purged


def capture_output_metrics(
    spark: SparkSession,
    catalog: str,
    schema: str,
    run_id: int,
) -> dict[str, dict[str, Any]]:
    """Capture row count, measure sums, and an order-independent row fingerprint."""
    metrics: dict[str, dict[str, Any]] = {}
    for table in OUTPUT_TABLES:
        fqn = f"{catalog}.{schema}.{table}"
        if not spark.catalog.tableExists(fqn):
            metrics[table] = {"exists": False, "row_count": 0}
            continue

        df = spark.table(fqn).filter(F.col("RunID") == int(run_id))
        columns = sorted(df.columns, key=str.lower)
        hash_inputs = [
            F.coalesce(_quoted(name).cast("string"), F.lit("<NULL>"))
            for name in columns
        ]
        row_hash = F.xxhash64(*hash_inputs)
        aggregations = [
            F.count(F.lit(1)).alias("row_count"),
            F.sum(row_hash.cast("decimal(38,0)")).alias("row_hash_sum"),
            F.min(row_hash).alias("row_hash_min"),
            F.max(row_hash).alias("row_hash_max"),
        ]
        present_measures = [
            name for name in MEASURE_COLUMNS if name in df.columns
        ]
        for name in present_measures:
            aggregations.append(
                F.sum(
                    F.coalesce(
                        _quoted(name).cast("decimal(38,12)"),
                        F.lit(0).cast("decimal(38,12)"),
                    )
                ).alias(f"sum_{name}")
            )

        row = df.agg(*aggregations).first().asDict()
        metrics[table] = {
            "exists": True,
            "columns": columns,
            **{
                key: (str(value) if value is not None else None)
                for key, value in row.items()
            },
        }
    return metrics


def compare_variants(
    original: dict[str, dict[str, Any]],
    updated: dict[str, dict[str, Any]],
) -> list[str]:
    """Return human-readable mismatches; an empty list means parity."""
    mismatches: list[str] = []
    for table in OUTPUT_TABLES:
        left = original.get(table)
        right = updated.get(table)
        if left == right:
            continue
        left = left or {}
        right = right or {}
        keys = sorted(set(left) | set(right))
        for key in keys:
            if left.get(key) != right.get(key):
                mismatches.append(
                    f"{table}.{key}: original={left.get(key)!r}, "
                    f"updated={right.get(key)!r}"
                )
    return mismatches


def summarize_metrics(metrics: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "tables_with_rows": sum(
            int(data.get("row_count", "0") or "0") > 0
            for data in metrics.values()
        ),
        "total_rows": sum(
            int(data.get("row_count", "0") or "0")
            for data in metrics.values()
        ),
    }
