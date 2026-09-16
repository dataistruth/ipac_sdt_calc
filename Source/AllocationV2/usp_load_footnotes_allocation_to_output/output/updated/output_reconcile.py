"""Reversible A/B state management and deterministic output metrics."""

from __future__ import annotations

import re
import uuid
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

_SAFE_NAME = re.compile(r"[^A-Za-z0-9_]")
_STATS_KEY = "spark.databricks.delta.stats.collect"


def _safe_name(value: object) -> str:
    return _SAFE_NAME.sub("_", str(value))


def _quoted_fqn(catalog: str, schema: str, table: str) -> str:
    return ".".join(f"`{part.replace('`', '``')}`" for part in (catalog, schema, table))


def _output_sql_predicate(run_id: int) -> str:
    return (
        f"RunID = {int(run_id)} AND "
        "(AllocationType LIKE 'Footnote%' OR AllocationType = '704c Footnote')"
    )


def _output_filter(df: DataFrame, run_id: int) -> DataFrame:
    return df.filter(
        (F.col("RunID") == int(run_id))
        & (
            F.col("AllocationType").like("Footnote%")
            | (F.col("AllocationType") == "704c Footnote")
        )
    )


def _write_snapshot(df: DataFrame, fqn: str, spark: SparkSession) -> None:
    try:
        previous = spark.conf.get(_STATS_KEY)
        existed = True
    except Exception:
        previous = None
        existed = False
    try:
        spark.conf.set(_STATS_KEY, "false")
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .saveAsTable(fqn)
        )
    finally:
        try:
            if existed and previous is not None:
                spark.conf.set(_STATS_KEY, previous)
            else:
                spark.conf.unset(_STATS_KEY)
        except Exception:
            pass


def create_benchmark_snapshot(
    spark: SparkSession,
    catalog: str,
    schema: str,
    run_id: int,
    execution_id: str | None = None,
) -> dict[str, Any]:
    """Snapshot all mutable rows before either benchmark variant runs."""
    suffix = _safe_name(execution_id or uuid.uuid4().hex)[:48]
    input_backup = _quoted_fqn(
        catalog, schema, f"_tmp_footnote_bench_input_{int(run_id)}_{suffix}"
    )
    output_backup = _quoted_fqn(
        catalog, schema, f"_tmp_footnote_bench_output_{int(run_id)}_{suffix}"
    )
    allocation_input = _quoted_fqn(catalog, schema, "AllocationInput")
    allocation_output = _quoted_fqn(catalog, schema, "AllocationOutput")

    try:
        _write_snapshot(
            spark.table(allocation_input).filter(
                F.col("RunID") == int(run_id)
            ),
            input_backup,
            spark,
        )
        _write_snapshot(
            _output_filter(spark.table(allocation_output), run_id),
            output_backup,
            spark,
        )
    except Exception:
        for backup in (input_backup, output_backup):
            try:
                spark.sql(f"DROP TABLE IF EXISTS {backup}")
            except Exception:
                pass
        raise
    snapshot = {
        "catalog": catalog,
        "schema": schema,
        "run_id": int(run_id),
        "allocation_input": allocation_input,
        "allocation_output": allocation_output,
        "input_backup": input_backup,
        "output_backup": output_backup,
    }
    snapshot["baseline_metrics"] = capture_output_metrics(
        spark, catalog, schema, run_id
    )
    print(
        f"[reconcile] snapshotted RunID={run_id}: "
        f"{input_backup}, {output_backup}"
    )
    return snapshot


def _restore_input(spark: SparkSession, snapshot: dict[str, Any]) -> None:
    run_id = int(snapshot["run_id"])
    spark.sql(
        f"DELETE FROM {snapshot['allocation_input']} WHERE RunID = {run_id}"
    )
    (
        spark.table(snapshot["input_backup"])
        .write.format("delta")
        .mode("append")
        .saveAsTable(snapshot["allocation_input"])
    )


def _purge_generated_output(
    spark: SparkSession,
    snapshot: dict[str, Any],
) -> None:
    spark.sql(
        f"DELETE FROM {snapshot['allocation_output']} WHERE "
        f"{_output_sql_predicate(snapshot['run_id'])}"
    )


def reset_before_variant(
    spark: SparkSession,
    snapshot: dict[str, Any],
) -> None:
    """Restore input and remove prior variant output before one A/B run."""
    _restore_input(spark, snapshot)
    _purge_generated_output(spark, snapshot)
    print(f"[reconcile] reset RunID={snapshot['run_id']} before variant")


def restore_original_state(
    spark: SparkSession,
    snapshot: dict[str, Any],
) -> None:
    """Restore both production tables to their exact pre-benchmark rows."""
    _restore_input(spark, snapshot)
    _purge_generated_output(spark, snapshot)
    (
        spark.table(snapshot["output_backup"])
        .write.format("delta")
        .mode("append")
        .saveAsTable(snapshot["allocation_output"])
    )
    restored_metrics = capture_output_metrics(
        spark,
        snapshot["catalog"],
        snapshot["schema"],
        snapshot["run_id"],
    )
    mismatches = compare_variants(
        snapshot["baseline_metrics"],
        restored_metrics,
    )
    if mismatches:
        raise RuntimeError(
            "Benchmark state restoration verification failed: "
            + "; ".join(mismatches[:10])
        )
    print(f"[reconcile] restored original state for RunID={snapshot['run_id']}")


def drop_benchmark_snapshot(
    spark: SparkSession,
    snapshot: dict[str, Any],
) -> None:
    for key in ("input_backup", "output_backup"):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {snapshot[key]}")
        except Exception:
            print(f"[reconcile] warning: failed to drop {snapshot[key]}")


def _metric_for_frame(
    df: DataFrame,
    measure_columns: tuple[str, ...],
) -> dict[str, Any]:
    columns = sorted(df.columns, key=str.lower)
    row_hash = F.xxhash64(
        *[
            F.coalesce(
                F.col(f"`{name.replace('`', '``')}`").cast("string"),
                F.lit("<NULL>"),
            )
            for name in columns
        ]
    )
    aggregations = [
        F.count(F.lit(1)).cast("long").alias("row_count"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("hash_sum"),
        F.min(row_hash).alias("hash_min"),
        F.max(row_hash).alias("hash_max"),
    ]
    for name in measure_columns:
        if name in df.columns:
            aggregations.append(
                F.sum(F.col(name).cast("decimal(38,10)")).alias(f"sum_{name}")
            )
    row = df.agg(*aggregations).collect()[0].asDict()
    return {
        "schema": sorted(
            (
                (field.name, field.dataType.simpleString())
                for field in df.schema
            ),
            key=lambda item: item[0].lower(),
        ),
        **{
            key: (
                str(value)
                if value is not None and (key.startswith("sum_") or key == "hash_sum")
                else value
            )
            for key, value in row.items()
        },
    }


def capture_output_metrics(
    spark: SparkSession,
    catalog: str,
    schema: str,
    run_id: int,
) -> dict[str, dict[str, Any]]:
    allocation_output = spark.table(
        _quoted_fqn(catalog, schema, "AllocationOutput")
    )
    allocation_input = spark.table(
        _quoted_fqn(catalog, schema, "AllocationInput")
    ).filter(F.col("RunID") == int(run_id))
    return {
        "AllocationOutput": _metric_for_frame(
            _output_filter(allocation_output, run_id), ("Amount",)
        ),
        "AllocationInput": _metric_for_frame(
            allocation_input, ("Amount", "Amount704b")
        ),
    }


def compare_variants(
    original: dict[str, dict[str, Any]],
    updated: dict[str, dict[str, Any]],
) -> list[str]:
    mismatches: list[str] = []
    for table in sorted(set(original) | set(updated)):
        if table not in original or table not in updated:
            mismatches.append(f"{table}: missing from one variant")
            continue
        keys = sorted(set(original[table]) | set(updated[table]))
        for key in keys:
            if original[table].get(key) != updated[table].get(key):
                mismatches.append(
                    f"{table}.{key}: original={original[table].get(key)!r} "
                    f"updated={updated[table].get(key)!r}"
                )
    return mismatches


def summarize_metrics(metrics: dict[str, dict[str, Any]]) -> dict[str, int]:
    return {
        "allocation_output_rows": int(
            metrics.get("AllocationOutput", {}).get("row_count") or 0
        ),
        "allocation_input_rows": int(
            metrics.get("AllocationInput", {}).get("row_count") or 0
        ),
    }
