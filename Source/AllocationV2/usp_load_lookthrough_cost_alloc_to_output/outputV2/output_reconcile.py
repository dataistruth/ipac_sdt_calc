"""Mutation-safe snapshots and fingerprints for both affected tables."""

from __future__ import annotations

import re
import uuid

import pyspark.sql.functions as F
from pyspark.sql.types import NumericType

TABLES = ("LookThroughAllocationOutput", "LookThroughAllocationInput")
_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _fqn(catalog, schema, table):
    return ".".join(
        f"`{part.replace('`', '``')}`"
        for part in (catalog, schema, table)
    )


def _snapshot_write(df, target):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.dataSkippingNumIndexedCols", "0")
        .saveAsTable(target)
    )


def create_benchmark_snapshot(
    spark, catalog, schema, run_id, execution_id=None
):
    suffix = _SAFE.sub(
        "_", str(execution_id or uuid.uuid4().hex)
    )[:48]
    snapshot = {
        "catalog": catalog,
        "schema": schema,
        "run_id": int(run_id),
        "tables": {},
    }
    try:
        for table in TABLES:
            target = _fqn(catalog, schema, table)
            backup = _fqn(
                catalog,
                schema,
                f"_tmp_lookthrough_cost_v2_{table}_{int(run_id)}_{suffix}",
            )
            _snapshot_write(
                spark.table(target).filter(F.col("RunID") == int(run_id)),
                backup,
            )
            snapshot["tables"][table] = {
                "target": target,
                "backup": backup,
            }
    except Exception:
        drop_benchmark_snapshot(spark, snapshot)
        raise
    snapshot["baseline"] = capture_metrics(
        spark, catalog, schema, run_id
    )
    return snapshot


def _restore_table(spark, snapshot, table):
    run_id = snapshot["run_id"]
    spec = snapshot["tables"][table]
    spark.sql(f"DELETE FROM {spec['target']} WHERE RunID = {run_id}")
    (
        spark.table(spec["backup"])
        .write.format("delta")
        .mode("append")
        .saveAsTable(spec["target"])
    )


def reset_before_variant(spark, snapshot):
    for table in TABLES:
        _restore_table(spark, snapshot, table)


def restore_original_state(spark, snapshot):
    reset_before_variant(spark, snapshot)
    restored = capture_metrics(
        spark,
        snapshot["catalog"],
        snapshot["schema"],
        snapshot["run_id"],
    )
    mismatches = compare_metrics(snapshot["baseline"], restored)
    if mismatches:
        raise RuntimeError(
            "State restoration failed: " + "; ".join(mismatches[:10])
        )


def drop_benchmark_snapshot(spark, snapshot):
    for spec in snapshot.get("tables", {}).values():
        try:
            spark.sql(f"DROP TABLE IF EXISTS {spec['backup']}")
        except Exception:
            pass


def _metrics(df):
    ordered = sorted(df.columns, key=str.lower)
    row_hash = F.xxhash64(
        *[
            F.coalesce(F.col(f"`{name}`").cast("string"), F.lit("<NULL>"))
            for name in ordered
        ]
    )
    aggregates = [
        F.count(F.lit(1)).cast("long").alias("row_count"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("hash_sum"),
        F.min(row_hash).alias("hash_min"),
        F.max(row_hash).alias("hash_max"),
    ]
    numeric = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
    ]
    for name in numeric:
        aggregates.append(
            F.sum(F.col(f"`{name}`").cast("decimal(38,10)")).alias(
                f"sum_{name}"
            )
        )
    values = df.agg(*aggregates).first().asDict()
    return {
        "schema": sorted(
            (field.name, field.dataType.simpleString())
            for field in df.schema.fields
        ),
        **{
            key: (
                str(value)
                if value is not None
                and (key == "hash_sum" or key.startswith("sum_"))
                else value
            )
            for key, value in values.items()
        },
    }


def capture_metrics(spark, catalog, schema, run_id):
    return {
        table: _metrics(
            spark.table(_fqn(catalog, schema, table)).filter(
                F.col("RunID") == int(run_id)
            )
        )
        for table in TABLES
    }


def compare_metrics(original, updated):
    mismatches = []
    for table in TABLES:
        for key in sorted(
            set(original.get(table, {})) | set(updated.get(table, {}))
        ):
            left = original.get(table, {}).get(key)
            right = updated.get(table, {}).get(key)
            if left != right:
                mismatches.append(
                    f"{table}.{key}: original={left!r} updated={right!r}"
                )
    return mismatches


def summarize_metrics(metrics):
    return {
        table: int(metrics[table].get("row_count") or 0)
        for table in TABLES
    }


__all__ = [
    "TABLES",
    "capture_metrics",
    "compare_metrics",
    "create_benchmark_snapshot",
    "drop_benchmark_snapshot",
    "reset_before_variant",
    "restore_original_state",
    "summarize_metrics",
]
