"""Full-table RunID snapshot, reconciliation, and restoration."""

from __future__ import annotations

import re
import uuid

import pyspark.sql.functions as F

OUTPUT_TABLES = (
    "AdjustmentAllocationSummary",
    "AtRiskAllocationSummary",
    "BoxJKLAllocationSummary",
    "CustomFootnoteAllocationSummary",
    "Form199AAllocationSummary",
    "Form200616AllocationSummary",
    "Form8865AllocationSummary",
    "Form8886AllocationSummary",
    "Form926AllocationSummary",
    "GAAPToTaxAllocation",
    "K1AllocationSummary",
    "Line18AAllocationSummary",
    "M1AdjAllocationSummary",
    "PFICFootnoteAllocationSummary",
    "PFICFootnoteAllocationText",
    "PassiveIncomeAllocationSummary",
    "UBTIAllocationSummary",
)
_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _fqn(catalog, schema, table):
    return ".".join(
        f"`{part.replace('`', '``')}`"
        for part in (catalog, schema, table)
    )


def _run_rows(spark, table, run_id):
    return spark.table(table).filter(F.col("RunID") == int(run_id))


def _write_snapshot(df, table):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.dataSkippingNumIndexedCols", "0")
        .saveAsTable(table)
    )


def create_benchmark_snapshot(
    spark, catalog, schema, run_id, execution_id=None
):
    suffix = _SAFE.sub(
        "_", str(execution_id or uuid.uuid4().hex)
    )[:48]
    backups = {}
    try:
        for table in OUTPUT_TABLES:
            target = _fqn(catalog, schema, table)
            backup = _fqn(
                catalog,
                schema,
                f"_tmp_add_summary_v2_{int(run_id)}_{table}_{suffix}",
            )
            _write_snapshot(_run_rows(spark, target, run_id), backup)
            backups[table] = backup
    except Exception:
        for backup in backups.values():
            try:
                spark.sql(f"DROP TABLE IF EXISTS {backup}")
            except Exception:
                pass
        raise
    return {
        "catalog": catalog,
        "schema": schema,
        "run_id": int(run_id),
        "backups": backups,
        "baseline": capture_metrics(
            spark, catalog, schema, run_id
        ),
    }


def reset_before_variant(spark, snapshot):
    run_id = snapshot["run_id"]
    for table in OUTPUT_TABLES:
        target = _fqn(
            snapshot["catalog"], snapshot["schema"], table
        )
        spark.sql(f"DELETE FROM {target} WHERE RunID = {run_id}")


def restore_original_state(spark, snapshot):
    reset_before_variant(spark, snapshot)
    for table, backup in snapshot["backups"].items():
        target = _fqn(
            snapshot["catalog"], snapshot["schema"], table
        )
        (
            spark.table(backup)
            .write.format("delta")
            .mode("append")
            .saveAsTable(target)
        )
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
    for backup in snapshot["backups"].values():
        spark.sql(f"DROP TABLE IF EXISTS {backup}")


def _metrics(df):
    columns = sorted(df.columns, key=str.lower)
    row_hash = F.xxhash64(
        *[
            F.coalesce(
                F.col(f"`{name}`").cast("string"), F.lit("<NULL>")
            )
            for name in columns
        ]
    )
    aggregates = [
        F.count(F.lit(1)).cast("long").alias("row_count"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("hash_sum"),
        F.min(row_hash).alias("hash_min"),
        F.max(row_hash).alias("hash_max"),
    ]
    numeric_types = {
        "byte", "short", "integer", "long", "float", "double", "decimal"
    }
    for field in df.schema:
        if field.dataType.typeName() in numeric_types:
            aggregates.append(
                F.sum(F.col(f"`{field.name}`").cast("decimal(38,10)"))
                .alias(f"sum__{field.name}")
            )
    for name in (
        "RunID",
        "ClientID",
        "TaxPeriodID",
        "EntityID",
        "PartnerNumber",
        "LineID",
    ):
        if name in df.columns:
            aggregates.append(
                F.sum(F.col(f"`{name}`").isNull().cast("long"))
                .alias(f"nulls__{name}")
            )
    values = df.agg(*aggregates).first().asDict()
    return {
        "schema": sorted(
            (
                field.name,
                field.dataType.simpleString(),
                field.nullable,
            )
            for field in df.schema
        ),
        **{
            key: (
                str(value)
                if value is not None
                and (key == "hash_sum" or key.startswith("sum__"))
                else value
            )
            for key, value in values.items()
        },
    }


def capture_metrics(spark, catalog, schema, run_id):
    return {
        table: _metrics(
            _run_rows(
                spark, _fqn(catalog, schema, table), run_id
            )
        )
        for table in OUTPUT_TABLES
    }


def compare_metrics(original, updated):
    mismatches = []
    for table in OUTPUT_TABLES:
        left_metrics = original.get(table, {})
        right_metrics = updated.get(table, {})
        for key in sorted(set(left_metrics) | set(right_metrics)):
            left = left_metrics.get(key)
            right = right_metrics.get(key)
            if left != right:
                mismatches.append(
                    f"{table}.{key}: original={left!r} updated={right!r}"
                )
    return mismatches


def metric_rows(pass_number, original, updated):
    mismatches = compare_metrics(original, updated)
    return [
        {
            "pass": pass_number,
            "table": table,
            "original_rows": int(
                original[table].get("row_count") or 0
            ),
            "updated_rows": int(
                updated[table].get("row_count") or 0
            ),
            "matches": not any(
                value.startswith(table + ".") for value in mismatches
            ),
        }
        for table in OUTPUT_TABLES
    ]


__all__ = [
    "OUTPUT_TABLES",
    "capture_metrics",
    "compare_metrics",
    "create_benchmark_snapshot",
    "drop_benchmark_snapshot",
    "metric_rows",
    "reset_before_variant",
    "restore_original_state",
]
