"""Run-scoped output cleanup and A/B fingerprint reconciliation."""

from __future__ import annotations

import json

import pyspark.sql.functions as F
from pyspark.sql.types import NumericType


OUTPUT_TABLES = (
    "AllocationInput",
    "PFICFootnoteFlowup",
    "PFICFootnoteFlowupWithTrackingKey",
    "Form926Flowup",
    "Form199AFlowup",
    "Form8865Flowup",
    "Form8886Flowup",
    "AtRiskFlowup",
    "CustomFootnoteFlowup",
    "Form200616Flowup",
    "PFICUpdateAlert",
    "PFICAlertDetails",
)


def _quoted_fqn(catalog: str, schema: str, table: str) -> str:
    return ".".join(
        f"`{str(part).replace('`', '``')}`"
        for part in (catalog, schema, table)
    )


def _run_frame(spark, catalog: str, schema: str, table: str, run_id: int):
    fqn = _quoted_fqn(catalog, schema, table)
    try:
        df = spark.table(fqn)
        columns = df.columns
    except Exception:
        return None
    if "RunID" not in columns:
        return None
    return df.filter(F.col("RunID") == int(run_id))


def purge_output_partitions_for_run(
    spark, catalog: str, schema: str, run_id: int
) -> None:
    """Delete only the benchmark RunID from known output tables."""
    for table in OUTPUT_TABLES:
        frame = _run_frame(spark, catalog, schema, table, run_id)
        if frame is None:
            continue
        fqn = _quoted_fqn(catalog, schema, table)
        spark.sql(f"DELETE FROM {fqn} WHERE RunID = {int(run_id)}")
        print(f"[purge] {table} RunID={run_id}")


def _metric_for_frame(table: str, df) -> dict:
    ordered = sorted(df.columns)
    row_hash = F.xxhash64(
        *[
            F.coalesce(F.col(name).cast("string"), F.lit("<NULL>"))
            for name in ordered
        ]
    )
    numeric = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
        and field.name.lower() in {
            "amount",
            "amount704b",
            "flowupamount",
            "percentage",
        }
    ]
    aggregations = [
        F.count("*").alias("rows"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("hash_sum"),
        F.min(row_hash).alias("hash_min"),
        F.max(row_hash).alias("hash_max"),
    ]
    aggregations.extend(
        F.sum(F.col(name).cast("decimal(38,6)")).alias(f"sum__{name}")
        for name in numeric
    )
    row = df.agg(*aggregations).first().asDict()
    return {
        "table": table,
        "schema": json.dumps(
            [(field.name, field.dataType.simpleString()) for field in df.schema],
            separators=(",", ":"),
        ),
        **{key: str(value) if value is not None else None for key, value in row.items()},
    }


def capture_output_metrics(
    spark, catalog: str, schema: str, run_id: int
) -> dict[str, dict]:
    metrics = {}
    for table in OUTPUT_TABLES:
        frame = _run_frame(spark, catalog, schema, table, run_id)
        if frame is not None:
            metrics[table] = _metric_for_frame(table, frame)
    return metrics


def compare_output_metrics(
    original: dict[str, dict], updated: dict[str, dict]
) -> list[dict]:
    rows = []
    for table in sorted(set(original) | set(updated)):
        left = original.get(table)
        right = updated.get(table)
        matched = left is not None and right is not None and left == right
        rows.append(
            {
                "table": table,
                "matched": matched,
                "original_rows": left.get("rows") if left else None,
                "updated_rows": right.get("rows") if right else None,
                "reason": (
                    ""
                    if matched
                    else "missing table metrics"
                    if left is None or right is None
                    else "schema, fingerprint, or business aggregate mismatch"
                ),
            }
        )
    return rows
