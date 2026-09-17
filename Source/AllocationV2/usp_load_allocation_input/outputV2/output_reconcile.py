"""Order-independent output fingerprints for Allocation Input A/B runs."""

from __future__ import annotations

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
)


def _quoted_fqn(catalog: str, schema: str, table: str) -> str:
    return ".".join(
        f"`{part.replace('`', '``')}`"
        for part in (catalog, schema, table)
    )


def fingerprint_table(
    spark,
    catalog: str,
    schema: str,
    table: str,
    run_id: int,
) -> dict:
    """Return schema, row-hash and rounded numeric aggregates for one run."""
    fqn = _quoted_fqn(catalog, schema, table)
    try:
        df = spark.table(fqn)
    except Exception as exc:
        return {"table": table, "error": f"{type(exc).__name__}: {exc}"}

    if "RunID" in df.columns:
        df = df.filter(F.col("RunID") == run_id)
    ordered = sorted(df.columns)
    row_hash = F.xxhash64(
        *[
            F.coalesce(F.col(name).cast("string"), F.lit("<NULL>"))
            for name in ordered
        ]
    )
    hash_row = (
        df.select(row_hash.alias("_h"))
        .agg(
            F.count("*").alias("rows"),
            F.sum(F.col("_h").cast("decimal(38,0)")).alias("hash_sum"),
            F.min("_h").alias("hash_min"),
            F.max("_h").alias("hash_max"),
        )
        .first()
    )
    numeric = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
    ]
    sums = {}
    if numeric:
        sum_row = df.agg(
            *[
                F.round(
                    F.sum(F.col(name).cast("decimal(38,8)")), 8
                ).alias(name)
                for name in numeric
            ]
        ).first()
        sums = {
            name: None if sum_row[name] is None else str(sum_row[name])
            for name in numeric
        }
    return {
        "table": table,
        "schema": sorted(
            (field.name, field.dataType.simpleString())
            for field in df.schema.fields
        ),
        "rows": hash_row["rows"],
        "hash_sum": (
            None if hash_row["hash_sum"] is None else str(hash_row["hash_sum"])
        ),
        "hash_min": hash_row["hash_min"],
        "hash_max": hash_row["hash_max"],
        "numeric_sums": sums,
    }


def capture_outputs(spark, catalog, schema, run_id) -> dict:
    return {
        table: fingerprint_table(spark, catalog, schema, table, run_id)
        for table in OUTPUT_TABLES
    }


def compare_outputs(production: dict, updated: dict) -> list[dict]:
    mismatches = []
    for table in OUTPUT_TABLES:
        before = production.get(table)
        after = updated.get(table)
        if before != after:
            mismatches.append(
                {"table": table, "production": before, "updated": after}
            )
    return mismatches


__all__ = [
    "OUTPUT_TABLES",
    "capture_outputs",
    "compare_outputs",
    "fingerprint_table",
]
