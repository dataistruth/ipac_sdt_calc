"""Order-independent output fingerprints for Allocation Input A/B runs."""

from __future__ import annotations

import uuid

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


def create_run_snapshots(spark, catalog, schema, run_id) -> dict:
    """Snapshot all existing RunID-scoped outputs before either variant."""
    snapshots = {}
    try:
        for table in OUTPUT_TABLES:
            source = _quoted_fqn(catalog, schema, table)
            if not spark.catalog.tableExists(source):
                continue
            if "RunID" not in spark.table(source).columns:
                continue
            name = (
                f"_benchmark_alloc_{table.lower().replace('_', '')[:14]}_"
                f"{int(run_id)}_{uuid.uuid4().hex[:8]}"
            )
            snapshot = _quoted_fqn(catalog, schema, name)
            spark.sql(
                f"CREATE TABLE {snapshot} USING DELTA AS "
                f"SELECT * FROM {source} WHERE RunID = {int(run_id)}"
            )
            snapshots[table] = name
    except Exception:
        drop_run_snapshots(spark, catalog, schema, snapshots)
        raise
    return snapshots


def restore_run_snapshots(spark, catalog, schema, run_id, snapshots) -> None:
    """Restore exact pre-benchmark partitions after success or failure."""
    for table in OUTPUT_TABLES:
        target = _quoted_fqn(catalog, schema, table)
        if (
            spark.catalog.tableExists(target)
            and "RunID" in spark.table(target).columns
        ):
            spark.sql(f"DELETE FROM {target} WHERE RunID = {int(run_id)}")
    for table, name in snapshots.items():
        target = _quoted_fqn(catalog, schema, table)
        snapshot = _quoted_fqn(catalog, schema, name)
        spark.sql(f"INSERT INTO {target} SELECT * FROM {snapshot}")


def drop_run_snapshots(spark, catalog, schema, snapshots) -> None:
    for name in snapshots.values():
        spark.sql(
            f"DROP TABLE IF EXISTS {_quoted_fqn(catalog, schema, name)}"
        )


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
    "create_run_snapshots",
    "drop_run_snapshots",
    "fingerprint_table",
    "restore_run_snapshots",
]
