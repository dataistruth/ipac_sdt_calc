"""Run-scoped reconciliation for the three FEP output tables."""

from __future__ import annotations

import uuid
from typing import Any

import pyspark.sql.functions as F

OUTPUT_TABLES = (
    "FinalEffectivePercentages",
    "FNFinalEffectivePercentages",
    "SM_FinalEffectivePercentages",
)
MEASURE_COLUMNS = ("EffPercentage", "EffAmount")


def _fqn(catalog: str, schema: str, table: str) -> str:
    return ".".join(
        f"`{part.replace('`', '``')}`" for part in (catalog, schema, table)
    )


def purge_run(spark, catalog: str, schema: str, run_id: int) -> list[str]:
    """Delete only the selected RunID from existing output tables."""
    purged = []
    for table in OUTPUT_TABLES:
        fqn = _fqn(catalog, schema, table)
        if spark.catalog.tableExists(fqn):
            spark.sql(f"DELETE FROM {fqn} WHERE RunID = {int(run_id)}")
            purged.append(table)
    print(f"[reconcile] purged RunID={run_id} from {len(purged)} table(s)")
    return purged


def create_run_snapshots(spark, catalog: str, schema: str, run_id: int) -> dict:
    """Snapshot every RunID partition mutated by the benchmark."""
    snapshots = {}
    try:
        for table in OUTPUT_TABLES:
            source = _fqn(catalog, schema, table)
            if not spark.catalog.tableExists(source):
                continue
            name = (
                f"_benchmark_fep_{table.lower().replace('_', '')[:16]}_"
                f"{int(run_id)}_{uuid.uuid4().hex[:8]}"
            )
            snapshot = _fqn(catalog, schema, name)
            spark.sql(
                f"CREATE TABLE {snapshot} USING DELTA AS "
                f"SELECT * FROM {source} WHERE RunID = {int(run_id)}"
            )
            snapshots[table] = name
    except Exception:
        drop_run_snapshots(spark, catalog, schema, snapshots)
        raise
    return snapshots


def restore_run_snapshots(
    spark, catalog: str, schema: str, run_id: int, snapshots: dict
) -> None:
    """Restore the exact pre-benchmark RunID state."""
    purge_run(spark, catalog, schema, run_id)
    for table, name in snapshots.items():
        spark.sql(
            f"INSERT INTO {_fqn(catalog, schema, table)} "
            f"SELECT * FROM {_fqn(catalog, schema, name)}"
        )


def drop_run_snapshots(spark, catalog: str, schema: str, snapshots: dict) -> None:
    for name in snapshots.values():
        spark.sql(f"DROP TABLE IF EXISTS {_fqn(catalog, schema, name)}")


def fingerprint_table(
    spark, catalog: str, schema: str, table: str, run_id: int
) -> dict[str, Any]:
    """Capture schema, count, business sums, and order-independent row hash."""
    fqn = _fqn(catalog, schema, table)
    if not spark.catalog.tableExists(fqn):
        return {"table": table, "exists": False}
    df = spark.table(fqn).filter(F.col("RunID") == int(run_id))
    ordered = sorted(df.columns)
    row_hash = F.xxhash64(
        *[
            F.coalesce(F.col(f"`{name.replace('`', '``')}`").cast("string"), F.lit("<NULL>"))
            for name in ordered
        ]
    )
    aggregations = [
        F.count("*").alias("rows"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("hash_sum"),
        F.min(row_hash).alias("hash_min"),
        F.max(row_hash).alias("hash_max"),
    ]
    present_measures = [name for name in MEASURE_COLUMNS if name in df.columns]
    aggregations.extend(
        F.sum(F.col(name).cast("decimal(38,12)")).alias(f"sum_{name}")
        for name in present_measures
    )
    row = df.agg(*aggregations).first().asDict()
    return {
        "table": table,
        "exists": True,
        "schema": sorted(
            (field.name, field.dataType.simpleString())
            for field in df.schema.fields
        ),
        **{
            key: value if isinstance(value, (int, float)) else (
                None if value is None else str(value)
            )
            for key, value in row.items()
        },
    }


def capture_outputs(spark, catalog: str, schema: str, run_id: int) -> dict:
    return {
        table: fingerprint_table(spark, catalog, schema, table, run_id)
        for table in OUTPUT_TABLES
    }


def compare_outputs(original: dict, updated: dict) -> list[dict]:
    return [
        {
            "table": table,
            "original": original.get(table),
            "updated": updated.get(table),
        }
        for table in OUTPUT_TABLES
        if original.get(table) != updated.get(table)
    ]


def summarize_outputs(outputs: dict) -> dict[str, int]:
    return {
        "total_rows": sum(
            int(item.get("rows", 0) or 0) for item in outputs.values()
        ),
        "tables_present": sum(
            bool(item.get("exists")) for item in outputs.values()
        ),
    }


purge_output_partitions_for_run = purge_run
capture_output_metrics = capture_outputs
compare_variants = compare_outputs
summarize_metrics = summarize_outputs

__all__ = [
    "OUTPUT_TABLES",
    "capture_output_metrics",
    "capture_outputs",
    "compare_outputs",
    "compare_variants",
    "create_run_snapshots",
    "drop_run_snapshots",
    "fingerprint_table",
    "purge_output_partitions_for_run",
    "purge_run",
    "restore_run_snapshots",
    "summarize_metrics",
    "summarize_outputs",
]
