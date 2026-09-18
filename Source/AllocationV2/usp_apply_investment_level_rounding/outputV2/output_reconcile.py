"""Mutation-safe RunID snapshots and output fingerprints for A/B testing."""

from __future__ import annotations

import uuid

import pyspark.sql.functions as F

OUTPUT_TABLES = (
    "K1LookThroughAllocationSummary",
    "UBTILookThroughAllocationSummary",
    "PassiveIncomeAllocationSummary",
    "BOXJKLAllocationSummary",
    "AdjustmentLookThroughAllocationSummary",
    "K1AllocationSummary",
    "UBTIAllocationSummary",
    "AdjustmentAllocationSummary",
)
MUTATED_TABLES = OUTPUT_TABLES + ("LookThroughOffsetUnRoundedLines",)
MEASURE_COLUMNS = ("Amount", "FlowupAmount")
KEY_COLUMNS = (
    "EntityID",
    "PartnerNumber",
    "ShareClass",
    "LineID",
    "LineTypeID",
    "TrackingKey",
    "AdjustmentTypeID",
)


def _quote(value):
    return f"`{str(value).replace('`', '``')}`"


def _fqn(catalog, schema, table):
    return ".".join(_quote(value) for value in (catalog, schema, table))


def create_run_snapshots(spark, catalog, schema, run_id):
    """Snapshot every existing RunID partition changed by either variant."""
    snapshots = {}
    for table in MUTATED_TABLES:
        source = _fqn(catalog, schema, table)
        if not spark.catalog.tableExists(source):
            snapshots[table] = None
            continue
        snapshot_name = (
            f"_benchmark_rounding_{table.lower()[:16]}_"
            f"{int(run_id)}_{uuid.uuid4().hex[:8]}"
        )
        snapshot = _fqn(catalog, schema, snapshot_name)
        spark.sql(
            f"CREATE TABLE {snapshot} USING DELTA AS "
            f"SELECT * FROM {source} WHERE RunID = {int(run_id)}"
        )
        snapshots[table] = snapshot_name
    return snapshots


def restore_run_snapshots(spark, catalog, schema, run_id, snapshots):
    """Restore exact pre-benchmark state for all existing target tables."""
    for table, snapshot_name in snapshots.items():
        target = _fqn(catalog, schema, table)
        if not spark.catalog.tableExists(target):
            continue
        spark.sql(f"DELETE FROM {target} WHERE RunID = {int(run_id)}")
        if snapshot_name:
            spark.sql(
                f"INSERT INTO {target} SELECT * FROM "
                f"{_fqn(catalog, schema, snapshot_name)}"
            )


def drop_run_snapshots(spark, catalog, schema, snapshots):
    for snapshot_name in snapshots.values():
        if snapshot_name:
            spark.sql(
                f"DROP TABLE IF EXISTS "
                f"{_fqn(catalog, schema, snapshot_name)}"
            )


def purge_outputs(spark, catalog, schema, run_id):
    """Delete only this RunID from output tables; leave restored input state."""
    purged = []
    for table in OUTPUT_TABLES:
        target = _fqn(catalog, schema, table)
        if spark.catalog.tableExists(target):
            spark.sql(
                f"DELETE FROM {target} WHERE RunID = {int(run_id)}"
            )
            purged.append(table)
    print(f"[reconcile] purged RunID={run_id} from {purged}")
    return purged


def fingerprint_table(spark, catalog, schema, table, run_id):
    target = _fqn(catalog, schema, table)
    if not spark.catalog.tableExists(target):
        return {"table": table, "exists": False}
    df = spark.table(target).filter(F.col("RunID") == int(run_id))
    ordered = sorted(df.columns)
    row_hash = F.xxhash64(
        *[
            F.coalesce(F.col(_quote(name)).cast("string"), F.lit("<NULL>"))
            for name in ordered
        ]
    )
    aggregations = [
        F.count("*").alias("rows"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("hash_sum"),
        F.min(row_hash).alias("hash_min"),
        F.max(row_hash).alias("hash_max"),
    ]
    aggregations.extend(
        F.sum(F.col(name).cast("decimal(38,12)")).alias(f"sum_{name}")
        for name in MEASURE_COLUMNS
        if name in df.columns
    )
    aggregations.extend(
        F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(
            f"null_{name}"
        )
        for name in KEY_COLUMNS
        if name in df.columns
    )
    values = df.agg(*aggregations).first().asDict()
    normalized = {
        key: (
            value
            if isinstance(value, (int, float))
            else None if value is None else str(value)
        )
        for key, value in values.items()
    }
    return {
        "table": table,
        "exists": True,
        "schema": sorted(
            (field.name, field.dataType.simpleString())
            for field in df.schema.fields
        ),
        **normalized,
    }


def capture_outputs(spark, catalog, schema, run_id):
    return {
        table: fingerprint_table(spark, catalog, schema, table, run_id)
        for table in MUTATED_TABLES
    }


def compare_outputs(original, updated):
    return [
        {
            "table": table,
            "original": original.get(table),
            "updated": updated.get(table),
        }
        for table in MUTATED_TABLES
        if original.get(table) != updated.get(table)
    ]


def summarize_outputs(outputs):
    return {
        "total_rows": sum(
            int(item.get("rows", 0) or 0) for item in outputs.values()
        ),
        "tables_present": sum(
            bool(item.get("exists")) for item in outputs.values()
        ),
    }


# Notebook-facing names describe the full mutation set, not just outputs.
snapshot_mutations = create_run_snapshots
restore_mutations = restore_run_snapshots
drop_snapshots = drop_run_snapshots
purge_run = purge_outputs


__all__ = [
    "MUTATED_TABLES",
    "OUTPUT_TABLES",
    "capture_outputs",
    "compare_outputs",
    "create_run_snapshots",
    "drop_snapshots",
    "drop_run_snapshots",
    "fingerprint_table",
    "purge_outputs",
    "purge_run",
    "restore_mutations",
    "restore_run_snapshots",
    "snapshot_mutations",
    "summarize_outputs",
]
