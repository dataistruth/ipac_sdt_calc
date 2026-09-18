"""Mutation-safe reconciliation for both RunID-scoped tables."""

from __future__ import annotations

import uuid

import pyspark.sql.functions as F

OUTPUT_TABLES = ("SM_LookThroughAllocationOutput",)
MUTATED_TABLES = (
    "SM_LookThroughAllocationOutput",
    "SM_LookThroughAllocationInput",
)
MEASURE_COLUMNS = ("Amount", "Amount704b")
KEY_COLUMNS = (
    "EntityID",
    "PartnerNumber",
    "LineTypeID",
    "StateID",
    "StateLineID",
    "TrackingKey",
)


def _quote(value):
    return f"`{str(value).replace('`', '``')}`"


def _fqn(catalog, schema, table):
    return ".".join(_quote(value) for value in (catalog, schema, table))


def purge_run(spark, catalog, schema, run_id):
    purged = []
    for table in OUTPUT_TABLES:
        fqn = _fqn(catalog, schema, table)
        if spark.catalog.tableExists(fqn):
            spark.sql(f"DELETE FROM {fqn} WHERE RunID = {int(run_id)}")
            purged.append(table)
    print(f"[reconcile] purged RunID={run_id} from {purged}")
    return purged


def create_run_snapshot(spark, catalog, schema, table, run_id):
    source = _fqn(catalog, schema, table)
    safe_table = table.lower().replace("_", "")[:20]
    name = (
        f"_benchmark_{safe_table}_{int(run_id)}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    snapshot = _fqn(catalog, schema, name)
    spark.sql(
        f"CREATE TABLE {snapshot} USING DELTA AS "
        f"SELECT * FROM {source} WHERE RunID = {int(run_id)}"
    )
    return name


def restore_run_snapshot(
    spark, catalog, schema, table, run_id, snapshot_table
):
    target = _fqn(catalog, schema, table)
    snapshot = _fqn(catalog, schema, snapshot_table)
    spark.sql(f"DELETE FROM {target} WHERE RunID = {int(run_id)}")
    spark.sql(f"INSERT INTO {target} SELECT * FROM {snapshot}")
    print(f"[reconcile] restored {table} RunID={run_id}")


def drop_run_snapshot(spark, catalog, schema, snapshot_table):
    spark.sql(
        f"DROP TABLE IF EXISTS {_fqn(catalog, schema, snapshot_table)}"
    )


def fingerprint_table(spark, catalog, schema, table, run_id):
    fqn = _fqn(catalog, schema, table)
    if not spark.catalog.tableExists(fqn):
        return {"table": table, "exists": False}
    df = spark.table(fqn).filter(F.col("RunID") == int(run_id))
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
    return {
        "table": table,
        "exists": True,
        "schema": sorted(
            (field.name, field.dataType.simpleString())
            for field in df.schema.fields
        ),
        **{
            key: (
                value
                if isinstance(value, (int, float))
                else None if value is None else str(value)
            )
            for key, value in values.items()
        },
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


__all__ = [
    "MUTATED_TABLES",
    "OUTPUT_TABLES",
    "capture_outputs",
    "compare_outputs",
    "create_run_snapshot",
    "drop_run_snapshot",
    "fingerprint_table",
    "purge_run",
    "restore_run_snapshot",
    "summarize_outputs",
]
