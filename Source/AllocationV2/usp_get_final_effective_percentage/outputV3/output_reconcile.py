"""Exact run-scoped fingerprints for the three FEP output tables."""

from __future__ import annotations

import uuid

import pyspark.sql.functions as F

OUTPUT_TABLES = (
    "FinalEffectivePercentages",
    "FNFinalEffectivePercentages",
    "SM_FinalEffectivePercentages",
)
MEASURE_COLUMNS = ("EffPercentage", "EffAmount")


def _fqn(catalog, schema, table):
    return ".".join(
        f"`{str(part).replace('`', '``')}`" for part in (catalog, schema, table)
    )


def purge_run(spark, catalog, schema, run_id):
    for table in OUTPUT_TABLES:
        name = _fqn(catalog, schema, table)
        if spark.catalog.tableExists(name):
            spark.sql(f"DELETE FROM {name} WHERE RunID = {int(run_id)}")


def create_run_snapshots(spark, catalog, schema, run_id):
    snapshots = {}
    try:
        for table in OUTPUT_TABLES:
            source = _fqn(catalog, schema, table)
            if not spark.catalog.tableExists(source):
                continue
            snapshot = (
                f"_benchmark_fep_v3_{table.lower().replace('_', '')[:12]}_"
                f"{int(run_id)}_{uuid.uuid4().hex[:8]}"
            )
            spark.sql(
                f"CREATE TABLE {_fqn(catalog, schema, snapshot)} USING DELTA AS "
                f"SELECT * FROM {source} WHERE RunID = {int(run_id)}"
            )
            snapshots[table] = snapshot
    except Exception:
        drop_run_snapshots(spark, catalog, schema, snapshots)
        raise
    return snapshots


def restore_run_snapshots(spark, catalog, schema, run_id, snapshots):
    purge_run(spark, catalog, schema, run_id)
    for table, snapshot in snapshots.items():
        spark.sql(
            f"INSERT INTO {_fqn(catalog, schema, table)} "
            f"SELECT * FROM {_fqn(catalog, schema, snapshot)}"
        )


def drop_run_snapshots(spark, catalog, schema, snapshots):
    for snapshot in snapshots.values():
        spark.sql(f"DROP TABLE IF EXISTS {_fqn(catalog, schema, snapshot)}")


def fingerprint_table(spark, catalog, schema, table, run_id):
    name = _fqn(catalog, schema, table)
    if not spark.catalog.tableExists(name):
        return {"table": table, "exists": False}
    df = spark.table(name).filter(F.col("RunID") == int(run_id))
    columns = sorted(df.columns)
    row_hash = F.xxhash64(
        *[
            F.coalesce(
                F.col(f"`{column.replace('`', '``')}`").cast("string"),
                F.lit("<NULL>"),
            )
            for column in columns
        ]
    )
    aggregates = [
        F.count("*").alias("rows"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("hash_sum"),
        F.min(row_hash).alias("hash_min"),
        F.max(row_hash).alias("hash_max"),
    ]
    aggregates.extend(
        F.sum(F.col(column).cast("decimal(38,12)")).alias(f"sum_{column}")
        for column in MEASURE_COLUMNS
        if column in df.columns
    )
    values = df.agg(*aggregates).first().asDict()
    return {
        "table": table,
        "exists": True,
        "schema": sorted(
            (field.name, field.dataType.simpleString())
            for field in df.schema.fields
        ),
        **{
            key: value
            if isinstance(value, (int, float))
            else (None if value is None else str(value))
            for key, value in values.items()
        },
    }


def capture_outputs(spark, catalog, schema, run_id):
    return {
        table: fingerprint_table(spark, catalog, schema, table, run_id)
        for table in OUTPUT_TABLES
    }


def compare_outputs(original, candidate):
    return [
        {
            "table": table,
            "production": original.get(table),
            "outputV3": candidate.get(table),
        }
        for table in OUTPUT_TABLES
        if original.get(table) != candidate.get(table)
    ]


def summarize_outputs(outputs):
    return {
        "total_rows": sum(
            int(value.get("rows", 0) or 0) for value in outputs.values()
        ),
        "tables_present": sum(
            bool(value.get("exists")) for value in outputs.values()
        ),
    }


__all__ = [
    "OUTPUT_TABLES",
    "capture_outputs",
    "compare_outputs",
    "create_run_snapshots",
    "drop_run_snapshots",
    "fingerprint_table",
    "purge_run",
    "restore_run_snapshots",
    "summarize_outputs",
]
