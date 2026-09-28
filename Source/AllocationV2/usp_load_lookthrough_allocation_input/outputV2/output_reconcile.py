"""Mutation-safe snapshot, restore, and parity fingerprints for one RunID."""

from __future__ import annotations

import uuid

import pyspark.sql.functions as F

TABLE_SPECS = (
    ("LookThroughAllocationInput", "RunID"),
    ("SchKTaxableIncome", "UpperTierRunID"),
    ("PFICtoK1IncomeAttributePercentages", "RunID"),
    ("AllocationRunErrors", "RunID"),
    ("AllocationRun", "RunID"),
)
OUTPUT_TABLES = tuple(name for name, _ in TABLE_SPECS[:-1])
MEASURE_COLUMNS = ("Amount", "Amount704b", "TaxableIncome", "EffPercentage")
KEY_COLUMNS = (
    "EntityID",
    "ParentEntityID",
    "LineTypeID",
    "LineID",
    "TrackingKey",
)


def _quote(value):
    return f"`{str(value).replace('`', '``')}`"


def _fqn(catalog, schema, table):
    return ".".join(_quote(value) for value in (catalog, schema, table))


def create_benchmark_snapshot(spark, catalog, schema, run_id):
    """Snapshot every table the procedure may mutate for this RunID."""
    snapshot = {
        "catalog": catalog,
        "schema": schema,
        "run_id": int(run_id),
        "tables": {},
    }
    for table, run_column in TABLE_SPECS:
        source = _fqn(catalog, schema, table)
        if not spark.catalog.tableExists(source):
            snapshot["tables"][table] = {
                "exists": False,
                "run_column": run_column,
                "backup": None,
            }
            continue
        backup = (
            f"_benchmark_ltai_{table.lower()[:16]}_"
            f"{int(run_id)}_{uuid.uuid4().hex[:8]}"
        )
        spark.sql(
            f"CREATE TABLE {_fqn(catalog, schema, backup)} USING DELTA AS "
            f"SELECT * FROM {source} "
            f"WHERE {_quote(run_column)} = {int(run_id)}"
        )
        snapshot["tables"][table] = {
            "exists": True,
            "run_column": run_column,
            "backup": backup,
        }
    return snapshot


def _replace_from_snapshot(spark, snapshot, table):
    spec = snapshot["tables"][table]
    if not spec["exists"]:
        return
    target = _fqn(snapshot["catalog"], snapshot["schema"], table)
    backup = _fqn(snapshot["catalog"], snapshot["schema"], spec["backup"])
    predicate = (
        f"{_quote(spec['run_column'])} = {int(snapshot['run_id'])}"
    )
    spark.sql(f"DELETE FROM {target} WHERE {predicate}")
    spark.sql(f"INSERT INTO {target} SELECT * FROM {backup}")


def reset_before_variant(spark, snapshot):
    """Restore control state and purge generated rows before each variant."""
    _replace_from_snapshot(spark, snapshot, "AllocationRun")
    for table in OUTPUT_TABLES:
        spec = snapshot["tables"][table]
        target = _fqn(snapshot["catalog"], snapshot["schema"], table)
        if spark.catalog.tableExists(target):
            spark.sql(
                f"DELETE FROM {target} WHERE {_quote(spec['run_column'])} "
                f"= {int(snapshot['run_id'])}"
            )


def restore_original_state(spark, snapshot):
    for table, _ in TABLE_SPECS:
        spec = snapshot["tables"][table]
        if spec["exists"]:
            _replace_from_snapshot(spark, snapshot, table)
            continue
        target = _fqn(snapshot["catalog"], snapshot["schema"], table)
        if spark.catalog.tableExists(target):
            spark.sql(
                f"DELETE FROM {target} WHERE {_quote(spec['run_column'])} "
                f"= {int(snapshot['run_id'])}"
            )
    print(
        f"[reconcile] restored all affected tables "
        f"for RunID={snapshot['run_id']}"
    )


def drop_benchmark_snapshot(spark, snapshot):
    for spec in snapshot["tables"].values():
        if spec["backup"]:
            spark.sql(
                f"DROP TABLE IF EXISTS "
                f"{_fqn(snapshot['catalog'], snapshot['schema'], spec['backup'])}"
            )


def fingerprint_table(spark, catalog, schema, table, run_column, run_id):
    fqn = _fqn(catalog, schema, table)
    if not spark.catalog.tableExists(fqn):
        return {"table": table, "exists": False}
    df = spark.table(fqn).filter(F.col(run_column) == int(run_id))
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


def capture_metrics(spark, catalog, schema, run_id):
    return {
        table: fingerprint_table(
            spark, catalog, schema, table, run_column, run_id
        )
        for table, run_column in TABLE_SPECS
    }


def compare_metrics(original, updated):
    return [
        {
            "table": table,
            "original": original.get(table),
            "updated": updated.get(table),
        }
        for table, _ in TABLE_SPECS
        if original.get(table) != updated.get(table)
    ]


def summarize_metrics(metrics):
    return {
        "total_rows": sum(
            int(item.get("rows", 0) or 0) for item in metrics.values()
        ),
        "tables_present": sum(
            bool(item.get("exists")) for item in metrics.values()
        ),
    }


__all__ = [
    "OUTPUT_TABLES",
    "TABLE_SPECS",
    "capture_metrics",
    "compare_metrics",
    "create_benchmark_snapshot",
    "drop_benchmark_snapshot",
    "fingerprint_table",
    "reset_before_variant",
    "restore_original_state",
    "summarize_metrics",
]
