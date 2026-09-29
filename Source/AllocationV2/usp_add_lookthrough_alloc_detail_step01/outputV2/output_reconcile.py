"""Detail-table hashes; restore LookThroughAllocationOutput input."""

from __future__ import annotations

import uuid

import pyspark.sql.functions as F

TABLE_SPECS = (
    ("M1AdjLookThroughSidePocketAllocationDetail", "RunID"),
    ("K1LookThroughBookAllocationDetail", "RunID"),
    ("K1LookThroughBookK1AdjustmentAllocationDetail", "RunID"),
    ("K1LookThroughOffsetAllocationDetail", "RunID"),
    ("K1LookThroughDatedTransferAllocationDetail", "RunID"),
    ("K1LOOKTHROUGHSPECIALALLOCATIONDETAIL", "RunID"),
    ("M1AdjLookThroughSidePocketResidualAllocationDetail", "RunID"),
    ("BoxJKLLookThroughAllocationDetail", "RunID"),
    ("K1LookThroughCompleteAllocationDetail", "RunID"),
    ("CYAdjustmentLookThroughAllocationDetail", "RunID"),
    ("K1AllocationDetail", "RunID"),
)
INPUT_RESTORE = frozenset({"LookThroughAllocationOutput"})
ALL_SPECS = TABLE_SPECS + (("LookThroughAllocationOutput", "RunID"),)
SNAPSHOT_PREFIX = "ltd01"


MEASURE_COLUMNS = ("Amount", "Amount704b", "FlowupAmount")
KEY_COLUMNS = (
    "EntityID",
    "ParentEntityID",
    "LineTypeID",
    "LineID",
    "TrackingKey",
    "PartnerNumber",
)


def _quote(value):
    return f"`{str(value).replace('`', '``')}`"


def _fqn(catalog, schema, table):
    return ".".join(_quote(value) for value in (catalog, schema, table))


def _columns(spark, fqn):
    try:
        return list(spark.table(fqn).columns)
    except Exception:
        return []


def create_benchmark_snapshot(spark, catalog, schema, run_id):
    snapshot = {
        "catalog": catalog,
        "schema": schema,
        "run_id": int(run_id),
        "tables": {},
    }
    for table, run_column in ALL_SPECS:
        source = _fqn(catalog, schema, table)
        if not spark.catalog.tableExists(source):
            snapshot["tables"][table] = {
                "exists": False,
                "run_column": run_column,
                "backup": None,
                "kind": "input" if table in INPUT_RESTORE else "output",
            }
            continue
        columns = _columns(spark, source)
        if run_column not in columns:
            snapshot["tables"][table] = {
                "exists": True,
                "run_column": run_column,
                "backup": None,
                "kind": "input" if table in INPUT_RESTORE else "output",
            }
            print(
                f"[reconcile] skip snapshot {table}: no {run_column}; "
                f"columns={columns[:12]}"
            )
            continue
        backup = (
            f"_benchmark_{SNAPSHOT_PREFIX}_{table.lower()[:16]}_"
            f"{int(run_id)}_{uuid.uuid4().hex[:8]}"
        )
        spark.sql(
            f"CREATE TABLE {_fqn(catalog, schema, backup)} USING DELTA AS "
            f"SELECT * FROM {source} "
            f"WHERE {_quote(run_column)} = {int(run_id)}"
        )
        backup_fqn = _fqn(catalog, schema, backup)
        backup_rows = spark.table(backup_fqn).count()
        print(
            f"[reconcile] snapshot {table} rows={backup_rows} "
            f"backup={backup}"
        )
        snapshot["tables"][table] = {
            "exists": True,
            "run_column": run_column,
            "backup": backup,
            "kind": "input" if table in INPUT_RESTORE else "output",
        }
    return snapshot


def _refresh_table(spark, fqn):
    try:
        spark.catalog.refreshTable(fqn)
    except Exception:
        spark.sql(f"REFRESH TABLE {fqn}")


def _replace_from_snapshot(spark, snapshot, table):
    spec = snapshot["tables"][table]
    if not spec["exists"] or not spec.get("backup"):
        return
    target = _fqn(snapshot["catalog"], snapshot["schema"], table)
    backup = _fqn(snapshot["catalog"], snapshot["schema"], spec["backup"])
    run_id = int(snapshot["run_id"])
    run_column = spec["run_column"]
    (
        spark.table(backup)
        .writeTo(target)
        .overwrite(F.col(run_column) == run_id)
    )
    _refresh_table(spark, target)
    restored = (
        spark.table(target)
        .filter(F.col(run_column) == run_id)
        .count()
    )
    print(f"[reconcile] restored {table} rows={restored}")


def reset_before_variant(spark, snapshot):
    """Restore snapshots with overwrite+refresh. Never DELETE then INSERT."""
    for table, _ in ALL_SPECS:
        _replace_from_snapshot(spark, snapshot, table)


def restore_original_state(spark, snapshot):
    for table, _ in ALL_SPECS:
        spec = snapshot["tables"][table]
        if spec.get("backup"):
            _replace_from_snapshot(spark, snapshot, table)
            continue
        if not spec["exists"]:
            continue
        if spec.get("kind") == "input":
            continue
        target = _fqn(snapshot["catalog"], snapshot["schema"], table)
        columns = _columns(spark, target)
        run_column = spec["run_column"]
        if run_column not in columns:
            continue
        spark.sql(
            f"DELETE FROM {target} WHERE {_quote(run_column)} "
            f"= {int(snapshot['run_id'])}"
        )
    print(
        f"[reconcile] restored all affected tables "
        f"for RunID={snapshot['run_id']}"
    )


def drop_benchmark_snapshot(spark, snapshot):
    for spec in snapshot["tables"].values():
        if spec.get("backup"):
            spark.sql(
                f"DROP TABLE IF EXISTS "
                f"{_fqn(snapshot['catalog'], snapshot['schema'], spec['backup'])}"
            )


def fingerprint_table(spark, catalog, schema, table, run_column, run_id):
    fqn = _fqn(catalog, schema, table)
    columns = _columns(spark, fqn)
    if not columns:
        return {"table": table, "exists": False}
    if run_column not in columns:
        return {"table": table, "exists": True, "skipped": True}
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
