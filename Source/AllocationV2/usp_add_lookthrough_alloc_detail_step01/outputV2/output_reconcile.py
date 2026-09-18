"""Mutation-safe snapshots and fingerprints for every table written by the SP."""

from __future__ import annotations

import re
import uuid

import pyspark.sql.functions as F

OUTPUT_TABLES = (
    "M1AdjLookThroughSidePocketAllocationDetail",
    "K1LookThroughBookAllocationDetail",
    "K1LookThroughBookK1AdjustmentAllocationDetail",
    "K1LookThroughOffsetAllocationDetail",
    "K1LookThroughDatedTransferAllocationDetail",
    "K1LOOKTHROUGHSPECIALALLOCATIONDETAIL",
    "M1AdjLookThroughSidePocketResidualAllocationDetail",
    "BoxJKLLookThroughAllocationDetail",
    "K1LookThroughCompleteAllocationDetail",
    "CYAdjustmentLookThroughAllocationDetail",
    "K1AllocationDetail",
)

MEASURE_COLUMNS = ("Amount", "FlowupAmount", "ProrataPercentage")
KEY_COLUMNS = (
    "RunID",
    "ClientID",
    "TaxPeriodID",
    "EntityID",
    "ParentEntityID",
    "PartnerNumber",
    "LineID",
    "TrackingKey",
)
_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _quote(value):
    return f"`{str(value).replace('`', '``')}`"


def _fqn(catalog, schema, table):
    return ".".join(_quote(value) for value in (catalog, schema, table))


def _run_rows(spark, fqn, run_id):
    return spark.table(fqn).filter(F.col("RunID") == int(run_id))


def create_benchmark_snapshot(
    spark, catalog, schema, run_id, execution_id=None
):
    """Snapshot all pre-existing RunID rows so the benchmark is reversible."""
    suffix = _SAFE.sub(
        "_", str(execution_id or uuid.uuid4().hex)
    )[:32]
    backups = {}
    try:
        for index, table in enumerate(OUTPUT_TABLES):
            source = _fqn(catalog, schema, table)
            if not spark.catalog.tableExists(source):
                backups[table] = None
                continue
            backup = _fqn(
                catalog,
                schema,
                f"_tmp_ltdetail01_v2_{int(run_id)}_{index}_{suffix}",
            )
            (
                _run_rows(spark, source, run_id)
                .write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("delta.dataSkippingNumIndexedCols", "0")
                .saveAsTable(backup)
            )
            backups[table] = backup
    except Exception:
        for backup in backups.values():
            if backup:
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {backup}")
                except Exception:
                    pass
        raise
    snapshot = {
        "catalog": catalog,
        "schema": schema,
        "run_id": int(run_id),
        "backups": backups,
    }
    snapshot["baseline"] = capture_outputs(
        spark, catalog, schema, run_id
    )
    return snapshot


def reset_before_variant(spark, snapshot):
    run_id = snapshot["run_id"]
    for table in OUTPUT_TABLES:
        target = _fqn(
            snapshot["catalog"], snapshot["schema"], table
        )
        if spark.catalog.tableExists(target):
            spark.sql(f"DELETE FROM {target} WHERE RunID = {run_id}")


def restore_original_state(spark, snapshot):
    reset_before_variant(spark, snapshot)
    for table, backup in snapshot["backups"].items():
        target = _fqn(
            snapshot["catalog"], snapshot["schema"], table
        )
        if not backup:
            if spark.catalog.tableExists(target):
                spark.sql(f"DROP TABLE {target}")
            continue
        (
            spark.table(backup)
            .write.format("delta")
            .mode("append")
            .saveAsTable(target)
        )
    restored = capture_outputs(
        spark,
        snapshot["catalog"],
        snapshot["schema"],
        snapshot["run_id"],
    )
    mismatches = compare_outputs(snapshot["baseline"], restored)
    if mismatches:
        raise RuntimeError(
            "Benchmark state restoration failed; first mismatch: "
            f"{mismatches[0]}"
        )


def drop_benchmark_snapshot(spark, snapshot):
    for backup in snapshot["backups"].values():
        if backup:
            spark.sql(f"DROP TABLE IF EXISTS {backup}")


def fingerprint_table(spark, catalog, schema, table, run_id):
    fqn = _fqn(catalog, schema, table)
    if not spark.catalog.tableExists(fqn):
        return {"table": table, "exists": False}
    df = _run_rows(spark, fqn, run_id)
    ordered = sorted(df.columns, key=str.lower)
    row_hash = F.xxhash64(
        *[
            F.coalesce(F.col(_quote(name)).cast("string"), F.lit("<NULL>"))
            for name in ordered
        ]
    )
    aggregations = [
        F.count(F.lit(1)).cast("long").alias("rows"),
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
        table: fingerprint_table(
            spark, catalog, schema, table, run_id
        )
        for table in OUTPUT_TABLES
    }


def compare_outputs(original, updated):
    return [
        {
            "table": table,
            "original": original.get(table),
            "updated": updated.get(table),
        }
        for table in OUTPUT_TABLES
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
    "OUTPUT_TABLES",
    "capture_outputs",
    "compare_outputs",
    "create_benchmark_snapshot",
    "drop_benchmark_snapshot",
    "fingerprint_table",
    "reset_before_variant",
    "restore_original_state",
    "summarize_outputs",
]
