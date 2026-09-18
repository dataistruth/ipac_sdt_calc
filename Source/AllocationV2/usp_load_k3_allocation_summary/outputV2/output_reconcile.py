"""Mutation-safe K3AllocationSummary snapshot and parity metrics."""

import re
import uuid

import pyspark.sql.functions as F

OUTPUT_TABLE = "K3AllocationSummary"
_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _fqn(catalog, schema, table):
    return ".".join(
        f"`{str(part).replace('`', '``')}`"
        for part in (catalog, schema, table)
    )


def create_snapshot(spark, catalog, schema, run_id, execution_id=None):
    table = _fqn(catalog, schema, OUTPUT_TABLE)
    exists = spark.catalog.tableExists(table)
    suffix = _SAFE.sub("_", str(execution_id or uuid.uuid4().hex))[:48]
    backup = _fqn(
        catalog, schema, f"_tmp_k3_summary_v2_{int(run_id)}_{suffix}"
    )
    if exists:
        (
            spark.table(table)
            .filter(F.col("RunID") == int(run_id))
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .saveAsTable(backup)
        )
    return {
        "table": table,
        "backup": backup,
        "table_existed": exists,
        "run_id": int(run_id),
    }


def purge_before_variant(spark, snapshot):
    if spark.catalog.tableExists(snapshot["table"]):
        spark.sql(
            f"DELETE FROM {snapshot['table']} "
            f"WHERE RunID = {snapshot['run_id']}"
        )


def restore_snapshot(spark, snapshot):
    if not snapshot["table_existed"]:
        if spark.catalog.tableExists(snapshot["table"]):
            spark.sql(f"DROP TABLE {snapshot['table']}")
        return
    purge_before_variant(spark, snapshot)
    (
        spark.table(snapshot["backup"])
        .write.format("delta")
        .mode("append")
        .saveAsTable(snapshot["table"])
    )


def drop_snapshot(spark, snapshot):
    if snapshot["table_existed"]:
        spark.sql(f"DROP TABLE IF EXISTS {snapshot['backup']}")


def fingerprint_df(df):
    columns = sorted(df.columns, key=str.lower)
    row_hash = F.xxhash64(
        *[
            F.coalesce(F.col(f"`{name}`").cast("string"), F.lit("<NULL>"))
            for name in columns
        ]
    )
    aggregates = [
        F.count(F.lit(1)).cast("long").alias("rows"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("hash_sum"),
        F.min(row_hash).alias("hash_min"),
        F.max(row_hash).alias("hash_max"),
    ]
    if "Amount" in df.columns:
        aggregates.append(
            F.sum(F.col("Amount").cast("decimal(38,10)")).alias("sum_Amount")
        )
    row = df.agg(*aggregates).first().asDict()
    return {
        "schema": sorted(
            (field.name, field.dataType.simpleString())
            for field in df.schema.fields
        ),
        **{
            key: (
                str(value)
                if value is not None
                and (key == "hash_sum" or key.startswith("sum_"))
                else value
            )
            for key, value in row.items()
        },
    }


def capture_table(spark, snapshot):
    return fingerprint_df(
        spark.table(snapshot["table"]).filter(
            F.col("RunID") == snapshot["run_id"]
        )
    )


def compare_metrics(original, updated):
    return [
        f"{key}: original={original.get(key)!r} updated={updated.get(key)!r}"
        for key in sorted(set(original) | set(updated))
        if original.get(key) != updated.get(key)
    ]


__all__ = [
    "OUTPUT_TABLE", "capture_table", "compare_metrics", "create_snapshot",
    "drop_snapshot", "fingerprint_df", "purge_before_variant",
    "restore_snapshot",
]
