"""Reversible state management for the two tables mutated by this SP."""

from __future__ import annotations

import re
import uuid

import pyspark.sql.functions as F

_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _fqn(catalog, schema, table):
    return ".".join(
        f"`{part.replace('`', '``')}`"
        for part in (catalog, schema, table)
    )


def _footnote_rows(df, run_id):
    return df.filter(
        (F.col("RunID") == int(run_id))
        & (
            F.col("AllocationType").like("Footnote%")
            | (F.col("AllocationType") == "704c Footnote")
        )
    )


def _write_snapshot(df, table):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.dataSkippingNumIndexedCols", "0")
        .saveAsTable(table)
    )


def create_benchmark_snapshot(
    spark, catalog, schema, run_id, execution_id=None
):
    suffix = _SAFE.sub(
        "_", str(execution_id or uuid.uuid4().hex)
    )[:48]
    input_table = _fqn(catalog, schema, "AllocationInput")
    output_table = _fqn(catalog, schema, "AllocationOutput")
    input_backup = _fqn(
        catalog,
        schema,
        f"_tmp_footnote_v2_input_{int(run_id)}_{suffix}",
    )
    output_backup = _fqn(
        catalog,
        schema,
        f"_tmp_footnote_v2_output_{int(run_id)}_{suffix}",
    )
    try:
        _write_snapshot(
            spark.table(input_table).filter(
                F.col("RunID") == int(run_id)
            ),
            input_backup,
        )
        _write_snapshot(
            _footnote_rows(spark.table(output_table), run_id),
            output_backup,
        )
    except Exception:
        for backup in (input_backup, output_backup):
            try:
                spark.sql(f"DROP TABLE IF EXISTS {backup}")
            except Exception:
                pass
        raise
    snapshot = {
        "catalog": catalog,
        "schema": schema,
        "run_id": int(run_id),
        "input_table": input_table,
        "output_table": output_table,
        "input_backup": input_backup,
        "output_backup": output_backup,
    }
    snapshot["baseline"] = capture_metrics(
        spark, catalog, schema, run_id
    )
    return snapshot


def _restore_input(spark, snapshot):
    run_id = snapshot["run_id"]
    spark.sql(
        f"DELETE FROM {snapshot['input_table']} WHERE RunID = {run_id}"
    )
    (
        spark.table(snapshot["input_backup"])
        .write.format("delta")
        .mode("append")
        .saveAsTable(snapshot["input_table"])
    )


def _purge_output(spark, snapshot):
    run_id = snapshot["run_id"]
    spark.sql(
        f"DELETE FROM {snapshot['output_table']} WHERE RunID = {run_id} "
        "AND (AllocationType LIKE 'Footnote%' "
        "OR AllocationType = '704c Footnote')"
    )


def reset_before_variant(spark, snapshot):
    _restore_input(spark, snapshot)
    _purge_output(spark, snapshot)


def restore_original_state(spark, snapshot):
    _restore_input(spark, snapshot)
    _purge_output(spark, snapshot)
    (
        spark.table(snapshot["output_backup"])
        .write.format("delta")
        .mode("append")
        .saveAsTable(snapshot["output_table"])
    )
    restored = capture_metrics(
        spark,
        snapshot["catalog"],
        snapshot["schema"],
        snapshot["run_id"],
    )
    mismatches = compare_metrics(snapshot["baseline"], restored)
    if mismatches:
        raise RuntimeError(
            "State restoration failed: " + "; ".join(mismatches[:10])
        )


def drop_benchmark_snapshot(spark, snapshot):
    for key in ("input_backup", "output_backup"):
        spark.sql(f"DROP TABLE IF EXISTS {snapshot[key]}")


def _metrics(df, amount_columns):
    columns = sorted(df.columns, key=str.lower)
    row_hash = F.xxhash64(
        *[
            F.coalesce(F.col(f"`{name}`").cast("string"), F.lit("<NULL>"))
            for name in columns
        ]
    )
    aggregates = [
        F.count(F.lit(1)).cast("long").alias("row_count"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("hash_sum"),
        F.min(row_hash).alias("hash_min"),
        F.max(row_hash).alias("hash_max"),
    ]
    for name in amount_columns:
        if name in df.columns:
            aggregates.append(
                F.sum(F.col(name).cast("decimal(38,10)")).alias(
                    f"sum_{name}"
                )
            )
    values = df.agg(*aggregates).first().asDict()
    return {
        "schema": sorted(
            (
                (field.name, field.dataType.simpleString())
                for field in df.schema
            ),
            key=lambda item: item[0].lower(),
        ),
        **{
            key: (
                str(value)
                if value is not None
                and (key == "hash_sum" or key.startswith("sum_"))
                else value
            )
            for key, value in values.items()
        },
    }


def capture_metrics(spark, catalog, schema, run_id):
    return {
        "AllocationInput": _metrics(
            spark.table(_fqn(catalog, schema, "AllocationInput")).filter(
                F.col("RunID") == int(run_id)
            ),
            ("Amount", "Amount704b"),
        ),
        "AllocationOutput": _metrics(
            _footnote_rows(
                spark.table(_fqn(catalog, schema, "AllocationOutput")),
                run_id,
            ),
            ("Amount",),
        ),
    }


def compare_metrics(original, updated):
    mismatches = []
    for table in sorted(set(original) | set(updated)):
        for key in sorted(
            set(original.get(table, {})) | set(updated.get(table, {}))
        ):
            left = original.get(table, {}).get(key)
            right = updated.get(table, {}).get(key)
            if left != right:
                mismatches.append(
                    f"{table}.{key}: original={left!r} updated={right!r}"
                )
    return mismatches


def summarize_metrics(metrics):
    return {
        "allocation_input_rows": int(
            metrics["AllocationInput"].get("row_count") or 0
        ),
        "allocation_output_rows": int(
            metrics["AllocationOutput"].get("row_count") or 0
        ),
    }
