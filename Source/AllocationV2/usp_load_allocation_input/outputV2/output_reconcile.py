"""Mutation-safe snapshot, restore, and parity fingerprints for one RunID."""

from __future__ import annotations

import uuid

import pyspark.sql.functions as F

# Only tables whose live schema includes RunID. Do not list PFICUpdateAlert
# or PFICAlertDetails here: those tables use AlertID/ClientID/EntityID/
# TaxPeriodID and have no RunID.
TABLE_SPECS = (
    ("AllocationInput", "RunID"),
    ("PFICFootnoteFlowup", "RunID"),
    ("PFICFootnoteFlowupWithTrackingKey", "RunID"),
    ("Form926Flowup", "RunID"),
    ("Form199AFlowup", "RunID"),
    ("Form8865Flowup", "RunID"),
    ("Form8886Flowup", "RunID"),
    ("AtRiskFlowup", "RunID"),
    ("CustomFootnoteFlowup", "RunID"),
    ("Form200616Flowup", "RunID"),
    ("AllocationRunErrors", "RunID"),
)
MEASURE_COLUMNS = ("Amount", "Amount704b")
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


def _columns(spark, fqn):
    try:
        if not spark.catalog.tableExists(fqn):
            return []
        return list(spark.table(fqn).columns)
    except Exception:
        return []


def create_benchmark_snapshot(spark, catalog, schema, run_id, **_kwargs):
    snapshot = {
        "catalog": catalog,
        "schema": schema,
        "run_id": int(run_id),
        "tables": {},
    }
    for table, run_column in TABLE_SPECS:
        source = _fqn(catalog, schema, table)
        columns = _columns(spark, source)
        if not columns:
            snapshot["tables"][table] = {
                "exists": False,
                "run_column": run_column,
                "backup": None,
            }
            continue
        if run_column not in columns:
            snapshot["tables"][table] = {
                "exists": True,
                "run_column": run_column,
                "backup": None,
            }
            print(
                f"[reconcile] skip snapshot {table}: no {run_column}; "
                f"columns={columns[:12]}"
            )
            continue
        backup = (
            f"_benchmark_lai_{table.lower()[:16]}_"
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
    if not spec["exists"] or not spec.get("backup"):
        return
    target = _fqn(snapshot["catalog"], snapshot["schema"], table)
    backup = _fqn(snapshot["catalog"], snapshot["schema"], spec["backup"])
    predicate = (
        f"{_quote(spec['run_column'])} = {int(snapshot['run_id'])}"
    )
    spark.sql(f"DELETE FROM {target} WHERE {predicate}")
    spark.sql(f"INSERT INTO {target} SELECT * FROM {backup}")


def reset_before_variant(spark, snapshot):
    for table, _ in TABLE_SPECS:
        spec = snapshot["tables"][table]
        if not spec.get("backup"):
            continue
        target = _fqn(snapshot["catalog"], snapshot["schema"], table)
        spark.sql(
            f"DELETE FROM {target} WHERE {_quote(spec['run_column'])} "
            f"= {int(snapshot['run_id'])}"
        )


def restore_original_state(spark, snapshot):
    for table, _ in TABLE_SPECS:
        spec = snapshot["tables"][table]
        if spec.get("backup"):
            _replace_from_snapshot(spark, snapshot, table)
            continue
        if not spec["exists"]:
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


FILE_FINGERPRINT_KEYS = (
    "volume_path",
    "volume_file_count",
    "volume_file_types",
    "volume_files",
    "delta_num_files",
    "delta_size_bytes",
    "delta_location",
)


def _get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except Exception:
        return None


def _join_path(*parts):
    return "/".join(
        str(part).strip().strip("/")
        for part in parts
        if part is not None and str(part).strip()
    )


def _file_type(name):
    base = str(name).rstrip("/").split("/")[-1]
    if base.startswith("_"):
        return base.split(".")[0]
    if "." in base:
        return base.rsplit(".", 1)[-1].lower()
    return "other"


def _list_files(dbutils, path, depth=0):
    if dbutils is None or not path or depth > 6:
        return []
    try:
        entries = dbutils.fs.ls(path)
    except Exception:
        return []
    found = []
    for entry in entries:
        entry_path = getattr(entry, "path", "") or ""
        if hasattr(entry, "isDir"):
            flag = entry.isDir
            is_dir = flag() if callable(flag) else bool(flag)
        else:
            is_dir = entry_path.endswith("/")
        if is_dir:
            found.extend(_list_files(dbutils, entry_path, depth + 1))
            continue
        name = getattr(entry, "name", None) or entry_path.rstrip("/").split("/")[-1]
        found.append(
            {
                "name": name,
                "type": _file_type(name),
                "bytes": int(getattr(entry, "size", 0) or 0),
            }
        )
    return found


def _delta_file_stats(spark, fqn):
    try:
        row = spark.sql(f"DESCRIBE DETAIL {fqn}").first()
    except Exception:
        return {
            "delta_num_files": None,
            "delta_size_bytes": None,
            "delta_location": "",
        }
    if row is None:
        return {
            "delta_num_files": None,
            "delta_size_bytes": None,
            "delta_location": "",
        }
    detail = row.asDict()
    return {
        "delta_num_files": int(detail.get("numFiles") or 0),
        "delta_size_bytes": int(detail.get("sizeInBytes") or 0),
        "delta_location": str(detail.get("location") or ""),
    }


def fingerprint_files(
    spark,
    catalog,
    schema,
    table,
    volume_path="",
    client_id=None,
    run_id=None,
    execution_id="1",
):
    fqn = _fqn(catalog, schema, table)
    stats = _delta_file_stats(spark, fqn)
    volume_root = ""
    if volume_path and client_id is not None and run_id is not None:
        joined = _join_path(
            str(volume_path).replace("dbfs:", ""),
            client_id,
            run_id,
            execution_id or "1",
            table,
        )
        volume_root = joined if joined.startswith("/") else f"/{joined}"
    listed = _list_files(_get_dbutils(spark), volume_root)
    type_counts = {}
    for item in listed:
        type_counts[item["type"]] = type_counts.get(item["type"], 0) + 1
    return {
        **stats,
        "volume_path": volume_root,
        "volume_file_count": len(listed),
        "volume_file_types": dict(sorted(type_counts.items())),
        "volume_files": [
            f"{item['name']}|{item['type']}|{item['bytes']}"
            for item in sorted(listed, key=lambda row: row["name"])
        ],
    }


def _data_fingerprint(item):
    return {
        key: value
        for key, value in (item or {}).items()
        if key not in FILE_FINGERPRINT_KEYS
    }


def _file_fingerprint(item):
    return {
        key: (item or {}).get(key)
        for key in FILE_FINGERPRINT_KEYS
    }


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


def capture_metrics(spark, catalog, schema, run_id, **kwargs):
    volume_path = kwargs.get("volume_path") or kwargs.get("VolumePath") or ""
    client_id = kwargs.get("client_id")
    if client_id is None:
        client_id = kwargs.get("ClientID")
    execution_id = (
        kwargs.get("execution_id")
        or kwargs.get("ExecutionID")
        or "1"
    )
    captured = {}
    for table, run_column in TABLE_SPECS:
        item = fingerprint_table(
            spark, catalog, schema, table, run_column, run_id
        )
        item.update(
            fingerprint_files(
                spark,
                catalog,
                schema,
                table,
                volume_path=volume_path,
                client_id=client_id,
                run_id=run_id,
                execution_id=execution_id,
            )
        )
        captured[table] = item
    return captured


def compare_metrics(original, updated):
    return [
        {
            "table": table,
            "original": _data_fingerprint(original.get(table)),
            "updated": _data_fingerprint(updated.get(table)),
        }
        for table, _ in TABLE_SPECS
        if _data_fingerprint(original.get(table))
        != _data_fingerprint(updated.get(table))
        and not (
            (original.get(table) or {}).get("skipped")
            and (updated.get(table) or {}).get("skipped")
        )
    ]


def compare_files(original, updated):
    return [
        {
            "table": table,
            "original": _file_fingerprint(original.get(table)),
            "updated": _file_fingerprint(updated.get(table)),
        }
        for table, _ in TABLE_SPECS
        if _file_fingerprint(original.get(table))
        != _file_fingerprint(updated.get(table))
    ]


def summarize_metrics(metrics):
    return {
        "total_rows": sum(
            int(item.get("rows", 0) or 0) for item in metrics.values()
        ),
        "tables_present": sum(
            bool(item.get("exists")) for item in metrics.values()
        ),
        "volume_files": sum(
            int(item.get("volume_file_count", 0) or 0)
            for item in metrics.values()
        ),
        "delta_files": sum(
            int(item.get("delta_num_files") or 0)
            for item in metrics.values()
            if item.get("delta_num_files") is not None
        ),
    }


def create_run_snapshots(spark, catalog, schema, run_id, **kwargs):
    return create_benchmark_snapshot(
        spark, catalog, schema, run_id, **kwargs
    )


def capture_outputs(spark, catalog, schema, run_id, **kwargs):
    return capture_metrics(spark, catalog, schema, run_id, **kwargs)


def compare_outputs(original, updated):
    return compare_metrics(original, updated)


def restore_run_snapshots(spark, catalog, schema, run_id, snapshots):
    del catalog, schema, run_id
    restore_original_state(spark, snapshots)


def drop_run_snapshots(spark, catalog, schema, snapshots):
    del catalog, schema
    drop_benchmark_snapshot(spark, snapshots)


__all__ = [
    "TABLE_SPECS",
    "capture_metrics",
    "capture_outputs",
    "compare_files",
    "compare_metrics",
    "compare_outputs",
    "fingerprint_files",
    "create_benchmark_snapshot",
    "create_run_snapshots",
    "drop_benchmark_snapshot",
    "drop_run_snapshots",
    "fingerprint_table",
    "reset_before_variant",
    "restore_original_state",
    "restore_run_snapshots",
    "summarize_metrics",
]
