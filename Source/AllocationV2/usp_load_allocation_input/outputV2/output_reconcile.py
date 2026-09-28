"""Mutation-safe snapshot, restore, and parity fingerprints for one RunID."""

from __future__ import annotations

import uuid

import pyspark.sql.functions as F

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
    ("PFICUpdateAlert", "RunID"),
    ("PFICAlertDetails", "RunID"),
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


def _table_columns(spark, fqn):
    try:
        if not spark.catalog.tableExists(fqn):
            return []
        return list(spark.table(fqn).columns)
    except Exception:
        return []


def _scope_kind(columns, preferred):
    if preferred in columns:
        return "run"
    if {"ClientID", "EntityID", "TaxPeriodID"}.issubset(set(columns)):
        return "entity"
    return "skip"


def _sql_predicate(kind, columns, snapshot, preferred):
    if kind == "run":
        return f"{_quote(preferred)} = {int(snapshot['run_id'])}"
    if kind == "entity":
        parts = []
        if "ClientID" in columns:
            parts.append(f"{_quote('ClientID')} = {int(snapshot['client_id'])}")
        if "EntityID" in columns:
            parts.append(f"{_quote('EntityID')} = {int(snapshot['entity_id'])}")
        if "TaxPeriodID" in columns:
            parts.append(
                f"{_quote('TaxPeriodID')} = {int(snapshot['tax_period_id'])}"
            )
        return " AND ".join(parts) if parts else "1 = 0"
    return "1 = 0"


def _filter_df(df, kind, preferred, snapshot):
    if kind == "run":
        return df.filter(F.col(preferred) == int(snapshot["run_id"]))
    if kind == "entity":
        out = df
        if "ClientID" in df.columns:
            out = out.filter(F.col("ClientID") == int(snapshot["client_id"]))
        if "EntityID" in df.columns:
            out = out.filter(F.col("EntityID") == int(snapshot["entity_id"]))
        if "TaxPeriodID" in df.columns:
            out = out.filter(
                F.col("TaxPeriodID") == int(snapshot["tax_period_id"])
            )
        return out
    return df.limit(0)


def create_benchmark_snapshot(
    spark,
    catalog,
    schema,
    run_id,
    client_id=None,
    entity_id=None,
    tax_period_id=None,
):
    snapshot = {
        "catalog": catalog,
        "schema": schema,
        "run_id": int(run_id),
        "client_id": int(client_id) if client_id is not None else None,
        "entity_id": int(entity_id) if entity_id is not None else None,
        "tax_period_id": (
            int(tax_period_id) if tax_period_id is not None else None
        ),
        "tables": {},
    }
    for table, preferred in TABLE_SPECS:
        source = _fqn(catalog, schema, table)
        columns = _table_columns(spark, source)
        if not columns:
            snapshot["tables"][table] = {
                "exists": False,
                "run_column": preferred,
                "kind": "missing",
                "backup": None,
            }
            print(f"[reconcile] snapshot skip {table}: missing")
            continue
        kind = _scope_kind(columns, preferred)
        if kind == "entity" and (
            snapshot["client_id"] is None
            or snapshot["entity_id"] is None
            or snapshot["tax_period_id"] is None
        ):
            kind = "skip"
        if kind == "skip":
            snapshot["tables"][table] = {
                "exists": True,
                "run_column": preferred,
                "kind": "skip",
                "backup": None,
            }
            print(
                f"[reconcile] snapshot skip {table}: "
                f"no {preferred}; columns={columns[:12]}"
            )
            continue
        backup = (
            f"_benchmark_lai_{table.lower()[:16]}_"
            f"{int(run_id)}_{uuid.uuid4().hex[:8]}"
        )
        predicate = _sql_predicate(kind, columns, snapshot, preferred)
        spark.sql(
            f"CREATE TABLE {_fqn(catalog, schema, backup)} USING DELTA AS "
            f"SELECT * FROM {source} WHERE {predicate}"
        )
        snapshot["tables"][table] = {
            "exists": True,
            "run_column": preferred,
            "kind": kind,
            "backup": backup,
        }
        print(f"[reconcile] snapshot {table} kind={kind}")
    return snapshot


def _replace_from_snapshot(spark, snapshot, table):
    spec = snapshot["tables"][table]
    if not spec.get("exists") or not spec.get("backup"):
        return
    target = _fqn(snapshot["catalog"], snapshot["schema"], table)
    backup = _fqn(snapshot["catalog"], snapshot["schema"], spec["backup"])
    columns = _table_columns(spark, target)
    predicate = _sql_predicate(
        spec.get("kind") or "run",
        columns,
        snapshot,
        spec["run_column"],
    )
    spark.sql(f"DELETE FROM {target} WHERE {predicate}")
    spark.sql(f"INSERT INTO {target} SELECT * FROM {backup}")


def reset_before_variant(spark, snapshot):
    for table, _ in TABLE_SPECS:
        spec = snapshot["tables"].get(table) or {}
        if spec.get("kind") in {None, "missing", "skip"}:
            continue
        target = _fqn(snapshot["catalog"], snapshot["schema"], table)
        columns = _table_columns(spark, target)
        if not columns:
            continue
        predicate = _sql_predicate(
            spec.get("kind") or "run",
            columns,
            snapshot,
            spec.get("run_column") or "RunID",
        )
        spark.sql(f"DELETE FROM {target} WHERE {predicate}")


def restore_original_state(spark, snapshot):
    for table, _ in TABLE_SPECS:
        spec = snapshot["tables"].get(table) or {}
        if spec.get("backup"):
            _replace_from_snapshot(spark, snapshot, table)
            continue
        if spec.get("kind") in {"missing", "skip"}:
            continue
        target = _fqn(snapshot["catalog"], snapshot["schema"], table)
        columns = _table_columns(spark, target)
        if not columns:
            continue
        predicate = _sql_predicate(
            spec.get("kind") or "run",
            columns,
            snapshot,
            spec.get("run_column") or "RunID",
        )
        spark.sql(f"DELETE FROM {target} WHERE {predicate}")
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


def fingerprint_table(
    spark, catalog, schema, table, run_column, snapshot_or_run_id
):
    if isinstance(snapshot_or_run_id, dict):
        snapshot = snapshot_or_run_id
        run_id = snapshot["run_id"]
        kind_hint = (snapshot.get("tables") or {}).get(table, {}).get("kind")
    else:
        snapshot = {
            "run_id": int(snapshot_or_run_id),
            "client_id": None,
            "entity_id": None,
            "tax_period_id": None,
        }
        run_id = int(snapshot_or_run_id)
        kind_hint = None
    fqn = _fqn(catalog, schema, table)
    columns = _table_columns(spark, fqn)
    if not columns:
        return {"table": table, "exists": False}
    kind = kind_hint or _scope_kind(columns, run_column)
    if kind == "skip":
        return {
            "table": table,
            "exists": True,
            "skipped": True,
            "rows": None,
        }
    df = _filter_df(spark.table(fqn), kind, run_column, snapshot)
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


def capture_metrics(
    spark,
    catalog,
    schema,
    run_id,
    client_id=None,
    entity_id=None,
    tax_period_id=None,
    snapshot=None,
):
    context = snapshot or {
        "run_id": int(run_id),
        "client_id": int(client_id) if client_id is not None else None,
        "entity_id": int(entity_id) if entity_id is not None else None,
        "tax_period_id": (
            int(tax_period_id) if tax_period_id is not None else None
        ),
        "tables": {},
    }
    return {
        table: fingerprint_table(
            spark, catalog, schema, table, run_column, context
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
        and not (
            (original.get(table) or {}).get("skipped")
            and (updated.get(table) or {}).get("skipped")
        )
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


def create_run_snapshots(
    spark,
    catalog,
    schema,
    run_id,
    client_id=None,
    entity_id=None,
    tax_period_id=None,
):
    return create_benchmark_snapshot(
        spark,
        catalog,
        schema,
        run_id,
        client_id=client_id,
        entity_id=entity_id,
        tax_period_id=tax_period_id,
    )


def capture_outputs(
    spark,
    catalog,
    schema,
    run_id,
    client_id=None,
    entity_id=None,
    tax_period_id=None,
    snapshot=None,
):
    return capture_metrics(
        spark,
        catalog,
        schema,
        run_id,
        client_id=client_id,
        entity_id=entity_id,
        tax_period_id=tax_period_id,
        snapshot=snapshot,
    )


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
    "compare_metrics",
    "compare_outputs",
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
