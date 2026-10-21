"""Shared write helpers: isolated-cfg fan-out, result merging, and parallel flushes.

SP-specific table lists and write functions stay in each SP's services; these
helpers only own the threading, cfg isolation, and result-merge mechanics.
Every distinct table is written by exactly one task.
"""

from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql import functions as F

from Common_V2.core.generic_result_storer import GenericResultStorer
from Common_V2.core.helpers import table_prefix
from Common_V2.core.parallel_helpers import isolated_cfg, run_parallel


def merge_parquet_results(cfg, parts, union=True):
    """Merge ``[(frames, schemas), ...]`` from isolated tasks into ``cfg``.

    With ``union`` a table produced by several parts is unioned by name;
    otherwise the later part replaces the earlier frame.
    """
    dest = cfg.setdefault("_parquet_results", {})
    schema_cache = cfg.setdefault("_schema_cache", {})
    for frames, schemas in parts:
        for table_name, types in (schemas or {}).items():
            schema_cache.setdefault(table_name, types)
        for table_name, df in (frames or {}).items():
            if union and table_name in dest:
                dest[table_name] = dest[table_name].unionByName(
                    df, allowMissingColumns=True
                )
            else:
                dest[table_name] = df


def run_isolated_tasks(spark, cfg, tasks, workers, activity, label, enabled_groups, union=True):
    """Run ``[(name, fn(spark, local_cfg)), ...]`` with one isolated cfg per task.

    Results collected into each task's ``_parquet_results`` are merged back
    into ``cfg`` in task order. Returns the task return values in task order.
    """
    locals_ = [isolated_cfg(cfg) for _ in tasks]
    results = run_parallel(
        [
            (name, lambda fn=fn, local=local: fn(spark, local))
            for (name, fn), local in zip(tasks, locals_)
        ],
        workers,
        activity,
        label,
        enabled_groups,
    )
    merge_parquet_results(
        cfg,
        [
            (local.get("_parquet_results") or {}, local.get("_schema_cache") or {})
            for local in locals_
        ],
        union=union,
    )
    return results


def merge_save_values(values):
    """Combine per-table GenericResultStorer JSON into a single-call shape."""
    merged = None
    for value in values:
        if not (value and isinstance(value, str) and value.strip().startswith("{")):
            continue
        info = json.loads(value)
        if merged is None:
            merged = {"ResultFilePath": info.get("ResultFilePath", "")}
        for key, files in info.items():
            if key != "ResultFilePath":
                merged[key] = files
    return json.dumps(merged) if merged is not None else None


def align_to_table(spark, cfg, df, tbl_name):
    """Add missing target columns as typed nulls and select in target order."""
    schema_info = cfg.get("_schema_cache", {})
    if tbl_name in schema_info:
        target_types = schema_info[tbl_name]
        target_cols = list(target_types.keys())
    else:
        fields = spark.table(f"{table_prefix(cfg)}.{tbl_name}").schema.fields
        target_types = {f.name: f.dataType for f in fields}
        target_cols = [f.name for f in fields]
    out = df
    for col_name in target_cols:
        if col_name not in out.columns:
            col_type = target_types.get(col_name)
            if col_type is not None:
                out = out.withColumn(col_name, F.lit(None).cast(col_type))
            else:
                out = out.withColumn(col_name, F.lit(None))
    return out.select(target_cols)


def save_table_via_storer(spark, cfg, tbl_name, df, client_id, entity_id, execution_id, coalesce_one=False):
    """One distinct-table write via GenericResultStorer."""
    local = isolated_cfg(cfg)
    write_df = align_to_table(spark, local, df, tbl_name)
    if coalesce_one:
        write_df = write_df.coalesce(1)
    storer = GenericResultStorer(spark, None)
    return storer.save_results(
        result={tbl_name: write_df},
        result_type=local.get("result_type", "deltalake"),
        catalog_name=local.get("catalog", ""),
        database_name=local.get("schema", ""),
        run_id=local["run_id"],
        client_id=client_id,
        entity_id=entity_id,
        execution_id=execution_id,
        volume_path=local.get("volume_path") or "",
        sql_url_path=local.get("sql_url_path", ""),
        sql_username=local.get("sql_username", ""),
        sql_password=local.get("sql_password", ""),
    )


def flush_collected_results(
    spark,
    cfg,
    client_id,
    entity_id,
    execution_id,
    workers,
    activity,
    enabled_groups,
    replace_where_run_tables=(),
    small_tables=frozenset(),
    label="output_writes",
):
    """Write ``cfg["_parquet_results"]``.

    Tables in ``replace_where_run_tables`` are written first, sequentially, as
    Delta ``replaceWhere RunID = <run_id>``. The rest go through
    GenericResultStorer in parallel; ``small_tables`` are coalesced to one file.
    Returns the merged storer JSON (or ``None``).
    """
    parquet_results = cfg.get("_parquet_results", {})
    if not parquet_results:
        return None

    run_id = cfg["run_id"]
    prefix = table_prefix(cfg)
    for tbl_name in replace_where_run_tables:
        df = parquet_results.get(tbl_name)
        if df is None:
            continue
        df.write.format("delta").mode("overwrite").option(
            "replaceWhere", f"RunID = {run_id}"
        ).saveAsTable(f"{prefix}.{tbl_name}")
        print(f"   [ok] {tbl_name} (delta)")

    items = [
        (tbl_name, df)
        for tbl_name, df in parquet_results.items()
        if tbl_name not in replace_where_run_tables
    ]
    if not items:
        return None

    print(f"[store] Writing {len(items)} tables in parallel: {datetime.now()}")
    save_values = run_parallel(
        [
            (
                tbl_name,
                lambda tbl_name=tbl_name, df=df: save_table_via_storer(
                    spark,
                    cfg,
                    tbl_name,
                    df,
                    client_id,
                    entity_id,
                    execution_id,
                    coalesce_one=tbl_name in small_tables,
                ),
            )
            for tbl_name, df in items
        ],
        workers,
        activity,
        label,
        enabled_groups,
    )
    print(f"[done] Stored {len(items)} tables: {datetime.now()}")
    return merge_save_values(save_values)


def write_tables_parallel(spark, cfg, tasks, workers, activity, enabled_groups, label="output_writes"):
    """Run ``[(table_name, fn(spark, cfg_copy)), ...]`` distinct-table writes.

    Each task gets its own shallow cfg copy; collected results stay shared.
    """
    return run_parallel(
        [
            (name, lambda fn=fn: fn(spark, isolated_cfg(cfg, fresh_results=False)))
            for name, fn in tasks
        ],
        workers,
        activity,
        label,
        enabled_groups,
    )


__all__ = [
    "align_to_table",
    "flush_collected_results",
    "merge_parquet_results",
    "merge_save_values",
    "run_isolated_tasks",
    "save_table_via_storer",
    "write_tables_parallel",
]
