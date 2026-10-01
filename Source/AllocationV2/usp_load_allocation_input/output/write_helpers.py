"""Collect result frames, then write flow-up tables in parallel."""

from __future__ import annotations

import json
from datetime import datetime

from Common_V2.core.generic_result_storer import GenericResultStorer
from Common_V2.core.helpers import table_prefix
from pyspark.sql import functions as _F

from .ai_finalization_service import (
    write_allocation_input,
    write_form_flowups,
    write_pfic_flowup,
)
from .parallel_helpers import isolated_cfg, run_parallel

SMALL_TABLES = {
    "Form926Flowup",
    "Form199AFlowup",
    "Form8865Flowup",
    "Form8886Flowup",
    "AtRiskFlowup",
    "CustomFootnoteFlowup",
    "Form200616Flowup",
    "PFICFootnoteFlowup",
}


def _merge_parquet_results(cfg, parts):
    dest = cfg.setdefault("_parquet_results", {})
    schema_cache = cfg.setdefault("_schema_cache", {})
    for frames, schemas in parts:
        for table_name, types in (schemas or {}).items():
            schema_cache.setdefault(table_name, types)
        for table_name, df in (frames or {}).items():
            if table_name in dest:
                dest[table_name] = dest[table_name].unionByName(
                    df, allowMissingColumns=True
                )
            else:
                dest[table_name] = df


def _collect_one(spark, cfg, fn):
    local = isolated_cfg(cfg)
    fn(spark, local)
    return (
        local.get("_parquet_results") or {},
        local.get("_schema_cache") or {},
    )


def _merge_save_values(values):
    """Combine per-table GenericResultStorer JSON into sdt_d's single-call shape."""
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


def collect_output_frames_parallel(
    spark,
    cfg,
    allocation_input_df,
    pfic_flowup_df,
    k1_workflow_df,
    workers,
    activity,
    enabled_groups,
):
    """Build disjoint collected frames concurrently. No disk writes."""
    parts = run_parallel(
        [
            (
                "AllocationInput",
                lambda: _collect_one(
                    spark,
                    cfg,
                    lambda s, c: write_allocation_input(
                        s, c, allocation_input_df
                    ),
                ),
            ),
            (
                "PFICFootnoteFlowup",
                lambda: _collect_one(
                    spark,
                    cfg,
                    lambda s, c: write_pfic_flowup(s, c, pfic_flowup_df),
                ),
            ),
            (
                "FormFlowups",
                lambda: _collect_one(
                    spark,
                    cfg,
                    lambda s, c: write_form_flowups(s, c, k1_workflow_df),
                ),
            ),
        ],
        workers,
        activity,
        "output_collect",
        enabled_groups,
    )
    _merge_parquet_results(cfg, parts)


def _align(spark, cfg, df, tbl_name):
    prefix = table_prefix(cfg)
    fqn = f"{prefix}.{tbl_name}"
    schema_info = cfg.get("_schema_cache", {})
    if tbl_name in schema_info:
        target_types = schema_info[tbl_name]
        target_cols = list(target_types.keys())
    else:
        fields = spark.table(fqn).schema.fields
        target_types = {f.name: f.dataType for f in fields}
        target_cols = [f.name for f in fields]
    out = df
    for col_name in target_cols:
        if col_name not in out.columns:
            col_type = target_types.get(col_name)
            if col_type is not None:
                out = out.withColumn(col_name, _F.lit(None).cast(col_type))
            else:
                out = out.withColumn(col_name, _F.lit(None))
    return out.select(target_cols)


def _write_one_table(spark, cfg, tbl_name, df, client_id, entity_id, execution_id):
    """One distinct-table write. Writer is built inside this task."""
    local = isolated_cfg(cfg)
    write_df = _align(spark, local, df, tbl_name)
    if tbl_name in SMALL_TABLES:
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
):
    """Write AllocationInput first, then flow-up tables in parallel."""
    parquet_results = cfg.get("_parquet_results", {})
    run_id = cfg["run_id"]
    save_return_value = None
    if not parquet_results:
        return save_return_value

    prefix = table_prefix(cfg)
    alloc_df = parquet_results.get("AllocationInput")
    if alloc_df is not None:
        alloc_df.write.format("delta").mode("overwrite").option(
            "replaceWhere", f"RunID = {run_id}"
        ).saveAsTable(f"{prefix}.AllocationInput")
        print("   [ok] AllocationInput (delta)")

    flowup_items = [
        (tbl_name, df)
        for tbl_name, df in parquet_results.items()
        if tbl_name != "AllocationInput"
    ]
    if not flowup_items:
        return save_return_value

    print(
        f"[store] Writing {len(flowup_items)} flow-up tables in parallel: "
        f"{datetime.now()}"
    )
    save_values = run_parallel(
        [
            (
                tbl_name,
                lambda tbl_name=tbl_name, df=df: _write_one_table(
                    spark,
                    cfg,
                    tbl_name,
                    df,
                    client_id,
                    entity_id,
                    execution_id,
                ),
            )
            for tbl_name, df in flowup_items
        ],
        workers,
        activity,
        "output_writes",
        enabled_groups,
    )
    print(
        f"[done] Stored {len(flowup_items)} flow-up tables: {datetime.now()}"
    )
    return _merge_save_values(save_values) or save_return_value


__all__ = [
    "collect_output_frames_parallel",
    "flush_collected_results",
]
