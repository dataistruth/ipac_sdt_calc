"""Collect independent result frames, then flush in production write order.

``write_allocation_input``, ``write_pfic_flowup``, and ``write_form_flowups``
only populate ``cfg["_parquet_results"]``. The Delta AllocationInput write
still runs before the Parquet storer, matching production failure semantics.
"""

from __future__ import annotations

from datetime import datetime

from Common_V2.core.generic_result_storer import GenericResultStorer
from Common_V2.core.helpers import table_prefix
from pyspark.sql import functions as _F

from .parallel_helpers import isolated_cfg, run_parallel
from .parent import output_module

_final = output_module("ai_finalization_service")
write_allocation_input = _final.write_allocation_input
write_pfic_flowup = _final.write_pfic_flowup
write_form_flowups = _final.write_form_flowups

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
    for part in parts:
        for table_name, df in (part or {}).items():
            if table_name in dest:
                dest[table_name] = dest[table_name].unionByName(
                    df, allowMissingColumns=True
                )
            else:
                dest[table_name] = df


def _collect_one(spark, cfg, fn):
    local = isolated_cfg(cfg)
    fn(spark, local)
    return local.get("_parquet_results") or {}


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
        "output_writes",
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


def flush_collected_results(spark, cfg, client_id, entity_id, execution_id):
    """Production Write 1 (AllocationInput Delta) then Write 2 (Parquet)."""
    parquet_results = cfg.get("_parquet_results", {})
    run_id = cfg["run_id"]
    save_return_value = None
    if not parquet_results:
        return save_return_value

    prefix = table_prefix(cfg)
    volume_path = cfg.get("volume_path") or ""

    alloc_df = parquet_results.get("AllocationInput")
    if alloc_df is not None:
        alloc_df.write.format("delta").mode("overwrite").option(
            "replaceWhere", f"RunID = {run_id}"
        ).saveAsTable(f"{prefix}.AllocationInput")
        print("   [ok] AllocationInput (delta)")

    parquet_tables = {}
    for tbl_name, df in parquet_results.items():
        if tbl_name == "AllocationInput":
            continue
        write_df = _align(spark, cfg, df, tbl_name)
        if tbl_name in SMALL_TABLES:
            write_df = write_df.coalesce(1)
        parquet_tables[tbl_name] = write_df

    if parquet_tables:
        result_type = cfg.get("result_type", "deltalake")
        print(
            f"[store] Writing {len(parquet_tables)} Parquet tables: "
            f"{datetime.now()}"
        )
        storer = GenericResultStorer(spark, None)
        save_return_value = storer.save_results(
            result=parquet_tables,
            result_type=result_type,
            catalog_name=cfg.get("catalog", ""),
            database_name=cfg.get("schema", ""),
            run_id=run_id,
            client_id=client_id,
            entity_id=entity_id,
            execution_id=execution_id,
            volume_path=volume_path,
            sql_url_path=cfg.get("sql_url_path", ""),
            sql_username=cfg.get("sql_username", ""),
            sql_password=cfg.get("sql_password", ""),
        )
        print(
            f"[done] Stored {len(parquet_tables)} Parquet tables: "
            f"{datetime.now()}"
        )
    return save_return_value


__all__ = [
    "collect_output_frames_parallel",
    "flush_collected_results",
]
