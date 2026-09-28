"""Collect independent result frames, then flush distinct tables in parallel."""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime
from pathlib import Path

from Common_V2.core.generic_result_storer import GenericResultStorer
from Common_V2.core.helpers import table_prefix
from pyspark.sql import functions as _F

from .parallel_helpers import isolated_cfg, run_parallel
from .parent import output_module


def _load_form_flowup_collect():
    """Import form_flowup_collect.py as a package member or from disk."""
    path = Path(__file__).resolve().with_name("form_flowup_collect.py")
    pkg = __package__ or "AllocationV2.usp_load_allocation_input.outputV2"
    name = f"{pkg}.form_flowup_collect"
    if name in sys.modules:
        return sys.modules[name]
    if __package__:
        try:
            from . import form_flowup_collect as module
            return module
        except (ModuleNotFoundError, ImportError):
            pass
    if not path.is_file():
        raise ModuleNotFoundError(
            "form_flowup_collect.py is not next to write_helpers.py at "
            f"{path}. Sync it as a Python source file, not a notebook."
        )
    spec = importlib.util.spec_from_file_location(
        name,
        path,
        submodule_search_locations=[str(path.parent)],
    )
    module = importlib.util.module_from_spec(spec)
    module.__package__ = pkg
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_form_flowup_collect = _load_form_flowup_collect()
FORM_FLOWUP_TABLES = _form_flowup_collect.FORM_FLOWUP_TABLES
collect_form_flowup_table = _form_flowup_collect.collect_form_flowup_table
prepare_unblocked_footnotes = _form_flowup_collect.prepare_unblocked_footnotes

_final = output_module("ai_finalization_service")
write_allocation_input = _final.write_allocation_input
write_pfic_flowup = _final.write_pfic_flowup

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
    prepare_unblocked_footnotes(spark, cfg)
    form_tasks = [
        (
            table_name,
            lambda table_name=table_name: _collect_one(
                spark,
                cfg,
                lambda s, c, table_name=table_name: collect_form_flowup_table(
                    s, c, k1_workflow_df, table_name
                ),
            ),
        )
        for table_name in FORM_FLOWUP_TABLES
    ]
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
            *form_tasks,
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
    """One distinct-table Delta write. Writer is built inside this task."""
    local = isolated_cfg(cfg)
    write_df = _align(spark, local, df, tbl_name)
    if tbl_name in SMALL_TABLES:
        write_df = write_df.coalesce(1)
    prefix = table_prefix(local)
    fqn = f"{prefix}.{tbl_name}"
    result_type = local.get("result_type", "deltalake")
    run_id = local["run_id"]
    if result_type == "deltalake" and "RunID" in write_df.columns:
        write_df.write.format("delta").mode("overwrite").option(
            "replaceWhere", f"RunID = {run_id}"
        ).saveAsTable(fqn)
        print(f"   [ok] {tbl_name} (delta)")
        return None
    storer = GenericResultStorer(spark, None)
    return storer.save_results(
        result={tbl_name: write_df},
        result_type=result_type,
        catalog_name=local.get("catalog", ""),
        database_name=local.get("schema", ""),
        run_id=run_id,
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
    """Write every collected table in one output_writes wave, including AllocationInput."""
    parquet_results = cfg.get("_parquet_results", {})
    save_return_value = None
    if not parquet_results:
        return save_return_value

    write_items = list(parquet_results.items())
    print(
        f"[store] Writing {len(write_items)} tables in parallel: "
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
            for tbl_name, df in write_items
        ],
        workers,
        activity,
        "output_writes",
        enabled_groups,
    )
    print(
        f"[done] Stored {len(write_items)} tables: {datetime.now()}"
    )
    for value in save_values:
        if (
            value
            and isinstance(value, str)
            and value.strip().startswith("{")
        ):
            save_return_value = value
    return save_return_value


__all__ = [
    "collect_output_frames_parallel",
    "flush_collected_results",
]
