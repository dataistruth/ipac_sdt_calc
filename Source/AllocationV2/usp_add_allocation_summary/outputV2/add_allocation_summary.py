"""Optimized, isolated entry point for uspAddAllocationSummary."""

from __future__ import annotations

import json
import logging
import time

import pyspark.sql.functions as F
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
)
from Common_V2.core.checkpoint_V2 import (
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.helpers import ns
from pyspark.sql import SparkSession

from .parallel_helpers import isolated_cfg, normalize_workers, run_parallel
from .parent import output_module
from .plan_profiler import (
    finish_action_profile,
    finish_checkpoint_plan_profile,
    finish_plan_profile,
    plan_profile_report,
    profile_action,
    start_action_profile,
    start_checkpoint_plan_profile,
    start_plan_profile,
    track_plan,
)

prod = output_module("add_allocation_summary")
logger = logging.getLogger("AllocationV2.usp_add_allocation_summary.outputV2")
_LAST_RUN_PROFILE = {}

build_working_tables = track_plan(prod.build_working_tables)
build_pfic_reclass_data = track_plan(prod.build_pfic_reclass_data)
build_custom_footnote_transactions = track_plan(
    prod.build_custom_footnote_transactions
)


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def _write_pfic_text(spark, cfg, pfic_reclass_df):
    """Preserve the production pfic_alloc_text materialization seam."""
    df = pfic_reclass_df.select(
        F.lit(cfg["run_id"]).cast("long").alias("RunID"),
        F.lit(cfg["client_id"]).cast("long").alias("ClientID"),
        F.lit(cfg["tax_period_id"]).alias("TaxPeriodID"),
        F.lit(cfg["entity_id"]).alias("EntityID"),
        F.col("SourceEntityID"),
        F.col("PFICFootnoteID"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("TextValue"),
        F.col("ParentEntityId"),
        F.col("TrackingKey"),
        F.col("OriginalParentEntityID"),
    ).distinct()
    df = checkpoint(spark, df, "pfic_alloc_text", cfg)
    return prod._store_result_table(
        spark, cfg, "PFICFootnoteAllocationText", df
    )


@track_plan
def _partner_pfic(spark, cfg):
    workflow_id = cfg.get("partner_workflow_id")
    transaction_id = cfg.get("partner_transaction_id")
    selected_id = workflow_id if workflow_id is not None else transaction_id
    return prod.read_table(spark, "Partner_Snapshot", cfg).filter(
        (
            F.coalesce(F.col("WorkFlowID"), F.col("TransactionID"))
            == F.lit(selected_id).cast("int")
        )
        & (F.col("EntityID") == cfg["entity_id"])
        & (F.col("ClientID") == cfg["client_id"])
    ).select(
        ns(F.col("ShareClass")).alias("ShareClass"),
        F.col("PartnerNumber"),
    ).distinct()


def _run_writer_pool(spark, cfg, tables, workers):
    """Pool only independent writers targeting distinct summary tables."""
    writers = [
        ("K1AllocationSummary", prod.write_k1_allocation_summary),
        ("M1AdjAllocationSummary", prod.write_m1_adj_allocation_summary),
        ("BoxJKLAllocationSummary", prod.write_box_jkl_allocation_summary),
        ("Form926AllocationSummary", prod.write_form926_allocation_summary),
        ("Form8865AllocationSummary", prod.write_form8865_allocation_summary),
        ("Form199AAllocationSummary", prod.write_form199a_allocation_summary),
        ("Line18AAllocationSummary", prod.write_line18a_allocation_summary),
        ("UBTIAllocationSummary", prod.write_ubti_allocation_summary),
        (
            "PassiveIncomeAllocationSummary",
            prod.write_passive_income_allocation_summary,
        ),
        ("Form8886AllocationSummary", prod.write_form8886_allocation_summary),
        ("AtRiskAllocationSummary", prod.write_at_risk_allocation_summary),
        ("GAAPToTaxAllocation", prod.write_gaap_to_tax_allocation),
        (
            "AdjustmentAllocationSummary",
            prod.write_adjustment_allocation_summary,
        ),
    ]

    def task(writer):
        local_cfg = isolated_cfg(cfg)
        writer(spark, local_cfg, tables)
        return local_cfg.get("_result_file_infos", [])

    results, activity = run_parallel(
        [(name, lambda writer=writer: task(writer)) for name, writer in writers],
        workers,
        "aos_summary_writers",
    )
    for _, file_infos in results:
        cfg["_result_file_infos"].extend(file_infos)
    return activity


def run_add_allocation_summary(
    spark: SparkSession,
    cfg: dict = None,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "None",
    VolumePath: str = None,
    ExecutionID: str = None,
    max_threads: int = 4,
    MaxThreads: int = None,
    profile_plan: bool = False,
    ProfilePlan=None,
    plan_checkpoint_threshold: int = 30,
    PlanCheckpointThreshold: int = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    **kwargs,
):
    """Run the outputV2 candidate with bounded independent summary writes."""
    global _LAST_RUN_PROFILE
    started = time.time()
    workers = normalize_workers(max_threads, MaxThreads)
    profile_enabled = _as_bool(
        ProfilePlan if ProfilePlan is not None else profile_plan
    )
    threshold = int(
        PlanCheckpointThreshold
        if PlanCheckpointThreshold is not None
        else plan_checkpoint_threshold
    )
    if cfg is None:
        cfg = prod.load_common_config(
            spark,
            entity_id=EntityID,
            client_id=ClientID,
            tax_period_id=TaxPeriodID,
            run_id=RunID,
            catalog=CatalogName,
            schema=SchemaName,
        )
    if ResultType is not None:
        cfg.setdefault("result_type", ResultType)
    if VolumePath is not None:
        cfg["volume_path"] = VolumePath
    if ExecutionID is not None:
        cfg["execution_id"] = ExecutionID
    checkpoint_mode = resolve_checkpoint_mode(
        cfg,
        checkpoint_mode=checkpoint_mode,
        CheckpointMode=CheckpointMode,
    )
    cfg.pop("_checkpoint_v2_state", None)
    cfg["_checkpoint_tables"] = []
    cfg["_checkpoint_paths"] = []
    cfg["_checkpoint_v2_activity"] = []
    cfg["_result_file_infos"] = []
    cfg["profile_plan"] = profile_enabled
    cfg["plan_checkpoint_threshold"] = threshold
    initialize_checkpoint_V2(cfg, checkpoint_mode)
    print(
        f"[CHECKPOINT_V2] mode={checkpoint_mode} "
        "(odd=local, even=delta)" if checkpoint_mode == 2
        else f"[CHECKPOINT_V2] mode={checkpoint_mode}"
    )

    if cfg.get("run_status") == "FAIL":
        return {
            "status": "SKIPPED",
            "reason": "RunStatus=FAIL",
            "elapsed_seconds": time.time() - started,
        }

    plan_token = checkpoint_token = action_token = None
    builder_records = []
    checkpoint_records = []
    action_records = []
    if profile_enabled:
        plan_token, builder_records = start_plan_profile()
        checkpoint_token, checkpoint_records = (
            start_checkpoint_plan_profile()
        )
        action_token, action_records = start_action_profile()
    parallel_activity = []
    original_store = prod._store_result_table

    def profiled_store(spark_session, local_cfg, table_name, df):
        return profile_action(
            f"{table_name}.save_results",
            df,
            lambda: original_store(
                spark_session, local_cfg, table_name, df
            ),
            local_cfg,
        )

    try:
        prod._store_result_table = profiled_store
        prod.load_sp_config(spark, cfg)
        cfg["allow_pe_book_inserts"] = prod._should_insert_pe_book(cfg)
        tables = build_working_tables(spark, cfg)
        # LowerTierFunds is already RunID-pruned and projected to three keys.
        tables["lower_tier_funds"] = F.broadcast(
            tables["lower_tier_funds"]
        )

        # Each task has an isolated cfg and a distinct target table. Writers
        # with two inserts keep those inserts serial inside the task.
        parallel_activity.append(
            _run_writer_pool(spark, cfg, tables, workers)
        )

        # This serial chain reads PFIC text before writing it.
        pfic_reclass_df = build_pfic_reclass_data(spark, cfg, tables)
        prod.write_pfic_footnote_allocation_summary(spark, cfg, tables)
        _write_pfic_text(spark, cfg, pfic_reclass_df)

        # Custom-footnote build and its same-table inserts stay serial.
        partner_pfic = F.broadcast(_partner_pfic(spark, cfg))
        tables["partner_pfic"] = partner_pfic
        cf_data = build_custom_footnote_transactions(spark, cfg, tables)
        prod.write_custom_footnote_allocation_summary(
            spark, cfg, tables, cf_data, partner_pfic
        )

        # Direct insert must commit before this writer reads its own table.
        prod.write_form200616_allocation_summary(spark, cfg, tables)

        merged = {}
        for blob in cfg.get("_result_file_infos", []):
            try:
                merged.update(json.loads(blob))
            except (json.JSONDecodeError, TypeError):
                pass
        result_json = json.dumps(merged) if merged else ""
        status = {
            "sp_name": "uspAddAllocationSummary",
            "run_id": cfg.get("run_id", RunID),
            "entity_id": cfg.get("entity_id", EntityID),
            "status": "SUCCESS",
            "elapsed_seconds": round(time.time() - started, 1),
        }
        return result_json if result_json else status
    except Exception:
        logger.exception(
            "uspAddAllocationSummary outputV2 FAILED after %.1fs",
            time.time() - started,
        )
        raise
    finally:
        prod._store_result_table = original_store
        if action_token is not None:
            finish_action_profile(action_token)
        if checkpoint_token is not None:
            finish_checkpoint_plan_profile(checkpoint_token)
        if plan_token is not None:
            finish_plan_profile(plan_token)
        if profile_enabled:
            print(
                "===== BUILDER-LEVEL PLAN PROFILE "
                "(where the plan grows) ====="
            )
            plan_profile_report(builder_records, threshold, label="BUILDER")
            print(
                "===== CHECKPOINT-LEVEL PLAN PROFILE "
                "(plan truncated at each checkpoint) ====="
            )
            plan_profile_report(
                checkpoint_records, threshold, label="CHECKPOINT"
            )
            print("===== ACTION-LEVEL PLAN PROFILE =====")
            plan_profile_report(action_records, threshold, label="ACTION")
        _LAST_RUN_PROFILE = {
            "plan_profile": list(builder_records),
            "checkpoint_plan_profile": list(checkpoint_records),
            "action_profile": list(action_records),
            "checkpoint_activity": list(
                cfg.get("_checkpoint_v2_activity", [])
            ),
            "parallel_activity": parallel_activity,
        }
        # drop_checkpoints_V2 is exported for optional debugging only.


__all__ = [
    "drop_checkpoints_V2",
    "get_last_run_profile",
    "run_add_allocation_summary",
]
