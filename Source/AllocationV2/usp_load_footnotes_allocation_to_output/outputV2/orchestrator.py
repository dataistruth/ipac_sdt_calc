"""outputV2 orchestration for Footnote Allocation."""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pyspark.sql.functions as F

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config

from ..output.allocation_704c import (
    apply_704c_deduction,
    build_704c_allocation_output,
    build_704c_config,
    build_allocation_percentage_temp,
)
from ..output.allocation_effective import (
    build_effective_pct_allocation,
    resolve_min_quarter,
)
from ..output.allocation_input import (
    build_temp_allocation_input,
    build_temp_book_effective,
    build_temp_final_effective_pct,
    build_zero_exclude_lines,
)
from ..output.config import load_sp_config, validate_run_preconditions
from ..output.quarter_logic import (
    update_form_quarters,
    update_pfic_partv_quarters,
    update_pfic_quarters_by_config,
)
from ..output.underlyings import (
    build_cost_percentage_data,
    build_underlyings_footnotes_ordered,
    filter_asset_class,
)
from ..output.writers import apply_deduction, write_allocation_output
from .join_optimizations import (
    broadcast_part_v_lines,
    broadcast_zero_exclude_lines,
    build_custom_footnote_line_types,
    derive_cost_underlying_types,
    quarter_join_hints,
)
from .plan_break_optimizations import (
    build_allocation_input,
    build_entity_hierarchy,
)
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

logger = logging.getLogger(__name__)

for _builder_name in (
    "build_cost_percentage_data",
    "build_underlyings_footnotes_ordered",
    "build_temp_allocation_input",
    "build_temp_book_effective",
    "build_temp_final_effective_pct",
    "build_zero_exclude_lines",
    "filter_asset_class",
    "update_form_quarters",
    "update_pfic_partv_quarters",
    "update_pfic_quarters_by_config",
    "build_704c_config",
    "build_allocation_percentage_temp",
    "build_704c_allocation_output",
    "apply_704c_deduction",
    "resolve_min_quarter",
    "build_effective_pct_allocation",
    "apply_deduction",
):
    globals()[_builder_name] = track_plan(globals()[_builder_name])


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "on", "true", "yes"}
    return bool(value)


def _normalize_workers(max_threads=4, MaxThreads=None):
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 4))


@contextmanager
def _timed(timings, step):
    started = time.time()
    try:
        yield
    finally:
        timings.append(
            {
                "step": step,
                "elapsed_seconds": round(time.time() - started, 3),
            }
        )


def _checkpoint(spark, df, name, cfg):
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = checkpoint_V2(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    return result


def _profile_reports(enabled, threshold, sinks):
    reports = {"builder": [], "checkpoint": [], "action": []}
    if not enabled:
        return reports
    for label, records, key in (
        ("BUILDER", sinks[0], "builder"),
        ("CHECKPOINT", sinks[1], "checkpoint"),
        ("ACTION", sinks[2], "action"),
    ):
        print(f"\n===== {label}-LEVEL PLAN PROFILE =====")
        try:
            reports[key] = plan_profile_report(
                records, threshold, label=label
            )
        except Exception:
            logger.warning(
                "[PLAN] %s report failed", label, exc_info=True
            )
    return reports


def run_load_footnotes_allocation_to_output(
    spark,
    cfg: dict = None,
    verbose: bool = False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    RankForRulePickup: int = None,
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
    """Run production S1-S13 semantics with shared-V2 checkpoints."""
    del kwargs
    started = time.time()
    timings = []
    workers = _normalize_workers(max_threads, MaxThreads)
    mode = None
    profile_enabled = _as_bool(
        ProfilePlan if ProfilePlan is not None else profile_plan
    )
    threshold = int(
        PlanCheckpointThreshold
        if PlanCheckpointThreshold is not None
        else plan_checkpoint_threshold
    )
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    status = {
        "sp_name": "uspLoadFootnotesAllocationToOutput",
        "run_id": RunID,
        "entity_id": EntityID,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }
    planning_pool = None
    plan_token = checkpoint_token = action_token = None
    builder_records = []
    checkpoint_records = []
    action_records = []

    try:
        with _timed(timings, "S1-S2/config"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    entity_id=EntityID,
                    client_id=ClientID,
                    tax_period_id=TaxPeriodID,
                    run_id=RunID,
                    catalog=CatalogName,
                    schema=SchemaName,
                )
            mode = resolve_checkpoint_mode(
                cfg,
                checkpoint_mode=checkpoint_mode,
                CheckpointMode=CheckpointMode,
            )
            cfg = {
                **cfg,
                "_checkpoint_tables": [],
                "_checkpoint_paths": [],
                "_checkpoint_v2_activity": [],
                "_plan_profile": [],
                "profile_plan": profile_enabled,
                "plan_checkpoint_threshold": threshold,
                "checkpoint_mode": mode,
                "max_threads": workers,
            }
            if RankForRulePickup is not None:
                cfg["rank_for_rule_pickup"] = RankForRulePickup
            assert cfg.get("rank_for_rule_pickup") is not None, (
                "rank_for_rule_pickup must be provided"
            )
            initialize_checkpoint_V2(cfg, mode)
            print(
                f"[outputV2] CheckpointMode={mode} MaxThreads={workers} "
                f"ProfilePlan={'on' if profile_enabled else 'off'}"
            )
            if profile_enabled:
                plan_token, builder_records = start_plan_profile()
                checkpoint_token, checkpoint_records = (
                    start_checkpoint_plan_profile()
                )
                action_token, action_records = start_action_profile()

            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            from Common_V2.core.helpers import read_table as _read

            cfg["_df_pfic_footnote_line_item"] = _read(
                spark, "PFICFootnoteLineItem", cfg
            )
            cfg["_df_entity"] = _read(spark, "Entity", cfg)
            load_sp_config(spark, cfg)
            preconditions_met = validate_run_preconditions(spark, cfg)

        if not preconditions_met:
            status["status"] = "SKIPPED"
            return status

        planning_pool = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="footnote-plan"
        )
        cost_future = planning_pool.submit(
            build_cost_percentage_data, spark, cfg
        )
        initial = {
            "book": planning_pool.submit(
                build_temp_book_effective, spark, cfg
            ),
            "allocation": planning_pool.submit(
                build_temp_allocation_input, spark, cfg
            ),
            "zero": planning_pool.submit(
                build_zero_exclude_lines, spark, cfg
            ),
            "effective": planning_pool.submit(
                build_temp_final_effective_pct, spark, cfg
            ),
        }
        with _timed(timings, "S3 initial plans"):
            df_temp_book_eff = initial["book"].result()
            df_temp_alloc_input = initial["allocation"].result()
            df_zero_exclude = broadcast_zero_exclude_lines(
                initial["zero"].result()
            )
            df_temp_final_eff_pct = initial["effective"].result()
            status["sections_completed"] = 3

        with _timed(timings, "S4 quarter updates"):
            with quarter_join_hints():
                df_temp_alloc_input, df_part_v_allocable = (
                    update_pfic_partv_quarters(
                        spark,
                        cfg,
                        df_temp_alloc_input,
                        df_temp_final_eff_pct,
                    )
                )
                df_part_v_allocable = broadcast_part_v_lines(
                    df_part_v_allocable
                )
                df_temp_alloc_input = update_pfic_quarters_by_config(
                    spark,
                    cfg,
                    df_temp_alloc_input,
                    df_part_v_allocable,
                    df_temp_final_eff_pct,
                )
                df_temp_alloc_input = update_form_quarters(
                    spark, cfg, df_temp_alloc_input
                )
            df_temp_alloc_input = _checkpoint(
                spark, df_temp_alloc_input, "temp_alloc_input", cfg
            )
            status["sections_completed"] = 4

        with _timed(timings, "S5 cost"):
            (
                df_cost_pct_snapshot,
                lazy_cost_underlying_types,
            ) = cost_future.result()
            planning_pool.shutdown(wait=True)
            planning_pool = None
            del lazy_cost_underlying_types
            df_cost_pct_snapshot = _checkpoint(
                spark, df_cost_pct_snapshot, "cost_snapshot", cfg
            )
            df_temp_cost_underlying_types = (
                derive_cost_underlying_types(df_cost_pct_snapshot)
            )
            status["sections_completed"] = 5

        with _timed(timings, "S6 hierarchy"):
            df_all_underlyings, df_asset_class_rel = build_entity_hierarchy(
                spark,
                cfg,
                df_cost_pct_snapshot,
                df_temp_cost_underlying_types,
            )
            status["sections_completed"] = 6

        with _timed(timings, "S7 filtering"):
            df_all_underlyings = filter_asset_class(
                spark, cfg, df_all_underlyings, df_asset_class_rel
            )
            df_all_underlyings = _checkpoint(
                spark, df_all_underlyings, "all_underlyings", cfg
            )
            status["sections_completed"] = 7

        with _timed(timings, "S8 ordering"):
            df_underlyings_fn = build_underlyings_footnotes_ordered(
                spark, cfg, df_all_underlyings, df_temp_alloc_input
            )
            df_underlyings_fn = _checkpoint(
                spark, df_underlyings_fn, "underlyings_fn", cfg
            )
            status["sections_completed"] = 8

        with _timed(timings, "S9 allocation input"):
            df_alloc_input = build_allocation_input(
                spark,
                cfg,
                df_temp_alloc_input,
                df_temp_book_eff,
                df_underlyings_fn,
            )
            df_alloc_input = _checkpoint(
                spark, df_alloc_input, "alloc_input", cfg
            )
            status["sections_completed"] = 9

        is_empty = profile_action(
            "allocation_input.isEmpty",
            df_alloc_input,
            df_alloc_input.isEmpty,
            cfg,
        )
        if is_empty:
            status["status"] = "SKIPPED"
            return status

        with _timed(timings, "S10 704c"):
            df_tmp_alloc_output_704c = None
            df_custom_fn_types = build_custom_footnote_line_types(spark, cfg)
            build_704c_config(spark, cfg)
            if cfg.get("is_704c_enabled"):
                df_alloc_pct = build_allocation_percentage_temp(spark, cfg)
                result_704c = build_704c_allocation_output(
                    spark,
                    cfg,
                    df_alloc_input,
                    df_alloc_pct,
                    df_custom_fn_types,
                )
                if result_704c is not None:
                    df_tmp_alloc_output_704c, df_alloc_input = result_704c
            status["sections_completed"] = 10

        with _timed(timings, "S11 deduction"):
            df_alloc_input, df_fn_allocated_lines = apply_704c_deduction(
                spark,
                cfg,
                df_alloc_input,
                df_tmp_alloc_output_704c,
                df_zero_exclude,
            )
            status["sections_completed"] = 11

        with _timed(timings, "S12 effective"):
            from Common_V2.core.helpers import read_table as _read
            from pyspark.sql import Window

            partner_snapshot = _read(
                spark, "Partner_Snapshot", cfg
            ).filter(
                (F.col("ClientID") == cfg["client_id"])
                & (F.col("TaxPeriodID") == cfg["tax_period_id"])
                & (F.col("EntityID") == cfg["entity_id"])
            )
            window = Window.partitionBy("EntityID")
            latest = (
                partner_snapshot.withColumn(
                    "_wf", F.coalesce(F.col("WorkFlowID"), F.lit(0))
                )
                .withColumn(
                    "_tx", F.coalesce(F.col("TransactionID"), F.lit(0))
                )
                .withColumn("_max_wf", F.max("_wf").over(window))
                .withColumn("_max_tx", F.max("_tx").over(window))
                .filter(
                    F.when(
                        F.col("_max_wf") != 0,
                        F.col("_wf") == F.col("_max_wf"),
                    ).otherwise(F.col("_tx") == F.col("_max_tx"))
                )
            )
            df_entity_partners = F.broadcast(
                latest.select(
                    F.col("PartnerNumber").alias("partnernumber"),
                    F.col("ShareClass"),
                ).distinct()
            )
            resolve_min_quarter(spark, cfg)
            df_tmp_alloc_output_eff = build_effective_pct_allocation(
                spark,
                cfg,
                df_alloc_input,
                df_temp_final_eff_pct,
                df_entity_partners,
                df_custom_fn_types,
            )
            status["sections_completed"] = 12

        with _timed(timings, "S13 writes"):
            shared_cols = [
                "RunID", "ClientID", "EntityID", "ShareClass",
                "PartnerNumber", "LineTypeID", "QuicklinkID", "LineID",
                "Amount", "AllocationType", "ParentEntityID",
                "SuperParentEntityID", "AllocationTypeID", "TrackingKey",
                "OriginalParentEntityID", "SchID",
            ]
            if (
                df_tmp_alloc_output_704c is not None
                and df_tmp_alloc_output_eff is not None
            ):
                normalized = df_tmp_alloc_output_704c
                if "SchID" not in normalized.columns:
                    normalized = normalized.withColumn(
                        "SchID", F.lit(None).cast("int")
                    )
                df_combined = normalized.select(*shared_cols).unionByName(
                    df_tmp_alloc_output_eff.select(*shared_cols)
                )
            elif df_tmp_alloc_output_eff is not None:
                df_combined = df_tmp_alloc_output_eff
            elif df_tmp_alloc_output_704c is not None:
                df_combined = df_tmp_alloc_output_704c
                if "SchID" not in df_combined.columns:
                    df_combined = df_combined.withColumn(
                        "SchID", F.lit(None).cast("int")
                    )
            else:
                df_combined = None

            if df_combined is not None and workers > 1:
                with ThreadPoolExecutor(
                    max_workers=2, thread_name_prefix="footnote-write"
                ) as write_pool:
                    output_future = write_pool.submit(
                        write_allocation_output,
                        spark,
                        {**cfg},
                        df_combined,
                    )
                    deduction_future = write_pool.submit(
                        apply_deduction,
                        spark,
                        {**cfg},
                        df_combined,
                        df_alloc_input,
                        df_fn_allocated_lines,
                        df_zero_exclude,
                    )
                    output_future.result()
                    deduction_future.result()
            elif df_combined is not None:
                write_allocation_output(spark, cfg, df_combined)
                apply_deduction(
                    spark,
                    cfg,
                    df_combined,
                    df_alloc_input,
                    df_fn_allocated_lines,
                    df_zero_exclude,
                )
            status["sections_completed"] = 13
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error("[FAIL] %s", exc, exc_info=True)
        raise
    finally:
        if planning_pool is not None:
            planning_pool.shutdown(wait=True, cancel_futures=True)
        if action_token is not None:
            finish_action_profile(action_token)
        if checkpoint_token is not None:
            finish_checkpoint_plan_profile(checkpoint_token)
        if plan_token is not None:
            finish_plan_profile(plan_token)
        status["elapsed_seconds"] = round(time.time() - started, 1)
        checkpoint_activity = (
            list(cfg.get("_checkpoint_v2_activity", ()))
            if isinstance(cfg, dict)
            else []
        )
        status["timings"] = timings
        status["checkpoint_activity"] = checkpoint_activity
        if profile_enabled and isinstance(cfg, dict):
            builder_records.extend(cfg.get("_plan_profile", ()))
        status["plan_profiles"] = _profile_reports(
            profile_enabled,
            threshold,
            (builder_records, checkpoint_records, action_records),
        )

    return status


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    result = run_load_footnotes_allocation_to_output(
        spark,
        RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
        EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
        ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
        TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
        CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
        SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
        RankForRulePickup=int(  # noqa: F821
            dbutils.widgets.get("rank_for_rule_pickup")  # noqa: F821
        ),
    )
    try:
        dbutils.notebook.exit(json.dumps(result))  # noqa: F821
    except Exception:
        print(json.dumps(result, indent=2))
