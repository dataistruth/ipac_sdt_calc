"""
orchestrator.py

Converted from: dbo.uspLoadFootnotesAllocationToOutput.sql
Production-inline Checkpoint V2 + parallel S3/S5 plans.
S13 writes AllocationOutput then AllocationInput deduction sequentially.
"""

import json
import logging
import time
from contextlib import contextmanager

import pyspark.sql.functions as F

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config
from Common_V2.core.execution_profiles import resolve_execution_profile

from AllocationV2.usp_load_footnotes_allocation_to_output.output.allocation_704c import (
    apply_704c_deduction,
    build_704c_allocation_output,
    build_704c_config,
    build_allocation_percentage_temp,
)
from AllocationV2.usp_load_footnotes_allocation_to_output.output.allocation_effective import (
    build_effective_pct_allocation,
    resolve_min_quarter,
)
from AllocationV2.usp_load_footnotes_allocation_to_output.output.allocation_input import (
    build_allocation_input,
    build_temp_allocation_input,
    build_temp_book_effective,
    build_temp_final_effective_pct,
    build_zero_exclude_lines,
)
from AllocationV2.usp_load_footnotes_allocation_to_output.output.config import load_sp_config, validate_run_preconditions
from AllocationV2.usp_load_footnotes_allocation_to_output.output.join_optimizations import (
    broadcast_part_v_lines,
    broadcast_zero_exclude_lines,
    build_custom_footnote_line_types,
    derive_cost_underlying_types,
    quarter_join_hints,
)
from AllocationV2.usp_load_footnotes_allocation_to_output.outputV2.parallel_helpers import parse_enabled_groups, run_phase
from AllocationV2.usp_load_footnotes_allocation_to_output.output.quarter_logic import (
    update_form_quarters,
    update_pfic_partv_quarters,
    update_pfic_quarters_by_config,
)
from AllocationV2.usp_load_footnotes_allocation_to_output.output.underlyings import (
    build_cost_percentage_data,
    build_entity_hierarchy,
    build_underlyings_footnotes_ordered,
    filter_asset_class,
)
from AllocationV2.usp_load_footnotes_allocation_to_output.output.writers import apply_deduction, write_allocation_output

logger = logging.getLogger(__name__)



def _start_dev_plan_profile(enabled):
    from .plan_profiler import disable_plan_profile

    if not enabled:
        disable_plan_profile()
        return None
    from .plan_profiler import (
        start_action_profile,
        start_checkpoint_plan_profile,
        start_plan_profile,
    )
    return (
        start_plan_profile(),
        start_checkpoint_plan_profile(),
        start_action_profile(),
    )


def _finish_dev_plan_profile(tokens, threshold=30, cfg=None):
    from .plan_profiler import disable_plan_profile, store_last_plan_profile

    if not tokens:
        disable_plan_profile()
        return
    from .plan_profiler import (
        finish_action_profile,
        finish_checkpoint_plan_profile,
        finish_plan_profile,
        plan_profile_report,
    )

    def _pair(item):
        if isinstance(item, tuple) and len(item) >= 2:
            return item[0], list(item[1] or [])
        return item, []

    plan_token, cp_token, action_token = tokens
    plan_tok, builder = _pair(plan_token)
    cp_tok, checkpoints = _pair(cp_token)
    action_tok, actions = _pair(action_token)
    finish_action_profile(action_tok)
    finish_checkpoint_plan_profile(cp_tok)
    finish_plan_profile(plan_tok)
    if isinstance(cfg, dict):
        builder = builder + list(cfg.get("_plan_profile") or [])
        checkpoints = checkpoints + list(cfg.get("_checkpoint_plan_profile") or [])
        actions = actions + list(cfg.get("_action_plan_profile") or [])
    builder_rep = plan_profile_report(builder, threshold, label="BUILDER") or []
    ckpt_rep = plan_profile_report(checkpoints, threshold, label="CHECKPOINT") or []
    action_rep = plan_profile_report(actions, threshold, label="ACTION") or []
    store_last_plan_profile(builder_rep, ckpt_rep, action_rep)
    disable_plan_profile()


def _normalize_workers(max_threads=4, MaxThreads=None):
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 4))


def _blank(value):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _as_int(value):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return int(value)


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
    max_threads: int = None,
    MaxThreads: int = None,
    parallel_groups: str = "all",
    ParallelGroups: str = None,
    execution_profile: str = "low",
    ExecutionProfile: str = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    SqlShufflePartitions=None,
    profile_plan: bool = False,
    plan_checkpoint_threshold: int = 30,
    **kwargs,
):
    del kwargs
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName
    rank_for_rule_pickup = RankForRulePickup

    started = time.time()
    timings = []
    enabled_groups = parse_enabled_groups(parallel_groups, ParallelGroups)
    profile_name = (
        _blank(ExecutionProfile) or _blank(execution_profile) or "low"
    )
    profile = resolve_execution_profile(profile_name)
    workers = _normalize_workers(
        max_threads=(
            MaxThreads
            if MaxThreads is not None
            else max_threads
            if max_threads is not None
            else profile["max_threads"]
        ),
        MaxThreads=MaxThreads,
    )
    mode = None
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    status = {
        "sp_name": "uspLoadFootnotesAllocationToOutput",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }
    original_shuffle = spark.conf.get("spark.sql.shuffle.partitions")

    try:
        with _timed(timings, "S1-S2/config"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    entity_id=entity_id,
                    client_id=client_id,
                    tax_period_id=tax_period_id,
                    run_id=run_id,
                    catalog=catalog,
                    schema=schema,
                )
            shuffle_override = _as_int(SqlShufflePartitions)
            if shuffle_override is None:
                spark.conf.set(
                    "spark.sql.shuffle.partitions",
                    str(profile["shuffle_partitions"]),
                )
            else:
                spark.conf.set(
                    "spark.sql.shuffle.partitions",
                    str(shuffle_override),
                )
            explicit_checkpoint = (
                CheckpointMode
                if CheckpointMode is not None
                else checkpoint_mode
            )
            mode = resolve_checkpoint_mode(
                cfg,
                checkpoint_mode=(
                    explicit_checkpoint
                    if explicit_checkpoint is not None
                    else profile["checkpoint_mode"]
                ),
                CheckpointMode=CheckpointMode,
            )
            cfg = {
                **cfg,
                "_checkpoint_tables": [],
                "_checkpoint_paths": [],
                "_checkpoint_v2_activity": [],
                "checkpoint_mode": mode,
                "max_threads": workers,
                "execution_profile": profile_name,
            }
            if rank_for_rule_pickup is not None:
                cfg["rank_for_rule_pickup"] = rank_for_rule_pickup
            assert cfg.get("rank_for_rule_pickup") is not None, (
                "rank_for_rule_pickup must be provided"
            )
            initialize_checkpoint_V2(cfg, mode)
            cfg["profile_plan"] = bool(profile_plan)
            _plan_tokens = _start_dev_plan_profile(profile_plan)
            print(
                f"[CHECKPOINT_V2] ExecutionProfile={profile_name} "
                f"CheckpointMode={mode} shuffle="
                f"{shuffle_override or profile['shuffle_partitions']} "
                f"MaxThreads={workers}"
            )
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            from Common_V2.core.helpers import read_table as _read

            cfg["_df_pfic_footnote_line_item"] = _read(
                spark, "PFICFootnoteLineItem", cfg
            )
            cfg["_df_entity"] = _read(spark, "Entity", cfg)
            load_sp_config(spark, cfg)
            if not validate_run_preconditions(spark, cfg):
                logger.info(
                    f"[SKIP] RunStatus=FAIL or wrong allocation type. "
                    f"RunID={cfg['run_id']}, EntityID={cfg['entity_id']}"
                )
                status["status"] = "SKIPPED"
                return status

        def _cost_snapshot():
            snapshot, _unused_types = build_cost_percentage_data(spark, cfg)
            del _unused_types
            return snapshot

        with _timed(timings, "S3-S5 plans"):
            planned = dict(
                run_phase(
                    "s3_s5_plans",
                    [
                        (
                            "book",
                            build_temp_book_effective,
                            (spark, cfg),
                            {},
                        ),
                        (
                            "allocation",
                            build_temp_allocation_input,
                            (spark, cfg),
                            {},
                        ),
                        (
                            "zero",
                            build_zero_exclude_lines,
                            (spark, cfg),
                            {},
                        ),
                        (
                            "effective",
                            build_temp_final_effective_pct,
                            (spark, cfg),
                            {},
                        ),
                        ("cost", _cost_snapshot, (), {}),
                    ],
                    workers,
                    enabled_groups,
                )
            )
            df_temp_book_eff = planned["book"]
            df_temp_alloc_input = planned["allocation"]
            df_zero_exclude = broadcast_zero_exclude_lines(planned["zero"])
            df_temp_final_eff_pct = planned["effective"]
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
            df_cost_pct_snapshot = planned["cost"]
            df_cost_pct_snapshot = _checkpoint(
                spark, df_cost_pct_snapshot, "cost_snapshot", cfg
            )
            df_temp_cost_underlying_types = derive_cost_underlying_types(
                df_cost_pct_snapshot
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

        if df_alloc_input.isEmpty():
            logger.info("[SKIP] #AllocationInput is empty — nothing to allocate")
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

            if df_combined is not None:
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

        try:
            _finish_dev_plan_profile(locals().get("_plan_tokens"), cfg=locals().get("cfg"))
        except Exception:
            pass
        spark.conf.set("spark.sql.shuffle.partitions", original_shuffle)
        status["elapsed_seconds"] = round(time.time() - started, 1)
        status["timings"] = timings
        if isinstance(cfg, dict) and cfg.get("drop_checkpoints"):
            drop_checkpoints_V2(spark, cfg)

    logger.info(
        f"[DONE] run_load_footnotes_allocation_to_output | "
        f"{status['elapsed_seconds']}s | "
        f"RunID={cfg['run_id']} EntityID={cfg['entity_id']}"
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
