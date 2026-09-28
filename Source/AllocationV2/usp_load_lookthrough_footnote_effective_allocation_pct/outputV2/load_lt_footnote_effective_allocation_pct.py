"""Parity-first outputV2 orchestrator for LT footnote effective allocation %."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager

import pyspark.sql.functions as F

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config
from Common_V2.core.execution_profiles import resolve_execution_profile

from .parallel_helpers import (
    normalize_workers,
    parse_enabled_groups,
)
from .parent import output_module
from .plan_profiler import (
    plan_profile_report,
    profile_action,
    track_checkpoint_plan,
    track_plan,
)
from .write_helpers import flush_result_tables

_prod = output_module("load_lt_footnote_effective_allocation_pct")
logger = _prod.logger

_load_sp_config = _prod._load_sp_config
load_mappings = track_plan(_prod.load_mappings)
expand_parent_k1_mappings = track_plan(_prod.expand_parent_k1_mappings)
build_distinct_mappings = track_plan(_prod.build_distinct_mappings)
build_yearly_effective_pct = track_plan(_prod.build_yearly_effective_pct)
load_partners = track_plan(_prod.load_partners)
load_final_effective_percentages = track_plan(
    _prod.load_final_effective_percentages
)
build_lt_allocation_output = track_plan(_prod.build_lt_allocation_output)
build_cost_effective_pct = track_plan(_prod.build_cost_effective_pct)
build_book_effective_pct = track_plan(_prod.build_book_effective_pct)
load_temp_allocation_input = track_plan(_prod.load_temp_allocation_input)
build_single_multi_alloc_type = track_plan(_prod.build_single_multi_alloc_type)
build_k1_data_amounts = track_plan(_prod.build_k1_data_amounts)
build_final_effective_pct = track_plan(_prod.build_final_effective_pct)
build_allocation_output = track_plan(_prod.build_allocation_output)

_LAST_RUN_PROFILE = {}


def _as_bool(value):
    if isinstance(value, str):
        return value.strip().lower() in {"1", "on", "true", "yes"}
    return bool(value)


def _blank(value):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _as_int(value):
    if _blank(value) is None:
        return None
    return int(value)


@contextmanager
def _timed(timings, step):
    started = time.perf_counter()
    try:
        yield
    finally:
        timings.append(
            {
                "step": step,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
            }
        )


def _v2(msg):
    print(f"[outputV2] {msg}", flush=True)


def _checkpoint(spark, df, name, cfg):
    """Checkpoint V2 with the footnotes local-backend qualifier reset."""
    if df is None:
        _v2(f"checkpoint {name}: skipped (df=None)")
        return None
    track_checkpoint_plan(name, df, cfg)
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = checkpoint(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    _v2(f"checkpoint {name}: done")
    return result


def _emit_reports(cfg):
    reports = {"builder": [], "checkpoint": [], "action": []}
    if not cfg.get("profile_plan"):
        return reports
    threshold = cfg["plan_checkpoint_threshold"]
    for heading, label, key, sink in (
        (
            "BUILDER-LEVEL PLAN PROFILE (where the plan grows)",
            "BUILDER",
            "builder",
            "_plan_profile",
        ),
        (
            "CHECKPOINT-LEVEL PLAN PROFILE (plan truncated at each checkpoint)",
            "CHECKPOINT",
            "checkpoint",
            "_checkpoint_plan_profile",
        ),
        (
            "ACTION-LEVEL PLAN PROFILE (materialization sites)",
            "ACTION",
            "action",
            "_action_plan_profile",
        ),
    ):
        print(f"\n===== {heading} =====")
        reports[key] = plan_profile_report(
            cfg.get(sink, []), threshold, label=label
        )
    return reports


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def run_load_lt_footnote_effective_allocation_pct(
    spark,
    cfg: dict = None,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "Parquet",
    VolumePath: str = None,
    ExecutionID: str = None,
    call_from: str = None,
    verbose: bool = False,
    max_threads: int = None,
    MaxThreads: int = None,
    parallel_groups: str = "all",
    ParallelGroups: str = None,
    execution_profile: str = "low",
    ExecutionProfile: str = None,
    profile_plan: bool = False,
    ProfilePlan=None,
    plan_checkpoint_threshold: int = 30,
    PlanCheckpointThreshold: int = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    SqlShufflePartitions=None,
    **kwargs,
):
    """Production semantics with Checkpoint V2 and bounded parallel phases."""
    del kwargs
    global _LAST_RUN_PROFILE
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName
    t0 = time.time()
    timings = []
    parallel_activity = []
    enabled_groups = parse_enabled_groups(parallel_groups, ParallelGroups)
    profile_name = (
        _blank(ExecutionProfile) or _blank(execution_profile) or "low"
    )
    profile = resolve_execution_profile(profile_name)
    workers = normalize_workers(
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
    profile_enabled = _as_bool(
        ProfilePlan if ProfilePlan is not None else profile_plan
    )
    threshold = int(
        PlanCheckpointThreshold
        if PlanCheckpointThreshold is not None
        else plan_checkpoint_threshold
    )
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    save_return_value = None
    status = {
        "sp_name": "uspLoadLookThroughFootnoteEffectiveAllocationPercentage",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
        "skip_reason": None,
    }

    try:
        with _timed(timings, "S1 config and profile"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    entity_id=entity_id,
                    client_id=client_id,
                    tax_period_id=tax_period_id,
                    run_id=run_id,
                    catalog=catalog,
                    schema=schema,
                    call_from=call_from,
                )
            cfg = {**cfg, "_checkpoint_tables": []}
            if call_from is not None:
                cfg["call_from"] = call_from
            if ResultType is not None:
                cfg.setdefault("result_type", ResultType)
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID
            cfg.setdefault("_parquet_results", {})
            cfg.setdefault("_checkpoint_paths", [])
            cfg.setdefault("_checkpoint_v2_activity", [])
            cfg.setdefault("_plan_profile", [])
            cfg.setdefault("_checkpoint_plan_profile", [])
            cfg.setdefault("_action_plan_profile", [])
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
                    if _blank(explicit_checkpoint) is not None
                    else profile["checkpoint_mode"]
                ),
                CheckpointMode=CheckpointMode,
            )
            cfg.update(
                {
                    "profile_plan": profile_enabled,
                    "plan_checkpoint_threshold": threshold,
                    "checkpoint_mode": mode,
                    "max_threads": workers,
                    "execution_profile": profile_name,
                }
            )
            initialize_checkpoint_V2(cfg, mode)
            print(
                f"[outputV2] ExecutionProfile={profile_name} "
                f"CheckpointMode={mode} shuffle="
                f"{shuffle_override or profile['shuffle_partitions']} "
                f"MaxThreads={workers} "
                f"ProfilePlan={'on' if profile_enabled else 'off'}"
            )
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            _load_sp_config(spark, cfg)
            if (cfg.get("run_status") or "").upper() == "FAIL":
                logger.error("RunStatus=FAIL — aborting.")
                status["status"] = "SKIPPED"
                status["skip_reason"] = "run_status_fail"
                status["error"] = "RunStatus=FAIL"
                _v2("SKIPPED: RunStatus=FAIL")
                return status
            if (cfg.get("allocation_type_name") or "").lower() != (
                "pe book allocation"
            ):
                logger.info(
                    "AllocationTypeName != 'PE Book Allocation' — skipping."
                )
                status["status"] = "SKIPPED"
                status["skip_reason"] = "not_pe_book_allocation"
                _v2(
                    "SKIPPED: AllocationTypeName="
                    f"{cfg.get('allocation_type_name')!r}"
                )
                return status
            if not cfg.get("register_type_id"):
                logger.info("RegisterTypeID is NULL/0 — skipping.")
                status["status"] = "SKIPPED"
                status["skip_reason"] = "no_register_type_id"
                _v2("SKIPPED: RegisterTypeID is NULL/0")
                return status

        with _timed(timings, "S2 mappings and K1 gate"):
            mappings_df = load_mappings(spark, cfg)
            mappings_df = expand_parent_k1_mappings(spark, cfg, mappings_df)
            distinct_mappings_df = build_distinct_mappings(
                spark, cfg, mappings_df
            )
            has_k1 = not distinct_mappings_df.filter(
                F.col("SourceTypeID") == cfg["enu_k1_line_type_id"]
            ).limit(1).isEmpty()
            if not has_k1:
                logger.info(
                    "K1 gate: no K1 SourceTypeID in distinct mappings — "
                    "skipping §6–§16."
                )
                status["sections_completed"] = 5
                status["status"] = "OK_NO_K1"
                status["skip_reason"] = "no_k1_mappings"
                _v2("OK_NO_K1: no K1 SourceTypeID in distinct mappings")
                return status
            distinct_mappings_df = _checkpoint(
                spark, distinct_mappings_df, "distinct_mappings", cfg
            )

        with _timed(timings, "S3 yearly partners FEP and lt_output"):
            yearly_pair = build_yearly_effective_pct(
                spark, cfg, distinct_mappings_df
            )
            total_amount_yearly_df, tmp_line_amounts_df = yearly_pair
            del total_amount_yearly_df
            tmp_line_amounts_df = _checkpoint(
                spark, tmp_line_amounts_df, "yearly_line_amounts", cfg
            )
            partners_df = load_partners(spark, cfg)
            partners_df = _checkpoint(spark, partners_df, "partners", cfg)
            final_eff_pct_df = load_final_effective_percentages(spark, cfg)
            final_eff_pct_df = _checkpoint(spark, final_eff_pct_df, "fep", cfg)
            lt_output_df = build_lt_allocation_output(
                spark, cfg, distinct_mappings_df
            )
            lt_output_df = _checkpoint(
                spark, lt_output_df, "lt_output", cfg
            )

        with _timed(timings, "S4 cost book and temp input"):
            cost_pct_df = build_cost_effective_pct(
                spark, cfg, lt_output_df, final_eff_pct_df
            )
            book_pct_df = build_book_effective_pct(
                spark, cfg, lt_output_df, final_eff_pct_df
            )
            temp_final_eff_pct_df = cost_pct_df.unionByName(book_pct_df)
            temp_final_eff_pct_df = _checkpoint(
                spark, temp_final_eff_pct_df, "temp_final_eff_pct", cfg
            )
            temp_alloc_input_df = load_temp_allocation_input(
                spark, cfg, distinct_mappings_df
            )
            temp_alloc_input_df = _checkpoint(
                spark, temp_alloc_input_df, "temp_alloc_input", cfg
            )
            if temp_alloc_input_df.isEmpty():
                logger.info("No allocation input rows — exiting.")
                status["status"] = "SKIPPED"
                status["skip_reason"] = "empty_temp_allocation_input"
                _v2("SKIPPED: temp allocation input is empty")
                return status

        with _timed(timings, "S5 classify k1 and allocation output"):
            single_percent_df = build_single_multi_alloc_type(
                spark,
                cfg,
                temp_alloc_input_df,
                distinct_mappings_df,
                temp_final_eff_pct_df,
            )
            single_percent_df = _checkpoint(
                spark, single_percent_df, "single_percent", cfg
            )
            total_amount_pct_df, total_amounts_df = build_k1_data_amounts(
                spark,
                cfg,
                lt_output_df,
                distinct_mappings_df,
                single_percent_df,
            )
            del total_amounts_df
            total_amount_pct_df = _checkpoint(
                spark, total_amount_pct_df, "k1_amount_pct", cfg
            )
            final_pct_df = build_final_effective_pct(
                spark,
                cfg,
                single_percent_df,
                temp_alloc_input_df,
                total_amount_pct_df,
                tmp_line_amounts_df,
            )
            final_pct_df = _checkpoint(
                spark, final_pct_df, "final_pct", cfg
            )
            alloc_output_df, grouped_output_df = build_allocation_output(
                spark, cfg, temp_alloc_input_df, final_pct_df, partners_df
            )
            alloc_output_df = _checkpoint(
                spark, alloc_output_df, "alloc_output", cfg
            )
            grouped_output_df = _checkpoint(
                spark, grouped_output_df, "grouped_output", cfg
            )

        with _timed(timings, "S6 write output then update input"):
            save_return_value = profile_action(
                "flush_result_tables",
                alloc_output_df,
                lambda: flush_result_tables(
                    spark,
                    cfg,
                    alloc_output_df,
                    grouped_output_df,
                    workers,
                    parallel_activity,
                    enabled_groups,
                ),
                cfg,
            )
            status["sections_completed"] = 16
            status["storer_return"] = save_return_value
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error(f"[FAIL] {exc}", exc_info=True)
        raise
    finally:
        elapsed = time.time() - t0
        reports = _emit_reports(cfg) if isinstance(cfg, dict) else {}
        status["elapsed_seconds"] = round(elapsed, 1)
        _LAST_RUN_PROFILE = {
            "timings": list(timings),
            "parallel_activity": list(parallel_activity),
            "checkpoint_activity": (
                list(cfg.get("_checkpoint_v2_activity", []))
                if isinstance(cfg, dict)
                else []
            ),
            "plan_profile": reports.get("builder", []),
            "checkpoint_plan_profile": reports.get("checkpoint", []),
            "action_profile": reports.get("action", []),
            "elapsed_seconds": round(elapsed, 1),
            "checkpoint_mode": mode,
            "max_threads": workers,
            "execution_profile": profile_name,
        }
        if isinstance(cfg, dict):
            drop_checkpoints_V2(spark, cfg)
        _v2(
            f"DONE status={status.get('status')} "
            f"skip_reason={status.get('skip_reason')} "
            f"sections={status.get('sections_completed')} "
            f"elapsed={status.get('elapsed_seconds')}s "
            f"profile={profile_name} checkpoint_mode={mode}"
        )

    logger.info(
        f"[DONE] run_load_lt_footnote_effective_allocation_pct | "
        f"{status['elapsed_seconds']}s | RunID={cfg['run_id']} "
        f"EntityID={cfg['entity_id']}"
    )
    return status


__all__ = [
    "get_last_run_profile",
    "run_load_lt_footnote_effective_allocation_pct",
]
