"""Parity-first outputV2 orchestrator for SM look-through effective allocation %."""

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
from Common_V2.core.helpers import read_table

from .parallel_helpers import (
    normalize_workers,
    parse_enabled_groups,
    run_parallel,
)
from .parent import output_module
from .plan_profiler import (
    plan_profile_report,
    profile_action,
    track_checkpoint_plan,
    track_plan,
)
from .write_helpers import flush_result_tables

_config = output_module("services.config_service")
_mapping = output_module("services.mapping_service")
_flowup = output_module("services.flowup_service")
_amount = output_module("services.amount_service")
_eff = output_module("services.effective_pct_service")

logger = logging.getLogger(
    "usp_sm_load_lookthrough_effective_allocation_pct"
)

load_sp_config = _config.load_sp_config
validate_allocation_type = _config.validate_allocation_type
build_mapping_data = track_plan(_mapping.build_mapping_data)
build_flowup_k1_amounts = track_plan(_flowup.build_flowup_k1_amounts)
compute_flowup_mapped_amounts = track_plan(
    _flowup.compute_flowup_mapped_amounts
)
compute_flowup_effective_amounts = track_plan(
    _flowup.compute_flowup_effective_amounts
)
write_flowup_allocation_output = _flowup.write_flowup_allocation_output
_build_sm_lt_input_and_state_lines = track_plan(
    _amount._build_sm_lt_input_and_state_lines
)
build_k1_amounts = track_plan(_amount.build_k1_amounts)
build_ubti_amounts = track_plan(_amount.build_ubti_amounts)
compute_state_mapped_amounts = track_plan(
    _amount.compute_state_mapped_amounts
)
compute_effective_percentages = track_plan(_eff.compute_effective_percentages)
apply_exclude_from_residual = track_plan(_eff.apply_exclude_from_residual)
apply_pe_book_unmapped_lines = track_plan(_eff.apply_pe_book_unmapped_lines)

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


def _v2(msg):
    print(f"[outputV2] {msg}", flush=True)


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


def _checkpoint(spark, df, name, cfg):
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


@contextmanager
def use_v2_amount_checkpoint():
    original = getattr(_amount, "checkpoint", None)
    _amount.checkpoint = _checkpoint
    try:
        yield
    finally:
        if original is not None:
            _amount.checkpoint = original


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


def run_sm_load_lt_effective_alloc_pct(
    spark,
    cfg=None,
    verbose=False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "Parquet",
    VolumePath: str = None,
    ExecutionID: str = None,
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
    del kwargs
    global _LAST_RUN_PROFILE
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName
    result_type = ResultType
    volume_path = VolumePath
    execution_id = ExecutionID
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
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    rows = None
    status = {
        "sp_name": "usp_SM_LoadLookThroughEffectiveAllocationPercentage",
        "run_id": None,
        "entity_id": None,
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
                    run_id=run_id,
                    entity_id=entity_id,
                    client_id=client_id,
                    tax_period_id=tax_period_id,
                    catalog=catalog,
                    schema=schema,
                )
            cfg = {**cfg, "_checkpoint_tables": []}
            if result_type is not None:
                cfg.setdefault("result_type", result_type)
            if volume_path is not None:
                cfg["volume_path"] = volume_path
            if execution_id is not None:
                cfg["execution_id"] = execution_id
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
            _v2(
                f"ExecutionProfile={profile_name} CheckpointMode={mode} "
                f"shuffle={shuffle_override or profile['shuffle_partitions']} "
                f"MaxThreads={workers} "
                f"ProfilePlan={'on' if profile_enabled else 'off'}"
            )
            cfg = load_sp_config(spark, cfg)
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            if not validate_allocation_type(spark, cfg):
                status["status"] = "FAIL"
                status["error"] = "Allocation logic not selected for the entity."
                status["skip_reason"] = "allocation_type_invalid"
                status["elapsed_seconds"] = round(time.time() - t0, 1)
                return status

        with use_v2_amount_checkpoint():
            with _timed(timings, "S2 mappings and flowup"):
                mappings = build_mapping_data(spark, cfg)
                mappings["distinct_mappings"] = _checkpoint(
                    spark,
                    mappings["distinct_mappings"],
                    "distinct_mappings",
                    cfg,
                )
                mappings["distinct_ubti_mappings"] = _checkpoint(
                    spark,
                    mappings["distinct_ubti_mappings"],
                    "distinct_ubti_mappings",
                    cfg,
                )
                is_flowup = cfg.get("is_sidepocket_flowup_partner", False)
                sm_fp_lt = read_table(
                    spark, "SM_FlowUpPartnerLookThroughAllocationInput", cfg
                )
                has_flowup_input = (
                    len(
                        sm_fp_lt.filter(
                            F.col("RunID") == cfg["run_id"]
                        ).head(1)
                    )
                    > 0
                )
                fp_data = None
                if is_flowup or has_flowup_input:
                    fp_data = build_flowup_k1_amounts(spark, cfg)
                    fp_total_amounts = compute_flowup_mapped_amounts(
                        spark, cfg, fp_data, mappings
                    )
                    fp_effective = compute_flowup_effective_amounts(
                        spark,
                        cfg,
                        fp_total_amounts,
                        mappings["state_mapped_lines"],
                    )
                    write_flowup_allocation_output(
                        spark, cfg, fp_effective
                    )
                else:
                    fp_data = build_flowup_k1_amounts(spark, cfg)
                    logger.info(
                        "[SKIP] Flow-up partner pipeline: condition not met"
                    )

            with _timed(timings, "S3 temp input and amounts"):
                (
                    sm_lt_input,
                    state_lines,
                    fed_lines,
                    non_sp_fp,
                    pruned_dm,
                    pruned_ubti_dm,
                ) = _build_sm_lt_input_and_state_lines(spark, cfg, mappings)
                sm_lt_input = _checkpoint(
                    spark, sm_lt_input, "temp_alloc_input", cfg
                )
                k1_pair, ubti_pair = run_parallel(
                    [
                        (
                            "k1_amounts",
                            lambda: build_k1_amounts(
                                spark,
                                cfg,
                                mappings,
                                fp_data["k1_sidepocket"],
                                fp_data["k1_sidepocket_res"],
                                fed_lines,
                                non_sp_fp,
                            ),
                        ),
                        (
                            "ubti_amounts",
                            lambda: build_ubti_amounts(
                                spark, cfg, fed_lines, non_sp_fp
                            ),
                        ),
                    ],
                    workers,
                    parallel_activity,
                    "independent_amounts",
                    enabled_groups,
                )
                partner_alloc, total_input = k1_pair
                partner_alloc_ubti, total_ubti_input = ubti_pair
                partner_alloc = _checkpoint(
                    spark, partner_alloc, "alloc_pass1", cfg
                )
                partner_alloc_ubti = _checkpoint(
                    spark, partner_alloc_ubti, "alloc_pass2", cfg
                )
                total_amounts = compute_state_mapped_amounts(
                    spark,
                    cfg,
                    pruned_dm,
                    pruned_ubti_dm,
                    partner_alloc,
                    total_input,
                    partner_alloc_ubti,
                    total_ubti_input,
                )
                total_amounts = _checkpoint(
                    spark, total_amounts, "alloc_pass3", cfg
                )
                effective_amounts, temp_effective = (
                    compute_effective_percentages(
                        spark, cfg, total_amounts, sm_lt_input
                    )
                )
                effective_amounts = apply_exclude_from_residual(
                    spark, cfg, effective_amounts, total_amounts
                )
                effective_amounts = apply_pe_book_unmapped_lines(
                    spark,
                    cfg,
                    effective_amounts,
                    temp_effective,
                    sm_lt_input,
                    mappings,
                )
                effective_amounts = _checkpoint(
                    spark, effective_amounts, "alloc_output", cfg
                )

            with _timed(timings, "S4 sequential writes"):
                partner_snap = read_table(spark, "Partner_Snapshot", cfg)
                partner_snapshot = partner_snap.filter(
                    F.coalesce(
                        F.col("WorkFlowID"), F.col("Transactionid")
                    )
                    == cfg["partner_txn_or_wf_id"]
                )
                rows = profile_action(
                    "flush_result_tables",
                    effective_amounts,
                    lambda: flush_result_tables(
                        spark, cfg, effective_amounts, partner_snapshot
                    ),
                    cfg,
                )
                status["sections_completed"] = 13
                status["status"] = "SUCCESS"
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error(f"[FAIL] {exc}", exc_info=True)
        raise
    finally:
        elapsed = round(time.time() - t0, 1)
        reports = _emit_reports(cfg) if isinstance(cfg, dict) else {}
        status["elapsed_seconds"] = elapsed
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
            "elapsed_seconds": elapsed,
            "checkpoint_mode": mode,
            "max_threads": workers,
            "execution_profile": profile_name,
        }
        if isinstance(cfg, dict):
            drop_checkpoints_V2(spark, cfg)
        _v2(
            f"DONE status={status.get('status')} "
            f"skip_reason={status.get('skip_reason')} elapsed={elapsed}s"
        )

    if rows and isinstance(rows, str):
        print(f"[PARQUET] Return JSON: {rows}")
        return rows
    return status


__all__ = ["get_last_run_profile", "run_sm_load_lt_effective_alloc_pct"]
