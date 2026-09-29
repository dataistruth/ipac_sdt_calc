"""Parity-first outputV2 orchestrator for investment-level rounding."""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config
from Common_V2.core.execution_profiles import resolve_execution_profile

from .parallel_helpers import (
    isolated_cfg,
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
from .write_helpers import flush_post_summary_writes

_config = output_module("config_service")
_lt = output_module("lookthrough_service")
_input = output_module("input_service")
_agg = output_module("aggregation_service")
_partner = output_module("partner_service")
_round = output_module("rounding_service")
_write = output_module("write_service")

logger = logging.getLogger("apply_investment_level_rounding")

load_sp_config = _config.load_sp_config
build_lookthrough_output_inv_level_callfrom = track_plan(
    _lt.build_lookthrough_output_inv_level_callfrom
)
build_lookthrough_output_inv_level_no_callfrom = track_plan(
    _lt.build_lookthrough_output_inv_level_no_callfrom
)
build_lookthrough_output_entity_callfrom = track_plan(
    _lt.build_lookthrough_output_entity_callfrom
)
build_lookthrough_output_entity_no_callfrom = track_plan(
    _lt.build_lookthrough_output_entity_no_callfrom
)
build_allocation_input = track_plan(_input.build_allocation_input)
build_ubti_passive_input = track_plan(_input.build_ubti_passive_input)
build_temp_allocation_output = track_plan(_agg.build_temp_allocation_output)
compute_rounding_diff = track_plan(_agg.compute_rounding_diff)
compute_max_allocation_type = track_plan(_agg.compute_max_allocation_type)
build_partner_snapshots = track_plan(_partner.build_partner_snapshots)
build_highest_percent_partner = track_plan(
    _partner.build_highest_percent_partner
)
build_nocost_partner = track_plan(_partner.build_nocost_partner)
apply_rounding_override = track_plan(_round.apply_rounding_override)
apply_rounding_plugged_to_gp = track_plan(_round.apply_rounding_plugged_to_gp)
apply_rounding_highest_percent = track_plan(
    _round.apply_rounding_highest_percent
)
apply_rounding_highest_amount = track_plan(
    _round.apply_rounding_highest_amount
)
apply_rounding_none = track_plan(_round.apply_rounding_none)
apply_book_k1_not_rounded_passthrough = track_plan(
    _write.apply_book_k1_not_rounded_passthrough
)
write_final_summaries = _write.write_final_summaries

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


def _load_lookthrough(spark, cfg):
    is_inv_level = cfg.get("is_investment_level_rounding") == "C"
    has_call_from = (cfg.get("call_from") or "") != ""
    if is_inv_level and has_call_from:
        return build_lookthrough_output_inv_level_callfrom(spark, cfg)
    if is_inv_level:
        return build_lookthrough_output_inv_level_no_callfrom(spark, cfg)
    if has_call_from:
        return build_lookthrough_output_entity_callfrom(spark, cfg)
    return build_lookthrough_output_entity_no_callfrom(spark, cfg)


def apply_investment_level_rounding(
    spark,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CallFrom: str = None,
    CatalogName: str = "dev7",
    SchemaName: str = "iPC_2025_dev7_15349",
    cfg: dict = None,
    verbose: bool = False,
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
    """Production semantics with Checkpoint V2 and bounded parallel phases."""
    del kwargs
    global _LAST_RUN_PROFILE
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    call_from = CallFrom
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
    return_value_summaries = None
    return_value_alloc = None
    status = {
        "sp_name": "uspApplyInvestmentLevelRounding",
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
                    run_id=run_id,
                    entity_id=entity_id,
                    client_id=client_id,
                    tax_period_id=tax_period_id,
                    catalog=catalog,
                    schema=schema,
                    call_from=call_from,
                )
            elif call_from is not None:
                cfg["call_from"] = call_from
            cfg = {**cfg, "_checkpoint_tables": []}
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
            if cfg.get("run_status") == "FAIL":
                status["status"] = "SKIPPED"
                status["error"] = "RunStatus=FAIL"
                status["skip_reason"] = "run_status_fail"
                return status

        not_rounded_lines_df = cfg.get("not_rounded_lines_df")

        with _timed(timings, "S2 independent_early"):
            partner_cfg = isolated_cfg(cfg)

            def _lookthrough_task():
                return _load_lookthrough(spark, cfg)

            def _partners_task():
                return build_partner_snapshots(spark, partner_cfg)

            lookthrough_pair, partner_pair = run_parallel(
                [
                    ("lookthrough", _lookthrough_task),
                    ("partner_snapshots", _partners_task),
                ],
                workers,
                parallel_activity,
                "independent_early",
                enabled_groups,
            )
            lookthrough_output_df, lookthrough_input_df = lookthrough_pair
            partner_snapshot_df, rounding_override_df = partner_pair
            cfg["partner_snapshot_df"] = partner_cfg.get(
                "partner_snapshot_df"
            )
            cfg["has_rounding_override"] = partner_cfg.get(
                "has_rounding_override", False
            )
            lookthrough_output_df = _checkpoint(
                spark, lookthrough_output_df, "lookthrough_output", cfg
            )
            lookthrough_input_df = _checkpoint(
                spark, lookthrough_input_df, "lookthrough_input", cfg
            )
            partner_snapshot_df = _checkpoint(
                spark, partner_snapshot_df, "partners", cfg
            )
            status["sections_completed"] = 12

        with _timed(timings, "S3 independent_builders"):
            def _alloc_input_task():
                return build_allocation_input(
                    spark, cfg, lookthrough_input_df, not_rounded_lines_df
                )

            def _temp_out_task():
                return build_temp_allocation_output(
                    spark, cfg, lookthrough_output_df, not_rounded_lines_df
                )

            def _max_type_task():
                return compute_max_allocation_type(
                    spark, cfg, lookthrough_output_df
                )

            allocation_input_df, temp_alloc_output_df, max_alloc_type_df = (
                run_parallel(
                    [
                        ("allocation_input", _alloc_input_task),
                        ("temp_alloc_output", _temp_out_task),
                        ("max_alloc_type", _max_type_task),
                    ],
                    workers,
                    parallel_activity,
                    "independent_builders",
                    enabled_groups,
                )
            )
            status["sections_completed"] = 11

        with _timed(timings, "S4 ubti rounding checkpoints"):
            allocation_input_df = build_ubti_passive_input(
                spark, cfg, allocation_input_df
            )
            allocation_input_df = _checkpoint(
                spark, allocation_input_df, "temp_alloc_input", cfg
            )
            rounded_diff_df = compute_rounding_diff(
                spark, cfg, temp_alloc_output_df, allocation_input_df
            )
            temp_alloc_output_df = _checkpoint(
                spark, temp_alloc_output_df, "temp_alloc_output", cfg
            )
            rounded_diff_df = _checkpoint(
                spark, rounded_diff_df, "rounded_diff", cfg
            )
            max_alloc_type_df = _checkpoint(
                spark, max_alloc_type_df, "max_alloc_type", cfg
            )
            status["sections_completed"] = 12

        with _timed(timings, "S5 rounding branch"):
            rounding_logic = cfg.get("rounding_logic")
            rounding_override_import = cfg.get(
                "rounding_override_import", False
            )
            has_rounding_override = cfg.get("has_rounding_override", False)
            k1_summary_df = None
            adjustment_summary_df = None
            alloc_output_detail_df = temp_alloc_output_df.select(
                "EntityID",
                "LineTypeID",
                "LineID",
                "ParentEntityID",
                "TrackingKey",
                "SuperParentEntityID",
                "AdjustmentTypeID",
                "Tag",
                "OriginalParentEntityID",
                "QuickLinkID",
            ).distinct()
            if has_rounding_override and rounding_override_import:
                k1_summary_df, adjustment_summary_df = apply_rounding_override(
                    spark,
                    cfg,
                    temp_alloc_output_df,
                    rounded_diff_df,
                    max_alloc_type_df,
                    rounding_override_df,
                    alloc_output_detail_df,
                )
            elif rounding_logic == "Plugged to GP":
                k1_summary_df, adjustment_summary_df = (
                    apply_rounding_plugged_to_gp(
                        spark,
                        cfg,
                        temp_alloc_output_df,
                        rounded_diff_df,
                        max_alloc_type_df,
                        partner_snapshot_df,
                    )
                )
            elif rounding_logic == "Plugged to Highest Allocation Percent":
                highest_pct_partner_df = build_highest_percent_partner(
                    spark, cfg, lookthrough_output_df
                )
                (
                    nocost_df,
                    rounding_partner_number,
                    rounding_share_class,
                ) = build_nocost_partner(
                    spark,
                    cfg,
                    lookthrough_output_df,
                    highest_pct_partner_df,
                    partner_snapshot_df,
                )
                cfg["rounding_partner_number"] = rounding_partner_number
                cfg["rounding_share_class"] = rounding_share_class
                k1_summary_df, adjustment_summary_df = (
                    apply_rounding_highest_percent(
                        spark,
                        cfg,
                        temp_alloc_output_df,
                        rounded_diff_df,
                        max_alloc_type_df,
                        highest_pct_partner_df,
                        nocost_df,
                        partner_snapshot_df,
                    )
                )
            elif rounding_logic == "Plugged to Highest Allocation Amount":
                k1_summary_df, adjustment_summary_df = (
                    apply_rounding_highest_amount(
                        spark,
                        cfg,
                        temp_alloc_output_df,
                        rounded_diff_df,
                        max_alloc_type_df,
                        partner_snapshot_df,
                    )
                )
            elif rounding_logic == "None":
                k1_summary_df, adjustment_summary_df = apply_rounding_none(
                    spark, cfg, temp_alloc_output_df, max_alloc_type_df
                )
            status["sections_completed"] = 19
            book_k1_passthrough_df = None
            if cfg.get("book_k1_adjustment_enabled", False):
                book_k1_passthrough_df = apply_book_k1_not_rounded_passthrough(
                    spark, cfg, lookthrough_output_df, not_rounded_lines_df
                )
                if (
                    k1_summary_df is not None
                    and book_k1_passthrough_df is not None
                ):
                    k1_summary_df = k1_summary_df.unionByName(
                        book_k1_passthrough_df, allowMissingColumns=True
                    )
            status["sections_completed"] = 20
            k1_summary_df = _checkpoint(
                spark, k1_summary_df, "k1_summary", cfg
            )

        with _timed(timings, "S6 writes"):
            k1_write_df, ubti_write_df, adj_write_df, return_value_summaries = (
                profile_action(
                    "write_final_summaries",
                    k1_summary_df,
                    lambda: write_final_summaries(
                        spark, cfg, k1_summary_df, adjustment_summary_df
                    ),
                    cfg,
                )
            )
            status["sections_completed"] = 21
            return_value_alloc = profile_action(
                "flush_post_summary_writes",
                k1_write_df,
                lambda: flush_post_summary_writes(
                    spark,
                    cfg,
                    k1_write_df,
                    ubti_write_df,
                    adj_write_df,
                    workers,
                    parallel_activity,
                    enabled_groups,
                ),
                cfg,
            )
            status["sections_completed"] = 23
            status["status"] = "SUCCESS"
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.exception(f"Failed: {exc}")
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

    combined = {}
    for rv in [return_value_summaries, return_value_alloc]:
        if rv and isinstance(rv, str):
            try:
                parsed = json.loads(rv)
                combined.update(parsed)
            except (json.JSONDecodeError, TypeError):
                pass
    if combined:
        result_json = json.dumps(combined)
        print(f"[PARQUET] Return JSON: {result_json}")
        return result_json
    return status


__all__ = ["apply_investment_level_rounding", "get_last_run_profile"]
