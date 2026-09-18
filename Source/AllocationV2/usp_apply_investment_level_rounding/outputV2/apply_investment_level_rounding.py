"""Optimized, parity-oriented candidate for investment-level rounding."""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import get_logger

from .config_service import load_sp_config
from .input_service import build_allocation_input, build_ubti_passive_input
from .lookthrough_service import (
    build_lookthrough_output_entity_callfrom,
    build_lookthrough_output_entity_no_callfrom,
    build_lookthrough_output_inv_level_callfrom,
    build_lookthrough_output_inv_level_no_callfrom,
)
from .parallel_helpers import isolated_cfg, normalize_workers, run_parallel
from .parent import output_module
from .partner_service import (
    build_highest_percent_partner,
    build_nocost_partner,
    build_partner_snapshots,
)
from .plan_profiler import plan_profile_report, profile_action, track_plan

logger = get_logger("apply_investment_level_rounding.outputV2")

_aggregation = output_module("aggregation_service")
_rounding = output_module("rounding_service")
_write = output_module("write_service")

build_temp_allocation_output = _aggregation.build_temp_allocation_output
compute_rounding_diff = _aggregation.compute_rounding_diff
compute_max_allocation_type = _aggregation.compute_max_allocation_type
apply_rounding_override = _rounding.apply_rounding_override
apply_rounding_plugged_to_gp = _rounding.apply_rounding_plugged_to_gp
apply_rounding_highest_percent = _rounding.apply_rounding_highest_percent
apply_rounding_highest_amount = _rounding.apply_rounding_highest_amount
apply_rounding_none = _rounding.apply_rounding_none
apply_book_k1_not_rounded_passthrough = (
    _write.apply_book_k1_not_rounded_passthrough
)
write_final_summaries = _write.write_final_summaries
write_allocation_summaries = _write.write_allocation_summaries
update_is_rounded_flag = _write.update_is_rounded_flag

for _builder_name in (
    "build_lookthrough_output_inv_level_callfrom",
    "build_lookthrough_output_inv_level_no_callfrom",
    "build_lookthrough_output_entity_callfrom",
    "build_lookthrough_output_entity_no_callfrom",
    "build_allocation_input",
    "build_ubti_passive_input",
    "build_temp_allocation_output",
    "compute_rounding_diff",
    "compute_max_allocation_type",
    "build_partner_snapshots",
    "build_highest_percent_partner",
    "build_nocost_partner",
    "apply_rounding_override",
    "apply_rounding_plugged_to_gp",
    "apply_rounding_highest_percent",
    "apply_rounding_highest_amount",
    "apply_rounding_none",
    "apply_book_k1_not_rounded_passthrough",
):
    globals()[_builder_name] = track_plan(globals()[_builder_name])

_LAST_RUN_PROFILE = {}


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "on", "true", "yes"}
    return bool(value)


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


def _emit_plan_reports(cfg):
    reports = {"builder": [], "checkpoint": [], "action": []}
    if not isinstance(cfg, dict) or not cfg.get("profile_plan"):
        return reports
    threshold = cfg["plan_checkpoint_threshold"]
    for heading, label, key, source_key in (
        (
            "BUILDER-LEVEL PLAN PROFILE (where the plan grows)",
            "BUILDER",
            "builder",
            "_plan_profile",
        ),
        (
            "CHECKPOINT-LEVEL PLAN PROFILE "
            "(plan truncated at each checkpoint)",
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
            cfg.get(source_key, []), threshold, label=label
        )
    return reports


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def _load_lookthrough(spark, cfg):
    is_inv_level = cfg.get("is_investment_level_rounding") == "C"
    has_call_from = bool(cfg.get("call_from") or "")
    if is_inv_level and has_call_from:
        return build_lookthrough_output_inv_level_callfrom(spark, cfg)
    elif is_inv_level:
        return build_lookthrough_output_inv_level_no_callfrom(spark, cfg)
    elif has_call_from:
        return build_lookthrough_output_entity_callfrom(spark, cfg)
    else:
        return build_lookthrough_output_entity_no_callfrom(spark, cfg)


def _apply_rounding(
    spark,
    cfg,
    lookthrough_output_df,
    temp_alloc_output_df,
    rounded_diff_df,
    max_alloc_type_df,
    partner_snapshot_df,
    rounding_override_df,
):
    """Execute exactly one production rounding branch."""
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
    logic = cfg.get("rounding_logic")
    if cfg.get("has_rounding_override") and cfg.get(
        "rounding_override_import", False
    ):
        return apply_rounding_override(
            spark,
            cfg,
            temp_alloc_output_df,
            rounded_diff_df,
            max_alloc_type_df,
            rounding_override_df,
            alloc_output_detail_df,
        )
    elif logic == "Plugged to GP":
        return apply_rounding_plugged_to_gp(
            spark,
            cfg,
            temp_alloc_output_df,
            rounded_diff_df,
            max_alloc_type_df,
            partner_snapshot_df,
        )
    elif logic == "Plugged to Highest Allocation Percent":
        highest_pct_partner_df = build_highest_percent_partner(
            spark, cfg, lookthrough_output_df
        )
        (
            nocost_df,
            cfg["rounding_partner_number"],
            cfg["rounding_share_class"],
        ) = build_nocost_partner(
            spark,
            cfg,
            lookthrough_output_df,
            highest_pct_partner_df,
            partner_snapshot_df,
        )
        return apply_rounding_highest_percent(
            spark,
            cfg,
            temp_alloc_output_df,
            rounded_diff_df,
            max_alloc_type_df,
            highest_pct_partner_df,
            nocost_df,
            partner_snapshot_df,
        )
    elif logic == "Plugged to Highest Allocation Amount":
        return apply_rounding_highest_amount(
            spark,
            cfg,
            temp_alloc_output_df,
            rounded_diff_df,
            max_alloc_type_df,
            partner_snapshot_df,
        )
    elif logic == "None":
        return apply_rounding_none(
            spark, cfg, temp_alloc_output_df, max_alloc_type_df
        )
    else:
        raise ValueError(f"Unsupported rounding logic: {logic!r}")


def apply_investment_level_rounding(
    spark: SparkSession,
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
    """Apply investment-level rounding with bounded preparation parallelism."""
    del kwargs
    global _LAST_RUN_PROFILE
    started = time.perf_counter()
    timings = []
    parallel_activity = []
    reports = {}
    return_value_summaries = None
    return_value_alloc = None
    workers = normalize_workers(max_threads, MaxThreads)
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
        "sp_name": "uspApplyInvestmentLevelRounding",
        "run_id": RunID,
        "entity_id": EntityID,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    try:
        with _timed(timings, "S1 config"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    run_id=RunID,
                    entity_id=EntityID,
                    client_id=ClientID,
                    tax_period_id=TaxPeriodID,
                    catalog=CatalogName,
                    schema=SchemaName,
                    call_from=CallFrom,
                )
            else:
                cfg = dict(cfg)
                if CallFrom is not None:
                    cfg["call_from"] = CallFrom

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
                "_checkpoint_plan_profile": [],
                "_action_plan_profile": [],
                "profile_plan": profile_enabled,
                "plan_checkpoint_threshold": threshold,
                "checkpoint_mode": mode,
                "max_threads": workers,
            }
            cfg.setdefault("result_type", ResultType)
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID
            initialize_checkpoint_V2(cfg, mode)
            print(
                f"[CHECKPOINT_V2] mode={mode}; "
                f"[outputV2] MaxThreads={workers} "
                f"ProfilePlan={'on' if profile_enabled else 'off'}"
            )
            cfg = load_sp_config(spark, cfg)
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            if cfg.get("run_status") == "FAIL":
                status["status"] = "SKIPPED"
                status["error"] = "RunStatus=FAIL"
                return status

        with _timed(timings, "S2-S6 lookthrough"):
            lookthrough_output_df, lookthrough_input_df = _load_lookthrough(
                spark, cfg
            )
            not_rounded_lines_df = cfg.get("not_rounded_lines_df")
            status["sections_completed"] = 6

        with _timed(timings, "S7-S12 independent preparation"):
            allocation_cfg = isolated_cfg(cfg)
            temp_cfg = isolated_cfg(cfg)
            max_cfg = isolated_cfg(cfg)
            partner_cfg = isolated_cfg(cfg)

            def allocation_task():
                allocation = build_allocation_input(
                    spark,
                    allocation_cfg,
                    lookthrough_input_df,
                    not_rounded_lines_df,
                )
                return build_ubti_passive_input(
                    spark, allocation_cfg, allocation
                )

            def partner_task():
                partner, override = build_partner_snapshots(
                    spark, partner_cfg
                )
                return (
                    partner,
                    override,
                    partner_cfg.get("has_rounding_override", False),
                )

            (
                allocation_input_df,
                temp_alloc_output_df,
                max_alloc_type_df,
                partner_result,
            ) = run_parallel(
                [
                    ("allocation_input", allocation_task),
                    (
                        "temp_alloc_output",
                        lambda: build_temp_allocation_output(
                            spark,
                            temp_cfg,
                            lookthrough_output_df,
                            not_rounded_lines_df,
                        ),
                    ),
                    (
                        "max_allocation_type",
                        lambda: compute_max_allocation_type(
                            spark, max_cfg, lookthrough_output_df
                        ),
                    ),
                    ("partner_snapshots", partner_task),
                ],
                workers,
                parallel_activity,
                "independent_preparation",
            )
            (
                partner_snapshot_df,
                rounding_override_df,
                cfg["has_rounding_override"],
            ) = partner_result
            cfg["partner_snapshot_df"] = partner_snapshot_df
            status["sections_completed"] = 12

        with _timed(timings, "S10 rounding difference"):
            rounded_diff_df = compute_rounding_diff(
                spark, cfg, temp_alloc_output_df, allocation_input_df
            )

        # Preserve both production lineage breaks and their order.
        with _timed(timings, "checkpoints"):
            temp_alloc_output_df = checkpoint(
                spark, temp_alloc_output_df, "temp_alloc_output", cfg
            )
            rounded_diff_df = checkpoint(
                spark, rounded_diff_df, "rounded_diff", cfg
            )

        with _timed(timings, "S13-S19 selected rounding branch"):
            k1_summary_df, adjustment_summary_df = _apply_rounding(
                spark,
                cfg,
                lookthrough_output_df,
                temp_alloc_output_df,
                rounded_diff_df,
                max_alloc_type_df,
                partner_snapshot_df,
                rounding_override_df,
            )
            status["sections_completed"] = 19

        with _timed(timings, "S20 BookK1 passthrough"):
            if cfg.get("book_k1_adjustment_enabled", False):
                passthrough_df = apply_book_k1_not_rounded_passthrough(
                    spark,
                    cfg,
                    lookthrough_output_df,
                    not_rounded_lines_df,
                )
                if k1_summary_df is not None and passthrough_df is not None:
                    k1_summary_df = k1_summary_df.unionByName(
                        passthrough_df, allowMissingColumns=True
                    )
            status["sections_completed"] = 20

        # These mutate persistent state and intentionally remain ordered.
        with _timed(timings, "S21 final summary writes"):
            (
                k1_write_df,
                ubti_write_df,
                adj_write_df,
                return_value_summaries,
            ) = profile_action(
                "write_final_summaries",
                k1_summary_df,
                lambda: write_final_summaries(
                    spark, cfg, k1_summary_df, adjustment_summary_df
                ),
                cfg,
            )
            status["sections_completed"] = 21

        with _timed(timings, "S22 IsRounded mutation"):
            update_is_rounded_flag(spark, cfg)
            status["sections_completed"] = 22

        with _timed(timings, "S23 allocation summary writes"):
            return_value_alloc = profile_action(
                "write_allocation_summaries",
                k1_write_df,
                lambda: write_allocation_summaries(
                    spark,
                    cfg,
                    k1_write_df,
                    ubti_write_df,
                    adj_write_df,
                ),
                cfg,
            )
            status["sections_completed"] = 23
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.exception("Failed: %s", exc)
        raise
    finally:
        status["elapsed_seconds"] = round(
            time.perf_counter() - started, 1
        )
        reports = _emit_plan_reports(cfg)
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
            "elapsed_seconds": status["elapsed_seconds"],
            "checkpoint_mode": (
                cfg.get("checkpoint_mode") if isinstance(cfg, dict) else None
            ),
            "max_threads": workers,
        }
        logger.info(
            "[COMPLETE] %s — %s in %.1fs, sections=%s/23",
            status["sp_name"],
            status["status"],
            status["elapsed_seconds"],
            status["sections_completed"],
        )

    combined = {}
    for value in (return_value_summaries, return_value_alloc):
        if value and isinstance(value, str):
            try:
                combined.update(json.loads(value))
            except (json.JSONDecodeError, TypeError):
                pass
    if combined:
        result_json = json.dumps(combined)
        print(f"[PARQUET] Return JSON: {result_json}")
        return result_json
    return status


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    result = apply_investment_level_rounding(
        spark,
        RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
        EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
        ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
        TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
        CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
        SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
    )
    try:
        dbutils.notebook.exit(  # noqa: F821
            json.dumps(result) if not isinstance(result, str) else result
        )
    except Exception:
        print(
            json.dumps(result, indent=2)
            if not isinstance(result, str)
            else result
        )


__all__ = [
    "apply_investment_level_rounding",
    "checkpoint",
    "drop_checkpoints_V2",
    "get_last_run_profile",
]
