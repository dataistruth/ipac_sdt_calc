"""Parity-first outputV2 orchestrator for look-through allocation input."""

from __future__ import annotations

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

from .parallel_helpers import normalize_workers, run_parallel
from .parent import output_module
from .plan_profiler import plan_profile_report, profile_action, track_plan

_helpers = output_module("lt_helpers")
logger = _helpers.logger

_config = output_module("lt_config_service")
_k1 = output_module("lt_k1_service")
_flowup = output_module("lt_flowup_service")
_pfic = output_module("lt_pfic_service")
_conversion = output_module("lt_pfic_conversion_service")
_final = output_module("lt_finalization_service")

load_config = _config.load_config
load_workflows = _config.load_workflows
load_lower_tier_funds = _config.load_lower_tier_funds
build_fx_rates = _config.build_fx_rates
load_reclass_k1_data = _config.load_reclass_k1_data
build_k1_input = _k1.build_k1_input
build_adjustments_input = _k1.build_adjustments_input
build_lt_flowup_k1 = _k1.build_lt_flowup_k1
build_rounding_diff = _k1.build_rounding_diff
recompute_lt_input_from_lower_tier = _k1.recompute_lt_input_from_lower_tier
build_lt_flowup_adjustment = _flowup.build_lt_flowup_adjustment
build_lt_flowup_m1 = _flowup.build_lt_flowup_m1
build_pfic_elections = _pfic.build_pfic_elections
build_pfic_mapped_lines = _pfic.build_pfic_mapped_lines
build_pfic_conversion = _conversion.build_pfic_conversion
build_pfic_income_attributes = _conversion.build_pfic_income_attributes
build_box_jkl_input = _final.build_box_jkl_input
apply_master_feed_exclusion = _final.apply_master_feed_exclusion
apply_blocker_entity = _final.apply_blocker_entity
apply_tag_percentages = _final.apply_tag_percentages
apply_line_exclusions = _final.apply_line_exclusions
validate_k3_rules = _final.validate_k3_rules
write_final_output = _final.write_final_output

for _builder_name in (
    "load_workflows",
    "load_lower_tier_funds",
    "build_fx_rates",
    "load_reclass_k1_data",
    "build_k1_input",
    "build_adjustments_input",
    "build_lt_flowup_k1",
    "build_rounding_diff",
    "recompute_lt_input_from_lower_tier",
    "build_lt_flowup_adjustment",
    "build_lt_flowup_m1",
    "build_pfic_elections",
    "build_pfic_mapped_lines",
    "build_pfic_conversion",
    "build_pfic_income_attributes",
    "build_box_jkl_input",
    "apply_master_feed_exclusion",
    "apply_blocker_entity",
    "apply_tag_percentages",
    "apply_line_exclusions",
):
    globals()[_builder_name] = track_plan(globals()[_builder_name])

_LAST_RUN_PROFILE = {}


def _as_bool(value):
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


def _emit_reports(cfg):
    reports = {"builder": [], "checkpoint": [], "action": []}
    if not cfg.get("profile_plan"):
        return reports
    threshold = cfg["plan_checkpoint_threshold"]
    for heading, label, key, sink in (
        ("BUILDER-LEVEL PLAN PROFILE (where the plan grows)", "BUILDER", "builder", "_plan_profile"),
        ("CHECKPOINT-LEVEL PLAN PROFILE (plan truncated at each checkpoint)", "CHECKPOINT", "checkpoint", "_checkpoint_plan_profile"),
        ("ACTION-LEVEL PLAN PROFILE (materialization sites)", "ACTION", "action", "_action_plan_profile"),
    ):
        print(f"\n===== {heading} =====")
        reports[key] = plan_profile_report(
            cfg.get(sink, []), threshold, label=label
        )
    return reports


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def run_load_lookthrough_allocation_input(
    spark,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = "dev7",
    SchemaName: str = "iPC_2025_dev7_15349",
    cfg: dict = None,
    verbose: bool = False,
    ResultType: str = "deltalake",
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
    """Run production semantics with V2 checkpoints and bounded plan pools."""
    del kwargs
    global _LAST_RUN_PROFILE
    started = time.perf_counter()
    timings = []
    parallel_activity = []
    workers = normalize_workers(max_threads, MaxThreads)
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
        "sp_name": "uspLoadLookThroughAllocationInput",
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
            cfg.setdefault("_checkpoint_tables", [])
            load_config(spark, cfg)
            cfg.setdefault("result_type", ResultType)
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID
            cfg.update(
                {
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
            )
            initialize_checkpoint_V2(cfg, mode)
            print(
                f"[CHECKPOINT_V2] mode={mode}; [outputV2] "
                f"MaxThreads={workers} ProfilePlan={'on' if profile_enabled else 'off'}"
            )
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            if cfg["run_status"] == "FAIL":
                status["status"] = "SKIPPED"
                status["error"] = "RunStatus=FAIL"
                return status
            status["sections_completed"] = 1

        # Each task only builds and returns a DataFrame; none mutates cfg or writes.
        with _timed(timings, "S2-S4 independent loads"):
            (
                workflow_data,
                lower_tier_funds_df,
                reclass_k1_df,
            ) = run_parallel(
                [
                    ("load_workflows", lambda: load_workflows(spark, cfg)),
                    ("load_lower_tier_funds", lambda: load_lower_tier_funds(spark, cfg)),
                    ("load_reclass_k1_data", lambda: load_reclass_k1_data(spark, cfg)),
                ],
                workers,
                parallel_activity,
                "independent_early_loads",
            )
            k1_workflow_df, adjustment_workflow_df = workflow_data
            fx_rates_df = build_fx_rates(spark, cfg, k1_workflow_df)
            status["sections_completed"] = 4

        # These builders consume immutable inputs and return disjoint plans.
        with _timed(timings, "S5-S7 independent input builders"):
            k1_input_df, adj_input_df, lower_tier_amount_df = run_parallel(
                [
                    (
                        "build_k1_input",
                        lambda: build_k1_input(
                            spark, cfg, k1_workflow_df, fx_rates_df
                        ),
                    ),
                    (
                        "build_adjustments_input",
                        lambda: build_adjustments_input(
                            spark, cfg, adjustment_workflow_df, fx_rates_df
                        ),
                    ),
                    (
                        "build_lt_flowup_k1",
                        lambda: build_lt_flowup_k1(spark, cfg, reclass_k1_df),
                    ),
                ],
                workers,
                parallel_activity,
                "independent_input_builders",
            )
            status["sections_completed"] = 7

        # Shared alloc_input, PFIC, validation, and all RunID mutations stay ordered.
        with _timed(timings, "S8-S10 sequential union pipeline"):
            lower_tier_amount_df = build_rounding_diff(
                spark, cfg, lower_tier_amount_df
            )
            lt_k1_input_df = recompute_lt_input_from_lower_tier(
                cfg, lower_tier_amount_df
            )
            alloc_input_df = k1_input_df
            if adj_input_df is not None:
                alloc_input_df = alloc_input_df.unionByName(
                    adj_input_df, allowMissingColumns=True
                )
            alloc_input_df = alloc_input_df.unionByName(
                lt_k1_input_df, allowMissingColumns=True
            )
            lt_adj_input_df = build_lt_flowup_adjustment(
                spark, cfg, lower_tier_funds_df
            )
            if lt_adj_input_df is not None:
                alloc_input_df = alloc_input_df.unionByName(
                    lt_adj_input_df, allowMissingColumns=True
                )
            lt_m1_input_df = build_lt_flowup_m1(
                spark, cfg, lower_tier_funds_df
            )
            if lt_m1_input_df is not None:
                alloc_input_df = alloc_input_df.unionByName(
                    lt_m1_input_df, allowMissingColumns=True
                )
            alloc_input_df = checkpoint(
                spark, alloc_input_df, "alloc_input_post_unions", cfg
            )
            status["sections_completed"] = 10

        with _timed(timings, "S11-S14 sequential PFIC pipeline"):
            pfic_data = build_pfic_elections(spark, cfg)
            (
                alloc_input_df,
                pfic_mapped_df,
                _fcc_blocked_df,
                reclass_unblocked_df,
            ) = build_pfic_mapped_lines(spark, cfg, alloc_input_df)
            pfic_alloc_input_df, converted_pfic_amounts_df = (
                build_pfic_conversion(
                    spark,
                    cfg,
                    reclass_unblocked_df,
                    pfic_mapped_df,
                    pfic_data,
                    lower_tier_funds_df,
                    pfic_data.get("pfic_footnote_entity_details"),
                    lower_tier_amount_df,
                    pfic_types_df=pfic_data.get("pfic_types"),
                )
            )
            if pfic_alloc_input_df is not None:
                alloc_input_df = alloc_input_df.unionByName(
                    pfic_alloc_input_df, allowMissingColumns=True
                )
            pfic_income_attr_df = build_pfic_income_attributes(
                spark,
                cfg,
                converted_pfic_amounts_df,
                lower_tier_amount_df,
                pfic_mapped_df,
                lower_tier_funds_df,
            )
            alloc_input_df = checkpoint(
                spark, alloc_input_df, "alloc_input_post_pfic", cfg
            )
            status["sections_completed"] = 14

        with _timed(timings, "S15-S19 sequential finalization"):
            alloc_input_df = build_box_jkl_input(
                spark, cfg, alloc_input_df, fx_rates_df
            )
            alloc_input_df = apply_master_feed_exclusion(
                spark, cfg, alloc_input_df
            )
            alloc_input_df = apply_blocker_entity(spark, cfg, alloc_input_df)
            alloc_input_df = apply_tag_percentages(
                spark, cfg, alloc_input_df, reclass_k1_df
            )
            alloc_input_df = apply_line_exclusions(
                spark, cfg, alloc_input_df
            )
            status["sections_completed"] = 19

        with _timed(timings, "S20 validation"):
            k3_status = profile_action(
                "validate_k3_rules",
                alloc_input_df,
                lambda: validate_k3_rules(spark, cfg, alloc_input_df),
                cfg,
            )
            status["sections_completed"] = 20
            if k3_status == "FAIL":
                status["status"] = "FAIL"
                status["error"] = "K3 Validations failed"
                return status

        with _timed(timings, "S21 final writes"):
            profile_action(
                "write_final_output",
                alloc_input_df,
                lambda: write_final_output(
                    spark,
                    cfg,
                    alloc_input_df,
                    lower_tier_funds_df,
                    pfic_income_attr_df,
                ),
                cfg,
            )
            status["sections_completed"] = 21
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error("[ERROR] %s", exc, exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(time.perf_counter() - started, 1)
        reports = _emit_reports(cfg) if isinstance(cfg, dict) else {}
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
            "checkpoint_mode": mode,
            "max_threads": workers,
        }
    return status


__all__ = [
    "checkpoint",
    "drop_checkpoints_V2",
    "get_last_run_profile",
    "run_load_lookthrough_allocation_input",
]
