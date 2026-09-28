"""Parity-first outputV2 orchestrator for uspLoadAllocationInput."""

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
from Common_V2.core.execution_profiles import resolve_execution_profile
from Common_V2.core.helpers import log_section, log_timing

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
from .write_helpers import (
    collect_output_frames_parallel,
    flush_collected_results,
)

logger = logging.getLogger(__name__)

_config = output_module("ai_config_service")
_views = output_module("ai_shared_views")
_validation = output_module("ai_validation_service")
_hierarchy = output_module("ai_hierarchy_service")
_k1 = output_module("ai_k1_service")
_form = output_module("ai_form_service")
_pfic = output_module("ai_pfic_service")
_flowup = output_module("ai_pfic_flowup_service")
_final = output_module("ai_finalization_service")

load_config = _config.load_config
register_shared_views = _views.register_shared_views
run_validations = _validation.run_validations
build_entity_hierarchy = track_plan(_hierarchy.build_entity_hierarchy)
build_lower_tier_funds = track_plan(_hierarchy.build_lower_tier_funds)
build_workflows = track_plan(_hierarchy.build_workflows)
build_k1_and_related_inputs = track_plan(_k1.build_k1_and_related_inputs)
build_all_form_inputs = track_plan(_form.build_all_form_inputs)
build_pfic_snapshot = track_plan(_pfic.build_pfic_snapshot)
build_pfic_allocation_input = track_plan(_pfic.build_pfic_allocation_input)
apply_pfic_election_deletes = _pfic.apply_pfic_election_deletes
apply_part_v_vii_flags = track_plan(_pfic.apply_part_v_vii_flags)
build_pfic_flowup_pipeline = track_plan(_flowup.build_pfic_flowup_pipeline)
build_custom_footnote_input = track_plan(_flowup.build_custom_footnote_input)
check_pfic_xml_override_alert = _flowup.check_pfic_xml_override_alert
apply_tag_percentages = track_plan(_final.apply_tag_percentages)
apply_master_feed_override = track_plan(_final.apply_master_feed_override)
apply_blocker_entity_cleanup = track_plan(_final.apply_blocker_entity_cleanup)
apply_distribution_line_suppression = track_plan(
    _final.apply_distribution_line_suppression
)
purge_output_tables = _final.purge_output_tables

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


def _checkpoint(spark, df, name, cfg):
    track_checkpoint_plan(name, df, cfg)
    return checkpoint(spark, df, name, cfg)


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


def run_load_allocation_input(
    spark,
    cfg: dict = None,
    entity_id: int = None,
    client_id: int = None,
    tax_period_id: int = None,
    run_id: int = None,
    catalog: str = None,
    schema: str = None,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "deltalake",
    VolumePath: str = "",
    ExecutionID: str = "1",
    result_type: str = None,
    volume_path: str = None,
    execution_id: str = None,
    call_from: str = None,
    CallFrom: str = None,
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
) -> dict:
    del kwargs
    global _LAST_RUN_PROFILE
    entity_id = entity_id or EntityID
    client_id = client_id or ClientID
    tax_period_id = tax_period_id or TaxPeriodID
    run_id = run_id or RunID
    catalog = catalog or CatalogName
    schema = schema or SchemaName
    result_type = result_type or ResultType or "deltalake"
    volume_path = volume_path or VolumePath or ""
    execution_id = execution_id or ExecutionID or "1"
    call_from = call_from or CallFrom

    t0 = time.time()
    started = time.perf_counter()
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
    save_return_value = None
    log_section("run_load_allocation_input")

    try:
        with _timed(timings, "S1 config and views"):
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
            elif call_from is not None:
                cfg["call_from"] = call_from
            cfg.setdefault("_checkpoint_tables", [])
            cfg.setdefault("_parquet_results", {})
            if volume_path:
                cfg["volume_path"] = volume_path
            cfg.setdefault("result_type", result_type)
            cfg.setdefault("execution_id", execution_id)

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
                    "_checkpoint_paths": [],
                    "_checkpoint_v2_activity": [],
                    "_plan_profile": [],
                    "_checkpoint_plan_profile": [],
                    "_action_plan_profile": [],
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
            if cfg.get("run_status") == "FAIL":
                logger.error(
                    f"RunStatus=FAIL - aborting. RunID={cfg.get('run_id')}, "
                    f"EntityID={cfg.get('entity_id')}"
                )
                return {"status": "FAIL", "reason": "run_status_fail"}
            cfg = load_config(spark, cfg)
            register_shared_views(spark, cfg)

        with _timed(timings, "S2 hierarchy and workflows"):
            build_entity_hierarchy(spark, cfg)
            lower_tier_df = build_lower_tier_funds(spark, cfg)
            k1_workflow_df, at_risk_workflow_df = build_workflows(spark, cfg)
            del at_risk_workflow_df

        with _timed(timings, "S3 validations"):
            should_continue = run_validations(spark, cfg, lower_tier_df)
            if not should_continue:
                return {"status": "FAIL", "reason": "validation_failed"}
            purge_output_tables(spark, cfg)

        with _timed(timings, "S4-S6 independent input builders"):
            form_df, k1_df, pfic_snapshot_df = run_parallel(
                [
                    (
                        "build_all_form_inputs",
                        lambda: build_all_form_inputs(
                            spark, {**cfg}, k1_workflow_df
                        ),
                    ),
                    (
                        "build_k1_and_related_inputs",
                        lambda: build_k1_and_related_inputs(
                            spark, {**cfg}
                        ),
                    ),
                    (
                        "build_pfic_snapshot",
                        lambda: build_pfic_snapshot(
                            spark, {**cfg}, k1_workflow_df
                        ),
                    ),
                ],
                workers,
                parallel_activity,
                "independent_input_builders",
                enabled_groups,
            )

        with _timed(timings, "S6 unions and PFIC snapshot checkpoint"):
            pfic_snapshot_df = _checkpoint(
                spark, pfic_snapshot_df, "pfic_snapshot", cfg
            )
            allocation_input_df = form_df.unionByName(
                k1_df, allowMissingColumns=True
            )
            pfic_alloc_df = build_pfic_allocation_input(
                spark, cfg, pfic_snapshot_df
            )
            allocation_input_df = allocation_input_df.unionByName(
                pfic_alloc_df, allowMissingColumns=True
            )
            custom_fn_df = build_custom_footnote_input(spark, cfg)
            allocation_input_df = allocation_input_df.unionByName(
                custom_fn_df, allowMissingColumns=True
            )
            allocation_input_df = _checkpoint(
                spark, allocation_input_df, "alloc_input", cfg
            )

        with _timed(timings, "S7 PFIC flowup"):
            pfic_flowup_df = build_pfic_flowup_pipeline(
                spark, cfg, pfic_snapshot_df, lower_tier_df
            )
            pfic_flowup_df = _checkpoint(
                spark, pfic_flowup_df, "pfic_raw", cfg
            )
            check_pfic_xml_override_alert(spark, cfg, pfic_flowup_df)
            allocation_input_df, pfic_flowup_df = apply_pfic_election_deletes(
                spark, cfg, allocation_input_df, pfic_flowup_df, lower_tier_df
            )
            pfic_flowup_df = apply_part_v_vii_flags(
                spark, cfg, pfic_flowup_df
            )
            pfic_flowup_df = _checkpoint(
                spark, pfic_flowup_df, "pfic_flowup", cfg
            )

        with _timed(timings, "S8 filters and tags"):
            allocation_input_df = apply_master_feed_override(
                spark, cfg, allocation_input_df
            )
            allocation_input_df = apply_blocker_entity_cleanup(
                spark, cfg, allocation_input_df
            )
            allocation_input_df = apply_distribution_line_suppression(
                spark, cfg, allocation_input_df
            )
            allocation_input_df = _checkpoint(
                spark, allocation_input_df, "alloc_filtered", cfg
            )
            allocation_input_df = apply_tag_percentages(
                spark, cfg, allocation_input_df
            )
            if cfg.get("investment_tag_workflow_id", 0) != 0:
                allocation_input_df = _checkpoint(
                    spark, allocation_input_df, "alloc_tagged", cfg
                )

        with _timed(timings, "S9 collect and store"):
            collect_output_frames_parallel(
                spark,
                cfg,
                allocation_input_df,
                pfic_flowup_df,
                k1_workflow_df,
                workers,
                parallel_activity,
                enabled_groups,
            )
            save_return_value = profile_action(
                "flush_collected_results",
                allocation_input_df,
                lambda: flush_collected_results(
                    spark,
                    cfg,
                    cfg.get("client_id", client_id),
                    cfg.get("entity_id", entity_id),
                    cfg.get("execution_id", execution_id) or "1",
                    workers,
                    parallel_activity,
                    enabled_groups,
                ),
                cfg,
            )
    except Exception:
        raise
    finally:
        elapsed = time.time() - t0
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
            "elapsed_seconds": round(elapsed, 1),
            "checkpoint_mode": mode,
            "max_threads": workers,
            "execution_profile": profile_name,
        }

    if isinstance(cfg, dict):
        drop_checkpoints_V2(spark, cfg)
    log_timing("run_load_allocation_input", t0)
    print(f"Overall time in Seconds: {elapsed:.1f}s")

    if (
        save_return_value
        and isinstance(save_return_value, str)
        and save_return_value.strip().startswith("{")
    ):
        return save_return_value

    return {
        "status": "SUCCESS",
        "elapsed_seconds": round(elapsed, 1),
    }


__all__ = [
    "checkpoint",
    "drop_checkpoints_V2",
    "get_last_run_profile",
    "run_load_allocation_input",
]
