"""Parity-first outputV2 orchestrator for SM look-through cost allocation.

Imports production builders from ``output/orchestrator.py``. Does not copy
that module. Checkpoint V2 is bound onto production ``checkpoint`` for
recursive hierarchy levels and production seams, plus extra plan breaks.
"""

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

_prod = output_module("orchestrator")
logger = _prod.logger

load_sp_config = _prod.load_sp_config
validate_run_status_for_sp = _prod.validate_run_status_for_sp
build_book_effective = track_plan(_prod.build_book_effective)
build_input_data_load = track_plan(_prod.build_input_data_load)
build_cost_percentage_snapshot = track_plan(
    _prod.build_cost_percentage_snapshot
)
build_cost_underlying_types = track_plan(_prod.build_cost_underlying_types)
build_entity_asset_class_relationship = track_plan(
    _prod.build_entity_asset_class_relationship
)
build_entity_hierarchy = track_plan(_prod.build_entity_hierarchy)
build_all_underlyings_combined = track_plan(
    _prod.build_all_underlyings_combined
)
apply_asset_class_filter = track_plan(_prod.apply_asset_class_filter)
build_states_dar_rule_mapping = track_plan(_prod.build_states_dar_rule_mapping)
build_all_underlyings_states = track_plan(_prod.build_all_underlyings_states)
build_allocation_input_pass1 = track_plan(_prod.build_allocation_input_pass1)
build_allocation_input_pass2 = track_plan(_prod.build_allocation_input_pass2)
build_allocation_input_pass3 = track_plan(_prod.build_allocation_input_pass3)
build_allocation_input_pass4 = track_plan(_prod.build_allocation_input_pass4)
build_entity_partners = track_plan(_prod.build_entity_partners)
build_final_effective_percentages = track_plan(
    _prod.build_final_effective_percentages
)
compute_by_amount_allocation = track_plan(_prod.compute_by_amount_allocation)
apply_amount_deduction = track_plan(_prod.apply_amount_deduction)
compute_by_percentage_allocation = track_plan(
    _prod.compute_by_percentage_allocation
)
build_final_output = track_plan(_prod.build_final_output)
_tbl = _prod._tbl

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
def use_v2_production_checkpoint():
    original = _prod.checkpoint
    _prod.checkpoint = _checkpoint
    try:
        yield
    finally:
        _prod.checkpoint = original


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


def run_sm_load_lookthrough_cost_allocation_to_output(
    spark,
    cfg: dict = None,
    verbose: bool = False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    RankForRulePickup: int = 0,
    CatalogName: str = None,
    SchemaName: str = None,
    CallFrom: str = None,
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
    rank_for_rule_pickup = RankForRulePickup
    catalog = CatalogName
    schema = SchemaName
    call_from = CallFrom
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
    return_value = None
    status = {
        "sp_name": "usp_SM_LoadLookThroughCostAllocationToOutput",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
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
                    rank_for_rule_pickup=rank_for_rule_pickup,
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
            load_sp_config(spark, cfg)
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            dar_txn_id = cfg["dar_txn_id"]
            global_dar_txn_id = cfg["global_dar_txn_id"]
            cfg["_sm_state_lines"] = F.broadcast(
                _tbl(spark, "SM_StateLines", cfg).select(
                    "StateFieldID", "StateID", "TransactionDate"
                )
            )
            cfg["_dar_setup"] = F.broadcast(
                _tbl(spark, "DefaultAllocationRuleSetup", cfg)
                .filter(
                    F.col("TransactionID").isin(dar_txn_id, global_dar_txn_id)
                )
                .select(
                    "RuleID",
                    "UnderlyingTypeID",
                    "RuleTypeID",
                    "AllocationByID",
                    "AllocationPercentageTypeID",
                )
            )
            cfg["_enu_allocation_by"] = F.broadcast(
                _tbl(spark, "ENU_AllocationBy", cfg)
            )
            cfg["_enu_custom_allocations"] = F.broadcast(
                _tbl(spark, "ENU_CustomAllocations", cfg).select(
                    "AllocationTypeID", "AllocationType"
                )
            )
            cfg["_enu_underlying_type"] = F.broadcast(
                _tbl(spark, "Enu_Underlyingtype", cfg)
            )
            cfg["_entity_lookup"] = F.broadcast(
                _tbl(spark, "Entity", cfg).select("EntityID", "AssetClassID")
            )
            if not validate_run_status_for_sp(spark, cfg):
                status["status"] = "SKIPPED"
                status["error"] = "RunStatus=FAIL or entity type mismatch"
                status["skip_reason"] = "run_status_or_entity_mismatch"
                return status

        with use_v2_production_checkpoint():
            with _timed(timings, "S2 independent_loads"):
                book_effective, input_data_load, cost_pct_snapshot, entity_ac_rel = (
                    run_parallel(
                        [
                            (
                                "book_effective",
                                lambda: build_book_effective(spark, cfg),
                            ),
                            (
                                "input_data_load",
                                lambda: build_input_data_load(spark, cfg),
                            ),
                            (
                                "cost_pct_snapshot",
                                lambda: build_cost_percentage_snapshot(
                                    spark, cfg
                                ),
                            ),
                            (
                                "entity_ac_rel",
                                lambda: build_entity_asset_class_relationship(
                                    spark, cfg
                                ),
                            ),
                        ],
                        workers,
                        parallel_activity,
                        "independent_loads",
                        enabled_groups,
                    )
                )
                book_effective = F.broadcast(book_effective)
                input_data_load = _checkpoint(
                    spark, input_data_load, "temp_alloc_input", cfg
                )
                cost_pct_snapshot = _checkpoint(
                    spark, cost_pct_snapshot, "cost_pct_snapshot", cfg
                )

            with _timed(timings, "S3 hierarchy"):
                cost_underlying_types = build_cost_underlying_types(
                    spark, cfg, cost_pct_snapshot
                )
                if cost_underlying_types.isEmpty():
                    entity_hier = None
                else:
                    entity_hier = build_entity_hierarchy(
                        spark, cfg, cost_underlying_types
                    )
                    entity_hier = _checkpoint(
                        spark, entity_hier, "entity_hier_final", cfg
                    )
                all_underlyings = build_all_underlyings_combined(
                    spark,
                    cfg,
                    cost_underlying_types,
                    cost_pct_snapshot,
                    entity_hier,
                )
                all_underlyings = apply_asset_class_filter(
                    spark, cfg, all_underlyings, entity_ac_rel
                )
                states_dar_mapping = build_states_dar_rule_mapping(spark, cfg)
                all_underlyings_states = build_all_underlyings_states(
                    spark,
                    cfg,
                    all_underlyings,
                    input_data_load,
                    states_dar_mapping,
                )

            with _timed(timings, "S4 alloc passes"):
                alloc_input, remaining_input, remaining_be = (
                    build_allocation_input_pass1(
                        spark,
                        cfg,
                        input_data_load,
                        book_effective,
                        cost_pct_snapshot,
                    )
                )
                alloc_input = _checkpoint(
                    spark, alloc_input, "alloc_pass1", cfg
                )
                alloc_input, remaining_input, remaining_be = (
                    build_allocation_input_pass2(
                        spark, cfg, remaining_input, remaining_be, alloc_input
                    )
                )
                alloc_input = _checkpoint(
                    spark, alloc_input, "alloc_pass2", cfg
                )
                alloc_input, remaining_input, remaining_be = (
                    build_allocation_input_pass3(
                        spark, cfg, remaining_input, remaining_be, alloc_input
                    )
                )
                alloc_input = _checkpoint(
                    spark, alloc_input, "alloc_pass3", cfg
                )
                alloc_input = build_allocation_input_pass4(
                    spark,
                    cfg,
                    remaining_input,
                    remaining_be,
                    all_underlyings_states,
                    alloc_input,
                )
                alloc_input = _checkpoint(
                    spark, alloc_input, "alloc_input_final", cfg
                )

            with _timed(timings, "S5 allocate and write"):
                if not alloc_input.isEmpty():
                    entity_partners = build_entity_partners(spark, cfg)
                    eff_pct = build_final_effective_percentages(spark, cfg)
                    eff_pct = _checkpoint(spark, eff_pct, "fep", cfg)
                    eff_pct = F.broadcast(eff_pct)
                    by_amount_output = compute_by_amount_allocation(
                        spark, cfg, alloc_input, eff_pct, entity_partners
                    )
                    by_amount_output = _checkpoint(
                        spark, by_amount_output, "alloc_pass1_amount", cfg
                    )
                    alloc_input = apply_amount_deduction(
                        spark, cfg, alloc_input, by_amount_output
                    )
                    alloc_input = _checkpoint(
                        spark, alloc_input, "alloc_pass2_deduct", cfg
                    )
                    by_pct_output = compute_by_percentage_allocation(
                        spark, cfg, alloc_input, eff_pct, entity_partners
                    )
                    by_pct_output = _checkpoint(
                        spark, by_pct_output, "alloc_pass4", cfg
                    )
                    alloc_output = by_amount_output.unionByName(by_pct_output)
                    alloc_output = _checkpoint(
                        spark, alloc_output, "alloc_output", cfg
                    )
                    final_output = build_final_output(
                        spark, cfg, alloc_output
                    )
                    return_value = profile_action(
                        "flush_result_tables",
                        final_output,
                        lambda: flush_result_tables(
                            spark, cfg, final_output, alloc_output
                        ),
                        cfg,
                    )
                else:
                    logger.warning(
                        "alloc_input is empty — skipping allocation logic"
                    )
                    status["skip_reason"] = "empty_alloc_input"
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

    if (
        return_value
        and isinstance(return_value, str)
        and return_value not in ("SUCCESS", "")
    ):
        logger.info(f"[PARQUET] Return JSON: {return_value}")
        status["parquet_path"] = return_value
    return status


__all__ = [
    "get_last_run_profile",
    "run_sm_load_lookthrough_cost_allocation_to_output",
]
