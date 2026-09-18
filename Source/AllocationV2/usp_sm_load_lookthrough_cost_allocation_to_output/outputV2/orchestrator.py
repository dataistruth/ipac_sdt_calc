"""Parity-safe outputV2 orchestrator for SM look-through cost allocation."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

import pyspark.sql.functions as F

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config

from .parent import production_orchestrator
from .plan_profiler import plan_profile_report, profile_action, track_plan

logger = logging.getLogger(__name__)
_prod = production_orchestrator()

load_sp_config = _prod.load_sp_config
validate_run_status_for_sp = _prod.validate_run_status_for_sp
build_book_effective = _prod.build_book_effective
build_input_data_load = _prod.build_input_data_load
build_cost_percentage_snapshot = _prod.build_cost_percentage_snapshot
build_cost_underlying_types = _prod.build_cost_underlying_types
build_entity_asset_class_relationship = _prod.build_entity_asset_class_relationship
build_all_underlyings_combined = _prod.build_all_underlyings_combined
apply_asset_class_filter = _prod.apply_asset_class_filter
build_states_dar_rule_mapping = _prod.build_states_dar_rule_mapping
build_all_underlyings_states = _prod.build_all_underlyings_states
build_allocation_input_pass1 = _prod.build_allocation_input_pass1
build_allocation_input_pass2 = _prod.build_allocation_input_pass2
build_allocation_input_pass3 = _prod.build_allocation_input_pass3
build_allocation_input_pass4 = _prod.build_allocation_input_pass4
build_entity_partners = _prod.build_entity_partners
compute_by_amount_allocation = _prod.compute_by_amount_allocation
apply_amount_deduction = _prod.apply_amount_deduction
compute_by_percentage_allocation = _prod.compute_by_percentage_allocation
build_final_output = _prod.build_final_output
write_allocation_output = _prod.write_allocation_output
write_update_allocation_input = _prod.write_update_allocation_input

_LAST_RUN_PROFILE = {}


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


def _run_parallel(tasks, workers, activity, label):
    """Run independent builders and return results in declared order."""
    pool_workers = max(1, min(int(workers), len(tasks), 4))
    started = time.perf_counter()
    results = {}

    def invoke(name, fn):
        task_started = time.perf_counter()
        try:
            value = fn()
        except Exception:
            activity.append(
                {
                    "group": label,
                    "task": name,
                    "status": "FAIL",
                    "elapsed_seconds": round(
                        time.perf_counter() - task_started, 3
                    ),
                    "thread": threading.current_thread().name,
                }
            )
            raise
        activity.append(
            {
                "group": label,
                "task": name,
                "status": "SUCCESS",
                "elapsed_seconds": round(
                    time.perf_counter() - task_started, 3
                ),
                "thread": threading.current_thread().name,
            }
        )
        return value

    if pool_workers == 1:
        for name, fn in tasks:
            results[name] = invoke(name, fn)
    else:
        with ThreadPoolExecutor(
            max_workers=pool_workers, thread_name_prefix="sm-cost"
        ) as pool:
            futures = {
                pool.submit(invoke, name, fn): name for name, fn in tasks
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception:
                    for pending in futures:
                        pending.cancel()
                    raise

    wall = round(time.perf_counter() - started, 3)
    activity.append(
        {
            "group": label,
            "task": "__pool__",
            "status": "SUCCESS",
            "elapsed_seconds": wall,
            "thread": threading.current_thread().name,
        }
    )
    print(
        f"[parallel] {label}: tasks={len(tasks)} workers={pool_workers} "
        f"wall={wall:.3f}s"
    )
    return [results[name] for name, _ in tasks]


def _load_shared_lookups(spark, cfg, workers, activity):
    """Build independent bounded lookup plans without sharing mutable writes."""
    table = _prod._tbl
    dar_ids = (cfg["dar_txn_id"], cfg["global_dar_txn_id"])

    def state_and_dar():
        state_lines = F.broadcast(
            table(spark, "SM_StateLines", cfg).select(
                "StateFieldID", "StateID", "TransactionDate"
            )
        )
        dar_setup = F.broadcast(
            table(spark, "DefaultAllocationRuleSetup", cfg)
            .filter(F.col("TransactionID").isin(*dar_ids))
            .select(
                "RuleID",
                "UnderlyingTypeID",
                "RuleTypeID",
                "AllocationByID",
                "AllocationPercentageTypeID",
            )
        )
        return state_lines, dar_setup

    def allocation_enums():
        return (
            F.broadcast(table(spark, "ENU_AllocationBy", cfg)),
            F.broadcast(
                table(spark, "ENU_CustomAllocations", cfg).select(
                    "AllocationTypeID", "AllocationType"
                )
            ),
        )

    def underlying_enum():
        return F.broadcast(table(spark, "Enu_Underlyingtype", cfg))

    def entity_lookup():
        # Entity is not a bounded enum. Keep it unhinted to avoid an unsafe
        # all-client broadcast; downstream filters/join keys remain unchanged.
        return table(spark, "Entity", cfg).select("EntityID", "AssetClassID")

    state_dar, allocation, underlying, entity = _run_parallel(
        [
            ("state_lines_and_dar", state_and_dar),
            ("allocation_enums", allocation_enums),
            ("underlying_enum", underlying_enum),
            ("entity_lookup", entity_lookup),
        ],
        workers,
        activity,
        "independent_lookup_loads",
    )
    cfg["_sm_state_lines"], cfg["_dar_setup"] = state_dar
    cfg["_enu_allocation_by"], cfg["_enu_custom_allocations"] = allocation
    cfg["_enu_underlying_type"] = underlying
    cfg["_entity_lookup"] = entity


def build_final_effective_percentages(spark, cfg):
    """Read only the current run/rank before projecting the effective fact."""
    source = _prod._tbl(spark, "SM_FinalEffectivePercentages", cfg)
    predicate = (
        (F.col("RunID") == cfg["run_id"])
        & (F.col("RankForRule") == cfg["rank_for_rule_pickup"])
    )
    if "ClientID" in source.columns:
        predicate = predicate & (F.col("ClientID") == cfg["client_id"])
    if "TaxPeriodID" in source.columns:
        predicate = predicate & (F.col("TaxPeriodID") == cfg["tax_period_id"])
    return source.filter(predicate).select(
        "InvestmentID",
        "PartnerNumber",
        "EffPercentage",
        "AllocationType",
        "Quarter",
        "TypeID",
        "TrackingKey",
        "Tag",
        "LineID",
        "EffAmount",
        "AssetClassID",
        "IsExcludefromTransfer",
    )


def build_entity_hierarchy(spark, cfg, cost_underlying_types):
    """Sequential hierarchy expansion with the exact per-level checkpoint seam."""
    er = (
        _prod._tbl(spark, "EntityRelationship", cfg)
        .filter(
            (F.col("ClientID") == cfg["client_id"])
            & (F.col("TaxPeriodID") == cfg["tax_period_id"])
        )
        .select("LowerTierEntityID", "UpperTierEntityID")
    )
    tc = cost_underlying_types
    base = (
        tc.alias("TC")
        .join(
            er.alias("ER"),
            F.col("ER.UpperTierEntityID")
            == F.when(
                F.upper(F.col("TC.EntityUnderlyingtype")) == "ASSET CLASS",
                F.col("TC.EntityId"),
            ).otherwise(F.col("TC.InvestmentID")),
        )
        .select(
            F.col("ER.LowerTierEntityID"),
            F.col("ER.UpperTierEntityID").alias("ParentEntityID"),
            F.col("ER.UpperTierEntityID").alias("CurrentEntityId"),
            F.lit(2).alias("HLevel"),
            F.col("TC.AllocationTypeId"),
            F.concat(
                F.lit("~"),
                F.when(
                    F.upper(F.col("TC.EntityUnderlyingtype")) == "ASSET CLASS",
                    F.col("ER.LowerTierEntityID").cast("string"),
                ).otherwise(
                    F.when(
                        _prod._ns(F.col("TC.TrackingKey")) == "",
                        F.col("TC.InvestmentID").cast("string"),
                    ).otherwise(F.col("TC.TrackingKey"))
                ),
                F.lit("~"),
            ).alias("TrackingKey"),
            F.col("TC.InvestmentID").alias("AssetClassId"),
            F.col("ER.LowerTierEntityID").alias(
                "ImmediateLowerTierEntityID"
            ),
        )
    )
    current_level = base
    all_levels = [base]
    level = 3
    while True:
        next_level = (
            er.alias("ER2")
            .join(
                current_level.alias("EH"),
                F.col("ER2.UpperTierEntityID")
                == F.col("EH.LowerTierEntityID"),
            )
            .select(
                F.col("ER2.LowerTierEntityID"),
                F.col("ER2.UpperTierEntityID").alias("ParentEntityID"),
                F.col("EH.CurrentEntityId"),
                F.lit(level).alias("HLevel"),
                F.col("EH.AllocationTypeId"),
                F.col("EH.TrackingKey"),
                F.col("EH.AssetClassId"),
                F.col("EH.ImmediateLowerTierEntityID"),
            )
        )
        next_level = checkpoint(
            spark, next_level, f"hier_level_{level}", cfg
        )
        if not profile_action(
            f"hier_level_{level}.first",
            next_level,
            next_level.first,
            cfg,
        ):
            break
        all_levels.append(next_level)
        current_level = next_level
        level += 1

    hierarchy = all_levels[0]
    for level_df in all_levels[1:]:
        hierarchy = hierarchy.unionByName(level_df)
    return hierarchy


for _name in (
    "build_book_effective",
    "build_input_data_load",
    "build_cost_percentage_snapshot",
    "build_cost_underlying_types",
    "build_entity_asset_class_relationship",
    "build_entity_hierarchy",
    "build_all_underlyings_combined",
    "apply_asset_class_filter",
    "build_states_dar_rule_mapping",
    "build_all_underlyings_states",
    "build_allocation_input_pass1",
    "build_allocation_input_pass2",
    "build_allocation_input_pass3",
    "build_allocation_input_pass4",
    "build_entity_partners",
    "build_final_effective_percentages",
    "compute_by_amount_allocation",
    "apply_amount_deduction",
    "compute_by_percentage_allocation",
    "build_final_output",
):
    globals()[_name] = track_plan(globals()[_name])


def _count(name, df, cfg):
    value = profile_action(name, df, df.count, cfg)
    logger.info("[COUNT] %s: %s", name, value)
    return value


def _emit_plan_reports(cfg):
    reports = {"builder": [], "checkpoint": [], "action": []}
    if not cfg.get("profile_plan"):
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
            cfg.get(source_key, []), threshold, label=label
        )
    return reports


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def run_sm_load_lookthrough_cost_allocation_to_output(
    spark,
    cfg=None,
    verbose=False,
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
    """Run the exact production flow with V2 checkpoints and safe load pools."""
    del kwargs
    global _LAST_RUN_PROFILE
    started = time.perf_counter()
    timings = []
    parallel_activity = []
    return_value = None
    workers = _normalize_workers(max_threads, MaxThreads)
    profile_enabled = _as_bool(
        ProfilePlan if ProfilePlan is not None else profile_plan
    )
    threshold = int(
        PlanCheckpointThreshold
        if PlanCheckpointThreshold is not None
        else plan_checkpoint_threshold
    )
    status = {
        "sp_name": "usp_SM_LoadLookThroughCostAllocationToOutput",
        "run_id": RunID,
        "entity_id": EntityID,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
    }
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    try:
        with _timed(timings, "S1-S3 config and validation"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    entity_id=EntityID,
                    client_id=ClientID,
                    tax_period_id=TaxPeriodID,
                    run_id=RunID,
                    catalog=CatalogName,
                    schema=SchemaName,
                    call_from=CallFrom,
                    rank_for_rule_pickup=RankForRulePickup,
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
                "_checkpoint_plan_profile": [],
                "_action_plan_profile": [],
                "profile_plan": profile_enabled,
                "plan_checkpoint_threshold": threshold,
                "checkpoint_mode": mode,
                "max_threads": workers,
            }
            if ResultType is not None:
                cfg.setdefault("result_type", ResultType)
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID
            initialize_checkpoint_V2(cfg, mode)
            print(
                f"[CHECKPOINT_V2] mode={mode}; [outputV2] "
                f"MaxThreads={workers} "
                f"ProfilePlan={'on' if profile_enabled else 'off'}"
            )
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            load_sp_config(spark, cfg)
            _load_shared_lookups(spark, cfg, workers, parallel_activity)
            if not validate_run_status_for_sp(spark, cfg):
                status["status"] = "SKIPPED"
                status["error"] = (
                    "RunStatus=FAIL or entity allocation type mismatch"
                )
                return status

        with _timed(timings, "S5-S10 independent source builders"):
            (
                book_effective,
                input_data_load,
                cost_pct_snapshot,
                entity_ac_rel,
                states_dar_mapping,
            ) = _run_parallel(
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
                        lambda: build_cost_percentage_snapshot(spark, cfg),
                    ),
                    (
                        "entity_ac_rel",
                        lambda: build_entity_asset_class_relationship(
                            spark, cfg
                        ),
                    ),
                    (
                        "states_dar_mapping",
                        lambda: build_states_dar_rule_mapping(spark, cfg),
                    ),
                ],
                workers,
                parallel_activity,
                "independent_source_builders",
            )
            book_effective = F.broadcast(book_effective)
            cost_pct_snapshot = checkpoint(
                spark, cost_pct_snapshot, "cost_pct_snapshot", cfg
            )
            if verbose:
                for name, frame in (
                    ("book_effective", book_effective),
                    ("input_data_load", input_data_load),
                    ("cost_pct_snapshot", cost_pct_snapshot),
                    ("entity_ac_rel", entity_ac_rel),
                    ("states_dar_mapping", states_dar_mapping),
                ):
                    _count(name, frame, cfg)

        # Hierarchy construction, each dependent pass, and all related state
        # transitions deliberately remain sequential.
        with _timed(timings, "S7-S10 hierarchy and state ranking"):
            cost_underlying_types = build_cost_underlying_types(
                spark, cfg, cost_pct_snapshot
            )
            has_cost_types = bool(
                profile_action(
                    "cost_underlying_types.head",
                    cost_underlying_types,
                    lambda: cost_underlying_types.head(1),
                    cfg,
                )
            )
            entity_hier = (
                build_entity_hierarchy(
                    spark, cfg, cost_underlying_types
                )
                if has_cost_types
                else None
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
            all_underlyings_states = build_all_underlyings_states(
                spark,
                cfg,
                all_underlyings,
                input_data_load,
                states_dar_mapping,
            )

        with _timed(timings, "S11-S14 sequential allocation passes"):
            alloc_input, remaining_input, remaining_be = (
                build_allocation_input_pass1(
                    spark,
                    cfg,
                    input_data_load,
                    book_effective,
                    cost_pct_snapshot,
                )
            )
            alloc_input, remaining_input, remaining_be = (
                build_allocation_input_pass2(
                    spark,
                    cfg,
                    remaining_input,
                    remaining_be,
                    alloc_input,
                )
            )
            alloc_input, remaining_input, remaining_be = (
                build_allocation_input_pass3(
                    spark,
                    cfg,
                    remaining_input,
                    remaining_be,
                    alloc_input,
                )
            )
            alloc_input = build_allocation_input_pass4(
                spark,
                cfg,
                remaining_input,
                remaining_be,
                all_underlyings_states,
                alloc_input,
            )
            alloc_input = checkpoint(
                spark, alloc_input, "alloc_input_final", cfg
            )

        has_alloc_input = bool(
            profile_action(
                "alloc_input.head",
                alloc_input,
                lambda: alloc_input.head(1),
                cfg,
            )
        )
        if has_alloc_input:
            with _timed(timings, "S15 independent output inputs"):
                entity_partners, eff_pct = _run_parallel(
                    [
                        (
                            "entity_partners",
                            lambda: build_entity_partners(spark, cfg),
                        ),
                        (
                            "final_effective_percentages",
                            lambda: build_final_effective_percentages(
                                spark, cfg
                            ),
                        ),
                    ],
                    workers,
                    parallel_activity,
                    "independent_output_inputs",
                )
                eff_pct = F.broadcast(eff_pct)

            with _timed(timings, "S16-S20 sequential allocation"):
                by_amount_output = compute_by_amount_allocation(
                    spark, cfg, alloc_input, eff_pct, entity_partners
                )
                alloc_input_after_amount = apply_amount_deduction(
                    spark, cfg, alloc_input, by_amount_output
                )
                by_pct_output = compute_by_percentage_allocation(
                    spark,
                    cfg,
                    alloc_input_after_amount,
                    eff_pct,
                    entity_partners,
                )
                alloc_output = by_amount_output.unionByName(by_pct_output)
                final_output = build_final_output(
                    spark, cfg, alloc_output
                )

            # The output insert and source-input deduction are related writes and
            # intentionally preserve production order.
            with _timed(timings, "S21 output write"):
                return_value = profile_action(
                    "write_allocation_output",
                    final_output,
                    lambda: write_allocation_output(
                        spark, cfg, final_output
                    ),
                    cfg,
                )
            with _timed(timings, "S22 input deduction write"):
                profile_action(
                    "write_update_allocation_input",
                    alloc_output,
                    lambda: write_update_allocation_input(
                        spark, cfg, alloc_output
                    ),
                    cfg,
                )
        else:
            logger.warning(
                "alloc_input is empty — skipping allocation logic"
            )
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error("[FAIL] %s", exc, exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(
            time.perf_counter() - started, 1
        )
        reports = _emit_plan_reports(cfg) if isinstance(cfg, dict) else {}
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
                cfg.get("checkpoint_mode")
                if isinstance(cfg, dict)
                else None
            ),
            "max_threads": workers,
        }

    logger.info(
        "[DONE] run_sm_load_lookthrough_cost_allocation_to_output | "
        "%.1fs | RunID=%s EntityID=%s",
        status["elapsed_seconds"],
        cfg["run_id"],
        cfg["entity_id"],
    )
    if (
        return_value
        and isinstance(return_value, str)
        and return_value not in ("SUCCESS", "")
    ):
        status["parquet_path"] = return_value
    return status


__all__ = [
    "checkpoint",
    "drop_checkpoints_V2",
    "get_last_run_profile",
    "run_sm_load_lookthrough_cost_allocation_to_output",
]
