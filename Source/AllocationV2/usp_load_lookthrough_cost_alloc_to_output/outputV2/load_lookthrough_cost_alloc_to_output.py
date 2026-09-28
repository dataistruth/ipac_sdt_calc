"""Parity-first outputV2 orchestrator for look-through cost allocation."""

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

from .hierarchy_plan_breaks import (
    load_cost_percentages as load_cost_percentages_v2,
    use_v2_hierarchy_checkpoint,
)
from .parallel_helpers import normalize_workers
from .parent import output_module
from .plan_profiler import (
    plan_profile_report,
    profile_action,
    track_checkpoint_plan,
    track_plan,
)
from .write_helpers import flush_result_tables

_prod = output_module("load_lookthrough_cost_alloc_to_output")
_data = output_module("_data_loading")
_hier = output_module("_hierarchy")
_alloc = output_module("_allocation")

logger = logging.getLogger(_prod.__name__)

_load_sp_specific_config = _prod._load_sp_specific_config
load_workflow_ids = _prod.load_workflow_ids
_readd_footnote_source_lines = track_plan(_prod._readd_footnote_source_lines)
_build_k1_lineitems_704c = track_plan(_prod._build_k1_lineitems_704c)

load_partners = track_plan(_data.load_partners)
load_line_items = track_plan(_data.load_line_items)
load_lookthrough_input = track_plan(_data.load_lookthrough_input)
load_allocation_rules = track_plan(_data.load_allocation_rules)
load_book_effective_rules = track_plan(_data.load_book_effective_rules)
add_footnote_inheritance = track_plan(_data.add_footnote_inheritance)
load_final_effective_percentages = track_plan(
    _data.load_final_effective_percentages
)
apply_704c_to_k1_mappings = track_plan(_data.apply_704c_to_k1_mappings)

build_entity_hierarchy = track_plan(_hier.build_entity_hierarchy)
build_rule_ordered_underlyings = track_plan(
    _hier.build_rule_ordered_underlyings
)

prepare_lookthrough_input = track_plan(_alloc.prepare_lookthrough_input)
validate_by_amount_allocations = _alloc.validate_by_amount_allocations
handle_offset_types_704c = track_plan(_alloc.handle_offset_types_704c)
process_by_amount_allocation = track_plan(_alloc.process_by_amount_allocation)
process_by_percentage_allocation = track_plan(
    _alloc.process_by_percentage_allocation
)
process_704c_allocation = track_plan(_alloc.process_704c_allocation)
insert_704c_by_amount_mapped_lines = track_plan(
    _alloc.insert_704c_by_amount_mapped_lines
)

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
    """Extra plan-break seams (footnotes-style) plus local qualifier reset."""
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


def load_cost_percentages(spark, cfg):
    return load_cost_percentages_v2(spark, cfg, _checkpoint)


load_cost_percentages = track_plan(load_cost_percentages)


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


def run_load_lookthrough_cost_alloc(
    spark,
    cfg: dict = None,
    line_type: str = "",
    rank_for_rule: int = 0,
    entity_id: int = None,
    client_id: int = None,
    tax_period_id: int = None,
    run_id: int = None,
    catalog: str = None,
    schema: str = None,
    result_type: str = "deltalake",
    volume_path: str = "",
    execution_id: str = None,
    verbose: bool = False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    VolumePath: str = None,
    ExecutionID: str = None,
    LineType: str = None,
    RankForRule: int = None,
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
    **run_params,
):
    """Production semantics with Checkpoint V2 extras and sequential writes."""
    del parallel_groups, ParallelGroups
    global _LAST_RUN_PROFILE
    entity_id = entity_id or EntityID
    client_id = client_id or ClientID
    tax_period_id = tax_period_id or TaxPeriodID
    run_id = run_id or RunID
    catalog = catalog or CatalogName
    schema = schema or SchemaName
    volume_path = volume_path or VolumePath or ""
    execution_id = execution_id or ExecutionID
    line_type = (
        line_type
        or LineType
        or run_params.get("LineType", "")
        or ""
    )
    rank_for_rule = (
        rank_for_rule
        or RankForRule
        or run_params.get("RankForRule", 0)
        or 0
    )
    result_type = (
        result_type
        or run_params.get("ResultType", "deltalake")
        or "deltalake"
    )
    t0 = time.time()
    timings = []
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
    status = {
        "sp": "uspLoadLookThroughCostAllocationToOutput",
        "status": "SUCCESS",
        "line_type": line_type,
        "elapsed": 0,
        "elapsed_seconds": 0,
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
                    **{
                        key: value
                        for key, value in run_params.items()
                        if key
                        not in {
                            "LineType",
                            "RankForRule",
                            "ResultType",
                        }
                    },
                )
            cfg["result_type"] = result_type
            cfg["volume_path"] = volume_path
            cfg["execution_id"] = execution_id
            cfg["verbose"] = verbose
            cfg = {**cfg, "_checkpoint_tables": []}
            cfg["line_type"] = line_type
            cfg["rank_for_rule"] = rank_for_rule
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
                f"ProfilePlan={'on' if profile_enabled else 'off'} "
                f"LineType={line_type!r}"
            )
            cfg = _load_sp_specific_config(spark, cfg)
            if (cfg.get("run_status") or "").upper() == "FAIL":
                status["status"] = "SKIP"
                status["skip_reason"] = "run_status_fail"
                _v2("SKIPPED: RunStatus=FAIL")
                return status
            cfg = load_workflow_ids(spark, cfg)
            if cfg is None:
                status["status"] = "SKIP"
                status["skip_reason"] = "no_cost_fep"
                _v2("SKIPPED: no Cost/704c FinalEffectivePercentages")
                return status

        with _timed(timings, "S2 loads"):
            partners = _checkpoint(
                spark, load_partners(spark, cfg), "partners", cfg
            )
            line_items = load_line_items(spark, cfg)
            input_data_raw = _checkpoint(
                spark,
                load_lookthrough_input(spark, cfg),
                "input_raw",
                cfg,
            )
            rules = load_allocation_rules(spark, cfg)
            cost_data = load_cost_percentages(spark, cfg)
            distinct_mappings = None
            if (
                line_type == "K1 with 704c"
                and (cfg.get("c704_allocation_type_name") or "").strip()
            ):
                mapped = apply_704c_to_k1_mappings(
                    spark,
                    cfg,
                    cost_data["cost_percentages"],
                    rules["map_rules"],
                    rules["default_rules"],
                )
                cost_data["cost_percentages"] = mapped["cost_percentages"]
                rules["map_rules"] = mapped["map_rules"]
                rules["default_rules"] = mapped["default_rules"]
                distinct_mappings = mapped["distinct_mappings"]
                distinct_mappings = _checkpoint(
                    spark, distinct_mappings, "distinct_mappings", cfg
                )
            cost_percentages = _checkpoint(
                spark, cost_data["cost_percentages"], "cost_percentages", cfg
            )

        with _timed(timings, "S3 hierarchy and book"):
            with use_v2_hierarchy_checkpoint(_checkpoint):
                all_underlyings = build_entity_hierarchy(
                    spark, cfg, cost_percentages
                )
                all_underlyings = build_rule_ordered_underlyings(
                    spark,
                    cfg,
                    all_underlyings,
                    input_data_raw,
                    rules["map_rules"],
                    rules["default_rules"],
                )
            all_underlyings = _checkpoint(
                spark, all_underlyings, "all_underlyings_ordered", cfg
            )
            book_effective = load_book_effective_rules(spark, cfg)
            book_effective = add_footnote_inheritance(
                spark, cfg, book_effective, input_data_raw
            )
            book_effective = book_effective.withColumn(
                "AdjustmentAllocationTypeID",
                F.when(
                    F.col("AdjustmentAllocationTypeID")
                    == cfg["book_allocation_type_id"],
                    cfg["cost_allocation_type_id"],
                ).otherwise(F.col("AdjustmentAllocationTypeID")),
            )
            book_effective = _checkpoint(
                spark, book_effective, "book_effective", cfg
            )
            line_items = _readd_footnote_source_lines(
                spark, cfg, line_items, book_effective, all_underlyings
            )
            line_items = _checkpoint(spark, line_items, "line_items", cfg)

        with _timed(timings, "S4 prepare input and FEP"):
            input_data = prepare_lookthrough_input(
                spark,
                cfg,
                input_data_raw,
                line_items,
                book_effective,
                rules["entity_rules"],
                all_underlyings,
                rules["default_rules"],
            )
            if (
                distinct_mappings is not None
                and cfg.get("is_custom_allocation_enabled") == "C"
            ):
                input_data = insert_704c_by_amount_mapped_lines(
                    spark,
                    cfg,
                    input_data,
                    distinct_mappings,
                    all_underlyings,
                    rules["default_rules"],
                    book_effective,
                )
            input_data = _checkpoint(
                spark, input_data, "temp_alloc_input", cfg
            )
            final_eff_pct = load_final_effective_percentages(spark, cfg)
            final_eff_pct = _checkpoint(spark, final_eff_pct, "fep", cfg)
            validate_by_amount_allocations(
                spark,
                cfg,
                input_data,
                final_eff_pct,
                partners,
                rules["default_rules"],
                rules["map_rules"],
            )

        with _timed(timings, "S5 allocate"):
            if line_type == "704c":
                allocation_percentages = final_eff_pct.groupBy(
                    "RunID",
                    "EntityID",
                    "InvestmentID",
                    "PartnerNumber",
                    "LineID",
                    "LineTypeID",
                    "Quarter",
                    "TrackingKey",
                    "TypeID",
                ).pivot(
                    "704cPercentType",
                    [
                        "OrdinaryPercentage",
                        "CapitalPercentage",
                        "CapitalGainPercentage",
                        "CapitalLossPercentage",
                    ],
                ).agg(F.max("EffPercentage"))
                allocation_percentages = _checkpoint(
                    spark,
                    allocation_percentages,
                    "alloc_pass1",
                    cfg,
                )
                k1_lineitems_704c = _build_k1_lineitems_704c(
                    spark, cfg, input_data, line_items
                )
                k1_lineitems_704c = handle_offset_types_704c(
                    spark, cfg, k1_lineitems_704c, allocation_percentages
                )
                k1_lineitems_704c = _checkpoint(
                    spark, k1_lineitems_704c, "alloc_pass2", cfg
                )
                allocation_output = process_704c_allocation(
                    spark,
                    cfg,
                    input_data,
                    final_eff_pct,
                    allocation_percentages,
                    k1_lineitems_704c,
                )
            else:
                by_amount = process_by_amount_allocation(
                    spark,
                    cfg,
                    input_data,
                    final_eff_pct,
                    partners,
                    rules["map_rules"],
                )
                by_amount = _checkpoint(
                    spark, by_amount, "alloc_pass1", cfg
                )
                if not by_amount.isEmpty():
                    by_amt_agg = by_amount.filter(
                        F.col("AllocationType").isin(
                            "Cost",
                            "CostAdjustedDatedTransfer",
                            "ProRata",
                            "DEFAULT",
                            "DefaultAdjustedDatedTransfer",
                            "Cost without Transfer Adj %",
                        )
                    ).groupBy(
                        "LineID",
                        "LineTypeID",
                        "EntityID",
                        "ParentEntityID",
                        "SuperParentEntityID",
                        "TrackingKey",
                        "AdjustmentTypeID",
                        "Tag",
                        "OriginalParentEntityID",
                    ).agg(
                        F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias(
                            "AllocatedAmount"
                        )
                    )
                    input_data = (
                        input_data.alias("L")
                        .join(
                            by_amt_agg.alias("AO"),
                            (
                                F.col("L.EntityID") == F.col("AO.EntityID")
                            )
                            & (
                                F.coalesce(
                                    F.col("L.ParentEntityID"), F.lit(0)
                                )
                                == F.coalesce(
                                    F.col("AO.ParentEntityID"), F.lit(0)
                                )
                            )
                            & (
                                F.coalesce(
                                    F.col("L.SuperParentEntityID"),
                                    F.lit(0),
                                )
                                == F.coalesce(
                                    F.col("AO.SuperParentEntityID"),
                                    F.lit(0),
                                )
                            )
                            & (
                                F.coalesce(
                                    F.col("L.TrackingKey"), F.lit("0")
                                )
                                == F.coalesce(
                                    F.col("AO.TrackingKey"), F.lit("0")
                                )
                            )
                            & (
                                F.coalesce(
                                    F.col("L.AdjustmentTypeID"), F.lit(0)
                                )
                                == F.coalesce(
                                    F.col("AO.AdjustmentTypeID"), F.lit(0)
                                )
                            )
                            & (F.col("L.LineID") == F.col("AO.LineID"))
                            & (
                                F.col("L.LineTypeID")
                                == F.col("AO.LineTypeID")
                            )
                            & (
                                F.coalesce(F.col("L.Tag"), F.lit(""))
                                == F.coalesce(F.col("AO.Tag"), F.lit(""))
                            ),
                            "left",
                        )
                        .withColumn(
                            "Amount",
                            F.when(
                                F.col("AO.AllocatedAmount").isNotNull(),
                                F.col("L.Amount")
                                - F.col("AO.AllocatedAmount"),
                            ).otherwise(F.col("L.Amount")),
                        )
                        .select(
                            F.col("L.RunID"),
                            F.col("L.ClientID"),
                            F.col("L.EntityID"),
                            F.col("L.LineTypeID"),
                            F.col("L.LineID"),
                            F.col("Amount"),
                            F.col("L.QuicklinkID"),
                            F.col("L.Amount704b"),
                            F.col("L.CategoryID"),
                            F.col("L.ParentEntityID"),
                            F.col("L.PeriodID"),
                            F.col("L.LineCode"),
                            F.col("L.SuperParentEntityID"),
                            F.col("L.AdjustmentTypeID"),
                            F.col("L.TrackingKey"),
                            F.col("L.Tag"),
                            F.col("L.OriginalParentEntityID"),
                            F.col("L.TransactionDate"),
                            F.col("L.TypeID"),
                            F.col("L.CustomTrackingKey"),
                            F.col("L.CustomTag"),
                            F.col("L.IsExcludefromTransfer"),
                            F.col("L.Classification"),
                            F.col("L.CapitalGainLoss"),
                        )
                    )
                    box_jkl_lti = cfg["box_jkl_line_type_id"]
                    input_data = input_data.filter(
                        ~(
                            (
                                F.coalesce(
                                    F.col("Amount"), F.lit(0)
                                ).between(-0.99, 0.99)
                            )
                            & (F.col("LineTypeID") != box_jkl_lti)
                        )
                    )
                    input_data = _checkpoint(
                        spark, input_data, "alloc_pass2", cfg
                    )
                fep_for_pct = (
                    final_eff_pct.alias("FEP")
                    .join(
                        F.broadcast(rules["default_rules"]).alias("DR"),
                        F.col("FEP.TypeID") == F.col("DR.RuleID"),
                    )
                    .join(
                        F.broadcast(
                            read_table(spark, "ENU_AllocationBy", cfg)
                        ).alias("EA"),
                        (
                            F.col("DR.AllocationByID")
                            == F.col("EA.AllocationByID")
                        )
                        & (F.lower(F.col("EA.AllocationBy")) == "amount"),
                    )
                    .select("FEP.TypeID")
                    .distinct()
                )
                final_eff_pct_filtered = final_eff_pct.join(
                    fep_for_pct, on="TypeID", how="left_anti"
                )
                final_eff_pct_filtered = _checkpoint(
                    spark, final_eff_pct_filtered, "alloc_pass3", cfg
                )
                by_pct = process_by_percentage_allocation(
                    spark,
                    cfg,
                    input_data,
                    final_eff_pct_filtered,
                    partners,
                )
                by_pct = _checkpoint(spark, by_pct, "alloc_pass4", cfg)
                allocation_output = by_amount.unionByName(by_pct)

            allocation_output = _checkpoint(
                spark, allocation_output, "alloc_output", cfg
            )

        with _timed(timings, "S6 sequential writes"):
            profile_action(
                "flush_result_tables",
                allocation_output,
                lambda: flush_result_tables(
                    spark, cfg, allocation_output
                ),
                cfg,
            )
            status["status"] = "SUCCESS"
    except Exception as exc:
        logger.error(
            f"[FAIL] uspLoadLookThroughCostAllocationToOutput: {exc}",
            exc_info=True,
        )
        status = {
            "sp": "uspLoadLookThroughCostAllocationToOutput",
            "status": "FAIL",
            "error": str(exc),
            "skip_reason": None,
        }
        raise
    finally:
        elapsed = round(time.time() - t0, 1)
        reports = _emit_reports(cfg) if isinstance(cfg, dict) else {}
        status["elapsed"] = elapsed
        status["elapsed_seconds"] = elapsed
        _LAST_RUN_PROFILE = {
            "timings": list(timings),
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
            f"skip_reason={status.get('skip_reason')} "
            f"elapsed={elapsed}s LineType={line_type!r}"
        )

    logger.info(f"[DONE] {status}")
    return status


__all__ = ["get_last_run_profile", "run_load_lookthrough_cost_alloc"]
