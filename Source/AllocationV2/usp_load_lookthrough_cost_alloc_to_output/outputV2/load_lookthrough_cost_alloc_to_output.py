"""Parity-first outputV2 conversion for look-through cost allocation output."""

from __future__ import annotations

import logging
import sys
import time

import pyspark.sql.functions as F
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
)
from Common_V2.core.checkpoint_V2 import (
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import read_table

from .parallel_helpers import (
    isolated_cfg,
    normalize_workers,
    run_distinct_writers,
    run_parallel,
)
from ._hierarchy import (
    build_entity_hierarchy,
    build_rule_ordered_underlyings,
)
from .parent import output_module
from .plan_profiler import (
    finish_action_profile,
    finish_checkpoint_plan_profile,
    finish_plan_profile,
    plan_profile_report,
    profile_action,
    start_action_profile,
    start_checkpoint_plan_profile,
    start_plan_profile,
    track_plan,
)

logger = logging.getLogger(__name__)

_prod = output_module("load_lookthrough_cost_alloc_to_output")
_loads = output_module("_data_loading")
_allocation = output_module("_allocation")

_load_sp_specific_config = _prod._load_sp_specific_config
load_workflow_ids = _prod.load_workflow_ids
write_allocation_output = _prod.write_allocation_output
update_input_table = _prod.update_input_table
_readd_footnote_source_lines = _prod._readd_footnote_source_lines
_build_k1_lineitems_704c = _prod._build_k1_lineitems_704c

load_partners = _loads.load_partners
load_line_items = _loads.load_line_items
load_lookthrough_input = _loads.load_lookthrough_input
load_allocation_rules = _loads.load_allocation_rules
load_cost_percentages = _loads.load_cost_percentages
apply_704c_to_k1_mappings = _loads.apply_704c_to_k1_mappings
load_book_effective_rules = _loads.load_book_effective_rules
add_footnote_inheritance = _loads.add_footnote_inheritance
load_final_effective_percentages = _loads.load_final_effective_percentages

prepare_lookthrough_input = _allocation.prepare_lookthrough_input
validate_by_amount_allocations = _allocation.validate_by_amount_allocations
handle_offset_types_704c = _allocation.handle_offset_types_704c
process_by_amount_allocation = _allocation.process_by_amount_allocation
process_by_percentage_allocation = _allocation.process_by_percentage_allocation
process_704c_allocation = _allocation.process_704c_allocation
insert_704c_by_amount_mapped_lines = (
    _allocation.insert_704c_by_amount_mapped_lines
)

for _name in (
    "load_partners",
    "load_line_items",
    "load_lookthrough_input",
    "load_allocation_rules",
    "load_cost_percentages",
    "apply_704c_to_k1_mappings",
    "build_entity_hierarchy",
    "build_rule_ordered_underlyings",
    "load_book_effective_rules",
    "add_footnote_inheritance",
    "prepare_lookthrough_input",
    "insert_704c_by_amount_mapped_lines",
    "load_final_effective_percentages",
    "handle_offset_types_704c",
    "process_by_amount_allocation",
    "process_by_percentage_allocation",
    "process_704c_allocation",
):
    globals()[_name] = track_plan(globals()[_name])


def _as_bool(value):
    if isinstance(value, str):
        return value.strip().lower() in {"1", "on", "true", "yes"}
    return bool(value)


def _checkpoint(spark, df, name, cfg):
    result = checkpoint(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if activity and activity[-1].get("backend") == "local":
        result = result.toDF(*result.columns)
    return result


# Isolated outputV2 hierarchy calls its module-global helper. Redirect to
# shared checkpoint_V2 so sequence, mode, and local alias reset stay aligned.
sys.modules[f"{__package__}._hierarchy"]._checkpoint = _checkpoint


def _emit_reports(enabled, threshold, sinks):
    reports = {"builder": [], "checkpoint": [], "action": []}
    if not enabled:
        return reports
    headings = (
        ("BUILDER", sinks[0], "builder", "where the plan grows"),
        ("CHECKPOINT", sinks[1], "checkpoint", "plan truncated at each checkpoint"),
        ("ACTION", sinks[2], "action", "materialization sites"),
    )
    for label, records, key, detail in headings:
        print(f"\n===== {label}-LEVEL PLAN PROFILE ({detail}) =====")
        reports[key] = plan_profile_report(records, threshold, label=label)
    return reports


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
    max_threads: int = 4,
    MaxThreads: int = None,
    profile_plan: bool = False,
    ProfilePlan=None,
    plan_checkpoint_threshold: int = 30,
    PlanCheckpointThreshold: int = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    **run_params,
) -> dict:
    """Run production business logic with isolated V2 optimizations."""
    started = time.perf_counter()
    workers = normalize_workers(max_threads, MaxThreads)
    profile_enabled = _as_bool(
        ProfilePlan if ProfilePlan is not None else profile_plan
    )
    threshold = int(
        PlanCheckpointThreshold
        if PlanCheckpointThreshold is not None
        else plan_checkpoint_threshold
    )
    parallel_activity = []
    plan_token = checkpoint_token = action_token = None
    builder_records = []
    checkpoint_records = []
    action_records = []

    entity_id = entity_id or EntityID
    client_id = client_id or ClientID
    tax_period_id = tax_period_id or TaxPeriodID
    run_id = run_id or RunID
    catalog = catalog or CatalogName
    schema = schema or SchemaName
    volume_path = volume_path or VolumePath or ""
    execution_id = execution_id or ExecutionID
    line_type = line_type or run_params.get("LineType", "") or ""
    rank_for_rule = (
        rank_for_rule or run_params.get("RankForRule", 0) or 0
    )
    result_type = (
        result_type
        or run_params.get("ResultType", "deltalake")
        or "deltalake"
    )
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if cfg is None:
        cfg = load_common_config(
            spark,
            run_id=run_id,
            entity_id=entity_id,
            client_id=client_id,
            tax_period_id=tax_period_id,
            catalog=catalog,
            schema=schema,
            **run_params,
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
        "result_type": result_type,
        "volume_path": volume_path,
        "execution_id": execution_id,
        "verbose": verbose,
        "line_type": line_type,
        "rank_for_rule": rank_for_rule,
        "profile_plan": profile_enabled,
        "plan_checkpoint_threshold": threshold,
        "checkpoint_mode": mode,
        "max_threads": workers,
    }
    cfg = _load_sp_specific_config(spark, cfg)
    initialize_checkpoint_V2(cfg, mode)
    print(
        f"[CHECKPOINT_V2] mode={mode}; [outputV2] "
        f"MaxThreads={workers} ProfilePlan={'on' if profile_enabled else 'off'}"
    )
    if profile_enabled:
        plan_token, builder_records = start_plan_profile()
        checkpoint_token, checkpoint_records = (
            start_checkpoint_plan_profile()
        )
        action_token, action_records = start_action_profile()

    status = {
        "sp": "uspLoadLookThroughCostAllocationToOutput",
        "status": "SUCCESS",
        "line_type": line_type,
    }
    try:
        cfg = load_workflow_ids(spark, cfg)
        if cfg is None:
            status["status"] = "SKIP"
            status["reason"] = (
                "RunStatus=FAIL or no eligible percentages"
            )
            return status

        # These five builders only read immutable cfg scalars and return
        # independent lazy plans. Per-task cfg copies isolate helper caches.
        (
            partners,
            line_items,
            input_data_raw,
            rules,
            cost_data,
        ) = run_parallel(
            [
                ("partners", lambda: load_partners(spark, isolated_cfg(cfg))),
                ("line_items", lambda: load_line_items(spark, isolated_cfg(cfg))),
                (
                    "lookthrough_input",
                    lambda: load_lookthrough_input(spark, isolated_cfg(cfg)),
                ),
                (
                    "allocation_rules",
                    lambda: load_allocation_rules(spark, isolated_cfg(cfg)),
                ),
                (
                    "cost_percentages",
                    lambda: load_cost_percentages(spark, isolated_cfg(cfg)),
                ),
            ],
            workers,
            parallel_activity,
            "lookthrough-load",
        )

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

        # The hierarchy recursion and all hierarchy checkpoints remain ordered.
        all_underlyings = build_entity_hierarchy(
            spark, cfg, cost_data["cost_percentages"]
        )
        all_underlyings = build_rule_ordered_underlyings(
            spark,
            cfg,
            all_underlyings,
            input_data_raw,
            rules["map_rules"],
            rules["default_rules"],
        )

        book_effective = load_book_effective_rules(spark, cfg)
        book_effective = add_footnote_inheritance(
            spark, cfg, book_effective, input_data_raw
        ).withColumn(
            "AdjustmentAllocationTypeID",
            F.when(
                F.col("AdjustmentAllocationTypeID")
                == cfg["book_allocation_type_id"],
                cfg["cost_allocation_type_id"],
            ).otherwise(F.col("AdjustmentAllocationTypeID")),
        )
        line_items = _readd_footnote_source_lines(
            spark, cfg, line_items, book_effective, all_underlyings
        )
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

        final_eff_pct = load_final_effective_percentages(spark, cfg)
        profile_action(
            "validate_by_amount_allocations",
            input_data,
            lambda: validate_by_amount_allocations(
                spark,
                cfg,
                input_data,
                final_eff_pct,
                partners,
                rules["default_rules"],
                rules["map_rules"],
            ),
            cfg,
        )

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
            k1_lineitems_704c = _build_k1_lineitems_704c(
                spark, cfg, input_data, line_items
            )
            k1_lineitems_704c = handle_offset_types_704c(
                spark, cfg, k1_lineitems_704c, allocation_percentages
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
            # By-amount mutates the logical input consumed by by-percentage;
            # these two phases intentionally remain sequential.
            by_amount = process_by_amount_allocation(
                spark,
                cfg,
                input_data,
                final_eff_pct,
                partners,
                rules["map_rules"],
            )
            by_amount_empty = profile_action(
                "by_amount.isEmpty",
                by_amount,
                by_amount.isEmpty,
                cfg,
            )
            if not by_amount_empty:
                by_amt_agg = (
                    by_amount.filter(
                        F.col("AllocationType").isin(
                            "Cost",
                            "CostAdjustedDatedTransfer",
                            "ProRata",
                            "DEFAULT",
                            "DefaultAdjustedDatedTransfer",
                            "Cost without Transfer Adj %",
                        )
                    )
                    .groupBy(
                        "LineID",
                        "LineTypeID",
                        "EntityID",
                        "ParentEntityID",
                        "SuperParentEntityID",
                        "TrackingKey",
                        "AdjustmentTypeID",
                        "Tag",
                        "OriginalParentEntityID",
                    )
                    .agg(
                        F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias(
                            "AllocatedAmount"
                        )
                    )
                )
                input_data = (
                    input_data.alias("L")
                    .join(
                        by_amt_agg.alias("AO"),
                        (F.col("L.EntityID") == F.col("AO.EntityID"))
                        & (
                            F.coalesce(F.col("L.ParentEntityID"), F.lit(0))
                            == F.coalesce(F.col("AO.ParentEntityID"), F.lit(0))
                        )
                        & (
                            F.coalesce(
                                F.col("L.SuperParentEntityID"), F.lit(0)
                            )
                            == F.coalesce(
                                F.col("AO.SuperParentEntityID"), F.lit(0)
                            )
                        )
                        & (
                            F.coalesce(F.col("L.TrackingKey"), F.lit("0"))
                            == F.coalesce(F.col("AO.TrackingKey"), F.lit("0"))
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
                        & (F.col("L.LineTypeID") == F.col("AO.LineTypeID"))
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
                            F.col("L.Amount") - F.col("AO.AllocatedAmount"),
                        ).otherwise(F.col("L.Amount")),
                    )
                    .select(
                        *[
                            (
                                F.col("Amount")
                                if column == "Amount"
                                else F.col(f"L.{column}")
                            )
                            for column in input_data.columns
                        ]
                    )
                )
                input_data = input_data.filter(
                    ~(
                        F.coalesce(F.col("Amount"), F.lit(0)).between(
                            -0.99, 0.99
                        )
                        & (
                            F.col("LineTypeID")
                            != cfg["box_jkl_line_type_id"]
                        )
                    )
                )

            fep_for_pct = (
                final_eff_pct.alias("FEP")
                .join(
                    rules["default_rules"].alias("DR"),
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
            by_pct = process_by_percentage_allocation(
                spark, cfg, input_data, final_eff_pct_filtered, partners
            )
            allocation_output = by_amount.unionByName(by_pct)

        # Preserve the production fan-out checkpoint and its exact name.
        allocation_output = _checkpoint(
            spark, allocation_output, "alloc_output", cfg
        )

        # Distinct target tables and isolated cfg dictionaries make these
        # existing mutations safe to run concurrently.
        write_cfg = isolated_cfg(cfg)
        update_cfg = isolated_cfg(cfg)
        write_cfg["_action_plan_profile"] = []
        update_cfg["_action_plan_profile"] = []
        run_distinct_writers(
            [
                (
                    "LookThroughAllocationOutput",
                    lambda: profile_action(
                        "write.LookThroughAllocationOutput",
                        allocation_output,
                        lambda: write_allocation_output(
                            spark, write_cfg, allocation_output
                        ),
                        write_cfg,
                    ),
                ),
                (
                    "LookThroughAllocationInput",
                    lambda: profile_action(
                        "write.LookThroughAllocationInput",
                        allocation_output,
                        lambda: update_input_table(
                            spark, update_cfg, allocation_output
                        ),
                        update_cfg,
                    ),
                ),
            ],
            parallel_activity,
        )
        if profile_enabled:
            action_records.extend(
                write_cfg.get("_action_plan_profile", ())
            )
            action_records.extend(
                update_cfg.get("_action_plan_profile", ())
            )
        status["elapsed"] = round(time.perf_counter() - started, 1)
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error("[FAIL] %s", exc, exc_info=True)
        raise
    finally:
        if profile_enabled and isinstance(cfg, dict):
            builder_records.extend(cfg.get("_plan_profile", ()))
        if action_token is not None:
            finish_action_profile(action_token)
        if checkpoint_token is not None:
            finish_checkpoint_plan_profile(checkpoint_token)
        if plan_token is not None:
            finish_plan_profile(plan_token)
        status["elapsed"] = round(time.perf_counter() - started, 1)
        status["parallel_activity"] = parallel_activity
        status["checkpoint_activity"] = (
            list(cfg.get("_checkpoint_v2_activity", ()))
            if isinstance(cfg, dict)
            else []
        )
        if profile_enabled and isinstance(cfg, dict):
            # ContextVars do not cross ThreadPoolExecutor boundaries. The
            # shared cfg lists collect records from pooled load/write tasks.
            builder_records.extend(cfg.get("_plan_profile", ()))
            action_records.extend(cfg.get("_action_plan_profile", ()))
        status["plan_profiles"] = _emit_reports(
            profile_enabled,
            threshold,
            (builder_records, checkpoint_records, action_records),
        )

    logger.info("[DONE] %s", status)
    return status


__all__ = [
    "checkpoint",
    "drop_checkpoints_V2",
    "run_load_lookthrough_cost_alloc",
]
