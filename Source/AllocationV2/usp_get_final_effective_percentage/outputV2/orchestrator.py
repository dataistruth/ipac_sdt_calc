"""
orchestrator.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Single entry point for Final Effective Percentage calculation.
Conversion date: 2026-05-04

Usage:
    from AllocationV2.usp_get_final_effective_percentage.outputV2.orchestrator import run_mode

    # Fused: modes 1+2+3 in a single invocation (returns 3 result DataFrames)
    out = run_mode(spark, mode=0, entity_id=152, client_id=15349,
                   tax_period_id=1, run_id=2093,
                   catalog="QA7", schema="iPC_2025_QA7_15347")
    df1, df2, df3 = out["results"][1], out["results"][2], out["results"][3]

    # Mode 4 (704c) -- standalone
    out = run_mode(spark, mode=4, cfg=cfg)
    df4 = out["results"][4]

    # Single-mode calls (mode=1/2/3) raise ValueError. Use mode=0 instead.

=======================================================================
MULTI-PHASE FUSION REFACTOR -- branch main_Raja_Pyspark_fineff_v2
=======================================================================
Goal:  Run cost_pct_by_type / effective_calc / plugging ONCE for fused
       mode=0 (instead of 3x per-mode loop) by tagging every row with
       a `_mode` column and adding `_mode` to all join predicates.

Background:  The SQL stored proc is invoked 4x (once per mode) and each
invocation has independent #TempInputLines / #TempDatedEntities etc. The
current PySpark mirror runs cost_pct_by_type 3x inside a per-mode loop
because mode 1/2/3 inputs differ (mode 2 augments via footnote, mode 3
augments via state allocation). Naively unioning the per-mode inputs
would corrupt mode 1's parent-hierarchy matching by exposing it to
footnote/state rows that wouldn't appear in a sequential mode-1 SP call.

The fix is to tag rows with `_mode` and require `_mode` equality in
every join, so the fused frames behave like 3 isolated frames inside
a single Spark plan.

Phases:
  Phase 1 (current): regression test infrastructure + this docstring.
                     No production-code changes. Establishes the
                     correctness gate for Phases 2-5.
  Phase 2:  build_cost_percentage_by_type -> _mode-aware. Run once on
            unioned (m1 + m2 + m3) inputs. Re-run regression test.
  Phase 3:  compute_effective_percentage_dated / non_dated /
            apply_plugging / apply_type_id_update -> _mode-aware.
  Phase 4:  build_final_output -> filter by _mode, produce 3 results.
            Orchestrator's per-mode loop collapses to a single fused
            call.
  Phase 5:  End-to-end regression verification + perf benchmark.

Correctness gate:  Source/UnitTest/test_fusion_regression.py
  Golden baseline (entity 152, client 15349, period 1, run 2093):
      mode 1 -> 47,210 rows
      mode 2 ->  9,030 rows
      mode 3 ->      0 rows (no SM_LookThroughAllocationInput data)

Each phase MUST keep this test passing. A row-count mismatch in any mode
is a hard fail and indicates a missing `_mode` predicate somewhere.
=======================================================================
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import read_table, ns, ns0
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.execution_profiles import resolve_execution_profile
from AllocationV2.usp_get_final_effective_percentage.outputV2.parallel_helpers import (
    isolated_cfg,
    normalize_workers,
    parse_enabled_groups,
    run_phase,
)

from AllocationV2.usp_get_final_effective_percentage.output.config_loader import (
    load_config,
)
from AllocationV2.usp_get_final_effective_percentage.output.input_builder import (
    build_allocation_input,
    build_sm_lookthrough_allocation_input,
    build_lookthrough_allocation_input,
)
from AllocationV2.usp_get_final_effective_percentage.outputV2.entity_hierarchy import (
    build_entity_partners,
    build_asset_class_relationship,
    build_cost_underlying_types,
    build_entity_hierarchy,
    build_underlyings_combined,
)
from AllocationV2.usp_get_final_effective_percentage.output.cost_percentage import (
    build_cost_percentage_snapshot_modes123,
    build_cost_percentage_snapshot_mode4,
    build_mode1_704c_pe_book_allocations,
    build_temp_cost_percentage,
)
from AllocationV2.usp_get_final_effective_percentage.output.book_effective import (
    load_allocation_rules,
    load_line_items,
    load_book_effective_data,
    load_yearly_lines,
    load_quarters,
    load_yearly_data,
    build_lookthrough_input_modes14,
    build_footnote_lines,
    build_footnote_book_effective,
)
from AllocationV2.usp_get_final_effective_percentage.output.underlyings import (
    filter_asset_class_underlyings,
    build_underlyings_hlevel_ordered,
    build_underlying_mod,
    build_all_underlyings_ordered,
)
from AllocationV2.usp_get_final_effective_percentage.output.input_lines import (
    build_input_lines,
    compute_amount_based_allocation,
)
from AllocationV2.usp_get_final_effective_percentage.output.entities import (
    build_non_dated_entities,
    build_dated_entities,
)
from AllocationV2.usp_get_final_effective_percentage.outputV2.pfic_footnotes import (
    build_footnote_underlyings_ordered,
    build_footnote_input_lines,
    build_footnote_dated_entities,
    _get_custom_footnote_line_types,
)
from AllocationV2.usp_get_final_effective_percentage.output.form199a import (
    compute_form199a_effective_percentage,
)
from AllocationV2.usp_get_final_effective_percentage.outputV2.state_allocation import (
    build_state_allocation_input,
    build_state_entities,
)
from AllocationV2.usp_get_final_effective_percentage.outputV2.cost_pct_loader import (
    build_entity_underlyings,
    load_transfers_adj_cost,
    build_cost_percentage_by_type,
    compute_missing_entities,
    build_final_cost_percentage,
    validate_cost_percentage_sum,
    compute_minimum_quarter,
)
from AllocationV2.usp_get_final_effective_percentage.outputV2.effective_calc import (
    compute_effective_percentage_dated,
    compute_effective_percentage_non_dated,
    apply_plugging,
    apply_type_id_update,
    build_final_output,
)
from AllocationV2.usp_get_final_effective_percentage.output.result_saver import (
    build_all_results,
)
from Common_V2.core.generic_result_storer import GenericResultStorer
from AllocationV2.usp_get_final_effective_percentage.outputV2.plan_profiler import (
    track_plan as _track_plan,
)

logger = logging.getLogger(__name__)

# Development-only wraps. Production output/ modules stay untouched.
# track_plan is a no-op unless ProfilePlan armed this run.
for _builder_name in (
    "build_allocation_input",
    "build_sm_lookthrough_allocation_input",
    "build_lookthrough_allocation_input",
    "build_entity_partners",
    "build_asset_class_relationship",
    "build_cost_underlying_types",
    "build_entity_hierarchy",
    "build_underlyings_combined",
    "build_cost_percentage_snapshot_modes123",
    "build_cost_percentage_snapshot_mode4",
    "build_mode1_704c_pe_book_allocations",
    "build_temp_cost_percentage",
    "load_allocation_rules",
    "load_line_items",
    "load_book_effective_data",
    "load_yearly_lines",
    "load_quarters",
    "load_yearly_data",
    "build_lookthrough_input_modes14",
    "build_footnote_lines",
    "build_footnote_book_effective",
    "filter_asset_class_underlyings",
    "build_underlyings_hlevel_ordered",
    "build_underlying_mod",
    "build_all_underlyings_ordered",
    "build_input_lines",
    "compute_amount_based_allocation",
    "build_non_dated_entities",
    "build_dated_entities",
    "build_footnote_underlyings_ordered",
    "build_footnote_input_lines",
    "build_footnote_dated_entities",
    "compute_form199a_effective_percentage",
    "build_state_allocation_input",
    "build_state_entities",
    "build_entity_underlyings",
    "load_transfers_adj_cost",
    "build_cost_percentage_by_type",
    "compute_missing_entities",
    "build_final_cost_percentage",
    "compute_minimum_quarter",
    "compute_effective_percentage_dated",
    "compute_effective_percentage_non_dated",
    "apply_plugging",
    "apply_type_id_update",
    "build_final_output",
    "build_all_results",
):
    _fn = globals().get(_builder_name)
    if callable(_fn):
        globals()[_builder_name] = _track_plan(_fn)

if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)



def _start_dev_plan_profile(enabled):
    from .plan_profiler import disable_plan_profile

    if not enabled:
        disable_plan_profile()
        return None
    from .plan_profiler import (
        start_action_profile,
        start_checkpoint_plan_profile,
        start_plan_profile,
    )
    return (
        start_plan_profile(),
        start_checkpoint_plan_profile(),
        start_action_profile(),
    )


def _finish_dev_plan_profile(tokens, threshold=30, cfg=None):
    from .plan_profiler import disable_plan_profile, store_last_plan_profile

    if not tokens:
        disable_plan_profile()
        return
    from .plan_profiler import (
        finish_action_profile,
        finish_checkpoint_plan_profile,
        finish_plan_profile,
        plan_profile_report,
    )

    def _pair(item):
        if isinstance(item, tuple) and len(item) >= 2:
            return item[0], list(item[1] or [])
        return item, []

    plan_token, cp_token, action_token = tokens
    plan_tok, builder = _pair(plan_token)
    cp_tok, checkpoints = _pair(cp_token)
    action_tok, actions = _pair(action_token)
    finish_action_profile(action_tok)
    finish_checkpoint_plan_profile(cp_tok)
    finish_plan_profile(plan_tok)
    if isinstance(cfg, dict):
        builder = builder + list(cfg.get("_plan_profile") or [])
        checkpoints = checkpoints + list(cfg.get("_checkpoint_plan_profile") or [])
        actions = actions + list(cfg.get("_action_plan_profile") or [])
    builder_rep = plan_profile_report(builder, threshold, label="BUILDER") or []
    ckpt_rep = plan_profile_report(checkpoints, threshold, label="CHECKPOINT") or []
    action_rep = plan_profile_report(actions, threshold, label="ACTION") or []
    store_last_plan_profile(builder_rep, ckpt_rep, action_rep)
    disable_plan_profile()


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


_PROMOTED_EXPERIMENT_DEFAULTS = {
    "sql_shuffle_partitions": 32,
    "warning_probe_removal": "off",
    "missing_entity_identity": True,
    "cpbt_input_break": "both",
    "cpbt_post_tag_entity_break": False,
    "parallel_cpbt_post_tag": True,
    "compact_all_entities": True,
    "cpbt_narrow_anti_keys": True,
    "cpbt_transfer_prefilter": True,
    "cpbt_drop_tracking_match": True,
    "batch_footnote_line_ids": True,
    "footnote_shared_lineage": False,
    "footnote_checkpoint_partitions": 4,
    "broadcast_cpbt_remaining": True,
    "hierarchy_materialize": True,
    "parallel_cpbt_validate": True,
    "skip_yearly_empty_probe": False,
    "collapse_state_passes": True,
    "batch_state_workflow_lookup": True,
    "single_pickup_antijoin": True,
    "materialize_effective_inputs": True,
    "parallel_effective": True,
    "business_optimization": "broadcast_entity_partners",
}


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


def _apply_promoted_flags(cfg):
    defaults = _PROMOTED_EXPERIMENT_DEFAULTS
    cfg["_output_v3_missing_entity_identity"] = defaults["missing_entity_identity"]
    cfg["_output_v3_cpbt_input_break"] = defaults["cpbt_input_break"]
    cfg["_output_v3_cpbt_post_tag_entity_break"] = defaults["cpbt_post_tag_entity_break"]
    cfg["_output_v3_parallel_cpbt_post_tag"] = defaults["parallel_cpbt_post_tag"]
    cfg["_output_v3_compact_all_entities"] = defaults["compact_all_entities"]
    cfg["_output_v3_cpbt_narrow_anti_keys"] = defaults["cpbt_narrow_anti_keys"]
    cfg["_output_v3_cpbt_transfer_prefilter"] = defaults["cpbt_transfer_prefilter"]
    cfg["_output_v3_cpbt_drop_tracking_match"] = defaults["cpbt_drop_tracking_match"]
    cfg["_output_v3_batch_footnote_line_ids"] = defaults["batch_footnote_line_ids"]
    cfg["_output_v3_footnote_shared_lineage"] = defaults["footnote_shared_lineage"]
    cfg["_output_v3_footnote_checkpoint_partitions"] = defaults[
        "footnote_checkpoint_partitions"
    ]
    cfg["_output_v3_broadcast_cpbt_remaining"] = defaults["broadcast_cpbt_remaining"]
    cfg["_output_v3_hierarchy_materialize"] = defaults["hierarchy_materialize"]
    cfg["_output_v3_parallel_cpbt_validate"] = defaults["parallel_cpbt_validate"]
    cfg["_output_v3_skip_yearly_empty_probe"] = defaults["skip_yearly_empty_probe"]
    cfg["_output_v3_collapse_state_passes"] = defaults["collapse_state_passes"]
    cfg["_output_v3_batch_state_workflow_lookup"] = defaults[
        "batch_state_workflow_lookup"
    ]
    cfg["_output_v3_single_pickup_antijoin"] = defaults["single_pickup_antijoin"]
    cfg["_output_v3_materialize_effective_inputs"] = defaults[
        "materialize_effective_inputs"
    ]
    cfg["_output_v3_parallel_effective"] = defaults["parallel_effective"]
    cfg["_output_v3_business_optimization"] = defaults["business_optimization"]
    return cfg


def _checkpoint(spark, df, name, cfg):
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = checkpoint_V2(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    return result


def _drop_checkpoints(spark, cfg):
    if cfg.get("drop_checkpoints"):
        drop_checkpoints_V2(spark, cfg)


def _save_results(spark, cfg, statuses):
    """Save mode results to Delta/Parquet/SQL using GenericResultStorer."""
    if cfg.get("_skip_save"):
        logger.info("[SAVE] _skip_save=True -- skipping result save")
        return

    results_dict = {
        m: s["result"] for m, s in statuses.items()
        if s.get("result") is not None
    }
    all_requested_modes = list(statuses.keys())

    output_tables = build_all_results(
        spark, cfg, results_dict,
        all_requested_modes=all_requested_modes,
    )
    if not output_tables:
        logger.info("[SAVE] No output tables built")
        return

    # Enforce column types to match Delta table schemas.
    _cast_map = {
        "TypeId": "int",
        "InvestmentID": "int",
        "GPPartnerReceivingCarry": "boolean",  # FinalEffectivePercentages
        "IsExcludefromTransfer": "boolean",     # all target tables
    }
    for tbl_name, tbl_df in output_tables.items():
        for col_name, target_type in _cast_map.items():
            if col_name in tbl_df.columns:
                tbl_df = tbl_df.withColumn(col_name, F.col(col_name).cast(target_type))
        output_tables[tbl_name] = tbl_df

    result_type = cfg.get("result_type", "deltalake")
    workers = int(cfg.get("max_threads") or 4)
    enabled_groups = cfg.get("_enabled_groups") or parse_enabled_groups("all")

    def _write_one(tbl_name, tbl_df):
        local = isolated_cfg(cfg)
        storer = GenericResultStorer(spark)
        return storer.save_results(
            result={tbl_name: tbl_df},
            result_type=result_type,
            catalog_name=local["catalog"],
            database_name=local["schema"],
            run_id=local["run_id"],
            client_id=local["client_id"],
            entity_id=local["entity_id"],
            execution_id=local.get("execution_id", ""),
            volume_path=local.get("volume_path", ""),
            sql_url_path=local.get("sql_url_path", ""),
            sql_username=local.get("sql_username", ""),
            sql_password=local.get("sql_password", ""),
        )

    written = dict(
        run_phase(
            "output_writes",
            [
                (tbl_name, _write_one, (tbl_name, tbl_df), {})
                for tbl_name, tbl_df in output_tables.items()
            ],
            workers,
            enabled_groups,
        )
    )
    logger.info(
        f"[SAVE] Results saved ({result_type}): {list(output_tables.keys())}"
    )
    return next((v for v in written.values() if v), None)


# ===============================================================
# Multi-mode entry point: run_modes
# ===============================================================

def run_modes(
    spark: SparkSession,
    modes: list,
    entity_id: int = None,
    client_id: int = None,
    tax_period_id: int = None,
    run_id: int = None,
    catalog: str = None,
    schema: str = None,
    cfg: dict = None,
    verbose: bool = False,
    ResultType: str = "deltalake",
    VolumePath: str = None,
    ExecutionID: str = None,
    max_threads: int = None,
    MaxThreads: int = None,
    parallel_groups: str = "all",
    ParallelGroups: str = None,
    execution_profile: str = "low",
    ExecutionProfile: str = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    SqlShufflePartitions=None,

    profile_plan: bool = False,
    plan_checkpoint_threshold: int = 30,
) -> dict:
    """Run Final Effective Percentage for one or more modes.

    Typical usage:
        # Run 1: modes 1, 2, 3 together (shared config, one load_config call)
        result = run_modes(spark, modes=[1, 2, 3], entity_id=144, ...)

        # Run 2: mode 4 separately
        result = run_modes(spark, modes=[4], entity_id=144, ...)

    Args:
        modes: list of modes to run, e.g. [1, 2, 3] or [4]
        (remaining args same as run_mode)

    Returns:
        dict with keys:
            statuses: {mode: status_dict} for each mode
            elapsed_seconds: total wall-clock time
            _checkpoint_tables: all checkpoint tables (for manual cleanup)
    """
    if isinstance(modes, int):
        modes = [modes]

    for m in modes:
        if m not in (1, 2, 3, 4):
            raise ValueError(f"mode must be 1, 2, 3, or 4 -- got {m}")

    t0 = time.time()
    has_mode4 = 4 in modes
    modes_123 = [m for m in modes if m != 4]

    if verbose:
        logger.setLevel(logging.DEBUG)

    # Build shared config ONCE
    # Mode 3 standalone: call load_common_config from IDs.
    # Modes 1/2 (Job/Orchestrator): cfg is passed in pre-built.
    if cfg is None:
        cfg = load_common_config(
            spark,
            entity_id=entity_id,
            client_id=client_id,
            tax_period_id=tax_period_id,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
        )
    cfg.setdefault("_checkpoint_tables", [])
    cfg.setdefault("_checkpoint_paths", [])
    cfg.setdefault("_checkpoint_v2_activity", [])

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
    shuffle_override = _as_int(SqlShufflePartitions)
    original_shuffle = spark.conf.get("spark.sql.shuffle.partitions")
    if shuffle_override is None:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            str(profile["shuffle_partitions"]),
        )
    else:
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_override))
    explicit_checkpoint = (
        CheckpointMode if CheckpointMode is not None else checkpoint_mode
    )
    mode_v2 = resolve_checkpoint_mode(
        cfg,
        checkpoint_mode=(
            explicit_checkpoint
            if explicit_checkpoint is not None
            else profile["checkpoint_mode"]
        ),
        CheckpointMode=CheckpointMode,
    )
    cfg["checkpoint_mode"] = mode_v2
    cfg["max_threads"] = workers
    cfg["execution_profile"] = profile_name
    cfg["_enabled_groups"] = enabled_groups
    initialize_checkpoint_V2(cfg, mode_v2)
    cfg["profile_plan"] = bool(profile_plan)
    _plan_tokens = _start_dev_plan_profile(profile_plan)
    _apply_promoted_flags(cfg)
    print(
        f"[CHECKPOINT_V2] ExecutionProfile={profile_name} "
        f"CheckpointMode={mode_v2} shuffle="
        f"{shuffle_override or profile['shuffle_partitions']} "
        f"MaxThreads={workers}"
    )

    # Pass through output options (same pattern as apply_investment_level_rounding)
    cfg.setdefault("result_type", ResultType)
    if VolumePath is not None:
        cfg["volume_path"] = VolumePath
    if ExecutionID is not None:
        cfg["execution_id"] = ExecutionID

    statuses = {}
    save_return_value = None

    try:
        # --- Config (once) ---
        if not cfg.get("_config_loaded"):
            load_config(spark, cfg)
            cfg["_config_loaded"] = True

        # --- Common Phase: Cost percentage snapshot ---
        # Modes 1-3 share the same snapshot; mode 4 uses a different one.
        # Compute the snapshot(s) needed for the requested modes.
        cost_pct_snapshot_123 = None
        cost_pct_snapshot_4 = None
        # 704c PE-Book artifacts (Mode 1 only; populated by
        # build_mode1_704c_pe_book_allocations when applicable). Stashed so
        # downstream functions (input_lines, load_allocation_rules) can
        # augment their own DataFrames.
        _704c_peb = None
        if modes_123:
            cost_pct_snapshot_123 = build_cost_percentage_snapshot_modes123(spark, cfg)

            # ── Gap A: Mode 1 + 704c PE-Book custom allocation block ─────
            # SQL lines 1941-2256. Only fires for mode 1 with a non-empty
            # _704c_allocation_type_name (set by config_loader from
            # ENU_704cAllocationLogic). UNIONs custom 'Special <field>'
            # rows into the snapshot and emits map_dar/dar_setup additions
            # that will be merged after load_allocation_rules.
            if 1 in modes_123 and cfg.get("_704c_allocation_type_name"):
                _prev_mode = cfg.get("mode")
                cfg["mode"] = 1
                try:
                    _704c_peb = build_mode1_704c_pe_book_allocations(
                        spark, cfg, cost_pct_function_df=None,
                    )
                finally:
                    if _prev_mode is None:
                        cfg.pop("mode", None)
                    else:
                        cfg["mode"] = _prev_mode
                if _704c_peb is not None:
                    cost_pct_snapshot_123 = cost_pct_snapshot_123.unionByName(
                        _704c_peb["snapshot_augment"],
                        allowMissingColumns=True,
                    )
                    cfg["has_704c_mappings"] = True
                    # Stash mappings for Gap B (input_lines variant split).
                    cfg["_704c_mappings_df"] = _704c_peb["mappings"]
                    logger.info(
                        "[704c-PE-Book] Augmenting cost_pct_snapshot, map_dar, dar_setup"
                    )
                else:
                    cfg["has_704c_mappings"] = False
            else:
                cfg["has_704c_mappings"] = False

            cost_pct_snapshot_123 = _checkpoint(
                spark, cost_pct_snapshot_123, "cost_pct_m123", cfg,
            )
        if has_mode4:
            cost_pct_snapshot_4 = build_cost_percentage_snapshot_mode4(spark, cfg)
            cost_pct_snapshot_4 = _checkpoint(
                spark, cost_pct_snapshot_4, "cost_pct_m4", cfg,
            )

        # --- Common Phase: Entity hierarchy (mode-independent) ---
        entity_partners = build_entity_partners(spark, cfg)
        if cfg.get("_output_v3_business_optimization") == "broadcast_entity_partners":
            entity_partners = F.broadcast(entity_partners)

        # Entity hierarchy uses cost_pct_snapshot. For modes [1,2,3] use 123;
        # for [4] use 4. If both present, use 123 (superset).
        _hierarchy_snapshot = cost_pct_snapshot_123 or cost_pct_snapshot_4
        cost_underlying_types = build_cost_underlying_types(
            spark, cfg, _hierarchy_snapshot,
        )
        entity_hierarchy = build_entity_hierarchy(spark, cfg, cost_underlying_types)
        asset_class_rel = build_asset_class_relationship(spark, cfg)

        # --- Common Phase: Underlyings combined ---
        underlyings_combined = build_underlyings_combined(
            spark, cfg, cost_underlying_types, entity_hierarchy, _hierarchy_snapshot,
        )

        # --- Common Phase: Allocation rules, book effective, line items ---
        dar_setup, map_dar, entity_alloc_rule = load_allocation_rules(spark, cfg)

        # ── Gap A (cont.): merge 704c PE-Book TransactionID=-2 rows into
        # the freshly loaded dar_setup / map_dar. This mirrors the SQL
        # INSERTs at lines 2197-2205 into #MapDefaultAllocRuleToLineItem
        # and #DefaultAllocationRuleSetup.
        if _704c_peb is not None:
            map_dar = map_dar.unionByName(
                _704c_peb["map_dar_704c"], allowMissingColumns=True,
            )
            dar_setup = dar_setup.unionByName(
                _704c_peb["dar_setup_704c"], allowMissingColumns=True,
            )
            logger.info(
                "[704c-PE-Book] map_dar + dar_setup augmented with TransactionID=-2 rows"
            )

        line_items = load_line_items(spark, cfg)
        book_effective_raw = load_book_effective_data(spark, cfg)
        yearly_lines = load_yearly_lines(book_effective_raw, cfg)
        quarters = load_quarters(spark, cfg)
        yearly_data = load_yearly_data(spark, cfg)

        # --- Common Phase: Underlyings ordering ---
        underlyings_filtered = filter_asset_class_underlyings(
            spark, cfg, underlyings_combined, asset_class_rel,
        )
        underlyings_ordered = build_underlyings_hlevel_ordered(underlyings_filtered)
        underlyings_ordered = _checkpoint(
            spark, underlyings_ordered, "uc_ordered_common", cfg,
        )

        # --- Common Phase: LT input, footnote lines, book effective enrichment ---
        lt_input_m14 = build_lookthrough_input_modes14(spark, cfg)
        footnote_lines = build_footnote_lines(spark, cfg)
        book_effective = build_footnote_book_effective(
            lt_input_m14, footnote_lines, book_effective_raw, cfg,
        )

        logger.info(f"[COMMON] Shared pipeline complete for modes {modes}")

        # ==========================================================
        # Mode-specific pipeline (Phase 2b -- fused):
        #   Mode 4: independent single-pass (different snapshot, no fusion).
        #   Modes 1+2+3: 3-pass fused pipeline.
        #     Pass A -- per-mode input prep (loops over modes_123)
        #     Pass B -- ONE build_cost_percentage_by_type call on unioned inputs
        #     Pass C -- per-mode downstream (loops over modes_123)
        # ==========================================================

        from functools import reduce as _reduce

        def _tag_mode(df, m):
            """Tag a DataFrame with a literal _mode column."""
            if df is None:
                return None
            return df.withColumn("_mode", F.lit(m))

        def _fold_union(dfs):
            """Reduce a list of DataFrames via unionByName(allowMissingColumns=True).
            Returns None if all inputs are None."""
            real = [d for d in dfs if d is not None]
            if not real:
                return None
            return _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), real)

        def _build_yearly_cost_rows():
            """Build the yearly cross-join rows used by every mode's temp_cost_pct.
            Returns None if either source is empty."""
            if yearly_lines.isEmpty() or yearly_data.isEmpty():
                return None
            return (
                yearly_lines.alias("Y")
                .crossJoin(F.broadcast(yearly_data.alias("YS")))
                .crossJoin(F.broadcast(quarters.alias("Q")))
                .select(
                    F.col("Y.UnderlyingEntityID").alias("DealId"),
                    F.col("YS.PartnerNumber").alias("Partnernumber"),
                    F.col("Q.Quarter"),
                    F.coalesce(F.col("YS.ProRataEffOwnPercent"), F.lit(0.0)).alias("CommitmentPercent"),
                    F.col("Y.AdjustmentAllocationTypeID").alias("TypeId"),
                    F.lit("").alias("TrackingKey"),
                    F.lit("").alias("Tag"),
                    F.lit(None).cast("int").alias("704cAllocationTypeID"),
                    F.lit(None).cast("string").alias("704cPercentageType"),
                    F.lit(None).cast("boolean").alias("GPPartnerReceivingCarry"),
                )
                .distinct()
            )

        # ?*"==========================================================?*--
        # + Common Phase 2 -- modes 1+2+3 base data (SP-correct)       +
        # +                                                            +
        # + Path A (SP-exact): SP populates #TempLookThroughAllocation +
        # + Input ONLY for `@Mode IN (1, 4)`. So mode 2 and mode 3     +
        # + see an EMPTY lt input downstream. Building one shared base +
        # + with populated lt would inject extra rows for modes 2/3.   +
        # +                                                            +
        # + We build TWO chains for the lt-dependent functions:        +
        # +   chain_with_lt    -- populated lt_input_m14, used by mode 1 +
        # +   chain_without_lt -- empty lt_input,         used by modes  +
        # +                       2 and 3                                +
        # +                                                            +
        # + lt-INDEPENDENT functions (temp_cost_pct, underlying_mod)   +
        # + are still built once and shared.                           +
        # +                                                            +
        # + Mode 4 has its own pipeline below (different snapshot,     +
        # + uses populated lt_input_m14 -- that matches SP).            +
        # ?*s==========================================================?*?
        common_temp_cost_pct_base = None
        common_underlying_mod = None
        chain_with_lt = None      # for mode 1
        chain_without_lt = None   # for modes 2, 3

        def _build_lt_dependent_chain(lt_in, label):
            """Build the lt-input-dependent chain of functions.

            Returns dict with keys: all_underlyings, input_lines,
            final_amounts, non_dated_entities, dated_entities,
            entity_underlyings.
            """
            logger.info(f"[COMMON-2 chain={label}] start")
            all_und, _ = build_all_underlyings_ordered(
                spark, cfg, common_underlying_mod, lt_in, book_effective,
                entity_alloc_rule, dar_setup, map_dar, cost_pct_snapshot_123,
            )
            all_und = _checkpoint(spark, all_und, f"all_und_common_{label}", cfg)

            in_lines, _, _ = build_input_lines(
                spark, cfg, lt_in, line_items, book_effective,
                entity_alloc_rule, all_und,
            )
            in_lines = _checkpoint(spark, in_lines, f"input_lines_{label}", cfg)

            fin_amts, all_und = compute_amount_based_allocation(
                spark, cfg, all_und, cost_pct_snapshot_123, lt_in, map_dar,
            )

            non_dated = build_non_dated_entities(in_lines, line_items, cfg)
            dated = build_dated_entities(spark, cfg, in_lines, line_items)

            ent_und = build_entity_underlyings(
                spark, cfg, in_lines, underlyings_ordered, asset_class_rel,
            )
            ent_und = _checkpoint(spark, ent_und, f"entity_und_common_{label}", cfg)

            logger.info(f"[COMMON-2 chain={label}] complete")
            return {
                "all_underlyings":   all_und,
                "input_lines":       in_lines,
                "final_amounts":     fin_amts,
                "non_dated_entities": non_dated,
                "dated_entities":    dated,
                "entity_underlyings": ent_und,
            }

        if modes_123:
            # cfg["mode"] context: any non-4 value works because the underlyings
            # helpers branch on `mode == 4`. Use a sentinel for checkpoint paths.
            cfg["mode"] = 1
            cfg["_current_mode"] = 0   # sentinel -- checkpoint names use "common"

            logger.info("[COMMON-2] Building shared base data for modes 1+2+3")

            # --- lt-INDEPENDENT functions (used by both chains) ---
            # temp_cost_pct + yearly cross-join union (SP #TempCostPercentage)
            common_temp_cost_pct_base = build_temp_cost_percentage(
                spark, cfg, cost_pct_snapshot_123,
            )
            _yearly_cost_rows = _build_yearly_cost_rows()
            if _yearly_cost_rows is not None:
                common_temp_cost_pct_base = common_temp_cost_pct_base.unionByName(_yearly_cost_rows)
                common_temp_cost_pct_base = _checkpoint(
                    spark, common_temp_cost_pct_base, "tcp_with_yearly_common", cfg,
                )

            # underlying_mod (no equivalent named SP table -- inline join,
            # not lt-dependent)
            common_underlying_mod = build_underlying_mod(
                underlyings_ordered, cost_pct_snapshot_123,
            )

            # --- lt-DEPENDENT chains ---
            # Chain WITH populated lt_input -- used by mode 1 (SP populates
            # #TempLookThroughAllocationInput when @Mode = 1).
            chain_tasks = []
            if 1 in modes_123:
                chain_tasks.append(
                    (
                        "lt",
                        _build_lt_dependent_chain,
                        (lt_input_m14, "lt"),
                        {},
                    )
                )
            if 2 in modes_123 or 3 in modes_123:
                lt_input_empty = lt_input_m14.limit(0)
                chain_tasks.append(
                    (
                        "nolt",
                        _build_lt_dependent_chain,
                        (lt_input_empty, "nolt"),
                        {},
                    )
                )
            built_chains = dict(
                run_phase(
                    "independent_builders",
                    chain_tasks,
                    workers,
                    enabled_groups,
                )
            )
            chain_with_lt = built_chains.get("lt")
            chain_without_lt = built_chains.get("nolt")

            logger.info("[COMMON-2] Shared base data complete (modes 1+2+3)")

        # ?*"==========================================================?*--
        # + Mode 4 (if requested) -- independent                       +
        # ?*s==========================================================?*?
        if has_mode4:
            mode = 4
            mt0 = time.time()
            cfg["mode"] = 4
            cfg["_current_mode"] = 4
            cost_pct_snapshot = cost_pct_snapshot_4

            logger.info(f"[START] mode 4 within run_modes({modes})")

            mode_status = {
                "sp_name": "uspGetFinalEffectivePercentage",
                "mode": 4,
                "status": "SUCCESS",
                "error": None,
                "elapsed_seconds": 0,
            }

            try:
                alloc_input = build_allocation_input(spark, cfg, modes=[4])
                lt_input = build_lookthrough_allocation_input(spark, cfg)
                _alloc_empty = alloc_input is None or alloc_input.isEmpty()
                _lt_empty = lt_input is None or lt_input.isEmpty()
                cfg["_alloc_empty"] = _alloc_empty
                cfg["_lt_empty"] = _lt_empty
                cfg["_sm_empty"] = True
                cfg["_inputs_empty"] = _alloc_empty and _lt_empty

                if cfg["_inputs_empty"]:
                    logger.info(
                        "[FAST-EXIT] mode 4: alloc_input+lt_input empty -- "
                        "skipping compute and returning result=None."
                    )
                    mode_status["result"] = None
                else:
                    temp_cost_pct = build_temp_cost_percentage(spark, cfg, cost_pct_snapshot)
                    yearly_cost_rows = _build_yearly_cost_rows()
                    if yearly_cost_rows is not None:
                        temp_cost_pct = temp_cost_pct.unionByName(yearly_cost_rows)
                        temp_cost_pct = _checkpoint(spark, temp_cost_pct, "tcp_with_yearly_m4", cfg)

                    underlying_mod = build_underlying_mod(underlyings_ordered, cost_pct_snapshot)
                    all_underlyings, _ = build_all_underlyings_ordered(
                        spark, cfg, underlying_mod, lt_input_m14, book_effective,
                        entity_alloc_rule, dar_setup, map_dar, cost_pct_snapshot,
                    )
                    all_underlyings = _checkpoint(spark, all_underlyings, "all_und_m4", cfg)

                    input_lines, _, _ = build_input_lines(
                        spark, cfg, lt_input_m14, line_items, book_effective,
                        entity_alloc_rule, all_underlyings,
                    )
                    final_amounts, all_underlyings = compute_amount_based_allocation(
                        spark, cfg, all_underlyings, cost_pct_snapshot, lt_input_m14, map_dar,
                    )

                    non_dated_entities = build_non_dated_entities(input_lines, line_items, cfg)
                    dated_entities = build_dated_entities(spark, cfg, input_lines, line_items)

                    # Mode 4: footnote augmentation per SP gate at line 1729
                    # `IF (@Mode = 2 OR (@IsPE=1 AND @Mode=1) OR @Mode = 4)`.
                    # Mode 4 is in the gate, so footnote DOES run (provided
                    # alloc_input is non-empty, which we check via _alloc_empty
                    # in fast-exit above; if we got here alloc_input is set).
                    if alloc_input is not None and not _alloc_empty:
                        custom_fn_line_types = _get_custom_footnote_line_types(spark, cfg)
                        all_underlyings = build_footnote_underlyings_ordered(
                            spark, cfg, underlying_mod, underlyings_ordered, alloc_input,
                            book_effective, all_underlyings, dar_setup, map_dar,
                            custom_fn_line_types,
                        )
                        footnote_input_lines = build_footnote_input_lines(
                            spark, cfg, alloc_input, book_effective, all_underlyings, map_dar,
                        )
                    else:
                        footnote_input_lines = None

                    non_dated_entities, dated_entities = build_footnote_dated_entities(
                        spark, cfg, footnote_input_lines, non_dated_entities, dated_entities,
                    )
                    non_dated_entities, _ = compute_form199a_effective_percentage(
                        spark, cfg, non_dated_entities, book_effective, input_lines, temp_cost_pct,
                    )

                    non_dated_entities = _checkpoint(spark, non_dated_entities, "nde_pre_cpbt_m4", cfg)
                    dated_entities = _checkpoint(spark, dated_entities, "de_pre_cpbt_m4", cfg)

                    entity_underlyings = build_entity_underlyings(
                        spark, cfg, input_lines, underlyings_ordered, asset_class_rel,
                    )
                    entity_underlyings = _checkpoint(spark, entity_underlyings, "entity_und_m4", cfg)
                    all_underlyings = _checkpoint(spark, all_underlyings, "all_und_final_m4", cfg)

                    transfers_adj = None  # mode 4 skips per SQL.

                    # Phase 2a contract: tag inputs with _mode.
                    _m4 = F.lit(4)
                    temp_cost_pct = temp_cost_pct.withColumn("_mode", _m4)
                    all_underlyings = all_underlyings.withColumn("_mode", _m4)
                    entity_underlyings = entity_underlyings.withColumn("_mode", _m4)
                    non_dated_entities = non_dated_entities.withColumn("_mode", _m4)
                    dated_entities = dated_entities.withColumn("_mode", _m4)

                    temp_cost_pct, transfers_adj = build_cost_percentage_by_type(
                        spark, cfg, cost_pct_snapshot, temp_cost_pct, all_underlyings,
                        entity_underlyings, non_dated_entities, dated_entities, transfers_adj,
                        checkpoint_fn=_checkpoint,
                    )

                    temp_cost_pct = temp_cost_pct.drop("_mode")
                    non_dated_entities = non_dated_entities.drop("_mode")
                    dated_entities = dated_entities.drop("_mode")
                    entity_underlyings = entity_underlyings.drop("_mode")
                    all_underlyings = all_underlyings.drop("_mode")
                    temp_cost_pct = _checkpoint(spark, temp_cost_pct, "tcp_by_type_m4", cfg)

                    non_dated_entities, dated_entities = compute_missing_entities(
                        cfg, non_dated_entities, dated_entities, temp_cost_pct,
                    )
                    non_dated_entities = _checkpoint(spark, non_dated_entities, "nde_post_miss_m4", cfg)
                    dated_entities = _checkpoint(spark, dated_entities, "de_post_miss_m4", cfg)

                    final_cost_pct = build_final_cost_percentage(temp_cost_pct, entity_partners)
                    final_cost_pct = _checkpoint(spark, final_cost_pct, "final_cost_pct_m4", cfg)

                    _, cost_pct_min_quarter, dated_entities = compute_minimum_quarter(
                        spark, cfg, final_cost_pct, dated_entities,
                    )

                    eff_pct_dated, pickup_order_dated, dated_entities = compute_effective_percentage_dated(
                        spark, cfg, dated_entities, final_cost_pct, cost_pct_min_quarter,
                        transfers_adj, entity_partners, line_items,
                        checkpoint_fn=_checkpoint,
                    )
                    if eff_pct_dated is None:
                        raise RuntimeError("mode 4: Yearly prorata percentages missing")

                    eff_pct_non_dated = compute_effective_percentage_non_dated(
                        spark, cfg, non_dated_entities, final_cost_pct,
                        cost_pct_min_quarter, transfers_adj,
                    )
                    eff_pct_non_dated = _checkpoint(spark, eff_pct_non_dated, "eff_nd_m4", cfg)

                    eff_pct_dated_rounded, eff_pct_nd_rounded = apply_plugging(
                        spark, cfg, eff_pct_dated, eff_pct_non_dated, dar_setup,
                    )
                    eff_pct_dated_rounded = _checkpoint(spark, eff_pct_dated_rounded, "eff_dt_plug_m4", cfg)
                    eff_pct_nd_rounded = _checkpoint(spark, eff_pct_nd_rounded, "eff_nd_plug_m4", cfg)

                    eff_pct_dated_rounded, eff_pct_nd_rounded = apply_type_id_update(
                        cfg, eff_pct_dated_rounded, eff_pct_nd_rounded,
                        cfg.get("_non_dated_entities_cost"), cfg.get("_dated_entities_cost"),
                    )

                    result = build_final_output(
                        spark, cfg, eff_pct_dated_rounded, eff_pct_nd_rounded,
                        pickup_order_dated, entity_underlyings,
                        None,  # mode 4 doesn't pass final_amounts.
                    )
                    result = result.withColumn("_mode", F.lit(4))
                    mode_status["result"] = result

                    log_id = cfg.get("log_id")
                    if log_id is not None:
                        spark.sql(f"""
                            UPDATE {cfg['catalog']}.{cfg['schema']}.AllocationLog
                            SET EndDate = current_timestamp()
                            WHERE LogID = {log_id}
                        """)

                    logger.info("[DONE] mode 4 computation complete")
            except Exception as e:
                mode_status["status"] = "FAIL"
                mode_status["error"] = str(e)
                logger.error(f"[FAIL] mode 4: {e}", exc_info=True)
                raise

            mode_status["elapsed_seconds"] = round(time.time() - mt0, 1)
            statuses[4] = mode_status

        # ?*"==========================================================?*--
        # + Modes 1+2+3 (fused via shared build_cost_percentage_by_type)+
        # ?*s==========================================================?*?
        if modes_123:
            per_mode_data = {}    # mode -> input-prep dict (only for modes that survived fast-exit)
            mode_t0 = {}          # mode -> wall-clock start

            # -- Pass A: concurrent per-mode input prep (locked 54s path) --
            def _prep_one_mode(mode):
                """Build mode-specific prep frames; leave pre-CPBT barriers for
                the next parallel wave so nde/de/txfr can overlap."""
                mt0 = time.time()
                local_cfg = isolated_cfg(cfg)
                local_cfg["mode"] = mode
                local_cfg["_current_mode"] = mode

                mode_status = {
                    "sp_name": "uspGetFinalEffectivePercentage",
                    "mode": mode,
                    "status": "SUCCESS",
                    "error": None,
                    "elapsed_seconds": 0,
                }
                logger.info(
                    f"[START] mode {mode} prep (Pass A) within run_modes({modes})"
                )
                try:
                    alloc_input = (
                        build_allocation_input(spark, local_cfg, modes=[mode])
                        if mode == 2
                        else None
                    )
                    sm_input = (
                        build_sm_lookthrough_allocation_input(spark, local_cfg)
                        if mode == 3
                        else None
                    )
                    lt_input = (
                        build_lookthrough_allocation_input(spark, local_cfg)
                        if mode == 1
                        else None
                    )

                    _alloc_empty = alloc_input is None or alloc_input.isEmpty()
                    _lt_empty = lt_input is None or lt_input.isEmpty()
                    _sm_empty = sm_input is None or sm_input.isEmpty()
                    local_cfg["_alloc_empty"] = _alloc_empty
                    local_cfg["_lt_empty"] = _lt_empty
                    local_cfg["_sm_empty"] = _sm_empty

                    if mode == 1:
                        local_cfg["_inputs_empty"] = _lt_empty
                    elif mode == 2:
                        local_cfg["_inputs_empty"] = _alloc_empty
                    else:
                        local_cfg["_inputs_empty"] = _sm_empty

                    if local_cfg["_inputs_empty"]:
                        _input_name = {
                            1: "lt_input",
                            2: "alloc_input",
                            3: "sm_input",
                        }[mode]
                        logger.info(
                            f"[FAST-EXIT] mode {mode}: {_input_name} empty -- "
                            "skipping compute and returning result=None."
                        )
                        mode_status["result"] = None
                        mode_status["elapsed_seconds"] = round(
                            time.time() - mt0, 1
                        )
                        return {
                            "mode": mode,
                            "mt0": mt0,
                            "status": mode_status,
                            "fast_exit": True,
                            "data": None,
                        }

                    mode_temp_cost_pct = common_temp_cost_pct_base
                    chain = chain_with_lt if mode == 1 else chain_without_lt
                    mode_all_underlyings = chain["all_underlyings"]
                    mode_input_lines = chain["input_lines"]
                    mode_final_amounts = chain["final_amounts"]
                    mode_non_dated = chain["non_dated_entities"]
                    mode_dated = chain["dated_entities"]
                    mode_entity_underlyings = chain["entity_underlyings"]

                    is_pe_model_flag = local_cfg.get("is_pe_model", False)
                    if mode == 2 or (mode == 1 and is_pe_model_flag):
                        custom_fn_line_types = _get_custom_footnote_line_types(
                            spark, local_cfg
                        )
                        mode_all_underlyings = build_footnote_underlyings_ordered(
                            spark,
                            local_cfg,
                            common_underlying_mod,
                            underlyings_ordered,
                            alloc_input,
                            book_effective,
                            mode_all_underlyings,
                            dar_setup,
                            map_dar,
                            custom_fn_line_types,
                        )
                        mode_all_underlyings = _checkpoint(
                            spark,
                            mode_all_underlyings,
                            f"all_und_final_m{mode}",
                            local_cfg,
                        )
                        footnote_input_lines = build_footnote_input_lines(
                            spark,
                            local_cfg,
                            alloc_input,
                            book_effective,
                            mode_all_underlyings,
                            map_dar,
                            checkpoint_fn=_checkpoint,
                        )
                        footnote_input_lines = _checkpoint(
                            spark,
                            footnote_input_lines,
                            f"fn_input_lines_m{mode}",
                            local_cfg,
                        )
                        mode_non_dated, mode_dated = (
                            build_footnote_dated_entities(
                                spark,
                                local_cfg,
                                footnote_input_lines,
                                mode_non_dated,
                                mode_dated,
                            )
                        )

                    mode_non_dated, _ = compute_form199a_effective_percentage(
                        spark,
                        local_cfg,
                        mode_non_dated,
                        book_effective,
                        mode_input_lines,
                        mode_temp_cost_pct,
                    )

                    _sm_has_data = (
                        mode == 3
                        and sm_input is not None
                        and not sm_input.isEmpty()
                    )
                    if _sm_has_data:
                        (
                            mode_all_underlyings,
                            state_input_lines,
                            sm_eff_amounts,
                        ) = build_state_allocation_input(
                            spark,
                            local_cfg,
                            common_underlying_mod,
                            sm_input,
                            cost_pct_snapshot_123,
                            mode_all_underlyings,
                            map_dar,
                            dar_setup,
                            entity_partners,
                            collapse_state_passes=local_cfg.get(
                                "_output_v3_collapse_state_passes", False
                            ),
                            batch_state_workflow_lookup=local_cfg.get(
                                "_output_v3_batch_state_workflow_lookup",
                                False,
                            ),
                        )
                        mode_non_dated, mode_dated = build_state_entities(
                            spark,
                            local_cfg,
                            state_input_lines,
                            mode_non_dated,
                            mode_dated,
                        )
                        if sm_eff_amounts is not None:
                            mode_final_amounts = (
                                mode_final_amounts.unionByName(
                                    sm_eff_amounts,
                                    allowMissingColumns=True,
                                )
                                if mode_final_amounts is not None
                                else sm_eff_amounts
                            )

                    is_footnote_path = mode == 2
                    is_state_path = _sm_has_data
                    transfers_adj = load_transfers_adj_cost(
                        spark,
                        local_cfg,
                        mode_all_underlyings,
                        mode_entity_underlyings,
                    )
                    return {
                        "mode": mode,
                        "mt0": mt0,
                        "status": mode_status,
                        "fast_exit": False,
                        "data": {
                            "temp_cost_pct": mode_temp_cost_pct,
                            "all_underlyings": mode_all_underlyings,
                            "entity_underlyings": mode_entity_underlyings,
                            "non_dated_entities": mode_non_dated,
                            "dated_entities": mode_dated,
                            "transfers_adj": transfers_adj,
                            "input_lines": mode_input_lines,
                            "final_amounts": mode_final_amounts,
                            "needs_pre_cpbt": (
                                is_footnote_path or is_state_path
                            ),
                            "local_cfg": local_cfg,
                        },
                    }
                except Exception as e:
                    mode_status["status"] = "FAIL"
                    mode_status["error"] = str(e)
                    logger.error(
                        f"[FAIL] mode {mode} prep: {e}", exc_info=True
                    )
                    raise

            prep_by_mode = dict(
                run_phase(
                    "mode_prep",
                    [
                        (f"mode_{mode}", _prep_one_mode, (mode,), {})
                        for mode in modes_123
                    ],
                    workers,
                    enabled_groups,
                )
            )

            # Pre-CPBT barriers: dated / non-dated / transfers overlap per mode.
            for mode in modes_123:
                prep = prep_by_mode[f"mode_{mode}"]
                mode_t0[mode] = prep["mt0"]
                statuses[mode] = prep["status"]
                if prep["fast_exit"]:
                    continue

                d = prep["data"]
                local_cfg = d["local_cfg"]
                mode_non_dated = d["non_dated_entities"]
                mode_dated = d["dated_entities"]
                transfers_adj = d["transfers_adj"]

                boundary_tasks = []
                if d["needs_pre_cpbt"]:
                    boundary_tasks.append(
                        (
                            f"non_dated_m{mode}",
                            _checkpoint,
                            (
                                spark,
                                mode_non_dated,
                                f"nde_pre_cpbt_m{mode}",
                                local_cfg,
                            ),
                            {},
                        )
                    )
                    boundary_tasks.append(
                        (
                            f"dated_m{mode}",
                            _checkpoint,
                            (
                                spark,
                                mode_dated,
                                f"de_pre_cpbt_m{mode}",
                                local_cfg,
                            ),
                            {},
                        )
                    )

                def _ckpt_transfers(
                    _transfers=transfers_adj,
                    _mode=mode,
                    _local_cfg=local_cfg,
                ):
                    if _transfers is None:
                        return None
                    return _checkpoint(
                        spark,
                        _transfers,
                        f"txfr_pre_cpbt_m{_mode}",
                        _local_cfg,
                    )

                boundary_tasks.append(
                    (f"transfers_m{mode}", _ckpt_transfers, (), {})
                )

                _ckpt_t0 = time.time()
                boundary = dict(
                    run_phase(
                        "mode_prep_boundaries",
                        boundary_tasks,
                        workers,
                        enabled_groups,
                    )
                )
                if d["needs_pre_cpbt"]:
                    mode_non_dated = boundary[f"non_dated_m{mode}"]
                    mode_dated = boundary[f"dated_m{mode}"]
                    logger.info(
                        f"[TIMING] pre_cpbt_checkpoints_m{mode}: "
                        f"{time.time() - _ckpt_t0:.1f}s"
                    )
                transfers_adj = boundary[f"transfers_m{mode}"]

                per_mode_data[mode] = {
                    "temp_cost_pct": d["temp_cost_pct"],
                    "all_underlyings": d["all_underlyings"],
                    "entity_underlyings": d["entity_underlyings"],
                    "non_dated_entities": mode_non_dated,
                    "dated_entities": mode_dated,
                    "transfers_adj": transfers_adj,
                    "input_lines": d["input_lines"],
                    "final_amounts": d["final_amounts"],
                }

            # --- Pass B: ONE fused build_cost_percentage_by_type call ---
            valid_modes_123 = sorted(per_mode_data.keys())
            fused_temp_cost_pct = None
            fused_transfers_adj = None

            if valid_modes_123:
                logger.info(
                    f"[FUSED] build_cost_percentage_by_type for modes {valid_modes_123} "
                    "(Phase 2b: single call on unioned inputs)"
                )

                tagged_temp_cost_pct = _fold_union(
                    [_tag_mode(per_mode_data[m]["temp_cost_pct"], m) for m in valid_modes_123]
                )
                tagged_all_underlyings = _fold_union(
                    [_tag_mode(per_mode_data[m]["all_underlyings"], m) for m in valid_modes_123]
                )
                tagged_entity_underlyings = _fold_union(
                    [_tag_mode(per_mode_data[m]["entity_underlyings"], m) for m in valid_modes_123]
                )
                tagged_non_dated = _fold_union(
                    [_tag_mode(per_mode_data[m]["non_dated_entities"], m) for m in valid_modes_123]
                )
                tagged_dated = _fold_union(
                    [_tag_mode(per_mode_data[m]["dated_entities"], m) for m in valid_modes_123]
                )
                tagged_transfers_adj = _fold_union(
                    [_tag_mode(per_mode_data[m]["transfers_adj"], m) for m in valid_modes_123]
                )

                # Sentinel _current_mode for checkpoint naming inside the function
                # (avoids collision with per-mode checkpoint paths).
                cfg["_current_mode"] = 0

                def _checkpoint_group(tasks):
                    return dict(
                        run_phase(
                            "independent_builders",
                            tasks,
                            workers,
                            enabled_groups,
                        )
                    )

                fused_temp_cost_pct, fused_transfers_adj = build_cost_percentage_by_type(
                    spark, cfg, cost_pct_snapshot_123,
                    tagged_temp_cost_pct, tagged_all_underlyings, tagged_entity_underlyings,
                    tagged_non_dated, tagged_dated, tagged_transfers_adj,
                    checkpoint_fn=_checkpoint,
                    checkpoint_group_fn=_checkpoint_group,
                )

                # Checkpoint fused outputs concurrently -- tcp_by_type has 3+
                # downstream consumers; txfr_adj_fused is REQUIRED because
                # localCheckpoint strips alias metadata used later by
                # compute_effective_percentage_dated (.alias("T")).
                fused_out = dict(
                    run_phase(
                        "independent_builders",
                        [
                            (
                                "temp_output",
                                _checkpoint,
                                (
                                    spark,
                                    fused_temp_cost_pct,
                                    "tcp_by_type_fused",
                                    cfg,
                                ),
                                {},
                            ),
                            (
                                "transfer_output",
                                _checkpoint,
                                (
                                    spark,
                                    fused_transfers_adj,
                                    "txfr_adj_fused",
                                    cfg,
                                ),
                                {},
                            ),
                        ],
                        workers,
                        enabled_groups,
                    )
                )
                fused_temp_cost_pct = fused_out["temp_output"]
                fused_transfers_adj = fused_out["transfer_output"]

            # --- Pass B+: ONE call each to compute_missing_entities,
            #            build_final_cost_percentage, compute_minimum_quarter
            #            on fused (mode-tagged) inputs. These three are
            #            _mode-aware (Phase 3a-1) so they preserve mode
            #            isolation while doing one shuffle each instead of
            #            three.
            # ---
            fused_final_cost_pct = None
            fused_cost_pct_min_quarter = None
            fused_non_dated_post = None
            fused_dated_post = None

            if valid_modes_123:
                # Tag per-mode entity DataFrames with _mode and union them so
                # the three _mode-aware functions can run on combined data.
                tagged_non_dated_entities = _fold_union(
                    [_tag_mode(per_mode_data[m]["non_dated_entities"], m) for m in valid_modes_123]
                )
                tagged_dated_entities = _fold_union(
                    [_tag_mode(per_mode_data[m]["dated_entities"], m) for m in valid_modes_123]
                )

                # compute_missing_entities + final cost -- lazy builders first,
                # then materialize the three fan-out barriers concurrently.
                cfg["_current_mode"] = 0   # sentinel for checkpoint naming
                fused_non_dated_post, fused_dated_post = compute_missing_entities(
                    cfg, tagged_non_dated_entities, tagged_dated_entities, fused_temp_cost_pct,
                )
                fused_final_cost_pct = build_final_cost_percentage(
                    fused_temp_cost_pct, entity_partners
                )
                post_miss = dict(
                    run_phase(
                        "independent_builders",
                        [
                            (
                                "post_missing_non_dated",
                                _checkpoint,
                                (
                                    spark,
                                    fused_non_dated_post,
                                    "nde_post_miss_fused",
                                    cfg,
                                ),
                                {},
                            ),
                            (
                                "post_missing_dated",
                                _checkpoint,
                                (
                                    spark,
                                    fused_dated_post,
                                    "de_post_miss_fused",
                                    cfg,
                                ),
                                {},
                            ),
                            (
                                "final_cost",
                                _checkpoint,
                                (
                                    spark,
                                    fused_final_cost_pct,
                                    "final_cost_pct_fused",
                                    cfg,
                                ),
                                {},
                            ),
                        ],
                        workers,
                        enabled_groups,
                    )
                )
                fused_non_dated_post = post_miss["post_missing_non_dated"]
                fused_dated_post = post_miss["post_missing_dated"]
                fused_final_cost_pct = post_miss["final_cost"]

                # validate_cost_percentage_sum -- runs only if mode 1 is present.
                # The validator is _mode-naive but mode-1-only; we filter the
                # fused frame to mode 1 just for the check.
                if 1 in valid_modes_123:
                    cfg["mode"] = 1
                    fcp_mode1 = fused_final_cost_pct.filter(F.col("_mode") == 1)
                    is_valid = validate_cost_percentage_sum(
                        spark, cfg, fcp_mode1, dar_setup,
                    )
                    if not is_valid:
                        raise RuntimeError(
                            "mode 1: Cost percentage does not sum to 100%"
                        )

                # compute_minimum_quarter -- runs once on fused inputs, then
                # materialize dated-after-min-quarter and min-quarter data
                # concurrently before effective calculation.
                cfg["_current_mode"] = 0
                _, fused_cost_pct_min_quarter, fused_dated_post = compute_minimum_quarter(
                    spark, cfg, fused_final_cost_pct, fused_dated_post,
                )
                minq = dict(
                    run_phase(
                        "effective_inputs",
                        [
                            (
                                "dated_after_min_quarter",
                                _checkpoint,
                                (
                                    spark,
                                    fused_dated_post,
                                    "de_post_minq_fused",
                                    cfg,
                                ),
                                {},
                            ),
                            (
                                "minimum_quarter",
                                _checkpoint,
                                (
                                    spark,
                                    fused_cost_pct_min_quarter,
                                    "cost_pct_min_q_fused",
                                    cfg,
                                ),
                                {},
                            ),
                        ],
                        workers,
                        enabled_groups,
                    )
                )
                fused_dated_post = minq["dated_after_min_quarter"]
                fused_cost_pct_min_quarter = minq["minimum_quarter"]

            # --- Pass C: ONE fused call to effective_calc + plugging chain,
            #            then per-mode loop for build_final_output only.
            #            (Phases 3a-2/3a-3 made these functions _mode-aware.)
            # ---
            fused_eff_pct_dated = None
            fused_eff_pct_non_dated = None
            fused_pickup_order_dated = None
            fused_eff_pct_dated_rounded = None
            fused_eff_pct_nd_rounded = None

            if valid_modes_123:
                logger.info(
                    f"[FUSED] effective_calc + plugging for modes {valid_modes_123} "
                    "(Phase 3a-2/3a-3: single call on fused mid-stage outputs)"
                )
                cfg["_current_mode"] = 0   # sentinel for checkpoint naming

                # Heavy effective_calc -- runs once on fused inputs.
                # NOTE: effective_calc uses dot-qualified column refs (D.InvestmentID)
                # in joins with table aliases. localCheckpoint strips alias context,
                # causing UNRESOLVED_COLUMN errors. Must use Delta checkpoint here.
                def _dated_effective():
                    return compute_effective_percentage_dated(
                        spark, cfg, fused_dated_post, fused_final_cost_pct,
                        fused_cost_pct_min_quarter, fused_transfers_adj,
                        entity_partners, line_items,
                        checkpoint_fn=_checkpoint,
                    )

                def _nondated_effective():
                    return compute_effective_percentage_non_dated(
                        spark, cfg, fused_non_dated_post, fused_final_cost_pct,
                        fused_cost_pct_min_quarter, fused_transfers_adj,
                    )

                if cfg.get("_output_v3_parallel_effective", True):
                    eff_pair = dict(
                        run_phase(
                            "independent_effective",
                            [
                                ("dated", _dated_effective, (), {}),
                                ("nondated", _nondated_effective, (), {}),
                            ],
                            workers,
                            enabled_groups,
                        )
                    )
                    (
                        fused_eff_pct_dated,
                        fused_pickup_order_dated,
                        fused_dated_post,
                    ) = eff_pair["dated"]
                    fused_eff_pct_non_dated = eff_pair["nondated"]
                else:
                    (
                        fused_eff_pct_dated,
                        fused_pickup_order_dated,
                        fused_dated_post,
                    ) = _dated_effective()
                    fused_eff_pct_non_dated = _nondated_effective()
                if fused_eff_pct_dated is None:
                    raise RuntimeError(
                        f"compute_effective_percentage_dated returned None: "
                        "Yearly prorata percentages missing"
                    )
                # Checkpoint dated/non-dated outputs concurrently -- the
                # "missing partners" step after the last internal checkpoint
                # re-introduces T.* alias refs on the dated branch.
                eff_out = dict(
                    run_phase(
                        "effective_boundaries",
                        [
                            (
                                "eff_dated",
                                _checkpoint,
                                (
                                    spark,
                                    fused_eff_pct_dated,
                                    "eff_dt_fused",
                                    cfg,
                                ),
                                {},
                            ),
                            (
                                "eff_nondated",
                                _checkpoint,
                                (
                                    spark,
                                    fused_eff_pct_non_dated,
                                    "eff_nd_fused",
                                    cfg,
                                ),
                                {},
                            ),
                        ],
                        workers,
                        enabled_groups,
                    )
                )
                fused_eff_pct_dated = eff_out["eff_dated"]
                fused_eff_pct_non_dated = eff_out["eff_nondated"]

                fused_eff_pct_dated_rounded, fused_eff_pct_nd_rounded = apply_plugging(
                    spark, cfg, fused_eff_pct_dated, fused_eff_pct_non_dated, dar_setup,
                )
                plug_out = dict(
                    run_phase(
                        "effective_boundaries",
                        [
                            (
                                "plugged_dated",
                                _checkpoint,
                                (
                                    spark,
                                    fused_eff_pct_dated_rounded,
                                    "eff_dt_plug_fused",
                                    cfg,
                                ),
                                {},
                            ),
                            (
                                "plugged_non_dated",
                                _checkpoint,
                                (
                                    spark,
                                    fused_eff_pct_nd_rounded,
                                    "eff_nd_plug_fused",
                                    cfg,
                                ),
                                {},
                            ),
                        ],
                        workers,
                        enabled_groups,
                    )
                )
                fused_eff_pct_dated_rounded = plug_out["plugged_dated"]
                fused_eff_pct_nd_rounded = plug_out["plugged_non_dated"]

                fused_eff_pct_dated_rounded, fused_eff_pct_nd_rounded = apply_type_id_update(
                    cfg, fused_eff_pct_dated_rounded, fused_eff_pct_nd_rounded,
                    cfg.get("_non_dated_entities_cost"), cfg.get("_dated_entities_cost"),
                )

            # Pass C: build per-mode outputs concurrently.
            def _build_one_mode_output(mode):
                local_cfg = isolated_cfg(cfg)
                local_cfg["mode"] = mode
                local_cfg["_current_mode"] = mode
                mt0 = mode_t0[mode]
                mode_status = statuses[mode]
                d = per_mode_data[mode]
                logger.info(
                    f"[START] mode {mode} build_final_output (Pass C) "
                    f"within run_modes({modes})"
                )
                try:
                    eff_pct_dated_rounded = (
                        fused_eff_pct_dated_rounded.filter(
                            F.col("_mode") == mode
                        ).drop("_mode")
                    )
                    eff_pct_nd_rounded = (
                        fused_eff_pct_nd_rounded.filter(
                            F.col("_mode") == mode
                        ).drop("_mode")
                    )
                    pickup_order_dated = (
                        fused_pickup_order_dated.filter(
                            F.col("_mode") == mode
                        ).drop("_mode")
                    )
                    result = build_final_output(
                        spark,
                        local_cfg,
                        eff_pct_dated_rounded,
                        eff_pct_nd_rounded,
                        pickup_order_dated,
                        d["entity_underlyings"],
                        d["final_amounts"],
                    )
                    result = result.withColumn("_mode", F.lit(mode))
                    mode_status["result"] = result

                    log_id = local_cfg.get("log_id")
                    if log_id is not None:
                        spark.sql(f"""
                            UPDATE {local_cfg['catalog']}.{local_cfg['schema']}.AllocationLog
                            SET EndDate = current_timestamp()
                            WHERE LogID = {log_id}
                        """)
                    logger.info(f"[DONE] mode {mode} computation complete")
                except Exception as e:
                    mode_status["status"] = "FAIL"
                    mode_status["error"] = str(e)
                    logger.error(
                        f"[FAIL] mode {mode} downstream: {e}",
                        exc_info=True,
                    )
                    raise
                mode_status["elapsed_seconds"] = round(time.time() - mt0, 1)
                return mode_status

            if valid_modes_123:
                built = dict(
                    run_phase(
                        "output_build",
                        [
                            (
                                f"mode_{mode}",
                                _build_one_mode_output,
                                (mode,),
                                {},
                            )
                            for mode in valid_modes_123
                        ],
                        workers,
                        enabled_groups,
                    )
                )
                for mode in valid_modes_123:
                    statuses[mode] = built[f"mode_{mode}"]

        # --- Save results to target tables ---
        # Save BEFORE dropping checkpoints -- the result DFs depend on them.
        try:
            save_return_value = _save_results(spark, cfg, statuses)
        except Exception as e:
            logger.error(f"[SAVE] Result save failed: {e}", exc_info=True)
            raise

    except Exception:
        # If error during common phase or re-raised from mode loop, clean up
        raise
    finally:

        try:
            _finish_dev_plan_profile(locals().get("_plan_tokens"), cfg=locals().get("cfg"))
        except Exception:
            pass
        spark.conf.set("spark.sql.shuffle.partitions", original_shuffle)
        status_out = {
            "statuses": statuses,
            "elapsed_seconds": round(time.time() - t0, 1),
            "_checkpoint_tables": list(cfg.get("_checkpoint_tables", [])),
            "_save_return_value": save_return_value,
        }
        _drop_checkpoints(spark, cfg)

    logger.info(
        f"[DONE] run_modes({modes}) | {status_out['elapsed_seconds']}s | "
        f"RunID={cfg['run_id']} EntityID={cfg['entity_id']}"
    )

    return status_out


# ===============================================================
# Single entry point: run_final_effective_percentages
# ===============================================================
# Two valid invocations:
#   run_final_effective_percentages(spark, mode=0, ...)  -> fused modes 1+2+3 with shared upstream
#   run_final_effective_percentages(spark, mode=4, ...)  -> mode 4 (704c) standalone
# Single-mode calls (mode=1/2/3) are intentionally rejected -- modes 1, 2, 3
# must always be invoked together via mode=0 to share the upstream pipeline.
# ===============================================================

def run_final_effective_percentages(
    spark: SparkSession,
    mode: int = None,
    entity_id: int = None,
    client_id: int = None,
    tax_period_id: int = None,
    run_id: int = None,
    catalog: str = None,
    schema: str = None,
    cfg: dict = None,
    verbose: bool = False,
    ResultType: str = "deltalake",
    VolumePath: str = None,
    ExecutionID: str = None,
    # Orchestrator CamelCase parameters
    RunID: int = None,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    Mode: int = None,
    profile_plan: bool = False,
    plan_checkpoint_threshold: int = 30,
    **kwargs,
) -> dict:
    """Run Final Effective Percentage.

    Args:
        mode:
            0 -> fused modes 1+2+3 (shared upstream, returns 3 result DataFrames)
            4 -> mode 4 (704c) standalone (returns 1 result DataFrame)
        entity_id ... schema: individual params (used when cfg is None)
        cfg: pre-built config dict (overrides individual params)
        verbose: enable DEBUG logging

    Returns:
        dict with keys:
            sp_name:          "uspGetFinalEffectivePercentage"
            mode:             0 or 4 (the mode argument that was passed)
            status:           "SUCCESS" if all sub-modes succeeded; raises on failure
            error:            None on success
            elapsed_seconds:  total wall-clock time
            results:          dict {sub_mode: DataFrame}
                              - mode=0 -> {1: df, 2: df, 3: df}
                              - mode=4 -> {4: df}
            statuses:         dict {sub_mode: per-mode status} for diagnostics
            _checkpoint_tables: list of Delta tables created (for caller cleanup)

    Raises:
        ValueError: if mode is not 0 or 4
        RuntimeError / Exception: any sub-mode failure fails the entire call
            (no partial results returned)
    """
    # Resolve CamelCase Orchestrator params -> snake_case
    if RunID is not None and run_id is None:
        run_id = int(RunID)
    if EntityID is not None and entity_id is None:
        entity_id = int(EntityID)
    if ClientID is not None and client_id is None:
        client_id = int(ClientID)
    if TaxPeriodID is not None and tax_period_id is None:
        tax_period_id = int(TaxPeriodID)
    if CatalogName is not None and catalog is None:
        catalog = CatalogName
    if SchemaName is not None and schema is None:
        schema = SchemaName
    if Mode is not None and mode is None:
        mode = int(Mode)

    if mode == 0:
        modes_to_run = [1, 2, 3]
    elif mode in (1, 2, 3):
        modes_to_run = [mode]
    elif mode == 4:
        modes_to_run = [4]
    else:
        raise ValueError(
            f"mode must be 0 (fused 1+2+3), 1, 2, 3, or 4, got {mode}."
        )

    # Delegate to run_modes (raises on any sub-mode failure).
    inner = run_modes(
        spark, modes=modes_to_run,
        entity_id=entity_id, client_id=client_id,
        tax_period_id=tax_period_id, run_id=run_id,
        catalog=catalog, schema=schema,
        cfg=cfg, verbose=verbose,
        ResultType=ResultType, VolumePath=VolumePath, ExecutionID=ExecutionID,
        max_threads=kwargs.get("max_threads"),
        MaxThreads=kwargs.get("MaxThreads"),
        parallel_groups=kwargs.get("parallel_groups", "all"),
        ParallelGroups=kwargs.get("ParallelGroups"),
        execution_profile=kwargs.get("execution_profile", "low"),
        ExecutionProfile=kwargs.get("ExecutionProfile"),
        checkpoint_mode=kwargs.get("checkpoint_mode"),
        CheckpointMode=kwargs.get("CheckpointMode"),
        SqlShufflePartitions=kwargs.get("SqlShufflePartitions"),
        profile_plan=profile_plan,
        plan_checkpoint_threshold=plan_checkpoint_threshold,
    )

    # Reshape to {results: {sub_mode: df}, ...}.
    results = {}
    for m in modes_to_run:
        sub = inner["statuses"].get(m, {})
        # run_modes raises on hard failures, so any FAIL status here is
        # defensive -- convert to a hard failure for consistency.
        if sub.get("status") != "SUCCESS":
            raise RuntimeError(
                f"mode {m} did not complete successfully: "
                f"{sub.get('error') or 'unknown error'}"
            )
        results[m] = sub.get("result")

    # If GenericResultStorer returned a JSON string (Parquet mode),
    # propagate it to the Orchestrator for WriteDataFromParquetToSQL.
    save_return_value = inner.get("_save_return_value")
    if save_return_value:
        return save_return_value

    return {
        "sp_name": "uspGetFinalEffectivePercentage",
        "mode": mode,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": inner["elapsed_seconds"],
        "results": results,
        "statuses": inner["statuses"],
        "_checkpoint_tables": inner.get("_checkpoint_tables", []),
    }


def run_mode(spark, mode=0, **kwargs):
    """Compatibility alias used by jobs that still call run_mode."""
    return run_final_effective_percentages(spark, mode=mode, **kwargs)


def get_last_run_profile():
    return {}


# ================================================================
# __main__: Databricks Job spark_python_task or standalone
# ================================================================

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    # Mode 3 standalone: read widget IDs, let run_final_effective_percentages() call load_common_config
    # via the `if cfg is None:` branch.
    _kwargs = dict(
        entity_id=int(dbutils.widgets.get("entity_id")),  # noqa: F821
        client_id=int(dbutils.widgets.get("client_id")),  # noqa: F821
        tax_period_id=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
        run_id=int(dbutils.widgets.get("run_id")),  # noqa: F821
        catalog=dbutils.widgets.get("catalog"),  # noqa: F821
        schema=dbutils.widgets.get("schema"),  # noqa: F821
    )

    # Run 1: fused modes 1+2+3 (shared upstream)
    out_0 = run_final_effective_percentages(spark, mode=0, verbose=True, **_kwargs)
    logger.info(
        f"[FINAL] mode=0 (fused 1+2+3): {out_0['status']} "
        f"({out_0['elapsed_seconds']}s)"
    )
    for m, df in out_0["results"].items():
        logger.info(f"[FINAL]   sub-mode {m}: {'has data' if df is not None else 'None'}")

    # Run 2: mode 4 (704c)
    out_4 = run_final_effective_percentages(spark, mode=4, verbose=True, **_kwargs)
    logger.info(
        f"[FINAL] mode=4 (704c): {out_4['status']} "
        f"({out_4['elapsed_seconds']}s)"
    )
    df4 = out_4["results"].get(4)
    logger.info(f"[FINAL]   mode 4: {'has data' if df4 is not None else 'None'}")
