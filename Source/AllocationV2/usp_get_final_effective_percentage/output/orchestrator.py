"""
orchestrator.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Single entry point for Final Effective Percentage calculation.
Conversion date: 2026-05-04

Usage:
    from AllocationV2.usp_get_final_effective_percentage.output.orchestrator import run_mode

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
import contextvars
import functools
import inspect
import logging
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable

from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import read_table, ns, ns0
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.execution_profiles import resolve_execution_profile

from AllocationV2.usp_get_final_effective_percentage.output import book_effective as _book_effective
from AllocationV2.usp_get_final_effective_percentage.output.checkpoint_policy import (
    drop_failed_run_checkpoints,
    initialize_named_checkpoint_policy,
    named_checkpoint,
)
from AllocationV2.usp_get_final_effective_percentage.output.pipeline import run_modes_parallel
from AllocationV2.usp_get_final_effective_percentage.output.stages import (
    FUNCTION_STAGE,
    StageName,
    stage_contracts,
)

from AllocationV2.usp_get_final_effective_percentage.output.config_loader import (
    load_config,
)
from AllocationV2.usp_get_final_effective_percentage.output.input_builder import (
    build_allocation_input,
    build_sm_lookthrough_allocation_input,
    build_lookthrough_allocation_input,
)
from AllocationV2.usp_get_final_effective_percentage.output.entity_hierarchy import (
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
from AllocationV2.usp_get_final_effective_percentage.output.pfic_footnotes import (
    build_footnote_underlyings_ordered,
    build_footnote_input_lines,
    build_footnote_dated_entities,
    _get_custom_footnote_line_types,
)
from AllocationV2.usp_get_final_effective_percentage.output.form199a import (
    compute_form199a_effective_percentage,
)
from AllocationV2.usp_get_final_effective_percentage.output.state_allocation import (
    build_state_allocation_input,
    build_state_entities,
)
from AllocationV2.usp_get_final_effective_percentage.output.cost_pct_loader import (
    build_entity_underlyings,
    load_transfers_adj_cost,
    build_cost_percentage_by_type,
    compute_missing_entities,
    build_final_cost_percentage,
    validate_cost_percentage_sum,
    compute_minimum_quarter,
)
from AllocationV2.usp_get_final_effective_percentage.output.effective_calc import (
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

logger = logging.getLogger(__name__)

if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _checkpoint(spark, df, name, cfg):
    return named_checkpoint(spark, df, name, cfg)


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
    storer = GenericResultStorer(spark)
    return_value = storer.save_results(
        result=output_tables,
        result_type=result_type,
        catalog_name=cfg["catalog"],
        database_name=cfg["schema"],
        run_id=cfg["run_id"],
        client_id=cfg["client_id"],
        entity_id=cfg["entity_id"],
        execution_id=cfg.get("execution_id", ""),
        volume_path=cfg.get("volume_path", ""),
        sql_url_path=cfg.get("sql_url_path", ""),
        sql_username=cfg.get("sql_username", ""),
        sql_password=cfg.get("sql_password", ""),
    )
    logger.info(
        f"[SAVE] Results saved ({result_type}): {list(output_tables.keys())}"
    )
    return return_value


# ===============================================================
# Multi-mode entry point: run_modes
# ===============================================================

def _production_run_modes(
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

    # -- AQE tuning --
    # Default 200 shuffle partitions is excessive; reducing to 32 eliminates
    # scheduling overhead from many tiny partitions and improves checkpoint
    # write speed. advisoryPartitionSizeInBytes is not available on serverless
    # compute -- set each independently so one failure doesn't block the other.
    _aqe_configs = {
        "spark.sql.shuffle.partitions": "32",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    }
    for _k, _v in _aqe_configs.items():
        try:
            spark.conf.set(_k, _v)
        except Exception:
            logger.info(f"[AQE] {_k} not available (serverless) -- skipped")


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
        underlyings_combined = _checkpoint(
            spark, underlyings_combined, "underlyings_common", cfg,
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
            if 1 in modes_123:
                chain_with_lt = _build_lt_dependent_chain(lt_input_m14, "lt")

            # Chain WITHOUT lt_input - used by modes 2 and 3. SP gate at
            # `IF @Mode IN (1, 4)` means #TempLookThroughAllocationInput is
            # empty for modes 2 and 3, so downstream queries on it return 0
            # rows. We replicate by passing an empty DataFrame with the same
            # schema - `lt_input_m14.limit(0)` is a metadata-only op in Spark.
            if 2 in modes_123 or 3 in modes_123:
                lt_input_empty = lt_input_m14.limit(0)
                chain_without_lt = _build_lt_dependent_chain(lt_input_empty, "nolt")

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

            # -- Pass A: per-mode input prep ----------------------
            for mode in modes_123:
                mt0 = time.time()
                mode_t0[mode] = mt0
                cfg["mode"] = mode
                cfg["_current_mode"] = mode
                cost_pct_snapshot = cost_pct_snapshot_123

                mode_status = {
                    "sp_name": "uspGetFinalEffectivePercentage",
                    "mode": mode,
                    "status": "SUCCESS",
                    "error": None,
                    "elapsed_seconds": 0,
                }
                statuses[mode] = mode_status   # placeholder, finalized in Pass C

                logger.info(f"[START] mode {mode} prep (Pass A) within run_modes({modes})")

                try:
                    alloc_input = build_allocation_input(spark, cfg, modes=[mode]) if mode == 2 else None
                    sm_input = build_sm_lookthrough_allocation_input(spark, cfg) if mode == 3 else None
                    lt_input = build_lookthrough_allocation_input(spark, cfg) if mode == 1 else None

                    _alloc_empty = alloc_input is None or alloc_input.isEmpty()
                    _lt_empty = lt_input is None or lt_input.isEmpty()
                    _sm_empty = sm_input is None or sm_input.isEmpty()
                    cfg["_alloc_empty"] = _alloc_empty
                    cfg["_lt_empty"] = _lt_empty
                    cfg["_sm_empty"] = _sm_empty

                    if mode == 1:
                        cfg["_inputs_empty"] = _lt_empty
                    elif mode == 2:
                        cfg["_inputs_empty"] = _alloc_empty
                    elif mode == 3:
                        cfg["_inputs_empty"] = _sm_empty

                    if cfg["_inputs_empty"]:
                        _input_name = {1: "lt_input", 2: "alloc_input", 3: "sm_input"}[mode]
                        logger.info(
                            f"[FAST-EXIT] mode {mode}: {_input_name} empty -- "
                            "skipping compute and returning result=None."
                        )
                        mode_status["result"] = None
                        mode_status["elapsed_seconds"] = round(time.time() - mt0, 1)
                        # Excluded from per_mode_data -> not part of the fused Pass B.
                        continue

                    # -- Pick the right chain from Common Phase 2 ----------
                    # Mode 1: SP populates #TempLookThroughAllocationInput
                    #         -> use chain built WITH lt_input.
                    # Modes 2, 3: SP leaves #TempLookThroughAllocationInput
                    #         empty -> use chain built WITHOUT lt_input.
                    # Per-mode augmentation creates new immutable DataFrames;
                    # the base remains shared with other modes that picked
                    # the same chain.
                    mode_temp_cost_pct = common_temp_cost_pct_base   # lt-independent
                    if mode == 1:
                        chain = chain_with_lt
                    else:   # mode 2 or 3
                        chain = chain_without_lt
                    mode_all_underlyings = chain["all_underlyings"]
                    mode_input_lines    = chain["input_lines"]
                    mode_final_amounts  = chain["final_amounts"]
                    mode_non_dated      = chain["non_dated_entities"]
                    mode_dated          = chain["dated_entities"]
                    mode_entity_underlyings = chain["entity_underlyings"]
                    # `mode_entity_underlyings` is now per-chain, not fully shared
                    # (mode 1's input_lines includes lt-derived rows; modes 2/3's
                    # don't). per_mode_data["entity_underlyings"] picks it up below.

                    is_pe_model_flag = cfg.get("is_pe_model", False)

                    # -- Mode-specific input-table augmentation (per SP) ---
                    # Footnote augmentation per SP gate at line 1729:
                    #   IF (@Mode = 2 OR (@IsPE=1 AND @Mode=1) OR @Mode = 4)
                    # In modes_123 (mode != 4) this collapses to:
                    #   mode == 2  OR  (mode == 1 AND IsPEModel)
                    if mode == 2 or (mode == 1 and is_pe_model_flag):
                        custom_fn_line_types = _get_custom_footnote_line_types(spark, cfg)
                        mode_all_underlyings = build_footnote_underlyings_ordered(
                            spark, cfg, common_underlying_mod, underlyings_ordered, alloc_input,
                            book_effective, mode_all_underlyings, dar_setup, map_dar,
                            custom_fn_line_types,
                        )
                        # Checkpoint footnote-augmented underlyings immediately.
                        # Without this, the lazy footnote DAG is re-materialized
                        # 3x at the pre-cpbt checkpoints below (~83s -> ~15s).
                        mode_all_underlyings = _checkpoint(
                            spark, mode_all_underlyings, f"all_und_final_m{mode}", cfg,
                        )
                        footnote_input_lines = build_footnote_input_lines(
                            spark, cfg, alloc_input, book_effective, mode_all_underlyings, map_dar,
                        )
                        # Checkpoint footnote_input_lines -- it's read 8+ times
                        # inside build_footnote_dated_entities (once per footnote
                        # type: PFIC, Form926, Form8865, etc.). Without this,
                        # materializing non_dated/dated later re-evaluates the
                        # entire input pipeline 8x each (18s+14s = 32s).
                        # With this, the 8 branches read from memory (~2s each).
                        footnote_input_lines = _checkpoint(
                            spark, footnote_input_lines, f"fn_input_lines_m{mode}", cfg,
                        )
                        mode_non_dated, mode_dated = build_footnote_dated_entities(
                            spark, cfg, footnote_input_lines, mode_non_dated, mode_dated,
                        )

                    # Form199A -- function has the SP-correct gate internally
                    # (mode in {2,4} AND !IsPEModel AND enabled). Skips otherwise.
                    mode_non_dated, _ = compute_form199a_effective_percentage(
                        spark, cfg, mode_non_dated, book_effective, mode_input_lines,
                        mode_temp_cost_pct,
                    )

                    # State allocation per SP gate at line 2866: IF @Mode = 3
                    _sm_has_data = mode == 3 and sm_input is not None and not sm_input.isEmpty()
                    if _sm_has_data:
                        mode_all_underlyings, state_input_lines, sm_eff_amounts = build_state_allocation_input(
                            spark, cfg, common_underlying_mod, sm_input, cost_pct_snapshot_123,
                            mode_all_underlyings, map_dar, dar_setup, entity_partners,
                        )
                        mode_non_dated, mode_dated = build_state_entities(
                            spark, cfg, state_input_lines, mode_non_dated, mode_dated,
                        )
                        if sm_eff_amounts is not None:
                            mode_final_amounts = (
                                mode_final_amounts.unionByName(sm_eff_amounts, allowMissingColumns=True)
                                if mode_final_amounts is not None else sm_eff_amounts
                            )

                    # Pre-cost_pct_by_type checkpoints for non_dated/dated.
                    # These break the DAG lineage so that build_cost_percentage_by_type's
                    # 7 internal checkpoints don't re-evaluate the upstream pipeline.
                    is_footnote_path = (mode == 2)
                    is_state_path = _sm_has_data
                    if is_footnote_path or is_state_path:
                        _ckpt_t0 = time.time()
                        mode_non_dated = _checkpoint(spark, mode_non_dated, f"nde_pre_cpbt_m{mode}", cfg)
                        mode_dated = _checkpoint(spark, mode_dated, f"de_pre_cpbt_m{mode}", cfg)
                        # all_underlyings:
                        #   Footnote path: checkpointed above as all_und_final_m{mode}.
                        #   State path:    checkpointed INSIDE build_state_allocation_input
                        #                  (state_updated_all_und) so passes 1-4 of
                        #                  state_input_lines + sm_entity_amounts share
                        #                  the same materialization. No outer cp needed.
                        logger.info(f"[TIMING] pre_cpbt_checkpoints_m{mode}: {time.time() - _ckpt_t0:.1f}s")

                    # transfers_adj per-mode: SP read is COMMON but the join
                    # with all_underlyings is per-mode (mode 2/3 augment differ
                    # AND chain differs between mode 1 and modes 2/3).
                    # Insert is gated `IF @Mode != 4` -- for modes_123 this fires
                    # for all 3 modes.
                    transfers_adj = load_transfers_adj_cost(
                        spark, cfg, mode_all_underlyings, mode_entity_underlyings,
                    )
                    if transfers_adj is not None:
                        transfers_adj = _checkpoint(spark, transfers_adj, f"txfr_pre_cpbt_m{mode}", cfg)

                    per_mode_data[mode] = {
                        "temp_cost_pct":     mode_temp_cost_pct,
                        "all_underlyings":   mode_all_underlyings,
                        "entity_underlyings": mode_entity_underlyings,   # per-chain
                        "non_dated_entities": mode_non_dated,
                        "dated_entities":    mode_dated,
                        "transfers_adj":     transfers_adj,
                        "input_lines":       mode_input_lines,           # per-chain
                        "final_amounts":     mode_final_amounts,
                    }
                except Exception as e:
                    mode_status["status"] = "FAIL"
                    mode_status["error"] = str(e)
                    logger.error(f"[FAIL] mode {mode} prep: {e}", exc_info=True)
                    raise

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

                fused_temp_cost_pct, fused_transfers_adj = build_cost_percentage_by_type(
                    spark, cfg, cost_pct_snapshot_123,
                    tagged_temp_cost_pct, tagged_all_underlyings, tagged_entity_underlyings,
                    tagged_non_dated, tagged_dated, tagged_transfers_adj,
                    checkpoint_fn=_checkpoint,
                )

                # Checkpoint fused outputs -- tcp_by_type has 3+ downstream
                # consumers (compute_missing_entities, build_final_cost_percentage,
                # compute_minimum_quarter). Without this, each consumer
                # re-evaluates the entire 7-step priority matching from scratch.
                fused_temp_cost_pct = _checkpoint(spark, fused_temp_cost_pct, "tcp_by_type_fused", cfg)
                # txfr_adj_fused: REQUIRED - localCheckpoint strips alias
                # metadata. Without this, compute_effective_percentage_dated
                # hits UNRESOLVED_COLUMN because trans_adj_default uses
                # .alias("T") internally, and the function re-aliases as "T".
                fused_transfers_adj = _checkpoint(spark, fused_transfers_adj, "txfr_adj_fused", cfg)

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

                # compute_missing_entities -- runs once on fused inputs.
                cfg["_current_mode"] = 0   # sentinel for checkpoint naming
                fused_non_dated_post, fused_dated_post = compute_missing_entities(
                    cfg, tagged_non_dated_entities, tagged_dated_entities, fused_temp_cost_pct,
                )
                fused_non_dated_post = _checkpoint(spark, fused_non_dated_post, "nde_post_miss_fused", cfg)
                fused_dated_post = _checkpoint(spark, fused_dated_post, "de_post_miss_fused", cfg)

                # build_final_cost_percentage -- runs once on fused temp_cost_pct.
                fused_final_cost_pct = build_final_cost_percentage(fused_temp_cost_pct, entity_partners)
                fused_final_cost_pct = _checkpoint(spark, fused_final_cost_pct, "final_cost_pct_fused", cfg)

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

                # compute_minimum_quarter -- runs once on fused inputs.
                # Note: returned dated_entities is the post-min-quarter version
                # (with _mode column preserved by the now-_mode-aware function).
                cfg["_current_mode"] = 0
                _, fused_cost_pct_min_quarter, fused_dated_post = compute_minimum_quarter(
                    spark, cfg, fused_final_cost_pct, fused_dated_post,
                )

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
                fused_eff_pct_dated, fused_pickup_order_dated, fused_dated_post = compute_effective_percentage_dated(
                    spark, cfg, fused_dated_post, fused_final_cost_pct, fused_cost_pct_min_quarter,
                    fused_transfers_adj, entity_partners, line_items,
                    checkpoint_fn=_checkpoint,
                )
                if fused_eff_pct_dated is None:
                    raise RuntimeError(
                        f"compute_effective_percentage_dated returned None: "
                        "Yearly prorata percentages missing"
                    )
                # Checkpoint dated output -- the "missing partners" step after
                # the last internal checkpoint re-introduces T.* alias refs.
                fused_eff_pct_dated = _checkpoint(spark, fused_eff_pct_dated, "eff_dt_fused", cfg)

                fused_eff_pct_non_dated = compute_effective_percentage_non_dated(
                    spark, cfg, fused_non_dated_post, fused_final_cost_pct,
                    fused_cost_pct_min_quarter, fused_transfers_adj,
                )
                fused_eff_pct_non_dated = _checkpoint(spark, fused_eff_pct_non_dated, "eff_nd_fused", cfg)

                fused_eff_pct_dated_rounded, fused_eff_pct_nd_rounded = apply_plugging(
                    spark, cfg, fused_eff_pct_dated, fused_eff_pct_non_dated, dar_setup,
                )
                fused_eff_pct_dated_rounded = _checkpoint(spark, fused_eff_pct_dated_rounded, "eff_dt_plug_fused", cfg)
                fused_eff_pct_nd_rounded = _checkpoint(spark, fused_eff_pct_nd_rounded, "eff_nd_plug_fused", cfg)

                fused_eff_pct_dated_rounded, fused_eff_pct_nd_rounded = apply_type_id_update(
                    cfg, fused_eff_pct_dated_rounded, fused_eff_pct_nd_rounded,
                    cfg.get("_non_dated_entities_cost"), cfg.get("_dated_entities_cost"),
                )

            # Per-mode loop for build_final_output (mode-specific output schema).
            for mode in valid_modes_123:
                cfg["mode"] = mode
                cfg["_current_mode"] = mode
                mt0 = mode_t0[mode]
                mode_status = statuses[mode]
                d = per_mode_data[mode]

                logger.info(f"[START] mode {mode} build_final_output (Pass C) within run_modes({modes})")

                try:
                    # Filter fused effective_calc/plugging outputs to this mode.
                    eff_pct_dated_rounded = (
                        fused_eff_pct_dated_rounded.filter(F.col("_mode") == mode).drop("_mode")
                    )
                    eff_pct_nd_rounded = (
                        fused_eff_pct_nd_rounded.filter(F.col("_mode") == mode).drop("_mode")
                    )
                    pickup_order_dated = (
                        fused_pickup_order_dated.filter(F.col("_mode") == mode).drop("_mode")
                    )
                    entity_underlyings = d["entity_underlyings"]
                    final_amounts      = d["final_amounts"]
                    input_lines        = d["input_lines"]

                    # Mode-specific output assembly (Phase 4: only build_final_output
                    # remains per-mode because output schema differs per mode).
                    result = build_final_output(
                        spark, cfg, eff_pct_dated_rounded, eff_pct_nd_rounded,
                        pickup_order_dated, entity_underlyings,
                        final_amounts,
                    )
                    result = result.withColumn("_mode", F.lit(mode))
                    mode_status["result"] = result

                    log_id = cfg.get("log_id")
                    if log_id is not None:
                        spark.sql(f"""
                            UPDATE {cfg['catalog']}.{cfg['schema']}.AllocationLog
                            SET EndDate = current_timestamp()
                            WHERE LogID = {log_id}
                        """)

                    logger.info(f"[DONE] mode {mode} computation complete")
                except Exception as e:
                    mode_status["status"] = "FAIL"
                    mode_status["error"] = str(e)
                    logger.error(f"[FAIL] mode {mode} downstream: {e}", exc_info=True)
                    raise

                mode_status["elapsed_seconds"] = round(time.time() - mt0, 1)

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
        status_out = {
            "statuses": statuses,
            "elapsed_seconds": round(time.time() - t0, 1),
            "_checkpoint_tables": list(cfg.get("_checkpoint_tables", [])),
            "_save_return_value": save_return_value,
        }
        # GAP-46: _drop_checkpoints called here. In test notebooks,
        # monkey-patch to no-op and clean up manually.
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

def _production_run_final(
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
    inner = _delegated_run_modes(
        spark, modes=modes_to_run,
        entity_id=entity_id, client_id=client_id,
        tax_period_id=tax_period_id, run_id=run_id,
        catalog=catalog, schema=schema,
        cfg=cfg, verbose=verbose,
        ResultType=ResultType, VolumePath=VolumePath, ExecutionID=ExecutionID,
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



# ---------------------------------------------------------------------------
# Optimized helpers (outputV2-owned)
# ---------------------------------------------------------------------------
# Live production helpers (copied from the locked outputV2 candidate).
# Promoted experiment defaults are applied as live cfg flags below.
from AllocationV2.usp_get_final_effective_percentage.output import (
    cost_pct_loader as _opt_cost_pct_loader,
    state_allocation as _opt_state_allocation,
    pfic_footnotes as _opt_pfic_footnotes,
    effective_calc as _opt_effective_calc,
    entity_hierarchy as _opt_entity_hierarchy,
)

_base = sys.modules[__name__]
_PRODUCTION_RUN_MODES = _production_run_modes
_PRODUCTION_RUN_FINAL = _production_run_final
_PRODUCTION_RESULT_STORER = GenericResultStorer

_OPTIMIZED_BUSINESS_EXPORTS = {
    _opt_cost_pct_loader: (
        "build_entity_underlyings",
        "load_transfers_adj_cost",
        "build_cost_percentage_by_type",
        "compute_missing_entities",
        "build_final_cost_percentage",
        "validate_cost_percentage_sum",
        "compute_minimum_quarter",
    ),
    _opt_state_allocation: (
        "build_state_allocation_input",
        "build_state_entities",
    ),
    _opt_pfic_footnotes: (
        "build_footnote_underlyings_ordered",
        "build_footnote_input_lines",
        "build_footnote_dated_entities",
        "_get_custom_footnote_line_types",
    ),
    _opt_effective_calc: (
        "compute_effective_percentage_dated",
        "compute_effective_percentage_non_dated",
        "apply_plugging",
        "apply_type_id_update",
        "build_final_output",
    ),
    _opt_entity_hierarchy: (
        "build_entity_hierarchy",
    ),
}

for _opt_module, _opt_names in _OPTIMIZED_BUSINESS_EXPORTS.items():
    for _opt_name in _opt_names:
        _optimized_fn = getattr(_opt_module, _opt_name, None)
        if callable(_optimized_fn):
            setattr(_base, _opt_name, _optimized_fn)

_ACTIVE_EVENTS: contextvars.ContextVar[list | None] = contextvars.ContextVar(
    "fep_output_v3_events", default=None
)
_ACTIVE_COORDINATOR = contextvars.ContextVar(
    "fep_output_v3_coordinator", default=None
)
_ACTIVE_RUN_CFG: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "fep_output_v3_run_cfg", default=None
)
_ACTIVE_STAGE_OVERRIDE: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("fep_output_v3_stage_override", default=None)
)
_ACTIVE_CHECKPOINT_MODE: contextvars.ContextVar[int] = contextvars.ContextVar(
    "fep_output_v3_checkpoint_mode", default=1
)
_ACTIVE_PROFILE_PLAN: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "fep_output_v3_profile_plan", default=False
)
_ACTIVE_PLAN_THRESHOLD: contextvars.ContextVar[int] = contextvars.ContextVar(
    "fep_output_v3_plan_threshold", default=30
)
_ACTIVE_EXPERIMENT: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "fep_output_v3_experiment", default=None
)
_NAMED_ACTION_DEPTH: contextvars.ContextVar[int] = contextvars.ContextVar(
    "fep_output_v3_named_action_depth", default=0
)
_EVENT_LOCK = threading.Lock()
_PROCESS_PRINT_LOCK = threading.Lock()
_LAST_RUN_PROFILE: dict[str, Any] = {}
_ALL_PARALLEL_GROUPS = frozenset(
    {
        "common_dimensions",
        "common_inputs",
        "lookthrough_metadata",
        "lt_nolt_branches",
        "mode_prep",
        "mode_prep_boundaries",
        "cpbt_boundaries",
        "cpbt_internal_boundaries",
        "cpbt_post_validate",
        "effective_inputs",
        "fused_effective",
        "effective_boundaries",
        "output_build",
        "output_writes",
    }
)

# Locked promoted outputV2 defaults for Entity 4137 / RunID 17376.
# Do not change a value here unless a new exact-parity Databricks run is
# faster. SkipYearlyEmptyProbe must stay False: enabling it dropped
# tcp_with_yearly_common and regressed tcp_post_et_m0 from ~2.5s to ~16s.
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


def _table(spark, cfg, name):
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _line_items_without_warning_probe(spark, cfg):
    k1 = _table(spark, cfg, "K1LineItem").select(
        "LineID",
        "AllocationTypeRuleId",
        F.lit(cfg["k1_line_type_id"]).cast("int").alias("LineTypeID"),
        "TransactionDate",
        "IsTransactionDate",
        "IsTransfersAdjusted",
    )
    box_jkl = _table(spark, cfg, "BoxjklLineItem").select(
        "LineID",
        F.lit(cfg["yearly_allocation_type_id"])
        .cast("int")
        .alias("AllocationTypeRuleId"),
        F.lit(cfg["box_jkl_line_type_id"]).cast("int").alias("LineTypeID"),
        F.lit(None).cast("timestamp").alias("TransactionDate"),
        F.lit(False).alias("IsTransactionDate"),
        F.lit(True).alias("IsTransfersAdjusted"),
    )
    return k1.unionByName(box_jkl)


def _quarters_without_warning_probe(spark, cfg):
    if (
        cfg.get("allocation_type_name", "") == "PE Book Allocation"
        and cfg.get("is_dated_transfers_configured", "") == "C"
    ):
        return _table(spark, cfg, "QuarterDates").select("Quarter")
    return (
        _table(spark, cfg, "ENU_DF_DataList")
        .filter(F.col("Category") == "Quarters")
        .select(F.col("LookUpData").alias("Quarter"))
    )


def _lookthrough_without_warning_probe(spark, cfg):
    k1_id = cfg["k1_line_type_id"]
    adjustment_id = cfg["adjustment_line_type_id"]
    box_jkl_id = cfg["box_jkl_line_type_id"]
    return (
        _table(spark, cfg, "LookThroughAllocationInput")
        .filter(
            (F.col("RunID") == cfg["run_id"])
            & (F.col("ClientID") == cfg["client_id"])
            & F.col("LineTypeID").isin(
                [k1_id, adjustment_id, box_jkl_id]
            )
            & (
                (F.col("LineTypeID") == box_jkl_id)
                | (
                    F.col("LineTypeID").isin([k1_id, adjustment_id])
                    & (
                        _book_effective._sql_round(
                            F.coalesce(F.col("Amount"), F.lit(0.0)), 0
                        )
                        != 0
                    )
                )
            )
        )
        .select(
            "RunID",
            "ClientID",
            "EntityID",
            "LineTypeID",
            "LineID",
            "Amount",
            "QuicklinkID",
            "Amount704b",
            "TrackingKey",
            "Tag",
        )
    )


WARNING_PROBE_BUILDERS = {
    "line_items": _line_items_without_warning_probe,
    "quarters": _quarters_without_warning_probe,
    "lookthrough": _lookthrough_without_warning_probe,
}


def _workers(value: Any) -> int:
    try:
        return max(1, min(int(value), 4))
    except (TypeError, ValueError):
        return 4


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "on", "true", "yes"}
    return bool(value)


def _blank(value: Any):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _record(
    stage: str, operation: str, elapsed: float, **details: Any
) -> None:
    sink = _ACTIVE_EVENTS.get()
    if sink is not None:
        with _EVENT_LOCK:
            sink.append(
                {
                    "stage": stage,
                    "operation": operation,
                    "elapsed_seconds": round(elapsed, 3),
                    **details,
                }
            )


def _print_process(
    event: str,
    kind: str,
    name: str,
    stage: str | None,
    *,
    elapsed: float | None = None,
    status: str | None = None,
) -> None:
    """Print one compact, thread-safe live process event."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    thread_name = threading.current_thread().name
    details = [
        f"[{timestamp}]",
        "[fep process]",
        event,
        f"kind={kind}",
        f"name={name}",
        f"stage={stage or 'n/a'}",
        f"thread={thread_name}",
    ]
    if status is not None:
        details.append(f"status={status}")
    if elapsed is not None:
        details.append(f"elapsed={elapsed:.3f}s")
    with _PROCESS_PRINT_LOCK:
        # Databricks can drop stdout from imported modules and worker threads,
        # while the production orchestrator's configured logger is consistently
        # rendered in the notebook cell output.
        _base.logger.info(" ".join(details))


def _relation_metrics(df, enabled: bool) -> dict:
    if df is None or not enabled:
        return {
            "incoming_plan_nodes": None,
            "incoming_plan_depth": None,
            "incoming_partitions": None,
        }
    try:
        tree = (
            df._jdf.queryExecution()
            .optimizedPlan()
            .numberedTreeString()
        )
        lines = [line for line in tree.splitlines() if line.strip()]
        depths = [
            max(0, (len(line) - len(line.lstrip(" |:+-"))) // 2)
            for line in lines
        ]
    except Exception:
        lines, depths = [], []
    try:
        partitions = int(
            df._jdf.queryExecution()
            .sparkPlan()
            .outputPartitioning()
            .numPartitions()
        )
    except Exception:
        partitions = None
    return {
        "incoming_plan_nodes": len(lines) or None,
        "incoming_plan_depth": max(depths, default=0) if lines else None,
        "incoming_partitions": partitions,
    }


def _timed(stage: str, operation: str, fn: Callable) -> Callable:
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        active_stage = _ACTIVE_STAGE_OVERRIDE.get() or stage
        started = time.time()
        status = "PASS"
        _print_process("START", "helper", operation, active_stage)
        try:
            return fn(*args, **kwargs)
        except Exception:
            status = "FAIL"
            raise
        finally:
            elapsed = time.time() - started
            _record(
                active_stage,
                operation,
                elapsed,
            )
            _print_process(
                "DONE",
                "helper",
                operation,
                active_stage,
                elapsed=elapsed,
                status=status,
            )

    return wrapped


class _Coordinator:
    """Bounded executor for explicitly approved independent operations."""

    def __init__(self, workers: int, enabled_groups=frozenset()):
        self.workers = workers
        self.enabled_groups = frozenset(enabled_groups)
        self._executor = (
            ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fep-v3")
            if workers > 1
            else None
        )
        self._futures = {}
        self._events = []
        self._lock = threading.Lock()
        self._next_wave = 0

    def submit_group(self, group, tasks) -> None:
        if self._executor is None or group not in self.enabled_groups:
            return
        with self._lock:
            pending = [
                task for task in tasks
                if (group, task[0]) not in self._futures
            ]
            if not pending:
                return
            self._next_wave += 1
            wave = self._next_wave
            for name, fn, args, kwargs in pending:
                key = (group, name)
                context = contextvars.copy_context()
                self._futures[key] = self._executor.submit(
                    context.run,
                    self._execute,
                    wave,
                    group,
                    name,
                    fn,
                    args,
                    kwargs,
                )

    def _execute(self, wave, group, name, fn, args, kwargs):
        started = time.time()
        status = "PASS"
        stage = {
            "common_dimensions": StageName.COMMON_READS.value,
            "common_inputs": StageName.COMMON_READS.value,
            "lookthrough_metadata": StageName.COMMON_READS.value,
            "mode_prep": StageName.MODE_PREP.value,
            "mode_prep_boundaries": StageName.MODE_PREP.value,
            "cpbt_boundaries": StageName.FUSED_CPBT.value,
            "cpbt_internal_boundaries": StageName.FUSED_CPBT.value,
            "cpbt_post_validate": StageName.FUSED_CPBT.value,
            "effective_inputs": StageName.FUSED_EFFECTIVE.value,
            "fused_effective": StageName.FUSED_EFFECTIVE.value,
            "effective_boundaries": StageName.FUSED_EFFECTIVE.value,
            "output_build": StageName.OUTPUT_BUILD.value,
            "output_writes": StageName.OUTPUT_WRITE.value,
        }.get(group)
        if group == "lt_nolt_branches":
            stage = (
                StageName.NO_LT_BRANCH.value
                if name == "nolt"
                else StageName.WITH_LT_BRANCH.value
            )
        stage_token = _ACTIVE_STAGE_OVERRIDE.set(stage)
        _print_process("START", "task", f"{group}.{name}", stage)
        try:
            return fn(*args, **kwargs)
        except Exception:
            status = "FAIL"
            raise
        finally:
            elapsed = time.time() - started
            _ACTIVE_STAGE_OVERRIDE.reset(stage_token)
            with self._lock:
                self._events.append(
                    {
                        "group": group,
                        "wave": wave,
                        "task": name,
                        "status": status,
                        "elapsed_seconds": round(elapsed, 3),
                        "thread": threading.current_thread().name,
                    }
                )
            _print_process(
                "DONE",
                "task",
                f"{group}.{name}",
                stage,
                elapsed=elapsed,
                status=status,
            )

    def result(self, group, name, fn, *args, **kwargs):
        if self._executor is None or group not in self.enabled_groups:
            return fn(*args, **kwargs)
        self.submit_group(group, ((name, fn, args, kwargs),))
        return self._futures[(group, name)].result()

    def run_group(self, group, tasks):
        """Run every task, observe every future, then raise on the main thread."""
        tasks = tuple(tasks)
        if self._executor is None or group not in self.enabled_groups:
            results = {}
            with self._lock:
                self._next_wave += 1
                wave = self._next_wave
            for name, fn, args, kwargs in tasks:
                results[name] = self._execute(
                    wave, group, name, fn, args, kwargs
                )
            return results
        self.submit_group(group, tasks)
        results = {}
        failures = []
        for name, _, _, _ in tasks:
            try:
                results[name] = self._futures[(group, name)].result()
            except Exception as exc:
                failures.append((name, exc))
        if failures:
            detail = [(name, f"{type(exc).__name__}: {exc}") for name, exc in failures]
            error = RuntimeError(f"Parallel group {group!r} failed: {detail}")
            raise error from failures[0][1]
        return results

    def close(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)

    @property
    def events(self):
        with self._lock:
            return sorted(
                (dict(item) for item in self._events),
                key=lambda item: (item["group"], item["task"]),
            )


def _checkpoint(spark, df, name, cfg):
    return named_checkpoint(spark, df, name, cfg)


def _profile_pipeline_action(name, stage, df, action, cfg):
    started = time.time()
    status = "PASS"
    _print_process("START", "action", name, stage)
    metrics = _relation_metrics(
        df, bool(isinstance(cfg, dict) and cfg.get("profile_plan"))
    )
    depth_token = _NAMED_ACTION_DEPTH.set(_NAMED_ACTION_DEPTH.get() + 1)
    try:
        return action()
    except Exception:
        status = "FAIL"
        raise
    finally:
        elapsed = time.time() - started
        _NAMED_ACTION_DEPTH.reset(depth_token)
        _record(
            stage,
            f"action:{name}",
            elapsed,
            **metrics,
        )
        _print_process(
            "DONE",
            "action",
            name,
            stage,
            elapsed=elapsed,
            status=status,
        )


_ORIGINAL_DF_ISEMPTY = DataFrame.isEmpty


def _profiled_dataframe_is_empty(self):
    """Name helper-internal probes while preserving the original action."""
    if _NAMED_ACTION_DEPTH.get() > 0:
        return _ORIGINAL_DF_ISEMPTY(self)
    frame = inspect.currentframe()
    caller_name = "unknown"
    try:
        frame = frame.f_back if frame is not None else None
        while frame is not None:
            module_name = str(frame.f_globals.get("__name__", ""))
            if (
                "usp_get_final_effective_percentage.output" in module_name
                and "orchestrator" not in module_name.split(".")[-1:]
            ):
                caller_name = frame.f_code.co_name
                break
            frame = frame.f_back
    finally:
        del frame
    stage = (
        _ACTIVE_STAGE_OVERRIDE.get()
        or FUNCTION_STAGE.get(caller_name)
        or StageName.MODE_PREP.value
    )
    cfg = _ACTIVE_RUN_CFG.get()
    return _profile_pipeline_action(
        f"{caller_name}.isEmpty",
        stage,
        self,
        lambda: _ORIGINAL_DF_ISEMPTY(self),
        cfg,
    )


def _drop_checkpoints_noop(spark, cfg):
    # Names include a sequence and UUID in Common_V2. Cleanup is intentionally
    # outside the hot path so returned lazy relations stay valid.
    return None


def _build_cfg(bound: inspect.BoundArguments) -> dict:
    cfg = bound.arguments.get("cfg")
    if cfg is None:
        cfg = _base.load_common_config(
            bound.arguments["spark"],
            entity_id=bound.arguments.get("entity_id"),
            client_id=bound.arguments.get("client_id"),
            tax_period_id=bound.arguments.get("tax_period_id"),
            run_id=bound.arguments.get("run_id"),
            catalog=bound.arguments.get("catalog"),
            schema=bound.arguments.get("schema"),
        )
        bound.arguments["cfg"] = cfg
    return cfg


def _delegated_run_modes(*args, **kwargs):
    bound = inspect.signature(_PRODUCTION_RUN_MODES).bind_partial(*args, **kwargs)
    bound.apply_defaults()
    cfg = _build_cfg(bound)
    cfg.pop("_checkpoint_v2_state", None)
    cfg["profile_plan"] = _ACTIVE_PROFILE_PLAN.get()
    cfg["plan_checkpoint_threshold"] = _ACTIVE_PLAN_THRESHOLD.get()
    cfg.update(_ACTIVE_EXPERIMENT.get() or {})
    cfg.setdefault("result_type", bound.arguments.get("ResultType"))
    if bound.arguments.get("VolumePath") is not None:
        cfg["volume_path"] = bound.arguments["VolumePath"]
    if bound.arguments.get("ExecutionID") is not None:
        cfg["execution_id"] = bound.arguments["ExecutionID"]
    mode = resolve_checkpoint_mode(
        cfg, checkpoint_mode=_ACTIVE_CHECKPOINT_MODE.get()
    )
    initialize_named_checkpoint_policy(cfg, checkpoint_mode=mode)
    _ACTIVE_RUN_CFG.set(cfg)
    coordinator = _ACTIVE_COORDINATOR.get()
    if coordinator is None:
        return _PRODUCTION_RUN_MODES(*bound.args, **bound.kwargs)
    return run_modes_parallel(
        _base,
        coordinator.run_group,
        _PRODUCTION_RUN_MODES,
        *bound.args,
        **bound.kwargs,
    )


_base._checkpoint = _checkpoint
_base._profile_pipeline_action = _profile_pipeline_action
_base._drop_checkpoints = _drop_checkpoints_noop
_base.run_modes = _delegated_run_modes

# Time production functions without replacing or copying their business logic.
for _function_name, _stage_name in FUNCTION_STAGE.items():
    _function = getattr(_base, _function_name, None)
    if callable(_function):
        setattr(
            _base,
            _function_name,
            _timed(_stage_name, _function_name, _function),
        )

_PARALLEL_ORIGINALS = {
    name: getattr(_base, name)
    for name in (
        "build_cost_percentage_snapshot_modes123",
        "build_cost_percentage_snapshot_mode4",
        "build_entity_partners",
        "build_asset_class_relationship",
        "load_line_items",
        "load_book_effective_data",
        "load_quarters",
        "load_yearly_data",
        "build_lookthrough_input_modes14",
        "build_footnote_lines",
    )
}

def _parallel_result(group, name, *args, **kwargs):
    coordinator = _ACTIVE_COORDINATOR.get()
    fn = _PARALLEL_ORIGINALS[name]
    cfg = args[1] if len(args) > 1 and isinstance(args[1], dict) else {}
    if cfg.get("_output_v3_warning_probe_removal") == {
        "load_line_items": "line_items",
        "load_quarters": "quarters",
        "build_lookthrough_input_modes14": "lookthrough",
    }.get(name):
        fn = WARNING_PROBE_BUILDERS[
            cfg["_output_v3_warning_probe_removal"]
        ]
    if coordinator is None:
        return fn(*args, **kwargs)
    return coordinator.result(group, name, fn, *args, **kwargs)


def _snapshot_wrapper(name):
    @functools.wraps(_PARALLEL_ORIGINALS[name])
    def wrapped(spark, cfg, *args, **kwargs):
        coordinator = _ACTIVE_COORDINATOR.get()
        if coordinator is not None:
            coordinator.submit_group(
                "common_dimensions",
                (
                    (
                        "build_entity_partners",
                        _PARALLEL_ORIGINALS["build_entity_partners"],
                        (spark, cfg),
                        {},
                    ),
                    (
                        "build_asset_class_relationship",
                        _PARALLEL_ORIGINALS["build_asset_class_relationship"],
                        (spark, cfg),
                        {},
                    ),
                    (name, _PARALLEL_ORIGINALS[name], (spark, cfg, *args), kwargs),
                ),
            )
        return _parallel_result(
            "common_dimensions", name, spark, cfg, *args, **kwargs
        )

    return wrapped


def _line_items_wrapper(spark, cfg, *args, **kwargs):
    coordinator = _ACTIVE_COORDINATOR.get()
    names = (
        "load_line_items",
        "load_book_effective_data",
        "load_quarters",
        "load_yearly_data",
    )
    if coordinator is not None:
        line_items_fn = (
            WARNING_PROBE_BUILDERS["line_items"]
            if cfg.get("_output_v3_warning_probe_removal") == "line_items"
            else _PARALLEL_ORIGINALS["load_line_items"]
        )
        coordinator.submit_group(
            "common_inputs",
            tuple(
                (
                    name,
                    (
                        line_items_fn
                        if name == "load_line_items"
                        else (
                            WARNING_PROBE_BUILDERS["quarters"]
                            if name == "load_quarters"
                            and cfg.get(
                                "_output_v3_warning_probe_removal"
                            )
                            == "quarters"
                            else _PARALLEL_ORIGINALS[name]
                        )
                    ),
                    (spark, cfg),
                    {},
                )
                for name in names
            ),
        )
    return _parallel_result(
        "common_inputs", "load_line_items", spark, cfg, *args, **kwargs
    )


def _lookthrough_wrapper(spark, cfg, *args, **kwargs):
    coordinator = _ACTIVE_COORDINATOR.get()
    names = ("build_lookthrough_input_modes14", "build_footnote_lines")
    if coordinator is not None:
        lookthrough_fn = (
            WARNING_PROBE_BUILDERS["lookthrough"]
            if cfg.get("_output_v3_warning_probe_removal") == "lookthrough"
            else _PARALLEL_ORIGINALS[
                "build_lookthrough_input_modes14"
            ]
        )
        coordinator.submit_group(
            "lookthrough_metadata",
            tuple(
                (
                    name,
                    (
                        lookthrough_fn
                        if name == "build_lookthrough_input_modes14"
                        else _PARALLEL_ORIGINALS[name]
                    ),
                    (spark, cfg),
                    {},
                )
                for name in names
            ),
        )
    return _parallel_result(
        "lookthrough_metadata",
        "build_lookthrough_input_modes14",
        spark,
        cfg,
        *args,
        **kwargs,
    )


for _name in (
    "build_cost_percentage_snapshot_modes123",
    "build_cost_percentage_snapshot_mode4",
):
    setattr(_base, _name, _snapshot_wrapper(_name))
for _name, _group in (
    ("build_entity_partners", "common_dimensions"),
    ("build_asset_class_relationship", "common_dimensions"),
    ("load_book_effective_data", "common_inputs"),
    ("load_quarters", "common_inputs"),
    ("load_yearly_data", "common_inputs"),
    ("build_footnote_lines", "lookthrough_metadata"),
):
    setattr(_base, _name, functools.partial(_parallel_result, _group, _name))
_base.load_line_items = _line_items_wrapper
_base.build_lookthrough_input_modes14 = _lookthrough_wrapper


class _ParallelResultStorer(_PRODUCTION_RESULT_STORER):
    """Write only distinct output tables concurrently."""

    def _write(self, df, catalog, database, table, run_id):
        started = time.time()
        status = "PASS"
        stage = StageName.OUTPUT_WRITE.value
        _print_process("START", "write", table, stage)
        try:
            return self.store_output_to_delta_table(
                df, catalog, database, table, run_id
            )
        except Exception:
            status = "FAIL"
            raise
        finally:
            elapsed = time.time() - started
            _record(
                stage,
                f"write:{table}",
                elapsed,
            )
            _print_process(
                "DONE",
                "write",
                table,
                stage,
                elapsed=elapsed,
                status=status,
            )

    def store_output_to_delta_lake(
        self, result, catalog_name, database_name, run_id
    ):
        coordinator = _ACTIVE_COORDINATOR.get()
        if (
            coordinator is None
            or coordinator.workers <= 1
            or "output_writes" not in coordinator.enabled_groups
            or len(result) <= 1
        ):
            return super().store_output_to_delta_lake(
                result, catalog_name, database_name, run_id
            )
        tasks = tuple(
            (
                table,
                self._write,
                (df, catalog_name, database_name, table, run_id),
                {},
            )
            for table, df in result.items()
        )
        coordinator.submit_group("output_writes", tasks)
        failures = []
        for table in result:
            try:
                coordinator.result("output_writes", table, self._write)
            except Exception as exc:
                failures.append((table, str(exc)))
        if failures:
            raise RuntimeError(f"FEP output write failures: {failures}")
        return None


_base.GenericResultStorer = _ParallelResultStorer


def _stage_summary(events):
    totals = defaultdict(float)
    calls = defaultdict(int)
    for event in events:
        totals[event["stage"]] += event["elapsed_seconds"]
        calls[event["stage"]] += 1
    return [
        {
            "stage": stage.value,
            "calls": calls[stage.value],
            "elapsed_seconds": round(totals[stage.value], 3),
        }
        for stage in StageName
    ]


def _performance_summary(wall, cfg, events, parallel_events):
    checkpoint_rows = (
        list(cfg.get("_checkpoint_policy_activity", ()))
        if isinstance(cfg, dict)
        else []
    )
    checkpoint_seconds = round(
        sum(float(row.get("elapsed_seconds", 0) or 0) for row in checkpoint_rows),
        3,
    )
    action_events = [
        row for row in events if str(row.get("operation", "")).startswith("action:")
    ]
    action_seconds = round(
        sum(float(row.get("elapsed_seconds", 0) or 0) for row in action_events),
        3,
    )
    parallel_by_group = defaultdict(list)
    for row in parallel_events:
        parallel_by_group[row["group"]].append(
            float(row.get("elapsed_seconds", 0) or 0)
        )
    parallel_critical = {
        group: round(max(values), 3)
        for group, values in sorted(parallel_by_group.items())
        if values
    }
    parallel_by_wave = defaultdict(list)
    for row in parallel_events:
        parallel_by_wave[int(row.get("wave", 0))].append(row)
    wave_critical_path = []
    for wave, rows in sorted(parallel_by_wave.items()):
        elapsed = max(
            float(row.get("elapsed_seconds", 0) or 0) for row in rows
        )
        wave_critical_path.append(
            {
                "wave": wave,
                "elapsed_seconds": round(elapsed, 3),
                "groups": sorted({row["group"] for row in rows}),
                "tasks": sorted(row["task"] for row in rows),
            }
        )
    critical_actions = sorted(
        [
            {
                "kind": "checkpoint",
                "name": row.get("name"),
                "stage": row.get("stage"),
                "elapsed_seconds": float(
                    row.get("elapsed_seconds", 0) or 0
                ),
                "incoming_plan_nodes": row.get("incoming_plan_nodes"),
                "incoming_plan_depth": row.get("incoming_plan_depth"),
                "incoming_partitions": row.get("incoming_partitions"),
            }
            for row in checkpoint_rows
        ]
        + [
            {
                "kind": "action",
                "name": str(row.get("operation", "")).removeprefix(
                    "action:"
                ),
                "stage": row.get("stage"),
                "elapsed_seconds": float(
                    row.get("elapsed_seconds", 0) or 0
                ),
                "incoming_plan_nodes": row.get("incoming_plan_nodes"),
                "incoming_plan_depth": row.get("incoming_plan_depth"),
                "incoming_partitions": row.get("incoming_partitions"),
            }
            for row in action_events
        ],
        key=lambda row: row["elapsed_seconds"],
        reverse=True,
    )
    return {
        "target_wall_seconds": 50.0,
        "wall_seconds": wall,
        "seconds_over_target": round(max(0.0, wall - 50.0), 3),
        "checkpoint_action_seconds": checkpoint_seconds,
        "explicit_action_seconds": action_seconds,
        "checkpoint_count": len(checkpoint_rows),
        "explicit_action_count": len(action_events),
        "parallel_group_critical_seconds": parallel_critical,
        "parallel_wave_critical_path": wave_critical_path,
        "parallel_wave_total_seconds": round(
            sum(row["elapsed_seconds"] for row in wave_critical_path), 3
        ),
        "critical_actions": critical_actions,
    }


def _run_profiled(fn, *args, **kwargs):
    profile_name = str(
        _blank(kwargs.pop("ExecutionProfile", None))
        or _blank(kwargs.pop("execution_profile", None))
        or "low"
    ).strip().lower()
    profile = resolve_execution_profile(profile_name)
    raw_threads = kwargs.pop("MaxThreads", kwargs.pop("max_threads", None))
    if _blank(raw_threads) is None:
        raw_threads = profile["max_threads"]
    raw_groups = kwargs.pop(
        "ParallelGroups",
        kwargs.pop("parallel_groups", ",".join(sorted(_ALL_PARALLEL_GROUPS))),
    )
    raw_checkpoint = kwargs.pop(
        "CheckpointMode", kwargs.pop("checkpoint_mode", None)
    )
    if _blank(raw_checkpoint) is None or str(raw_checkpoint).strip().lower() == "default":
        checkpoint_mode = int(profile["checkpoint_mode"])
    else:
        checkpoint_mode = int(raw_checkpoint)
    if checkpoint_mode not in {1, 2, 3, 4, 5}:
        raise ValueError("CheckpointMode must be one of 1, 2, 3, 4, 5")
    kwargs.pop("ProfilePlan", None)
    kwargs.pop("profile_plan", None)
    kwargs.pop("PlanCheckpointThreshold", None)
    kwargs.pop("plan_checkpoint_threshold", None)
    profile_plan = False
    plan_threshold = 30
    raw_shuffle = kwargs.pop(
        "SqlShufflePartitions",
        kwargs.pop("sql_shuffle_partitions", None),
    )
    shuffle_partitions = (
        int(profile["shuffle_partitions"])
        if _blank(raw_shuffle) is None
        else int(raw_shuffle)
    )
    experiment = {
        "_output_v3_experiment_id": str(
            kwargs.pop(
                "ExperimentID",
                kwargs.pop("experiment_id", "baseline"),
            )
        ).strip() or "baseline",
        "_output_v3_shuffle_partitions": shuffle_partitions,
        "_output_v3_execution_profile": profile_name,
        "_output_v3_warning_probe_removal": str(
            kwargs.pop(
                "WarningProbeRemoval",
                kwargs.pop(
                    "warning_probe_removal",
                    _PROMOTED_EXPERIMENT_DEFAULTS["warning_probe_removal"],
                ),
            )
        ).strip().lower(),
        "_output_v3_missing_entity_identity": _as_bool(
            kwargs.pop(
                "MissingEntityIdentity",
                kwargs.pop(
                    "missing_entity_identity",
                    _PROMOTED_EXPERIMENT_DEFAULTS["missing_entity_identity"],
                ),
            )
        ),
        "_output_v3_cpbt_input_break": str(
            kwargs.pop(
                "CpbtInputBreak",
                kwargs.pop(
                    "cpbt_input_break",
                    _PROMOTED_EXPERIMENT_DEFAULTS["cpbt_input_break"],
                ),
            )
        ).strip().lower(),
        "_output_v3_cpbt_post_tag_entity_break": _as_bool(
            kwargs.pop(
                "CpbtPostTagEntityBreak",
                kwargs.pop(
                    "cpbt_post_tag_entity_break",
                    _PROMOTED_EXPERIMENT_DEFAULTS["cpbt_post_tag_entity_break"],
                ),
            )
        ),
        "_output_v3_parallel_cpbt_post_tag": _as_bool(
            kwargs.pop(
                "ParallelCpbtPostTag",
                kwargs.pop(
                    "parallel_cpbt_post_tag",
                    _PROMOTED_EXPERIMENT_DEFAULTS["parallel_cpbt_post_tag"],
                ),
            )
        ),
        "_output_v3_compact_all_entities": _as_bool(
            kwargs.pop(
                "CompactAllEntities",
                kwargs.pop(
                    "compact_all_entities",
                    _PROMOTED_EXPERIMENT_DEFAULTS["compact_all_entities"],
                ),
            )
        ),
        "_output_v3_cpbt_narrow_anti_keys": _as_bool(
            kwargs.pop(
                "CpbtNarrowAntiKeys",
                kwargs.pop(
                    "cpbt_narrow_anti_keys",
                    _PROMOTED_EXPERIMENT_DEFAULTS["cpbt_narrow_anti_keys"],
                ),
            )
        ),
        "_output_v3_cpbt_transfer_prefilter": _as_bool(
            kwargs.pop(
                "CpbtTransferPrefilter",
                kwargs.pop(
                    "cpbt_transfer_prefilter",
                    _PROMOTED_EXPERIMENT_DEFAULTS["cpbt_transfer_prefilter"],
                ),
            )
        ),
        "_output_v3_cpbt_drop_tracking_match": _as_bool(
            kwargs.pop(
                "CpbtDropTrackingMatch",
                kwargs.pop(
                    "cpbt_drop_tracking_match",
                    _PROMOTED_EXPERIMENT_DEFAULTS["cpbt_drop_tracking_match"],
                ),
            )
        ),
        "_output_v3_batch_footnote_line_ids": _as_bool(
            kwargs.pop(
                "BatchFootnoteLineIds",
                kwargs.pop(
                    "batch_footnote_line_ids",
                    _PROMOTED_EXPERIMENT_DEFAULTS["batch_footnote_line_ids"],
                ),
            )
        ),
        "_output_v3_footnote_shared_lineage": _as_bool(
            kwargs.pop(
                "FootnoteSharedLineage",
                kwargs.pop(
                    "footnote_shared_lineage",
                    _PROMOTED_EXPERIMENT_DEFAULTS["footnote_shared_lineage"],
                ),
            )
        ),
        "_output_v3_footnote_checkpoint_partitions": int(
            kwargs.pop(
                "FootnoteCheckpointPartitions",
                kwargs.pop(
                    "footnote_checkpoint_partitions",
                    _PROMOTED_EXPERIMENT_DEFAULTS[
                        "footnote_checkpoint_partitions"
                    ],
                ),
            )
        ),
        "_output_v3_broadcast_cpbt_remaining": _as_bool(
            kwargs.pop(
                "BroadcastCpbtRemaining",
                kwargs.pop(
                    "broadcast_cpbt_remaining",
                    _PROMOTED_EXPERIMENT_DEFAULTS["broadcast_cpbt_remaining"],
                ),
            )
        ),
        "_output_v3_hierarchy_materialize": _as_bool(
            kwargs.pop(
                "HierarchyMaterialize",
                kwargs.pop(
                    "hierarchy_materialize",
                    _PROMOTED_EXPERIMENT_DEFAULTS["hierarchy_materialize"],
                ),
            )
        ),
        "_output_v3_parallel_cpbt_validate": _as_bool(
            kwargs.pop(
                "ParallelCpbtValidate",
                kwargs.pop(
                    "parallel_cpbt_validate",
                    _PROMOTED_EXPERIMENT_DEFAULTS["parallel_cpbt_validate"],
                ),
            )
        ),
        "_output_v3_skip_yearly_empty_probe": _as_bool(
            kwargs.pop(
                "SkipYearlyEmptyProbe",
                kwargs.pop(
                    "skip_yearly_empty_probe",
                    _PROMOTED_EXPERIMENT_DEFAULTS["skip_yearly_empty_probe"],
                ),
            )
        ),
        "_output_v3_collapse_state_passes": _as_bool(
            kwargs.pop(
                "CollapseStatePasses",
                kwargs.pop(
                    "collapse_state_passes",
                    _PROMOTED_EXPERIMENT_DEFAULTS["collapse_state_passes"],
                ),
            )
        ),
        "_output_v3_batch_state_workflow_lookup": _as_bool(
            kwargs.pop(
                "BatchStateWorkflowLookup",
                kwargs.pop(
                    "batch_state_workflow_lookup",
                    _PROMOTED_EXPERIMENT_DEFAULTS[
                        "batch_state_workflow_lookup"
                    ],
                ),
            )
        ),
        "_output_v3_single_pickup_antijoin": _as_bool(
            kwargs.pop(
                "SinglePickupAntiJoin",
                kwargs.pop(
                    "single_pickup_antijoin",
                    _PROMOTED_EXPERIMENT_DEFAULTS["single_pickup_antijoin"],
                ),
            )
        ),
        "_output_v3_materialize_effective_inputs": _as_bool(
            kwargs.pop(
                "MaterializeEffectiveInputs",
                kwargs.pop(
                    "materialize_effective_inputs",
                    _PROMOTED_EXPERIMENT_DEFAULTS[
                        "materialize_effective_inputs"
                    ],
                ),
            )
        ),
        "_output_v3_parallel_effective": _as_bool(
            kwargs.pop(
                "ParallelEffective",
                kwargs.pop(
                    "parallel_effective",
                    _PROMOTED_EXPERIMENT_DEFAULTS["parallel_effective"],
                ),
            )
        ),
        "_output_v3_target_checkpoint": str(
            kwargs.pop(
                "TargetCheckpoint",
                kwargs.pop("target_checkpoint", ""),
            )
        ).strip(),
        "_output_v3_target_partition_strategy": str(
            kwargs.pop(
                "TargetPartitionStrategy",
                kwargs.pop("target_partition_strategy", "off"),
            )
        ).strip().lower(),
        "_output_v3_target_partitions": int(
            kwargs.pop(
                "TargetPartitions",
                kwargs.pop("target_partitions", 0),
            )
        ),
        "_output_v3_target_partition_keys": [
            item.strip()
            for item in str(
                kwargs.pop(
                    "TargetPartitionKeys",
                    kwargs.pop("target_partition_keys", ""),
                )
            ).split(",")
            if item.strip()
        ],
        "_output_v3_business_optimization": str(
            kwargs.pop(
                "BusinessOptimization",
                kwargs.pop(
                    "business_optimization",
                    _PROMOTED_EXPERIMENT_DEFAULTS["business_optimization"],
                ),
            )
        ).strip().lower(),
    }
    if experiment["_output_v3_shuffle_partitions"] < 1:
        raise ValueError("SqlShufflePartitions must be >= 1")
    if experiment["_output_v3_footnote_checkpoint_partitions"] < 0:
        raise ValueError("FootnoteCheckpointPartitions must be >= 0")
    if experiment["_output_v3_warning_probe_removal"] not in {
        "off",
        "line_items",
        "quarters",
        "lookthrough",
    }:
        raise ValueError(
            "WarningProbeRemoval must be off, line_items, quarters, "
            "or lookthrough"
        )
    if experiment["_output_v3_cpbt_input_break"] not in {
        "off",
        "non_dated",
        "dated",
        "both",
    }:
        raise ValueError(
            "CpbtInputBreak must be off, non_dated, dated, or both"
        )
    if experiment["_output_v3_target_partition_strategy"] not in {
        "off",
        "coalesce",
        "repartition",
    }:
        raise ValueError(
            "TargetPartitionStrategy must be off, coalesce, or repartition"
        )
    if (
        experiment["_output_v3_target_partition_strategy"] != "off"
        and (
            not experiment["_output_v3_target_checkpoint"]
            or experiment["_output_v3_target_partitions"] < 1
        )
    ):
        raise ValueError(
            "TargetCheckpoint and TargetPartitions >= 1 are required for "
            "targeted partitioning"
        )
    if (
        experiment["_output_v3_target_partition_strategy"] == "repartition"
        and not experiment["_output_v3_target_partition_keys"]
    ):
        raise ValueError(
            "TargetPartitionKeys is required for keyed repartition"
        )
    if experiment["_output_v3_business_optimization"] not in {
        "off",
        "broadcast_entity_partners",
    }:
        raise ValueError(
            "BusinessOptimization must be off or broadcast_entity_partners"
        )
    call_arguments = inspect.signature(fn).bind_partial(
        *args, **kwargs
    ).arguments
    supplied_cfg = call_arguments.get("cfg")
    volume_path = call_arguments.get("VolumePath")
    spark = call_arguments.get("spark") or (args[0] if args else None)
    if spark is not None and _blank(raw_shuffle) is None:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            str(profile["shuffle_partitions"]),
        )
    elif spark is not None and _blank(raw_shuffle) is not None:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            str(shuffle_partitions),
        )
    if checkpoint_mode == 3 and not (
        volume_path
        or (
            isinstance(supplied_cfg, dict)
            and (
                supplied_cfg.get("checkpoint_volume_path")
                or supplied_cfg.get("volume_path")
                or supplied_cfg.get("VolumePath")
            )
        )
    ):
        raise ValueError("CheckpointMode 3 requires VolumePath")
    max_threads = _workers(raw_threads)
    if raw_groups is None:
        requested_groups = set(_ALL_PARALLEL_GROUPS)
    elif isinstance(raw_groups, str):
        requested_groups = {
            item.strip() for item in raw_groups.split(",") if item.strip()
        }
    else:
        requested_groups = {str(item).strip() for item in raw_groups}
    if requested_groups == {"all"}:
        requested_groups = set(_ALL_PARALLEL_GROUPS)
    elif requested_groups == {"none"}:
        requested_groups = set()
    unknown_groups = requested_groups - _ALL_PARALLEL_GROUPS
    if unknown_groups:
        raise ValueError(
            f"Unknown ParallelGroups: {sorted(unknown_groups)}; "
            f"valid groups are {sorted(_ALL_PARALLEL_GROUPS)}"
        )
    events = []
    event_token = _ACTIVE_EVENTS.set(events)
    mode_token = _ACTIVE_CHECKPOINT_MODE.set(checkpoint_mode)
    profile_token = _ACTIVE_PROFILE_PLAN.set(profile_plan)
    threshold_token = _ACTIVE_PLAN_THRESHOLD.set(plan_threshold)
    experiment_token = _ACTIVE_EXPERIMENT.set(experiment)
    coordinator = _Coordinator(max_threads, requested_groups)
    coordinator_token = _ACTIVE_COORDINATOR.set(coordinator)
    cfg_token = _ACTIVE_RUN_CFG.set(None)
    started = time.time()
    succeeded = False
    run_name = getattr(fn, "__name__", "fep")
    _base.logger.info(
        "[fep] ExecutionProfile=%s CheckpointMode=%s shuffle=%s "
        "MaxThreads=%s",
        profile_name,
        checkpoint_mode,
        shuffle_partitions,
        max_threads,
    )
    _base.logger.info("[CHECKPOINT_V2] mode=%s", checkpoint_mode)
    _print_process("START", "run", run_name, "pipeline")
    try:
        result = fn(*args, **kwargs)
        succeeded = True
    finally:
        coordinator.close()
        cfg = _ACTIVE_RUN_CFG.get()
        if not succeeded and isinstance(cfg, dict):
            try:
                spark = args[0] if args else kwargs.get("spark")
                if spark is not None:
                    drop_failed_run_checkpoints(spark, cfg)
            except Exception as cleanup_error:
                print(
                    "[fep] failed-run checkpoint cleanup also failed: "
                    f"{cleanup_error}"
                )
        wall = round(time.time() - started, 3)
        pipeline_strategy = (
            cfg.get("_output_v3_pipeline_strategy")
            if isinstance(cfg, dict)
            else None
        )
        uses_parallel_pipeline = (
            pipeline_strategy == "parallel_modes_123_control_flow"
        )
        performance = _performance_summary(
            wall, cfg, events, coordinator.events
        )
        reports = {"builder": [], "checkpoint": [], "action": []}
        _LAST_RUN_PROFILE.clear()
        _LAST_RUN_PROFILE.update(
            {
                "updated_wall_seconds": wall,
                "checkpoint_mode": checkpoint_mode,
                "profile_plan": profile_plan,
                "plan_checkpoint_threshold": plan_threshold,
                "experiment_id": experiment["_output_v3_experiment_id"],
                "requested_shuffle_partitions": experiment[
                    "_output_v3_shuffle_partitions"
                ],
                "experiment_settings": {
                    key.removeprefix("_output_v3_"): value
                    for key, value in experiment.items()
                },
                "effective_spark_config": (
                    dict(cfg.get("_output_v3_effective_spark_config", {}))
                    if isinstance(cfg, dict)
                    else {}
                ),
                "effective_max_threads": max_threads,
                "enabled_parallel_groups": sorted(requested_groups),
                "execution_strategy": (
                    "bounded_parallel_staged_pipeline"
                    if max_threads > 1
                    else "sequential"
                ),
                "branch_strategy": (
                    "parallel_isolated_cfg"
                    if uses_parallel_pipeline and max_threads > 1
                    else (
                        "sequential_isolated_cfg"
                        if uses_parallel_pipeline
                        else "production_control_flow"
                    )
                ),
                "pass_a_strategy": (
                    "parallel_isolated_cfg"
                    if uses_parallel_pipeline and max_threads > 1
                    else (
                        "sequential_isolated_cfg"
                        if uses_parallel_pipeline
                        else "production_control_flow"
                    )
                ),
                "output_build_strategy": (
                    "parallel_isolated_cfg"
                    if uses_parallel_pipeline and max_threads > 1
                    else (
                        "sequential_isolated_cfg"
                        if uses_parallel_pipeline
                        else "production_control_flow"
                    )
                ),
                "pipeline_strategy": pipeline_strategy,
                "checkpoint_policy": f"checkpoint_v2_mode_{checkpoint_mode}",
                "checkpoint_activity": list(
                    cfg.get("_checkpoint_policy_activity", ())
                ) if isinstance(cfg, dict) else [],
                "parallel_activity": coordinator.events,
                "stage_timings": _stage_summary(events),
                "operation_timings": list(events),
                "stage_contracts": stage_contracts(),
                "artifact_merges": list(
                    cfg.get("_output_v3_artifact_merges", ())
                ) if isinstance(cfg, dict) else [],
                "plan_profile": reports["builder"],
                "checkpoint_plan_profile": reports["checkpoint"],
                "action_profile": reports["action"],
                "performance_summary": performance,
            }
        )
        _print_process(
            "DONE",
            "run",
            run_name,
            "pipeline",
            elapsed=wall,
            status="PASS" if succeeded else "FAIL",
        )
        _ACTIVE_RUN_CFG.reset(cfg_token)
        _ACTIVE_COORDINATOR.reset(coordinator_token)
        _ACTIVE_EVENTS.reset(event_token)
        _ACTIVE_PROFILE_PLAN.reset(profile_token)
        _ACTIVE_CHECKPOINT_MODE.reset(mode_token)
        _ACTIVE_PLAN_THRESHOLD.reset(threshold_token)
        _ACTIVE_EXPERIMENT.reset(experiment_token)
    print(
        f"[fep timing] wall={wall:.3f}s "
        f"checkpoint_mode={checkpoint_mode} profile_plan={profile_plan} "
        f"experiment={experiment['_output_v3_experiment_id']} "
        f"threads={max_threads} "
        f"groups={','.join(sorted(requested_groups)) or 'none'}"
    )
    effective_config = _LAST_RUN_PROFILE.get("effective_spark_config", {})
    print(
        "[fep config] "
        f"shuffle_partitions={effective_config.get('spark.sql.shuffle.partitions')} "
        f"aqe_enabled={effective_config.get('spark.sql.adaptive.enabled')} "
        "advisory_partition_bytes="
        f"{effective_config.get('spark.sql.adaptive.advisoryPartitionSizeInBytes')}"
    )
    for stage in _LAST_RUN_PROFILE.get("stage_timings", ()):
        print(
            f"[fep timing] stage={stage['stage']} "
            f"elapsed={stage['elapsed_seconds']:.3f}s "
            f"calls={stage['calls']}"
        )
    perf = _LAST_RUN_PROFILE.get("performance_summary", {})
    print(
        "[fep budget] "
        f"target={perf.get('target_wall_seconds', 50.0):.1f}s "
        f"over={perf.get('seconds_over_target', 0.0):.3f}s "
        f"checkpoint_actions={perf.get('checkpoint_action_seconds', 0.0):.3f}s "
        f"explicit_actions={perf.get('explicit_action_seconds', 0.0):.3f}s"
    )
    for row in perf.get("critical_actions", ())[:10]:
        print(
            "[fep critical] "
            f"kind={row['kind']} name={row['name']} stage={row['stage']} "
            f"elapsed={row['elapsed_seconds']:.3f}s "
            f"nodes={row.get('incoming_plan_nodes')} "
            f"depth={row.get('incoming_plan_depth')} "
            f"partitions={row.get('incoming_partitions')}"
        )
    for row in perf.get("parallel_wave_critical_path", ()):
        print(
            "[fep wave] "
            f"wave={row['wave']} elapsed={row['elapsed_seconds']:.3f}s "
            f"groups={','.join(row['groups'])} "
            f"tasks={','.join(row['tasks'])}"
        )
    return result


def run_modes(*args, **kwargs):
    return _run_profiled(_delegated_run_modes, *args, **kwargs)


def run_final_effective_percentages(*args, **kwargs):
    return _run_profiled(_PRODUCTION_RUN_FINAL, *args, **kwargs)


def get_last_run_profile() -> dict[str, Any]:
    return {
        **_LAST_RUN_PROFILE,
        **{
            key: [dict(item) for item in _LAST_RUN_PROFILE.get(key, ())]
            for key in (
                "checkpoint_activity",
                "parallel_activity",
                "stage_timings",
                "operation_timings",
                "stage_contracts",
                "artifact_merges",
                "plan_profile",
                "checkpoint_plan_profile",
                "action_profile",
            )
        },
        "enabled_parallel_groups": list(
            _LAST_RUN_PROFILE.get("enabled_parallel_groups", ())
        ),
        "performance_summary": {
            **_LAST_RUN_PROFILE.get("performance_summary", {}),
            "parallel_group_critical_seconds": dict(
                _LAST_RUN_PROFILE.get("performance_summary", {}).get(
                    "parallel_group_critical_seconds", {}
                )
            ),
            "parallel_wave_critical_path": [
                dict(item)
                for item in _LAST_RUN_PROFILE.get(
                    "performance_summary", {}
                ).get("parallel_wave_critical_path", ())
            ],
            "critical_actions": [
                dict(item)
                for item in _LAST_RUN_PROFILE.get(
                    "performance_summary", {}
                ).get("critical_actions", ())
            ],
        },
    }


run_mode = run_final_effective_percentages

__all__ = [
    "get_last_run_profile",
    "run_final_effective_percentages",
    "run_mode",
    "run_modes",
]


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
