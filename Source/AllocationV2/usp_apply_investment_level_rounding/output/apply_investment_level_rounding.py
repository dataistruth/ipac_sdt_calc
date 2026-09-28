"""
apply_investment_level_rounding — Main orchestrator.

Converted from: uspApplyInvestmentLevelRounding.sql
Original procedure: dbo.uspApplyInvestmentLevelRounding

Usage:
    from output.apply_investment_level_rounding import apply_investment_level_rounding

    apply_investment_level_rounding(
        spark,
        EntityID=100, ClientID=9999, TaxPeriodID=1, RunID=500,
        CatalogName="my_catalog", SchemaName="my_schema",
        ResultType="Parquet",
        VolumePath="/Volumes/my_catalog/my_schema/parquet-output",
        ExecutionID="abc-123",
    )
"""

from pyspark.sql import SparkSession
import logging
import time
import sys
import os

# Ensure Common_V2 parent (Source/) is on sys.path for imports
_source_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _source_dir not in sys.path:
    sys.path.insert(0, _source_dir)

try:
    from Common_V2.core.writers import collect_parquet_result
except ImportError:
    from core.writers import collect_parquet_result

from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import get_logger
from Common_V2.core.checkpoint import checkpoint, drop_checkpoints

logger = get_logger("apply_investment_level_rounding")

try:
    from .config_service import load_sp_config
    from .lookthrough_service import (
        build_lookthrough_output_inv_level_callfrom,
        build_lookthrough_output_inv_level_no_callfrom,
        build_lookthrough_output_entity_callfrom,
        build_lookthrough_output_entity_no_callfrom,
    )
    from .input_service import build_allocation_input, build_ubti_passive_input
    from .aggregation_service import (
        build_temp_allocation_output,
        compute_rounding_diff,
        compute_max_allocation_type,
    )
    from .partner_service import (
        build_partner_snapshots,
        build_highest_percent_partner,
        build_nocost_partner,
    )
    from .rounding_service import (
        apply_rounding_override,
        apply_rounding_plugged_to_gp,
        apply_rounding_highest_percent,
        apply_rounding_highest_amount,
        apply_rounding_none,
    )
    from .write_service import (
        apply_book_k1_not_rounded_passthrough,
        write_final_summaries,
        write_allocation_summaries,
        update_is_rounded_flag,
    )
except ImportError:
    from config_service import load_sp_config
    from lookthrough_service import (
        build_lookthrough_output_inv_level_callfrom,
        build_lookthrough_output_inv_level_no_callfrom,
        build_lookthrough_output_entity_callfrom,
        build_lookthrough_output_entity_no_callfrom,
    )
    from input_service import build_allocation_input, build_ubti_passive_input
    from aggregation_service import (
        build_temp_allocation_output,
        compute_rounding_diff,
        compute_max_allocation_type,
    )
    from partner_service import (
        build_partner_snapshots,
        build_highest_percent_partner,
        build_nocost_partner,
    )
    from rounding_service import (
        apply_rounding_override,
        apply_rounding_plugged_to_gp,
        apply_rounding_highest_percent,
        apply_rounding_highest_amount,
        apply_rounding_none,
    )
    from write_service import (
        apply_book_k1_not_rounded_passthrough,
        write_final_summaries,
        write_allocation_summaries,
        update_is_rounded_flag,
    )


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
    **kwargs,
):
    """
    Apply investment-level rounding to allocation output data.

    Determines the rounding logic (Rounding Override, Plugged to GP,
    Plugged to Highest Percent, Plugged to Highest Amount, or None)
    and applies rounding differences accordingly, writing results to
    K1/UBTI/Passive/BoxJKL allocation summary tables.
    """
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

    if verbose:
        logger.setLevel(logging.DEBUG)

    status = {
        "sp_name": "uspApplyInvestmentLevelRounding",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    try:
        # --- Section 1: Config ---
        # Mode 3 (Standalone): load common config when no cfg was supplied.
        # Mode 1/2 (Job/Orchestrator): caller passes cfg with all scalars pre-resolved.
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

        # Copy checkpoint list so parallel SPs don't interfere; pass through output options.
        cfg = {**cfg, "_checkpoint_tables": []}
        cfg.setdefault("result_type", result_type)
        if volume_path is not None:
            cfg["volume_path"] = volume_path
        if execution_id is not None:
            cfg["execution_id"] = execution_id

        # SP-specific aliases + BookK1 derived values.
        cfg = load_sp_config(spark, cfg)

        if cfg.get("run_status") == "FAIL":
            logger.error(f"RunStatus=FAIL — aborting. RunID={cfg['run_id']}")
            status["status"] = "SKIPPED"
            status["error"] = "RunStatus=FAIL"
            return status

        # --- Section 2-5: Load LookThrough data ---
        is_inv_level = cfg.get("is_investment_level_rounding") == "C"
        has_call_from = (cfg.get("call_from") or "") != ""

        if is_inv_level and has_call_from:
            lookthrough_output_df, lookthrough_input_df = build_lookthrough_output_inv_level_callfrom(spark, cfg)
        elif is_inv_level:
            lookthrough_output_df, lookthrough_input_df = build_lookthrough_output_inv_level_no_callfrom(spark, cfg)
        elif has_call_from:
            lookthrough_output_df, lookthrough_input_df = build_lookthrough_output_entity_callfrom(spark, cfg)
        else:
            lookthrough_output_df, lookthrough_input_df = build_lookthrough_output_entity_no_callfrom(spark, cfg)

        status["sections_completed"] = 6
        not_rounded_lines_df = cfg.get("not_rounded_lines_df")

        # --- Section 7: Build AllocationInput ---
        allocation_input_df = build_allocation_input(spark, cfg, lookthrough_input_df, not_rounded_lines_df)
        status["sections_completed"] = 7

        # --- Section 8: UBTI & Passive input ---
        allocation_input_df = build_ubti_passive_input(spark, cfg, allocation_input_df)
        status["sections_completed"] = 8

        # --- Section 9: TempAllocationOutput ---
        temp_alloc_output_df = build_temp_allocation_output(spark, cfg, lookthrough_output_df, not_rounded_lines_df)
        status["sections_completed"] = 9

        # --- Section 10: Rounding Diff ---
        rounded_diff_df = compute_rounding_diff(spark, cfg, temp_alloc_output_df, allocation_input_df)
        status["sections_completed"] = 10

        # --- Section 11: Max Allocation Type ---
        max_alloc_type_df = compute_max_allocation_type(spark, cfg, lookthrough_output_df)
        status["sections_completed"] = 11

        # --- Section 12: Partner Snapshots ---
        partner_snapshot_df, rounding_override_df = build_partner_snapshots(spark, cfg)
        status["sections_completed"] = 12

        # --- CHECKPOINT ---
        temp_alloc_output_df = checkpoint(spark, temp_alloc_output_df, "temp_alloc_output", cfg)
        rounded_diff_df = checkpoint(spark, rounded_diff_df, "rounded_diff", cfg)

        # --- Section 13-19: Rounding Logic (branched) ---
        rounding_logic = cfg.get("rounding_logic")
        rounding_override_import = cfg.get("rounding_override_import", False)
        has_rounding_override = cfg.get("has_rounding_override", False)

        k1_summary_df = None
        adjustment_summary_df = None

        alloc_output_detail_df = temp_alloc_output_df.select(
            "EntityID", "LineTypeID", "LineID", "ParentEntityID",
            "TrackingKey", "SuperParentEntityID", "AdjustmentTypeID",
            "Tag", "OriginalParentEntityID", "QuickLinkID"
        ).distinct()

        if has_rounding_override and rounding_override_import:
            k1_summary_df, adjustment_summary_df = apply_rounding_override(
                spark, cfg, temp_alloc_output_df, rounded_diff_df,
                max_alloc_type_df, rounding_override_df, alloc_output_detail_df)
        elif rounding_logic == "Plugged to GP":
            k1_summary_df, adjustment_summary_df = apply_rounding_plugged_to_gp(
                spark, cfg, temp_alloc_output_df, rounded_diff_df,
                max_alloc_type_df, partner_snapshot_df)
        elif rounding_logic == "Plugged to Highest Allocation Percent":
            highest_pct_partner_df = build_highest_percent_partner(spark, cfg, lookthrough_output_df)
            nocost_df, rounding_partner_number, rounding_share_class = build_nocost_partner(
                spark, cfg, lookthrough_output_df, highest_pct_partner_df, partner_snapshot_df)
            cfg["rounding_partner_number"] = rounding_partner_number
            cfg["rounding_share_class"] = rounding_share_class
            k1_summary_df, adjustment_summary_df = apply_rounding_highest_percent(
                spark, cfg, temp_alloc_output_df, rounded_diff_df,
                max_alloc_type_df, highest_pct_partner_df, nocost_df, partner_snapshot_df)
        elif rounding_logic == "Plugged to Highest Allocation Amount":
            k1_summary_df, adjustment_summary_df = apply_rounding_highest_amount(
                spark, cfg, temp_alloc_output_df, rounded_diff_df,
                max_alloc_type_df, partner_snapshot_df)
        elif rounding_logic == "None":
            k1_summary_df, adjustment_summary_df = apply_rounding_none(
                spark, cfg, temp_alloc_output_df, max_alloc_type_df)

        status["sections_completed"] = 19

        # --- Section 20: BookK1 Not-Rounded passthrough ---
        book_k1_passthrough_df = None
        if cfg.get("book_k1_adjustment_enabled", False):
            book_k1_passthrough_df = apply_book_k1_not_rounded_passthrough(
                spark, cfg, lookthrough_output_df, not_rounded_lines_df)
            if k1_summary_df is not None and book_k1_passthrough_df is not None:
                k1_summary_df = k1_summary_df.unionByName(book_k1_passthrough_df, allowMissingColumns=True)
        status["sections_completed"] = 20

        # --- Section 21: Write final summaries ---
        k1_write_df, ubti_write_df, adj_write_df, return_value_summaries = write_final_summaries(spark, cfg, k1_summary_df, adjustment_summary_df)
        status["sections_completed"] = 21

        # --- Section 22: Update IsRounded flag ---
        update_is_rounded_flag(spark, cfg)
        status["sections_completed"] = 22

        # --- Section 23: Write AllocationSummary tables (Delta + Parquet) ---
        return_value_alloc = write_allocation_summaries(spark, cfg, k1_write_df, ubti_write_df, adj_write_df)
        status["sections_completed"] = 23

    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.exception(f"Failed: {e}")
        raise
    finally:
        drop_checkpoints(spark, cfg)
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        logger.info(f"[COMPLETE] {status['sp_name']} — {status['status']} "
                    f"in {status['elapsed_seconds']}s, "
                    f"sections={status['sections_completed']}/23")

    # Merge both save_results JSON outputs into one combined result for ParquetToSQL
    import json
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


# ════════════════════════════════════════════════════════════════
# __main__: standalone execution (Mode 3).
# Read widget params → call apply_investment_level_rounding(...).
# The function's `if cfg is None` branch is the single point that calls
# load_common_config. Job/Orchestrator modes pass cfg in directly and skip
# this block.
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import json
    spark = SparkSession.builder.getOrCreate()

    try:
        result = apply_investment_level_rounding(
            spark,
            RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
            EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
            ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
            TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
            CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
            SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
        )
    except Exception as exc:
        print(f"Usage: provide run_id, entity_id, etc. as widget parameters ({exc})")
        import sys
        sys.exit(1)

    try:
        dbutils.notebook.exit(json.dumps(result) if not isinstance(result, str) else result)  # noqa: F821
    except Exception:
        print(json.dumps(result, indent=2) if not isinstance(result, str) else result)
