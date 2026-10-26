"""
orchestrator.py

Converted from: dbo.uspLoadFootnotesAllocationToOutput.sql
Original procedure: dbo.uspLoadFootnotesAllocationToOutput
Conversion date: 2026-05-05

Allocates footnote amounts to partners using effective percentages.
Handles PE Book 704c allocation, PFIC/Form926/Form8865/Form8886/Form199A/
At Risk/Custom footnote line types. Writes final allocations to AllocationOutput
and deducts from AllocationInput.

Usage (standalone):
    from AllocationV2.usp_load_footnotes_allocation_to_output.output.orchestrator import (
        run_load_footnotes_allocation_to_output,
    )
    run_load_footnotes_allocation_to_output(
        spark, entity_id=123, client_id=456, tax_period_id=789,
        run_id=1001, rank_for_rule_pickup=1,
        catalog="dev7", schema="testschema",
    )

Usage (reuse shared config from a workflow):
    cfg = load_common_config(spark, run_id, entity_id, client_id, tax_period_id, catalog, schema)
    cfg["rank_for_rule_pickup"] = 1
    run_load_footnotes_allocation_to_output(spark, cfg=cfg)
"""

import json
import logging
import time

import pyspark.sql.functions as F

from Common_V2.core.config import load_common_config
from Common_V2.core.checkpoint import checkpoint, drop_checkpoints
from Common_V2.core.observability import log_section, log_timing

from .config import load_sp_config, validate_run_preconditions
from .allocation_input import (
    build_temp_book_effective,
    build_temp_allocation_input,
    build_zero_exclude_lines,
    build_temp_final_effective_pct,
    build_allocation_input,
)
from .quarter_logic import (
    update_pfic_partv_quarters,
    update_pfic_quarters_by_config,
    update_form_quarters,
)
from .underlyings import (
    build_cost_percentage_data,
    build_entity_hierarchy,
    filter_asset_class,
    build_underlyings_footnotes_ordered,
)
from .allocation_704c import (
    build_704c_config,
    build_custom_footnote_line_types,
    build_allocation_percentage_temp,
    build_704c_allocation_output,
    apply_704c_deduction,
)
from .allocation_effective import (
    resolve_min_quarter,
    build_effective_pct_allocation,
)
from .writers import (
    write_allocation_output,
    apply_deduction,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_load_footnotes_allocation_to_output(
    spark,
    cfg: dict = None,
    verbose: bool = False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    RankForRulePickup: int = None,
    **kwargs,
):
    """Allocate footnote amounts to partners using effective percentages.

    Handles PE Book 704c allocation, PFIC/Form/Custom line types.
    Writes to AllocationOutput and deducts from AllocationInput.

    Args:
        RankForRulePickup: Required SP parameter (1 or 2).
        verbose: If True, log row counts at every section boundary.
    """
    # Map CamelCase params to snake_case for use in function body
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName
    rank_for_rule_pickup = RankForRulePickup

    t0 = time.time()

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    status = {
        "sp_name": "uspLoadFootnotesAllocationToOutput",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    # --- Config resolution (3 modes) ---
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

    # Copy checkpoint list for thread-safety
    cfg = {**cfg, "_checkpoint_tables": []}

    # SP-specific parameter
    if rank_for_rule_pickup is not None:
        cfg["rank_for_rule_pickup"] = rank_for_rule_pickup
    assert cfg.get("rank_for_rule_pickup") is not None, \
        "rank_for_rule_pickup must be provided"

    status["run_id"] = cfg.get("run_id")
    status["entity_id"] = cfg.get("entity_id")

    # --- Pre-read shared lookup tables (read once, reused 7× and 5× across modules) ---
    from Common_V2.core.helpers import read_table as _rt_pre
    cfg["_df_pfic_footnote_line_item"] = _rt_pre(spark, "PFICFootnoteLineItem", cfg)
    cfg["_df_entity"] = _rt_pre(spark, "Entity", cfg)

    # --- Load SP-specific config (needed before precondition check) ---
    load_sp_config(spark, cfg)

    # --- Validate preconditions ---
    if not validate_run_preconditions(spark, cfg):
        logger.info(
            f"[SKIP] RunStatus=FAIL or wrong allocation type. "
            f"RunID={cfg['run_id']}, EntityID={cfg['entity_id']}"
        )
        status["status"] = "SKIPPED"
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        return status

    try:
        # S3: Initial data load
        df_temp_book_eff = build_temp_book_effective(spark, cfg)
        df_temp_alloc_input = build_temp_allocation_input(spark, cfg)
        df_zero_exclude = build_zero_exclude_lines(spark, cfg)
        df_temp_final_eff_pct = build_temp_final_effective_pct(spark, cfg)
        status["sections_completed"] = 3

        # S4: Quarter updates
        df_temp_alloc_input, df_part_v_allocable = update_pfic_partv_quarters(
            spark, cfg, df_temp_alloc_input, df_temp_final_eff_pct,
        )
        df_temp_alloc_input = update_pfic_quarters_by_config(
            spark, cfg, df_temp_alloc_input,
            df_part_v_allocable, df_temp_final_eff_pct,
        )
        df_temp_alloc_input = update_form_quarters(
            spark, cfg, df_temp_alloc_input,
        )
        # CHECKPOINT: break lineage from AllocationInput read + 3 quarter-update
        # transforms. Without this, the 5-pass INSERT/DELETE in S9 multiplies
        # plan depth exponentially (each left_anti re-evaluates the full chain).
        df_temp_alloc_input = checkpoint(spark, df_temp_alloc_input, "temp_alloc_input", cfg)
        status["sections_completed"] = 4

        # S5: Cost percentage + DAR
        df_cost_pct_snapshot, df_temp_cost_underlying_types = \
            build_cost_percentage_data(spark, cfg)
        status["sections_completed"] = 5

        # S6: Entity hierarchy
        df_all_underlyings, df_asset_class_rel = build_entity_hierarchy(
            spark, cfg, df_cost_pct_snapshot, df_temp_cost_underlying_types,
        )
        status["sections_completed"] = 6

        # S7: Asset class filtering
        df_all_underlyings = filter_asset_class(
            spark, cfg, df_all_underlyings, df_asset_class_rel,
        )
        # CHECKPOINT: break lineage from 2 recursive hierarchies (S5+S6) before
        # the expensive 5-way non-equi join in S8. Without this, the single
        # checkpoint after S8 materializes ~9 levels of iterative expansion.
        df_all_underlyings = checkpoint(spark, df_all_underlyings, "all_underlyings", cfg)
        status["sections_completed"] = 7

        # S8: Underlyings footnotes ordered
        df_underlyings_fn = build_underlyings_footnotes_ordered(
            spark, cfg, df_all_underlyings, df_temp_alloc_input,
        )
        # CHECKPOINT: break lineage from S8 5-way join + ROW_NUMBER
        df_underlyings_fn = checkpoint(spark, df_underlyings_fn, "underlyings_fn", cfg)
        status["sections_completed"] = 8

        # S9: Build #AllocationInput (multi-pass)
        df_alloc_input = build_allocation_input(
            spark, cfg, df_temp_alloc_input, df_temp_book_eff, df_underlyings_fn,
        )
        # CHECKPOINT: break lineage for forward consumers (S10-S13)
        df_alloc_input = checkpoint(spark, df_alloc_input, "alloc_input", cfg)
        status["sections_completed"] = 9

        # Early exit if no allocation input rows
        if df_alloc_input.isEmpty():
            logger.info("[SKIP] #AllocationInput is empty — nothing to allocate")
            status["status"] = "SKIPPED"
            status["elapsed_seconds"] = round(time.time() - t0, 1)
            return status

        # S10: 704c allocation (conditional)
        df_tmp_alloc_output_704c = None
        df_custom_fn_types = build_custom_footnote_line_types(spark, cfg)

        build_704c_config(spark, cfg)  # sets cfg["is_704c_enabled"]

        if cfg.get("is_704c_enabled"):
            df_alloc_pct = build_allocation_percentage_temp(spark, cfg)
            result_704c = build_704c_allocation_output(
                spark, cfg, df_alloc_input, df_alloc_pct, df_custom_fn_types,
            )
            if result_704c is not None:
                df_tmp_alloc_output_704c, df_alloc_input = result_704c
        status["sections_completed"] = 10

        # S11: 704c deduction (if applicable)
        df_alloc_input, df_fn_allocated_lines = apply_704c_deduction(
            spark, cfg, df_alloc_input, df_tmp_alloc_output_704c, df_zero_exclude,
        )
        status["sections_completed"] = 11

        # S12: Main effective % allocation
        # BUG-22 FIX: SQL UDF Udf_pe_getpartnerslistforallocations only returns partners
        # from the LATEST workflow/transaction per entity, not all snapshots.
        # Logic: if PS.WorkFlowID != 0 → match against max(WorkFlowID) for entity;
        #        else → match PS.TransactionID against max(TransactionID) for entity.
        from Common_V2.core.helpers import read_table as _rt
        from pyspark.sql import Window as W

        _ps = (
            _rt(spark, "Partner_Snapshot", cfg)
            .filter(
                (F.col("ClientID") == cfg["client_id"])
                & (F.col("TaxPeriodID") == cfg["tax_period_id"])
                & (F.col("EntityID") == cfg["entity_id"])
            )
        )
        _w = W.partitionBy("EntityID")
        _ps_latest = (
            _ps
            .withColumn("_wf", F.coalesce(F.col("WorkFlowID"), F.lit(0)))
            .withColumn("_tx", F.coalesce(F.col("TransactionID"), F.lit(0)))
            .withColumn("_max_wf", F.max("_wf").over(_w))
            .withColumn("_max_tx", F.max("_tx").over(_w))
            .filter(
                F.when(F.col("_max_wf") != 0, F.col("_wf") == F.col("_max_wf"))
                .otherwise(F.col("_tx") == F.col("_max_tx"))
            )
        )
        df_entity_partners = F.broadcast(
            _ps_latest
            .select(
                F.col("PartnerNumber").alias("partnernumber"),
                F.col("ShareClass"),
            )
            .distinct()
        )

        resolve_min_quarter(spark, cfg)
        df_tmp_alloc_output_eff = build_effective_pct_allocation(
            spark, cfg, df_alloc_input, df_temp_final_eff_pct,
            df_entity_partners, df_custom_fn_types,
        )
        status["sections_completed"] = 12

        # S13: Final write & deduction
        # Combine 704c + effective % outputs
        if df_tmp_alloc_output_704c is not None and df_tmp_alloc_output_eff is not None:
            # Normalize columns before union
            shared_cols = [
                "RunID", "ClientID", "EntityID", "ShareClass", "PartnerNumber",
                "LineTypeID", "QuicklinkID", "LineID", "Amount", "AllocationType",
                "ParentEntityID", "SuperParentEntityID", "AllocationTypeID",
                "TrackingKey", "OriginalParentEntityID", "SchID",
            ]
            df_704c_norm = df_tmp_alloc_output_704c
            if "SchID" not in df_704c_norm.columns:
                df_704c_norm = df_704c_norm.withColumn("SchID", F.lit(None).cast("int"))
            df_combined = df_704c_norm.select(*shared_cols).unionByName(
                df_tmp_alloc_output_eff.select(*shared_cols)
            )
        elif df_tmp_alloc_output_eff is not None:
            df_combined = df_tmp_alloc_output_eff
        elif df_tmp_alloc_output_704c is not None:
            df_combined = df_tmp_alloc_output_704c
            if "SchID" not in df_combined.columns:
                df_combined = df_combined.withColumn("SchID", F.lit(None).cast("int"))
        else:
            df_combined = None

        if df_combined is not None:
            write_allocation_output(spark, cfg, df_combined)
            apply_deduction(
                spark, cfg, df_combined, df_alloc_input,
                df_fn_allocated_lines, df_zero_exclude,
            )
        status["sections_completed"] = 13

    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error(f"[FAIL] {e}", exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        drop_checkpoints(spark, cfg)

    logger.info(
        f"[DONE] run_load_footnotes_allocation_to_output | "
        f"{status['elapsed_seconds']}s | "
        f"RunID={cfg['run_id']} EntityID={cfg['entity_id']}"
    )
    return status


# ════════════════════════════════════════════════════════════════════════════
# __main__: Databricks Job runs this file directly as spark_python_task (Mode 1)
#           or standalone execution (Mode 3)
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Mode 3 standalone: read widget IDs, let run_*() call load_common_config
    # via the `if cfg is None:` branch.
    status = run_load_footnotes_allocation_to_output(
        spark,
        RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
        EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
        ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
        TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
        CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
        SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
        RankForRulePickup=int(dbutils.widgets.get("rank_for_rule_pickup")),  # noqa: F821
    )

    try:
        dbutils.notebook.exit(json.dumps(status))  # noqa: F821
    except Exception:
        print(json.dumps(status, indent=2))
