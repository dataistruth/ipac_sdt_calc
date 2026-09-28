"""
orchestrator.py

Converted from: dbo.usp_SM_LoadLookThroughEffectiveAllocationPercentage.sql
Original procedure: dbo.usp_SM_LoadLookThroughEffectiveAllocationPercentage
Conversion date: 2026-05-04

Calculates and applies effective allocation percentages for state
look-through allocations. Handles flow-up partners, UBTI lines,
side-pocket logic, and exclude-from-residual recalculation.

Usage (standalone):
    from orchestrator import run_sm_load_lt_effective_alloc_pct
    run_sm_load_lt_effective_alloc_pct(
        spark, entity_id=123, client_id=456,
        tax_period_id=789, run_id=1001,
        catalog="dev7", schema="Qa7testschema",
    )

Usage (reuse shared config from a workflow):
    cfg = load_common_config(spark, run_id, entity_id, ...)
    run_sm_load_lt_effective_alloc_pct(spark, cfg=cfg)
"""

import json
import logging
import time

import pyspark.sql.functions as F

from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import read_table
from Common_V2.core.checkpoint import checkpoint, drop_checkpoints
from Common_V2.core.observability import log_section, log_timing


from services.config_service import load_sp_config, validate_allocation_type
from services.mapping_service import build_mapping_data, build_parent_k1_mappings
from services.flowup_service import (
    build_flowup_k1_amounts,
    compute_flowup_mapped_amounts,
    compute_flowup_effective_amounts,
    write_flowup_allocation_output,
)
from services.amount_service import (
    build_k1_amounts,
    build_ubti_amounts,
    compute_state_mapped_amounts,
)
from services.effective_pct_service import (
    compute_effective_percentages,
    apply_exclude_from_residual,
    apply_pe_book_unmapped_lines,
)
from services.writer_service import write_allocation_output, update_allocation_input

logger = logging.getLogger(__name__)


def run_sm_load_lt_effective_alloc_pct(
    spark,
    cfg=None,
    verbose=False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "Parquet",
    VolumePath: str = None,
    ExecutionID: str = None,
    **kwargs,
):
    """Main entry point for SM Load LookThrough Effective Allocation Percentage.

    Converted from: dbo.usp_SM_LoadLookThroughEffectiveAllocationPercentage

    Args:
        spark: SparkSession
        cfg: Pre-loaded config dict (Mode 1/2) or None (Mode 3)
        verbose: If True, log row counts at every section boundary
        **run_params: entity_id, client_id, tax_period_id, run_id, catalog, schema
    """
    # Map CamelCase params to snake_case for use in function body
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName
    result_type = ResultType
    volume_path = VolumePath
    execution_id = ExecutionID

    t0 = time.time()

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    status = {
        "sp_name": "usp_SM_LoadLookThroughEffectiveAllocationPercentage",
        "run_id": None,
        "entity_id": None,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    if cfg is None:
        cfg = load_common_config(
            spark,
            run_id=run_id,
            entity_id=entity_id,
            client_id=client_id,
            tax_period_id=tax_period_id,
            catalog=catalog,
            schema=schema,
        )

    # Copy checkpoint list for thread safety
    cfg = {**cfg, "_checkpoint_tables": []}

    # Pass through output options
    if result_type is not None:
        cfg.setdefault("result_type", result_type)
    if volume_path is not None:
        cfg["volume_path"] = volume_path
    if execution_id is not None:
        cfg["execution_id"] = execution_id

    print(f"[DEBUG] ResultType={cfg.get('result_type')}, VolumePath={cfg.get('volume_path')}, ExecutionID={cfg.get('execution_id')}")
    status["run_id"] = cfg.get("run_id")
    status["entity_id"] = cfg.get("entity_id")

    # S2: Load SP-specific config
    cfg = load_sp_config(spark, cfg)

    # S3: Validation gate
    if not validate_allocation_type(spark, cfg):
        status["status"] = "FAIL"
        status["error"] = "Allocation logic not selected for the entity."
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        return status

    try:
        # S4: Build mapping data
        mappings = build_mapping_data(spark, cfg)

        # S5-S6: Flow-up partner pipeline (conditional)
        # IF (@IsSidePocketFlowUpPartner = 1) OR EXISTS(SM_FlowUpPartnerLookThroughAllocationInput)
        is_flowup = cfg.get("is_sidepocket_flowup_partner", False)
        sm_fp_lt = read_table(spark, "SM_FlowUpPartnerLookThroughAllocationInput", cfg)
        has_flowup_input = len(
            sm_fp_lt.filter(F.col("RunID") == cfg["run_id"]).head(1)
        ) > 0

        fp_data = None
        if is_flowup or has_flowup_input:
            fp_data = build_flowup_k1_amounts(spark, cfg)
            fp_total_amounts = compute_flowup_mapped_amounts(spark, cfg, fp_data, mappings)
            fp_effective = compute_flowup_effective_amounts(
                spark, cfg, fp_total_amounts, mappings["state_mapped_lines"],
            )
            write_flowup_allocation_output(spark, cfg, fp_effective)
        else:
            # Still need sidepocket data for later sections
            fp_data = build_flowup_k1_amounts(spark, cfg)
            logger.info("[SKIP] Flow-up partner pipeline: condition not met")

        # S7: Pre-compute shared datasets
        from services.amount_service import (
            _build_sm_lt_input_and_state_lines,
        )
        (
            sm_lt_input, state_lines, fed_lines,
            non_sp_fp, pruned_dm, pruned_ubti_dm,
        ) = _build_sm_lt_input_and_state_lines(spark, cfg, mappings)

        # S7-S8: K1 & UBTI amount aggregation
        partner_alloc, total_input = build_k1_amounts(
            spark, cfg, mappings,
            fp_data["k1_sidepocket"], fp_data["k1_sidepocket_res"],
            fed_lines, non_sp_fp,
        )
        partner_alloc_ubti, total_ubti_input = build_ubti_amounts(
            spark, cfg, fed_lines, non_sp_fp,
        )

        # S9: State-mapped amount calculation
        total_amounts = compute_state_mapped_amounts(
            spark, cfg, pruned_dm, pruned_ubti_dm,
            partner_alloc, total_input,
            partner_alloc_ubti, total_ubti_input,
        )

        # S10: Effective percentage calculation + CHECKPOINT
        effective_amounts, temp_effective = compute_effective_percentages(
            spark, cfg, total_amounts, sm_lt_input,
        )

        # S11: Exclude-from-residual recalculation (conditional)
        effective_amounts = apply_exclude_from_residual(
            spark, cfg, effective_amounts, total_amounts,
        )

        # S12: PE Book unmapped lines (conditional)
        effective_amounts = apply_pe_book_unmapped_lines(
            spark, cfg, effective_amounts, temp_effective,
            sm_lt_input, mappings,
        )

        # S13: Final write & cleanup
        partner_snap = read_table(spark, "Partner_Snapshot", cfg)
        partner_snapshot = partner_snap.filter(
            F.coalesce(F.col("WorkFlowID"), F.col("Transactionid"))
            == cfg["partner_txn_or_wf_id"]
        )

        rows = write_allocation_output(spark, cfg, effective_amounts, partner_snapshot)
        update_allocation_input(spark, cfg, effective_amounts)
        status["sections_completed"] = 13

        # Note: uspUpdateAllocationLog(@LogID, @EndDate) is handled
        # by the workflow framework after this SP returns.

    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error(f"[FAIL] {e}", exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        drop_checkpoints(spark, cfg)

    logger.info(
        f"[DONE] run_sm_load_lt_effective_alloc_pct | "
        f"{status['elapsed_seconds']}s | "
        f"RunID={cfg['run_id']} EntityID={cfg['entity_id']}"
    )

    # Return JSON string for Orchestrator parquet file tracking
    if rows and isinstance(rows, str):
        print(f"[PARQUET] Return JSON: {rows}")
        return rows

    return status


# ════════════════════════════════════════════════════════════════
# __main__: standalone execution (Mode 3).
# Read widget params → call run_sm_load_lt_effective_alloc_pct(...).
# The function's `if cfg is None` branch is the single point that calls
# load_common_config. Job/Orchestrator modes pass cfg in directly and skip
# this block.
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    try:
        result = run_sm_load_lt_effective_alloc_pct(
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
        dbutils.notebook.exit(json.dumps(result))  # noqa: F821
    except Exception:
        print(json.dumps(result, indent=2))
