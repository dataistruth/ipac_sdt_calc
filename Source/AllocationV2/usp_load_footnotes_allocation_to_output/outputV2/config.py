"""
SP-local config aliasing for uspLoadFootnotesAllocationToOutput.

All scalar lookups (ENU_LineType / ENU_CustomAllocations / ENU_AllocationLogic /
ENU_EntityType / ENU_Event / GlobalMenu flags, AllocationRun + DAR transaction
IDs, Entity AllocationTypeName, PFIC/Form quarter line IDs) are pre-resolved by
Common_V2.core.config.load_common_config. This module aliases those scalars
into the SP's legacy key names so downstream module files don't need to be
touched.
"""

import logging
import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from Common_V2.core.observability import log_section, log_timing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# load_sp_config
# SQL lines: 211–430, 541–565, 840–851
# ---------------------------------------------------------------------------
def load_sp_config(spark: SparkSession, cfg: dict) -> dict:
    """Alias Common_V2 cfg scalars into SP-local legacy keys."""
    log_section("load_sp_config")
    t0 = time.time()

    # ── ENU_AllocationLogic / Entity AllocationType ──
    # pe_book_allocation_type_id already in cfg under same name.
    cfg["entity_allocation_type_id"] = cfg.get("entity_allocation_type_id")
    cfg["allocation_type_name"] = cfg.get("entity_allocation_type_name") or ""

    # ── GlobalMenu flag derivations ──
    cfg["part_v_allocated"] = (cfg.get("flag_part_v_by_distribution_date") == "C")
    cfg["is_dated_transfers_configured"] = cfg.get("flag_transfer_by_date")
    cfg["ignore_assetclass_for_partnership_level"] = (
        cfg.get("flag_ignore_asset_class_partnership_level")
    )
    cfg["override_indirect_lookthrough_asset_class"] = (
        cfg.get("flag_override_indirect_lookthrough_asset_class")
    )
    cfg["is_pfic_allocation_by_quarter"] = cfg.get("flag_pfic_allocation_by_quarter")

    # ── ENU_LineType IDs ──
    cfg["adjustment_line_type_id"] = cfg.get("book_k1_adjustments_line_type_id")
    # k1, at_risk, pfic_footnote, form926, form199a, form8886, form8865
    # already in cfg under same names.

    # ── ENU_CustomAllocations IDs ──
    cfg["cost_allocation_type_id"] = cfg.get("custom_allocation_id_cost")
    cfg["book_allocation_type_id"] = cfg.get("custom_allocation_id_book")
    cfg["offset_allocation_type_id"] = cfg.get("custom_allocation_id_offset")
    cfg["gp_offset_allocation_type_id"] = cfg.get("custom_allocation_id_gp_offset")
    cfg["lp_offset_allocation_type_id"] = cfg.get("custom_allocation_id_lp_offset")

    # ── ENU_EntityType: Investment ──
    cfg["inv_entity_type_id"] = cfg.get("entity_type_id_investment")

    # ── Quarter Line IDs from Form*LineItem and PFICFootNoteLineItem ──
    # All five (pfic_quarter, pfic_distribution_date, form199a_quarter,
    # form8886_quarter, form926_quarter, form8865_quarter) already in cfg.

    # ── DAR Transaction IDs (from AllocationRun pre-stamped values) ──
    cfg["dar_event_type_id"] = cfg.get("event_type_id_import_default_allocation_rule")
    cfg["default_allocation_rule_transaction_id"] = cfg.get("dar_entity_transaction_id")
    cfg["global_default_allocation_rule_transaction_id"] = cfg.get("dar_global_transaction_id")

    log_timing("load_sp_config", t0)
    return cfg


# ---------------------------------------------------------------------------
# validate_run_preconditions
# SQL lines: 249–260
# ---------------------------------------------------------------------------
def validate_run_preconditions(spark: SparkSession, cfg: dict) -> bool:
    """Check RunStatus != FAIL and entity AllocationTypeID matches PE Book.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 249-260.
    Returns False if SP should exit early (RunStatus=FAIL or wrong allocation type).
    """
    log_section("validate_run_preconditions")

    # IF @RunStatus = 'FAIL'
    if (cfg.get("run_status") or "").strip().upper() == "FAIL":
        logger.info(f"[SKIP] RunStatus=FAIL | RunID={cfg['run_id']}")
        return False

    # Entity AllocationTypeID matches PE Book Allocation
    entity_alloc_type_id = cfg.get("entity_allocation_type_id")
    pe_book_id = cfg.get("pe_book_allocation_type_id")
    if entity_alloc_type_id is None:
        logger.warning(f"[SKIP] Entity not found | EntityID={cfg['entity_id']}")
        return False
    if entity_alloc_type_id != pe_book_id:
        logger.info(
            f"[SKIP] Entity AllocationTypeID={entity_alloc_type_id} "
            f"!= PE Book ({pe_book_id}) | EntityID={cfg['entity_id']}"
        )
        return False

    return True
