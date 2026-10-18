"""
config_loader.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
SP-local config aliasing for Final Effective Percentage.

All scalar lookups (LineTypes, CustomAllocations, UnderlyingTypes,
EntityType=Investment, GlobalMenu flags, AllocationRun workflow/transaction
IDs, Entity AllocationTypeName, 704c AllocationTypeName) are pre-resolved by
Common_V2.core.config.load_common_config. This module aliases those scalars
into the SP's legacy key names so downstream service files don't need to be
touched.
"""

import logging
import time

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _log_timing(name, start):
    elapsed = time.time() - start
    logger.info(f"[TIMING] {name}: {elapsed:.1f}s")


def load_config(spark: SparkSession, cfg: dict) -> dict:
    """Alias Common_V2 cfg scalars into SP-local legacy keys.

    Replaces the original 8-section scalar-collection routine (470+ lines of
    ENU_LineType / ENU_CustomAllocations / ENU_UnderlyingType / ENU_EntityType /
    GlobalMenu / AllocationRun / ENU_AllocationLogic / ENU_704cAllocationLogic /
    ENU_Event collects). All values now come from load_common_config.
    """
    t0 = time.time()
    logger.info("[SECTION] load_config")

    # ── ENU_LineType IDs ──
    cfg["adjustment_line_type_id"] = cfg.get("book_k1_adjustments_line_type_id")
    cfg["box_jkl_line_type_id"] = cfg.get("boxjkl_line_type_id")
    # k1_line_type_id, state_input_line_type_id, at_risk_line_type_id,
    # pfic_footnote_line_type_id are already in cfg under the same names.

    # ── ENU_CustomAllocations IDs ──
    cfg["cost_allocation_type_id"] = cfg.get("custom_allocation_id_cost")
    cfg["book_allocation_type_id"] = cfg.get("custom_allocation_id_book")
    cfg["offset_allocation_type_id"] = cfg.get("custom_allocation_id_offset")
    cfg["gp_offset_allocation_type_id"] = cfg.get("custom_allocation_id_gp_offset")
    cfg["lp_offset_allocation_type_id"] = cfg.get("custom_allocation_id_lp_offset")
    cfg["yearly_allocation_type_id"] = cfg.get("custom_allocation_id_yearly")
    cfg["_704c_allocation_type_id"] = cfg.get("custom_allocation_id_704c")
    cfg["allocation_type_id_for_704c"] = cfg.get("custom_allocation_id_704c")

    # ── ENU_UnderlyingType IDs ──
    cfg["entity_underlying_type_id"] = cfg.get("underlying_type_id_k1_only")
    cfg["underlying_only_type_id"] = cfg.get("underlying_type_id_underlying_only")
    cfg["entity_total_underlying_type_id"] = cfg.get("underlying_type_id_entity_total")
    cfg["asset_class_underlying_type_id"] = cfg.get("underlying_type_id_asset_class")

    # ── ENU_EntityType: Investment ──
    cfg["inv_entity_type_id"] = cfg.get("entity_type_id_investment")

    # ── GlobalMenu flag derivations ──
    cfg["part_v_allocated"] = (cfg.get("flag_part_v_by_distribution_date") == "C")
    cfg["is_dar_setup"] = 1 if (cfg.get("flag_dar_setup") in ("C", "CG")) else 0
    cfg["is_dated_transfers_configured"] = cfg.get("flag_transfer_by_date")
    cfg["is_custom_allocation_rule_enabled"] = cfg.get("flag_custom_allocation_rule")
    cfg["is_form_199a_effective_pct_logic"] = (
        1 if cfg.get("flag_form199a_effective_pct_logic") == "C" else 0
    )
    cfg["is_pfic_allocation_by_quarter"] = cfg.get("flag_pfic_allocation_by_quarter")
    cfg["ignore_asset_class_for_partnership_level"] = cfg.get(
        "flag_ignore_asset_class_partnership_level"
    )
    cfg["override_indirect_lookthrough_asset_class"] = cfg.get(
        "flag_override_indirect_lookthrough_asset_class"
    )

    # ── AllocationRun workflow/transaction IDs ──
    cfg["custom_allocation_workflow_id"] = cfg.get("car_workflow_id")
    cfg["cost_percentage_workflow_id"] = cfg.get("cost_workflow_id")
    cfg["entity_allocation_rule_workflow_id"] = cfg.get(
        "entity_default_rule_override_workflow_id"
    )
    cfg["default_alloc_rule_transaction_id"] = cfg.get("dar_entity_transaction_id")
    cfg["global_default_alloc_rule_transaction_id"] = cfg.get(
        "dar_global_transaction_id"
    )
    # yearly_workflow_id, phase_id already in cfg under the same names.

    # ── Entity AllocationTypeName + 704c AllocationTypeName ──
    cfg["allocation_type_name"] = cfg.get("entity_allocation_type_name")
    cfg["_704c_allocation_type_name"] = cfg.get("entity_704c_allocation_type_name") or ""

    # ── Run-status gate ──
    if (cfg.get("run_status") or "").upper() == "FAIL":
        raise ValueError(
            f"AllocationRun not found or FAIL for RunID={cfg.get('run_id')}"
        )

    _log_timing("load_config", t0)
    return cfg
