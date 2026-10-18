"""Config loading and validation for SM LookThrough Effective Allocation Percentage.

All scalar lookups now come from load_common_config. This module:
  - Aliases the pre-resolved cfg keys to the legacy names that the downstream
    service files (amount_service, mapping_service, flowup_service, etc.) read.
  - Validates the entity's allocation type and writes RunStatus=FAIL to
    AllocationRun when blank.
"""

from pyspark.sql import SparkSession
import logging
import time

from Common_V2.core.helpers import table_prefix
from Common_V2.core.observability import log_section, log_timing

logger = logging.getLogger(__name__)

# Valid allocation type names (lowered for CI comparison)
_VALID_ALLOCATION_TYPES = {
    "pro rata",
    "aggregate 704(c) partial netting",
    "aggregate 704(c) full netting",
    "aggregate 704(c) full netting with incentive",
    "aggregate 704(c) partial netting with trading income",
    "pe book allocation",
}


def load_sp_config(spark: SparkSession, cfg: dict) -> dict:
    """Alias pre-resolved cfg scalars to the legacy names used by this SP.

    The actual lookups happen inside load_common_config. This function just
    preserves the legacy cfg key names so downstream service files (which
    read e.g. cfg['federal_amount_id'], cfg['k1_line_type']) work unchanged.
    """
    log_section("load_sp_config")
    t0 = time.time()

    # --- Allocation / Partner / Phase scalars (already on cfg from common config) ---
    cfg["allocation_type_name"] = cfg.get("entity_allocation_type_name")

    # `partner_txn_or_wf_id` — derived: prefer workflow id, fall back to transaction id.
    cfg["partner_txn_or_wf_id"] = (
        cfg.get("partner_workflow_id")
        if cfg.get("partner_workflow_id") is not None
        else cfg.get("partner_transaction_id")
    )

    # --- Line type IDs (legacy names without `_id` suffix kept for downstream) ---
    cfg["k1_line_type"] = cfg.get("k1_line_type_id")
    cfg["ubti_line_type"] = cfg.get("ubti_line_type_id")

    # --- GlobalMenu state flags ---
    state = (cfg.get("flag_configure_k1") or "").strip().upper()
    cfg["is_config_k1_checked"] = state in ("C", "CG")

    cfg["is_sidepocket_flowup_partner"] = (
        (cfg.get("flag_side_pocket_by_partner") or "").strip().upper() == "C"
    )

    # --- Menu-ID lookups (resolved in common config) ---
    cfg["register_type_id"] = cfg.get("register_type_menu_id")
    cfg["k1_input_source_type_id"] = cfg.get("k1_input_quicklinks_menu_id")
    cfg["_fed_to_footnote_menu_id"] = cfg.get("federal_to_footnote_menu_id")

    # --- ENU_MappingSource ---
    cfg["parent_k1_line_source_id"] = cfg.get("mapping_source_id_parent_k1")
    cfg["contributor_k1_line_source_id"] = cfg.get("mapping_source_id_contributor_k1")

    # --- ENU_StateDataList FieldSource scalars (legacy names) ---
    cfg["federal_amount_id"] = cfg.get("state_sourcing_federal_amount_id")
    cfg["federal_adj_id"] = cfg.get("state_sourcing_federal_adj_id")
    cfg["federal_ubti_id"] = cfg.get("state_sourcing_federal_ubti_id")
    cfg["federal_ubti_adj_id"] = cfg.get("state_sourcing_federal_ubti_adj_id")
    cfg["alloc_only_federal_amount_id"] = cfg.get("state_sourcing_alloc_only_federal_amount_id")
    cfg["alloc_only_federal_ubti_id"] = cfg.get("state_sourcing_alloc_only_federal_ubti_id")

    log_timing("load_sp_config", t0)
    return cfg


def validate_allocation_type(spark: SparkSession, cfg: dict) -> bool:
    """Check if allocation type is valid; if blank, set RunStatus=FAIL.

    Converted from: SQL lines 457-501.
    Returns:
        True  — allocation type is valid; continue.
        False — blank or not in the valid set; SP should exit.
    """
    log_section("validate_allocation_type")
    t0 = time.time()

    alloc_name = cfg.get("allocation_type_name")

    if not alloc_name or alloc_name.strip() == "":
        # UPDATE AllocationRun SET RunStatus='FAIL' ...
        run_id = cfg["run_id"]
        client_id = cfg["client_id"]
        tax_period_id = cfg["tax_period_id"]
        assert run_id is not None, "run_id must not be None"
        assert client_id is not None, "client_id must not be None"

        fqn = f"{table_prefix(cfg)}.AllocationRun"
        spark.sql(f"""
            UPDATE {fqn}
            SET RunStatus = 'FAIL',
                RunEndDate = current_timestamp(),
                StatusDesc = 'Allocation logic not selected for the entity.'
            WHERE RunID = {run_id}
              AND ClientID = {client_id}
              AND TaxPeriodID = {tax_period_id}
        """)
        logger.warning(
            f"[FAIL] AllocationTypeName is blank. RunID={run_id}, "
            f"EntityID={cfg.get('entity_id')} — RunStatus set to FAIL."
        )
        log_timing("validate_allocation_type", t0)
        return False

    if alloc_name.strip().lower() not in _VALID_ALLOCATION_TYPES:
        logger.warning(
            f"[SKIP] AllocationTypeName='{alloc_name}' is not in valid set. "
            f"RunID={cfg['run_id']}, EntityID={cfg.get('entity_id')}"
        )
        log_timing("validate_allocation_type", t0)
        return False

    log_timing("validate_allocation_type", t0)
    return True
