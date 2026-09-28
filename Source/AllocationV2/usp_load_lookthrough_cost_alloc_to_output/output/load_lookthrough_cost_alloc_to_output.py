"""
load_lookthrough_cost_alloc_to_output.py

Converted from: uspLoadLookThroughCostAllocationToOutput (existing PySpark v1)
Original procedure: dbo.uspLoadLookThroughCostAllocationToOutput
Conversion date: 2026-05-05

Usage (standalone):
    from load_lookthrough_cost_alloc_to_output import run_load_lookthrough_cost_alloc

    run_load_lookthrough_cost_alloc(
        spark,
        entity_id=123, client_id=456, tax_period_id=789, run_id=1001,
        catalog="qa7", schema="iPC_2025_QA7_15347",
        line_type="K1 with Cost", rank_for_rule=0,
    )

Usage (reuse shared config from a workflow):
    cfg = load_common_config(spark, entity_id, client_id, tax_period_id, run_id,
                             catalog="qa7", schema="iPC_2025_QA7_15347")
    run_load_lookthrough_cost_alloc(spark, cfg=cfg, line_type="K1 with Cost")
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
import logging
import time

from Common_V2.core.helpers import read_table, table_prefix, ns0
from Common_V2.core.checkpoint import checkpoint, drop_checkpoints
from Common_V2.core.observability import log_section, log_timing
from Common_V2.core.config import load_common_config
from Common_V2.core.writers import write_insert

from _data_loading import (
    load_partners, load_line_items, load_lookthrough_input,
    load_allocation_rules, load_cost_percentages,
    load_book_effective_rules, add_footnote_inheritance,
    load_final_effective_percentages,
)
from _hierarchy import build_entity_hierarchy, build_rule_ordered_underlyings, _checkpoint
from _allocation import (
    prepare_lookthrough_input,
    validate_by_amount_allocations, handle_offset_types_704c,
    process_by_amount_allocation, process_by_percentage_allocation,
    process_704c_allocation,
    insert_704c_by_amount_mapped_lines,
)

# ---------------------------------------------------------------------------
# Module logger
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)



def _load_sp_specific_config(spark: SparkSession, cfg: dict) -> dict:
    """Alias common cfg scalars to the legacy SP-internal names used downstream.

    All scalar config (line type IDs, allocation type IDs, GlobalMenu flags,
    entity/704c names) comes from load_common_config. No DB reads here.
    """
    log_section("load_sp_specific_config")
    t0 = time.time()

    # --- Line type ID aliases ---
    cfg["adjustment_line_type_id"] = cfg.get("book_k1_adjustments_line_type_id")
    cfg["box_jkl_line_type_id"] = cfg.get("boxjkl_line_type_id")
    # k1_line_type_id already in cfg under same name

    # --- ENU_CustomAllocations ID aliases ---
    cfg["cost_allocation_type_id"] = cfg.get("custom_allocation_id_cost")
    cfg["book_allocation_type_id"] = cfg.get("custom_allocation_id_book")
    cfg["offset_allocation_type_id"] = cfg.get("custom_allocation_id_offset")
    cfg["yearly_allocation_type_id"] = cfg.get("custom_allocation_id_yearly")
    cfg["c704_allocation_type_id"] = cfg.get("custom_allocation_id_704c")
    cfg["lp_offset_type_id"] = cfg.get("custom_allocation_id_lp_offset")
    cfg["gp_offset_type_id"] = cfg.get("custom_allocation_id_gp_offset")
    # allocation_by_amount_type_id already on cfg under same name

    # --- Entity allocation type names ---
    cfg["allocation_type_name"] = cfg.get("entity_allocation_type_name")
    cfg["c704_allocation_type_name"] = cfg.get("entity_704c_allocation_type_name")

    # --- GlobalMenu state flags (pre-resolved) ---
    cfg["is_dated_transfers"] = (cfg.get("flag_transfer_by_date") or "") == "C"
    cfg["is_separate_gains_loss"] = (cfg.get("flag_separate_gains_loss_stuffing") or "U") == "C"
    cfg["is_custom_allocation_enabled"] = cfg.get("flag_custom_allocation_rule") or ""
    cfg["ignore_assetclass_partnership"] = (
        cfg.get("flag_ignore_asset_class_partnership_level") or ""
    )
    cfg["override_indirect_lookthrough_assetclass"] = (
        cfg.get("flag_override_indirect_lookthrough_asset_class") or ""
    )

    log_timing("load_sp_specific_config", t0)
    return cfg


def load_workflow_ids(spark: SparkSession, cfg: dict) -> dict:
    """Validate run status and check FinalEffectivePercentages eligibility.

    All AllocationRun scalars are already on cfg from load_common_config.
    Returns cfg if SP should proceed, or None to skip.
    """
    log_section("load_workflow_ids")
    t0 = time.time()

    # RunStatus check (already on cfg from load_common_config).
    if cfg.get("run_status") and cfg["run_status"].upper() == "FAIL":
        logger.warning(
            f"[SKIP] RunStatus=FAIL for RunID={cfg['run_id']}. Skipping.")
        log_timing("load_workflow_ids", t0)
        return None

    # SQL: NOT EXISTS (SELECT 1 FROM FinalEffectivePercentages ...)
    # Business-data eligibility check — keep in SP.
    fep_check = read_table(spark, "FinalEffectivePercentages", cfg).filter(
        (F.col("RunID") == cfg["run_id"]) &
        (F.col("AllocationType").isin(
            "Cost", "CostAdjustedDatedTransfer",
            "ProRata", "Cost without Transfer Adj %", "704c"
        ))
    ).limit(1).first()

    if fep_check is None:
        logger.warning(
            f"[SKIP] No FinalEffectivePercentages found for RunID={cfg['run_id']}.")
        log_timing("load_workflow_ids", t0)
        return None

    # Normalise: load_common_config leaves entity_default_rule_override_workflow_id
    # and dar_*_transaction_id as None when AllocationRun.* is NULL; this SP
    # expects 0 sentinels downstream.
    cfg["entity_default_rule_override_workflow_id"] = (
        cfg.get("entity_default_rule_override_workflow_id") or 0
    )
    cfg["dar_entity_transaction_id"] = cfg.get("dar_entity_transaction_id") or 0
    cfg["dar_global_transaction_id"] = cfg.get("dar_global_transaction_id") or 0

    log_timing("load_workflow_ids", t0)
    return cfg




def write_allocation_output(spark: SparkSession, cfg: dict,
                            allocation_output: DataFrame) -> int:
    """Write allocation output to LookThroughAllocationOutput.
    Applies allocation type naming logic (Cost→Special, DEFAULT→Cost, etc).
    For 704c line types, uses a separate direct-write branch (FAIL-3 fix)
    matching SQL: writes the 704c allocation-type literal and filters Amount<>0.
    Returns row count written.
    """
    log_section("write_allocation_output")
    t0 = time.time()
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    line_type = cfg.get("line_type", "")

    # FAIL-3 fix: 704c write branch — SQL writes a separate INSERT with
    # AllocationType = @AllocationType704c (the runtime name for the special
    # 704c allocation, e.g. 'Special 704c Gain/Loss') and filters Amount <> 0.
    if line_type == "704c":
        c704_name = cfg.get("c704_allocation_type_name") or "704c"
        final_704c = allocation_output.alias("L").join(
            read_table(spark, "ENU_CustomAllocations", cfg).alias("EC"),
            F.col("L.TypeID") == F.col("EC.AllocationTypeID")
        ).filter(ns0(F.col("L.Amount")) != 0).select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.col("L.EntityID"),
            F.col("L.ShareClass"),
            F.col("L.PartnerNumber"),
            F.col("L.LineTypeID"),
            F.col("L.LineID"),
            F.col("L.Amount"),
            F.lit(c704_name).alias("AllocationType"),
            F.col("L.QuicklinkID"),
            F.col("L.Amount704b"),
            F.lit(0).alias("CategoryID"),
            F.col("L.ParentEntityID"),
            F.lit(None).cast("int").alias("PeriodID"),
            F.lit("").alias("LineCode"),
            F.col("L.SuperParentEntityID"),
            F.col("L.AdjustmentTypeID"),
            F.col("L.TrackingKey"),
            F.col("L.Tag"),
            F.col("EC.AllocationTypeID"),
            F.col("L.OriginalParentEntityID"),
            F.lit(None).cast("string").alias("FlowUpPartner"),
        )
        row_count = write_insert(spark, cfg, final_704c,
                                 "LookThroughAllocationOutput")
        log_timing("write_allocation_output", t0)
        return row_count

    # Add allocation type naming logic
    output_with_type = allocation_output.alias("L").join(
        read_table(spark, "ENU_CustomAllocations", cfg).alias("EC"),
        F.col("L.TypeID") == F.col("EC.AllocationTypeID")
    ).withColumn(
        "AllocationTypeNew",
        F.when(F.lower(F.col("L.AllocationType")) == "cost",
               F.when(F.lower(F.col("EC.AllocationType")).startswith("special "),
                      F.lit("Special Allocation"))
               .otherwise(F.col("EC.AllocationType")))
        .when(F.lower(F.col("L.AllocationType")) == "costadjusteddatedtransfer",
              F.concat(F.col("EC.AllocationType"),
                       F.lit("AdjustedDatedTransfer")))
        .when(F.lower(F.col("L.AllocationType")) == "default", F.lit("Cost"))
        .when(F.lower(F.col("L.AllocationType")) == "defaultadjusteddatedtransfer",
              F.lit("CostAdjustedDatedTransfer"))
        .when(F.lower(F.col("L.AllocationType")) == "cost without transfer adj %",
              F.concat(F.col("EC.AllocationType"),
                       F.lit(" without Transfer Adj %")))
        .when(F.lower(F.col("L.AllocationType")) == "default without transfer adj %",
              F.lit("Cost without Transfer Adj %"))
        .otherwise(F.lit("ProRata"))
    )

    # Final output columns
    final_output = output_with_type.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.col("L.EntityID"),
        F.col("L.ShareClass"),
        F.col("L.PartnerNumber"),
        F.col("L.LineTypeID"),
        F.col("L.LineID"),
        F.col("L.Amount"),
        F.col("AllocationTypeNew").alias("AllocationType"),
        F.col("L.QuicklinkID"),
        F.col("L.Amount704b"),
        F.lit(0).alias("CategoryID"),
        F.col("L.ParentEntityID"),
        F.lit(None).cast("int").alias("PeriodID"),
        F.lit("").alias("LineCode"),
        F.col("L.SuperParentEntityID"),
        F.col("L.AdjustmentTypeID"),
        F.col("L.TrackingKey"),
        F.col("L.Tag"),
        F.col("EC.AllocationTypeID"),
        F.col("L.OriginalParentEntityID"),
        F.lit(None).cast("string").alias("FlowUpPartner"),
    )

    # Write via named-column INSERT
    row_count = write_insert(spark, cfg, final_output,
                             "LookThroughAllocationOutput")
    log_timing("write_allocation_output", t0)
    return row_count


def update_input_table(spark: SparkSession, cfg: dict,
                       allocation_output: DataFrame) -> None:
    """Deduct allocated amounts from LookThroughAllocationInput + zero residuals.

    Strategy: Read-Modify-Write with broadcast join (single Delta commit, no MERGE).
      1. Aggregate alloc_output -> deduction amounts (per natural key)
      2. Read RunID rows from target table
      3. Broadcast-join with deductions; compute new Amount and zero residuals
         (|amount| < 1.00 -> 0.0) in one expression
      4. Atomic overwrite of RunID rows

    Folds the previous separate ``cleanup_small_amounts`` MERGE into the same
    pass.
    """
    log_section("update_input_table")
    t0 = time.time()
    run_id = cfg["run_id"]
    fqn = f"{table_prefix(cfg)}.LookThroughAllocationInput"

    # Step 1: Aggregate alloc_output -> deduction amounts per key.
    # ns0 normalizes nullable join keys so they compare equal to COALESCE(..,0)
    # on the target side.
    deductions = (
        allocation_output
        .withColumn("AdjustmentTypeID_g", ns0(F.col("AdjustmentTypeID")))
        .withColumn("OriginalParentEntityID_g", ns0(F.col("OriginalParentEntityID")))
        .groupBy(
            "LineID", "LineTypeID", "EntityID",
            "ParentEntityID", "SuperParentEntityID", "TrackingKey",
            "Tag", "AdjustmentTypeID_g", "OriginalParentEntityID_g",
        )
        .agg(F.sum(ns0(F.col("Amount"))).alias("DeductAmount"))
        .withColumnRenamed("AdjustmentTypeID_g", "AdjustmentTypeID")
        .withColumnRenamed("OriginalParentEntityID_g", "OriginalParentEntityID")
    )

    # Step 2: Read all RunID rows from target table.
    current = spark.table(fqn).filter(F.col("RunID") == run_id)

    # Step 3: Broadcast-join + compute new Amount in one pass.
    join_cond = (
        (F.col("t.EntityID") == F.col("s.EntityID"))
        & (F.coalesce(F.col("t.ParentEntityID"), F.lit(0)) == F.coalesce(F.col("s.ParentEntityID"), F.lit(0)))
        & (F.coalesce(F.col("t.SuperParentEntityID"), F.lit(0)) == F.coalesce(F.col("s.SuperParentEntityID"), F.lit(0)))
        & (F.coalesce(F.col("t.TrackingKey"), F.lit("")) == F.coalesce(F.col("s.TrackingKey"), F.lit("")))
        & (F.coalesce(F.col("t.AdjustmentTypeID"), F.lit(0)) == F.col("s.AdjustmentTypeID"))
        & (F.coalesce(F.col("t.OriginalParentEntityID"), F.lit(0)) == F.col("s.OriginalParentEntityID"))
        & (F.col("t.LineID") == F.col("s.LineID"))
        & (F.col("t.LineTypeID") == F.col("s.LineTypeID"))
        & (F.coalesce(F.col("t.Tag"), F.lit("")) == F.coalesce(F.col("s.Tag"), F.lit("")))
    )

    # Single expression: deduct if matched, zero if small residual, else pass-through.
    raw_amount = F.when(
        F.col("s.DeductAmount").isNotNull(),
        F.round(F.col("t.Amount") - F.col("s.DeductAmount"), 2)
    ).otherwise(F.col("t.Amount"))

    # Zero out tiny residuals (folds in former cleanup_small_amounts logic) but
    # ONLY for the line types that the legacy cleanup targeted; other line
    # types must pass through unchanged.
    k1_lti = cfg["k1_line_type_id"]
    adj_lti = cfg["adjustment_line_type_id"]
    new_amount = F.when(
        F.col("t.LineTypeID").isin(k1_lti, adj_lti)
        & raw_amount.between(-0.99, 0.99),
        F.lit(0.0)
    ).otherwise(raw_amount).cast("double")

    updated = (
        current.alias("t")
        .join(F.broadcast(deductions).alias("s"), join_cond, "left")
        .select(
            F.col("t.RunID"), F.col("t.ClientID"), F.col("t.EntityID"),
            F.col("t.LineTypeID"), F.col("t.LineID"),
            new_amount.alias("Amount"),
            F.col("t.QuicklinkID"), F.col("t.Amount704b"), F.col("t.CategoryID"),
            F.col("t.ParentEntityID"), F.col("t.PeriodID"), F.col("t.LineCode"),
            F.col("t.SuperParentEntityID"), F.col("t.AdjustmentTypeID"),
            F.col("t.TrackingKey"), F.col("t.Tag"),
            F.col("t.OriginalParentEntityID"), F.col("t.FlowUpPartner"),
        )
    )

    # Step 4: Atomic overwrite of RunID partition -- single Delta commit.
    updated.writeTo(fqn).overwrite(F.col("RunID") == run_id)
    logger.info(f"[WRITE] Overwrote deducted amounts + zeroed residuals in {fqn}")
    log_timing("update_input_table", t0)


# ===========================================================================
# Orchestrator
# ===========================================================================

def run_load_lookthrough_cost_alloc(spark: SparkSession, cfg: dict = None,
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
                                    **run_params) -> dict:
    """Main entry point — works with all three execution modes.

    Mode 1 (Job):         wrapper passes cfg=cfg_dict (pre-built by Task 0).
    Mode 2 (Orchestrator): caller passes cfg=cfg_dict
    Mode 3 (Standalone):   no cfg → loads its own config
    """
    # Orchestrator passes PascalCase params — map to lowercase
    entity_id = entity_id or EntityID
    client_id = client_id or ClientID
    tax_period_id = tax_period_id or TaxPeriodID
    run_id = run_id or RunID
    catalog = catalog or CatalogName
    schema = schema or SchemaName
    volume_path = volume_path or VolumePath or ""
    execution_id = execution_id or ExecutionID

    # Map PascalCase from JSON Parameters (e.g. "LineType": "K1")
    line_type = line_type or run_params.get("LineType", "") or ""
    rank_for_rule = rank_for_rule or run_params.get("RankForRule", 0) or 0
    result_type = result_type or run_params.get("ResultType", "deltalake") or "deltalake"

    if cfg is None:
        cfg = load_common_config(spark, run_id=run_id, entity_id=entity_id,
                                 client_id=client_id, tax_period_id=tax_period_id,
                                 catalog=catalog, schema=schema, **run_params)
    cfg["result_type"] = result_type
    cfg["volume_path"] = volume_path
    cfg["execution_id"] = execution_id
    cfg["verbose"] = verbose

    # Copy checkpoint list for thread safety
    cfg = {**cfg, "_checkpoint_tables": []}
    cfg["line_type"] = line_type
    cfg["rank_for_rule"] = rank_for_rule

    # SP-specific config
    cfg = _load_sp_specific_config(spark, cfg)

    t0 = time.time()
    try:
        # Load workflow IDs and validate
        cfg = load_workflow_ids(spark, cfg)
        if cfg is None:
            return {"sp": "uspLoadLookThroughCostAllocationToOutput",
                    "status": "SKIP", "reason": "RunStatus=FAIL"}

        # --- Data Loading ---
        partners = load_partners(spark, cfg)
        line_items = load_line_items(spark, cfg)
        input_data_raw = load_lookthrough_input(spark, cfg)
        rules = load_allocation_rules(spark, cfg)
        cost_data = load_cost_percentages(spark, cfg)

        # FAIL-1 fix: 704c-to-K1 line mapping (UNPIVOT block).
        # SQL lines 407-565: when LineType='K1 with 704c' and
        # @704cAllocationTypeName is set, unpivot the 704c snapshot's 10
        # amount columns into synthetic K1 cost rows, and append synthetic
        # rules (TransactionID=-2) to map_rules and default_rules.
        distinct_mappings = None
        if (line_type == "K1 with 704c"
                and (cfg.get("c704_allocation_type_name") or "").strip()):
            from _data_loading import apply_704c_to_k1_mappings
            mapped = apply_704c_to_k1_mappings(
                spark, cfg, cost_data["cost_percentages"],
                rules["map_rules"], rules["default_rules"]
            )
            cost_data["cost_percentages"] = mapped["cost_percentages"]
            rules["map_rules"] = mapped["map_rules"]
            rules["default_rules"] = mapped["default_rules"]
            distinct_mappings = mapped["distinct_mappings"]

        # --- Entity Hierarchy ---
        cost_percentages = cost_data["cost_percentages"]
        all_underlyings = build_entity_hierarchy(spark, cfg, cost_percentages)
        all_underlyings = build_rule_ordered_underlyings(
            spark, cfg, all_underlyings, input_data_raw,
            rules["map_rules"], rules["default_rules"]
        )

        # --- Book Effective + Input Prep ---
        book_effective = load_book_effective_rules(spark, cfg)
        book_effective = add_footnote_inheritance(
            spark, cfg, book_effective, input_data_raw)

        # Convert book to cost allocation type
        book_effective = book_effective.withColumn(
            "AdjustmentAllocationTypeID",
            F.when(
                F.col("AdjustmentAllocationTypeID") == cfg["book_allocation_type_id"],
                cfg["cost_allocation_type_id"]
            ).otherwise(F.col("AdjustmentAllocationTypeID"))
        )

        # SQL: Re-add footnote-source lines to #LineItem that are in
        # book_effective or all_underlyings (these were excluded by LEFT ANTI JOIN)
        line_items = _readd_footnote_source_lines(
            spark, cfg, line_items, book_effective, all_underlyings)

        input_data = prepare_lookthrough_input(
            spark, cfg, input_data_raw, line_items, book_effective,
            rules["entity_rules"], all_underlyings, rules["default_rules"]
        )

        # FAIL-5 fix: 704c By-Amount mapped lines (SQL lines 933-957).
        # When CAR-enabled (@IsCustomAllocationRuleEnabled='C'), mapped 704c
        # lines exist, and there are By-Amount default rules, insert
        # additional rows into the lookthrough input with AllocationTypeID
        # taken from the rule.
        if (distinct_mappings is not None
                and cfg.get("is_custom_allocation_enabled") == "C"):
            input_data = insert_704c_by_amount_mapped_lines(
                spark, cfg, input_data, distinct_mappings,
                all_underlyings, rules["default_rules"], book_effective
            )

        # --- Final Effective Percentages ---
        final_eff_pct = load_final_effective_percentages(spark, cfg)

        # --- Validation ---
        validate_by_amount_allocations(
            spark, cfg, input_data, final_eff_pct, partners,
            rules["default_rules"], rules["map_rules"]
        )

        # --- Process Allocations ---
        if line_type == "704c":
            allocation_percentages = final_eff_pct.groupBy(
                "RunID", "EntityID", "InvestmentID", "PartnerNumber",
                "LineID", "LineTypeID", "Quarter", "TrackingKey", "TypeID"
            ).pivot("704cPercentType", [
                "OrdinaryPercentage", "CapitalPercentage",
                "CapitalGainPercentage", "CapitalLossPercentage"
            ]).agg(F.max("EffPercentage"))

            k1_lineitems_704c = _build_k1_lineitems_704c(
                spark, cfg, input_data, line_items)
            k1_lineitems_704c = handle_offset_types_704c(
                spark, cfg, k1_lineitems_704c, allocation_percentages)

            allocation_output = process_704c_allocation(
                spark, cfg, input_data, final_eff_pct,
                allocation_percentages, k1_lineitems_704c
            )
        else:
            by_amount = process_by_amount_allocation(
                spark, cfg, input_data, final_eff_pct,
                partners, rules["map_rules"]
            )

            # SQL: After by-amount allocation, subtract allocated amounts
            # from input, delete small amounts, and remove FEP by-amount entries
            # before running by-percentage
            if not by_amount.isEmpty():
                # Aggregate allocated amounts per key
                # SQL: WHERE AllocationType IN ('Cost','CostAdjustedDatedTransfer',...)
                by_amt_agg = by_amount.filter(
                    F.col("AllocationType").isin(
                        "Cost", "CostAdjustedDatedTransfer", "ProRata",
                        "DEFAULT", "DefaultAdjustedDatedTransfer",
                        "Cost without Transfer Adj %"
                    )
                ).groupBy(
                    "LineID", "LineTypeID", "EntityID",
                    "ParentEntityID", "SuperParentEntityID",
                    "TrackingKey", "AdjustmentTypeID", "Tag",
                    "OriginalParentEntityID"
                ).agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("AllocatedAmount"))

                # Subtract from input_data
                input_data = input_data.alias("L").join(
                    by_amt_agg.alias("AO"),
                    (F.col("L.EntityID") == F.col("AO.EntityID")) &
                    (F.coalesce(F.col("L.ParentEntityID"), F.lit(0)) ==
                     F.coalesce(F.col("AO.ParentEntityID"), F.lit(0))) &
                    (F.coalesce(F.col("L.SuperParentEntityID"), F.lit(0)) ==
                     F.coalesce(F.col("AO.SuperParentEntityID"), F.lit(0))) &
                    (F.coalesce(F.col("L.TrackingKey"), F.lit("0")) ==
                     F.coalesce(F.col("AO.TrackingKey"), F.lit("0"))) &
                    (F.coalesce(F.col("L.AdjustmentTypeID"), F.lit(0)) ==
                     F.coalesce(F.col("AO.AdjustmentTypeID"), F.lit(0))) &
                    (F.col("L.LineID") == F.col("AO.LineID")) &
                    (F.col("L.LineTypeID") == F.col("AO.LineTypeID")) &
                    (F.coalesce(F.col("L.Tag"), F.lit("")) ==
                     F.coalesce(F.col("AO.Tag"), F.lit(""))),
                    "left"
                ).withColumn(
                    "Amount",
                    F.when(F.col("AO.AllocatedAmount").isNotNull(),
                           F.col("L.Amount") - F.col("AO.AllocatedAmount"))
                    .otherwise(F.col("L.Amount"))
                ).select(
                    F.col("L.RunID"), F.col("L.ClientID"), F.col("L.EntityID"),
                    F.col("L.LineTypeID"), F.col("L.LineID"), F.col("Amount"),
                    F.col("L.QuicklinkID"), F.col("L.Amount704b"),
                    F.col("L.CategoryID"), F.col("L.ParentEntityID"),
                    F.col("L.PeriodID"), F.col("L.LineCode"),
                    F.col("L.SuperParentEntityID"), F.col("L.AdjustmentTypeID"),
                    F.col("L.TrackingKey"), F.col("L.Tag"),
                    F.col("L.OriginalParentEntityID"),
                    F.col("L.TransactionDate"), F.col("L.TypeID"),
                    F.col("L.CustomTrackingKey"), F.col("L.CustomTag"),
                    F.col("L.IsExcludefromTransfer"),
                    F.col("L.Classification"), F.col("L.CapitalGainLoss"),
                )

                # Delete rows with small amounts (except BoxJKL)
                box_jkl_lti = cfg["box_jkl_line_type_id"]
                input_data = input_data.filter(
                    ~(
                        (F.coalesce(F.col("Amount"), F.lit(0)).between(-0.99, 0.99)) &
                        (F.col("LineTypeID") != box_jkl_lti)
                    )
                )

            # Remove FEP entries with AllocationBy='AMOUNT' before by-percentage
            fep_for_pct = final_eff_pct.alias("FEP").join(
                F.broadcast(rules["default_rules"]).alias("DR"),
                F.col("FEP.TypeID") == F.col("DR.RuleID")
            ).join(
                F.broadcast(read_table(spark, "ENU_AllocationBy", cfg)).alias("EA"),
                (F.col("DR.AllocationByID") == F.col("EA.AllocationByID")) &
                (F.lower(F.col("EA.AllocationBy")) == "amount")
            ).select("FEP.TypeID").distinct()

            final_eff_pct_filtered = final_eff_pct.join(
                fep_for_pct,
                on="TypeID",
                how="left_anti"
            )

            by_pct = process_by_percentage_allocation(
                spark, cfg, input_data, final_eff_pct_filtered, partners
            )
            allocation_output = by_amount.unionByName(by_pct)

        # Checkpoint allocation_output -- it is consumed twice below
        # (write_allocation_output + update_input_table). Materializing once
        # avoids re-evaluating the full plan in each branch.
        # Per Rule 35: localCheckpoint (~0.5-1s) instead of Delta (~3-9s).
        # The result lives in executor memory and is read twice by the
        # parallel writers below -- it does NOT need to survive the
        # SparkSession.
        allocation_output = _checkpoint(spark, allocation_output, "alloc_output", cfg)

        # --- Write Output + Update Input (parallel) ---
        with ThreadPoolExecutor(max_workers=2) as executor:
            f_write = executor.submit(
                write_allocation_output, spark, cfg, allocation_output)
            f_update = executor.submit(
                update_input_table, spark, cfg, allocation_output)

            for future in as_completed([f_write, f_update]):
                future.result()  # raises if failed

        # NOTE: cleanup_small_amounts removed -- its zero-residual logic is
        # now folded into update_input_table (single Delta commit).

        status = {
            "sp": "uspLoadLookThroughCostAllocationToOutput",
            "status": "SUCCESS",
            "line_type": line_type,
            "elapsed": round(time.time() - t0, 1),
        }
    except Exception as e:
        logger.error(f"[FAIL] uspLoadLookThroughCostAllocationToOutput: {e}",
                     exc_info=True)
        status = {
            "sp": "uspLoadLookThroughCostAllocationToOutput",
            "status": "FAIL",
            "error": str(e),
        }
        raise
    finally:
        if cfg is not None:
            drop_checkpoints(spark, cfg)

    logger.info(f"[DONE] {status}")
    return status


# ===========================================================================
# Internal Helpers
# ===========================================================================

def _lookup_id(rows: list, key_col: str, key_val: str, id_col: str):
    """Find a scalar ID from collected lookup rows (case-insensitive)."""
    for r in rows:
        if r[key_col] is not None and r[key_col].lower() == key_val.lower():
            return r[id_col]
    return None


def _lookup_alloc_by(rows: list, name: str):
    """Find AllocationByID from ENU_AllocationBy rows."""
    for r in rows:
        if r["AllocationBy"] is not None and r["AllocationBy"].upper() == name.upper():
            return r["AllocationByID"]
    return None


def _readd_footnote_source_lines(spark: SparkSession, cfg: dict,
                                  line_items: DataFrame,
                                  book_effective: DataFrame,
                                  all_underlyings: DataFrame) -> DataFrame:
    """Re-add footnote-source lines to line_items that exist in book_effective
    or all_underlyings.
    SQL: INSERT INTO #LineItem
         SELECT DISTINCT K.LineID, K.AllocationTypeRuleId, K.LinetypeId,...
         FROM #K1lineItem K
         INNER JOIN #TempBookEffective B ON K.LineID = B.LineID
         INNER JOIN MAP_SourceAttributeRelation M ON B.LineID = M.AttributeLineID
         UNION
         SELECT DISTINCT K.LineID, K.AllocationTypeRuleId, K.LinetypeId,...
         FROM #K1lineItem K
         INNER JOIN #TempAllUnderlyings D ON K.LineID = D.LineID
         INNER JOIN MAP_SourceAttributeRelation M ON D.LineID = M.AttributeLineID
    """
    prefix = table_prefix(cfg)
    k1_lti = cfg["k1_line_type_id"]

    # Get all K1 lines (including those excluded by anti-join)
    all_k1_lines = spark.sql(f"""
        SELECT DISTINCT K.LineID, K.AllocationTypeRuleId,
               {k1_lti} AS LineTypeID,
               K.TransactionDate, K.IsTransactionDate, K.IsTransfersAdjusted,
               K.Classification, K.CapitalGainLoss
        FROM {prefix}.K1LineItem K
    """)

    # Lines in MAP_SourceAttributeRelation that are in book_effective
    source_attr = spark.table(f"{prefix}.MAP_SourceAttributeRelation")

    k1_cols = [F.col(f"K.{c}") for c in all_k1_lines.columns]

    book_footnote_lines = all_k1_lines.alias("K").join(
        book_effective.select("LineID").distinct().alias("B"),
        F.col("K.LineID") == F.col("B.LineID")
    ).join(
        source_attr.alias("M"),
        F.col("B.LineID") == F.col("M.AttributeLineID")
    ).select(k1_cols).distinct()

    # Lines in MAP_SourceAttributeRelation that are in all_underlyings
    underlying_footnote_lines = all_k1_lines.alias("K").join(
        all_underlyings.select("LineID").distinct().alias("D"),
        F.col("K.LineID") == F.col("D.LineID")
    ).join(
        source_attr.alias("M"),
        F.col("D.LineID") == F.col("M.AttributeLineID")
    ).select(k1_cols).distinct()

    # Union and add to existing line_items
    footnote_readd = book_footnote_lines.unionByName(
        underlying_footnote_lines
    ).distinct()

    if footnote_readd.isEmpty():
        return line_items

    return line_items.unionByName(footnote_readd).distinct()


def _build_k1_lineitems_704c(spark: SparkSession, cfg: dict,
                             input_data: DataFrame,
                             line_items: DataFrame) -> DataFrame:
    """Build K1 line items DataFrame for 704c processing."""
    return input_data.alias("I").join(
        line_items.alias("K"),
        (F.col("I.LineID") == F.col("K.LineID")) &
        (F.col("I.LineTypeID") == F.col("K.LineTypeID"))
    ).select(
        F.col("I.EntityID").alias("EntityId"),
        F.col("I.TrackingKey"),
        F.col("I.LineID"),
        F.col("K.Classification"),
        F.col("K.CapitalGainLoss"),
        F.when(
            F.col("I.TypeID") == cfg["cost_allocation_type_id"],
            cfg["c704_allocation_type_id"]
        ).otherwise(F.col("I.TypeID")).alias("AllocationTypeRuleId"),
        F.col("I.LineTypeID"),
    ).distinct()


# ════════════════════════════════════════════════════════════════
# __main__: standalone execution (Mode 3).
# Read widget params → call run_load_lookthrough_cost_alloc(...). The function's
# `if cfg is None` branch is the single point that calls load_common_config.
# Job/Orchestrator modes pass cfg in directly and skip this block.
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    line_type = ""
    rank_for_rule = 0
    try:
        line_type = dbutils.widgets.get("line_type")  # noqa: F821
        rank_for_rule = int(dbutils.widgets.get("rank_for_rule"))  # noqa: F821
    except Exception:
        pass

    try:
        status = run_load_lookthrough_cost_alloc(
            spark,
            RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
            EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
            ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
            TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
            CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
            SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
            line_type=line_type,
            rank_for_rule=rank_for_rule,
        )
    except Exception as exc:
        print(f"Usage: provide run_id, entity_id, client_id, tax_period_id, catalog, schema "
              f"as widget parameters ({exc})")
        import sys
        sys.exit(1)

    try:
        dbutils.notebook.exit(json.dumps(status))
    except Exception:
        print(json.dumps(status, indent=2))
