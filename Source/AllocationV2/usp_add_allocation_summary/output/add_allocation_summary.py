"""
add_allocation_summary.py

Converted from: dbo.uspAddAllocationSummary
Original lines: ~1935
Conversion date: 2026-06-01

Loads allocated amounts from AllocationOutput/AllocationOutputSummary into
17 summary tables based on LineTypeID. Each summary table receives amounts
for its specific form type (K1, BoxJKL, Form926, PFIC, Custom Footnotes, etc.).

Performance profile:
    Complexity: Medium | Data volume: Medium | SLA target: 12s
    Checkpoints: 0
    Key optimizations: broadcast all lookup joins, read tables once,
    batch all config lookups.

Usage:
    from AllocationV2.usp_add_allocation_summary.output.add_allocation_summary import run_add_allocation_summary
    run_add_allocation_summary(spark, run_id=3778, entity_id=5944, client_id=15347,
                               tax_period_id=1, catalog="dev7", schema="myschema")
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time
import json

from Common_V2.core.helpers import read_table, ns, ns0, table_prefix, tbl_name
from Common_V2.core.config import load_common_config
from Common_V2.core.generic_result_storer import GenericResultStorer
from Common_V2.core.checkpoint import checkpoint, drop_checkpoints

logger = logging.getLogger("AllocationV2.usp_add_allocation_summary")


# ---------------------------------------------------------------------------
# Section 1: SP-specific config (SQL lines 200-600)
# ---------------------------------------------------------------------------

def load_sp_config(spark: SparkSession, cfg: dict) -> dict:
    """Alias common cfg scalars to the legacy SP-internal names used downstream.

    All actual scalar lookups happen in load_common_config. This function just
    maps the pre-resolved keys to the names the SP's downstream code reads.
    No DB reads, no lookup_* calls — fully Job-mode safe.
    """
    # ── Line-type ID aliases (SP-local names → common cfg keys with matching DB LineType) ──
    cfg["box_jkl_line_type_id"] = cfg.get("boxjkl_line_type_id")
    cfg["pass_income_line_type_id"] = cfg.get("passive_income_line_type_id")
    cfg["book_k1_adj_line_type_id"] = cfg.get("book_k1_adjustments_line_type_id")
    # All other *_line_type_id keys (k1, form926/199A/8865/8886, pfic_footnote, line18a,
    # ubti, gaap_to_tax, at_risk, m1) already populated by load_common_config under the
    # same names downstream code reads — no aliasing needed.

    # ── Entity scalar aliases ──
    cfg["allocation_type_name"] = cfg.get("entity_allocation_type_name")
    cfg["is_domestic_blocker"] = bool(cfg.get("entity_is_domestic_blocker") or False)

    # ── GlobalMenu state flags ──
    # `is_tracking_key` defaults to 'C' (matches legacy SP behavior when the flag is absent).
    cfg["is_tracking_key"] = cfg.get("flag_keep_tracking_keys") or "C"
    cfg["is_investment_level_rounding"] = cfg.get("flag_investment_level_rounding_logic")
    cfg["is_auto_elec_d_enabled"] = (
        (cfg.get("flag_automate_deemed_sale_election") or "U").strip().upper() in ("C", "CG")
    )
    cfg["is_k1_input_international"] = (
        (cfg.get("flag_separate_international_signoff") or "").strip().upper() == "C"
    )

    # PFIC-footnote line IDs (cfc_tested_income_line_id, cfc_tested_loss_line_id,
    # cumulative_qef_line_id, cumulative_qef_distributions_line_id,
    # reversal_py_qef_line_id, election_d_line_id, type_of_pfic_line_id) are
    # already populated by load_common_config as individual named scalars.
    # No aliasing needed — downstream reads cfg['cfc_tested_income_line_id'] etc.

    return cfg


# ---------------------------------------------------------------------------
# Section 2: Build working tables (SQL lines 600-780)
# ---------------------------------------------------------------------------

def build_working_tables(spark: SparkSession, cfg: dict) -> dict:
    """Build K1Workflow, AtRiskWorkflow, LowerTierFunds, AllocationOutput, AllocationOutputSummary.

    SQL lines: 600-780
    Returns dict with DataFrames.
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]

    # AllocationInputWorkflow — read once
    aiw = read_table(spark, "AllocationInputWorkflow", cfg).filter(
        F.col("RunID") == run_id
    ).select("EntityID", "K1WorkflowID", "K1InternationalWorkflowID", "ImportAtRiskWorkflowID")

    # #K1Workflow
    k1_workflow = aiw.select(
        F.col("EntityID"), F.col("K1WorkflowID").alias("WorkFlowID")
    ).filter(F.col("WorkFlowID").isNotNull())

    if cfg["is_k1_input_international"]:
        k1_intl = aiw.select(
            F.col("EntityID"), F.col("K1InternationalWorkflowID").alias("WorkFlowID")
        ).filter(F.col("WorkFlowID").isNotNull())
        k1_workflow = k1_workflow.unionByName(k1_intl)

    # #AtRiskWorkflow
    at_risk_workflow = aiw.select(
        F.col("EntityID"), F.col("ImportAtRiskWorkflowID").alias("WorkFlowID")
    ).filter(F.col("WorkFlowID").isNotNull())

    # #LowerTierFunds
    lower_tier_funds = read_table(spark, "LowerTierFunds", cfg).filter(
        F.col("RunID") == run_id
    ).select(
        F.col("EntityID"), F.col("PartnerNumber"), F.col("LTRunID").alias("RunID")
    )

    # #AllocationOutput — full table for this run
    allocation_output = read_table(spark, "AllocationOutput", cfg).filter(
        F.col("RunID") == run_id
    )

    # #AllocationOutputSummary — full table for this run
    allocation_output_summary = read_table(spark, "AllocationOutputSummary", cfg).filter(
        F.col("RunID") == run_id
    )

    # Shared lookups (read once, used by multiple sections)
    entity_tbl = cfg.get("_entity_tbl")
    if entity_tbl is None:
        entity_tbl = read_table(spark, "Entity", cfg)
        cfg["_entity_tbl"] = entity_tbl

    k1_package = read_table(spark, "K1Package", cfg)
    pfic_line_item_all = cfg.get("_pfic_line_item_tbl")
    if pfic_line_item_all is None:
        pfic_line_item_all = read_table(spark, "PFICFootnoteLineItem", cfg)
        cfg["_pfic_line_item_tbl"] = pfic_line_item_all

    pfic_line_item = pfic_line_item_all.filter(
        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == cfg["tax_period_id"])
    )
    reclass_footnote_data = read_table(spark, "ReclassFootnoteAllocationData", cfg).filter(
        F.col("RunID") == run_id
    )

    # SQL: IF EXISTS (SELECT TOP 1 RunID FROM AllocationOutput WHERE RunID=@LocalRunID AND PeriodID IS NOT NULL)
    # Computed once here so both write_k1_allocation_summary and write_m1_adj_allocation_summary
    # share a single Spark action rather than each triggering their own.
    has_periodic = not allocation_output.filter(F.col("PeriodID").isNotNull()).isEmpty()

    return {
        "k1_workflow": k1_workflow,
        "at_risk_workflow": at_risk_workflow,
        "lower_tier_funds": lower_tier_funds,
        "allocation_output": allocation_output,
        "allocation_output_summary": allocation_output_summary,
        "entity_tbl": entity_tbl,
        "k1_package": k1_package,
        "pfic_line_item_all": pfic_line_item_all,
        "pfic_line_item": pfic_line_item,
        "reclass_footnote_data": reclass_footnote_data,
        "has_periodic": has_periodic,
    }


# ---------------------------------------------------------------------------
# Helper: Tracking key expression
# ---------------------------------------------------------------------------

def _tracking_key_expr(is_tracking_key: str, entity_id: int, alias: str = None):
    """Build the CASE WHEN tracking key expression used across multiple summary inserts."""
    if is_tracking_key != "C":
        return F.lit(None).cast("string")
    tk_col = F.col(f"{alias}.TrackingKey") if alias else F.col("TrackingKey")
    eid_col = F.col(f"{alias}.EntityID") if alias else F.col("EntityID")
    return F.when(
        tk_col.isNull(),
        eid_col.cast("string")
    ).otherwise(
        F.concat(tk_col, F.lit("~"), F.lit(entity_id).cast("string"))
    )


# ---------------------------------------------------------------------------
# Helper: PE Book Allocation condition
# ---------------------------------------------------------------------------

def _should_insert_pe_book(cfg: dict) -> bool:
    """Check if PE Book Allocation condition allows insert.

    SQL: IF ((@AllocationTypeName = 'PE Book Allocation' AND @IsInvestmentLevelRounding = 'U')
             OR (@AllocationTypeName <> 'PE Book Allocation'))
    When @AllocationTypeName IS NULL in SQL, both comparisons yield NULL so the whole
    expression is NULL — the IF block does NOT execute.

    SQL parity: when AllocationTypeName is NULL, SQL IF is not true and gated inserts
    do not execute.
    """
    alloc_type = cfg.get("allocation_type_name")
    if alloc_type is None:
        return False
    rounding = cfg.get("is_investment_level_rounding")  # None when not in GlobalMenu
    if alloc_type == "PE Book Allocation":
        return rounding == "U"
    return True


def _store_result_table(spark: SparkSession, cfg: dict, table_name: str, df: DataFrame) -> None:
    """Write one result DataFrame via GenericResultStorer (Delta + optional Parquet).

    On parquet runs, accumulates the save_results JSON payload into
    cfg["_result_file_infos"] so the entry point can merge every table's
    ResultFilePath/ResultFileName for the DataBrickExecutionStatus row.
    """
    storer = GenericResultStorer(spark)
    return_value = storer.save_results(
        result={table_name: df},
        result_type=cfg.get("result_type", "deltalake"),
        catalog_name=cfg["catalog"],
        database_name=cfg["schema"],
        run_id=cfg["run_id"],
        client_id=cfg["client_id"],
        entity_id=cfg["entity_id"],
        execution_id=str(cfg.get("execution_id", "")),
        volume_path=cfg.get("volume_path", ""),
        sql_url_path=None,
        sql_username=None,
        sql_password=None,
    )
    if return_value and isinstance(return_value, str) and return_value.strip().startswith("{"):
        cfg.setdefault("_result_file_infos", []).append(return_value)


# ---------------------------------------------------------------------------
# Section 3: K1AllocationSummary (SQL lines 780-890)
# ---------------------------------------------------------------------------

def write_k1_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into K1AllocationSummary from AllocationOutputSummary where LineTypeID = K1.

    Also handles periodic K1 amounts (GROUP BY PeriodID) from AllocationOutput.
    SQL lines: 780-890
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    target_table = "K1AllocationSummary"

    aos = tables["allocation_output_summary"]

    # Main K1 insert from AllocationOutputSummary.
    # SQL lines 417-423: this insert is gated by the PE Book Allocation condition.
    # NOTE: the periodic K1 insert below is NOT gated (it lives in a separate
    # IF EXISTS(...PeriodID...) block in the SQL, outside the PE Book IF), so the
    # early-return must only skip the main insert — not the periodic one.
    if cfg["allow_pe_book_inserts"]:
        k1_df = aos.filter(F.col("LineTypeID") == k1_lt_id).select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("ShareClass"),
            F.col("PartnerNumber"),
            F.col("LineID"),
            F.col("Amount"),
            F.col("Amount").alias("FlowupAmount"),
        )
        _store_result_table(spark, cfg, target_table, k1_df)

    # Periodic K1 from AllocationOutput (PeriodID IS NOT NULL).
    # SQL lines 433-440: ungated by the PE Book condition.
    ao = tables["allocation_output"]
    has_periodic = tables["has_periodic"]

    if has_periodic:
        periodic_k1 = ao.filter(
            (F.col("LineTypeID") == k1_lt_id) & F.col("PeriodID").isNotNull()
        ).groupBy(
            ns(F.col("ShareClass")).alias("ShareClass"),
            F.col("PartnerNumber"),
            F.col("LineID"),
            F.col("PeriodID"),
        ).agg(
            F.sum(ns0(F.col("Amount"))).alias("Amount"),
        ).select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("ShareClass"),
            F.col("PartnerNumber"),
            F.col("LineID"),
            F.col("Amount"),
            F.col("Amount").alias("FlowupAmount"),
            F.col("PeriodID"),
        )
        _store_result_table(spark, cfg, target_table, periodic_k1)


# ---------------------------------------------------------------------------
# Section 4: M1AdjAllocationSummary (SQL lines 830-850)
# ---------------------------------------------------------------------------

def write_m1_adj_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into M1AdjAllocationSummary from AllocationOutputSummary.

    SQL lines: 830-850, 870-890 (periodic M1)
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    m1_lt_id = cfg["m1_line_type_id"]
    target_table = "M1AdjAllocationSummary"

    aos = tables["allocation_output_summary"]
    m1_df = aos.filter(F.col("LineTypeID") == m1_lt_id).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        ns(F.col("ShareClass")).alias("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        ns0(F.col("Amount")).alias("Amount"),
        ns0(F.col("Amount")).alias("FlowupAmount"),
        F.col("LineCode").alias("WorkPaperCode"),
    )
    _store_result_table(spark, cfg, target_table, m1_df)

    # Periodic M1 from AllocationOutput
    ao = tables["allocation_output"]
    has_periodic = tables["has_periodic"]
    if has_periodic:
        periodic_m1 = ao.filter(
            (F.col("LineTypeID") == m1_lt_id) & F.col("PeriodID").isNotNull()
        ).groupBy(
            ns(F.col("ShareClass")).alias("ShareClass"),
            F.col("PartnerNumber"),
            F.col("LineID"),
            F.col("PeriodID"),
            F.col("LineCode"),
        ).agg(
            F.sum(ns0(F.col("Amount"))).alias("Amount"),
        ).select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("ShareClass"),
            F.col("PartnerNumber"),
            F.col("LineID"),
            F.col("Amount"),
            F.col("Amount").alias("FlowupAmount"),
            F.col("PeriodID"),
            F.col("LineCode").alias("WorkPaperCode"),
        )
        _store_result_table(spark, cfg, target_table, periodic_m1)


# ---------------------------------------------------------------------------
# Section 5: BoxJKLAllocationSummary (SQL lines 890-905)
# ---------------------------------------------------------------------------

def write_box_jkl_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into BoxJKLAllocationSummary.

    SQL lines: 890-905
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "BoxJKLAllocationSummary"

    aos = tables["allocation_output_summary"]
    df = aos.filter(F.col("LineTypeID") == cfg["box_jkl_line_type_id"]).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
    )
    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 6: Form926AllocationSummary (SQL lines 905-925)
# ---------------------------------------------------------------------------

def write_form926_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into Form926AllocationSummary with tracking key logic.

    SQL lines: 905-925
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "Form926AllocationSummary"

    aos = tables["allocation_output_summary"]
    tk_expr = _tracking_key_expr(cfg["is_tracking_key"], entity_id)

    df = aos.filter(F.col("LineTypeID") == cfg["form926_line_type_id"]).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("EntityID").alias("SourceEntityID"),
        F.col("QuicklinkID").alias("Form926ID"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.lit(None).cast("string").alias("TextValue"),
        F.col("ParentEntityId"),
        tk_expr.alias("TrackingKey"),
        F.col("OriginalParentEntityID"),
    ).distinct()

    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 7: Form8865AllocationSummary (SQL lines 925-940)
# ---------------------------------------------------------------------------

def write_form8865_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into Form8865AllocationSummary with tracking key + SchID.

    SQL lines: 925-940
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "Form8865AllocationSummary"

    aos = tables["allocation_output_summary"]
    tk_expr = _tracking_key_expr(cfg["is_tracking_key"], entity_id)

    df = aos.filter(F.col("LineTypeID") == cfg["form8865_line_type_id"]).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("EntityID").alias("SourceEntityID"),
        F.col("QuicklinkID").alias("Form8865ID"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.lit(None).cast("string").alias("TextValue"),
        ns0(F.col("SchID")).alias("SchID"),
        F.col("ParentEntityId").alias("ParentEntityID"),
        tk_expr.alias("TrackingKey"),
        F.col("OriginalParentEntityID"),
    )

    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 8: Form199AAllocationSummary (SQL lines 940-960)
# ---------------------------------------------------------------------------

def write_form199a_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into Form199AAllocationSummary with tracking key.

    SQL lines: 940-960
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "Form199AAllocationSummary"

    aos = tables["allocation_output_summary"]
    tk_expr = _tracking_key_expr(cfg["is_tracking_key"], entity_id)

    df = aos.filter(F.col("LineTypeID") == cfg["form199a_line_type_id"]).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("EntityID").alias("SourceEntityID"),
        F.col("QuicklinkID").alias("Form199AID"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.lit(None).cast("string").alias("TextValue"),
        F.col("ParentEntityId"),
        tk_expr.alias("TrackingKey"),
        F.col("OriginalParentEntityID"),
    ).distinct()

    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 9: Build PFIC reclass data (SQL lines 960-1280)
# ---------------------------------------------------------------------------

def build_pfic_reclass_data(spark: SparkSession, cfg: dict, tables: dict) -> DataFrame:
    """Build the PFIC reclass temp table with non-allocable lines, reclass data,
    Part5/7 updates, Election D logic, and Domestic Blocker filtering.

    SQL lines: 960-1280
    Returns the final #TempReclassPFICFootnoteAllocationSummary equivalent DataFrame.
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    pfic_lt_id = cfg["pfic_footnote_line_type_id"]

    aos = tables["allocation_output_summary"]
    k1_workflow = tables["k1_workflow"]

    fcr_txn_id = cfg.get("foreign_currency_rate_txn_id")

    # #TempPFICAllocationOutputSummary — distinct EntityID, QuicklinkID, TrackingKey from AOS
    temp_pfic_aos = aos.filter(F.col("LineTypeID") == pfic_lt_id).select(
        F.col("EntityID"), F.col("QuicklinkID"), F.col("TrackingKey")
    ).distinct()

    # Existing PFICFootnoteAllocationText for this run (read from write schema —
    # SQL reads from the same table it inserts into, which starts empty for a fresh run)
    existing_pfic_text = spark.table(tbl_name("PFICFootnoteAllocationText", cfg)).filter(
        F.col("RunID") == run_id
    ).select("PFICFootnoteID", "LineID")

    # Part 1: Non-allocable lines from PFICFootnoteInput_Snapshot
    pfic_input = read_table(spark, "PFICFootnoteInput_Snapshot", cfg)
    pfic_line_item = tables["pfic_line_item"]

    # Build excluded line IDs.
    # SQL: AND FL.LineID NOT IN(@CumulativeQEFLineID, @CumulativeQEFDistributionsLineID, @ReversalPYQEFLineID)
    # SQL Server NOT IN with any NULL argument evaluates to UNKNOWN for every row, meaning NO rows
    # are excluded when any variable is NULL. We must replicate: only apply the exclusion when ALL
    # three IDs are known; if any is None, skip the filter entirely (same as SQL's NULL behaviour).
    _raw_excluded_ids = [
        cfg["cumulative_qef_line_id"],
        cfg["cumulative_qef_distributions_line_id"],
        cfg["reversal_py_qef_line_id"],
    ]
    if any(x is None for x in _raw_excluded_ids):
        excl_line_filter = F.lit(True)
    else:
        excl_line_filter = ~F.col("LineID").isin(_raw_excluded_ids)

    # Join chain for non-allocable PFIC lines
    entity_tbl = tables["entity_tbl"]
    fx_rates = read_table(spark, "ForeignCurrencyAverageRate", cfg)

    non_alloc_pfic = (
        pfic_input.alias("PFIC")
        .join(
            F.broadcast(pfic_line_item.filter(
                (F.coalesce(F.col("IsAllocated"), F.lit(False)) == False)
                & excl_line_filter
            ).select("LineID", "ClientID", "TaxPeriodID")).alias("FL"),
            (F.col("PFIC.LineID") == F.col("FL.LineID"))
            & (F.col("FL.ClientID") == F.col("PFIC.ClientID"))
            & (F.col("FL.TaxPeriodID") == F.col("PFIC.TaxPeriodID")),
            "inner"
        )
        .join(
            k1_workflow.alias("KW"),
            F.col("PFIC.WorkflowID") == F.col("KW.WorkFlowID"),
            "inner"
        )
        .join(
            entity_tbl.alias("E"),
            F.col("E.EntityID") == F.col("KW.EntityID"),
            "inner"
        )
        .join(
            temp_pfic_aos.alias("AO"),
            (F.col("AO.EntityID") == F.col("E.EntityID"))
            & (F.col("AO.QuicklinkID") == F.col("PFIC.PFICFootnoteID")),
            "inner"
        )
        .join(
            fx_rates.alias("R"),
            (F.col("FL.ClientID") == F.col("R.ClientID"))
            & (F.col("R.CurrencyCode") == F.col("E.CurrencyCode"))
            & (F.col("R.TransactionID") == F.lit(fcr_txn_id).cast("int")),
            "left"
        )
        .join(
            existing_pfic_text.alias("P"),
            (F.col("P.PFICFootnoteID") == F.col("PFIC.PFICFootnoteID"))
            & (F.col("P.LineID") == F.col("FL.LineID")),
            "left_anti"
        )
    )

    cfc_income_id = cfg["cfc_tested_income_line_id"]
    cfc_loss_id = cfg["cfc_tested_loss_line_id"]
    # SQL: PFIC.LineID IN (@CFCTestedIncome,@CFCTestedLoss)
    # SQL Server treats NULL in IN(...) as UNKNOWN — never matches. Filter out None values
    # to prevent Spark's isin([None,...]) from matching LineID IS NULL rows.
    _cfc_ids = [x for x in [cfc_income_id, cfc_loss_id] if x is not None]
    _cfc_condition = F.col("PFIC.LineID").isin(_cfc_ids) if _cfc_ids else F.lit(False)

    non_alloc_result = non_alloc_pfic.select(
        F.col("KW.EntityID").alias("SourceEntityID"),
        F.col("PFIC.PFICFootnoteID").alias("PFICFootnoteID"),
        F.col("PFIC.LineID").alias("LineID"),
        F.when(
            _cfc_condition,
            ns0(F.col("PFIC.Amount")) / F.coalesce(F.col("R.AverageRate"), F.lit(1))
        ).otherwise(ns0(F.col("PFIC.Amount"))).alias("Amount"),
        F.lit(None).cast("float").alias("FlowupAmount"),
        F.col("PFIC.TextValue"),
        F.lit(0).alias("ParentEntityId"),
        F.concat(
            F.col("KW.EntityID").cast("string"), F.lit("~"), F.lit(entity_id).cast("string")
        ).alias("TrackingKey"),
        F.lit(None).cast("int").alias("OriginalParentEntityID"),
    )

    # Part 2: Reclass from ReclassFootnoteAllocationData
    reclass_data = tables["reclass_footnote_data"].filter(
        F.col("LineTypeID") == pfic_lt_id
    )
    pfic_package = read_table(spark, "PFICFootnotePackage", cfg)
    k1_package = tables["k1_package"]

    reclass_pfic = (
        reclass_data.alias("PFIC")
        .join(
            F.broadcast(pfic_line_item.filter(
                F.coalesce(F.col("IsAllocated"), F.lit(False)) == False
            ).select("LineID", "ClientID", "TaxPeriodID")).alias("FL"),
            (F.col("PFIC.LineID") == F.col("FL.LineID"))
            & (F.col("FL.ClientID") == F.col("PFIC.ClientID"))
            & (F.col("FL.TaxPeriodID") == F.col("PFIC.TaxPeriodID")),
            "inner"
        )
        .join(
            pfic_package.alias("P"),
            F.col("P.PFICFootnoteID") == F.col("PFIC.FootnoteID"),
            "inner"
        )
        .join(
            k1_package.alias("K"),
            F.col("K.K1PackageID") == F.col("P.K1PackageID"),
            "inner"
        )
        .join(
            temp_pfic_aos.alias("AO"),
            (F.col("AO.QuicklinkID") == F.col("PFIC.FootnoteID"))
            & (F.col("AO.EntityID") == F.col("PFIC.SourceEntityID"))
            & (F.col("AO.TrackingKey") == F.col("PFIC.TrackingKey")),
            "inner"
        )
    ).filter(
        (F.col("FL.ClientID") == client_id)
        & (F.col("FL.TaxPeriodID") == tax_period_id)
    )

    tk_expr_reclass = F.when(
        F.lit(cfg["is_tracking_key"] == "C"),
        F.when(
            F.col("PFIC.TrackingKey").isNull(),
            F.col("PFIC.EntityID").cast("string")
        ).otherwise(
            F.concat(F.col("PFIC.TrackingKey"), F.lit("~"), F.lit(entity_id).cast("string"))
        )
    ).otherwise(F.lit(None).cast("string"))

    reclass_result = reclass_pfic.select(
        F.col("PFIC.SourceEntityID").alias("SourceEntityID"),
        F.col("PFIC.FootnoteID").alias("PFICFootnoteID"),
        F.col("PFIC.LineID").alias("LineID"),
        F.col("PFIC.Amount"),
        F.col("PFIC.FlowupAmount"),
        F.col("PFIC.TextValue"),
        F.when(
            ns0(F.col("PFIC.ParentEntityId")) == 0,
            F.when(
                F.col("PFIC.LTEntityID") == F.col("K.LowerTierEntityID"), F.lit(0)
            ).otherwise(F.col("PFIC.LTEntityID"))
        ).otherwise(F.col("PFIC.ParentEntityId")).alias("ParentEntityId"),
        tk_expr_reclass.alias("TrackingKey"),
        F.col("PFIC.OriginalParentEntityID"),
    ).distinct()

    # Union non-allocable + reclass
    temp_reclass = non_alloc_result.unionByName(reclass_result)

    # Part 3: Update Part 5 and 7 values from PFICfootnoteflowup
    pfic_flowup = read_table(spark, "PFICfootnoteflowup", cfg).filter(
        F.col("RunID") == run_id
    )
    # SQL: INNER JOIN PFICFootnoteLineItem L ON L.ShortName IN ('IsPart_7','IsPart_5')
    # has NO ClientID/TaxPeriodID filter — use the shared global PFICFootnoteLineItem read.
    pfic_line_item_all = tables["pfic_line_item_all"]
    part_5_7_lines = pfic_line_item_all.filter(
        F.col("ShortName").isin(["IsPart_7", "IsPart_5"])
    ).select("LineID").collect()
    part_5_7_ids = [r["LineID"] for r in part_5_7_lines]

    if part_5_7_ids:
        # Replace rows in temp_reclass that match Part 5/7 lines with updated text
        temp_reclass_no_update = temp_reclass.filter(~F.col("LineID").isin(part_5_7_ids))
        temp_reclass_update_target = temp_reclass.filter(F.col("LineID").isin(part_5_7_ids))

        # SQL UPDATE sets PFA.textvalue = PF.textvalue unconditionally when a PF row matches,
        # including when PF.textvalue IS NULL (which overwrites T.TextValue with NULL).
        # coalesce() would incorrectly fall back to T.TextValue in that case, so we use
        # when/otherwise keyed on whether the LEFT JOIN produced a PF match.
        # No .distinct() here: SQL UPDATE does not deduplicate rows — rows that differ in
        # non-join columns (e.g. Amount) must remain separate after the textvalue update.
        # PFICfootnoteflowup has no unique constraint on (PFICFootnoteID, LineID, SourceEntityID)
        # so duplicates can exist; SQL's UPDATE acts in-place (one result per PFA row), but a
        # LEFT JOIN would multiply rows when multiple PF rows match the same T row. Deduplicate
        # PF on the join keys so the LEFT JOIN behaves like the SQL UPDATE (one match per T row).
        pfic_flowup_deduped = pfic_flowup.dropDuplicates(["PFICFootnoteID", "lineid", "SourceEntityID"])
        updated_rows = (
            temp_reclass_update_target.alias("T")
            .join(
                pfic_flowup_deduped.alias("PF"),
                (F.col("PF.PFICFootnoteID") == F.col("T.PFICFootnoteID"))
                & (F.col("PF.lineid") == F.col("T.LineID"))
                & (F.col("PF.SourceEntityID") == F.col("T.SourceEntityID")),
                "left"
            )
            .select(
                F.col("T.SourceEntityID"),
                F.col("T.PFICFootnoteID"),
                F.col("T.LineID"),
                F.col("T.Amount"),
                F.col("T.FlowupAmount"),
                F.when(F.col("PF.PFICFootnoteID").isNotNull(), F.col("PF.textvalue"))
                 .otherwise(F.col("T.TextValue")).alias("TextValue"),
                F.col("T.ParentEntityId"),
                F.col("T.TrackingKey"),
                F.col("T.OriginalParentEntityID"),
            )
        )
        temp_reclass = temp_reclass_no_update.unionByName(updated_rows)

    # Part 4: Auto Election D logic
    if cfg["is_auto_elec_d_enabled"] and cfg["election_d_line_id"]:
        elec_d_line_id = cfg["election_d_line_id"]
        pfic_flowup_tk = read_table(spark, "PFICFootnoteFlowupWithTrackingKey", cfg).filter(
            (F.col("RunID") == run_id) & (F.col("LineID") == elec_d_line_id)
        )

        # Update textvalue where matching
        temp_reclass_elec_d = temp_reclass.filter(F.col("LineID") == elec_d_line_id)
        temp_reclass_not_elec_d = temp_reclass.filter(F.col("LineID") != elec_d_line_id)

        # SQL UPDATE sets PFA.textvalue = PF.textvalue unconditionally when a PF row matches,
        # including when PF.textvalue IS NULL. Use when/otherwise keyed on LEFT JOIN match
        # so NULL PF.TextValue overwrites PFA.TextValue (not coalesce which would preserve it).
        # No .distinct(): SQL UPDATE does not deduplicate — rows differing in Amount or other
        # non-join columns must remain separate even after the textvalue is updated.
        updated_elec_d = (
            temp_reclass_elec_d.alias("PFA")
            .join(
                pfic_flowup_tk.alias("PF"),
                (ns(F.col("PFA.PFICFootnoteID").cast("string")) == ns(F.col("PF.PFICFootnoteID").cast("string")))
                & (ns(F.col("PFA.SourceEntityID").cast("string")) == ns(F.col("PF.SourceEntityID").cast("string")))
                & (F.col("PFA.LineID") == F.col("PF.LineID"))
                & (ns(F.col("PFA.TrackingKey")) == ns(F.col("PF.TrackingKey"))),
                "left"
            )
            .select(
                F.col("PFA.SourceEntityID"),
                F.col("PFA.PFICFootnoteID"),
                F.col("PFA.LineID"),
                F.col("PFA.Amount"),
                F.col("PFA.FlowupAmount"),
                F.when(F.col("PF.LineID").isNotNull(), F.col("PF.TextValue"))
                 .otherwise(F.col("PFA.TextValue")).alias("TextValue"),
                F.col("PFA.ParentEntityId"),
                F.col("PFA.TrackingKey"),
                F.col("PFA.OriginalParentEntityID"),
            )
        )

        # Insert missing Election D rows from PFICFootnoteFlowupWithTrackingKey.
        # SQL: FROM temp_reclass PF JOIN pfic_flowup_tk PFA ON ISNULL equijoin,
        # LEFT JOIN temp_reclass TPF ON PF.*=TPF.* (no ISNULL for TrackingKey),
        # WHERE TPF.LineID IS NULL.
        # Since PF IS a row in temp_reclass, the LEFT JOIN fails only when
        # PF.TrackingKey IS NULL (NULL != NULL). The ISNULL equijoin then allows
        # PFA.TrackingKey to be NULL or empty string ('').
        # Result: inserts matching PFA rows where PF.TrackingKey is NULL and
        # ISNULL(PFA.TrackingKey,'') = ''.
        temp_reclass_null_tk = temp_reclass.filter(F.col("TrackingKey").isNull())

        missing_elec_d = (
            pfic_flowup_tk.alias("PFA")
            .join(
                temp_reclass_null_tk.alias("PF"),
                (F.col("PFA.PFICFootnoteID") == F.col("PF.PFICFootnoteID"))
                & (F.coalesce(F.col("PFA.SourceEntityID").cast("string"), F.lit("")) ==
                   F.coalesce(F.col("PF.SourceEntityID").cast("string"), F.lit("")))
                & (F.col("PFA.LineID") == F.col("PF.LineID")),
                "inner"
            )
            .filter(
                F.coalesce(F.col("PFA.TrackingKey"), F.lit(""))
                == F.coalesce(F.col("PF.TrackingKey"), F.lit(""))
            )
            .select(
                F.col("PFA.SourceEntityID"),
                F.col("PFA.PFICFootnoteID"),
                F.col("PFA.LineID"),
                F.lit(None).cast("float").alias("Amount"),
                F.lit(None).cast("float").alias("FlowupAmount"),
                F.col("PFA.TextValue"),
                F.lit(0).alias("ParentEntityId"),
                F.col("PFA.TrackingKey"),
                F.lit(None).cast("int").alias("OriginalParentEntityID"),
            )
        )

        temp_reclass = temp_reclass_not_elec_d.unionByName(updated_elec_d).unionByName(missing_elec_d)

    # Part 5: Domestic Blocker filtering
    if cfg["is_domestic_blocker"]:
        type_of_pfic_line_id = cfg["type_of_pfic_line_id"]

        # IDs to delete: PFICFootnoteIDs where SourceEntityID != local entity
        delete_ids = temp_reclass.filter(
            F.col("SourceEntityID") != entity_id
        ).select("PFICFootnoteID").distinct()

        # IDs NOT to delete: where SourceEntityID == local entity AND TypeOfPFIC line
        # has a value other than the two allowed PFIC types.
        # SQL: D.TEXTVALUE NOT IN ('IS1293ELIGIBLEDEEMED','IS1291ANYDISTRIBUTION')
        # SQL Server collation is typically CI so matching exact uppercase constants.
        non_delete_ids = temp_reclass.filter(
            (F.col("SourceEntityID") == entity_id)
            & (F.col("LineID") == type_of_pfic_line_id)
            & (~F.col("TextValue").isin("IS1293ELIGIBLEDEEMED", "IS1291ANYDISTRIBUTION"))
        ).select("PFICFootnoteID").distinct()

        # Delete from temp_reclass where PFICFootnoteID in delete_ids AND NOT in non_delete_ids
        ids_to_remove = delete_ids.join(non_delete_ids, "PFICFootnoteID", "left_anti")
        temp_reclass = temp_reclass.join(ids_to_remove, "PFICFootnoteID", "left_anti")

    return temp_reclass


# ---------------------------------------------------------------------------
# Section 10: PFICFootnoteAllocationSummary (SQL lines 1080-1120)
# ---------------------------------------------------------------------------

def write_pfic_footnote_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into PFICFootnoteAllocationSummary from AllocationOutputSummary.

    SQL lines: 1080-1120
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    pfic_lt_id = cfg["pfic_footnote_line_type_id"]
    target_table = "PFICFootnoteAllocationSummary"

    aos = tables["allocation_output_summary"]
    tk_expr = _tracking_key_expr(cfg["is_tracking_key"], entity_id)

    df = aos.filter(F.col("LineTypeID") == pfic_lt_id).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("EntityID").alias("SourceEntityID"),
        F.col("QuicklinkID").alias("PFICFootnoteID"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.lit(None).cast("string").alias("TextValue"),
        F.col("ParentEntityId"),
        tk_expr.alias("TrackingKey"),
        F.col("OriginalParentEntityID"),
    ).distinct()

    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 11: PFICFootnoteAllocationText (SQL lines 1280-1370)
# ---------------------------------------------------------------------------

def write_pfic_footnote_allocation_text(spark: SparkSession, cfg: dict, pfic_reclass_df: DataFrame) -> None:
    """Insert into PFICFootnoteAllocationText from the PFIC reclass temp DataFrame.

    SQL lines: 1280-1370
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "PFICFootnoteAllocationText"

    df = pfic_reclass_df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("SourceEntityID"),
        F.col("PFICFootnoteID"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("TextValue"),
        F.col("ParentEntityId"),
        F.col("TrackingKey"),
        F.col("OriginalParentEntityID"),
    ).distinct()

    # Materialize: parallel write to the same table causes a read-own-writes race.
    df = checkpoint(spark, df, "pfic_alloc_text", cfg)
    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 12: Build custom footnote transactions (SQL lines 1380-1490)
# Inlines: udfGetLatestCustomFootnoteTransactionIDs, udfGetPhaseID, udfGetLastTransactionID_Phase
# ---------------------------------------------------------------------------

def build_custom_footnote_transactions(spark: SparkSession, cfg: dict, tables: dict) -> dict:
    """Inline udfGetLatestCustomFootnoteTransactionIDs: build custom footnote
    transaction IDs, line items, and footnote IDs.

    SQL lines: 1380-1490
    Returns dict with DataFrames: custom_footnote_txns, custom_footnote_line_items, custom_footnote_ids
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    phase_id = cfg.get("phase_id")

    aos = tables["allocation_output_summary"]

    # Inline udfGetPhaseID if phase_id is None
    if phase_id is None:
        phase_row = read_table(spark, "Phase", cfg).filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
            & (F.col("EndDate").isNull())
        ).select("PhaseID").first()
        phase_id = phase_row["PhaseID"] if phase_row else None

    # Build entity list: just the local entity + its lower-tier investments
    entity_tbl = tables["entity_tbl"]
    er_tbl = read_table(spark, "EntityRelationship", cfg)

    # Entity list = local entity + lower-tier investments.
    # The UDF looks up InvestmentTypeID with ClientID only (no TaxPeriodID):
    #   SELECT EntityTypeID FROM ENU_EntityType WHERE EntityTypeName='Investment' AND ClientID=@LocalClientID
    # We intentionally use a ClientID-only lookup here to match the UDF exactly.
    enu_et_row = read_table(spark, "ENU_EntityType", cfg).filter(
        (F.col("ClientID") == client_id) & (F.col("EntityTypeName") == "Investment")
    ).select("EntityTypeID").first()
    udf_inv_entity_type_id = enu_et_row["EntityTypeID"] if enu_et_row else None

    lower_tier_entities = (
        er_tbl.filter(F.col("UpperTierEntityID") == entity_id)
        .join(
            entity_tbl.filter(F.col("FundOrInvestmentID") == udf_inv_entity_type_id),
            er_tbl["LowerTierEntityID"] == entity_tbl["EntityID"],
            "inner"
        )
        .select(er_tbl["LowerTierEntityID"].alias("EntityID"))
    )
    tmp_entity = spark.createDataFrame([(entity_id,)], ["EntityID"]).unionByName(lower_tier_entities)

    # Build custom footnote event IDs
    custom_import_detail = read_table(spark, "CustomImportDetail", cfg).filter(
        F.col("IsCustomFootnote") == True
    )
    # SQL UDF joins ENU_Event and ENU_LineType with NO ClientID/TaxPeriodID filter — global tables.
    enu_event = read_table(spark, "ENU_Event", cfg)
    enu_line_type = read_table(spark, "ENU_LineType", cfg)

    custom_footnote_events = (
        custom_import_detail.alias("CD")
        .join(F.broadcast(enu_event).alias("EE"), F.col("CD.ImportName") == F.col("EE.EventName"), "inner")
        .join(F.broadcast(enu_line_type).alias("EL"), F.col("EL.LineType") == F.col("CD.ImportName"), "inner")
        .select(
            F.col("EE.EventTypeID").alias("CustomFootnoteEventTypeID"),
            F.col("EL.LineTypeID"),
            F.col("CD.GlobalMenuID").alias("RegisterTypeID"),
        )
    )

    # Build the custom footnote transactions with the same latest-transaction pruning as
    # dbo.udfGetLatestCustomFootnoteTransactionIDs + dbo.udfGetLastTransactionID_Phase.
    k1_package = tables["k1_package"]
    eligible_entities = (
        tmp_entity.alias("E")
        .join(k1_package.alias("K"), F.col("E.EntityID") == F.col("K.UpperTierEntityID"), "inner")
        .select(
            F.col("E.EntityID").alias("EntityID"),
            ns0(F.col("K.K1PackageID")).alias("K1PackageID"),
        )
        .distinct()
    )

    entityless_event_names = [
        "Import_EntityRelationship",
        "Import_Historic",
        "Import_MasterTaxableIncome",
        "DataFeed_ByEntityInvestment",
        "DataFeed_Entities",
        "DataFeed_Deals-Specific",
        "DataFeed_Investors-Specific",
        "DataFeed_Investors",
        "DataFeed_Deals",
        "DataFeed_Chart of Accounts",
        "DataFeed_Financial",
        "Import_CompositeWithholdingBridge",
        "Import_WHPaymentAllocation",
        "Import_EntityConfiguration",
    ]
    entityless_event_ids = enu_event.filter(
        F.col("EventName").isin(entityless_event_names)
    ).select("EventTypeID").distinct()

    custom_footnote_events = (
        custom_footnote_events.alias("TF")
        .join(
            entityless_event_ids.alias("EE"),
            F.col("TF.CustomFootnoteEventTypeID") == F.col("EE.EventTypeID"),
            "left",
        )
        .select(
            F.col("TF.CustomFootnoteEventTypeID"),
            F.col("TF.LineTypeID"),
            F.col("TF.RegisterTypeID"),
            F.when(F.col("EE.EventTypeID").isNotNull(), F.lit(0)).otherwise(F.lit(1)).alias("UseEntityID"),
        )
        .distinct()
    )

    if phase_id is None:
        custom_footnote_txns = spark.createDataFrame([], "EntityID INT, TransactionID INT, LineTypeID INT, EventTypeid INT, RegisterTypeID INT, K1PackageID INT")
    else:
        excluded_status_ids = read_table(spark, "WorkflowStatus", cfg).filter(
            F.col("EnumerationName").isin(["Rejected", "Err_Critical", "Err_NonCritical"])
        ).select("StatusID").distinct()

        filtered_transaction_log = (
            read_table(spark, "TransactionLog", cfg)
            .filter(
                (F.col("ClientID") == client_id)
                & (F.col("TaxPeriodID") == tax_period_id)
                & (F.col("PhaseID") == phase_id)
                & (F.col("StatusID") != 0)
            )
            .join(excluded_status_ids, "StatusID", "left_anti")
        )

        global_latest_txn = filtered_transaction_log.groupBy("EventTypeID").agg(
            F.max("TransactionID").alias("TransactionID")
        )
        entity_latest_txn = filtered_transaction_log.groupBy("EventTypeID", "EntityID").agg(
            F.max("TransactionID").alias("TransactionID")
        )

        scoped_entity_txns = (
            eligible_entities.alias("E")
            .crossJoin(F.broadcast(custom_footnote_events.filter(F.col("UseEntityID") == 1).alias("TF")))
            .join(
                entity_latest_txn.alias("TL"),
                (F.col("TL.EventTypeID") == F.col("TF.CustomFootnoteEventTypeID"))
                & (F.col("TL.EntityID") == F.col("E.EntityID")),
                "left",
            )
            .select(
                F.col("E.EntityID"),
                F.col("TL.TransactionID"),
                F.col("TF.LineTypeID"),
                F.col("TF.CustomFootnoteEventTypeID").alias("EventTypeid"),
                F.col("TF.RegisterTypeID"),
                F.col("E.K1PackageID"),
            )
        )

        unscoped_entity_txns = (
            eligible_entities.alias("E")
            .crossJoin(F.broadcast(custom_footnote_events.filter(F.col("UseEntityID") == 0).alias("TF")))
            .join(
                global_latest_txn.alias("TL"),
                F.col("TL.EventTypeID") == F.col("TF.CustomFootnoteEventTypeID"),
                "left",
            )
            .select(
                F.col("E.EntityID"),
                F.col("TL.TransactionID"),
                F.col("TF.LineTypeID"),
                F.col("TF.CustomFootnoteEventTypeID").alias("EventTypeid"),
                F.col("TF.RegisterTypeID"),
                F.col("E.K1PackageID"),
            )
        )

        custom_footnote_txns = scoped_entity_txns.unionByName(unscoped_entity_txns).filter(
            F.col("TransactionID").isNotNull()
        )

    # Get distinct LineTypeIDs — must be derived from the entity+K1Package-joined result,
    # matching SQL's #tmpCustomFootnoteLineTypes which is populated from the UDF output
    # after NULL TransactionIDs are deleted.
    custom_footnote_line_types = custom_footnote_txns.select("LineTypeID").distinct()

    # Get CustomFootnoteLineItems (non-allocable)
    custom_footnote_line_items = read_table(spark, "CustomFootnoteLineItem", cfg).filter(
        (F.col("ClientID") == client_id)
        & (F.col("TaxPeriodID") == tax_period_id)
        & (F.coalesce(F.col("IsAllocable"), F.lit(False)) == False)
    ).select("LineID", "IsActive", "LineDataType", "IsAllocable", "ClientID", "TaxPeriodID")

    # Add the -1 LineID row for entity ID text
    extra_row = spark.createDataFrame(
        [(-1, True, "TEXT", False, client_id, tax_period_id)],
        ["LineID", "IsActive", "LineDataType", "IsAllocable", "ClientID", "TaxPeriodID"]
    )
    custom_footnote_line_items = custom_footnote_line_items.unionByName(extra_row)

    # Get CustomFootnoteIDs from AllocationOutputSummary.
    # SQL alignment:
    #   SELECT DISTINCT QuicklinkID
    #   FROM #AllocationOutputSummary AO
    #   INNER JOIN #tmpCustomFootnoteLineTypes CFT ON CFT.LineTypeID = AO.LineTypeID
    custom_footnote_ids = (
        aos.join(F.broadcast(custom_footnote_line_types), "LineTypeID", "inner")
        .select(F.col("QuicklinkID").alias("CustomFootnoteID"))
        .distinct()
    )

    return {
        "custom_footnote_txns": custom_footnote_txns,
        "custom_footnote_line_types": custom_footnote_line_types,
        "custom_footnote_line_items": custom_footnote_line_items,
        "custom_footnote_ids": custom_footnote_ids,
    }


# ---------------------------------------------------------------------------
# Section 13: CustomFootnoteAllocationSummary (SQL lines 1490-1600)
# ---------------------------------------------------------------------------

def write_custom_footnote_allocation_summary(
    spark: SparkSession, cfg: dict, tables: dict, cf_data: dict, partner_pfic: DataFrame
) -> None:
    """Insert into CustomFootnoteAllocationSummary: non-allocable + allocated.

    SQL lines: 1490-1600
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "CustomFootnoteAllocationSummary"

    custom_footnote_ids = cf_data["custom_footnote_ids"]
    custom_footnote_line_items = cf_data["custom_footnote_line_items"]
    custom_footnote_line_types = cf_data["custom_footnote_line_types"]

    # Non-allocable: from CustomFootnoteFlowup joined with line items and footnote IDs
    cf_flowup = read_table(spark, "CustomFootnoteFlowup", cfg).filter(
        F.col("RunID") == run_id
    )

    non_alloc_cf = (
        cf_flowup.alias("CFI")
        .join(F.broadcast(custom_footnote_line_items.alias("CFL")),
              F.col("CFI.LineID") == F.col("CFL.LineID"), "inner")
        .join(F.broadcast(custom_footnote_ids.alias("CF")),
              F.col("CFI.CustomFootnoteID") == F.col("CF.CustomFootnoteID"), "inner")
        .crossJoin(partner_pfic.alias("AO"))
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("AO.ShareClass"),
            F.col("AO.PartnerNumber"),
            F.col("CFI.SourceEntityID"),
            F.col("CFI.CustomFootnoteID"),
            F.col("CFI.LineID"),
            F.col("CFI.LineTypeID"),
            F.col("CFI.Amount"),
            F.col("CFI.Amount").alias("FlowupAmount"),
            F.col("CFI.TextValue"),
            F.lit(0).alias("ParentEntityId"),
            F.lit(entity_id).cast("string").alias("TrackingKey"),
        )
    )

    _store_result_table(spark, cfg, target_table, non_alloc_cf)

    # Allocated: from AllocationOutputSummary joined with custom footnote line types
    aos = tables["allocation_output_summary"]
    tk_expr = _tracking_key_expr(cfg["is_tracking_key"], entity_id)

    alloc_cf = (
        aos.join(F.broadcast(custom_footnote_line_types), "LineTypeID", "inner")
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("ShareClass"),
            F.col("PartnerNumber"),
            F.col("EntityID").alias("SourceEntityID"),
            F.col("QuicklinkID").alias("CustomFootnoteID"),
            F.col("LineID"),
            F.col("LineTypeID"),
            F.col("Amount"),
            F.col("Amount").alias("FlowupAmount"),
            F.lit(None).cast("string").alias("TextValue"),
            F.col("ParentEntityId"),
            tk_expr.alias("TrackingKey"),
            F.col("OriginalParentEntityID"),
        ).distinct()
    )

    _store_result_table(spark, cfg, target_table, alloc_cf)


# ---------------------------------------------------------------------------
# Section 14: Line18AAllocationSummary (SQL lines 1600-1620)
# ---------------------------------------------------------------------------

def write_line18a_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into Line18AAllocationSummary.

    SQL lines: 1600-1620
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "Line18AAllocationSummary"

    aos = tables["allocation_output_summary"]
    df = aos.filter(F.col("LineTypeID") == cfg["line18a_line_type_id"]).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID").alias("LocationID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
    )
    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 15: UBTIAllocationSummary (SQL lines 1630-1650)
# ---------------------------------------------------------------------------

def write_ubti_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into UBTIAllocationSummary with UBTIType CASE expression.

    SQL lines: 1630-1650
    """
    if not cfg["allow_pe_book_inserts"]:
        return

    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "UBTIAllocationSummary"

    aos = tables["allocation_output_summary"]
    df = aos.filter(F.col("LineTypeID") == cfg["ubti_line_type_id"]).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.when(F.col("QuicklinkID") == 1, F.lit("Qualified"))
         .when(F.col("QuicklinkID") == 2, F.lit("Non-Qualified"))
         .alias("UBTIType"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
    )
    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 16: PassiveIncomeAllocationSummary (SQL lines 1650-1670)
# ---------------------------------------------------------------------------

def write_passive_income_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into PassiveIncomeAllocationSummary.

    SQL lines: 1650-1670
    """
    if not cfg["allow_pe_book_inserts"]:
        return

    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "PassiveIncomeAllocationSummary"

    aos = tables["allocation_output_summary"]
    df = aos.filter(F.col("LineTypeID") == cfg["pass_income_line_type_id"]).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
    )
    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 17: Form200616AllocationSummary (SQL lines 1670-1740)
# ---------------------------------------------------------------------------

def write_form200616_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into Form200616AllocationSummary: direct + LTF flowup.

    SQL lines: 1670-1740
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "Form200616AllocationSummary"

    k1_workflow = tables["k1_workflow"]
    lower_tier_funds = tables["lower_tier_funds"]

    # Build #AllocationPercentage
    alloc_pct = read_table(spark, "AllocationPercentage", cfg).filter(
        (F.col("RunID") == run_id) & (F.col("ClientID") == client_id)
    ).select(
        F.col("PartnerNumber"), ns(F.col("ShareClass")).alias("ShareClass")
    ).distinct()

    # Direct insert: Form200616_Snapshot x K1Workflow x AllocationPercentage
    form200616_snapshot = read_table(spark, "Form200616_Snapshot", cfg).filter(
        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
    )

    direct_df = (
        form200616_snapshot.alias("F2006")
        .join(k1_workflow.alias("KW"), F.col("F2006.WorkflowID") == F.col("KW.WorkFlowID"), "inner")
        .crossJoin(F.broadcast(alloc_pct.alias("AP")))
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("AP.ShareClass"),
            F.col("AP.PartnerNumber"),
            F.col("KW.EntityID").alias("SourceEntityID"),
            F.col("F2006.Form2006EntityID"),
        )
    )

    _store_result_table(spark, cfg, target_table, direct_df)

    # LTF flowup insert: from Form200616AllocationSummary itself
    existing_form200616 = spark.table(tbl_name("Form200616AllocationSummary", cfg)).filter(
        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
    )

    ltf_df = (
        existing_form200616.alias("F2006")
        .join(
            lower_tier_funds.alias("LT"),
            (F.col("F2006.RunID") == F.col("LT.RunID"))
            & (F.col("F2006.EntityID") == F.col("LT.EntityID"))
            & (F.col("F2006.PartnerNumber") == F.col("LT.PartnerNumber")),
            "inner"
        )
        .crossJoin(F.broadcast(alloc_pct.alias("AP")))
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("AP.ShareClass"),
            F.col("AP.PartnerNumber"),
            F.col("F2006.SourceEntityID"),
            F.col("F2006.Form2006EntityID"),
        ).distinct()
    )

    _store_result_table(spark, cfg, target_table, ltf_df)


# ---------------------------------------------------------------------------
# Section 18: Form8886AllocationSummary (SQL lines 1740-1810)
# ---------------------------------------------------------------------------

def write_form8886_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into Form8886AllocationSummary: direct + reclass.

    SQL lines: 1740-1810
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    form8886_lt_id = cfg["form8886_line_type_id"]
    target_table = "Form8886AllocationSummary"

    aos = tables["allocation_output_summary"]
    k1_workflow = tables["k1_workflow"]
    tk_expr = _tracking_key_expr(cfg["is_tracking_key"], entity_id, alias="AO")

    # Direct insert
    form8886_input = read_table(spark, "Form8886Input_Snapshot", cfg)
    form8886_line_item = read_table(spark, "Form8886LineItem", cfg).filter(
        F.coalesce(F.col("IsActive"), F.lit(False)) == True
    )

    direct_df = (
        aos.filter(F.col("LineTypeID") == form8886_lt_id).alias("AO")
        .join(
            form8886_input.select(
                F.col("Form8886ID"), F.col("LineID").alias("F8886_LineID"),
                F.col("WorkflowID"), F.col("TransactionName"),
                F.col("EntityID").alias("F8886_EntityID"),
                F.col("Comments"), F.col("SecIIComments"),
            ).alias("F8886"),
            (F.col("AO.QuicklinkID") == F.col("F8886.Form8886ID"))
            & (F.col("AO.LineID") == F.col("F8886.F8886_LineID")),
            "inner"
        )
        .join(k1_workflow.select(
            F.col("WorkFlowID"), F.col("EntityID").alias("KW_EntityID")
        ).alias("KW"), F.col("F8886.WorkflowID") == F.col("KW.WorkFlowID"), "inner")
        .join(F.broadcast(form8886_line_item.select("LineID").alias("F")), F.col("F.LineID") == F.col("F8886.F8886_LineID"), "inner")
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("AO.ShareClass"),
            F.col("AO.PartnerNumber"),
            F.col("KW.KW_EntityID").alias("SourceEntityID"),
            F.col("AO.QuicklinkID").alias("Form8886ID"),
            F.col("AO.LineID"),
            F.col("AO.Amount"),
            F.col("AO.Amount").alias("FlowupAmount"),
            F.lit(None).cast("string").alias("TextValue"),
            F.col("F8886.TransactionName"),
            F.col("F8886.F8886_EntityID").alias("TransactionEntityID"),
            F.substring(F.col("F8886.Comments"), 1, 1000).alias("Comments"),
            F.substring(F.col("F8886.SecIIComments"), 1, 1000).alias("SecIIComments"),
            F.col("AO.ParentEntityId"),
            tk_expr.alias("TrackingKey"),
            F.col("AO.OriginalParentEntityID"),
        )
    )

    _store_result_table(spark, cfg, target_table, direct_df)

    # Reclass insert
    reclass_data = tables["reclass_footnote_data"].filter(
        F.col("LineTypeID") == form8886_lt_id
    )

    reclass_f8886 = (
        reclass_data
        .select(
            F.col("SourceEntityID"),
            F.col("FootnoteID").alias("Form8886ID"),
            F.col("TransactionName"),
            F.col("TransactionEntityID"),
            F.col("LineID"),
            F.col("Comments"),
            F.col("SecIIComments"),
        ).distinct()
    )

    reclass_df = (
        aos.filter(F.col("LineTypeID") == form8886_lt_id).alias("AO")
        .join(F.broadcast(form8886_line_item.select("LineID").alias("F")), F.col("F.LineID") == F.col("AO.LineID"), "inner")
        .join(
            reclass_f8886.alias("F8886"),
            (F.col("AO.QuicklinkID") == F.col("F8886.Form8886ID"))
            & (F.col("AO.LineID") == F.col("F8886.LineID")),
            "inner"
        )
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("AO.ShareClass"),
            F.col("AO.PartnerNumber"),
            F.col("F8886.SourceEntityID"),
            F.col("AO.QuicklinkID").alias("Form8886ID"),
            F.col("AO.LineID"),
            F.col("AO.Amount"),
            F.col("AO.Amount").alias("FlowupAmount"),
            F.lit(None).cast("string").alias("TextValue"),
            F.col("F8886.TransactionName"),
            F.col("F8886.TransactionEntityID"),
            F.substring(F.col("F8886.Comments"), 1, 1000).alias("Comments"),
            F.substring(F.col("F8886.SecIIComments"), 1, 1000).alias("SecIIComments"),
            F.col("AO.ParentEntityId"),
            tk_expr.alias("TrackingKey"),
            F.col("AO.OriginalParentEntityID"),
        ).distinct()
    )

    _store_result_table(spark, cfg, target_table, reclass_df)


# ---------------------------------------------------------------------------
# Section 19: AtRiskAllocationSummary (SQL lines 1810-1840)
# ---------------------------------------------------------------------------

def write_at_risk_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into AtRiskAllocationSummary (conditional on data existence).

    SQL lines: 1810-1840
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    at_risk_lt_id = cfg["at_risk_line_type_id"]
    target_table = "AtRiskAllocationSummary"

    aos = tables["allocation_output_summary"]
    tk_expr = _tracking_key_expr(cfg["is_tracking_key"], entity_id)

    df = aos.filter(F.col("LineTypeID") == at_risk_lt_id).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("EntityID").alias("SourceEntityID"),
        F.col("QuicklinkID").alias("AtRiskID"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("OriginalParentEntityID"),
        F.col("ParentEntityID").alias("ParentEntityId"),
        tk_expr.alias("TrackingKey"),
    ).distinct()

    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 20: GAAPToTaxAllocation (SQL lines 1840-1860)
# ---------------------------------------------------------------------------

def write_gaap_to_tax_allocation(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into GAAPToTaxAllocation.

    SQL lines: 1840-1860
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "GAAPToTaxAllocation"

    aos = tables["allocation_output_summary"]
    df = aos.filter(
        (F.col("EntityID") == entity_id)
        & (F.col("LineTypeID") == cfg["gaap_to_tax_line_type_id"])
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("int").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.lit(entity_id).cast("int").alias("EntityID"),
        F.col("PartnerNumber"),
        F.col("ShareClass"),
        F.col("LineID").cast("int").alias("K1LineID"),
        F.col("QuicklinkID").cast("int").alias("GaapToTaxLineID"),
        F.col("CategoryID").cast("int").alias("CategoryID"),
        F.col("Amount"),
    )
    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Section 21: AdjustmentAllocationSummary (SQL lines 1860-1910)
# ---------------------------------------------------------------------------

def write_adjustment_allocation_summary(spark: SparkSession, cfg: dict, tables: dict) -> None:
    """Insert into AdjustmentAllocationSummary (conditional on PE Book).

    SQL lines: 1860-1910
    """
    if not cfg["allow_pe_book_inserts"]:
        return

    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    target_table = "AdjustmentAllocationSummary"

    aos = tables["allocation_output_summary"]
    df = aos.filter(F.col("LineTypeID") == cfg["book_k1_adj_line_type_id"]).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("LineTypeID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("AdjustmentTypeID"),
    )
    _store_result_table(spark, cfg, target_table, df)


# ---------------------------------------------------------------------------
# Orchestrator: run_add_allocation_summary
# ---------------------------------------------------------------------------

def run_add_allocation_summary(
    spark: SparkSession,
    cfg: dict = None,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "None",
    VolumePath: str = None,
    ExecutionID: str = None,
    **kwargs,
) -> dict:
    """Main entry point for uspAddAllocationSummary conversion.

    Args:
        spark: SparkSession (Databricks Connect or cluster)
        EntityID, ClientID, TaxPeriodID, RunID: SP parameters
        CatalogName, SchemaName: Unity Catalog target
        cfg: Optional pre-built config dict (from orchestrator)
        ResultType, VolumePath, ExecutionID: GenericResultStorer output options
            (rule 48 — propagated to cfg so the storer reads them downstream).

    Returns:
        dict with execution status and timing.
    """
    # Map CamelCase params to snake_case for use in function body
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName

    t0 = time.time()

    status = {
        "sp_name": "uspAddAllocationSummary",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "elapsed_seconds": 0,
    }

    # Build or reuse config.
    # Mode 3 (Standalone): no cfg → load it.
    # Mode 1/2 (Job/Orchestrator): cfg passed in with all scalars pre-resolved.
    if cfg is None:
        cfg = load_common_config(
            spark, entity_id=entity_id, client_id=client_id,
            tax_period_id=tax_period_id, run_id=run_id,
            catalog=catalog, schema=schema,
        )

    # GenericResultStorer output options — set only when the caller provided
    # a value (setdefault preserves any orchestrator-supplied cfg entry).
    if ResultType is not None:  cfg.setdefault("result_type", ResultType)
    if VolumePath is not None:  cfg["volume_path"] = VolumePath
    if ExecutionID is not None: cfg["execution_id"] = ExecutionID

    # Reset the parquet JSON accumulator for this run (populated per table by
    # _store_result_table, merged into the return value below).
    cfg["_result_file_infos"] = []

    # AllocationRun scalars (run_status, phase_id, partner_workflow_id,
    # partner_transaction_id, foreign_currency_rate_txn_id) are already on cfg
    # from load_common_config — no extra read needed here.

    # Early exit on FAIL
    if cfg.get("run_status") == "FAIL":
        logger.warning("Run status is FAIL — exiting early")
        return {"status": "SKIPPED", "reason": "RunStatus=FAIL", "elapsed_seconds": time.time() - t0}

    try:
        # Section 1: Load SP-specific config
        load_sp_config(spark, cfg)
        cfg["allow_pe_book_inserts"] = _should_insert_pe_book(cfg)

        # Section 2: Build working tables
        tables = build_working_tables(spark, cfg)

        # Build partner PFIC for custom footnotes (needed later)
        partner_wf_id = cfg.get("partner_workflow_id")
        partner_txn_id = cfg.get("partner_transaction_id")
        partner_pfic = read_table(spark, "Partner_Snapshot", cfg).filter(
            (F.coalesce(F.col("WorkFlowID"), F.col("TransactionID")) ==
             F.lit(partner_wf_id if partner_wf_id is not None else partner_txn_id).cast("int"))
            & (F.col("EntityID") == cfg["entity_id"])
            & (F.col("ClientID") == cfg["client_id"])
        ).select(
            ns(F.col("ShareClass")).alias("ShareClass"),
            F.col("PartnerNumber"),
        ).distinct()
        tables["partner_pfic"] = partner_pfic

        # Sections 3-5: Simple summary inserts
        write_k1_allocation_summary(spark, cfg, tables)
        write_m1_adj_allocation_summary(spark, cfg, tables)
        write_box_jkl_allocation_summary(spark, cfg, tables)

        # Sections 6-8: Form summary inserts with tracking key
        write_form926_allocation_summary(spark, cfg, tables)
        write_form8865_allocation_summary(spark, cfg, tables)
        write_form199a_allocation_summary(spark, cfg, tables)

        # Sections 9-11: PFIC
        pfic_reclass_df = build_pfic_reclass_data(spark, cfg, tables)
        write_pfic_footnote_allocation_summary(spark, cfg, tables)
        write_pfic_footnote_allocation_text(spark, cfg, pfic_reclass_df)

        # Sections 12-13: Custom Footnotes
        cf_data = build_custom_footnote_transactions(spark, cfg, tables)
        write_custom_footnote_allocation_summary(spark, cfg, tables, cf_data, partner_pfic)

        # Sections 14-16: Simple summary inserts
        write_line18a_allocation_summary(spark, cfg, tables)
        write_ubti_allocation_summary(spark, cfg, tables)
        write_passive_income_allocation_summary(spark, cfg, tables)

        # Sections 17-21: Form + special summary inserts
        write_form200616_allocation_summary(spark, cfg, tables)
        write_form8886_allocation_summary(spark, cfg, tables)
        write_at_risk_allocation_summary(spark, cfg, tables)
        write_gaap_to_tax_allocation(spark, cfg, tables)
        write_adjustment_allocation_summary(spark, cfg, tables)

        elapsed = time.time() - t0
        logger.info("uspAddAllocationSummary completed in %.1fs", elapsed)

        # Merge every table's save_results JSON blob into one payload so the
        # Orchestrator can populate ResultFilePath/ResultFileName. Each blob is
        # {"ResultFilePath": "<client>/<run>/<exec>/", "<TableName>": [parts...]};
        # ResultFilePath is identical across tables (same client/run/exec), so a
        # flat dict.update merge is safe (mirrors apply_investment_level_rounding).
        merged = {}
        for blob in cfg.get("_result_file_infos", []):
            try:
                merged.update(json.loads(blob))
            except (json.JSONDecodeError, TypeError):
                pass
        result_json = json.dumps(merged) if merged else ""
        if result_json:
            print(f"[PARQUET] Return JSON: {result_json}")
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        # If GenericResultStorer returned a value (JSON string for Parquet mode,
        # "SUCCESS" for Delta/SQL), propagate it directly so the task runtime can
        # parse ResultFilePath/ResultFileName for DataBrickExecutionStatus (same
        # pattern as uspGetFinalEffectivePercentage's run_mode).
        return result_json if result_json else status

    except Exception as e:
        elapsed = time.time() - t0
        logger.error("uspAddAllocationSummary FAILED after %.1fs: %s", elapsed, str(e))
        raise
    finally:
        drop_checkpoints(spark, cfg)


# ════════════════════════════════════════════════════════════════
# __main__: standalone execution (Mode 3).
# Read widget params → call run_add_allocation_summary(...).
# The function's `if cfg is None` branch is the single point that calls
# load_common_config. Job/Orchestrator modes pass cfg in directly and skip
# this block.
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import json
    try:
        spark = SparkSession.builder.getOrCreate()
    except Exception:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.profile("dev").getOrCreate()

    try:
        result = run_add_allocation_summary(
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
    print(f"Result: {result}")
