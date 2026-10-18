"""
config_service.py — Section 1: Configuration loading for uspLoadLookThroughAllocationInput.

Loads all config variables, line type IDs, event type IDs, global menu flags,
PFIC line IDs, entity flags, and allocation run metadata.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

try:
    from .lt_helpers import tbl, tbl_name, ns, ns0, log_section, log_timing, logger
except ImportError:
    from lt_helpers import tbl, tbl_name, ns, ns0, log_section, log_timing, logger

import time


# ---------------------------------------------------------------------------
# Section 1: load_config
# SQL lines ~262-950
# ---------------------------------------------------------------------------

def load_config(spark: SparkSession, cfg: dict) -> dict:
    """Alias Common_V2 cfg scalars into SP-local legacy keys + read TechConfig.

    All scalar lookups (AllocationRun fields, ENU_LineType / ENU_Event /
    ENU_EntityType / ENU_TrialBalanceSource / PFIC line items, GlobalMenu
    flags + menu IDs, Entity row scalars) are pre-resolved by load_common_config.

    Local reads kept (SP-specific business data):
      - ENU_DF_DataList "TECH CONFIG" entries (BOXJKLLOGIC, FLOWUPBOILIABILITIES)
    """
    log_section("load_config")
    t0 = time.time()

    # ── Run-level gate ──
    if (cfg.get("run_status") or "").upper() == "FAIL":
        log_timing("load_config", t0)
        return cfg

    # ── AllocationRun alias (snake_case naming convention) ──
    cfg["foreign_currency_rate_transaction_id"] = cfg.get("foreign_currency_rate_txn_id")

    # ── AllocationType (entity-scoped allocation name) ──
    cfg["allocation_type"] = cfg.get("entity_allocation_type_name")

    # ── ENU_LineType IDs ──
    cfg["pfic_footnote_type_id"] = cfg.get("pfic_footnote_line_type_id")
    cfg["book_k1_adjustment_line_type_id"] = cfg.get("book_k1_adjustments_line_type_id")
    cfg["box_jkl_line_type_id"] = cfg.get("boxjkl_line_type_id")
    # k1_line_type_id, m1_line_type_id, pfic_footnote_line_type_id already in cfg.

    # ── GlobalMenu derived flags / menu IDs ──
    cfg["box_jkl_allocation"] = cfg.get("box_jkl_allocation_menu_name")
    _intl = (cfg.get("flag_separate_international_signoff") or "").strip().upper()
    cfg["is_k1_input_international"] = (_intl == "C")
    cfg["is_investment_level_rounding"] = (
        cfg.get("flag_investment_level_rounding_logic") or "U"
    )
    cfg["pfic_classification"] = cfg.get("flag_foreign_corp_configuration")
    cfg["disable_adjustments_allocations"] = (
        cfg.get("flag_disable_adjustment_allocations") or "U"
    )
    cfg["disable_adjustment_flowup_allocations"] = (
        cfg.get("flag_disable_adjustment_flowup_allocations") or "U"
    )
    # m1_quick_link_id already in cfg.

    # ── TechConfig (SP-specific business data) ──
    tech_config_rows = tbl(spark, "ENU_DF_DataList", cfg).filter(
        F.upper(F.col("Category")) == "TECH CONFIG"
    ).select("LookUpData", "LookUpValue").collect()
    tech_map = {
        (r["LookUpData"].upper() if r["LookUpData"] else ""): r["LookUpValue"]
        for r in tech_config_rows
    }
    cfg["box_jkl_logic"] = tech_map.get("BOXJKLLOGIC", "0") or "0"
    cfg["flowup_boi_liabilities"] = tech_map.get("FLOWUPBOILIABILITIES", "0") or "0"

    # ── ENU_Event IDs ──
    cfg["k1_event_type_id"] = cfg.get("event_type_id_k1_input")
    cfg["k1_international_event_type_id"] = cfg.get("event_type_id_k1_input_international")
    cfg["adjustment_event_type_id"] = cfg.get("event_type_id_adjustments")
    cfg["investment_tag_event_type_id"] = cfg.get("event_type_id_import_investment_tag")
    cfg["lookthrough_reclass_event_type_id"] = cfg.get("event_type_id_lookthrough_reclass")

    # ── ENU_EntityType IDs ──
    cfg["inv_entity_type_id"] = cfg.get("entity_type_id_investment")
    cfg["fund_entity_type_id"] = cfg.get("entity_type_id_fund")

    # ── ENU_TrialBalanceSource ──
    cfg["adjustment_source_type_id"] = cfg.get("trial_balance_source_id_book_k1")

    # ── PFICFootNoteLineItem (legacy alias for back-compat) ──
    cfg["pfic_txt11_held"] = cfg.get("pfic_txt11_held_line_id")
    # qef_election_line_id, pfic_6a/7a/7a_longterm/8b/10c, pfic_investment,
    # type_of_pfic, type_of_foreign_corp already in cfg.

    # ── IsForeignEntity (Entity.IsForeign OR TaxClass='Disregarded Entity' with NOT IsForeign) ──
    _tax_class_name = (cfg.get("entity_tax_class_name") or "").strip().lower()
    cfg["is_foreign_entity"] = bool(cfg.get("entity_is_foreign")) or (
        _tax_class_name == "disregarded entity" and not cfg.get("entity_is_foreign")
    )

    # ── IsBlockerEntity — Entity is PFIC/DomesticBlocker/CFC/QualifiedForeignCorp ──
    cfg["is_blocker_entity"] = bool(
        cfg.get("entity_is_pfic")
        or cfg.get("entity_is_domestic_blocker")
        or cfg.get("entity_is_cfc")
        or cfg.get("entity_is_qualified_foreign_corp")
    )

    # ── PE Book Allocation eligibility ──
    cfg["is_pe_book_allocation"] = (
        (cfg.get("entity_allocation_type_name") or "").strip().lower()
        == "pe book allocation"
    )

    # ── InvestmentTagWorkflowID / LookthroughReclassWorkflowID from AllocationRun ──
    # Only meaningful for PE Book Allocation entities (SQL gated the same way).
    if cfg["is_pe_book_allocation"]:
        cfg["investment_tag_workflow_id"] = cfg.get("investment_tag_workflow_id") or 0
        cfg["lookthrough_reclass_workflow_id"] = (
            cfg.get("lookthrough_reclass_workflow_id") or 0
        )
    else:
        cfg["investment_tag_workflow_id"] = 0
        cfg["lookthrough_reclass_workflow_id"] = 0

    log_timing("load_config", t0)
    return cfg


# ---------------------------------------------------------------------------
# Section 2: load_workflows
# SQL lines ~951-980
# ---------------------------------------------------------------------------

def load_workflows(spark: SparkSession, cfg: dict):
    """
    Load #K1Workflow and #AdjustmentWorkflow from AllocationInputWorkflow.
    Returns (k1_workflow_df, adjustment_workflow_df).
    """
    log_section("load_workflows")
    t0 = time.time()

    run_id = cfg["run_id"]

    aiw = tbl(spark, "AllocationInputWorkflow", cfg).filter(F.col("RunID") == run_id)

    # K1Workflow: always include K1WorkflowID
    k1_wf = aiw.select(
        F.col("EntityID"),
        F.col("K1WorkflowID").alias("WorkflowID"),
    ).filter(F.coalesce(F.col("WorkflowID"), F.lit(0)) != 0)

    # If IsK1InputInternational, also include K1InternationalWorkflowID
    if cfg["is_k1_input_international"]:
        k1_intl = aiw.select(
            F.col("EntityID"),
            F.col("K1InternationalWorkflowID").alias("WorkflowID"),
        ).filter(F.coalesce(F.col("WorkflowID"), F.lit(0)) != 0)
        k1_wf = k1_wf.unionByName(k1_intl)

    # AdjustmentWorkflow (only if adjustments not disabled)
    adj_wf = None
    if cfg["disable_adjustments_allocations"] != "C":
        adj_wf = aiw.select(
            F.col("EntityID"),
            F.col("AdjustmentsWorkflowID").alias("WorkflowID"),
        ).filter(F.coalesce(F.col("WorkflowID"), F.lit(0)) != 0)

    log_timing("load_workflows", t0)
    return k1_wf, adj_wf


# ---------------------------------------------------------------------------
# Section 3: load_lower_tier_funds
# SQL lines ~981-1000
# ---------------------------------------------------------------------------

def load_lower_tier_funds(spark: SparkSession, cfg: dict):
    """
    Load #LowerTierFunds from LowerTierFunds table.
    Returns DataFrame with (EntityID, PartnerNumber, RunID).
    """
    log_section("load_lower_tier_funds")
    t0 = time.time()

    run_id = cfg["run_id"]

    lt_funds = tbl(spark, "LowerTierFunds", cfg).filter(
        F.col("RunID") == run_id
    ).select(
        F.col("EntityID"),
        F.col("PartnerNumber"),
        F.col("LTRunID").alias("RunID"),
    )

    log_timing("load_lower_tier_funds", t0)
    return lt_funds


# ---------------------------------------------------------------------------
# Section 4: build_fx_rates
# SQL lines ~1001-1105
# ---------------------------------------------------------------------------

def build_fx_rates(spark: SparkSession, cfg: dict, k1_workflow_df):
    """
    Build #K1LineItemsWithRates — conversion rates per entity/line.
    Returns DataFrame with (EntityID, CurrencyCode, LineID, TransactionDate, ConversionRate).
    """
    log_section("build_fx_rates")
    t0 = time.time()

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    fx_txn_id = cfg["foreign_currency_rate_transaction_id"]


    # Use k1_workflow_df (already filtered for WorkflowID != 0) as the source
    k1_workflow_df.select("EntityID").distinct().createOrReplaceTempView("_tmp_k1_workflow_entities")

    # Join K1Workflow entities → Entity → K1LineItem → FX rates
    fx_rates = spark.sql(f"""
        SELECT
            KW.EntityID,
            E.CurrencyCode,
            K1.LineID,
            K1.TransactionDate,
            COALESCE(COALESCE(F2.Rate, F1.AverageRate), 1) AS ConversionRate
        FROM _tmp_k1_workflow_entities KW
        INNER JOIN {tbl_name('Entity', cfg)} E
            ON KW.EntityID = E.EntityID
        LEFT JOIN {tbl_name('K1LineItem', cfg)} K1
            ON K1.ClientID = E.ClientID
            AND K1.TaxPeriodID = E.TaxPeriodID
            AND K1.LineDataType = 'Number'
        LEFT JOIN {tbl_name('ForeignCurrencyAverageRate', cfg)} F1
            ON F1.ClientID = E.ClientID
            AND F1.CurrencyCode = E.CurrencyCode
            AND F1.TransactionID = {fx_txn_id or 'NULL'}
        LEFT JOIN {tbl_name('ForeignCurrencyRate', cfg)} F2
            ON F2.ClientID = E.ClientID
            AND F2.CurrencyCode = E.CurrencyCode
            AND F2.TransactionID = {fx_txn_id or 'NULL'}
            AND F2.Range = K1.TransactionDate
        WHERE E.ClientID = {client_id}
          AND E.TaxPeriodID = {tax_period_id}
    """)

    log_timing("build_fx_rates", t0)
    return fx_rates


# ---------------------------------------------------------------------------
# Section 4b: load_reclass_k1_data
# SQL lines ~485-500 (INSERT INTO #ReclassK1LookThroughAllocationData)
# ---------------------------------------------------------------------------

def load_reclass_k1_data(spark: SparkSession, cfg: dict):
    """
    Load ReclassK1LookThroughAllocationData filtered by RunID.
    This is loaded early (before main logic) and used in flowup sections.
    """
    log_section("load_reclass_k1_data")
    t0 = time.time()

    run_id = cfg["run_id"]

    reclass_k1 = tbl(spark, "ReclassK1LookThroughAllocationData", cfg).filter(
        F.col("RunID") == run_id
    ).select(
        "RunID", "ReclassWorkflowID", "LowerTierRunID", "ClientID", "TaxPeriodID",
        "EntityID", "LineID", "Amount", "FlowupAmount", "ParentEntityID",
        "TrackingKey", "SuperParentEntityID", "PeriodID", "LTEntityID", "Tag",
        "OriginalParentEntityID",
    )

    log_timing("load_reclass_k1_data", t0)
    return reclass_k1
