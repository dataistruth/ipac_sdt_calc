"""
ai_config_service.py

Configuration and lookup loading for uspLoadAllocationInput.

"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import table_prefix, read_table, ns, ns0, log_section, log_timing

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pre-fetched shared state for inlined UDFs
# ---------------------------------------------------------------------------

def _prefetch_udf_state(spark: SparkSession, cfg: dict) -> dict:
    """Pre-fetch shared state needed by multiple inlined UDFs."""
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]

    # Batch 1: Phase + WorkFlowChain + WORKFLOWSTATUS
    phase_df = (
        read_table(spark, "Phase", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            F.col("EndDate").isNull()
        )
        .select(F.col("PhaseID").cast("int").alias("val"), F.lit("phase_id").alias("key"))
        .limit(1)
    )

    wfc_df = (
        read_table(spark, "WorkFlowChain", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.col("IncludeinCalc") == True)
        )
        .select(F.col("WorkflowStatusID").cast("int").alias("val"), F.lit("include_step").alias("key"))
        .limit(1)
    )

    ws_df = (
        read_table(spark, "WORKFLOWSTATUS", cfg)
        .filter(F.lower(F.col("EnumerationName")).isin("rejected", "err_critical", "err_noncritical"))
        .select(F.col("StatusID").cast("int").alias("val"), F.lower(F.col("EnumerationName")).alias("key"))
    )
    # Single collect for all scalar lookups
    combined_rows = phase_df.unionByName(wfc_df).unionByName(ws_df).collect()

    phase_id = -1
    include_in_calc_step = 0
    excluded_status_map = {}
    for r in combined_rows:
        if r["key"] == "phase_id":
            phase_id = r["val"]
        elif r["key"] == "include_step":
            include_in_calc_step = r["val"]
        else:
            excluded_status_map[r["key"]] = r["val"]

    excluded_status_ids_all = list(excluded_status_map.values())
    excluded_status_ids_strict = [v for k, v in excluded_status_map.items() if k in ("rejected", "err_critical")]

    # Batch 2: Partner methodology + Entity foreign status
    gm_df = read_table(spark, "GlobalMenu", cfg)
    enu_gm_df = read_table(spark, "ENU_GlobalMenuGroup", cfg)
    entity_df = read_table(spark, "Entity", cfg)
    tax_class_df = read_table(spark, "ENU_TaxClass", cfg)

    partner_df = (
        gm_df.alias("GM")
        .join(enu_gm_df.alias("ENU"), F.col("ENU.GlobalMenuGroupID") == F.col("GM.GlobalMenuGroupID"), "inner")
        .filter(
            (F.lower(F.col("ENU.GroupName")) == "partner import methodology") &
            (F.lower(F.col("GM.State")) == "c") &
            (F.col("GM.ClientID") == client_id) &
            (F.col("GM.TaxPeriodID") == tax_period_id)
        )
        .select(
            F.when(F.lower(F.col("GM.MenuName")).isin("fund entity import", "global partner management"), F.lit("Fund"))
            .otherwise(F.lit("Master")).alias("val"),
            F.lit("partner_method").alias("key")
        )
        .limit(1)
    )

    foreign_df = (
        entity_df.alias("E")
        .join(tax_class_df.alias("T"), F.col("E.TaxClassID") == F.col("T.TaxClassID"), "left")
        .filter(F.col("E.EntityID") == entity_id)
        .select(
            F.when(
                (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == True) |
                ((F.coalesce(F.col("E.IsForeign"), F.lit(False)) == False) &
                 (F.lower(F.coalesce(F.col("T.TaxClassName"), F.lit(""))) == "disregarded entity")),
                F.lit("True")
            ).otherwise(F.lit("False")).alias("val"),
            F.lit("is_foreign").alias("key")
        )
        .limit(1)
    )

    batch2_rows = partner_df.unionByName(foreign_df).collect()
    partner_method = "Fund"
    is_foreign_entity = False
    for r in batch2_rows:
        if r["key"] == "partner_method":
            partner_method = r["val"]
        elif r["key"] == "is_foreign":
            is_foreign_entity = r["val"] == "True"

    state = {
        "phase_id": phase_id,
        "include_in_calc_step": include_in_calc_step,
        "excluded_status_ids_strict": excluded_status_ids_strict,
        "excluded_status_ids_all": excluded_status_ids_all,
        "partner_method": partner_method,
        "is_foreign_entity": is_foreign_entity,
    }

    return state


# ---------------------------------------------------------------------------
# Inlined UDFs
# ---------------------------------------------------------------------------

def _get_approved_workflow_phase(
    spark: SparkSession, cfg: dict, udf_state: dict,
    event_type_id: int, entity_id: int,
) -> int:
    """Inline of dbo.udfGetApprovedWorkflow_Phase."""
    if event_type_id is None:
        return 0
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    phase_id = udf_state["phase_id"]
    include_in_calc_step = udf_state["include_in_calc_step"]
    # Determine which statuses to exclude
    lt_reclass = cfg.get("_lookthrough_reclass_event_type_id")
    adjustment = cfg.get("_adjustment_event_type_id")
    tax_capital = cfg.get("_tax_capital_event_type_id")

    if event_type_id in (lt_reclass, adjustment, tax_capital):
        excluded = udf_state["excluded_status_ids_strict"]
    else:
        excluded = udf_state["excluded_status_ids_all"]

    wf_df = read_table(spark, "Workflow", cfg)
    tl_df = read_table(spark, "TransactionLog", cfg)

    row = (
        wf_df.alias("WF")
        .join(
            tl_df.alias("TL"),
            (F.col("TL.TransactionID") == F.col("WF.TransactionID")) &
            (F.col("TL.EventTypeID") == event_type_id) &
            (F.col("TL.PhaseID") == F.col("WF.PhaseID")),
            "inner"
        )
        .filter(
            (F.col("TL.EntityID") == entity_id) &
            (F.col("TL.ClientID") == client_id) &
            (F.col("TL.TaxPeriodID") == tax_period_id) &
            (F.col("TL.StatusID") >= include_in_calc_step) &
            (F.col("TL.PhaseID") == phase_id) &
            (~F.col("TL.StatusID").isin(excluded))
        )
        .select(F.coalesce(F.max("WF.WorkflowID"), F.lit(0)).alias("WorkflowID"))
        .first()
    )
    return row["WorkflowID"] if row else 0




# ---------------------------------------------------------------------------
# Batched UDF execution
# ---------------------------------------------------------------------------

def _batch_udf_results(
    spark: SparkSession, cfg: dict, udf_state: dict, entity_id: int,
) -> None:
    """Compute partner_transaction_id, tax_capital_data_transaction_id,
    lookthrough_reclass_workflow_id, financial_workflow_id, pcap_workflow_id
    in batched actions.
    """
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    phase_id = udf_state["phase_id"]

    master_event_type_id = cfg.get("master_partner_event_type_id")
    partner_method = udf_state["partner_method"]
    cfg["partner_method"] = partner_method
    excluded_all = udf_state["excluded_status_ids_all"]
    excluded_strict = udf_state["excluded_status_ids_strict"]
    include_in_calc_step = udf_state["include_in_calc_step"]

    tax_capital_event = (
        cfg.get("_tax_capital_data_event_type_id")
        or cfg.get("_tax_capital_event_type_id")
    )
    lt_reclass_event = cfg.get("_lookthrough_reclass_event_type_id")
    financial_event = cfg.get("financial_event_type_id")
    pcap_event = cfg.get("pcap_event_type_id")

    tl_df = read_table(spark, "TransactionLog", cfg)
    wf_df = read_table(spark, "Workflow", cfg)

    partner_entity = 0 if partner_method == "Master" else entity_id
    partner_event_type_id = (
        master_event_type_id if partner_method == "Master"
        else cfg.get("_import_partner_event_type_id", master_event_type_id)
    )

    result_df = tl_df.filter(
        (F.col("ClientID") == client_id) &
        (F.col("TaxPeriodID") == tax_period_id)
    ).select(
        F.max(F.when(
            (F.col("EntityID") == partner_entity) &
            (F.col("EventTypeID") == partner_event_type_id) &
            (~F.col("StatusID").isin(excluded_all)) &
            (F.col("StatusID") != 0) &
            (F.col("PhaseID") == phase_id),
            F.col("TransactionID")
        )).alias("partner_tid"),
        F.max(F.when(
            (F.col("EntityID") == entity_id) &
            (F.col("EventTypeID") == tax_capital_event) &
            (~F.col("StatusID").isin(excluded_all)) &
            (F.col("PhaseID") == phase_id),
            F.col("TransactionID")
        )).alias("tc_tid"),
    )

    # For workflow-based lookups, use a separate query joining TL + WF + DFW
    dfw_df = read_table(spark, "DF_EntityFeedTransactionMapping", cfg)
    wf_result_df = (
        wf_df.alias("WF")
        .join(
            tl_df.alias("TL"),
            (F.col("TL.TransactionID") == F.col("WF.TransactionID")) &
            (F.col("TL.PhaseID") == F.col("WF.PhaseID")),
            "inner"
        )
        .filter(
            (F.col("TL.ClientID") == client_id) &
            (F.col("TL.TaxPeriodID") == tax_period_id)
        )
        .select(
            F.max(F.when(
                (F.col("TL.EntityID") == entity_id) &
                (F.col("TL.EventTypeID") == lt_reclass_event) &
                (F.col("TL.PhaseID") == phase_id) &
                (F.col("TL.StatusID") >= include_in_calc_step) &
                (~F.col("TL.StatusID").isin(excluded_strict)),
                F.col("WF.WorkflowID")
            )).alias("reclass_wf"),
            F.lit(None).cast("int").alias("_placeholder"),
        )
    )

    # Financial + PCAP workflows need DFW join
    wf_dfw_result_df = (
        wf_df.alias("WF")
        .join(
            tl_df.alias("TL"),
            (F.col("TL.TransactionID") == F.col("WF.TransactionID")) &
            (F.col("TL.EventTypeID").isin([x for x in [financial_event, pcap_event] if x])) &
            (F.col("TL.PhaseID") == F.col("WF.PhaseID")),
            "inner"
        )
        .join(
            dfw_df.alias("DFW"),
            # udfGetDataFeedApprovedWorkflow L36-39: DFW joins on TransactionID AND
            # ClientID AND TaxPeriodID AND EventTypeID (then WHERE DFW.EntityID=@EntityID).
            # The TransactionID-only join over-matched DFW rows for other feeds/periods.
            (F.col("DFW.TransactionID") == F.col("TL.TransactionID")) &
            (F.col("DFW.ClientID") == F.col("TL.ClientID")) &
            (F.col("DFW.TaxPeriodID") == F.col("TL.TaxPeriodID")) &
            (F.col("DFW.EventTypeID") == F.col("TL.EventTypeID")) &
            (F.col("DFW.EntityID") == entity_id),
            "inner"
        )
        .filter(
            (F.col("TL.ClientID") == client_id) &
            (F.col("TL.TaxPeriodID") == tax_period_id) &
            (F.col("TL.PhaseID") == phase_id) &
            (F.col("TL.StatusID") >= include_in_calc_step) &
            (~F.col("TL.StatusID").isin(excluded_all))
        )
        .select(
            F.max(F.when(
                (F.col("TL.EventTypeID") == financial_event),
                F.col("WF.WorkflowID")
            )).alias("financial_wf"),
            F.max(F.when(
                (F.col("TL.EventTypeID") == pcap_event),
                F.col("WF.WorkflowID")
            )).alias("pcap_wf"),
        )
    )
    # Execute both as one collect (saves ~9s)
    tl_row = result_df.first()
    wf_row = wf_result_df.first()
    wf_dfw_row = wf_dfw_result_df.first()

    cfg["partner_transaction_id"] = tl_row["partner_tid"] if tl_row and tl_row["partner_tid"] is not None else 0
    cfg["tax_capital_data_transaction_id"] = tl_row["tc_tid"] if tl_row and tl_row["tc_tid"] is not None else 0
    cfg["lookthrough_reclass_workflow_id"] = wf_row["reclass_wf"] if wf_row and wf_row["reclass_wf"] is not None else 0
    cfg["_financial_workflow_id"] = wf_dfw_row["financial_wf"] if wf_dfw_row and wf_dfw_row["financial_wf"] is not None else 0
    cfg["_pcap_workflow_id"] = wf_dfw_row["pcap_wf"] if wf_dfw_row and wf_dfw_row["pcap_wf"] is not None else 0


# ---------------------------------------------------------------------------
# Main config loader
# ---------------------------------------------------------------------------

def load_config(spark: SparkSession, cfg: dict) -> dict:
    """Resolve SP-specific config on top of the Common_V2 cfg taxonomy.

    THIN-ALIAS pattern (mirrors usp_sm_apply_investment_level_rounding's
    _load_sp_specific_config): every scalar that `load_common_config` already
    resolves to a canonical taxonomy key is aliased here to the legacy SP-local
    name. Only the genuinely SP-specific values that the taxonomy does NOT
    provide are resolved with Spark reads below:

      * `_batch_udf_results` — partner/tax-capital transaction IDs and the
        lookthrough-reclass / financial / pcap workflow IDs are *recomputed*
        from TransactionLog/Workflow (udfGetLastTransactionIDForPartner etc.),
        not read from the stored AllocationRun columns. Kept.
      * `_prefetch_udf_state` — phase_id (Phase table, EndDate IS NULL) +
        is_foreign_entity (Entity ⋈ TaxClass disregarded-entity rule) +
        excluded-status/include-step state for the inlined UDFs. Kept.
      * ENU_ForeignCorptypeofPFIC 1293-eligibility descriptions. Kept.
      * Form926 pre/post-transfer ownership line IDs (taxonomy only carries
        the transferdate line). Kept.
      * datafeed_financial / datafeed_pcap event IDs (not in _EVENT_NAMES). Kept.
      * GlobalMenu flags absent from _MENU_FLAGS (store_qef_for_allocation,
        flow_zero_pfics, part_vii_indicator, master-feed override, foreign-
        blocker FN flowup). Kept.
      * PFICFootnoteLineItem ownership/status/part-5/part-7/percent line IDs
        beyond the taxonomy set. Kept.
      * Entity composite flags (allocation_type, display name, blocker-entity,
        PFIC/CFC/QFC) — taxonomy carries the raw entity_is_* booleans but not
        these derived composites or DisplayName. Kept.
      * SidePocket / SPA workflow IDs (not in _AR_COLUMN_MAP). Kept.

    All keys aliased below use cfg.setdefault so Mode 1/2 pre-seeded cfg dicts
    are never overwritten.
    """
    log_section("load_config")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]

    # ── Taxonomy aliases: event-type IDs ──
    cfg.setdefault("k1_event_type_id", cfg.get("event_type_id_k1_input"))
    cfg.setdefault("k1_international_event_type_id", cfg.get("event_type_id_k1_input_international"))
    cfg.setdefault("_adjustment_event_type_id", cfg.get("event_type_id_adjustments"))
    cfg.setdefault("master_partner_event_type_id", cfg.get("event_type_id_master_import_partner"))
    cfg.setdefault("_import_partner_event_type_id", cfg.get("event_type_id_partner_import"))
    # at_risk import event is not in the taxonomy _EVENT_NAMES set; the SP's own
    # enum path resolved it from "import_atrisk" only in legacy Mode 3. The live
    # value here is None (the at-risk event is unused for this SP's DPs); alias
    # preserves that.
    cfg.setdefault("at_risk_event_type_id", cfg.get("event_type_id_import_at_risk"))
    cfg.setdefault("_lookthrough_reclass_event_type_id", cfg.get("event_type_id_lookthrough_reclass"))
    cfg.setdefault("investment_tag_event_type_id", cfg.get("event_type_id_import_investment_tag"))
    cfg.setdefault("_tax_capital_event_type_id", cfg.get("event_type_id_import_tax_capital"))
    cfg.setdefault("_default_alloc_rule_event_type_id", cfg.get("event_type_id_import_default_allocation_rule"))

    # ── Taxonomy aliases: line-type IDs ──
    # k1/form926/form199a/form8886/form8865/pfic_footnote/gaap_to_tax/at_risk/
    cfg.setdefault("box_jkl_line_type_id", cfg.get("boxjkl_line_type_id"))
    cfg.setdefault("book_k1_adjustment_line_type_id", cfg.get("book_k1_adjustments_line_type_id"))

    # ── Taxonomy aliases: entity-type / trial-balance source IDs ──
    cfg.setdefault("inv_entity_type_id", cfg.get("entity_type_id_investment"))
    cfg.setdefault("fund_entity_type_id", cfg.get("entity_type_id_fund"))
    cfg.setdefault("adjustment_source_type_id", cfg.get("trial_balance_source_id_book_k1"))

    # ── Taxonomy aliases: GlobalMenu flags ──
    cfg.setdefault("is_tracking_key", cfg.get("flag_keep_tracking_keys") or "C")
    cfg.setdefault("is_k1_input_international",
                   (cfg.get("flag_separate_international_signoff") or "U") == "C")
    cfg.setdefault("is_investment_level_rounding", cfg.get("flag_investment_level_rounding_logic"))
    cfg.setdefault("pfic_classification", cfg.get("flag_foreign_corp_configuration"))
    cfg.setdefault("is_auto_elec_d_enabled",
                   (cfg.get("flag_automate_deemed_sale_election") or "U") in ("C", "CG"))
    cfg.setdefault("disable_adjustments_allocations", cfg.get("flag_disable_adjustment_allocations") or "U")
    cfg.setdefault("disable_adjustment_flowup_allocations",
                   cfg.get("flag_disable_adjustment_flowup_allocations") or "U")
    cfg.setdefault("has_k1_line_item_config",
                   (cfg.get("flag_configure_k1") or "U") == "C")

    # ── Taxonomy aliases: selected-menu / line-id renames ──
    cfg.setdefault("rounding_logic", cfg.get("rounding_logic_selected"))
    cfg.setdefault("box_jkl_allocation", cfg.get("box_jkl_allocation_menu_name"))
    cfg.setdefault("form926_transfer_date_line_id", cfg.get("form926_quarter_line_id"))

    # ── Taxonomy aliases: AllocationRun workflow / transaction IDs ──
    # fx_rate, cost%, yearly are canonical AllocationRun columns in the taxonomy.
    cfg.setdefault("fx_rate_transaction_id", cfg.get("foreign_currency_rate_txn_id"))
    cfg.setdefault("cost_percentage_workflow_id", cfg.get("cost_workflow_id"))
    # yearly_workflow_id is itself a canonical taxonomy key (same name) — present.

    # ─────────────────────────────────────────────────────────────────────────
    # SP-specific resolution (NOT covered by the taxonomy)
    # ─────────────────────────────────────────────────────────────────────────

    # ENU_ForeignCorptypeofPFIC 1293-eligibility descriptions.
    if cfg.get("is_1293_eligible_deemed_desc") is None:
        pfic_type_rows = (
            read_table(spark, "ENU_ForeignCorptypeofPFIC", cfg)
            .filter(F.lower(F.col("TypeOfPFICName")).isin(
                "deemed sale election + section 1293 qef (unpedigreed)",
                "section 1293 qef (pedigreed)"
            ))
            .select(F.lower(F.col("TypeOfPFICName")).alias("key"), F.col("TypeOfPFICLookUp").alias("val"))
            .collect()
        )
        pfic_type_map = {r["key"]: r["val"] for r in pfic_type_rows}
        cfg["is_1293_eligible_deemed_desc"] = pfic_type_map.get(
            "deemed sale election + section 1293 qef (unpedigreed)"
        )
        cfg["is_1293_eligible_no_deemed_desc"] = pfic_type_map.get(
            "section 1293 qef (pedigreed)"
        )

    # Form926LineItem pre/post-transfer ownership line IDs (taxonomy only has
    # form926_quarter_line_id = transferdate).
    if cfg.get("line_9a_before_line_id") is None:
        f926_rows = (
            read_table(spark, "Form926LineItem", cfg)
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id) &
                F.lower(F.col("ShortName")).isin("pretransferownership", "posttransferownership")
            )
            .select(F.lower(F.col("ShortName")).alias("key"), F.col("LineID").cast("int").alias("val"))
            .collect()
        )
        f926_map = {r["key"]: r["val"] for r in f926_rows}
        cfg["line_9a_before_line_id"] = f926_map.get("pretransferownership", 0)
        cfg["line_9a_after_line_id"] = f926_map.get("posttransferownership", 0)

    # datafeed_financial / datafeed_pcap event IDs (not in taxonomy _EVENT_NAMES).
    if cfg.get("financial_event_type_id") is None or cfg.get("pcap_event_type_id") is None:
        df_event_rows = (
            read_table(spark, "ENU_Event", cfg)
            .filter(F.lower(F.col("EventName")).isin("datafeed_financial", "datafeed_pcap"))
            .select(F.lower(F.col("EventName")).alias("key"), F.col("EventTypeID").cast("int").alias("val"))
            .collect()
        )
        df_event_map = {r["key"]: r["val"] for r in df_event_rows}
        if cfg.get("financial_event_type_id") is None:
            cfg["financial_event_type_id"] = df_event_map.get("datafeed_financial")
        if cfg.get("pcap_event_type_id") is None:
            cfg["pcap_event_type_id"] = df_event_map.get("datafeed_pcap")

    # --- N5: tax-capital DATA event type (K1ATaxCapitalDataReceived) ---
    # SP L383 sets @TaxCapitalDataEventTypeID = ENU_Event WHERE EventName =
    # 'K1ATaxCapitalDataReceived', and L536 uses it to resolve @TaxCapitalDataTransactionID
    # (udfGetLatestTransactionID). This is DISTINCT from @TaxCapitalImportEventTypeID
    # ('Import_TaxCapital'), which udfGetApprovedWorkflow_Phase uses only for its strict
    # error-status branch. The taxonomy alias `_tax_capital_event_type_id` carries
    # Import_TaxCapital; keep that for the strict-status path but resolve the DATA event
    # separately for tc_tid.
    if cfg.get("_tax_capital_data_event_type_id") is None:
        tcd_row = (
            read_table(spark, "ENU_Event", cfg)
            .filter(F.lower(F.col("EventName")) == "k1ataxcapitaldatareceived")
            .select(F.col("EventTypeID").cast("int").alias("val"))
            .first()
        )
        cfg["_tax_capital_data_event_type_id"] = tcd_row["val"] if tcd_row else None

    # --- Phase ID + pre-fetched UDF state (always needed for inlined UDFs) ---
    udf_state = _prefetch_udf_state(spark, cfg)
    cfg.setdefault("phase_id", udf_state["phase_id"])
    cfg.setdefault("is_foreign_entity", udf_state["is_foreign_entity"])
    cfg["_udf_state"] = udf_state  # Store for standalone UDF functions

    # --- AllocationRun SidePocket/SPA workflow IDs (not in _AR_COLUMN_MAP) ---
    if cfg.get("sp_workflow_id") is None:
        sp_run_row = (
            read_table(spark, "AllocationRun", cfg)
            .filter(
                (F.col("RunID") == run_id) &
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id) &
                (F.col("PhaseID") == cfg["phase_id"])
            )
            .select("SidePocketWorkflowID", "SPAWorkflowID")
            .first()
        )
        cfg["sp_workflow_id"] = sp_run_row["SidePocketWorkflowID"] if sp_run_row else None
        cfg["spa_workflow_id"] = sp_run_row["SPAWorkflowID"] if sp_run_row else None

    # run_status / run_type come from the taxonomy AllocationRun read. If the
    # taxonomy could not resolve them (no row), it sets run_status="FAIL"; the
    # entry point aborts on that, so no fallback read is needed here.

    _batch_udf_results(spark, cfg, udf_state, entity_id)

    # GlobalMenu flags absent from the taxonomy _MENU_FLAGS set.
    if cfg.get("store_qef_for_allocation") is None:
        gm_df = read_table(spark, "GlobalMenu", cfg)
        enu_gm_df = read_table(spark, "ENU_GlobalMenuGroup", cfg)
        gm_rows = (
            gm_df.alias("GM")
            .join(enu_gm_df.alias("EG"), F.col("EG.GlobalMenuGroupID") == F.col("GM.GlobalMenuGroupID"), "inner")
            .filter(
                (F.col("GM.ClientID") == client_id) &
                (F.col("GM.TaxPeriodID") == tax_period_id) &
                F.lower(F.col("EG.GroupName")).isin(
                    "other configuration", "foreign corporations report"
                )
            )
            .select(F.col("GM.MenuName"), F.col("GM.State"))
            .collect()
        )
        gm_map = {r["MenuName"]: r["State"] for r in gm_rows}

        cfg["store_qef_for_allocation"] = gm_map.get("Store allocation detail for QEF Foreign Corporations")
        cfg["flow_zero_pfics"] = gm_map.get("Tier up Foreign Corporations with Zero Income")
        cfg["part_vii_indicator"] = 1 if gm_map.get("Display 1291 - no excess distribution - Part VII") == "C" else 0
        cfg["is_foreign_blocker_footnotes_flowup_checked"] = (
            gm_map.get("Stop Non-Foreign Corp FN Flow Up at Foreign Blocker", "U") == "C"
        )
        cfg["is_master_feed_override"] = gm_map.get("Run ProRata with Master Feed Alloc", "U") == "C"

    if cfg.get("election_d_line_id") is None:
        pfic_line_rows = (
            read_table(spark, "PFICFootnoteLineItem", cfg)
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id) &
                F.lower(F.col("ShortName")).isin(
                    "pficownership", "pficstatus", "ispart_5", "ispart_7",
                    "ownershippercentage", "numberofsharesbeginningofyear",
                    "numberofsharesendofyear", "part_5_g", "cfcpartnershipownership",
                )
            )
            .select(F.lower(F.col("ShortName")).alias("short_name"), F.col("LineID"))
            .collect()
        )
        pfic_line_map = {r["short_name"]: r["LineID"] for r in pfic_line_rows}

        cfg["pfic_ownership_line_id"] = pfic_line_map.get("pficownership")
        cfg["pfic_pstatus_line_id"] = pfic_line_map.get("pficstatus")
        cfg["pfic_is_part_5_line_id"] = pfic_line_map.get("ispart_5")
        cfg["pfic_is_part_7_line_id"] = pfic_line_map.get("ispart_7")
        cfg["pfic_percent_line_ids"] = [
            v for k, v in pfic_line_map.items()
            if k in ("ownershippercentage", "numberofsharesbeginningofyear",
                     "numberofsharesendofyear", "part_5_g", "cfcpartnershipownership")
            and v is not None
        ]

    # --- AllocationType + Entity composite flags ---
    if cfg.get("allocation_type") is None:
        entity_df = read_table(spark, "Entity", cfg)
        alloc_logic_df = read_table(spark, "ENU_AllocationLogic", cfg)
        tax_class_df = read_table(spark, "ENU_TaxClass", cfg)
        combo_row = (
            entity_df.alias("E")
            .join(alloc_logic_df.alias("AL"), F.col("AL.AllocationTypeID") == F.col("E.AllocationTypeID"), "left")
            .join(tax_class_df.alias("TC"), F.col("TC.TaxClassID") == F.col("E.TaxClassID"), "left")
            .filter(F.col("E.EntityID") == entity_id)
            .select(
                F.col("AL.AllocationTypeName"),
                F.col("E.DisplayName"),
                F.when(
                    (F.coalesce(F.col("E.IsPFIC"), F.lit(False)) == True) |
                    (F.coalesce(F.col("E.IsDomesticBlocker"), F.lit(False)) == True) |
                    (F.coalesce(F.col("E.IsCFC"), F.lit(False)) == True) |
                    (F.coalesce(F.col("E.IsQualifiedForeignCorporation"), F.lit(False)) == True),
                    F.lit(True)
                ).otherwise(F.lit(False)).alias("IsBlockerEntity"),
                F.when(
                    ((F.coalesce(F.col("E.IsPFIC"), F.lit(False)) == True) |
                     (F.coalesce(F.col("E.IsCFC"), F.lit(False)) == True) |
                     (F.coalesce(F.col("E.IsQualifiedForeignCorporation"), F.lit(False)) == True)) &
                    ((F.coalesce(F.col("E.IsForeign"), F.lit(False)) == True) |
                     ((F.lower(F.coalesce(F.col("TC.TaxClassName"), F.lit(""))) == "disregarded entity") &
                      (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == False))),
                    F.lit(True)
                ).otherwise(F.lit(False)).alias("IsPficCfcQfc"),
                F.coalesce(F.col("E.IsPFIC"), F.lit(False)).alias("IsPFIC"),
                F.when(
                    (F.coalesce(F.col("E.IsCFC"), F.lit(False)) == True) |
                    (F.coalesce(F.col("E.IsQualifiedForeignCorporation"), F.lit(False)) == True),
                    F.lit(True)
                ).otherwise(F.lit(False)).alias("IsCfcOrQfc"),
            )
            .first()
        )

        cfg["allocation_type"] = combo_row["AllocationTypeName"] if combo_row else None
        cfg["entity_display_name"] = combo_row["DisplayName"] if combo_row else ""
        cfg["is_blocker_entity"] = combo_row["IsBlockerEntity"] if combo_row else False
        cfg["is_pfic_cfc_qfc_entity"] = combo_row["IsPficCfcQfc"] if combo_row else False
        cfg["is_pfic"] = combo_row["IsPFIC"] if combo_row else False
        cfg["is_cfc_or_qfc"] = combo_row["IsCfcOrQfc"] if combo_row else False

    cfg.setdefault("investment_tag_workflow_id", 0)
    if cfg.get("allocation_type") and cfg["allocation_type"].lower() == "pe book allocation":
        if cfg["investment_tag_workflow_id"] == 0:
            cfg["investment_tag_workflow_id"] = _get_approved_workflow_phase(
                spark, cfg, udf_state, cfg["investment_tag_event_type_id"], entity_id
            )

    log_timing("load_config", t0)
    return cfg
