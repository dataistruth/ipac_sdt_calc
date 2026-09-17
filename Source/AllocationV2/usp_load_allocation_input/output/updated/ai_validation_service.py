"""
ai_validation_service.py

Validation checks for uspLoadAllocationInput.
Validates GP partner, tax capital timestamps, partner link warnings,
PCAP/Financial partner mismatches, and run status.

SQL lines: 1094-2210
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import logging
import threading
import time

from Common_V2.core.helpers import table_prefix, read_table, log_section, log_timing

try:
    from .checkpoint import run_parallel as _run_parallel
except Exception:  # pragma: no cover - fallback when helper import fails
    def _run_parallel(tasks, label):
        return [(name, task()) for name, task in tasks]

logger = logging.getLogger(__name__)

# Serializes the (rare) AllocationRunErrors Delta appends so the warning checks
# can run concurrently without racing on the same table's commit log.
_ERROR_LOCK = threading.Lock()


def run_validations(spark: SparkSession, cfg: dict, lower_tier_df: DataFrame) -> bool:
    """Run all validation checks. Returns True if SP should continue, False if FAIL.

    SQL lines: 1094-2210
    Checks:
    1. Run status validation (abort if already FAIL)
    2. Tax capital data timestamp warnings
    3. Partner count warnings (extra partners in yearly)
    4. GP partner existence (when rounding logic = 'Plugged to GP')
    5. Lower tier fund partner link warnings
    6. Multiple partner flow-up warnings
    7. PCAP vs Financial/Cost partner mismatch warnings
    8. Financial feed partner not in entity warnings
    9. Multiple upper-tier flow-up warnings
    10. Entity relationship unlinked partner warnings
    """
    log_section("run_validations")
    t0 = time.time()
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    log_id = cfg.get("log_id", 0)

    # Check 1: Run status — if status is already FAIL, abort
    run_status = cfg.get("run_status")
    if run_status and run_status.upper() == "FAIL":
        log_timing("run_validations", t0)
        return False

    # Check 4: GP partner validation (SQL lines 1204-1230). This is the only
    # GATING validation (it can abort the run), so it runs first on the caller
    # thread before the independent warning checks fan out.
    rounding_logic = cfg.get("rounding_logic")
    if rounding_logic and rounding_logic.lower() == "plugged to gp":
        gp_exists = not (
            _entity_partner_rows(spark, cfg)
            .filter(F.upper(F.coalesce(F.col("GPorLP"), F.lit(""))) == "G")
            .isEmpty()
        )
        if not gp_exists:
            msg = "GP Partner does not exist. Please select one of the Partner as GP."
            _insert_run_error(spark, cfg, msg, "Error")
            log_timing("run_validations", t0)
            return False

    # Checks 2,3,5-10 are independent, read-only warning checks that each fire
    # Spark actions (isEmpty/collect/count). Run them in the shared four-thread
    # pool; any AllocationRunErrors append is serialized via _ERROR_LOCK.
    warning_tasks = [
        ("tax_capital", lambda: _check_tax_capital_warning(spark, cfg)),
        ("extra_partners", lambda: _check_extra_partners_warning(spark, cfg)),
        ("lower_tier_partner",
         lambda: _check_lower_tier_partner_warnings(spark, cfg, lower_tier_df)),
        ("multiple_partner_flowup",
         lambda: _check_multiple_partner_flowup(spark, cfg, lower_tier_df)),
        ("pcap_financial_mismatch",
         lambda: _check_pcap_financial_mismatch(spark, cfg)),
        ("financial_partner_not_in_entity",
         lambda: _check_financial_partner_not_in_entity(spark, cfg)),
        ("multiple_upper_tier_flowup",
         lambda: _check_multiple_upper_tier_flowup(spark, cfg)),
        ("entity_relationship_unlinked",
         lambda: _check_entity_relationship_unlinked(spark, cfg)),
    ]
    _run_parallel(warning_tasks, "validations")

    log_timing("run_validations", t0)
    return True


def _check_tax_capital_warning(spark: SparkSession, cfg: dict) -> None:
    """SQL lines 1115-1130: Warn if yearly data was updated after K-1A tax capital."""
    prefix = table_prefix(cfg)
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    tc_txn_id = cfg.get("tax_capital_data_transaction_id", 0)
    yearly_wf_id = cfg.get("yearly_workflow_id", 0)

    if not tc_txn_id or tc_txn_id == 0:
        return

    # Check if GlobalMenu "Do not display K-1A Tax Capital Data Warning" is unchecked
    # Use already-loaded GlobalMenu data from config to avoid redundant read
    gm_df = read_table(spark, "GlobalMenu", cfg)
    enu_gm_df = read_table(spark, "ENU_GlobalMenuGroup", cfg)
    gm_check = not (
        gm_df.alias("GM")
        .join(enu_gm_df.alias("ENU"),
              F.col("GM.GlobalMenuGroupID") == F.col("ENU.GlobalMenuGroupID"), "inner")
        .filter(
            (F.lower(F.col("ENU.GroupName")) == "other configuration") &
            (F.lower(F.col("GM.MenuName")) == "do not display  k-1a tax capital data warning") &
            (F.upper(F.coalesce(F.col("GM.State"), F.lit("U"))) == "U") &
            (F.col("GM.ClientID") == client_id) &
            (F.col("GM.TaxPeriodID") == tax_period_id)
        )
        .isEmpty()
    )
    if not gm_check:
        return

    # Get K1A timestamp and yearly submit date in one query via join
    tl_df = read_table(spark, "TransactionLog", cfg)
    wf_df = read_table(spark, "Workflow", cfg)

    dates_row = not (
        tl_df.filter(F.col("TransactionID") == tc_txn_id)
        .select(F.col("TransactionDate").alias("k1a_date"))
        .crossJoin(
            wf_df.filter(
                (F.col("WorkflowID") == yearly_wf_id) &
                (F.lower(F.col("SubmitByID")) != "k1auser")
            )
            .select(F.col("SubmitDate").alias("yearly_date"))
        )
        .filter(F.col("yearly_date") > F.col("k1a_date"))
        .isEmpty()
    )
    if dates_row:
        _insert_run_error(
            spark, cfg,
            "Yearly Data updated manually after K-1A Tax Capital Data is bridged.",
            "Warning"
        )


def _entity_partner_rows(spark: SparkSession, cfg: dict) -> DataFrame:
    """R3: entity Partner_Snapshot rows via udf_PE_GetPartnersList* WorkFlowID-preferred key
    (udf_PE_GetPartnersListForAllocations L101-111):
      per Partner_Snapshot row, match
        (PS.WorkFlowID<>0 ? PS.WorkFlowID : PS.TransactionID)
          = (PS.WorkFlowID<>0 ? PartnerLatestWorkflowID : PartnerLatestTransactionID)
    where PartnerLatestWorkflowID = udfGetLastSubmittedWorkflow_Phase(partnerImportEvent, entity)
    and   PartnerLatestTransactionID = udfGetLastTransactionIDForPartner(entity) = cfg.partner_transaction_id.
    Returns the FULL keyed rows (so callers can read PartnerNumber, GPorLP, etc.).
    Round 5 R3: the GP-existence check (Check 4) must use this resolver too — a bare
    TransactionID filter misses GP rows keyed by a non-zero WorkFlowID and wrongly FAILs the run.
    """
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    phase_id = cfg.get("phase_id")
    partner_txn_id = cfg.get("partner_transaction_id", 0) or 0

    # Partner import event (udf_PE_GetPartnerImportEventID): Master→MasterImport_Partner else Import_Partner
    partner_method = cfg.get("partner_method", "Fund")
    event_name = "MasterImport_Partner" if partner_method == "Master" else "Import_Partner"
    ev_row = (
        read_table(spark, "ENU_Event", cfg)
        .filter(F.col("EventName") == event_name)
        .select(F.col("EventTypeID")).first()
    )
    partner_event_id = ev_row["EventTypeID"] if ev_row else None

    # PartnerLatestWorkflowID = MAX(WF.WorkflowID) from Workflow⋈TransactionLog (Rejected excluded)
    latest_wf = 0
    if partner_event_id is not None and phase_id is not None:
        rejected_ids = [
            r["StatusID"] for r in
            read_table(spark, "WORKFLOWSTATUS", cfg)
            .filter(F.lower(F.col("EnumerationName")) == "rejected")
            .select("StatusID").collect()
        ]
        wf_row = (
            read_table(spark, "Workflow", cfg).alias("WF")
            .join(
                read_table(spark, "TransactionLog", cfg).alias("TL"),
                (F.col("TL.TransactionID") == F.col("WF.TransactionID")) &
                (F.col("TL.EventTypeID") == partner_event_id) &
                (F.col("TL.PhaseID") == F.col("WF.PhaseID")),
                "inner"
            )
            .filter(
                (F.col("TL.EntityID") == entity_id) &
                (F.col("TL.ClientID") == client_id) &
                (F.col("TL.TaxPeriodID") == tax_period_id) &
                (F.col("TL.PhaseID") == phase_id) &
                (~F.col("TL.StatusID").isin(rejected_ids))
            )
            .select(F.max("WF.WorkflowID").alias("mwf")).first()
        )
        latest_wf = wf_row["mwf"] if wf_row and wf_row["mwf"] is not None else 0

    ps = (
        read_table(spark, "Partner_Snapshot", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.col("EntityID") == entity_id)
        )
    )
    keyed = ps.filter(
        F.when(
            F.coalesce(F.col("WorkFlowID"), F.lit(0)) != 0,
            F.col("WorkFlowID") == F.lit(latest_wf)
        ).otherwise(
            F.coalesce(F.col("TransactionID"), F.lit(0)) == F.lit(partner_txn_id)
        )
    )
    return keyed


def _entity_partner_numbers(spark: SparkSession, cfg: dict) -> DataFrame:
    """Distinct PartnerNumber for the local entity, via the WorkFlowID-preferred resolver."""
    return _entity_partner_rows(spark, cfg).select(F.col("PartnerNumber")).distinct()


def _check_extra_partners_warning(spark: SparkSession, cfg: dict) -> None:
    """SQL lines 1169-1175: Warn if # partners in yearly > # partners of entity."""
    yearly_wf_id = cfg.get("yearly_workflow_id", 0)
    partner_txn_id = cfg.get("partner_transaction_id", 0)

    if not yearly_wf_id:
        return

    yearly_partners = (
        read_table(spark, "Yearly_Snapshot", cfg)
        .filter(F.col("WorkflowID") == yearly_wf_id)
        .select(F.col("PartnerNumber")).distinct()
    )
    entity_partners = (
        read_table(spark, "Partner_Snapshot", cfg)
        .filter(F.col("TransactionID") == partner_txn_id)
        .select(F.col("PartnerNumber")).distinct()
    )
    extra = yearly_partners.alias("Y").join(
        entity_partners.alias("EP"),
        F.col("Y.PartnerNumber").eqNullSafe(F.col("EP.PartnerNumber")),
        "left_anti"
    )
    if not extra.isEmpty():
        _insert_run_error(
            spark, cfg,
            "Number of partners with an allocation % is greater than total number of partners",
            "Warning"
        )


def _check_lower_tier_partner_warnings(spark: SparkSession, cfg: dict, lower_tier_df: DataFrame) -> None:
    """SQL lines 1947-1965: Warn if lower tier funds have no linked partners or multiple."""
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    entity_name = cfg.get("entity_display_name", "")

    # Warn about lower tiers with no linked partner.
    no_partner = (
        lower_tier_df
        .filter(F.col("PartnerNumber").isNull())
        .select("EntityID")
    )

    entity_df = read_table(spark, "Entity", cfg)
    names = (
        no_partner.alias("NP")
        .join(
            entity_df.filter(F.col("ClientID") == client_id).alias("E"),
            F.col("NP.EntityID") == F.col("E.EntityID"), "inner"
        )
        .select(F.col("E.DisplayName"))
        .collect()
    )
    for row in names:
        _insert_run_error(
            spark, cfg,
            f"No partners in {entity_name} are linked to {row['DisplayName']}.",
            "Warning"
        )

    # Warn if flow-up through multiple partners overall.
    distinct_partners = (
        lower_tier_df
        .filter(F.col("PartnerNumber").isNotNull())
        .select("PartnerNumber")
        .distinct()
        .limit(2)
        .count()
    )
    if distinct_partners > 1:
        _insert_run_error(
            spark, cfg,
            f"Flow up from {entity_name} occurred through multiple partners.",
            "Warning"
        )


def _check_multiple_partner_flowup(spark: SparkSession, cfg: dict, lower_tier_df: DataFrame) -> None:
    """SQL lines 2095-2115: Warn for each lower-tier entity flowing through multiple partners."""
    entity_name = cfg.get("entity_display_name", "")
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]

    multi_partner_entities = (
        lower_tier_df
        .groupBy("EntityID")
        .agg(F.countDistinct("PartnerNumber").alias("cnt"))
        .filter(F.col("cnt") > 1)
        .select("EntityID")
    )

    entity_df = read_table(spark, "Entity", cfg)
    rows = (
        multi_partner_entities.alias("M")
        .join(
            entity_df.filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            ).alias("E"),
            F.col("M.EntityID") == F.col("E.EntityID"), "inner"
        )
        .select(F.col("E.DisplayName"))
        .collect()
    )
    for row in rows:
        _insert_run_error(
            spark, cfg,
            f"Two or more partners from {row['DisplayName']} are flowing up to {entity_name}.",
            "Warning"
        )


def _check_pcap_financial_mismatch(spark: SparkSession, cfg: dict) -> None:
    """SQL lines 2007-2049: Warn if partners in Financial/Cost % but missing in PCAP."""
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    pcap_wf_id = cfg.get("_pcap_workflow_id", 0)
    financial_wf_id = cfg.get("_financial_workflow_id", 0)
    cost_wf_id = cfg.get("cost_percentage_workflow_id", 0)

    # These workflow IDs may not be loaded yet. Try to compute them.
    if not pcap_wf_id and not financial_wf_id:
        return
    if not pcap_wf_id:
        return

    # Get PCAP partners
    pcap_partners = (
        read_table(spark, "DF_PCAP_Archive", cfg)
        .filter(F.col("WorkflowID") == pcap_wf_id)
        .select(F.col("InvestorID").cast("string").alias("InvestorID"))
        .distinct()
    )

    # Get Financial + Cost partners
    fin_cost_parts = []
    if financial_wf_id:
        fin_df = (
            read_table(spark, "DF_FinancialAlloc_Archive", cfg)
            .filter(
                (F.col("WorkflowID") == financial_wf_id) &
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id) &
                (F.col("DrCrAmount") != 0)
            )
            .select(F.col("InvestorID").cast("string").alias("InvestorID"))
        )
        fin_cost_parts.append(fin_df)

    if cost_wf_id:
        run_id = cfg["run_id"]
        asset_class_ids = (
            read_table(spark, "ENU_UnderlyingType", cfg)
            .filter(F.upper(F.col("UnderlyingType")) == "ASSET CLASS")
            .select(F.col("UnderlyingTypeID"))
        )
        entity_tbl = read_table(spark, "Entity", cfg).filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id)
        )
        valid_entity_ids = entity_tbl.select(F.col("EntityID").alias("CE_EntityID"))

        # base: workflow rows, CommitmentPercent<>0, holding EntityID is a valid entity
        cost_base = (
            read_table(spark, "CostPercentage_Snapshot", cfg)
            .filter((F.col("WorkflowID") == cost_wf_id) & (F.col("CommitmentPercent") != 0))
            .alias("C")
            .join(valid_entity_ids.alias("E1"), F.col("C.EntityId") == F.col("E1.CE_EntityID"), "inner")
        )
        non_asset_class_ids = (
            read_table(spark, "ENU_UnderlyingType", cfg)
            .filter(F.upper(F.col("UnderlyingType")) != "ASSET CLASS")
            .select(F.col("UnderlyingTypeID").alias("NA_UnderlyingTypeID"))
        )
        non_asset = cost_base.join(
            non_asset_class_ids.alias("NAC"),
            F.col("C.Underlyingtype") == F.col("NAC.NA_UnderlyingTypeID"), "inner"
        )

        b1 = non_asset.filter(F.col("C.InvestmentID") == -1) \
            .select(F.col("C.PartnerNumber").cast("string").alias("InvestorID"))
        b2 = (
            non_asset.filter(~F.col("C.InvestmentID").isin(-1, -2))
            .join(entity_tbl.alias("E2"), F.col("C.InvestmentID") == F.col("E2.EntityID"), "inner")
            .select(F.col("C.PartnerNumber").cast("string").alias("InvestorID"))
        )
        asset_only = cost_base.join(
            asset_class_ids.alias("AC2"),
            F.col("C.Underlyingtype") == F.col("AC2.UnderlyingTypeID"), "left_semi"
        )
        b3 = (
            asset_only.alias("CA")
            .join(read_table(spark, "Enu_AssetClass", cfg).alias("EA"),
                  F.col("CA.InvestmentID") == F.col("EA.AssetClassID"), "inner")
            .select(F.col("CA.PartnerNumber").cast("string").alias("InvestorID"))
        )
        cost_branches = [b1, b2, b3]

        # Bdeal — deal-level (InvestmentID = -2) Custom10 expansion, FAITHFUL to the UDF.
        # SQL (UDF L79-150): @EntityHierarchy is a recursive descendants closure rooted at
        # EACH cost-row EntityId that has DealID<>'' (@tmpSelectedEntities), plus the root
        # itself; @TempEntityDeals = (root EntityID, descendant.Custom10) for non-empty
        # Custom10; the deal INSERT matches `C.DealID = Custom10 AND C.EntityId = root`.
        # So the match is ROOT-KEYED: a -2 cost row qualifies only when its DealID equals a
        # Custom10 of an entity in the hierarchy rooted at THAT row's own EntityId — not just
        # any Custom10 anywhere. Build the per-root closure from EntityRelationship (BFS),
        # mirroring the CTE, instead of the local-entity-only _entity_hierarchy view.
        deal_src = non_asset.filter(
            (F.col("C.InvestmentID") == -2) &
            (F.coalesce(F.col("C.DealID"), F.lit("")) != "")
        )
        deal_roots = [r["EntityId"] for r in deal_src.select(F.col("C.EntityId").alias("EntityId")).distinct().collect()]
        deal_roots = [r for r in deal_roots if r is not None and r != -1]
        if deal_roots:
            rels = (
                read_table(spark, "EntityRelationship", cfg)
                .select("UpperTierEntityID", "LowerTierEntityID")
                .collect()
            )
            children = {}
            for rr in rels:
                children.setdefault(rr["UpperTierEntityID"], []).append(rr["LowerTierEntityID"])
            # (root, descendant) closure incl. the root itself (SQL self-row L112-115)
            root_desc = set()
            for root in deal_roots:
                root_desc.add((root, root))
                frontier = [root]
                seen = {root}
                while frontier:
                    nxt = []
                    for u in frontier:
                        for c in children.get(u, []):
                            root_desc.add((root, c))
                            if c not in seen:
                                seen.add(c)
                                nxt.append(c)
                    frontier = nxt
            pairs_df = spark.createDataFrame(
                list(root_desc), "DealRoot int, Descendant int"
            )
            deal_map = (
                pairs_df.alias("P")
                .join(entity_tbl.alias("ED"), F.col("P.Descendant") == F.col("ED.EntityID"), "inner")
                .filter(F.coalesce(F.col("ED.Custom10"), F.lit("")) != "")
                .select(F.col("P.DealRoot"), F.col("ED.Custom10").alias("DealCustom10")).distinct()
            )
            b_deal = (
                deal_src
                .join(
                    deal_map,
                    (F.col("C.EntityId") == F.col("DealRoot")) &
                    (F.col("C.DealID") == F.col("DealCustom10")),
                    "inner"
                )
                .select(F.col("C.PartnerNumber").cast("string").alias("InvestorID"))
            )
            cost_branches.append(b_deal)

        from functools import reduce as _reduce
        cost_df = _reduce(DataFrame.unionByName, cost_branches).distinct()
        fin_cost_parts.append(cost_df)

    if not fin_cost_parts:
        return

    from functools import reduce
    fin_cost_all = reduce(DataFrame.unionByName, fin_cost_parts).distinct()

    missing_in_pcap = (
        fin_cost_all.alias("FC")
        .join(pcap_partners.alias("P"), F.col("FC.InvestorID") == F.col("P.InvestorID"), "left_anti")
    )
    missing_rows = missing_in_pcap.collect()
    for row in missing_rows:
        _insert_run_error(
            spark, cfg,
            f"Partner {row['InvestorID']} exists in either Financial feed or investment cost % but missing in PCAP.",
            "Warning"
        )


def _check_financial_partner_not_in_entity(spark: SparkSession, cfg: dict) -> None:
    """SQL lines 2049-2095: Warn if partner in financial feed but not in entity."""
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    phase_id = cfg.get("phase_id")

    # Get max financial workflow for this entity
    entity_df = read_table(spark, "Entity", cfg)
    vw_fin = read_table(spark, "DF_FinancialAlloc_Archive", cfg)

    entity_identification = (
        entity_df.filter(
            (F.col("EntityID") == entity_id) &
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id)
        )
        .select("EntityIdentification")
        .first()
    )
    if not entity_identification:
        return

    entity_ident_val = entity_identification["EntityIdentification"]

    max_wf_row = (
        vw_fin.alias("D")
        .join(entity_df.alias("E"),
              (F.col("D.entityid") == F.col("E.EntityIdentification")) &
              (F.col("E.EntityID") == entity_id) &
              (F.col("E.ClientID") == client_id) &
              (F.col("E.TaxPeriodID") == tax_period_id),
              "inner")
        .agg(F.max("D.WorkflowID").alias("max_wf"))
        .first()
    )
    if not max_wf_row or not max_wf_row["max_wf"]:
        return

    max_wf = max_wf_row["max_wf"]
    fin_partners = (
        vw_fin
        .filter(F.col("WorkflowID") == max_wf)
        .select(F.col("InvestorID")).distinct()
    )

    entity_partners = (
        _entity_partner_numbers(spark, cfg)
        .select(F.col("PartnerNumber").alias("InvestorID"))
    )

    missing = (
        fin_partners.alias("FD")
        .join(entity_partners.alias("P"), F.col("FD.InvestorID") == F.col("P.InvestorID"), "left_anti")
    )
    missing_rows = missing.collect()
    for row in missing_rows:
        _insert_run_error(
            spark, cfg,
            f"{row['InvestorID']} : Partner exists in financial feed, but is not present in this entity.",
            "Warning"
        )


def _check_multiple_upper_tier_flowup(spark: SparkSession, cfg: dict) -> None:
    """SQL lines 2177-2199: Warn if multiple partners linked to same upper-tier entity."""
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    entity_name = cfg.get("entity_display_name", "")
    partner_txn_id = cfg.get("partner_transaction_id", 0)

    if not partner_txn_id:
        return

    partner_df = read_table(spark, "Partner_Snapshot", cfg)
    entity_df = read_table(spark, "Entity", cfg)
    er_df = read_table(spark, "EntityRelationShip", cfg)

    # Find entities where multiple partners match via EIN or UpperTierEntityIdentification.
    ranked = (
        partner_df.filter(
            (F.col("EntityID") == entity_id) &
            (F.col("TransactionID") == partner_txn_id)
        )
        .alias("P")
        .join(
            entity_df.filter(F.col("ClientID") == client_id).alias("E"),
            (
                (F.col("E.EntityIdentification") == F.col("P.UpperTierEntityIdentification")) &
                F.col("P.UpperTierEntityIdentification").isNotNull() &
                (F.trim(F.col("P.UpperTierEntityIdentification")) != "")
            ) | (
                (F.regexp_replace(F.coalesce(F.col("E.EIN"), F.lit("")), "[-\\s]", "") ==
                 F.regexp_replace(F.coalesce(F.col("P.EIN"), F.lit("")), "[-\\s]", "")) &
                F.col("P.EIN").isNotNull() &
                (F.trim(F.col("P.EIN")) != "") &
                (F.col("E.EntityIdentification") == F.when(
                    F.trim(F.coalesce(F.col("P.UpperTierEntityIdentification"), F.lit(""))) != "",
                    F.col("P.UpperTierEntityIdentification")
                ).otherwise(F.col("E.EntityIdentification")))
            ),
            "inner"
        )
        .select(
            F.col("E.EntityID").alias("EntityID"),
            F.col("E.EIN").alias("EIN"),
            F.col("E.DisplayName").alias("DisplayName"),
            F.col("E.EntityIdentification").alias("EntityIdentification"),
            F.col("P.PartnerNumber").alias("PartnerNumber"),
        )
        .distinct()
        .withColumn(
            "RNK",
            F.rank().over(Window.partitionBy("DisplayName").orderBy("PartnerNumber"))
        )
        .filter(F.col("RNK") > 1)
    )
    upper_tier_multi = (
        ranked.alias("T")
        .join(
            er_df.filter(
                (F.col("LowerTierEntityID") == entity_id) &
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            ).alias("ER"),
            F.col("T.EntityID") == F.col("ER.UpperTierEntityID"), "inner"
        )
        .filter(F.col("T.EntityID") != entity_id)
        .select(F.col("T.EntityID").alias("EntityID"), F.col("T.DisplayName").alias("DisplayName"))
        .distinct()
        .collect()
    )

    for row in upper_tier_multi:
        _insert_run_error(
            spark, cfg,
            f"Two or more partners from {entity_name} are linked to {row['DisplayName']}.",
            "Warning"
        )


def _check_entity_relationship_unlinked(spark: SparkSession, cfg: dict) -> None:
    """SQL lines 2199-2210: Warn if entity relationship exists but no partner linked."""
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    entity_name = cfg.get("entity_display_name", "")
    partner_txn_id = cfg.get("partner_transaction_id", 0)

    if not partner_txn_id:
        return

    partner_df = read_table(spark, "Partner_Snapshot", cfg)
    entity_df = read_table(spark, "Entity", cfg)
    er_df = read_table(spark, "EntityRelationShip", cfg)

    upper_tiers = (
        er_df.filter(F.col("LowerTierEntityID") == entity_id)
        .select("UpperTierEntityID")
    )

    # Get entities linked via partner EIN OR UpperTierEntityIdentification
    linked_entities = (
        partner_df.filter(
            (F.col("EntityID") == entity_id) &
            (F.col("TransactionID") == partner_txn_id)
        )
        .alias("P")
        .join(
            entity_df.filter(F.col("ClientID") == client_id).alias("E"),
            (
                (F.regexp_replace(F.coalesce(F.col("E.EIN"), F.lit("")), "[-\\s]", "") ==
                 F.regexp_replace(F.coalesce(F.col("P.EIN"), F.lit("")), "[-\\s]", "")) &
                F.col("P.EIN").isNotNull() &
                (F.trim(F.col("P.EIN")) != "")
            ) | (
                (F.col("E.EntityIdentification") == F.col("P.UpperTierEntityIdentification")) &
                F.col("P.UpperTierEntityIdentification").isNotNull() &
                (F.trim(F.col("P.UpperTierEntityIdentification")) != "")
            ),
            "inner"
        )
        .select(F.col("E.EntityID"))
        .distinct()
    )

    # Upper tiers NOT in linked entities
    unlinked = (
        upper_tiers.alias("UT")
        .join(linked_entities.alias("L"), F.col("UT.UpperTierEntityID") == F.col("L.EntityID"), "left_anti")
    )
    if not unlinked.isEmpty():
        entity_names = (
            unlinked.alias("U")
            .join(
                entity_df.filter(F.col("ClientID") == client_id).alias("E"),
                F.col("U.UpperTierEntityID") == F.col("E.EntityID"), "inner"
            )
            .select(F.col("E.DisplayName"))
            .collect()
        )
        for row in entity_names:
            _insert_run_error(
                spark, cfg,
                f"No partners in {entity_name} are linked to {row['DisplayName']}.",
                "Warning"
            )


def _insert_run_error(spark: SparkSession, cfg: dict, message: str, error_type: str = "Error") -> None:
    """Insert a row into AllocationRunErrors."""
    from Common_V2.core.helpers import table_prefix
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    log_id = cfg.get("log_id", 0)

    error_df = spark.createDataFrame(
        [(run_id, entity_id, message, log_id, error_type)],
        ["RunID", "EntityID", "ErrorMessage", "LogID", "ErrororWarning"]
    )
    error_df = (
        error_df
        .withColumn("RunID", F.col("RunID").cast("long"))
        .withColumn("EntityID", F.col("EntityID").cast("int"))
        .withColumn("LogID", F.col("LogID").cast("int"))
    )
    # Serialize the Delta append: warning checks run on multiple threads and
    # concurrent commits to the same table would otherwise race.
    with _ERROR_LOCK:
        error_df.write.format("delta").mode("append").saveAsTable(
            f"{prefix}.AllocationRunErrors"
        )
