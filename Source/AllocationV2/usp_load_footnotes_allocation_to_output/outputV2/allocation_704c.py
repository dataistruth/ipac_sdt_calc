"""
704c allocation logic for uspLoadFootnotesAllocationToOutput.

Handles the conditional 704c block that only executes when:
- RankForRulePickup = 2
- AllocationTypeName = 'PE Book Allocation'
- AllocationTypeName704c is configured (non-empty)
"""
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0, sql_round
from Common_V2.core.observability import log_section, log_timing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# build_704c_config
# SQL lines: 1750–1850
# Row count: N/A (config function)
# ---------------------------------------------------------------------------
def build_704c_config(spark: SparkSession, cfg: dict) -> dict:
    """Load 704c-specific configuration (conditional on rank=2 + PE Book).

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 1750-1850.
    Loads: AllocationTypeName704c, IsSeparateGainsLoss, CapitalLossLineID,
    GAAPToTaxLineTypeID, allocationTypeIDfor704c, custom footnote line types.
    Returns updated cfg dict with 704c config values.

    Precondition: only called when rank_for_rule_pickup=2 AND allocation_type_name='PE Book Allocation'
    """
    log_section("build_704c_config")
    t0 = time.time()

    # @AllocationTypeName704c — from pre-resolved entity 704c name in cfg.
    alloc_type_name_704c = (cfg.get("entity_704c_allocation_type_name") or "").strip()
    cfg["allocation_type_name_704c"] = alloc_type_name_704c

    # Check if 704c is actually enabled
    alloc_type_name = (cfg.get("allocation_type_name") or "").strip().lower()
    is_704c_enabled = (
        alloc_type_name == "pe book allocation"
        and alloc_type_name_704c != ""
        and cfg["rank_for_rule_pickup"] == 2
    )
    cfg["is_704c_enabled"] = is_704c_enabled

    if not is_704c_enabled:
        cfg["capital_loss_line_id"] = 0
        cfg["is_separate_gains_loss"] = False
        cfg["gaap_to_tax_line_type_id"] = None
        cfg["allocation_type_id_for_704c"] = None
        log_timing("build_704c_config", t0)
        return cfg

    # @CapitalLossLineID — from pre-resolved Form8886 active line.
    cfg["capital_loss_line_id"] = cfg.get("form8886_capital_loss_line_id") or 0

    # @GAAPToTaxLineTypeID — from cfg scalar.
    cfg["gaap_to_tax_line_type_id"] = cfg.get("gaap_to_tax_line_type_id")

    # @IsSeparateGainsLoss — from pre-resolved GlobalMenu flag.
    cfg["is_separate_gains_loss"] = (
        cfg.get("flag_separate_gains_loss_stuffing") == "C"
    )

    # @allocationTypeIDfor704c — from pre-resolved custom allocation ID.
    cfg["allocation_type_id_for_704c"] = cfg.get("custom_allocation_id_704c")

    log_timing("build_704c_config", t0)
    return cfg


# ---------------------------------------------------------------------------
# build_custom_footnote_line_types
# SQL lines: 1768–1772
# ---------------------------------------------------------------------------
def build_custom_footnote_line_types(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load custom footnote line types from udfGetLatestCustomFootnoteTransactionIDs.

    SQL: SELECT DISTINCT LineTypeID FROM dbo.udfGetLatestCustomFootnoteTransactionIDs(...)
    The UDF (ELSE branch, @RegisterTypeID=-1) does:
      CustomImportDetail CD (WHERE IsCustomFootnote=1)
      JOIN ENU_LineType EL ON EL.LineType = CD.ImportName
    Returns DataFrame with single column: LineTypeID.
    """
    log_section("build_custom_footnote_line_types")

    # BUG-04 FIX: SQL UDF derives LineTypeIDs from CustomImportDetail + ENU_LineType,
    # not from CustomFootnoteLineItem.RegisterTypeID.
    df_cid = (
        read_table(spark, "CustomImportDetail", cfg)
        .filter(F.col("IsCustomFootnote") == F.lit(True))
        .select(F.col("ImportName"))
    )
    df_elt = read_table(spark, "ENU_LineType", cfg).select("LineTypeID", "LineType")
    df = (
        df_cid.join(df_elt, df_cid["ImportName"] == df_elt["LineType"], "inner")
        .select("LineTypeID")
        .distinct()
    )
    return df


# ---------------------------------------------------------------------------
# build_allocation_percentage_temp
# SQL lines: 1815–1845
# Row count: LEGITIMATELY-EMPTY
# ---------------------------------------------------------------------------
def build_allocation_percentage_temp(
    spark: SparkSession, cfg: dict,
) -> DataFrame:
    """Build allocation percentages via PIVOT on FNFinalEffectivePercentages.704cPercentageType.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 1815-1845.
    Row count: LEGITIMATELY-EMPTY — only when 704c is configured.
    PIVOTs: OrdinaryPercentage, CapitalPercentage, CapitalGainPercentage, CapitalLossPercentage.

    Output columns:
        RunID, ClientID, EntityID, InvestmentID, PartnerNumber, LineID,
        Quarter, TrackingKey, OrdinaryPercentage, CapitalPercentage,
        CapitalGainPercentage, CapitalLossPercentage, TypeID, LineTypeID
    """
    log_section("build_allocation_percentage_temp")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]

    # SELECT * FROM FNFinalEffectivePercentages WHERE RunID=@LocalRunID
    #   AND ISNULL([704cPercentageType],'') <> '' AND @rankforrulepickup=2
    fn_eff = (
        read_table(spark, "FNFinalEffectivePercentages", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (ns(F.col("`704cPercentageType`"), F.lit("")) != "")
        )
        .select(
            "RunID", "EntityID", "InvestmentID", "PartnerNumber", "LineID",
            "Quarter",
            F.col("Trackingkey").alias("TrackingKey"),
            F.col("TypeId").alias("TypeID"),
            "LineTypeID",
            "EffPercentage", "`704cPercentageType`",
        )
    )

    # PIVOT: MAX(EffPercentage) FOR 704cPercentageType IN (...)
    pivoted = (
        fn_eff
        .groupBy(
            "RunID", "EntityID", "InvestmentID", "PartnerNumber", "LineID",
            "Quarter", "TrackingKey", "TypeID", "LineTypeID",
        )
        .pivot("`704cPercentageType`", [
            "OrdinaryPercentage", "CapitalPercentage",
            "CapitalGainPercentage", "CapitalLossPercentage",
        ])
        .agg(F.max("EffPercentage"))
    )

    # Add ClientID + ShareClass (ShareClass is in the SQL #AllocationPercentageTemp
    # schema but the INSERT omits it, so SQL always reads it as NULL — match that).
    df = (
        pivoted
        .withColumn("ClientID", F.lit(client_id))
        .withColumn("ShareClass", F.lit(None).cast("string"))
    )

    # Rename pivot columns to standard names (in case they differ)
    for col_name in ["OrdinaryPercentage", "CapitalPercentage",
                     "CapitalGainPercentage", "CapitalLossPercentage"]:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast("double"))

    log_timing("build_allocation_percentage_temp", t0)
    return df


# ---------------------------------------------------------------------------
# build_704c_allocation_output
# SQL lines: 1850–2070
# Row count: LEGITIMATELY-EMPTY
# ---------------------------------------------------------------------------
def build_704c_allocation_output(
    spark: SparkSession, cfg: dict,
    df_alloc_input: DataFrame,
    df_alloc_pct: DataFrame,
    df_custom_fn_types: DataFrame,
) -> DataFrame:
    """Generate 704c allocation output for PFIC, Custom Footnotes, and other form types.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 1850-2070.
    Row count: LEGITIMATELY-EMPTY — only when rank=2 AND PE Book AND 704c configured.

    Three INSERT blocks:
    1. PFIC Footnote lines — use K1/PFICFootnoteLineItem.Classification (Ordinary vs Capital)
    2. Custom Footnote lines — use CustomFootnoteLineItem.IsOrdinary/IsCapitalGain/IsCapitalLoss
    3. Other line types (Form926, Form8865, Form199A, Form8886, GAAP, AtRisk)
       — complex CASE based on AllocationTypeName704c setting

    Also applies TypeID update: Cost/LP/GP → 704c type

    Output columns (same as #tmpAllocationOutput):
        RunID, ClientID, EntityID, ShareClass, PartnerNumber, LineTypeID,
        QuicklinkID, LineID, Amount, AllocationType, ParentEntityID,
        SuperParentEntityID, AllocationTypeID, TrackingKey, OriginalParentEntityID
    """
    log_section("build_704c_allocation_output")
    t0 = time.time()

    if not cfg.get("is_704c_enabled"):
        log_timing("build_704c_allocation_output", t0)
        return None

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    pfic_lt = cfg["pfic_footnote_line_type_id"]
    at_risk_lt = cfg["at_risk_line_type_id"]
    f926_lt = cfg["form926_line_type_id"]
    f8865_lt = cfg["form8865_line_type_id"]
    f199a_lt = cfg["form199a_line_type_id"]
    f8886_lt = cfg["form8886_line_type_id"]
    gaap_lt = cfg.get("gaap_to_tax_line_type_id")
    cost_at = cfg["cost_allocation_type_id"]
    lp_at = cfg["lp_offset_allocation_type_id"]
    gp_at = cfg["gp_offset_allocation_type_id"]
    alloc_704c = cfg["allocation_type_id_for_704c"]
    cap_loss_lid = cfg.get("capital_loss_line_id", 0)
    is_sep = cfg.get("is_separate_gains_loss", False)
    alloc_name_704c = (cfg.get("allocation_type_name_704c") or "").strip()

    # ── UPDATE TypeID on AllocationInput: Cost/LP/GP → 704c ──
    df_alloc_input = df_alloc_input.withColumn(
        "TypeID",
        F.when(
            F.col("TypeID").isin([cost_at, lp_at, gp_at]),
            F.lit(alloc_704c),
        ).otherwise(F.col("TypeID")),
    )

    # ── Common join condition for alloc_pct ──
    # ON AI.RunID=AP.RunID AND AI.EntityID=AP.InvestmentID AND AI.ClientID=AP.ClientID
    # AND AI.TypeID=AP.TypeID AND ISNULL(AI.LineTypeID,-1)=ISNULL(AP.LineTypeID,-1)
    # AND ISNULL(AI.TrackingKey,'')=ISNULL(AP.TrackingKey,'')
    ai = df_alloc_input.alias("AI")
    ap = df_alloc_pct.alias("AP")

    join_cond = (
        (F.col("AI.RunID") == F.col("AP.RunID"))
        & (F.col("AI.EntityID") == F.col("AP.InvestmentID"))
        & (F.col("AI.ClientID") == F.col("AP.ClientID"))
        & (F.col("AI.TypeID") == F.col("AP.TypeID"))
        & (ns0(F.col("AI.LineTypeID")) == ns0(F.col("AP.LineTypeID")))
        & (ns(F.col("AI.TrackingKey"), F.lit("")) == ns(F.col("AP.TrackingKey"), F.lit("")))
    )

    # ── INSERT 1: PFIC Footnote ──
    pfic_li = (
        cfg["_df_pfic_footnote_line_item"]
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select(F.col("LineID").alias("k1_lid"), F.col("Classification"))
    )

    pfic_join = (
        ai.filter(F.col("AI.LineTypeID") == pfic_lt)
        .join(ap, join_cond, "inner")
        .join(pfic_li, F.col("AI.LineID") == pfic_li["k1_lid"], "inner")
    )

    pfic_amount = (
        F.col("AI.Amount") * F.when(
            F.lower(F.col("Classification")) == "ordinary",
            F.coalesce(F.col("AP.OrdinaryPercentage"), F.lit(0.0)),
        ).when(
            (F.lower(ns(F.col("Classification"), F.lit(""))) != "ordinary") & (F.lit(not is_sep)),
            F.coalesce(F.col("AP.CapitalPercentage"), F.lit(0.0)),
        ).otherwise(
            F.when(F.col("AI.Amount") > 0, F.col("AP.CapitalGainPercentage"))
            .otherwise(F.col("AP.CapitalLossPercentage"))
        )
    )

    insert_pfic = pfic_join.select(
        F.lit(run_id).cast("long").alias("RunID"), F.lit(client_id).alias("ClientID"),
        F.col("AI.EntityID"), F.col("AP.ShareClass"), F.col("AP.PartnerNumber"),
        F.col("AI.LineTypeID"), F.col("AI.QuicklinkID"), F.col("AI.LineID"),
        pfic_amount.alias("Amount"),
        F.lit("704c Footnote").alias("AllocationType"),
        F.col("AI.ParentEntityID"), F.coalesce(F.col("AI.SuperParentEntityID"), F.lit(0)).alias("SuperParentEntityID"),
        F.col("AP.TypeID").alias("AllocationTypeID"),
        ns(F.col("AI.TrackingKey"), F.lit("")).alias("TrackingKey"),
        F.col("AI.OriginalParentEntityID"),
    )

    # ── INSERT 2: Custom Footnote ──
    cfl = (
        read_table(spark, "CustomFootnoteLineItem", cfg)
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select(
            F.col("LineID").alias("cfl_lid"),
            F.col("IsOrdinary"), F.col("IsCapitalGain"), F.col("IsCapitalLoss"),
        )
    )

    custom_join = (
        ai.join(ap, join_cond, "inner")
        .join(cfl, F.col("AI.LineID") == cfl["cfl_lid"], "inner")
        .join(df_custom_fn_types.alias("CFT"), F.col("AI.LineTypeID") == F.col("CFT.LineTypeID"), "inner")
    )

    custom_amount = (
        F.col("AI.Amount") * F.when(
            F.coalesce(F.col("IsOrdinary"), F.lit(False)) == F.lit(True),
            F.coalesce(F.col("AP.OrdinaryPercentage"), F.lit(0.0)),
        ).when(
            (F.coalesce(F.col("IsOrdinary"), F.lit(False)) != F.lit(True)) & (F.lit(not is_sep)),
            F.coalesce(F.col("AP.CapitalPercentage"), F.lit(0.0)),
        ).otherwise(
            F.when(
                F.coalesce(F.col("IsCapitalGain"), F.lit(False)) == F.lit(True),
                F.col("AP.CapitalGainPercentage"),
            ).when(
                F.coalesce(F.col("IsCapitalLoss"), F.lit(False)) == F.lit(True),
                F.col("AP.CapitalLossPercentage"),
            ).otherwise(F.lit(0.0))
        )
    )

    insert_custom = custom_join.select(
        F.lit(run_id).cast("long").alias("RunID"), F.lit(client_id).alias("ClientID"),
        F.col("AI.EntityID"), F.col("AP.ShareClass"), F.col("AP.PartnerNumber"),
        F.col("AI.LineTypeID"), F.col("AI.QuicklinkID"), F.col("AI.LineID"),
        custom_amount.alias("Amount"),
        F.lit("704c Footnote").alias("AllocationType"),
        # BUG-05 FIX: SQL column order maps AI.CategoryID → ParentEntityID,
        # AI.ParentEntityId → SuperParentEntityID, ISNULL(AI.SuperParentEntityID,0) → AllocationTypeID
        F.col("AI.CategoryID").alias("ParentEntityID"),
        F.col("AI.ParentEntityID").alias("SuperParentEntityID"),
        F.coalesce(F.col("AI.SuperParentEntityID"), F.lit(0)).alias("AllocationTypeID"),
        ns(F.col("AI.TrackingKey"), F.lit("")).alias("TrackingKey"),
        F.col("AI.OriginalParentEntityID"),
    )

    # ── INSERT 3: Other form types ──
    other_lts = [f926_lt, f8865_lt, f199a_lt, f8886_lt, at_risk_lt]
    if gaap_lt:
        other_lts.append(gaap_lt)

    k1_item = (
        read_table(spark, "K1Lineitem", cfg)
        .filter(F.col("ClientID") == client_id)
        .select(
            F.col("LineID").alias("k1l_lid"),
            F.lower(ns(F.col("Classification"), F.lit(""))).alias("k1_class"),
            F.lower(ns(F.col("CapitalGainLoss"), F.lit(""))).alias("k1_cgl"),
        )
    )

    other_join = (
        ai.filter(
            (F.col("AI.LineTypeID").isin(other_lts))
            & (F.col("AI.Amount") != 0)
        )
        .join(ap, join_cond, "inner")
        .join(k1_item, (F.col("AI.LineID") == k1_item["k1l_lid"])
              & (F.col("AI.LineTypeID") == at_risk_lt), "left")
    )

    # Complex CASE for Amount calculation
    # Depends on AllocationTypeName704c and line type. Compare case-insensitively
    # to match SQL collation behavior under UTF8_BINARY.
    _alloc_name_704c_lc = alloc_name_704c.lower()
    is_sp = _alloc_name_704c_lc == "704(c) - sp"
    is_agg_trading = _alloc_name_704c_lc == "aggregate 704(c) partial netting with trading income"

    other_amount = F.when(
        ~F.lit(is_sp),
        # Not SP path
        F.when(
            F.lit(is_agg_trading),
            F.when(
                (F.col("AI.LineTypeID") == f8886_lt) & (F.col("AI.LineID") == cap_loss_lid),
                F.col("AI.Amount") * F.coalesce(F.col("AP.CapitalLossPercentage"), F.lit(0.0)),
            ).otherwise(F.col("AI.Amount") * F.coalesce(F.col("AP.OrdinaryPercentage"), F.lit(0.0))),
        ).otherwise(
            F.when(
                (F.col("AI.LineTypeID") == f8886_lt) & (F.col("AI.LineID") == cap_loss_lid),
                F.col("AI.Amount") * F.coalesce(F.col("AP.CapitalPercentage"), F.lit(0.0)),
            ).otherwise(F.col("AI.Amount") * F.coalesce(F.col("AP.OrdinaryPercentage"), F.lit(0.0)))
        ),
    ).when(
        # SP + Form8886 CapitalLoss
        (F.col("AI.LineTypeID") == f8886_lt) & (F.col("AI.LineID") == cap_loss_lid),
        ns0(F.col("AI.Amount")) * F.when(
            F.lit(is_sep), F.coalesce(F.col("AP.CapitalPercentage"), F.lit(0.0))
        ).otherwise(F.col("AP.CapitalLossPercentage")),
    ).when(
        # SP + At Risk
        F.col("AI.LineTypeID") == at_risk_lt,
        ns0(F.col("AI.Amount")) * F.when(
            F.col("k1_class") == "ordinary", F.coalesce(F.col("AP.OrdinaryPercentage"), F.lit(0.0)),
        ).when(
            (F.col("k1_class") != "ordinary") & (F.lit(not is_sep)),
            F.coalesce(F.col("AP.CapitalPercentage"), F.lit(0.0)),
        ).when(
            (F.col("k1_class") == "capital") & (F.col("k1_cgl") == "capital gain"),
            F.col("AP.CapitalGainPercentage"),
        ).when(
            (F.col("k1_class") == "capital") & (F.col("k1_cgl") == "capital loss"),
            F.col("AP.CapitalLossPercentage"),
        ).otherwise(
            F.when(F.col("AI.Amount") > 0, F.col("AP.CapitalGainPercentage"))
            .otherwise(F.col("AP.CapitalLossPercentage"))
        ),
    ).otherwise(
        # SP + other: gain/loss split
        F.when(F.col("AI.Amount") > 0, ns0(F.col("AI.Amount")) * F.col("AP.CapitalGainPercentage"))
        .otherwise(ns0(F.col("AI.Amount")) * F.col("AP.CapitalLossPercentage"))
    )

    insert_other = other_join.select(
        F.lit(run_id).cast("long").alias("RunID"), F.lit(client_id).alias("ClientID"),
        F.col("AI.EntityID"), F.col("AP.ShareClass"), F.col("AP.PartnerNumber"),
        F.col("AI.LineTypeID"), F.col("AI.QuicklinkID"), F.col("AI.LineID"),
        other_amount.alias("Amount"),
        F.lit("704c Footnote").alias("AllocationType"),
        F.col("AI.ParentEntityID"), F.coalesce(F.col("AI.SuperParentEntityID"), F.lit(0)).alias("SuperParentEntityID"),
        F.col("AI.TypeID").alias("AllocationTypeID"),
        ns(F.col("AI.TrackingKey"), F.lit("")).alias("TrackingKey"),
        F.col("AI.OriginalParentEntityID"),
    )

    # ── Combine all 704c inserts ──
    # Normalize column order
    output_cols = [
        "RunID", "ClientID", "EntityID", "ShareClass", "PartnerNumber",
        "LineTypeID", "QuicklinkID", "LineID", "Amount", "AllocationType",
        "ParentEntityID", "SuperParentEntityID", "AllocationTypeID",
        "TrackingKey", "OriginalParentEntityID",
    ]

    # Ensure all DataFrames have same columns in same order
    insert_pfic_final = insert_pfic.select(*output_cols)
    insert_custom_final = insert_custom.select(*output_cols)
    insert_other_final = insert_other.select(*output_cols)

    df_result = (
        insert_pfic_final
        .unionByName(insert_custom_final)
        .unionByName(insert_other_final)
    )

    log_timing("build_704c_allocation_output", t0)
    return df_result, df_alloc_input


# ---------------------------------------------------------------------------
# apply_704c_deduction
# SQL lines: 2071–2113
# Row count: LEGITIMATELY-EMPTY
# ---------------------------------------------------------------------------
def apply_704c_deduction(
    spark: SparkSession, cfg: dict,
    df_alloc_input: DataFrame,
    df_704c_output: DataFrame,
    df_zero_exclude: DataFrame,
) -> tuple:
    """Deduct 704c allocated amounts from #AllocationInput (in-memory).

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 2071-2113.
    Row count: LEGITIMATELY-EMPTY — only applicable when 704c produced output.

    Steps:
    1. Aggregate 704c output → AllocationOutputTotal (SUM by composite key)
    2. UPDATE AllocationInput.Amount -= allocated (with Allocate>100% cap logic)
    3. Zero-out residuals (|Amount| < 1.00), excluding ZeroExcludeLines
    4. Track allocated lines in FNAllocatedLines accumulator

    Returns:
        (df_alloc_input_deducted, df_fn_allocated_lines)
    """
    log_section("apply_704c_deduction")
    t0 = time.time()

    if df_704c_output is None or cfg.get("is_704c_enabled") is not True:
        # No 704c output — nothing to deduct
        df_fn_alloc = spark.createDataFrame([], "LineID INT, LineTypeID INT, RunID BIGINT")
        log_timing("apply_704c_deduction", t0)
        return df_alloc_input, df_fn_alloc

    run_id = cfg["run_id"]
    dar_tid = cfg.get("default_allocation_rule_transaction_id")
    gdar_tid = cfg.get("global_default_allocation_rule_transaction_id")

    # Step 1: Aggregate 704c output → AllocationOutputTotal
    # SQL #tmpAllocationOutput.AdjustmentTypeID is never populated by INSERTs, so
    # SQL aggregates/joins on NULL. SchID IS populated by 704c inserts so we include
    # it as a real group key (null-safe).
    _agg_with_schid = "SchID" in df_704c_output.columns
    _group_cols = [
        "RunID", "ClientID", "EntityID", "LineTypeID",
        "QuicklinkID", "LineID", "ParentEntityID",
        "SuperParentEntityID", "AllocationTypeID", "TrackingKey",
    ]
    if _agg_with_schid:
        _group_cols.append("SchID")
    df_aot = (
        df_704c_output
        .filter(
            (F.col("RunID") == run_id)
            # BUG-24 FIX: SQL uses exact match: AllocationType = '704c Footnote'
            & (F.col("AllocationType") == "704c Footnote")
        )
        .groupBy(*_group_cols)
        .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("AllocatedAmount"))
    )

    # Step 2: Deduction UPDATE on temp #AllocationInput
    # LEFT JOIN DefaultAllocationRuleSetup to check "Allocate > 100%" rule
    dar = (
        read_table(spark, "DefaultAllocationRuleSetup", cfg)
        .filter(F.col("TransactionID").isin([dar_tid, gdar_tid]))
        .select(F.col("RuleID").alias("dar_RuleID"), "AllocationPercentageTypeID")
    )

    # The LEFT JOIN to ENU_AllocationPercentageType is dead code in original SQL —
    # The condition only checks D.RuleID IS NOT NULL.
    # We just need to know if the TypeID matches a DAR rule.

    L = df_alloc_input.alias("L")
    AO = df_aot.alias("AO")

    deduction_join = (
        (F.col("L.RunID") == F.col("AO.RunID"))
        & (F.col("L.EntityID") == F.col("AO.EntityID"))
        & (F.col("L.ClientID") == F.col("AO.ClientID"))
        & (ns0(F.col("L.ParentEntityID")) == ns0(F.col("AO.ParentEntityID")))
        & (ns0(F.col("L.SuperParentEntityID")) == ns0(F.col("AO.SuperParentEntityID")))
        & (ns(F.col("L.TrackingKey")) == ns(F.col("AO.TrackingKey")))
        & (F.col("L.LineID") == F.col("AO.LineID"))
        & (F.col("L.LineTypeID") == F.col("AO.LineTypeID"))
        & (F.col("L.QuicklinkID") == F.col("AO.QuicklinkID"))
    )
    if _agg_with_schid:
        deduction_join = deduction_join & (
            ns0(F.col("L.SchID")) == ns0(F.col("AO.SchID"))
        )

    joined = L.join(AO, deduction_join, "left")

    # LEFT JOIN DAR to check if rule exists
    joined = joined.join(
        dar,
        F.col("L.TypeID") == F.col("dar_RuleID"),
        "left",
    )

    # Apply deduction logic:
    # IF (D.RuleID IS NOT NULL AND ABS(AO.Amount) > ABS(L.Amount)) THEN 0
    # ELSE L.Amount - AO.Amount
    has_rule = F.col("dar_RuleID").isNotNull()
    alloc_exceeds = F.abs(F.col("AllocatedAmount")) > F.abs(F.col("L.Amount"))
    cap_condition = has_rule & alloc_exceeds

    new_amount = F.when(
        F.col("AllocatedAmount").isNull(), F.col("L.Amount")
    ).when(
        cap_condition, F.lit(0)
    ).otherwise(
        F.col("L.Amount") - F.col("AllocatedAmount")
    )

    new_amount704b = F.when(
        F.col("AllocatedAmount").isNull(), F.col("L.Amount704b")
    ).when(
        cap_condition, sql_round(F.col("AllocatedAmount"), 0)
    ).otherwise(
        F.col("L.Amount704b")
    )

    # Select original columns + updated Amount/Amount704b
    alloc_input_cols = [c for c in df_alloc_input.columns if c not in ("Amount", "Amount704b")]
    df_deducted = joined.select(
        *[F.col(f"L.{c}").alias(c) for c in alloc_input_cols],
        new_amount.alias("Amount"),
        new_amount704b.alias("Amount704b"),
    )

    # Step 3: Track allocated lines
    df_fn_alloc = (
        df_aot
        .select("LineID", "LineTypeID", "RunID")
        .distinct()
    )

    # Step 4: Zero-out residuals (Amount BETWEEN -0.99 AND 0.99)
    # Excluding ZeroExcludeLines
    fn_match = df_fn_alloc.select(
        F.col("LineID").alias("fn_lid"),
        F.col("LineTypeID").alias("fn_ltid"),
        F.col("RunID").alias("fn_rid"),
    )
    ze_match = df_zero_exclude.select(
        F.col("LineTypeID").alias("ze_ltid"),
        F.col("LineID").alias("ze_lid"),
    )

    df_deducted = df_deducted.alias("D")
    zero_candidates = (
        df_deducted
        .join(fn_match,
              (F.col("D.LineID") == F.col("fn_lid"))
              & (F.col("D.LineTypeID") == F.col("fn_ltid"))
              & (F.col("D.RunID") == F.col("fn_rid")),
              "inner")
        .join(ze_match,
              (F.col("D.LineTypeID") == F.col("ze_ltid"))
              & (F.col("D.LineID") == F.col("ze_lid")),
              "left")
        .filter(
            (F.col("D.RunID") == run_id)
            & (F.coalesce(F.col("D.Amount"), F.lit(0)).between(-0.99, 0.99))
            & (F.col("ze_lid").isNull())
        )
        .select(
            *[F.col(f"D.{c}") for c in alloc_input_cols],
            F.lit(0).cast("double").alias("Amount"),
            F.col("D.Amount704b").alias("Amount704b"),
        )
    )

    # Non-zero rows keep their updated amount
    non_zero = (
        df_deducted
        .join(fn_match,
              (F.col("D.LineID") == F.col("fn_lid"))
              & (F.col("D.LineTypeID") == F.col("fn_ltid"))
              & (F.col("D.RunID") == F.col("fn_rid")),
              "inner")
        .join(ze_match,
              (F.col("D.LineTypeID") == F.col("ze_ltid"))
              & (F.col("D.LineID") == F.col("ze_lid")),
              "left")
        .filter(
            ~(
                (F.col("D.RunID") == run_id)
                & (F.coalesce(F.col("D.Amount"), F.lit(0)).between(-0.99, 0.99))
                & (F.col("ze_lid").isNull())
            )
        )
        .select(
            *[F.col(f"D.{c}") for c in alloc_input_cols],
            F.col("D.Amount").alias("Amount"),
            F.col("D.Amount704b").alias("Amount704b"),
        )
    )

    # Rows not matched to fn_allocated_lines keep their original values
    unmatched = (
        df_deducted
        .join(fn_match,
              (F.col("D.LineID") == F.col("fn_lid"))
              & (F.col("D.LineTypeID") == F.col("fn_ltid"))
              & (F.col("D.RunID") == F.col("fn_rid")),
              "left_anti")
        .select(
            *[F.col(f"D.{c}") for c in alloc_input_cols],
            F.col("D.Amount").alias("Amount"),
            F.col("D.Amount704b").alias("Amount704b"),
        )
    )

    df_alloc_input_final = zero_candidates.unionByName(non_zero).unionByName(unmatched)

    log_timing("apply_704c_deduction", t0)
    return df_alloc_input_final, df_fn_alloc
