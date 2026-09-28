"""
Effective percentage allocation logic for uspLoadFootnotesAllocationToOutput.

Applies effective percentages from #TempFinalEffectivePercentage to
#AllocationInput and generates the main allocation output rows for
each line type (PFIC, Form926, Form8865, Form1042S, Form8886, Form199A, At Risk, Custom).
"""
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0
from Common_V2.core.observability import log_section, log_timing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# resolve_min_quarter
# SQL lines: 2171–2190
# Row count: N/A (config function)
# ---------------------------------------------------------------------------
def resolve_min_quarter(spark: SparkSession, cfg: dict) -> str:
    """Determine @MinQuarter based on allocation type and transfer config.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 2171-2190.
    If PE Book + IsDatedTransfersConfigured='C': use QuarterDates.Preference=0
    Else: use ENU_DF_DataList.Category='Quarters', DisplayOrder=1.
    Stores result in cfg["min_quarter"] and returns the value.
    """
    log_section("resolve_min_quarter")
    t0 = time.time()

    alloc_type_name = (cfg.get("allocation_type_name") or "").strip().lower()
    is_dated = (cfg.get("is_dated_transfers_configured") or "").strip().upper()

    if alloc_type_name == "pe book allocation" and is_dated == "C":
        rows = (
            read_table(spark, "QuarterDates", cfg)
            .filter(F.col("Preference") == 0)
            .select(F.coalesce(F.col("Quarter"), F.lit("Q0")).alias("min_q"))
            .collect()
        )
        min_quarter = rows[0]["min_q"] if rows else "Q0"
    else:
        rows = (
            read_table(spark, "ENU_DF_DataList", cfg)
            .filter(
                (F.lower(F.col("Category")) == "quarters")
                & (F.col("DisplayOrder") == 1)
            )
            .select(F.coalesce(F.col("LookUpData"), F.lit("Q0")).alias("min_q"))
            .collect()
        )
        min_quarter = rows[0]["min_q"] if rows else "Q0"

    cfg["min_quarter"] = min_quarter
    log_timing("resolve_min_quarter", t0)
    return min_quarter


# ---------------------------------------------------------------------------
# _build_allocation_type_expr (internal helper)
# ---------------------------------------------------------------------------
def _build_allocation_type_expr(include_ti: bool = False):
    """Build the AllocationType CASE expression used by all line type inserts.

    Maps AP.AllocationType → 'Footnote-...' display name.
    """
    expr = (
        F.when(F.lower(F.col("AP.AllocationType")) == "cost",
               F.concat(F.lit("Footnote-"), F.col("EC.AllocationType")))
        .when(F.lower(F.col("AP.AllocationType")) == "costadjusteddatedtransfer",
              F.concat(F.lit("Footnote-"), F.col("EC.AllocationType"), F.lit("AdjustedDatedTransfer")))
        .when(F.lower(F.col("AP.AllocationType")) == "default",
              F.lit("Footnote-Cost"))
        .when(F.lower(F.col("AP.AllocationType")) == "defaultadjusteddatedtransfer",
              F.lit("Footnote-CostAdjustedDatedTransfer"))
    )
    if include_ti:
        expr = expr.when(
            F.lower(F.col("AP.AllocationType")) == "ti",
            F.lit("Footnote-TI")
        )
    expr = (
        expr
        .when(F.lower(F.col("AP.AllocationType")) == "cost without transfer adj %",
              F.concat(F.lit("Footnote-"), F.col("EC.AllocationType"), F.lit(" without Transfer Adj %")))
        .when(F.lower(F.col("AP.AllocationType")) == "default without transfer adj %",
              F.lit("Footnote-Cost without Transfer Adj %"))
        .otherwise(F.lit("Footnote-ProRata"))
    )
    return expr


# ---------------------------------------------------------------------------
# _build_line_type_insert (internal helper)
# ---------------------------------------------------------------------------
def _build_line_type_insert(
    df_ai, df_ap, df_ep, df_ec, df_el,
    cfg: dict,
    line_type: str,
    include_quarter_join: bool = True,
    include_schid: bool = False,
    include_ti_logic: bool = False,
    extra_join_df: DataFrame = None,
    extra_join_cond=None,
    schid_filter=None,
    pfic_join_df: DataFrame = None,
):
    """Generate one line-type INSERT block for effective % allocation.

    All line types share the same pattern with slight variations:
    - JOIN conditions (quarter match, extra line item table)
    - Output columns (SchID for Form8865)
    - AllocationType expression (TI for Form199A)
    """
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]

    # Common join: AI → AP (TempFinalEffectivePercentage)
    join_cond = (
        (F.col("AI.EntityID") == F.col("AP.InvestmentID"))
        & (F.col("AI.TypeID") == F.col("AP.TypeID"))
        & (ns0(F.col("AI.LineTypeID")) == ns0(F.col("AP.LineTypeID")))
        & (F.col("AI.IsExcludefromTransfer").cast("int") == F.col("AP.IsExcludefromTransfer").cast("int"))
        & (ns(F.col("AI.TrackingKey")) == ns(F.col("AP.TrackingKey")))
    )

    if include_quarter_join:
        join_cond = join_cond & (F.col("AP.Quarter") == F.col("AI.Quarter"))

    # Form199A TI logic: additional LINEID condition
    if include_ti_logic:
        join_cond = join_cond & (
            ns0(F.col("AP.LINEID")) == F.when(
                (F.lower(F.col("AP.AllocationType")) == "ti") & (F.col("AP.LINEID") != -1),
                F.col("AI.LineID"),
            ).otherwise(ns0(F.col("AP.LINEID")))
        )

    # Start with AI
    ai = df_ai.alias("AI")
    ap = df_ap.alias("AP")
    ep = df_ep.alias("EP")
    ec = df_ec.alias("EC")
    el = df_el.alias("EL")

    result = (
        ai
        .join(ap, join_cond, "inner")
        .join(ep, F.col("EP.partnernumber") == F.col("AP.PartnerNumber"), "inner")
        .join(ec, F.col("EC.AllocationTypeID") == F.col("AI.TypeID"), "inner")
        .join(el, ns0(F.col("AI.LineTypeID")) == ns0(F.col("EL.LineTypeID")), "inner")
    )

    # Line type filter (skip for custom footnote where line_type=None)
    if line_type is not None:
        result = result.filter(F.lower(F.col("EL.LineType")) == line_type.lower())

    # PFIC: additional JOIN to PFICFootnoteLineItem (existence check)
    if pfic_join_df is not None:
        result = result.join(
            pfic_join_df.alias("PL"),
            F.col("AI.LineID") == F.col("PL.pfic_lid"),
            "inner",
        )

    # Custom footnote: JOIN to tmpCustomFootnoteLineTypes
    if extra_join_df is not None and extra_join_cond is not None:
        result = result.join(extra_join_df, extra_join_cond, "inner")

    # SchID filter (Form8865)
    if schid_filter is not None:
        result = result.filter(schid_filter)

    # Amount calculation: AI.Amount * AP.EffPercentage
    amount_expr = ns0(F.col("AI.Amount")) * F.coalesce(F.col("AP.EffPercentage"), F.lit(0.0))

    # AllocationType expression
    alloc_type_expr = _build_allocation_type_expr(include_ti=include_ti_logic)

    # Output columns
    select_cols = [
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.col("AI.EntityID"),
        F.col("EP.ShareClass"),
        F.col("AP.PartnerNumber"),
        F.col("AI.LineTypeID"),
        F.col("AI.QuicklinkID"),
        F.col("AI.LineID"),
        amount_expr.alias("Amount"),
        alloc_type_expr.alias("AllocationType"),
        F.col("AI.ParentEntityID"),
        F.col("AI.SuperParentEntityID"),
        F.col("EC.AllocationTypeID"),
        F.col("AI.TrackingKey"),
        F.col("AI.OriginalParentEntityID"),
    ]

    if include_schid:
        select_cols.append(F.col("AI.SchID"))

    return result.select(*select_cols)


# ---------------------------------------------------------------------------
# build_effective_pct_allocation
# SQL lines: 2191–2640
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_effective_pct_allocation(
    spark: SparkSession, cfg: dict,
    df_alloc_input: DataFrame,
    df_temp_final_eff_pct: DataFrame,
    df_entity_partners: DataFrame,
    df_custom_fn_types: DataFrame,
) -> DataFrame:
    """Generate allocation output rows using effective percentages for all line types.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 2191-2640.
    Row count: POSSIBLY-EMPTY — only if #TempFinalEffectivePercentage > 0.

    Inserts for each line type with AllocationType name derivation:
    - PFIC Footnote
    - Form926
    - Form8865 with SchID=0
    - Form8865 with SchID<>0
    - Form1042S (no quarter join)
    - Form8886
    - Form199A (TI allocation logic)
    - At Risk
    - Custom footnote
    """
    log_section("build_effective_pct_allocation")
    t0 = time.time()

    # Guard: only runs when rank_for_rule_pickup != 2 (SQL: IF @RankForRulePickup <> 2)
    if df_temp_final_eff_pct is None or cfg.get("rank_for_rule_pickup") == 2:
        logger.info("[SKIP] TempFinalEffectivePercentage not loaded (rank=2) — skipping effective % allocation")
        log_timing("build_effective_pct_allocation", t0)
        return None

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]

    # Reference tables (broadcast-sized — tiny enum tables)
    df_ec = F.broadcast(
        read_table(spark, "ENU_CustomAllocations", cfg)
        .select("AllocationTypeID", "AllocationType")
    )
    df_el = F.broadcast(
        read_table(spark, "ENU_LineType", cfg)
        .filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select("LineTypeID", "LineType")
    )

    # PFIC: load PFICFootnoteLineItem for existence join (broadcast — tiny lookup)
    # BUG-25 FIX: SQL has no ClientID/TaxPeriodID filter on PFICFootnoteLineItem:
    #   EXISTS (SELECT 1 FROM PFICFootnoteLineItem WHERE LineID = AI.LineID)
    pfic_li = F.broadcast(
        cfg["_df_pfic_footnote_line_item"]
        .select(F.col("LineID").alias("pfic_lid"))
        .distinct()
    )

    results = []

    # 1. PFIC Footnote
    df_pfic = _build_line_type_insert(
        df_alloc_input, df_temp_final_eff_pct, df_entity_partners, df_ec, df_el,
        cfg, line_type="PFIC Footnote",
        include_quarter_join=True,
        pfic_join_df=pfic_li,
    )
    results.append(df_pfic)

    # 2. Form926
    df_f926 = _build_line_type_insert(
        df_alloc_input, df_temp_final_eff_pct, df_entity_partners, df_ec, df_el,
        cfg, line_type="Form926",
        include_quarter_join=True,
    )
    results.append(df_f926)

    # 3. Form8865 SchID=0
    df_f8865_0 = _build_line_type_insert(
        df_alloc_input, df_temp_final_eff_pct, df_entity_partners, df_ec, df_el,
        cfg, line_type="Form8865",
        include_quarter_join=True,
        include_schid=True,
        schid_filter=(ns0(F.col("AI.SchID")) == 0),
    )
    results.append(df_f8865_0)

    # 4. Form8865 SchID<>0
    df_f8865_n = _build_line_type_insert(
        df_alloc_input, df_temp_final_eff_pct, df_entity_partners, df_ec, df_el,
        cfg, line_type="Form8865",
        include_quarter_join=True,
        include_schid=True,
        schid_filter=(ns0(F.col("AI.SchID")) != 0),
    )
    results.append(df_f8865_n)

    # 5. Form1042S (no quarter join)
    df_f1042s = _build_line_type_insert(
        df_alloc_input, df_temp_final_eff_pct, df_entity_partners, df_ec, df_el,
        cfg, line_type="Form1042S",
        include_quarter_join=False,
    )
    results.append(df_f1042s)

    # 6. Form8886
    df_f8886 = _build_line_type_insert(
        df_alloc_input, df_temp_final_eff_pct, df_entity_partners, df_ec, df_el,
        cfg, line_type="Form8886",
        include_quarter_join=True,
    )
    results.append(df_f8886)

    # 7. Form199A (with TI allocation logic)
    df_f199a = _build_line_type_insert(
        df_alloc_input, df_temp_final_eff_pct, df_entity_partners, df_ec, df_el,
        cfg, line_type="Form199A",
        include_quarter_join=True,
        include_ti_logic=True,
    )
    results.append(df_f199a)

    # 8. At Risk
    df_at_risk = _build_line_type_insert(
        df_alloc_input, df_temp_final_eff_pct, df_entity_partners, df_ec, df_el,
        cfg, line_type="At Risk",
        include_quarter_join=True,
    )
    results.append(df_at_risk)

    # 9. Custom footnote (JOIN to tmpCustomFootnoteLineTypes)
    cf_alias = df_custom_fn_types.alias("CF")
    df_custom = _build_line_type_insert(
        df_alloc_input, df_temp_final_eff_pct, df_entity_partners, df_ec, df_el,
        cfg, line_type=None,  # not filtered by EL.LineType; uses custom join instead
        include_quarter_join=True,
        extra_join_df=cf_alias,
        extra_join_cond=(ns0(F.col("CF.LineTypeID")) == ns0(F.col("EL.LineTypeID"))),
    )
    results.append(df_custom)

    # Combine all results with unionByName (handle SchID difference)
    # SchID is only present in Form8865 inserts; add NULL for others
    output_cols = [
        "RunID", "ClientID", "EntityID", "ShareClass", "PartnerNumber",
        "LineTypeID", "QuicklinkID", "LineID", "Amount", "AllocationType",
        "ParentEntityID", "SuperParentEntityID", "AllocationTypeID",
        "TrackingKey", "OriginalParentEntityID", "SchID",
    ]

    normalized = []
    for df in results:
        if df is not None:
            if "SchID" not in df.columns:
                df = df.withColumn("SchID", F.lit(None).cast("int"))
            normalized.append(df.select(*output_cols))

    if not normalized:
        log_timing("build_effective_pct_allocation", t0)
        return None

    df_result = normalized[0]
    for dfn in normalized[1:]:
        df_result = df_result.unionByName(dfn)

    log_timing("build_effective_pct_allocation", t0)
    return df_result
