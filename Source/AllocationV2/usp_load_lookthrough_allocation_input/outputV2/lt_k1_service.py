"""
k1_service.py — Sections 5-8: K1 input, adjustments, LT flowup, rounding diff.

Functions:
    build_k1_input          — Section 5: K1 input (yearly vs periodic)
    build_adjustments_input — Section 6: Book K-1 Adjustments (UNPIVOT)
    build_lt_flowup_k1      — Section 7: Lower Tier K1 flowup
    build_rounding_diff     — Section 8: Rounding difference (IsInvestmentLevelRounding='U')
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import time

try:
    from .lt_helpers import tbl, tbl_name, ns, ns0, sql_round, log_section, log_timing, logger
except ImportError:
    from lt_helpers import tbl, tbl_name, ns, ns0, sql_round, log_section, log_timing, logger


# ---------------------------------------------------------------------------
# Section 5: build_k1_input
# SQL lines ~1106-1250
# IF NOT EXISTS (GenericPeriodicInfo_Snapshot WHERE WorkflowID = @YearlyWorkflowID)
#   → K1Input_Snapshot (yearly)
# ELSE
#   → K1InputByPeriod_Snapshot (periodic) + M1Adjustments_Snapshot
# ---------------------------------------------------------------------------

def build_k1_input(spark: SparkSession, cfg: dict, k1_workflow_df: DataFrame,
                   fx_rates_df: DataFrame) -> DataFrame:
    """
    Build K1 allocation input from K1Input_Snapshot or K1InputByPeriod_Snapshot.
    Returns DataFrame matching #LookThroughAllocationInput schema.
    """
    log_section("build_k1_input")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    k1_lt = cfg["k1_line_type_id"]
    m1_lt = cfg["m1_line_type_id"]
    yearly_wf_id = cfg["yearly_workflow_id"]

    # Check if periodic exists
    periodic_exists = (
        tbl(spark, "GenericPeriodicInfo_Snapshot", cfg)
        .filter(F.col("WorkflowID") == yearly_wf_id)
        .select(F.lit(1)).first() is not None
    ) if yearly_wf_id else False

    # K1LineItem filter (LineDataType='Number') — broadcast (small lookup)
    k1_line_item = F.broadcast(
        tbl(spark, "K1LineItem", cfg).filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.lower(F.col("LineDataType")) == "number")
        ).select("LineID", "ClientID", "TaxPeriodID")
    )

    # Tracking key expression
    tracking_key_expr = (
        F.col("KW_EntityID").cast("string")
        .alias("TrackingKey")
    )

    # Pre-broadcast small lookup DFs used in both yearly/periodic paths
    k1_wf_bc = F.broadcast(k1_workflow_df.withColumnRenamed("EntityID", "KW_EntityID"))
    fx_bc = F.broadcast(fx_rates_df.select(
        F.col("EntityID").alias("fx_EntityID"),
        F.col("LineID").alias("fx_LineID"),
        "ConversionRate",
    ))

    if not periodic_exists:
        # --- Yearly: K1Input_Snapshot ---
        k1_input = (
            tbl(spark, "K1Input_Snapshot", cfg)
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            )
            .join(k1_line_item, ["LineID", "ClientID", "TaxPeriodID"])
            .join(k1_wf_bc, "WorkflowID")
            .join(
                fx_bc,
                (F.col("KW_EntityID") == F.col("fx_EntityID")) &
                (F.col("LineID") == F.col("fx_LineID")),
                "left",
            )
            .select(
                F.lit(None).cast("int").alias("SuperParentEntityID"),
                F.lit(entity_id).cast("int").alias("ParentEntityID"),
                F.col("KW_EntityID").alias("EntityID"),
                F.lit(k1_lt).cast("int").alias("LineTypeID"),
                F.col("LineID"),
                sql_round(F.col("TotalAmount") / F.coalesce(F.col("ConversionRate"), F.lit(1)), 0).alias("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.lit(None).cast("int").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                tracking_key_expr,
                F.lit(None).cast("string").alias("TAG"),
                F.lit(None).cast("int").alias("OriginalParentEntityID"),
                F.lit(None).cast("int").alias("LTEntityID"),
            )
        )
        result = k1_input

    else:
        # --- Periodic: K1InputByPeriod_Snapshot ---
        k1_periodic = (
            tbl(spark, "K1InputByPeriod_Snapshot", cfg)
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            )
            .join(k1_line_item, ["LineID", "ClientID", "TaxPeriodID"])
            .join(k1_wf_bc, "WorkflowID")
            .join(
                fx_bc,
                (F.col("KW_EntityID") == F.col("fx_EntityID")) &
                (F.col("LineID") == F.col("fx_LineID")),
                "left",
            )
            .select(
                F.lit(None).cast("int").alias("SuperParentEntityID"),
                F.lit(entity_id).cast("int").alias("ParentEntityID"),
                F.col("KW_EntityID").alias("EntityID"),
                F.lit(k1_lt).cast("int").alias("LineTypeID"),
                F.col("LineID"),
                sql_round(F.col("TotalAmount") / F.coalesce(F.col("ConversionRate"), F.lit(1)), 0).alias("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.lit(None).cast("int").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.col("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                tracking_key_expr,
                F.lit(None).cast("string").alias("TAG"),
                F.lit(None).cast("int").alias("OriginalParentEntityID"),
                F.lit(None).cast("int").alias("LTEntityID"),
            )
        )

        # --- M1Adjustments_Snapshot (only in periodic mode) ---
        m1_input = (
            tbl(spark, "M1Adjustments_Snapshot", cfg)
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            )
            .join(
                k1_line_item,
                (F.col("K1LineID") == k1_line_item["LineID"]) &
                (F.col("ClientID") == k1_line_item["ClientID"]) &
                (F.col("TaxPeriodID") == k1_line_item["TaxPeriodID"]),
            )
            .join(k1_wf_bc, "WorkflowID")
            .join(
                fx_bc,
                (F.col("KW_EntityID") == F.col("fx_EntityID")) &
                (F.col("K1LineID") == F.col("fx_LineID")),
                "left",
            )
            .select(
                F.lit(None).cast("int").alias("SuperParentEntityID"),
                F.lit(entity_id).cast("int").alias("ParentEntityID"),
                F.col("KW_EntityID").alias("EntityID"),
                F.lit(m1_lt).cast("int").alias("LineTypeID"),
                F.col("K1LineID").alias("LineID"),
                sql_round(F.col("Amount") / F.coalesce(F.col("ConversionRate"), F.lit(1)), 0).alias("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.lit(None).cast("int").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.col("PeriodID"),
                F.col("WorkpaperCode").alias("LineCode"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                tracking_key_expr,
                F.lit(None).cast("string").alias("TAG"),
                F.lit(None).cast("int").alias("OriginalParentEntityID"),
                F.lit(None).cast("int").alias("LTEntityID"),
            )
        )

        result = k1_periodic.unionByName(m1_input, allowMissingColumns=True)

    log_timing("build_k1_input", t0)
    return result


# ---------------------------------------------------------------------------
# Section 6: build_adjustments_input
# SQL lines ~1250-1450
# UNPIVOT TrialBalanceAdjustments_SnapShot → per-AdjustmentType rows
# ---------------------------------------------------------------------------

def build_adjustments_input(spark: SparkSession, cfg: dict,
                            adjustment_workflow_df: DataFrame,
                            fx_rates_df: DataFrame) -> DataFrame:
    """
    Build Book K-1 Adjustments input via UNPIVOT of TrialBalanceAdjustments_SnapShot.
    Returns DataFrame matching #LookThroughAllocationInput schema, or None if disabled.
    """
    log_section("build_adjustments_input")
    t0 = time.time()

    if cfg["disable_adjustments_allocations"] == "C":
        log_timing("build_adjustments_input", t0)
        return None

    if adjustment_workflow_df is None:
        log_timing("build_adjustments_input", t0)
        return None

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    book_k1_lt = cfg["book_k1_adjustment_line_type_id"]
    adj_source_type_id = cfg["adjustment_source_type_id"]

    # Load AdjustmentType IDs
    adj_types = tbl(spark, "ENU_AdjustmentType", cfg).select(
        "AdjustmentTypeID", "AdjustmentTypeName"
    ).collect()
    adj_map = {r["AdjustmentTypeName"]: r["AdjustmentTypeID"] for r in adj_types}

    book_adj_id = adj_map.get("Adjustments - Book Adjustments", 2)
    per_book_id = adj_map.get("Adjustments - Per Book", 1)
    book_reclass_id = adj_map.get("Adjustments - Book ReClass", 3)
    m1_id = adj_map.get("Adjustments - M-1", 4)
    eliminations_id = adj_map.get("Adjustments - Eliminations", 5)

    # K1LineItem filter — broadcast (small lookup)
    k1_line_item = F.broadcast(
        tbl(spark, "K1LineItem", cfg).filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.lower(F.col("LineDataType")) == "number")
        ).select("LineID", "ClientID", "TaxPeriodID")
    )

    # Build pivoted data from TrialBalanceAdjustments_SnapShot
    tba = (
        tbl(spark, "TrialBalanceAdjustments_SnapShot", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.col("SourceTypeID") == adj_source_type_id)
        )
        .join(k1_line_item, ["LineID", "ClientID", "TaxPeriodID"])
        .join(
            F.broadcast(adjustment_workflow_df.withColumnRenamed("EntityID", "KW_EntityID")),
            "WorkflowID",
        )
        .join(
            F.broadcast(fx_rates_df.select(
                F.col("EntityID").alias("fx_EntityID"),
                F.col("LineID").alias("fx_LineID"),
                "ConversionRate",
            )),
            (F.col("KW_EntityID") == F.col("fx_EntityID")) &
            (F.col("LineID") == F.col("fx_LineID")),
            "left",
        )
        .select(
            F.lit(entity_id).cast("int").alias("ParentEntityID"),
            F.col("KW_EntityID").alias("EntityID"),
            F.lit(book_k1_lt).cast("int").alias("LineTypeID"),
            F.col("LineID"),
            sql_round(F.col("BookAdjustments") / F.coalesce(F.col("ConversionRate"), F.lit(1)), 0).alias("BookAdjustments"),
            sql_round(F.col("M1") / F.coalesce(F.col("ConversionRate"), F.lit(1)), 0).alias("M1"),
            sql_round(F.col("PerBook") / F.coalesce(F.col("ConversionRate"), F.lit(1)), 0).alias("PerBook"),
            sql_round(F.col("BookReClass") / F.coalesce(F.col("ConversionRate"), F.lit(1)), 0).alias("BookReClass"),
            sql_round(F.col("Eliminations") / F.coalesce(F.col("ConversionRate"), F.lit(1)), 0).alias("Eliminations"),
            F.col("KW_EntityID").cast("string").alias("TrackingKey"),
        )
    )

    # UNPIVOT: stack the 5 adjustment type columns into rows
    unpivoted = tba.select(
        "ParentEntityID", "EntityID", "LineTypeID", "LineID", "TrackingKey",
        F.explode(F.array(
            F.struct(F.lit("PerBook").alias("TypeName"), F.col("PerBook").alias("Amount")),
            F.struct(F.lit("BookAdjustments").alias("TypeName"), F.col("BookAdjustments").alias("Amount")),
            F.struct(F.lit("M1").alias("TypeName"), F.col("M1").alias("Amount")),
            F.struct(F.lit("Eliminations").alias("TypeName"), F.col("Eliminations").alias("Amount")),
            F.struct(F.lit("BookReClass").alias("TypeName"), F.col("BookReClass").alias("Amount")),
        )).alias("unpvt"),
    ).select(
        "ParentEntityID", "EntityID", "LineTypeID", "LineID", "TrackingKey",
        F.col("unpvt.Amount").alias("Amount"),
        F.col("unpvt.TypeName").alias("TypeName"),
    )

    # Map TypeName → AdjustmentTypeID
    result = unpivoted.select(
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        "ParentEntityID",
        "EntityID",
        "LineTypeID",
        "LineID",
        "Amount",
        F.lit(None).cast("string").alias("TransactionName"),
        F.lit(None).cast("int").alias("TransactionEntityID"),
        F.lit(None).cast("int").alias("QuicklinkID"),
        F.lit(None).cast("int").alias("CategoryID"),
        F.lit(None).cast("int").alias("PeriodID"),
        F.lit(None).cast("string").alias("LineCode"),
        F.when(F.lower(F.col("TypeName")) == "bookadjustments", F.lit(book_adj_id))
         .when(F.lower(F.col("TypeName")) == "perbook", F.lit(per_book_id))
         .when(F.lower(F.col("TypeName")) == "m1", F.lit(m1_id))
         .when(F.lower(F.col("TypeName")) == "eliminations", F.lit(eliminations_id))
         .when(F.lower(F.col("TypeName")) == "bookreclass", F.lit(book_reclass_id))
         .otherwise(F.lit(None).cast("int")).alias("AdjustmentTypeID"),
        "TrackingKey",
        F.lit(None).cast("string").alias("TAG"),
        F.lit(None).cast("int").alias("OriginalParentEntityID"),
        F.lit(None).cast("int").alias("LTEntityID"),
    )

    log_timing("build_adjustments_input", t0)
    return result


# ---------------------------------------------------------------------------
# Section 7: build_lt_flowup_k1
# SQL lines ~1450-1610
# Lower Tier K1 flowup from ReclassK1LookThroughAllocationData
# ---------------------------------------------------------------------------

def build_lt_flowup_k1(spark: SparkSession, cfg: dict,
                       reclass_k1_df: DataFrame) -> DataFrame:
    """
    Build #LowerTierAmount from ReclassK1LookThroughAllocationData, then
    compute #LookThroughAllocationInputLowerTierAmounts.
    Returns DataFrame matching #LookThroughAllocationInput schema.
    Also returns the raw lower_tier_amount_df for use by rounding_diff.
    """
    log_section("build_lt_flowup_k1")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    k1_lt = cfg["k1_line_type_id"]

    # Build #LowerTierAmount from ReclassK1LookThroughAllocationData
    lower_tier_amount = reclass_k1_df.filter(
        (F.col("ClientID") == client_id) &
        (F.col("TaxPeriodID") == tax_period_id)
    ).groupBy(
        "ParentEntityID", "EntityID", "LineID", "PeriodID", "LTEntityID",
        "TrackingKey",
        F.coalesce(F.col("Tag"), F.lit("")).alias("TAG_grp"),
        F.coalesce(F.col("OriginalParentEntityID"), F.lit(entity_id)).alias("OrigParent_grp"),
    ).agg(
        F.sum(F.coalesce(F.col("FlowupAmount"), F.lit(0))).alias("FlowupAmount"),
    ).select(
        # ParentEntityID: CASE WHEN EntityID = LTEntityID THEN @LocalEntityID ELSE ParentEntityID END
        F.when(F.col("EntityID") == F.col("LTEntityID"), F.lit(entity_id))
         .otherwise(F.col("ParentEntityID")).alias("ParentEntityID"),
        F.col("EntityID"),
        F.col("LineID"),
        F.col("FlowupAmount"),
        F.col("PeriodID"),
        F.col("LTEntityID").alias("SuperParentEntityID"),
        F.col("TrackingKey"),
        F.col("TAG_grp").alias("TAG"),
        F.col("OrigParent_grp").alias("OriginalParentEntityID"),
        F.col("LTEntityID"),
    )

    log_timing("build_lt_flowup_k1", t0)
    return lower_tier_amount


def recompute_lt_input_from_lower_tier(cfg: dict, lower_tier_amount_df: DataFrame) -> DataFrame:
    """
    Re-compute #LookThroughAllocationInputLowerTierAmounts from an updated
    #LowerTierAmount (e.g., after rounding diff rows are appended).
    Returns DataFrame matching #LookThroughAllocationInput schema.
    """
    entity_id = cfg["entity_id"]
    k1_lt = cfg["k1_line_type_id"]

    lt_input = lower_tier_amount_df.groupBy(
        F.coalesce(F.col("SuperParentEntityID"), F.lit(0)).alias("_spe"),
        "ParentEntityID", "EntityID", "LineID", "PeriodID",
        "TrackingKey",
        F.coalesce(F.col("TAG"), F.lit("")).alias("TAG"),
        "OriginalParentEntityID", "LTEntityID",
    ).agg(
        F.sum(F.coalesce(F.col("FlowupAmount"), F.lit(0))).alias("Amount"),
    ).select(
        F.when(
            (F.col("_spe") == F.coalesce(F.col("ParentEntityID"), F.lit(0))) |
            (F.col("ParentEntityID") == entity_id),
            F.lit(0)
        ).otherwise(F.col("_spe")).cast("int").alias("SuperParentEntityID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.lit(k1_lt).cast("int").alias("LineTypeID"),
        F.col("LineID"),
        F.col("Amount"),
        F.lit(None).cast("string").alias("TransactionName"),
        F.lit(None).cast("int").alias("TransactionEntityID"),
        F.lit(None).cast("int").alias("QuicklinkID"),
        F.lit(None).cast("int").alias("CategoryID"),
        F.col("PeriodID"),
        F.lit(None).cast("string").alias("LineCode"),
        F.lit(None).cast("int").alias("AdjustmentTypeID"),
        F.col("TrackingKey"),
        F.col("TAG"),
        F.col("OriginalParentEntityID"),
        F.col("LTEntityID"),
    )
    return lt_input


# ---------------------------------------------------------------------------
# Section 8: build_rounding_diff
# SQL lines ~1470-1580
# IF IsInvestmentLevelRounding = 'U':
#   Compute diff between ReclassK1AllocationData (rounded) and unrounded pickup
#   and add correction rows to lower_tier_amount
# ---------------------------------------------------------------------------

def build_rounding_diff(spark: SparkSession, cfg: dict,
                        lower_tier_amount_df: DataFrame) -> DataFrame:
    """
    If IsInvestmentLevelRounding='U', compute rounding diff and append correction rows.
    Returns updated lower_tier_amount_df (with diff rows appended).
    """
    log_section("build_rounding_diff")
    t0 = time.time()

    if cfg["is_investment_level_rounding"] != "U":
        log_timing("build_rounding_diff", t0)
        return lower_tier_amount_df

    entity_id = cfg["entity_id"]
    run_id = cfg["run_id"]

    # #RoundedData
    rounded_data = tbl(spark, "ReclassK1AllocationData", cfg).filter(
        F.col("RunID") == run_id
    ).select("LineID", "Amount", "LTEntityID")

    # #UnRoundedPickup
    unrounded_pickup = lower_tier_amount_df.groupBy(
        "LineID", F.col("SuperParentEntityID").alias("LTEntityID")
    ).agg(
        F.sum(F.col("FlowupAmount")).alias("Amount")
    )

    # Compute diff: (Rounded - UnRounded) for matching rows
    diff_matched = (
        unrounded_pickup.alias("U")
        .join(rounded_data.alias("R"),
              (F.col("U.LTEntityID") == F.col("R.LTEntityID")) &
              (F.col("U.LineID") == F.col("R.LineID")),
              "left")
        .select(
            F.col("U.LTEntityID"),
            F.col("U.LineID"),
            (F.coalesce(F.col("R.Amount"), F.lit(0)) - F.coalesce(F.col("U.Amount"), F.lit(0))).alias("DiffAmount"),
        )
    )

    # UNION: Rounded rows with no match in UnRounded
    diff_unmatched = (
        rounded_data.alias("R")
        .join(unrounded_pickup.alias("U"),
              (F.col("R.LTEntityID") == F.col("U.LTEntityID")) &
              (F.col("R.LineID") == F.col("U.LineID")),
              "left_anti")
        .select(
            F.col("R.LTEntityID"),
            F.col("R.LineID"),
            F.coalesce(F.col("R.Amount"), F.lit(0)).alias("DiffAmount"),
        )
    )

    diff = diff_matched.unionByName(diff_unmatched, allowMissingColumns=True)

    # Build correction rows for #LowerTierAmount
    correction_rows = diff.select(
        F.lit(entity_id).cast("int").alias("ParentEntityID"),
        F.col("LTEntityID").alias("EntityID"),
        F.col("LineID"),
        F.col("DiffAmount").alias("FlowupAmount"),
        F.lit(None).cast("int").alias("PeriodID"),
        F.col("LTEntityID").alias("SuperParentEntityID"),
        # TrackingKey = CONVERT(VARCHAR, LTEntityID) + '~' + CONVERT(VARCHAR, LTEntityID)
        F.concat(F.col("LTEntityID").cast("string"), F.lit("~"), F.col("LTEntityID").cast("string")).alias("TrackingKey"),
        F.lit("").alias("TAG"),
        F.lit(entity_id).cast("int").alias("OriginalParentEntityID"),
        F.col("LTEntityID"),
    )

    # Append to lower_tier_amount_df
    result = lower_tier_amount_df.unionByName(correction_rows, allowMissingColumns=True)

    log_timing("build_rounding_diff", t0)
    return result
