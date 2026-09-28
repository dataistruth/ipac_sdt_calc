"""
flowup_service.py — Sections 9-10: Adjustment/M1 Lower Tier flowup.

Functions:
    build_lt_flowup_adjustment — Section 9: Adjustment LT flowup (investment-level vs non)
    build_lt_flowup_m1         — Section 10: M1 periodic LT flowup
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import time

try:
    from .lt_helpers import tbl, tbl_name, ns, ns0, log_section, log_timing, logger
except ImportError:
    from lt_helpers import tbl, tbl_name, ns, ns0, log_section, log_timing, logger


# ---------------------------------------------------------------------------
# Section 9: build_lt_flowup_adjustment
# SQL lines ~1630-1740
# IF IsInvestmentLevelRounding='C':
#   AdjustmentLookThroughAllocationSummary → #LowerTierAmount
# ELSE:
#   AdjustmentLookThroughSidePocketAllocationDetail +
#   AdjustmentLookThroughSidePocketResidualAllocationDetail → #LowerTierAmount
# Then → #LookThroughAllocationInput
# ---------------------------------------------------------------------------

def build_lt_flowup_adjustment(spark: SparkSession, cfg: dict,
                               lower_tier_funds_df: DataFrame) -> DataFrame:
    """
    Build lower tier adjustment flowup.
    Returns DataFrame matching #LookThroughAllocationInput schema, or None if disabled.
    """
    log_section("build_lt_flowup_adjustment")
    t0 = time.time()

    # Skip if either adjustment flag is not 'U' (SQL: both must equal 'U' to proceed)
    if (cfg["disable_adjustments_allocations"] != "U" or
            cfg["disable_adjustment_flowup_allocations"] != "U"):
        log_timing("build_lt_flowup_adjustment", t0)
        return None

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    book_k1_lt = cfg["book_k1_adjustment_line_type_id"]
    is_inv_level = cfg["is_investment_level_rounding"]

    if is_inv_level == "C":
        # Investment-level rounding: use AdjustmentLookThroughAllocationSummary
        lt_adj = _build_adj_from_summary(spark, cfg, lower_tier_funds_df)
    else:
        # Non-investment-level: use SidePocket + SidePocketResidual detail tables
        lt_adj = _build_adj_from_sidepocket(spark, cfg, lower_tier_funds_df)

    if lt_adj is None:
        log_timing("build_lt_flowup_adjustment", t0)
        return None

    # Final grouping → #LookThroughAllocationInput shape
    result = lt_adj.groupBy(
        F.coalesce(F.col("SuperParentEntityID"), F.lit(0)).alias("_spe"),
        "ParentEntityID", "EntityID", "LineID", "PeriodID",
        "TrackingKey", "LineTypeID", "AdjustmentTypeID", "OriginalParentEntityID",
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
        F.col("LineTypeID"),
        F.col("LineID"),
        F.col("Amount"),
        F.lit(None).cast("string").alias("TransactionName"),
        F.lit(None).cast("int").alias("TransactionEntityID"),
        F.lit(None).cast("int").alias("QuicklinkID"),
        F.lit(None).cast("int").alias("CategoryID"),
        F.col("PeriodID"),
        F.lit(None).cast("string").alias("LineCode"),
        F.col("AdjustmentTypeID"),
        F.col("TrackingKey"),
        F.lit(None).cast("string").alias("TAG"),
        F.col("OriginalParentEntityID"),
        F.lit(None).cast("int").alias("LTEntityID"),
    )

    log_timing("build_lt_flowup_adjustment", t0)
    return result


def _build_adj_from_summary(spark, cfg, lower_tier_funds_df):
    """IsInvestmentLevelRounding='C': from AdjustmentLookThroughAllocationSummary."""
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    book_k1_lt = cfg["book_k1_adjustment_line_type_id"]

    adj_summary = tbl(spark, "AdjustmentLookThroughAllocationSummary", cfg).filter(
        (F.col("ClientID") == client_id) &
        (F.col("TaxPeriodID") == tax_period_id) &
        (F.col("LineTypeID") == book_k1_lt)
    )

    joined = adj_summary.join(
        lower_tier_funds_df,
        (adj_summary["RunID"] == lower_tier_funds_df["RunID"]) &
        (lower_tier_funds_df["PartnerNumber"] == adj_summary["PartnerNumber"]),
    )

    return joined.groupBy(
        adj_summary["ParentEntityID"], adj_summary["EntityID"],
        adj_summary["LineID"], adj_summary["AdjustmentTypeID"],
        lower_tier_funds_df["EntityID"].alias("LT_EntityID"),
        adj_summary["TrackingKey"],
    ).agg(
        F.sum(F.coalesce(adj_summary["Amount"], F.lit(0))).alias("FlowupAmount"),
    ).select(
        F.when(F.col("EntityID") == F.col("LT_EntityID"), F.lit(entity_id))
         .otherwise(F.col("ParentEntityID")).alias("ParentEntityID"),
        F.col("EntityID"),
        F.col("LineID"),
        F.col("FlowupAmount"),
        F.lit(None).cast("int").alias("PeriodID"),
        F.col("LT_EntityID").alias("SuperParentEntityID"),
        F.col("TrackingKey"),
        F.lit(book_k1_lt).cast("int").alias("LineTypeID"),
        F.col("AdjustmentTypeID"),
        F.lit(None).cast("int").alias("OriginalParentEntityID"),
    )


def _build_adj_from_sidepocket(spark, cfg, lower_tier_funds_df):
    """Non-investment-level: from SidePocket + SidePocketResidual detail."""
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    book_k1_lt = cfg["book_k1_adjustment_line_type_id"]

    def _query_table(table_name):
        detail = tbl(spark, table_name, cfg).filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.col("LineTypeID") == book_k1_lt)
        )
        joined = detail.join(
            lower_tier_funds_df,
            (detail["RunID"] == lower_tier_funds_df["RunID"]) &
            (lower_tier_funds_df["PartnerNumber"] == detail["PartnerNumber"]),
        )
        return joined.groupBy(
            detail["ParentEntityID"], detail["EntityID"],
            detail["LineID"], detail["AdjustmentTypeID"],
            lower_tier_funds_df["EntityID"].alias("LT_EntityID"),
            detail["TrackingKey"],
        ).agg(
            F.sum(F.coalesce(detail["Amount"], F.lit(0))).alias("FlowupAmount"),
        ).select(
            F.when(F.col("EntityID") == F.col("LT_EntityID"), F.lit(entity_id))
             .otherwise(F.col("ParentEntityID")).alias("ParentEntityID"),
            F.col("EntityID"),
            F.col("LineID"),
            F.col("FlowupAmount"),
            F.lit(None).cast("int").alias("PeriodID"),
            F.col("LT_EntityID").alias("SuperParentEntityID"),
            F.col("TrackingKey"),
            F.lit(book_k1_lt).cast("int").alias("LineTypeID"),
            F.col("AdjustmentTypeID"),
            F.lit(None).cast("int").alias("OriginalParentEntityID"),
        )

    sp_detail = _query_table("AdjustmentLookThroughSidePocketAllocationDetail")
    sp_residual = _query_table("AdjustmentLookThroughSidePocketResidualAllocationDetail")

    return sp_detail.unionByName(sp_residual, allowMissingColumns=True)


# ---------------------------------------------------------------------------
# Section 10: build_lt_flowup_m1
# SQL lines ~1740-1830
# Only if periodic exists (GenericPeriodicInfo_Snapshot has records for YearlyWorkflowID)
# M1AdjLookThroughSidePocketAllocationDetail +
# M1AdjLookThroughSidePocketResidualAllocationDetail → #LowerTierAmount → input
# ---------------------------------------------------------------------------

def build_lt_flowup_m1(spark: SparkSession, cfg: dict,
                       lower_tier_funds_df: DataFrame) -> DataFrame:
    """
    Build M1 periodic lower tier flowup.
    Returns DataFrame matching #LookThroughAllocationInput schema, or None if not periodic.
    """
    log_section("build_lt_flowup_m1")
    t0 = time.time()

    yearly_wf_id = cfg["yearly_workflow_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    m1_lt = cfg["m1_line_type_id"]
    m1_ql = cfg["m1_quick_link_id"]

    # Only runs if periodic exists
    periodic_exists = (
        tbl(spark, "GenericPeriodicInfo_Snapshot", cfg)
        .filter(F.col("WorkflowID") == yearly_wf_id)
        .select(F.lit(1)).first() is not None
    ) if yearly_wf_id else False

    if not periodic_exists:
        log_timing("build_lt_flowup_m1", t0)
        return None

    def _query_m1_table(table_name):
        detail = tbl(spark, table_name, cfg).filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id)
        )
        joined = detail.join(
            lower_tier_funds_df,
            (detail["RunID"] == lower_tier_funds_df["RunID"]) &
            (lower_tier_funds_df["PartnerNumber"] == detail["PartnerNumber"]),
        )
        return joined.groupBy(
            detail["ParentEntityID"], detail["EntityID"],
            detail["LineID"], detail["PeriodID"],
            F.col("WorkPaperCode").alias("LineCode"),
            lower_tier_funds_df["EntityID"].alias("LT_EntityID"),
            detail["TrackingKey"],
        ).agg(
            F.sum(F.coalesce(detail["FlowupAmount"], F.lit(0))).alias("FlowupAmount"),
        ).select(
            F.when(F.col("EntityID") == F.col("LT_EntityID"), F.lit(entity_id))
             .otherwise(F.col("ParentEntityID")).alias("ParentEntityID"),
            F.col("EntityID"),
            F.col("LineID"),
            F.col("FlowupAmount"),
            F.col("PeriodID"),
            F.col("LineCode"),
            F.col("LT_EntityID").alias("SuperParentEntityID"),
            F.col("TrackingKey"),
        )

    m1_sp = _query_m1_table("M1AdJLookThroughSidePocketAllocationDetail")
    m1_residual = _query_m1_table("M1AdjLookThroughSidePocketResidualAllocationDetail")
    lt_m1 = m1_sp.unionByName(m1_residual, allowMissingColumns=True)

    # Final grouping → #LookThroughAllocationInput shape
    result = lt_m1.groupBy(
        F.coalesce(F.col("SuperParentEntityID"), F.lit(0)).alias("_spe"),
        "ParentEntityID", "EntityID", "LineID", "PeriodID", "LineCode",
        "TrackingKey",
    ).agg(
        F.sum(F.col("FlowupAmount")).alias("Amount"),
    ).select(
        F.when(
            (F.col("_spe") == F.coalesce(F.col("ParentEntityID"), F.lit(0))) |
            (F.col("ParentEntityID") == entity_id),
            F.lit(0)
        ).otherwise(F.col("_spe")).cast("int").alias("SuperParentEntityID"),
        F.coalesce(F.col("ParentEntityID"), F.lit(0)).cast("int").alias("ParentEntityID"),
        F.col("EntityID"),
        F.lit(m1_lt).cast("int").alias("LineTypeID"),
        F.col("LineID"),
        F.col("Amount"),
        F.lit(None).cast("string").alias("TransactionName"),
        F.lit(None).cast("int").alias("TransactionEntityID"),
        F.lit(m1_ql).cast("int").alias("QuicklinkID"),
        F.lit(None).cast("int").alias("CategoryID"),
        F.col("PeriodID"),
        F.coalesce(F.col("LineCode"), F.lit("")).alias("LineCode"),
        F.lit(None).cast("int").alias("AdjustmentTypeID"),
        F.col("TrackingKey"),
        F.lit(None).cast("string").alias("TAG"),
        F.lit(None).cast("int").alias("OriginalParentEntityID"),
        F.lit(None).cast("int").alias("LTEntityID"),
    )

    log_timing("build_lt_flowup_m1", t0)
    return result
