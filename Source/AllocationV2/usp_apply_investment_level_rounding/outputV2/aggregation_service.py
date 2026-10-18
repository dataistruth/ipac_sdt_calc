"""
Aggregation and computation services (Sections 9-11).

- build_temp_allocation_output: Aggregate LookThroughAllocationOutput.
- compute_rounding_diff: Compute TempAggregatedAmount vs TempAggregatedRoundedAmount.
- compute_max_allocation_type: Determine dominant AllocationType per line.
"""

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import time

from Common_V2.core.helpers import ns, ns0, sql_round
from Common_V2.core.observability import log_section, log_timing


# ---------------------------------------------------------------------------
# Section 9: build_temp_allocation_output
# SQL lines: 473-499
# ---------------------------------------------------------------------------
def build_temp_allocation_output(spark, cfg, lookthrough_output_df, not_rounded_lines_df):
    """Aggregate LookThroughAllocationOutput into TempAllocationOutput."""
    log_section("build_temp_allocation_output")
    t0 = time.time()

    k1_lt = cfg["k1_line_type_id"]
    book_k1_adj_lt = cfg["book_k1_adjustment_line_type_id"]
    ubti_lt = cfg["ubti_line_type_id"]
    passive_lt = cfg["passive_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]
    book_k1_adjustment_enabled = cfg["book_k1_adjustment_enabled"]

    ao_df = lookthrough_output_df.filter(
        F.col("LineTypeID").isin(k1_lt, book_k1_adj_lt, ubti_lt, passive_lt, box_jkl_lt)
    )

    if book_k1_adjustment_enabled and not_rounded_lines_df is not None:
        ao_df = ao_df.join(
            not_rounded_lines_df.select("EntityID", "LineID", "LineTypeID"),
            on=["EntityID", "LineID", "LineTypeID"],
            how="left_anti"
        )

    temp_alloc_output_df = ao_df.groupBy(
        F.coalesce(F.col("ShareClass"), F.lit("")).alias("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("LineTypeID"),
        F.col("EntityID"),
        F.col("ParentEntityID"),
        F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("AdjustmentTypeID"),
        F.col("TrackingKey"),
        F.coalesce(F.col("SuperParentEntityID"), F.lit(0)).alias("SuperParentEntityID"),
        F.coalesce(F.col("Tag"), F.lit("")).alias("Tag"),
        F.col("OriginalParentEntityID"),
        F.col("QuickLinkID"),
    ).agg(
        F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("Amount")
    )

    log_timing("build_temp_allocation_output", t0)
    return temp_alloc_output_df


# ---------------------------------------------------------------------------
# Section 10: compute_rounding_diff
# SQL lines: 500-530
# ---------------------------------------------------------------------------
def compute_rounding_diff(spark, cfg, temp_alloc_output_df, allocation_input_df):
    """Compute TempAggregatedAmount, TempAggregatedRoundedAmount, TempRoundedDiff."""
    log_section("compute_rounding_diff")
    t0 = time.time()

    # TempAggregatedAmount
    temp_agg_amount = temp_alloc_output_df.groupBy(
        "EntityID", "ParentEntityID", "LineID", "LineTypeID",
        F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("AdjustmentTypeID"),
        "TrackingKey", "SuperParentEntityID", "Tag",
        "OriginalParentEntityID", "QuickLinkID",
    ).agg(
        F.sum(sql_round(F.coalesce(F.col("Amount"), F.lit(0.0)), 0)).alias("TotalAmount")
    )

    # TempAggregatedRoundedAmount
    temp_agg_rounded = allocation_input_df.groupBy(
        "EntityID", "ParentEntityID", "LineID", "LineTypeID",
        F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("AdjustmentTypeID"),
        "TrackingKey",
        F.coalesce(F.col("SuperParentEntityID"), F.lit(0)).alias("SuperParentEntityID"),
        "Tag", "QuickLinkID",
    ).agg(
        F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("TotalAmount")
    ).filter(F.col("TotalAmount") != 0)

    # TempRoundedDiff
    rounded_diff_df = temp_agg_amount.alias("TA").join(
        temp_agg_rounded.alias("TR"),
        (F.col("TA.EntityID") == F.col("TR.EntityID")) &
        (F.col("TA.LineID") == F.col("TR.LineID")) &
        (F.col("TA.LineTypeID") == F.col("TR.LineTypeID")) &
        (ns(F.col("TA.QuickLinkID"), "0") == ns(F.col("TR.QuickLinkID"), "0")) &
        (ns(F.col("TA.AdjustmentTypeID"), "0") == ns(F.col("TR.AdjustmentTypeID"), "0")) &
        (ns(F.col("TA.TrackingKey")) == ns(F.col("TR.TrackingKey"))) &
        (ns(F.col("TA.Tag")) == ns(F.col("TR.Tag"))),
        "left"
    ).select(
        F.col("TA.EntityID"),
        F.col("TA.ParentEntityID"),
        F.col("TA.LineID"),
        F.col("TA.LineTypeID"),
        F.col("TA.AdjustmentTypeID"),
        (F.coalesce(F.col("TR.TotalAmount"), F.lit(0.0)) -
         F.coalesce(F.col("TA.TotalAmount"), F.lit(0.0))).alias("DiffAmount"),
        F.col("TA.TrackingKey"),
        F.col("TA.SuperParentEntityID"),
        F.coalesce(F.col("TA.Tag"), F.lit("")).alias("Tag"),
        F.col("TA.OriginalParentEntityID"),
        F.col("TA.QuickLinkID"),
    )

    log_timing("compute_rounding_diff", t0)
    return rounded_diff_df


# ---------------------------------------------------------------------------
# Section 11: compute_max_allocation_type
# SQL lines: 531-556
# ---------------------------------------------------------------------------
def compute_max_allocation_type(spark, cfg, lookthrough_output_df):
    """Determine the dominant AllocationType per line using RANK window function."""
    log_section("compute_max_allocation_type")
    t0 = time.time()

    agg_by_alloc_type = lookthrough_output_df.groupBy(
        "EntityID", "TrackingKey", "LineTypeID", "LineID", "AllocationType",
        F.coalesce(F.col("Tag"), F.lit("")).alias("Tag"), "QuickLinkID",
    ).agg(
        F.sum("Amount").alias("Amount")
    )

    w = Window.partitionBy(
        "EntityID", "TrackingKey", "LineTypeID", "LineID", "Tag", "QuickLinkID"
    ).orderBy(F.col("Amount").desc(), F.col("AllocationType").asc())

    ranked = agg_by_alloc_type.withColumn("Rnk", F.rank().over(w))

    max_alloc_type_df = ranked.filter(F.col("Rnk") == 1).select(
        "EntityID", "TrackingKey", "LineTypeID", "LineID", "AllocationType", "Tag", "QuickLinkID"
    )

    log_timing("compute_max_allocation_type", t0)
    return max_alloc_type_df
