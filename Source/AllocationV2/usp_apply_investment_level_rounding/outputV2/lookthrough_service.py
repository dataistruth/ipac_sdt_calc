"""
LookThrough data loading services (Sections 2-5).

Four variants based on IsInvestmentLevelRounding and CallFrom.
"""

import pyspark.sql.functions as F
import time

from Common_V2.core.helpers import tbl, ns
from Common_V2.core.observability import log_section, log_timing


# ---------------------------------------------------------------------------
# Section 2: build_lookthrough_output_inv_level_callfrom
# SQL lines: 142-191
# ---------------------------------------------------------------------------
def build_lookthrough_output_inv_level_callfrom(spark, cfg):
    """Load LookThroughAllocationOutput/Input when IsInvestmentLevelRounding='C' and CallFrom is set."""
    log_section("build_lookthrough_output_inv_level_callfrom")
    t0 = time.time()

    run_id = cfg["run_id"]
    call_from = cfg["call_from"]

    os_df = tbl(spark, "LookThroughOffsetUnRoundedLines", cfg).filter(
        F.col("RunID") == run_id
    ).alias("OS")
    lo_df = tbl(spark, "LookThroughAllocationOutput", cfg).filter(
        F.col("RunID") == run_id
    ).alias("LO")

    joined = lo_df.join(
        os_df,
        (F.col("LO.RunID") == F.col("OS.RunID")) &
        (F.col("LO.EntityID") == F.col("OS.EntityID")) &
        (F.col("LO.LineTypeID") == F.col("OS.LineTypeID")) &
        (F.col("LO.LineID") == F.col("OS.LineID")) &
        (ns(F.col("LO.TrackingKey")) == ns(F.col("OS.TrackingKey"))),
        "inner"
    ).filter(
        (F.lower(F.col("OS.SourceType")) == call_from.lower()) &
        (F.coalesce(F.col("OS.IsRounded").cast("int"), F.lit(0)) == 0)
    )

    lookthrough_output_df = joined.select(
        F.col("LO.EntityID"), F.col("LO.ParentEntityID"),
        F.col("LO.PartnerNumber"), F.col("LO.LineID"), F.col("LO.LineTypeID"),
        F.col("LO.ShareClass"), F.col("LO.AdjustmentTypeID"),
        F.col("LO.TrackingKey"), F.col("LO.SuperParentEntityID"),
        F.col("LO.Tag"),
        F.col("LO.AllocationTypeID"),
        F.col("LO.Amount"), F.col("LO.PeriodID"),
        F.coalesce(F.col("LO.AllocationType"), F.lit("ProRata")).alias("AllocationType"),
        F.col("LO.OriginalParentEntityID"), F.col("LO.QuickLinkID"),
    ).distinct()

    # LookThroughAllocationInput
    li_df = tbl(spark, "LookThroughAllocationInput", cfg).filter(
        F.col("RunID") == run_id
    ).alias("T1")
    joined_input = li_df.join(
        os_df,
        (F.col("T1.RunID") == F.col("OS.RunID")) &
        (F.col("T1.EntityID") == F.col("OS.EntityID")) &
        (F.col("T1.LineID") == F.col("OS.LineID")) &
        (ns(F.col("T1.TrackingKey")) == ns(F.col("OS.TrackingKey"))),
        "inner"
    ).filter(
        (F.lower(F.col("OS.SourceType")) == call_from.lower()) &
        (F.coalesce(F.col("OS.IsRounded").cast("int"), F.lit(0)) == 0) &
        (F.col("T1.FlowUpPartner").isNull())
    )

    lookthrough_input_df = joined_input.select(
        F.col("T1.EntityID"), F.col("T1.ParentEntityID"),
        F.col("T1.LineID"), F.col("T1.LineTypeID"),
        F.col("T1.Amount704b").alias("Amount"), F.col("T1.AdjustmentTypeID"),
        F.col("T1.TrackingKey"), F.col("T1.SuperParentEntityID"),
        ns(F.col("T1.Tag")).alias("Tag"),
    )

    log_timing("build_lookthrough_output_inv_level_callfrom", t0)
    return lookthrough_output_df, lookthrough_input_df


# ---------------------------------------------------------------------------
# Section 3: build_lookthrough_output_inv_level_no_callfrom
# SQL lines: 192-232
# ---------------------------------------------------------------------------
def build_lookthrough_output_inv_level_no_callfrom(spark, cfg):
    """Load LookThroughAllocationOutput/Input when IsInvestmentLevelRounding='C' and no CallFrom."""
    log_section("build_lookthrough_output_inv_level_no_callfrom")
    t0 = time.time()

    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    k1_lt = cfg["k1_line_type_id"]
    book_k1_adj_lt = cfg["book_k1_adjustment_line_type_id"]
    ubti_lt = cfg["ubti_line_type_id"]
    passive_lt = cfg["passive_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]

    lo_df = tbl(spark, "LookThroughAllocationOutput", cfg).filter(F.col("RunID") == run_id)

    # Part 1: K1, BookK1Adj, UBTI line types — row-level
    part1 = lo_df.filter(
        F.col("LineTypeID").isin(k1_lt, book_k1_adj_lt, ubti_lt)
    ).select(
        "EntityID", "ParentEntityID", "PartnerNumber", "LineID", "LineTypeID",
        "ShareClass", "AdjustmentTypeID", "TrackingKey", "SuperParentEntityID",
        "Tag", "AllocationTypeID", "Amount", "PeriodID",
        F.coalesce(F.col("AllocationType"), F.lit("ProRata")).alias("AllocationType"),
        "OriginalParentEntityID", "QuickLinkID",
    )

    # Part 2: Passive + BoxJKL — grouped
    part2 = lo_df.filter(
        F.col("LineTypeID").isin(passive_lt, box_jkl_lt)
    ).groupBy(
        "PartnerNumber", "LineID", "LineTypeID", "ShareClass",
        "AdjustmentTypeID", "Tag", "PeriodID", "QuickLinkID"
    ).agg(
        F.sum("Amount").alias("Amount")
    ).select(
        F.lit(entity_id).alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("PartnerNumber"), F.col("LineID"), F.col("LineTypeID"),
        F.col("ShareClass"), F.col("AdjustmentTypeID"),
        F.lit(None).cast("string").alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.col("Tag"),
        F.lit(None).cast("int").alias("AllocationTypeID"),
        F.col("Amount"), F.col("PeriodID"),
        F.lit(None).cast("string").alias("AllocationType"),
        F.lit(None).cast("int").alias("OriginalParentEntityID"),
        F.col("QuickLinkID"),
    )

    lookthrough_output_df = part1.unionByName(part2, allowMissingColumns=True)

    # LookThroughAllocationInput
    li_df = tbl(spark, "LookThroughAllocationInput", cfg).filter(
        (F.col("RunID") == run_id) & (F.col("FlowUpPartner").isNull())
    )
    lookthrough_input_df = li_df.select(
        "EntityID", "ParentEntityID", "LineID", "LineTypeID",
        F.col("Amount704b").alias("Amount"), "AdjustmentTypeID",
        "TrackingKey", "SuperParentEntityID", ns(F.col("Tag")).alias("Tag"),
    )

    log_timing("build_lookthrough_output_inv_level_no_callfrom", t0)
    return lookthrough_output_df, lookthrough_input_df


# ---------------------------------------------------------------------------
# Section 4: build_lookthrough_output_entity_callfrom
# SQL lines: 233-261
# ---------------------------------------------------------------------------
def build_lookthrough_output_entity_callfrom(spark, cfg):
    """Entity-level grouped load when IsInvestmentLevelRounding != 'C' and CallFrom is set."""
    log_section("build_lookthrough_output_entity_callfrom")
    t0 = time.time()

    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    call_from = cfg["call_from"]
    k1_lt = cfg["k1_line_type_id"]

    os_df = tbl(spark, "LookThroughOffsetUnRoundedLines", cfg).filter(
        F.col("RunID") == run_id
    ).alias("OS")
    lo_df = tbl(spark, "LookThroughAllocationOutput", cfg).filter(
        F.col("RunID") == run_id
    ).alias("LO")

    joined = lo_df.join(
        os_df,
        (F.col("LO.RunID") == F.col("OS.RunID")) &
        (F.col("LO.EntityID") == F.col("OS.EntityID")) &
        (F.col("LO.LineTypeID") == F.col("OS.LineTypeID")) &
        (F.col("LO.LineID") == F.col("OS.LineID")) &
        (ns(F.col("LO.TrackingKey")) == ns(F.col("OS.TrackingKey"))),
        "inner"
    ).filter(
        (F.col("LO.LineTypeID") == k1_lt) &
        (F.lower(F.col("OS.SourceType")) == call_from.lower()) &
        (F.coalesce(F.col("OS.IsRounded").cast("int"), F.lit(0)) == 0)
    )

    lookthrough_output_df = joined.groupBy(
        F.col("LO.PartnerNumber"), F.col("LO.LineID"), F.col("LO.LineTypeID"),
        F.col("LO.ShareClass"), F.col("LO.AdjustmentTypeID"), F.col("LO.Tag"),
        F.col("LO.PeriodID"), F.col("LO.QuickLinkID"),
    ).agg(
        F.sum(F.col("LO.Amount")).alias("Amount")
    ).select(
        F.lit(entity_id).alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("PartnerNumber"), F.col("LineID"), F.col("LineTypeID"),
        F.col("ShareClass"), F.col("AdjustmentTypeID"),
        F.lit(None).cast("string").alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.col("Tag"),
        F.lit(None).cast("int").alias("AllocationTypeID"),
        F.col("Amount"), F.col("PeriodID"),
        F.lit(None).cast("string").alias("AllocationType"),
        F.lit(None).cast("int").alias("OriginalParentEntityID"),
        F.col("QuickLinkID"),
    )

    # Input
    li_df = tbl(spark, "LookThroughAllocationInput", cfg).filter(
        F.col("RunID") == run_id
    ).alias("T1")
    joined_input = li_df.join(
        os_df,
        (F.col("T1.RunID") == F.col("OS.RunID")) &
        (F.col("T1.EntityID") == F.col("OS.EntityID")) &
        (F.col("T1.LineID") == F.col("OS.LineID")) &
        (ns(F.col("T1.TrackingKey")) == ns(F.col("OS.TrackingKey"))),
        "inner"
    ).filter(
        (F.lower(F.col("OS.SourceType")) == call_from.lower()) &
        (F.coalesce(F.col("OS.IsRounded").cast("int"), F.lit(0)) == 0) &
        (F.col("T1.FlowUpPartner").isNull())
    )

    lookthrough_input_df = joined_input.groupBy(
        F.col("T1.LineID"), F.col("T1.LineTypeID"),
        F.col("T1.AdjustmentTypeID"),
        ns(F.col("T1.Tag")).alias("Tag"),
    ).agg(
        F.sum(F.col("T1.Amount704b")).alias("Amount")
    ).select(
        F.lit(entity_id).alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("LineID"), F.col("LineTypeID"),
        F.col("Amount"), F.col("AdjustmentTypeID"),
        F.lit(None).cast("string").alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.col("Tag"),
    )

    log_timing("build_lookthrough_output_entity_callfrom", t0)
    return lookthrough_output_df, lookthrough_input_df


# ---------------------------------------------------------------------------
# Section 5: build_lookthrough_output_entity_no_callfrom
# SQL lines: 262-283
# ---------------------------------------------------------------------------
def build_lookthrough_output_entity_no_callfrom(spark, cfg):
    """Entity-level grouped load when IsInvestmentLevelRounding != 'C' and no CallFrom."""
    log_section("build_lookthrough_output_entity_no_callfrom")
    t0 = time.time()

    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]

    lo_df = tbl(spark, "LookThroughAllocationOutput", cfg).filter(F.col("RunID") == run_id)

    lookthrough_output_df = lo_df.groupBy(
        "PartnerNumber", "LineID", "LineTypeID", "ShareClass",
        "AdjustmentTypeID", "Tag", "PeriodID", "QuickLinkID",
    ).agg(
        F.sum("Amount").alias("Amount")
    ).select(
        F.lit(entity_id).alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("PartnerNumber"), F.col("LineID"), F.col("LineTypeID"),
        F.col("ShareClass"), F.col("AdjustmentTypeID"),
        F.lit(None).cast("string").alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.col("Tag"),
        F.lit(None).cast("int").alias("AllocationTypeID"),
        F.col("Amount"), F.col("PeriodID"),
        F.lit(None).cast("string").alias("AllocationType"),
        F.lit(None).cast("int").alias("OriginalParentEntityID"),
        F.col("QuickLinkID"),
    )

    li_df = tbl(spark, "LookThroughAllocationInput", cfg).filter(
        (F.col("RunID") == run_id) & (F.col("FlowUpPartner").isNull())
    )
    lookthrough_input_df = li_df.groupBy(
        "LineID", "LineTypeID", "AdjustmentTypeID",
        ns(F.col("Tag")).alias("Tag"),
    ).agg(
        F.sum("Amount704b").alias("Amount")
    ).select(
        F.lit(entity_id).alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("LineID"), F.col("LineTypeID"),
        F.col("Amount"), F.col("AdjustmentTypeID"),
        F.lit(None).cast("string").alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.col("Tag"),
    )

    log_timing("build_lookthrough_output_entity_no_callfrom", t0)
    return lookthrough_output_df, lookthrough_input_df
