"""
Rounding strategy services (Sections 13, 14, 17, 18, 19).

- apply_rounding_override: CROSS JOIN distribution to override partners.
- apply_rounding_plugged_to_gp: Plug to GP partner.
- apply_rounding_highest_percent: Plug to highest allocation percent partner.
- apply_rounding_highest_amount: Plug to highest allocation amount partner.
- apply_rounding_none: Pass through without rounding.
"""

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import time

from Common_V2.core.helpers import ns, sql_round
from Common_V2.core.observability import log_section, log_timing


# ---------------------------------------------------------------------------
# Section 13: apply_rounding_override
# SQL lines: 581-700
# ---------------------------------------------------------------------------
def apply_rounding_override(spark, cfg, temp_alloc_output_df, rounded_diff_df,
                            max_alloc_type_df, rounding_override_df, alloc_output_detail_df):
    """Apply rounding using override partners (CROSS JOIN, mod distribution, ROW_NUMBER remainder)."""
    log_section("apply_rounding_override")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    ubti_lt = cfg["ubti_line_type_id"]
    passive_lt = cfg["passive_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]
    book_k1_adj_lt = cfg["book_k1_adjustment_line_type_id"]

    if alloc_output_detail_df is None:
        alloc_output_detail_df = temp_alloc_output_df.select(
            "EntityID", "LineTypeID", "LineID", "ParentEntityID", "TrackingKey",
            "SuperParentEntityID", "AdjustmentTypeID",
            F.coalesce(F.col("Tag"), F.lit("")).alias("Tag"),
            "OriginalParentEntityID", "QuickLinkID",
        ).distinct()

    p_count = rounding_override_df.count()

    # CROSS JOIN: RoundingOverride x AllocationOutputDetail
    rpa_base = rounding_override_df.crossJoin(alloc_output_detail_df)

    # Get amounts from TempAllocationOutput
    # Pre-aggregate to JOIN keys to prevent row multiplication (RR-5).
    # SQL UPDATE picks last-write-wins (same row count); PySpark LEFT JOIN multiplies.
    # Aggregating Amount to the 7 JOIN keys makes the right side unique.
    tao_for_join = temp_alloc_output_df.groupBy(
        F.col("LineTypeID").alias("j_LineTypeID"),
        F.col("LineID").alias("j_LineID"),
        F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("j_AdjTypeID"),
        F.col("PartnerNumber").alias("j_PN"),
        ns(F.col("TrackingKey")).alias("j_TK"),
        F.col("EntityID").alias("j_EID"),
        F.coalesce(F.col("Tag"), F.lit("")).alias("j_Tag"),
    ).agg(
        sql_round(F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))), 0).alias("MatchedAmount")
    )

    rpa = rpa_base.join(
        tao_for_join,
        (F.col("LineTypeID") == F.col("j_LineTypeID")) &
        (F.col("LineID") == F.col("j_LineID")) &
        (F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)) == F.col("j_AdjTypeID")) &
        (F.col("PartnerNumber") == F.col("j_PN")) &
        (ns(F.col("TrackingKey")) == F.col("j_TK")) &
        (F.col("EntityID") == F.col("j_EID")) &
        (F.coalesce(F.col("Tag"), F.lit("")) == F.col("j_Tag")),
        "left"
    ).select(
        rpa_base["PartnerNumber"], rpa_base["ShareClass"],
        rpa_base["Name1"], rpa_base["Name2"], rpa_base["Name3"],
        rpa_base["EntityID"], rpa_base["LineTypeID"], rpa_base["LineID"],
        rpa_base["ParentEntityID"], rpa_base["TrackingKey"],
        rpa_base["SuperParentEntityID"],
        F.coalesce(rpa_base["Tag"], F.lit("")).alias("Tag"),
        rpa_base["AdjustmentTypeID"],
        rpa_base["OriginalParentEntityID"], rpa_base["QuickLinkID"],
        F.coalesce(F.col("MatchedAmount"), F.lit(0.0)).alias("Amount"),
    )

    # RoundingAmount = ROUND(DiffAmount, 0) / @PCount
    # Pre-aggregate to JOIN keys to prevent row multiplication (RR-4).
    # SQL UPDATE picks last-write-wins (same row count); PySpark LEFT JOIN multiplies.
    # Aggregating DiffAmount to the 6 JOIN keys makes the right side unique.
    diff_for_join = rounded_diff_df.groupBy(
        F.col("LineTypeID").alias("d_LineTypeID"),
        F.col("LineID").alias("d_LineID"),
        F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("d_AdjTypeID"),
        ns(F.col("TrackingKey")).alias("d_TK"),
        F.col("EntityID").alias("d_EID"),
        F.coalesce(F.col("Tag"), F.lit("")).alias("d_Tag"),
    ).agg(
        F.sum(F.col("DiffAmount")).alias("DiffAmount")
    )

    rpa = rpa.join(
        diff_for_join,
        (F.col("LineTypeID") == F.col("d_LineTypeID")) &
        (F.col("LineID") == F.col("d_LineID")) &
        (F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)) == F.col("d_AdjTypeID")) &
        (ns(F.col("TrackingKey")) == F.col("d_TK")) &
        (F.col("EntityID") == F.col("d_EID")) &
        (F.coalesce(F.col("Tag"), F.lit("")) == F.col("d_Tag")),
        "left"
    ).withColumn(
        "RoundingAmount",
        (F.round(F.coalesce(F.col("DiffAmount"), F.lit(0.0)), 0) / F.lit(p_count)).cast("long")
    ).withColumn(
        "ModAmount",
        (F.round(F.coalesce(F.col("DiffAmount"), F.lit(0.0)), 0).cast("decimal(38,0)") % F.lit(p_count)).cast("int")
    )

    # Distribute remainder
    w_order = Window.partitionBy(
        "EntityID", "LineTypeID", "LineID", "TrackingKey", "AdjustmentTypeID", "Tag"
    ).orderBy(
        F.abs(F.coalesce(F.col("Amount"), F.lit(0.0))).desc(),
        F.col("Name1").asc(), F.col("Name2").asc(), F.col("Name3").asc()
    )

    rpa = rpa.withColumn("OrderID", F.row_number().over(w_order))

    rpa = rpa.withColumn(
        "RoundingAmount",
        F.when(
            (F.col("ModAmount") != 0) & (F.col("OrderID") <= F.abs(F.col("ModAmount"))),
            F.col("RoundingAmount") + F.when(F.col("ModAmount") > 0, F.lit(1)).otherwise(F.lit(-1))
        ).otherwise(F.col("RoundingAmount"))
    )

    # TrackingKey expression
    tracking_key_expr = (
        F.when(F.col("TrackingKey").isNull(),
               F.col("EntityID").cast("string"))
        .otherwise(F.concat(F.col("TrackingKey"), F.lit("~"), F.lit(str(entity_id))))
    )

    # K1Summary: override partners
    k1_override = rpa.alias("R").join(
        max_alloc_type_df.alias("M"),
        (F.col("R.EntityID") == F.col("M.EntityID")) &
        (ns(F.col("R.TrackingKey")) == ns(F.col("M.TrackingKey"))) &
        (F.col("R.LineID") == F.col("M.LineID")) &
        (F.col("R.LineTypeID") == F.col("M.LineTypeID")) &
        (ns(F.col("R.QuickLinkID"), "0") == ns(F.col("M.QuickLinkID"), "0")) &
        (F.coalesce(F.col("R.Tag"), F.lit("")) == F.coalesce(F.col("M.Tag"), F.lit(""))),
        "inner"
    ).filter(
        F.col("R.LineTypeID").isin(k1_lt, ubti_lt, passive_lt, box_jkl_lt)
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("R.EntityID").alias("EntityID"),
        F.col("R.ShareClass").alias("ShareClass"),
        F.col("R.PartnerNumber").alias("PartnerNumber"),
        F.col("R.LineID").alias("LineID"),
        F.col("R.LineTypeID").alias("LineTypeID"),
        F.when(F.coalesce(F.col("R.RoundingAmount"), F.lit(0)) != 0,
               sql_round(F.coalesce(F.col("R.Amount"), F.lit(0.0)), 0) + F.col("R.RoundingAmount"))
        .otherwise(sql_round(F.coalesce(F.col("R.Amount"), F.lit(0.0)), 0)).alias("Amount"),
        F.col("R.ParentEntityID").alias("ParentEntityID"),
        F.when(F.col("R.TrackingKey").isNull(), F.col("R.EntityID").cast("string"))
        .otherwise(F.concat(F.col("R.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))).alias("TrackingKey"),
        F.col("R.SuperParentEntityID").alias("SuperParentEntityID"),
        F.coalesce(F.col("R.Tag"), F.lit("")).alias("Tag"),
        F.col("R.OriginalParentEntityID").alias("OriginalParentEntityID"),
        F.col("R.QuickLinkID").alias("QuickLinkID"),
        F.col("M.AllocationType").alias("AllocationType"),
    )

    # K1Summary for non-override partners
    k1_non_override = temp_alloc_output_df.alias("TR").join(
        rpa.select(
            F.col("LineTypeID").alias("rp_LT"), F.col("LineID").alias("rp_LID"),
            F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("rp_ADJ"),
            ns(F.col("QuickLinkID"), "0").alias("rp_QL"),
            F.col("PartnerNumber").alias("rp_PN"),
            ns(F.col("TrackingKey")).alias("rp_TK"),
            F.col("EntityID").alias("rp_EID"),
            F.coalesce(F.col("Tag"), F.lit("")).alias("rp_Tag"),
        ),
        (F.col("TR.LineTypeID") == F.col("rp_LT")) &
        (F.col("TR.LineID") == F.col("rp_LID")) &
        (F.coalesce(F.col("TR.AdjustmentTypeID"), F.lit(0)) == F.col("rp_ADJ")) &
        (ns(F.col("TR.QuickLinkID"), "0") == F.col("rp_QL")) &
        (F.col("TR.PartnerNumber") == F.col("rp_PN")) &
        (ns(F.col("TR.TrackingKey")) == F.col("rp_TK")) &
        (F.col("TR.EntityID") == F.col("rp_EID")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.col("rp_Tag")),
        "left_anti"
    ).join(
        max_alloc_type_df.alias("M"),
        (F.col("TR.EntityID") == F.col("M.EntityID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("M.TrackingKey"))) &
        (F.col("TR.LineID") == F.col("M.LineID")) &
        (F.col("TR.LineTypeID") == F.col("M.LineTypeID")) &
        (ns(F.col("TR.QuickLinkID"), "0") == ns(F.col("M.QuickLinkID"), "0")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("M.Tag"), F.lit(""))),
        "inner"
    ).filter(
        F.col("TR.LineTypeID").isin(k1_lt, ubti_lt, passive_lt, box_jkl_lt)
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        F.col("TR.LineTypeID"),
        sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0).alias("Amount"),
        F.col("TR.ParentEntityID"),
        F.when(F.col("TR.TrackingKey").isNull(), F.col("TR.EntityID").cast("string"))
        .otherwise(F.concat(F.col("TR.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))).alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
        F.coalesce(F.col("TR.Tag"), F.lit("")).alias("Tag"),
        F.col("TR.OriginalParentEntityID"),
        F.col("TR.QuickLinkID"),
        F.col("M.AllocationType").alias("AllocationType"),
    )

    k1_summary_df = k1_override.unionByName(k1_non_override)

    # AdjustmentSummary (BookK1Adj)
    adj_override = rpa.filter(
        F.col("LineTypeID") == book_k1_adj_lt
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.when(F.coalesce(F.col("RoundingAmount"), F.lit(0)) != 0,
               sql_round(F.coalesce(F.col("Amount"), F.lit(0.0)), 0) + F.col("RoundingAmount"))
        .otherwise(sql_round(F.coalesce(F.col("Amount"), F.lit(0.0)), 0)).alias("Amount"),
        F.col("ParentEntityID"),
        F.col("LineTypeID"),
        F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("AdjustmentTypeID"),
        tracking_key_expr.alias("TrackingKey"),
        F.col("SuperParentEntityID"),
    )

    adj_non_override = temp_alloc_output_df.alias("TR").join(
        rpa.filter(F.col("LineTypeID") == book_k1_adj_lt).select(
            F.col("LineTypeID").alias("rp_LT"), F.col("LineID").alias("rp_LID"),
            F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("rp_ADJ"),
            F.col("PartnerNumber").alias("rp_PN"),
            ns(F.col("TrackingKey")).alias("rp_TK"),
            F.col("EntityID").alias("rp_EID"),
        ),
        (F.col("TR.LineTypeID") == F.col("rp_LT")) &
        (F.col("TR.LineID") == F.col("rp_LID")) &
        (F.coalesce(F.col("TR.AdjustmentTypeID"), F.lit(0)) == F.col("rp_ADJ")) &
        (F.col("TR.PartnerNumber") == F.col("rp_PN")) &
        (ns(F.col("TR.TrackingKey")) == F.col("rp_TK")) &
        (F.col("TR.EntityID") == F.col("rp_EID")),
        "left_anti"
    ).filter(
        F.col("TR.LineTypeID") == book_k1_adj_lt
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0).alias("Amount"),
        F.col("TR.ParentEntityID"),
        F.col("TR.LineTypeID"),
        F.coalesce(F.col("TR.AdjustmentTypeID"), F.lit(0)).alias("AdjustmentTypeID"),
        F.when(F.col("TR.TrackingKey").isNull(), F.col("TR.EntityID").cast("string"))
        .otherwise(F.concat(F.col("TR.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))).alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
    )

    adjustment_summary_df = adj_override.unionByName(adj_non_override)

    log_timing("apply_rounding_override", t0)
    return k1_summary_df, adjustment_summary_df


# ---------------------------------------------------------------------------
# Section 14: apply_rounding_plugged_to_gp
# SQL lines: 701-750
# ---------------------------------------------------------------------------
def apply_rounding_plugged_to_gp(spark, cfg, temp_alloc_output_df, rounded_diff_df,
                                  max_alloc_type_df, partner_snapshot_df):
    """Plug rounding difference to the GP partner."""
    log_section("apply_rounding_plugged_to_gp")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    ubti_lt = cfg["ubti_line_type_id"]
    passive_lt = cfg["passive_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]
    book_k1_adj_lt = cfg["book_k1_adjustment_line_type_id"]

    # GP partner lookup
    gp_row = partner_snapshot_df.filter(F.lower(F.col("GPorLP")) == "g").select(
        "PartnerNumber", "ShareClass"
    ).first()
    rounding_pn = gp_row["PartnerNumber"] if gp_row else None
    rounding_sc = gp_row["ShareClass"] if gp_row else ""

    # Dummy insert for GP partner missing from TempAllocationOutput
    dummy_gp = rounded_diff_df.alias("TD").join(
        temp_alloc_output_df.filter(F.col("PartnerNumber") == rounding_pn).alias("TR"),
        (F.col("TD.EntityID") == F.col("TR.EntityID")) &
        (F.col("TD.LineID") == F.col("TR.LineID")) &
        (F.col("TD.LineTypeID") == F.col("TR.LineTypeID")) &
        (ns(F.col("TD.TrackingKey")) == ns(F.col("TR.TrackingKey"))) &
        (F.col("TD.AdjustmentTypeID") == F.col("TR.AdjustmentTypeID")) &
        (F.coalesce(F.col("TD.Tag"), F.lit("")) == F.coalesce(F.col("TR.Tag"), F.lit(""))) &
        (ns(F.col("TD.QuickLinkID"), "0") == ns(F.col("TR.QuickLinkID"), "0")),
        "left_anti"
    ).filter(
        F.coalesce(F.col("TD.DiffAmount"), F.lit(0)) != 0
    ).select(
        F.col("TD.EntityID"), F.col("TD.ParentEntityID"),
        F.lit(rounding_sc).alias("ShareClass"),
        F.lit(rounding_pn).alias("PartnerNumber"),
        F.col("TD.LineID"), F.col("TD.LineTypeID"),
        F.col("TD.AdjustmentTypeID"),
        F.lit(0.0).alias("Amount"),
        F.col("TD.TrackingKey"), F.col("TD.SuperParentEntityID"),
        F.coalesce(F.col("TD.Tag"), F.lit("")).alias("Tag"),
        F.col("TD.OriginalParentEntityID"), F.col("TD.QuickLinkID"),
    )

    combined_output = temp_alloc_output_df.unionByName(dummy_gp, allowMissingColumns=True)

    # TrackingKey expression
    tracking_key_expr = (
        F.when(F.col("TR.TrackingKey").isNull(), F.col("TR.EntityID").cast("string"))
        .otherwise(F.concat(F.col("TR.TrackingKey"), F.lit("~"), F.lit(str(entity_id))))
    )

    # K1Summary
    k1_summary_df = combined_output.alias("TR").join(
        rounded_diff_df.alias("TD"),
        (F.col("TR.EntityID") == F.col("TD.EntityID")) &
        (F.col("TR.LineID") == F.col("TD.LineID")) &
        (F.col("TR.LineTypeID") == F.col("TD.LineTypeID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("TD.TrackingKey"))) &
        (F.col("TR.AdjustmentTypeID") == F.col("TD.AdjustmentTypeID")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("TD.Tag"), F.lit(""))) &
        (ns(F.col("TR.QuickLinkID"), "0") == ns(F.col("TD.QuickLinkID"), "0")),
        "left"
    ).join(
        max_alloc_type_df.alias("M"),
        (F.col("TR.EntityID") == F.col("M.EntityID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("M.TrackingKey"))) &
        (F.col("TR.LineID") == F.col("M.LineID")) &
        (F.col("TR.LineTypeID") == F.col("M.LineTypeID")) &
        (ns(F.col("TR.QuickLinkID"), "0") == ns(F.col("M.QuickLinkID"), "0")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("M.Tag"), F.lit(""))),
        "inner"
    ).filter(
        F.col("TR.LineTypeID").isin(k1_lt, ubti_lt, passive_lt, box_jkl_lt)
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        F.col("TR.LineTypeID"),
        F.when(
            F.coalesce(F.col("TD.DiffAmount"), F.lit(0)) != 0,
            F.when(
                (F.col("TR.PartnerNumber") == F.lit(rounding_pn)) &
                (F.coalesce(F.col("TR.ShareClass"), F.lit("")) == F.lit(rounding_sc)),
                sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0) + F.coalesce(F.col("TD.DiffAmount"), F.lit(0))
            ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0))
        ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0)).alias("Amount"),
        F.col("TR.ParentEntityID"),
        tracking_key_expr.alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
        F.coalesce(F.col("TR.Tag"), F.lit("")).alias("Tag"),
        F.col("M.AllocationType").alias("AllocationType"),
        F.col("TR.OriginalParentEntityID"),
        F.col("TR.QuickLinkID"),
    )

    # AdjustmentSummary (BookK1Adj)
    adjustment_summary_df = combined_output.alias("TR").join(
        rounded_diff_df.alias("TD"),
        (F.col("TR.EntityID") == F.col("TD.EntityID")) &
        (F.col("TR.LineID") == F.col("TD.LineID")) &
        (F.col("TR.LineTypeID") == F.col("TD.LineTypeID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("TD.TrackingKey"))) &
        (F.col("TR.AdjustmentTypeID") == F.col("TD.AdjustmentTypeID")),
        "left"
    ).filter(
        F.col("TR.LineTypeID") == book_k1_adj_lt
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        F.when(
            F.coalesce(F.col("TD.DiffAmount"), F.lit(0)) != 0,
            F.when(
                (F.col("TR.PartnerNumber") == F.lit(rounding_pn)) &
                (F.coalesce(F.col("TR.ShareClass"), F.lit("")) == F.lit(rounding_sc)),
                sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0) + F.coalesce(F.col("TD.DiffAmount"), F.lit(0))
            ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0))
        ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0)).alias("Amount"),
        F.col("TR.ParentEntityID"),
        F.col("TR.LineTypeID"),
        F.col("TR.AdjustmentTypeID"),
        F.when(F.col("TR.TrackingKey").isNull(), F.col("TR.EntityID").cast("string"))
        .otherwise(F.concat(F.col("TR.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))).alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
    )

    log_timing("apply_rounding_plugged_to_gp", t0)
    return k1_summary_df, adjustment_summary_df


# ---------------------------------------------------------------------------
# Section 17: apply_rounding_highest_percent
# SQL lines: 861-1010
# ---------------------------------------------------------------------------
def apply_rounding_highest_percent(spark, cfg, temp_alloc_output_df, rounded_diff_df,
                                    max_alloc_type_df, highest_pct_partner_df,
                                    nocost_df, partner_snapshot_df):
    """Apply rounding by plugging to highest allocation percent partner."""
    log_section("apply_rounding_highest_percent")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    ubti_lt = cfg["ubti_line_type_id"]
    passive_lt = cfg["passive_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]
    book_k1_adj_lt = cfg["book_k1_adjustment_line_type_id"]

    rounding_pn = cfg.get("rounding_partner_number")
    rounding_sc = cfg.get("rounding_share_class", "")

    def _tracking_key_expr(tk_col, eid_col):
        return (
            F.when(F.col(tk_col).isNull(), F.col(eid_col).cast("string"))
            .otherwise(F.concat(F.col(tk_col), F.lit("~"), F.lit(str(entity_id))))
        )

    # K1Summary
    k1_summary_df = temp_alloc_output_df.alias("TR").join(
        rounded_diff_df.alias("TD"),
        (F.col("TR.EntityID") == F.col("TD.EntityID")) &
        (F.col("TR.LineID") == F.col("TD.LineID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("TD.TrackingKey"))) &
        (F.col("TR.LineTypeID") == F.col("TD.LineTypeID")) &
        (ns(F.col("TR.QuickLinkID"), "0") == ns(F.col("TD.QuickLinkID"), "0")) &
        (F.col("TR.AdjustmentTypeID") == F.col("TD.AdjustmentTypeID")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("TD.Tag"), F.lit(""))) &
        (F.col("TR.SuperParentEntityID") == F.col("TD.SuperParentEntityID")),
        "left"
    ).join(
        highest_pct_partner_df.alias("HP") if highest_pct_partner_df is not None
        else spark.createDataFrame([], "EntityID int, PartnerNumber string, LineID int, TrackingKey string, LineTypeID int, AdjustmentTypeID int, Tag string").alias("HP"),
        (F.col("HP.EntityID") == F.col("TR.EntityID")) &
        (F.col("HP.PartnerNumber") == F.col("TR.PartnerNumber")) &
        (F.col("HP.LineID") == F.col("TR.LineID")) &
        (ns(F.col("HP.TrackingKey")) == ns(F.col("TR.TrackingKey"))) &
        (F.col("HP.AdjustmentTypeID") == F.col("TR.AdjustmentTypeID")) &
        (F.coalesce(F.col("HP.Tag"), F.lit("")) == F.coalesce(F.col("TR.Tag"), F.lit(""))) &
        (F.col("HP.LineTypeID") == F.lit(k1_lt)),
        "left"
    ).join(
        nocost_df.alias("NC") if nocost_df is not None
        else spark.createDataFrame([], "EntityID int, LineID int, TrackingKey string, Tag string, LineTypeID int").alias("NC"),
        (F.col("NC.EntityID") == F.col("TR.EntityID")) &
        (F.col("NC.LineID") == F.col("TR.LineID")) &
        (ns(F.col("NC.TrackingKey")) == ns(F.col("TR.TrackingKey"))) &
        (F.coalesce(F.col("NC.Tag"), F.lit("")) == F.coalesce(F.col("TR.Tag"), F.lit(""))) &
        (F.col("NC.LineTypeID") == F.col("TR.LineTypeID")),
        "left"
    ).join(
        max_alloc_type_df.alias("M"),
        (F.col("TR.EntityID") == F.col("M.EntityID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("M.TrackingKey"))) &
        (F.col("TR.LineID") == F.col("M.LineID")) &
        (F.col("TR.LineTypeID") == F.col("M.LineTypeID")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("M.Tag"), F.lit(""))),
        "inner"
    ).filter(
        F.col("TR.LineTypeID").isin(k1_lt, ubti_lt, passive_lt, box_jkl_lt)
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        F.col("TR.LineTypeID"),
        F.when(
            F.coalesce(F.col("TD.DiffAmount"), F.lit(0)) != 0,
            F.when(
                F.col("TR.PartnerNumber") == F.coalesce(F.col("HP.PartnerNumber"), F.lit("__NOMATCH__")),
                sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0) + F.coalesce(F.col("TD.DiffAmount"), F.lit(0))
            ).when(
                (F.col("NC.EntityID").isNotNull()) & (F.col("TR.PartnerNumber") == F.lit(rounding_pn)),
                sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0) + F.coalesce(F.col("TD.DiffAmount"), F.lit(0))
            ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0))
        ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0)).alias("Amount"),
        F.col("TR.ParentEntityID"),
        _tracking_key_expr("TR.TrackingKey", "TR.EntityID").alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
        F.coalesce(F.col("TR.Tag"), F.lit("")).alias("Tag"),
        F.col("M.AllocationType").alias("AllocationType"),
        F.col("TR.OriginalParentEntityID"),
        F.col("TR.QuickLinkID"),
    ).distinct()

    # AdjustmentSummary (BookK1Adj)
    adjustment_summary_df = temp_alloc_output_df.alias("TR").join(
        rounded_diff_df.alias("TD"),
        (F.col("TR.EntityID") == F.col("TD.EntityID")) &
        (F.col("TR.LineID") == F.col("TD.LineID")) &
        (F.col("TR.LineTypeID") == F.col("TD.LineTypeID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("TD.TrackingKey"))) &
        (F.col("TR.AdjustmentTypeID") == F.col("TD.AdjustmentTypeID")),
        "left"
    ).join(
        highest_pct_partner_df.alias("HP") if highest_pct_partner_df is not None
        else spark.createDataFrame([], "EntityID int, PartnerNumber string, LineID int, TrackingKey string, LineTypeID int, AdjustmentTypeID int, Tag string").alias("HP"),
        (F.col("HP.EntityID") == F.col("TR.EntityID")) &
        (F.col("HP.PartnerNumber") == F.col("TR.PartnerNumber")) &
        (F.col("HP.LineID") == F.col("TR.LineID")) &
        (ns(F.col("HP.TrackingKey")) == ns(F.col("TR.TrackingKey"))) &
        (F.col("HP.LineTypeID") == F.col("TR.LineTypeID")) &
        (F.col("HP.AdjustmentTypeID") == F.col("TR.AdjustmentTypeID")) &
        (F.coalesce(F.col("HP.Tag"), F.lit("")) == F.coalesce(F.col("TR.Tag"), F.lit(""))) &
        (F.col("HP.LineTypeID") == F.lit(book_k1_adj_lt)),
        "left"
    ).join(
        nocost_df.alias("NC") if nocost_df is not None
        else spark.createDataFrame([], "EntityID int, LineID int, TrackingKey string, Tag string, LineTypeID int").alias("NC"),
        (F.col("NC.EntityID") == F.col("TR.EntityID")) &
        (F.col("NC.LineID") == F.col("TR.LineID")) &
        (ns(F.col("NC.TrackingKey")) == ns(F.col("TR.TrackingKey"))) &
        (F.coalesce(F.col("NC.Tag"), F.lit("")) == F.coalesce(F.col("TR.Tag"), F.lit(""))) &
        (F.col("NC.LineTypeID") == F.lit(book_k1_adj_lt)),
        "left"
    ).filter(
        F.col("TR.LineTypeID") == book_k1_adj_lt
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        F.when(
            F.coalesce(F.col("TD.DiffAmount"), F.lit(0)) != 0,
            F.when(
                F.col("TR.PartnerNumber") == F.coalesce(F.col("HP.PartnerNumber"), F.lit("__NOMATCH__")),
                sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0) + F.coalesce(F.col("TD.DiffAmount"), F.lit(0))
            ).when(
                (F.col("NC.EntityID").isNotNull()) & (F.col("TR.PartnerNumber") == F.lit(rounding_pn)),
                sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0) + F.coalesce(F.col("TD.DiffAmount"), F.lit(0))
            ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0))
        ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0)).alias("Amount"),
        F.col("TR.ParentEntityID"),
        F.col("TR.LineTypeID"),
        F.col("TR.AdjustmentTypeID"),
        _tracking_key_expr("TR.TrackingKey", "TR.EntityID").alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
    ).distinct()

    # RoundingPartnerMissed
    rounded_data = partner_snapshot_df.alias("P").join(
        temp_alloc_output_df.alias("TR2"),
        F.col("TR2.PartnerNumber") == F.col("P.PartnerNumber"),
        "inner"
    ).join(
        highest_pct_partner_df.alias("HP2") if highest_pct_partner_df is not None
        else spark.createDataFrame([], "EntityID int, LineID int, TrackingKey string, LineTypeID int, AdjustmentTypeID int, PartnerNumber string").alias("HP2"),
        (F.col("HP2.EntityID") == F.col("TR2.EntityID")) &
        (F.col("HP2.LineID") == F.col("TR2.LineID")) &
        (ns(F.col("HP2.TrackingKey")) == ns(F.col("TR2.TrackingKey"))) &
        (F.col("HP2.LineTypeID") == F.col("TR2.LineTypeID")) &
        (F.col("HP2.AdjustmentTypeID") == F.col("TR2.AdjustmentTypeID")),
        "left"
    ).filter(
        F.col("P.PartnerNumber") == F.coalesce(F.col("HP2.PartnerNumber"), F.lit(rounding_pn))
    ).select(
        F.col("TR2.EntityID"), F.col("TR2.ParentEntityID"), F.col("TR2.SuperParentEntityID"),
        F.col("TR2.TrackingKey"), F.col("TR2.LineID"), F.col("TR2.LineTypeID"),
        F.col("TR2.AdjustmentTypeID"), F.col("TR2.ShareClass"),
        F.coalesce(F.col("TR2.Tag"), F.lit("")).alias("Tag"),
    ).distinct()

    rounding_missed = temp_alloc_output_df.alias("TR").join(
        rounded_diff_df.alias("TD"),
        (F.col("TR.EntityID") == F.col("TD.EntityID")) &
        (F.col("TR.LineID") == F.col("TD.LineID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("TD.TrackingKey"))) &
        (F.col("TR.LineTypeID") == F.col("TD.LineTypeID")) &
        (F.col("TR.AdjustmentTypeID") == F.col("TD.AdjustmentTypeID")) &
        (ns(F.col("TR.QuickLinkID"), "0") == ns(F.col("TD.QuickLinkID"), "0")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("TD.Tag"), F.lit(""))),
        "inner"
    ).join(
        rounded_data.alias("RD"),
        (F.col("TR.EntityID") == F.col("RD.EntityID")) &
        (F.col("TR.LineID") == F.col("RD.LineID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("RD.TrackingKey"))) &
        (F.col("TR.LineTypeID") == F.col("RD.LineTypeID")) &
        (F.col("TR.AdjustmentTypeID") == F.col("RD.AdjustmentTypeID")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("RD.Tag"), F.lit(""))),
        "left_anti"
    ).filter(
        F.col("TD.DiffAmount") != 0
    ).select(
        F.col("TR.EntityID"), F.col("TR.ParentEntityID"), F.col("TR.SuperParentEntityID"),
        F.col("TR.TrackingKey"), F.col("TR.LineID"), F.col("TR.LineTypeID"),
        F.col("TR.AdjustmentTypeID"), F.col("TR.ShareClass"),
        F.coalesce(F.col("TR.Tag"), F.lit("")).alias("Tag"),
        F.col("TR.OriginalParentEntityID"), F.col("TR.QuickLinkID"),
    ).distinct()

    # Missed partner insertion
    if rounding_pn is not None:
        missed_k1 = rounding_missed.alias("TR").join(
            rounded_diff_df.alias("TD"),
            (F.col("TR.EntityID") == F.col("TD.EntityID")) &
            (F.col("TR.LineID") == F.col("TD.LineID")) &
            (ns(F.col("TR.TrackingKey")) == ns(F.col("TD.TrackingKey"))) &
            (F.col("TR.LineTypeID") == F.col("TD.LineTypeID")) &
            (ns(F.col("TR.QuickLinkID"), "0") == ns(F.col("TD.QuickLinkID"), "0")) &
            (F.col("TR.AdjustmentTypeID") == F.col("TD.AdjustmentTypeID")) &
            (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("TD.Tag"), F.lit(""))),
            "inner"
        ).join(
            max_alloc_type_df.alias("M"),
            (F.col("TR.EntityID") == F.col("M.EntityID")) &
            (ns(F.col("TR.TrackingKey")) == ns(F.col("M.TrackingKey"))) &
            (F.col("TR.LineID") == F.col("M.LineID")) &
            (F.col("TR.LineTypeID") == F.col("M.LineTypeID")) &
            (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("M.Tag"), F.lit(""))),
            "inner"
        ).filter(
            (F.col("TR.LineTypeID").isin(k1_lt, ubti_lt, passive_lt, box_jkl_lt)) &
            (F.col("TD.DiffAmount") != 0)
        ).select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.col("TR.EntityID"),
            F.col("TR.ShareClass"),
            F.lit(rounding_pn).alias("PartnerNumber"),
            F.col("TR.LineID"),
            F.col("TR.LineTypeID"),
            F.coalesce(F.col("TD.DiffAmount"), F.lit(0.0)).alias("Amount"),
            F.col("TR.ParentEntityID"),
            _tracking_key_expr("TR.TrackingKey", "TR.EntityID").alias("TrackingKey"),
            F.col("TR.SuperParentEntityID"),
            F.coalesce(F.col("TR.Tag"), F.lit("")).alias("Tag"),
            F.col("M.AllocationType").alias("AllocationType"),
            F.col("TR.OriginalParentEntityID"),
            F.col("TR.QuickLinkID"),
        )

        k1_summary_df = k1_summary_df.unionByName(missed_k1)

        # Missed AdjustmentSummary
        missed_adj = rounding_missed.alias("TR").join(
            rounded_diff_df.alias("TD"),
            (F.col("TR.EntityID") == F.col("TD.EntityID")) &
            (F.col("TR.LineID") == F.col("TD.LineID")) &
            (ns(F.col("TR.TrackingKey")) == ns(F.col("TD.TrackingKey"))) &
            (F.col("TR.LineTypeID") == F.col("TD.LineTypeID")) &
            (F.col("TR.AdjustmentTypeID") == F.col("TD.AdjustmentTypeID")),
            "inner"
        ).filter(
            (F.col("TR.LineTypeID") == book_k1_adj_lt) &
            (F.col("TD.DiffAmount") != 0)
        ).select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.col("TR.EntityID"),
            F.col("TR.ShareClass"),
            F.lit(rounding_pn).alias("PartnerNumber"),
            F.col("TR.LineID"),
            F.coalesce(F.col("TD.DiffAmount"), F.lit(0.0)).alias("Amount"),
            F.col("TR.ParentEntityID"),
            F.col("TR.LineTypeID"),
            F.col("TR.AdjustmentTypeID"),
            _tracking_key_expr("TR.TrackingKey", "TR.EntityID").alias("TrackingKey"),
            F.col("TR.SuperParentEntityID"),
        )

        adjustment_summary_df = adjustment_summary_df.unionByName(missed_adj)

    log_timing("apply_rounding_highest_percent", t0)
    return k1_summary_df, adjustment_summary_df


# ---------------------------------------------------------------------------
# Section 18: apply_rounding_highest_amount
# SQL lines: 1011-1110
# ---------------------------------------------------------------------------
def apply_rounding_highest_amount(spark, cfg, temp_alloc_output_df, rounded_diff_df,
                                   max_alloc_type_df, partner_snapshot_df):
    """Apply rounding by plugging to the partner with the highest allocation amount."""
    log_section("apply_rounding_highest_amount")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    ubti_lt = cfg["ubti_line_type_id"]
    passive_lt = cfg["passive_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]
    book_k1_adj_lt = cfg["book_k1_adjustment_line_type_id"]

    # HighestAmount
    highest_amount = temp_alloc_output_df.groupBy(
        "EntityID", "ParentEntityID", "LineID", "LineTypeID",
        "TrackingKey", "SuperParentEntityID",
        F.coalesce(F.col("Tag"), F.lit("")).alias("Tag"),
    ).agg(
        F.max(F.abs(F.coalesce(F.col("Amount"), F.lit(0.0)))).alias("MaxAmount")
    ).filter(F.col("MaxAmount") != 0)

    # HighestAmountPartner (non-BookK1Adj)
    w_non_adj = Window.partitionBy(
        "TR.EntityID", "TR.ParentEntityID", "TR.LineID", "TR.LineTypeID",
        "TR.AdjustmentTypeID", "TR.TrackingKey", "TR.SuperParentEntityID", "TR.Tag"
    ).orderBy(F.col("PS.Name1").asc(), F.col("PS.Name2").asc(), F.col("PS.Name3").asc())

    ha_join = temp_alloc_output_df.alias("TR").join(
        highest_amount.alias("TR1"),
        (F.col("TR.LineID") == F.col("TR1.LineID")) &
        (F.col("TR.EntityID") == F.col("TR1.EntityID")) &
        (F.abs(F.col("TR.Amount")) == F.col("TR1.MaxAmount")) &
        (F.col("TR.LineTypeID") == F.col("TR1.LineTypeID")) &
        (F.col("TR.LineTypeID") != book_k1_adj_lt) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("TR1.TrackingKey"))) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.col("TR1.Tag")),
        "inner"
    ).join(
        partner_snapshot_df.alias("PS"),
        (F.col("TR.PartnerNumber") == F.col("PS.PartnerNumber")) &
        (ns(F.col("TR.ShareClass")) == ns(F.col("PS.ShareClass"))),
        "inner"
    ).withColumn("RowNum", F.row_number().over(w_non_adj))

    highest_amount_partner = ha_join.filter(F.col("RowNum") == 1).select(
        F.col("TR.EntityID").alias("EntityID"),
        F.col("TR.ParentEntityID").alias("ParentEntityID"),
        F.col("TR.LineID").alias("LineID"),
        F.col("TR.LineTypeID").alias("LineTypeID"),
        F.col("TR.AdjustmentTypeID").alias("AdjustmentTypeID"),
        F.col("TR.PartnerNumber").alias("PartnerNumber"),
        F.col("TR.ShareClass").alias("ShareClass"),
        F.col("TR.TrackingKey").alias("TrackingKey"),
        F.col("TR.SuperParentEntityID").alias("SuperParentEntityID"),
        F.coalesce(F.col("TR.Tag"), F.lit("")).alias("Tag"),
    )

    tracking_key_expr = (
        F.when(F.col("TR.TrackingKey").isNull(), F.col("TR.EntityID").cast("string"))
        .otherwise(F.concat(F.col("TR.TrackingKey"), F.lit("~"), F.lit(str(entity_id))))
    )

    # K1Summary
    k1_summary_df = temp_alloc_output_df.alias("TR").join(
        rounded_diff_df.alias("TD"),
        (F.col("TR.LineID") == F.col("TD.LineID")) &
        (F.col("TR.LineTypeID") == F.col("TD.LineTypeID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("TD.TrackingKey"))) &
        (F.col("TR.AdjustmentTypeID") == F.col("TD.AdjustmentTypeID")) &
        (F.coalesce(F.col("TR.ParentEntityID"), F.lit(0)) == F.coalesce(F.col("TD.ParentEntityID"), F.lit(0))) &
        (F.col("TR.EntityID") == F.col("TD.EntityID")) &
        (F.col("TR.SuperParentEntityID") == F.col("TD.SuperParentEntityID")) &
        (ns(F.col("TR.QuickLinkID"), "0") == ns(F.col("TD.QuickLinkID"), "0")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("TD.Tag"), F.lit(""))),
        "left"
    ).join(
        highest_amount_partner.alias("HP"),
        (F.col("TR.LineID") == F.col("HP.LineID")) &
        (F.col("TR.EntityID") == F.col("HP.EntityID")) &
        (F.col("TR.LineTypeID") == F.col("HP.LineTypeID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("HP.TrackingKey"))) &
        (F.col("TR.AdjustmentTypeID") == F.col("HP.AdjustmentTypeID")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("HP.Tag"), F.lit(""))),
        "left"
    ).join(
        max_alloc_type_df.alias("M"),
        (F.col("TR.EntityID") == F.col("M.EntityID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("M.TrackingKey"))) &
        (F.col("TR.LineID") == F.col("M.LineID")) &
        (F.col("TR.LineTypeID") == F.col("M.LineTypeID")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("M.Tag"), F.lit(""))) &
        (ns(F.col("TR.QuickLinkID"), "0") == ns(F.col("M.QuickLinkID"), "0")),
        "inner"
    ).filter(
        F.col("TR.LineTypeID").isin(k1_lt, ubti_lt, passive_lt, box_jkl_lt)
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        F.col("TR.LineTypeID"),
        F.when(
            F.coalesce(F.col("TD.DiffAmount"), F.lit(0)) != 0,
            F.when(
                (F.col("TR.PartnerNumber") == F.col("HP.PartnerNumber")) &
                (ns(F.col("TR.ShareClass")) == ns(F.col("HP.ShareClass"))),
                sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0) + F.coalesce(F.col("TD.DiffAmount"), F.lit(0))
            ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0))
        ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0)).alias("Amount"),
        F.col("TR.ParentEntityID"),
        tracking_key_expr.alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
        F.coalesce(F.col("TR.Tag"), F.lit("")).alias("Tag"),
        F.col("M.AllocationType").alias("AllocationType"),
        F.col("TR.OriginalParentEntityID"),
        F.col("TR.QuickLinkID"),
    )

    # HighestAmountPartner for BookK1Adj
    w_adj = Window.partitionBy(
        "TR.EntityID", "TR.ParentEntityID", "TR.LineID", "TR.LineTypeID",
        "TR.AdjustmentTypeID", "TR.TrackingKey", "TR.Tag"
    ).orderBy(F.col("PS.Name1").asc(), F.col("PS.Name2").asc(), F.col("PS.Name3").asc())

    ha_adj_join = temp_alloc_output_df.alias("TR").join(
        highest_amount.alias("TR1"),
        (F.col("TR.LineID") == F.col("TR1.LineID")) &
        (F.col("TR.EntityID") == F.col("TR1.EntityID")) &
        (F.abs(F.col("TR.Amount")) == F.col("TR1.MaxAmount")) &
        (F.col("TR.LineTypeID") == F.col("TR1.LineTypeID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("TR1.TrackingKey"))) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.col("TR1.Tag")) &
        (F.col("TR.LineTypeID") == book_k1_adj_lt) &
        (F.col("TR1.LineTypeID") == book_k1_adj_lt),
        "inner"
    ).join(
        partner_snapshot_df.alias("PS"),
        (F.col("TR.PartnerNumber") == F.col("PS.PartnerNumber")) &
        (ns(F.col("TR.ShareClass")) == ns(F.col("PS.ShareClass"))),
        "inner"
    ).withColumn("RowNum", F.row_number().over(w_adj))

    ha_partner_adj = ha_adj_join.filter(F.col("RowNum") == 1).select(
        F.col("TR.EntityID").alias("EntityID"),
        F.col("TR.ParentEntityID").alias("ParentEntityID"),
        F.col("TR.LineID").alias("LineID"),
        F.col("TR.LineTypeID").alias("LineTypeID"),
        F.col("TR.AdjustmentTypeID").alias("AdjustmentTypeID"),
        F.col("TR.PartnerNumber").alias("PartnerNumber"),
        F.col("TR.ShareClass").alias("ShareClass"),
        F.col("TR.TrackingKey").alias("TrackingKey"),
        F.coalesce(F.col("TR.Tag"), F.lit("")).alias("Tag"),
    )

    # AdjustmentSummary (BookK1Adj)
    adjustment_summary_df = temp_alloc_output_df.alias("TR").join(
        rounded_diff_df.alias("TD"),
        (F.col("TR.LineID") == F.col("TD.LineID")) &
        (F.col("TR.LineTypeID") == F.col("TD.LineTypeID")) &
        (F.col("TR.AdjustmentTypeID") == F.col("TD.AdjustmentTypeID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("TD.TrackingKey"))) &
        (F.col("TR.EntityID") == F.col("TD.EntityID")),
        "left"
    ).join(
        ha_partner_adj.alias("HP"),
        (F.col("TR.LineID") == F.col("HP.LineID")) &
        (F.col("TR.EntityID") == F.col("HP.EntityID")) &
        (F.col("TR.LineTypeID") == F.col("HP.LineTypeID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("HP.TrackingKey"))) &
        (F.col("TR.AdjustmentTypeID") == F.col("HP.AdjustmentTypeID")),
        "left"
    ).filter(
        F.col("TR.LineTypeID") == book_k1_adj_lt
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        F.when(
            F.coalesce(F.col("TD.DiffAmount"), F.lit(0)) != 0,
            F.when(
                (F.col("TR.PartnerNumber") == F.col("HP.PartnerNumber")) &
                (ns(F.col("TR.ShareClass")) == ns(F.col("HP.ShareClass"))),
                sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0) + F.coalesce(F.col("TD.DiffAmount"), F.lit(0))
            ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0))
        ).otherwise(sql_round(F.coalesce(F.col("TR.Amount"), F.lit(0.0)), 0)).alias("Amount"),
        F.col("TR.ParentEntityID"),
        F.col("TR.LineTypeID"),
        F.col("TR.AdjustmentTypeID"),
        F.when(F.col("TR.TrackingKey").isNull(), F.col("TR.EntityID").cast("string"))
        .otherwise(F.concat(F.col("TR.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))).alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
    )

    log_timing("apply_rounding_highest_amount", t0)
    return k1_summary_df, adjustment_summary_df


# ---------------------------------------------------------------------------
# Section 19: apply_rounding_none
# SQL lines: 1111-1135
# ---------------------------------------------------------------------------
def apply_rounding_none(spark, cfg, temp_alloc_output_df, max_alloc_type_df):
    """No rounding — pass through amounts directly."""
    log_section("apply_rounding_none")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    book_k1_adj_lt = cfg["book_k1_adjustment_line_type_id"]

    tracking_key_expr = (
        F.when(F.col("TR.TrackingKey").isNull(), F.col("TR.EntityID").cast("string"))
        .otherwise(F.concat(F.col("TR.TrackingKey"), F.lit("~"), F.lit(str(entity_id))))
    )

    k1_summary_df = temp_alloc_output_df.alias("TR").join(
        max_alloc_type_df.alias("M"),
        (F.col("TR.EntityID") == F.col("M.EntityID")) &
        (ns(F.col("TR.TrackingKey")) == ns(F.col("M.TrackingKey"))) &
        (F.col("TR.LineID") == F.col("M.LineID")) &
        (F.col("TR.LineTypeID") == F.col("M.LineTypeID")) &
        (ns(F.col("TR.QuickLinkID"), "0") == ns(F.col("M.QuickLinkID"), "0")) &
        (F.coalesce(F.col("TR.Tag"), F.lit("")) == F.coalesce(F.col("M.Tag"), F.lit(""))),
        "inner"
    ).filter(
        F.col("TR.LineTypeID") == k1_lt
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        F.col("TR.LineTypeID"),
        F.coalesce(F.col("TR.Amount"), F.lit(0.0)).alias("Amount"),
        F.col("TR.ParentEntityID"),
        tracking_key_expr.alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
        F.coalesce(F.col("TR.Tag"), F.lit("")).alias("Tag"),
        F.col("M.AllocationType").alias("AllocationType"),
        F.col("TR.OriginalParentEntityID"),
        F.col("TR.QuickLinkID"),
    )

    adjustment_summary_df = temp_alloc_output_df.alias("TR").filter(
        F.col("TR.LineTypeID") == book_k1_adj_lt
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.col("TR.EntityID"),
        F.col("TR.ShareClass"),
        F.col("TR.PartnerNumber"),
        F.col("TR.LineID"),
        F.coalesce(F.col("TR.Amount"), F.lit(0.0)).alias("Amount"),
        F.col("TR.ParentEntityID"),
        F.col("TR.LineTypeID"),
        F.col("TR.AdjustmentTypeID"),
        F.when(F.col("TR.TrackingKey").isNull(), F.col("TR.EntityID").cast("string"))
        .otherwise(F.concat(F.col("TR.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))).alias("TrackingKey"),
        F.col("TR.SuperParentEntityID"),
    )

    log_timing("apply_rounding_none", t0)
    return k1_summary_df, adjustment_summary_df
