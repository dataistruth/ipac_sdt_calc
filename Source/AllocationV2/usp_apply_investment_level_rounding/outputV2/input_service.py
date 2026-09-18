"""
Allocation input building services (Sections 7-8).

- build_allocation_input: AllocationInput from LookThroughAllocationInput + BoxJKL.
- build_ubti_passive_input: UBTI and Passive Income with currency conversion.
"""

import pyspark.sql.functions as F
import time

from Common_V2.core.helpers import tbl, ns0, sql_round
from Common_V2.core.observability import log_section, log_timing


# ---------------------------------------------------------------------------
# Section 7: build_allocation_input
# SQL lines: 358-418
# ---------------------------------------------------------------------------
def build_allocation_input(spark, cfg, lookthrough_input_df, not_rounded_lines_df):
    """Build #AllocationInput from LookThroughAllocationInput with conditional NotRoundedLines exclusion + BoxJKL."""
    log_section("build_allocation_input")
    t0 = time.time()

    k1_lt = cfg["k1_line_type_id"]
    book_k1_adj_lt = cfg["book_k1_adjustment_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]
    entity_id = cfg["entity_id"]
    book_k1_adjustment_enabled = cfg["book_k1_adjustment_enabled"]

    # Filter to K1 + BookK1Adj line types
    k1_input = lookthrough_input_df.filter(
        F.col("LineTypeID").isin(k1_lt, book_k1_adj_lt)
    )

    # If BookK1AdjustmentEnabled, exclude NotRoundedLines via anti-join
    if book_k1_adjustment_enabled and not_rounded_lines_df is not None:
        k1_input = k1_input.join(
            not_rounded_lines_df.select("EntityID", "LineID", "LineTypeID"),
            on=["EntityID", "LineID", "LineTypeID"],
            how="left_anti"
        )

    alloc_input = k1_input.select(
        "EntityID", "ParentEntityID", "LineID", "LineTypeID", "Amount",
        "AdjustmentTypeID", "TrackingKey", "SuperParentEntityID", "Tag",
    ).withColumn("QuickLinkID", F.lit(None).cast("int"))

    # BoxJKL line type
    box_jkl_input = lookthrough_input_df.filter(
        F.col("LineTypeID") == box_jkl_lt
    ).select(
        F.lit(entity_id).alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("LineID"), F.col("LineTypeID"), F.col("Amount"),
        F.lit(None).cast("int").alias("AdjustmentTypeID"),
        F.lit(None).cast("string").alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.col("Tag"),
        F.lit(None).cast("int").alias("QuickLinkID"),
    )

    allocation_input_df = alloc_input.unionByName(box_jkl_input)

    log_timing("build_allocation_input", t0)
    return allocation_input_df


# ---------------------------------------------------------------------------
# Section 8: build_ubti_passive_input
# SQL lines: 419-472
# ---------------------------------------------------------------------------
def build_ubti_passive_input(spark, cfg, allocation_input_df):
    """Load UBTI (K1UBTI_Snapshot + lower tier) and Passive Income with currency conversion."""
    log_section("build_ubti_passive_input")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    ubti_lt = cfg["ubti_line_type_id"]
    passive_lt = cfg["passive_line_type_id"]
    is_inv_level = cfg["is_investment_level_rounding"]
    fx_txn_id = cfg["foreign_currency_rate_transaction_id"]

    ubti_snap = tbl(spark, "K1UBTI_Snapshot", cfg).filter(
        (F.col("ClientID") == client_id)
        & (F.col("TaxPeriodID") == tax_period_id)
        & (F.coalesce(F.col("Total"), F.lit(0)) != 0)
    )
    passive_snap = tbl(spark, "PassiveIncomeInput_Snapshot", cfg).filter(
        (F.col("ClientID") == client_id)
        & (F.col("TaxPeriodID") == tax_period_id)
        & (F.coalesce(F.col("Amount"), F.lit(0)) != 0)
    )

    # Common lookups
    k1_wf_df = tbl(spark, "AllocationInputWorkflow", cfg).filter(
        F.col("RunID") == run_id
    ).select(F.col("EntityID"), F.col("K1WorkflowID").alias("WorkflowID"))

    lt_funds_df = tbl(spark, "LowerTierFunds", cfg).filter(
        F.col("RunID") == run_id
    ).select("EntityID", "PartnerNumber", F.col("LTRunID").alias("RunID"))

    entity_df = tbl(spark, "Entity", cfg).select("EntityID", "CurrencyCode")
    fx_avg_df = tbl(spark, "ForeignCurrencyAverageRate", cfg).filter(
        (F.col("ClientID") == client_id) & (F.col("TransactionID") == fx_txn_id)
    ).select("CurrencyCode", "AverageRate")
    k1_line_item_df = tbl(spark, "K1LineItem", cfg).filter(
        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
    ).select("LineID", "TransactionDate")
    fx_rate_df = tbl(spark, "ForeignCurrencyRate", cfg).filter(
        (F.col("ClientID") == client_id) & (F.col("TransactionID") == fx_txn_id)
    ).select("CurrencyCode", F.col("Range").alias("TransactionDate"), "Rate")

    new_rows = []

    # ========== UBTI from K1UBTI_Snapshot ==========
    ubti_joined = ubti_snap.alias("UBTI").join(
        F.broadcast(k1_wf_df).alias("KW"),
        (F.col("UBTI.WorkflowID") == F.col("KW.WorkflowID")) &
        (F.col("UBTI.ClientID") == F.lit(client_id)),
        "inner"
    ).join(
        entity_df.alias("E"),
        F.col("E.EntityID") == F.col("KW.EntityID"),
        "inner"
    ).join(
        fx_avg_df.alias("R"),
        F.col("R.CurrencyCode") == F.col("E.CurrencyCode"),
        "left"
    ).join(
        k1_line_item_df.alias("K1"),
        F.col("K1.LineID") == F.col("UBTI.LineID"),
        "left"
    ).join(
        fx_rate_df.alias("CR"),
        (F.col("CR.CurrencyCode") == F.col("E.CurrencyCode")) &
        (F.col("CR.TransactionDate") == F.col("K1.TransactionDate")),
        "left"
    )

    rate_expr = F.coalesce(F.col("CR.Rate"), F.col("R.AverageRate"), F.lit(1.0))
    entity_expr = (
        F.when(F.lit(is_inv_level == "C"), F.col("KW.EntityID"))
        .otherwise(F.lit(entity_id))
    )
    tracking_key_expr = (
        F.when(F.lit(is_inv_level == "C"), F.col("KW.EntityID").cast("string"))
        .otherwise(F.lit(None).cast("string"))
    )
    quicklink_expr = (
        F.when(F.lower(F.col("UBTI.UBTIType")) == "qualified", F.lit(1))
        .when(F.lower(F.col("UBTI.UBTIType")) == "non-qualified", F.lit(2))
        .otherwise(F.lit(None).cast("int"))
    )

    ubti_rows = ubti_joined.select(
        entity_expr.alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("UBTI.LineID").alias("LineID"),
        F.lit(ubti_lt).alias("LineTypeID"),
        sql_round(ns0(F.col("UBTI.Total")) / rate_expr, 0).alias("Amount"),
        F.lit(None).cast("int").alias("AdjustmentTypeID"),
        tracking_key_expr.alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.lit("").alias("Tag"),
        quicklink_expr.alias("QuickLinkID"),
    )
    new_rows.append(ubti_rows)

    # ========== UBTI from UBTILookThroughAllocationSummary (lower tier) ==========
    ubti_lt_summary = tbl(
        spark, "UBTILookThroughAllocationSummary", cfg
    ).filter(
        (F.col("ClientID") == client_id)
        & (F.col("TaxPeriodID") == tax_period_id)
        & (F.coalesce(F.col("Amount"), F.lit(0)) != 0)
    ).alias("UBTI")
    ubti_lt_joined = ubti_lt_summary.join(
        F.broadcast(lt_funds_df).alias("LT"),
        (F.col("UBTI.RunID") == F.col("LT.RunID")) &
        (F.col("LT.PartnerNumber") == F.col("UBTI.PartnerNumber")),
        "inner"
    )

    ubti_lt_entity_expr = (
        F.when(F.lit(is_inv_level == "C"), F.col("UBTI.EntityID"))
        .otherwise(F.lit(entity_id))
    )
    ubti_lt_tracking_expr = (
        F.when(F.lit(is_inv_level == "C"), F.col("UBTI.TrackingKey"))
        .otherwise(F.lit(None).cast("string"))
    )
    ubti_lt_quicklink_expr = (
        F.when(F.lower(F.col("UBTI.UBTIType")) == "qualified", F.lit(1))
        .when(F.lower(F.col("UBTI.UBTIType")) == "non-qualified", F.lit(2))
        .otherwise(F.lit(None).cast("int"))
    )

    ubti_lt_rows = ubti_lt_joined.select(
        ubti_lt_entity_expr.alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("UBTI.LineID").alias("LineID"),
        F.lit(ubti_lt).alias("LineTypeID"),
        sql_round(ns0(F.col("UBTI.Amount")), 0).alias("Amount"),
        F.lit(None).cast("int").alias("AdjustmentTypeID"),
        ubti_lt_tracking_expr.alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.coalesce(F.col("UBTI.Tag"), F.lit("")).alias("Tag"),
        ubti_lt_quicklink_expr.alias("QuickLinkID"),
    )
    new_rows.append(ubti_lt_rows)

    # ========== Passive from PassiveIncomeInput_Snapshot ==========
    passive_joined = passive_snap.alias("P").join(
        F.broadcast(k1_wf_df).alias("KW"),
        (F.col("P.WorkflowID") == F.col("KW.WorkflowID")) &
        (F.col("P.ClientID") == F.lit(client_id)),
        "inner"
    ).join(
        entity_df.alias("E"),
        F.col("E.EntityID") == F.col("KW.EntityID"),
        "inner"
    ).join(
        fx_avg_df.alias("R"),
        F.col("R.CurrencyCode") == F.col("E.CurrencyCode"),
        "left"
    ).join(
        k1_line_item_df.alias("K1"),
        F.col("K1.LineID") == F.col("P.LineID"),
        "left"
    ).join(
        fx_rate_df.alias("CR"),
        (F.col("CR.CurrencyCode") == F.col("E.CurrencyCode")) &
        (F.col("CR.TransactionDate") == F.col("K1.TransactionDate")),
        "left"
    )

    p_rate_expr = F.coalesce(F.col("CR.Rate"), F.col("R.AverageRate"), F.lit(1.0))

    passive_rows = passive_joined.select(
        F.lit(entity_id).alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("P.LineID").alias("LineID"),
        F.lit(passive_lt).alias("LineTypeID"),
        sql_round(ns0(F.col("P.Amount")) / p_rate_expr, 0).alias("Amount"),
        F.lit(None).cast("int").alias("AdjustmentTypeID"),
        F.lit(None).cast("string").alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.lit("").alias("Tag"),
        F.lit(None).cast("int").alias("QuickLinkID"),
    )
    new_rows.append(passive_rows)

    # ========== Passive from PassiveIncomeAllocationSummary (lower tier) ==========
    passive_alloc_summary = tbl(
        spark, "PassiveIncomeAllocationSummary", cfg
    ).filter(
        (F.col("ClientID") == client_id)
        & (F.col("TaxPeriodID") == tax_period_id)
        & (F.coalesce(F.col("Amount"), F.lit(0)) != 0)
    ).alias("P")
    passive_lt_joined = passive_alloc_summary.join(
        F.broadcast(lt_funds_df).alias("LT"),
        (F.col("P.RunID") == F.col("LT.RunID")) &
        (F.col("LT.PartnerNumber") == F.col("P.PartnerNumber")),
        "inner"
    )

    passive_lt_rows = passive_lt_joined.select(
        F.lit(entity_id).alias("EntityID"),
        F.lit(None).cast("int").alias("ParentEntityID"),
        F.col("P.LineID").alias("LineID"),
        F.lit(passive_lt).alias("LineTypeID"),
        sql_round(ns0(F.col("P.Amount")), 0).alias("Amount"),
        F.lit(None).cast("int").alias("AdjustmentTypeID"),
        F.lit(None).cast("string").alias("TrackingKey"),
        F.lit(None).cast("int").alias("SuperParentEntityID"),
        F.lit("").alias("Tag"),
        F.lit(None).cast("int").alias("QuickLinkID"),
    )
    new_rows.append(passive_lt_rows)

    # Union all new rows with existing allocation_input_df
    combined = allocation_input_df
    for df in new_rows:
        combined = combined.unionByName(df, allowMissingColumns=True)

    log_timing("build_ubti_passive_input", t0)
    return combined
