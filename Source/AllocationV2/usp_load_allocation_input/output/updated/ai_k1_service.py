"""
ai_k1_service.py

K1 input, adjustment, sidepocket, special allocation, GAAP-to-tax,
at-risk, and M1 periodic builders for uspLoadAllocationInput.

SQL lines: 2700-3700 (K1/adjustments/M1/GAAP/at-risk)
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import table_prefix, read_table, log_section, log_timing

logger = logging.getLogger(__name__)


def build_k1_and_related_inputs(
    spark: SparkSession,
    cfg: dict,
    workflows: dict,
) -> DataFrame:
    """Build K1, adjustment, M1, GAAP-to-tax, at-risk, sidepocket, and special allocation inputs.

    Combines all non-form allocation input sources into one DataFrame
    matching the AllocationInput schema.

    SQL lines: 2700-3900
    Returns: DataFrame with AllocationInput schema
    """
    log_section("build_k1_and_related_inputs")
    t0 = time.time()
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    phase_id = cfg["phase_id"]
    fx_tid = cfg.get("fx_rate_transaction_id") or 0
    is_tracking = cfg.get("is_tracking_key", "C") == "C"

    gaap_line_type = cfg.get("gaap_to_tax_line_type_id")
    at_risk_line_type = cfg.get("at_risk_line_type_id")

    # Load shared temp views as DataFrames
    kl_df = spark.table("_k1_line_item")
    map_df = spark.table("_map_k1_line_type")
    aiw_df = spark.table("_aiw")
    entity_view = spark.table("_entity")
    k1p_df = spark.table("_k1_package")
    fx_rate_df = spark.table("_fx_rate")
    fx_avg_df = spark.table("_fx_avg_rate")
    reclass_df = spark.table("_reclass_data")

    # Catalog tables
    k1_snapshot = (
        read_table(spark, "K1Input_Snapshot", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
    )

    parts = []

    # ─── K1 Input ─────────────────────────────────────────────────────────

    # ─── Adjustment / Book-K1 Input ───────────────────────────────────────

    # ─── At-Risk Input ────────────────────────────────────────────────────
    is_pfic_cfc_qfc = cfg.get("is_pfic_cfc_qfc_entity", False)
    is_fb_checked = cfg.get("is_foreign_blocker_footnotes_flowup_checked", False)
    if at_risk_line_type and (not is_pfic_cfc_qfc or not is_fb_checked):
        ar_snapshot = (
            read_table(spark, "AtRiskInput_Snapshot", cfg)
            .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
        )
        ar_package = (
            read_table(spark, "AtRiskPackage", cfg)
            .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
        )

        ar_base = (
            ar_snapshot.alias("AR")
            .join(kl_df.alias("KL"),
                  (F.col("AR.LineID") == F.col("KL.LineID")) &
                  (F.upper(F.col("KL.LineDataType")) == "NUMBER") & (F.col("KL.IsActive") == True),
                  "inner")
            .join(map_df.alias("M"),
                  (F.col("KL.LineID") == F.col("M.K1LineItemID")) & (F.col("M.LineTypeID") == at_risk_line_type),
                  "inner")
            .join(aiw_df.alias("AIW"), F.col("AR.WorkflowID") == F.col("AIW.ImportAtRiskWorkflowID"), "inner")
            .join(entity_view.alias("E"), F.col("E.EntityID") == F.col("AIW.EntityID"), "inner")
            .join(ar_package.alias("P"),
                  F.col("P.AtRiskID") == F.col("AR.AtRiskID"),
                  "inner")
            .join(k1p_df.alias("K1P"), F.col("K1P.K1PackageID") == F.col("P.K1PackageID"), "inner")
            .join(fx_avg_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .filter(F.coalesce(F.col("AR.Amount"), F.lit(0)) != 0)
            .select(
                F.lit(None).cast("int").alias("SuperParentEntityID"),
                F.col("K1P.LowerTierEntityID").alias("EntityID"),
                F.lit(at_risk_line_type).alias("LineTypeID"),
                F.col("AR.LineID"),
                F.round(F.col("AR.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0).alias("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                # SQL (SP L1834): QuicklinkID = AR.AtRiskID for the At-Risk snapshot insert.
                # Was hardcoded NULL, which broke the AllocationInput key for LineType 17.
                F.col("AR.AtRiskID").cast("int").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                F.lit(0).alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.when(F.lit(is_tracking), F.col("AIW.EntityID").cast("string")).otherwise(F.lit(None)).alias("TrackingKey"),
                F.lit(None).cast("int").alias("SchID"),
                F.lit(None).cast("int").alias("OriginalParentEntityID"),
            )
        )
        parts.append(ar_base)

        # At-Risk flowup
        ar_flowup = (
            reclass_df.alias("RFA")
            .join(kl_df.alias("FL"),
                  (F.col("RFA.LineID") == F.col("FL.LineID")) & (F.upper(F.col("FL.LineDataType")) == "NUMBER"),
                  "inner")
            .join(map_df.alias("M"),
                  (F.col("FL.LineID") == F.col("M.K1LineItemID")) & (F.col("M.LineTypeID") == at_risk_line_type),
                  "inner")
            .join(ar_package.alias("P"), F.col("P.AtRiskID") == F.col("RFA.FootnoteID"), "inner")
            .join(k1p_df.alias("K"), F.col("K.K1PackageID") == F.col("P.K1PackageID"), "inner")
            .filter(F.col("RFA.LineTypeID") == at_risk_line_type)
            .groupBy(
                F.col("RFA.LineID"), F.col("RFA.FootnoteID"),
                F.col("K.LowerTierEntityID"),
                F.col("RFA.ParentEntityID").alias("_parent_raw"),
                F.col("RFA.SourceEntityID"),
                F.col("RFA.LTEntityID"),
                F.coalesce(F.col("RFA.TrackingKey"), F.lit("")).alias("_tk"),
                F.coalesce(F.col("RFA.OriginalParentEntityID"), F.lit(entity_id)).alias("_orig_parent"),
            )
            .agg(F.sum("RFA.FlowupAmount").alias("Amount"))
            .select(
                F.col("RFA.LTEntityID").alias("SuperParentEntityID"),
                F.col("K.LowerTierEntityID").alias("EntityID"),
                F.lit(at_risk_line_type).alias("LineTypeID"),
                F.col("RFA.LineID"),
                F.col("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.col("RFA.FootnoteID").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                F.when(
                    (F.coalesce(F.col("_parent_raw"), F.lit(0)) == 0)
                    | (F.coalesce(F.col("_parent_raw"), F.lit(0)) == F.col("RFA.SourceEntityID")),
                    F.when(F.col("RFA.LTEntityID") == F.col("K.LowerTierEntityID"), F.lit(0))
                    .otherwise(F.col("RFA.LTEntityID"))
                ).otherwise(F.coalesce(F.col("_parent_raw"), F.lit(0))).alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.col("_tk").alias("TrackingKey"),
                F.lit(None).cast("int").alias("SchID"),
                F.col("_orig_parent").alias("OriginalParentEntityID"),
            )
        )
        parts.append(ar_flowup)

    # ─── GAAP-to-Tax Input ────────────────────────────────────────────────
    if gaap_line_type:
        gaap_df = (
            read_table(spark, "GAAPToTax", cfg)
            .filter(
                (F.col("EntityID") == entity_id) &
                (F.col("RunID") == run_id) &
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            )
        )
        k1_wf_entities = (
            aiw_df.filter(F.coalesce(F.col("K1WorkflowID"), F.lit(0)) != 0)
            .select(F.col("EntityID"))
        )
        if cfg.get("is_k1_input_international"):
            k1_wf_entities = k1_wf_entities.unionByName(
                aiw_df.filter(F.coalesce(F.col("K1InternationalWorkflowID"), F.lit(0)) != 0)
                .select(F.col("EntityID"))
            )
        gaap_base = (
            gaap_df.alias("GP")
            .join(k1_wf_entities.alias("KW"), F.col("GP.EntityID") == F.col("KW.EntityID"), "inner")
            # SQL #K1LineItemsWithRates (SP L942-964) joins K1LineItem ON LineDataType='Number'
            # only — so the GAAP↔K1 join must filter to Number lines, else GAAP rows for
            # non-Number K1 lines leak in (the ConversionRate source is Number-scoped).
            .join(kl_df.alias("KL"),
                  (F.col("GP.LineID") == F.col("KL.LineID")) & (F.upper(F.col("KL.LineDataType")) == "NUMBER"),
                  "inner")
            .join(entity_view.alias("E"), F.col("E.EntityID") == entity_id, "inner")
            .join(fx_avg_df.alias("F1"), F.col("F1.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .join(fx_rate_df.alias("F2"),
                  (F.col("F2.CurrencyCode") == F.col("E.CurrencyCode")) &
                  (F.col("F2.Range") == F.col("KL.TransactionDate")),
                  "left")
            .select(
                F.lit(None).cast("int").alias("SuperParentEntityID"),
                F.lit(entity_id).alias("EntityID"),
                F.lit(gaap_line_type).alias("LineTypeID"),
                F.col("GP.LineID"),
                F.round(F.col("GP.Amount") / F.coalesce(F.coalesce(F.col("F2.Rate"), F.col("F1.AverageRate")), F.lit(1)), 0).alias("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.col("GP.GaapToTaxLineID").alias("QuicklinkID"),
                F.col("GP.CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                F.lit(0).alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.lit(None).cast("string").alias("TrackingKey"),
                F.lit(None).cast("int").alias("SchID"),
                F.lit(None).cast("int").alias("OriginalParentEntityID"),
            )
        )
        parts.append(gaap_base)

    # ─── M1 Periodic Input ────────────────────────────────────────────────

    # ─── Union all parts ──────────────────────────────────────────────────
    if not parts:
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
        empty_schema = StructType([
            StructField("SuperParentEntityID", IntegerType(), True),
            StructField("EntityID", IntegerType(), True),
            StructField("LineTypeID", IntegerType(), True),
            StructField("LineID", IntegerType(), True),
            StructField("Amount", LongType(), True),
            StructField("TransactionName", StringType(), True),
            StructField("TransactionEntityID", IntegerType(), True),
            StructField("QuicklinkID", IntegerType(), True),
            StructField("CategoryID", IntegerType(), True),
            StructField("PeriodID", IntegerType(), True),
            StructField("LineCode", StringType(), True),
            StructField("ParentEntityID", IntegerType(), True),
            StructField("AdjustmentTypeID", IntegerType(), True),
            StructField("Tag", StringType(), True),
            StructField("TrackingKey", StringType(), True),
            StructField("SchID", IntegerType(), True),
            StructField("OriginalParentEntityID", IntegerType(), True),
        ])
        result = spark.createDataFrame([], schema=empty_schema)
    else:
        result = parts[0]
        for p in parts[1:]:
            result = result.unionByName(p, allowMissingColumns=True)

    log_timing("build_k1_and_related_inputs", t0)
    return result
