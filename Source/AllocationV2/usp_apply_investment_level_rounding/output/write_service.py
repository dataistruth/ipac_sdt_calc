"""
Write services (Sections 20-22).

- apply_book_k1_not_rounded_passthrough: Pass not-rounded lines through.
- write_final_summaries: Write K1/UBTI/Passive/BoxJKL/Adjustment summary tables.
- update_is_rounded_flag: Mark lines as rounded.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

from Common_V2.core.helpers import tbl, tbl_name, ns, sql_round, get_logger
from Common_V2.core.checkpoint import checkpoint
from Common_V2.core.observability import log_section, log_timing

logger = get_logger("apply_investment_level_rounding")

import sys
import os
from pathlib import Path

# Add Common_V2 to path for GenericResultStorer
try:
    _source_root = Path(__file__).resolve().parent.parent.parent.parent  # output -> usp_* -> AllocationV2 -> Source
    _common_v2_path = str(_source_root / "Common_V2")
except NameError:
    _common_v2_path = os.path.abspath(os.path.join(os.getcwd(), "Common_V2"))

if _common_v2_path not in sys.path:
    sys.path.insert(0, _common_v2_path)

from core.generic_result_storer import GenericResultStorer


# ---------------------------------------------------------------------------
# Section 20: apply_book_k1_not_rounded_passthrough
# SQL lines: 1136-1155
# ---------------------------------------------------------------------------
def apply_book_k1_not_rounded_passthrough(spark, cfg, lookthrough_output_df, not_rounded_lines_df):
    """If BookK1AdjustmentEnabled, add not-rounded lines to K1Summary without rounding."""
    log_section("apply_book_k1_not_rounded_passthrough")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    ubti_lt = cfg["ubti_line_type_id"]
    passive_lt = cfg["passive_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]

    passthrough_df = lookthrough_output_df.alias("AO").join(
        not_rounded_lines_df.select("LineID").distinct().alias("NR"),
        F.col("AO.LineID") == F.col("NR.LineID"),
        "inner"
    ).filter(
        (F.col("AO.LineTypeID").isin(k1_lt, ubti_lt, passive_lt, box_jkl_lt)) &
        (F.col("AO.EntityID") == entity_id)
    )

    tracking_key_expr = (
        F.when(F.col("AO.TrackingKey").isNull(),
               F.col("AO.EntityID").cast("string"))
        .otherwise(F.concat(F.col("AO.TrackingKey"), F.lit("~"), F.lit(str(entity_id))))
    )

    result = passthrough_df.groupBy(
        F.coalesce(F.col("AO.ShareClass"), F.lit("")).alias("ShareClass"),
        F.col("AO.PartnerNumber"),
        F.col("AO.LineID"),
        F.col("AO.LineTypeID"),
        F.col("AO.ParentEntityID"),
        tracking_key_expr.alias("TrackingKey"),
        F.coalesce(F.col("AO.SuperParentEntityID"), F.lit(0)).alias("SuperParentEntityID"),
        F.coalesce(F.col("AO.Tag"), F.lit("")).alias("Tag"),
        F.col("AO.OriginalParentEntityID"),
    ).agg(
        F.sum(F.coalesce(F.col("AO.Amount"), F.lit(0.0))).alias("Amount")
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.lit(tax_period_id).alias("TaxPeriodID"),
        F.lit(entity_id).alias("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("LineTypeID"),
        F.col("Amount"),
        F.col("ParentEntityID"),
        F.col("TrackingKey"),
        F.col("SuperParentEntityID"),
        F.col("Tag"),
        F.col("OriginalParentEntityID"),
    )

    log_timing("apply_book_k1_not_rounded_passthrough", t0)
    return result


# ---------------------------------------------------------------------------
# Section 21: write_final_summaries
# SQL lines: 1156-1215
# ---------------------------------------------------------------------------
def write_final_summaries(spark, cfg, k1_summary_df, adjustment_summary_df=None):
    """Write final data to K1/UBTI/Passive/BoxJKL/Adjustment summary tables (Delta + Parquet)."""
    log_section("write_final_summaries")
    t0 = time.time()

    def _align_schema(df, table_name, cfg):
        """Cast columns to match target Delta table schema, reorder, and drop extras."""
        full_name = tbl_name(table_name, cfg)
        try:
            # Break join lineage — resolve all columns from output schema only
            df = df.select([F.col(f"`{c}`").alias(c) for c in df.columns])

            target_schema = spark.table(full_name).schema
            target_col_names = [f.name for f in target_schema.fields]
            target_fields = {f.name: f.dataType for f in target_schema.fields}
            src_cols_lower = {c.lower(): c for c in df.columns}
            for col_name in target_col_names:
                src_col = src_cols_lower.get(col_name.lower())
                if src_col is not None:
                    df = df.withColumn(col_name, df[src_col].cast(target_fields[col_name]))
                else:
                    df = df.withColumn(col_name, F.lit(None).cast(target_fields[col_name]))
            df = df.select(target_col_names)
        except Exception as e:
            logger.error(f"_align_schema FAILED for {full_name}: {e}")
            print(f"[ERROR] _align_schema FAILED for {full_name}: {e}")
            raise
        return df

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    k1_lt = cfg["k1_line_type_id"]
    ubti_lt = cfg["ubti_line_type_id"]
    passive_lt = cfg["passive_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]



    # --- K1LookThroughAllocationSummary ---
    k1_final = k1_summary_df.filter(
        (F.col("LineTypeID") == k1_lt) & (F.col("Amount") != 0)
    ).groupBy(
        "RunID", "ShareClass", "EntityID", "PartnerNumber", "LineID", "LineTypeID",
        "ParentEntityID", "TrackingKey", "SuperParentEntityID", "Tag",
        "AllocationType", "OriginalParentEntityID",
    ).agg(
        F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("Amount")
    ).filter(F.col("Amount") != 0)

    k1_write = k1_final.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("int").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        "EntityID", "ShareClass", "PartnerNumber", "LineID",
        "Amount", "ParentEntityID", "TrackingKey", "SuperParentEntityID",
        "Tag", "AllocationType", "OriginalParentEntityID",
    )
    k1_aligned = _align_schema(k1_write, "K1LookThroughAllocationSummary", cfg)

    # --- UBTILookThroughAllocationSummary ---
    ubti_final = k1_summary_df.filter(
        (F.col("LineTypeID") == ubti_lt) & (F.col("Amount") != 0)
    ).groupBy(
        "RunID", "ShareClass", "EntityID", "PartnerNumber", "LineID", "LineTypeID",
        "ParentEntityID", "TrackingKey", "SuperParentEntityID", "Tag",
        "AllocationType", "OriginalParentEntityID", "QuickLinkID",
    ).agg(
        F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("Amount")
    ).filter(F.col("Amount") != 0)

    ubti_write = ubti_final.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        "EntityID", "ShareClass", "PartnerNumber", "LineID",
        "Amount", "TrackingKey", "ParentEntityID", "SuperParentEntityID",
        "Tag", "AllocationType", "OriginalParentEntityID",
        F.when(F.col("QuickLinkID") == 1, F.lit("Qualified"))
         .when(F.col("QuickLinkID") == 2, F.lit("Non-Qualified"))
         .otherwise(F.lit(None).cast("string")).alias("UBTIType"),
    )
    ubti_aligned = _align_schema(ubti_write, "UBTILookThroughAllocationSummary", cfg)

    # --- PassiveIncomeAllocationSummary ---
    # SQL: GROUP BY ShareClass, EntityID, PartnerNumber, LineID (4 keys)
    # SELECT SUM(Amount), SUM(Amount) as FlowupAmount
    passive_final = k1_summary_df.filter(
        (F.col("LineTypeID") == passive_lt) & (F.col("Amount") != 0)
    ).groupBy(
        "ShareClass", "EntityID", "PartnerNumber", "LineID",
    ).agg(
        F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("Amount")
    ).filter(F.col("Amount") != 0)

    passive_write = passive_final.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("int").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        "EntityID", "ShareClass", "PartnerNumber", "LineID",
        "Amount",
        F.col("Amount").alias("FlowupAmount"),
    )
    passive_aligned = _align_schema(passive_write, "PassiveIncomeAllocationSummary", cfg)

    # --- BOXJKLAllocationSummary ---
    # SQL: GROUP BY ShareClass, EntityID, PartnerNumber, LineID (4 keys)
    # SELECT SUM(Amount), SUM(Amount) as FlowupAmount
    boxjkl_final = k1_summary_df.filter(
        (F.col("LineTypeID") == box_jkl_lt) & (F.col("Amount") != 0)
    ).groupBy(
        "ShareClass", "EntityID", "PartnerNumber", "LineID",
    ).agg(
        F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("Amount")
    ).filter(F.col("Amount") != 0)

    boxjkl_write = boxjkl_final.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("int").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        "EntityID", "ShareClass", "PartnerNumber", "LineID",
        "Amount",
        F.col("Amount").alias("FlowupAmount"),
    )
    boxjkl_aligned = _align_schema(boxjkl_write, "BOXJKLAllocationSummary", cfg)

    # --- AdjustmentLookThroughAllocationSummary ---
    adj_aligned = None
    if adjustment_summary_df is not None:
        adj_write = adjustment_summary_df.filter(F.col("Amount") != 0)
        adj_write_final = adj_write.select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            "EntityID", "ShareClass", "PartnerNumber", "LineID",
            "Amount", "ParentEntityID", "LineTypeID", "AdjustmentTypeID",
            "TrackingKey", "SuperParentEntityID",
        )
        adj_aligned = _align_schema(adj_write_final, "AdjustmentLookThroughAllocationSummary", cfg)

    # --- Write all summaries via GenericResultStorer ---
    result = {
        "K1LookThroughAllocationSummary": k1_aligned,
        "UBTILookThroughAllocationSummary": ubti_aligned,
        "PassiveIncomeAllocationSummary": passive_aligned,
        "BOXJKLAllocationSummary": boxjkl_aligned,
    }
    if adj_aligned is not None:
        result["AdjustmentLookThroughAllocationSummary"] = adj_aligned

    storer = GenericResultStorer(spark)
    return_value = storer.save_results(
        result=result,
        result_type=cfg.get("result_type", "Parquet"),
        catalog_name=cfg["catalog"],
        database_name=cfg["schema"],
        run_id=cfg["run_id"],
        client_id=cfg["client_id"],
        entity_id=cfg["entity_id"],
        execution_id=cfg.get("execution_id", ""),
        volume_path=cfg.get("volume_path", ""),
        sql_url_path="",
        sql_username="",
        sql_password="",
    )

    log_timing("write_final_summaries", t0)
    return k1_write, ubti_write, adj_write_final, return_value


# ---------------------------------------------------------------------------
# Section 22: update_is_rounded_flag
# SQL lines: 1216-1220
# ---------------------------------------------------------------------------
def update_is_rounded_flag(spark, cfg):
    """UPDATE LookThroughOffsetUnRoundedLines SET IsRounded = 1."""
    log_section("update_is_rounded_flag")
    t0 = time.time()

    run_id = cfg["run_id"]
    table = tbl_name("LookThroughOffsetUnRoundedLines", cfg)

    spark.sql(f"""
        MERGE INTO {table} AS target
        USING (SELECT {run_id} AS RunID) AS source
        ON target.RunID = source.RunID
           AND (target.IsRounded IS NULL OR target.IsRounded = false)
        WHEN MATCHED THEN UPDATE SET target.IsRounded = true
    """)

    log_timing("update_is_rounded_flag", t0)


# ---------------------------------------------------------------------------
# Section 23: write_allocation_summaries (uspLoadInvestmentLevelRoundingSummary)
# Aggregates LookThrough tables → K1/UBTI/Adjustment AllocationSummary
# ---------------------------------------------------------------------------
def write_allocation_summaries(spark, cfg, k1_write_df, ubti_write_df, adj_write_df):
    """Aggregate LookThrough summaries into final AllocationSummary tables and write Delta+Parquet."""
    log_section("write_allocation_summaries")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]

    # --- K1AllocationSummary ---
    k1_alloc = k1_write_df.groupBy(
        "PartnerNumber",
        F.coalesce(F.col("ShareClass"), F.lit("")).alias("ShareClass"),
        F.col("LineID"),
    ).agg(
        F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("Amount")
    ).select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.lit(entity_id).cast("int").alias("EntityID"),
        F.col("PartnerNumber"), F.col("ShareClass"), F.col("LineID"),
        F.col("Amount"), F.col("Amount").alias("FlowupAmount"),
        F.lit(None).cast("int").alias("PeriodID"),
    )

    # --- UBTIAllocationSummary ---
    ubti_alloc = None
    if ubti_write_df is not None:
        ubti_alloc = ubti_write_df.groupBy(
            "PartnerNumber",
            F.coalesce(F.col("ShareClass"), F.lit("")).alias("ShareClass"),
            F.col("LineID"), F.col("UBTIType"),
        ).agg(
            F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("Amount")
        ).select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("PartnerNumber"), F.col("ShareClass"), F.col("LineID"),
            F.col("Amount"), F.col("Amount").alias("FlowupAmount"),
            F.col("UBTIType"),
        )

    # --- AdjustmentAllocationSummary ---
    adj_alloc = None
    if adj_write_df is not None:
        adj_alloc = adj_write_df.groupBy(
            "PartnerNumber",
            F.coalesce(F.col("ShareClass"), F.lit("")).alias("ShareClass"),
            F.col("LineID"), F.col("LineTypeID"), F.col("AdjustmentTypeID"),
        ).agg(
            F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("Amount")
        ).select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("PartnerNumber"), F.col("ShareClass"), F.col("LineID"),
            F.col("Amount"), F.col("Amount").alias("FlowupAmount"),
            F.lit(None).cast("int").alias("PeriodID"),
            F.col("LineTypeID"), F.col("AdjustmentTypeID"),
        )

    # Write to Delta + Parquet using GenericResultStorer
    results = {"K1AllocationSummary": k1_alloc}
    if ubti_alloc is not None:
        results["UBTIAllocationSummary"] = ubti_alloc
    if adj_alloc is not None:
        results["AdjustmentAllocationSummary"] = adj_alloc

    storer = GenericResultStorer(spark)
    return_value = storer.save_results(
        result=results,
        result_type=cfg.get("result_type", "Parquet"),
        catalog_name=cfg["catalog"],
        database_name=cfg["schema"],
        run_id=cfg["run_id"],
        client_id=cfg["client_id"],
        entity_id=cfg["entity_id"],
        execution_id=cfg.get("execution_id", ""),
        volume_path=cfg.get("volume_path", ""),
        sql_url_path="",
        sql_username="",
        sql_password="",
    )

    log_timing("write_allocation_summaries", t0)
    return return_value
