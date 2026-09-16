"""
ai_form_service.py

Form input and flowup builders for uspLoadAllocationInput.

Handles Form 926, Form 8886, Form 199A, Form 8865 inputs
and their corresponding flowup data from ReclassFootnoteAllocationData.

SQL lines: 2380-3700
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import table_prefix, read_table, log_section, log_timing
from . import checkpoint as _ckpt


def scoped(df, cfg):
    fn = getattr(_ckpt, "scoped", None)
    if fn:
        return fn(df, cfg)
    if "ClientID" in df.columns:
        df = df.filter(F.col("ClientID") == cfg["client_id"])
    if "TaxPeriodID" in df.columns:
        df = df.filter(F.col("TaxPeriodID") == cfg["tax_period_id"])
    return df


def prune_to_lower_tier_runs(df, spark, cfg):
    fn = getattr(_ckpt, "prune_to_lower_tier_runs", None)
    if fn:
        return fn(df, spark, cfg)
    if "RunID" not in df.columns:
        return df
    keys = (
        spark.table(f"_lower_tier_funds_{cfg['run_id']}")
        .select(F.col("RunID").cast("long").alias("RunID"))
        .where(F.col("RunID").isNotNull())
        .distinct()
    )
    return df.join(F.broadcast(keys), "RunID", "left_semi")

logger = logging.getLogger(__name__)


def _flowup_parent_entity(
    parent_col: str, source_col: str, lt_col: str, lower_tier_col: str
) -> F.Column:
    """Standard flowup ParentEntityID logic reused across all forms."""
    return (
        F.when(
            (F.coalesce(F.col(parent_col), F.lit(0)) == 0) |
            (F.coalesce(F.col(parent_col), F.lit(0)) == F.col(source_col)),
            F.when(F.col(lt_col) == F.col(lower_tier_col), F.lit(0))
             .otherwise(F.col(lt_col))
        ).otherwise(F.coalesce(F.col(parent_col), F.lit(0)))
    )


def build_all_form_inputs(
    spark: SparkSession,
    cfg: dict,
) -> DataFrame:
    """Build all form-type allocation inputs (926, 199A, 8886, 8865).

    Each form inserts rows into AllocationInput from:
    1. Direct snapshot data (Form{X}Input_Snapshot JOIN K1Workflow)
    2. Flowup data (ReclassFootnoteAllocationData)

    Returns combined DataFrame[SuperParentEntityID, EntityID, LineTypeID, LineID,
        Amount, TransactionName, TransactionEntityID, QuicklinkID, CategoryID,
        PeriodID, LineCode, ParentEntityID, AdjustmentTypeID, Tag, TrackingKey,
        SchID, OriginalParentEntityID]

    SQL lines: 2380-3700
    """
    log_section("build_all_form_inputs")
    t0 = time.time()
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    fx_tid = cfg.get("fx_rate_transaction_id") or 0
    is_tracking = cfg.get("is_tracking_key", "C") == "C"
    is_pfic_cfc_qfc = cfg.get("is_pfic_cfc_qfc_entity", False)
    is_blocker_checked = cfg.get("is_foreign_blocker_footnotes_flowup_checked", False)

    # Schema for AllocationInput
    alloc_cols = """
        SuperParentEntityID INT, EntityID INT, LineTypeID INT, LineID INT,
        Amount DOUBLE, TransactionName STRING, TransactionEntityID INT,
        QuicklinkID INT, CategoryID INT, PeriodID INT, LineCode STRING,
        ParentEntityID INT, AdjustmentTypeID INT, Tag STRING,
        TrackingKey STRING, SchID INT, OriginalParentEntityID INT
    """

    parts = []

    # Config line IDs for Form 926 special handling
    line_9a_after = cfg.get("line_9a_after_line_id", 0)
    line_9a_before = cfg.get("line_9a_before_line_id", 0)
    form926_transfer_date_line_id = cfg.get("form926_transfer_date_line_id", 0)

    # Temp views
    aiw_df = spark.table("_aiw")
    k1_wf_df = spark.table(f"_k1_workflow_{run_id}")
    entity_df = spark.table("_entity")
    fx_avg_df = spark.table("_fx_avg_rate")
    reclass_df = spark.table("_reclass_data")
    k1_pkg_df = spark.table("_k1_package")

    # ─── Form 926 ─────────────────────────────────────────────────────────
    form926_line_type_id = cfg.get("form926_line_type_id")
    if form926_line_type_id:
        f926_snapshot = (
            read_table(spark, "Form926Input_Snapshot", cfg)
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            )
        )
        f926_line_item = F.broadcast(
            scoped(read_table(spark, "Form926LineItem", cfg), cfg)
        )
        f926_package = F.broadcast(
            scoped(read_table(spark, "Form926Package", cfg), cfg)
        )

        if not is_pfic_cfc_qfc or not is_blocker_checked:
            # Build Form926DateValue (non-Various transfer dates for spot rate)
            form926_date_value = (
                f926_snapshot.alias("F926")
                .join(
                    f926_line_item.alias("li"),
                    (F.col("li.LineID") == F.col("F926.LineID")) &
                    (F.col("li.LineID") == F.lit(form926_transfer_date_line_id)) &
                    (F.lower(F.col("F926.TextValue")) != "various") &
                    (F.col("li.IsActive") == True),
                    "inner"
                )
                .join(
                    k1_wf_df.alias("AIW"),
                    F.col("F926.WorkflowID") == F.col("AIW.WorkflowID"),
                    "inner"
                )
                .filter(F.coalesce(F.col("F926.TextValue"), F.lit("")) != "")
                .select(
                    F.col("F926.Form926ID"),
                    F.col("F926.TextValue").alias("DateValue")
                )
            )

            has_transfer_date = (
                form926_transfer_date_line_id is not None
                and form926_transfer_date_line_id != 0
                and not read_table(spark, "Form926LineItem", cfg)
                .filter(
                    (F.col("ClientID") == client_id)
                    & (F.col("TaxPeriodID") == tax_period_id)
                    & (F.lower(F.col("ShortName")) == "transferdate")
                    & (F.col("IsActive") == True)
                )
                .isEmpty()
            )

            # Common amount expression for Form 926
            def _f926_amount_expr(rate_col):
                return (
                    F.when(
                        F.col("FL.LineID").isin(line_9a_after, line_9a_before),
                        F.col("F926.Amount")
                    ).otherwise(
                        F.round(F.col("F926.Amount") / F.coalesce(rate_col, F.lit(1)), 0)
                    )
                )

            if has_transfer_date:
                # Branch A: 'Various' transfer date → use average rate
                various_ss = (
                    f926_snapshot.alias("ss")
                    .filter(
                        (F.lower(F.col("ss.TextValue")) == "various") &
                        (F.col("ss.LineID") == form926_transfer_date_line_id)
                    )
                    .select("ss.Form926ID", "ss.WorkflowID")
                )

                branch_a = (
                    f926_snapshot.alias("F926")
                    .join(
                        f926_line_item.alias("FL"),
                        (F.col("F926.LineID") == F.col("FL.LineID")) &
                        (F.upper(F.col("FL.LineDataType")) == "NUMBER") &
                        (F.col("FL.IsAllocated") == True) &
                        (F.col("FL.ClientID") == F.col("F926.ClientID")) &
                        (F.col("FL.TaxPeriodID") == F.col("F926.TaxPeriodID")) &
                        (F.col("FL.IsActive") == True),
                        "inner"
                    )
                    .join(
                        k1_wf_df.alias("AIW"),
                        F.col("F926.WorkflowID") == F.col("AIW.WorkflowID"),
                        "inner"
                    )
                    .join(
                        various_ss.alias("ss"),
                        (F.col("ss.Form926ID") == F.col("F926.Form926ID")) &
                        (F.col("ss.WorkflowID") == F.col("F926.WorkflowID")),
                        "inner"
                    )
                    .join(
                        entity_df.alias("E"),
                        F.col("E.EntityID") == F.col("AIW.EntityID"),
                        "inner"
                    )
                    .join(
                        f926_package.alias("P"),
                        F.col("P.Form926ID") == F.col("F926.Form926ID"),
                        "inner"
                    )
                    .join(
                        k1_pkg_df.alias("K1P"),
                        F.col("K1P.K1PackageID") == F.col("P.K1PackageID"),
                        "inner"
                    )
                    .join(
                        fx_avg_df.alias("R"),
                        F.col("R.CurrencyCode") == F.col("E.CurrencyCode"),
                        "left"
                    )
                    .filter(F.coalesce(F.col("F926.Amount"), F.lit(0)) != 0)
                    .select(
                        F.lit(None).cast("int").alias("SuperParentEntityID"),
                        F.col("K1P.LowerTierEntityID").alias("EntityID"),
                        F.lit(form926_line_type_id).alias("LineTypeID"),
                        F.col("FL.LineID"),
                        _f926_amount_expr(F.col("R.AverageRate")).alias("Amount"),
                        F.lit(None).cast("string").alias("TransactionName"),
                        F.lit(None).cast("int").alias("TransactionEntityID"),
                        F.col("F926.Form926ID").alias("QuicklinkID"),
                        F.lit(None).cast("int").alias("CategoryID"),
                        F.lit(None).cast("int").alias("PeriodID"),
                        F.lit(None).cast("string").alias("LineCode"),
                        F.lit(0).alias("ParentEntityID"),
                        F.lit(None).cast("int").alias("AdjustmentTypeID"),
                        F.lit(None).cast("string").alias("Tag"),
                        F.when(F.lit(is_tracking), F.col("AIW.EntityID").cast("string"))
                         .otherwise(F.lit(None).cast("string")).alias("TrackingKey"),
                        F.lit(None).cast("int").alias("SchID"),
                        F.lit(None).cast("int").alias("OriginalParentEntityID"),
                    )
                )
                parts.append(branch_a)

                # Branch B: Specific date → spot rate lookup via ForeignCurrencyRate
                fx_rate_tbl = read_table(spark, "ForeignCurrencyRate", cfg)
                entity_tbl = spark.table("_entity")
                alloc_run_tbl = read_table(spark, "AllocationRun", cfg)
                run_type = cfg.get("run_type")
                phase_id_val = cfg.get("phase_id")
                max_success_run = (
                    alloc_run_tbl
                    .filter(
                        (F.col("EntityID") == entity_id) &
                        (F.col("ClientID") == client_id) &
                        (F.col("TaxPeriodID") == tax_period_id) &
                        (F.upper(F.col("RunStatus")) == "SUCCESS") &
                        (F.col("RunType") == run_type) &
                        (F.col("PhaseID") == phase_id_val)
                    )
                    .agg(F.max("RunID").alias("MaxRunID"))
                    .first()
                )
                if max_success_run and max_success_run["MaxRunID"]:
                    fx_run_detail = (
                        alloc_run_tbl
                        .filter(F.col("RunID") == max_success_run["MaxRunID"])
                        .select("ForeignCurrencyRateTransactionID")
                        .first()
                    )
                    spot_fx_tid = fx_run_detail["ForeignCurrencyRateTransactionID"] if fx_run_detail else fx_tid
                else:
                    spot_fx_tid = fx_tid

                spot_rate = (
                    fx_rate_tbl
                    .filter((F.col("TransactionID") == spot_fx_tid) & (F.col("ClientID") == client_id))
                    .groupBy("ClientID", "CurrencyCode", "TransactionID", "Range")
                    .agg(F.first("Rate", ignorenulls=True).alias("Rate"))
                )

                branch_b = (
                    f926_snapshot.alias("F926")
                    .join(
                        f926_line_item.alias("FL"),
                        (F.col("F926.LineID") == F.col("FL.LineID")) &
                        (F.upper(F.col("FL.LineDataType")) == "NUMBER") &
                        (F.col("FL.IsAllocated") == True) &
                        (F.col("FL.ClientID") == F.col("F926.ClientID")) &
                        (F.col("FL.TaxPeriodID") == F.col("F926.TaxPeriodID")) &
                        (F.col("FL.IsActive") == True),
                        "inner"
                    )
                    .join(
                        k1_wf_df.alias("AIW"),
                        F.col("F926.WorkflowID") == F.col("AIW.WorkflowID"),
                        "inner"
                    )
                    .join(
                        form926_date_value.alias("ss"),
                        F.col("ss.Form926ID") == F.col("F926.Form926ID"),
                        "inner"
                    )
                    .join(
                        f926_package.alias("P"),
                        F.col("P.Form926ID") == F.col("F926.Form926ID"),
                        "inner"
                    )
                    .join(
                        k1_pkg_df.alias("K1P"),
                        F.col("K1P.K1PackageID") == F.col("P.K1PackageID"),
                        "inner"
                    )
                    .join(
                        entity_tbl.alias("E"),
                        F.col("E.EntityID") == F.col("AIW.EntityID"),
                        "inner"
                    )
                    .join(
                        spot_rate.alias("SR"),  # N4: deduped (one row per natural key)
                        (F.col("SR.ClientID") == F.col("F926.ClientID")) &
                        (F.col("SR.CurrencyCode") == F.col("E.CurrencyCode")) &
                        (F.col("SR.TransactionID") == spot_fx_tid) &
                        (F.col("SR.Range") == F.col("ss.DateValue").cast("date")),
                        "left"
                    )
                    .filter(F.coalesce(F.col("F926.Amount"), F.lit(0)) != 0)
                    .select(
                        F.lit(None).cast("int").alias("SuperParentEntityID"),
                        F.col("K1P.LowerTierEntityID").alias("EntityID"),
                        F.lit(form926_line_type_id).alias("LineTypeID"),
                        F.col("FL.LineID"),
                        F.when(
                            F.col("FL.LineID").isin(line_9a_after, line_9a_before),
                            F.col("F926.Amount")
                        ).otherwise(
                            F.round(F.col("F926.Amount") / F.when(
                                (F.upper(F.col("E.CurrencyCode")) == "USD") | (F.coalesce(F.col("E.CurrencyCode"), F.lit("")) == ""),
                                F.lit(1)
                            ).otherwise(
                                F.coalesce(F.col("SR.Rate"), F.lit(1))
                            ), 0)
                        ).alias("Amount"),
                        F.lit(None).cast("string").alias("TransactionName"),
                        F.lit(None).cast("int").alias("TransactionEntityID"),
                        F.col("F926.Form926ID").alias("QuicklinkID"),
                        F.lit(None).cast("int").alias("CategoryID"),
                        F.lit(None).cast("int").alias("PeriodID"),
                        F.lit(None).cast("string").alias("LineCode"),
                        F.lit(0).alias("ParentEntityID"),
                        F.lit(None).cast("int").alias("AdjustmentTypeID"),
                        F.lit(None).cast("string").alias("Tag"),
                        F.when(F.lit(is_tracking), F.col("AIW.EntityID").cast("string"))
                         .otherwise(F.lit(None).cast("string")).alias("TrackingKey"),
                        F.lit(None).cast("int").alias("SchID"),
                        F.lit(None).cast("int").alias("OriginalParentEntityID"),
                    )
                )
                parts.append(branch_b)
            else:
                # No TransferDate line → average rate for all
                no_td = (
                    f926_snapshot.alias("F926")
                    .join(
                        f926_line_item.alias("FL"),
                        (F.col("F926.LineID") == F.col("FL.LineID")) &
                        (F.upper(F.col("FL.LineDataType")) == "NUMBER") &
                        (F.col("FL.IsAllocated") == True) &
                        (F.col("FL.ClientID") == F.col("F926.ClientID")) &
                        (F.col("FL.TaxPeriodID") == F.col("F926.TaxPeriodID")) &
                        (F.col("FL.IsActive") == True),
                        "inner"
                    )
                    .join(
                        k1_wf_df.alias("AIW"),
                        F.col("F926.WorkflowID") == F.col("AIW.WorkflowID"),
                        "inner"
                    )
                    .join(
                        entity_df.alias("E"),
                        F.col("E.EntityID") == F.col("AIW.EntityID"),
                        "inner"
                    )
                    .join(
                        f926_package.alias("P"),
                        F.col("P.Form926ID") == F.col("F926.Form926ID"),
                        "inner"
                    )
                    .join(
                        k1_pkg_df.alias("K1P"),
                        F.col("K1P.K1PackageID") == F.col("P.K1PackageID"),
                        "inner"
                    )
                    .join(
                        fx_avg_df.alias("R"),
                        F.col("R.CurrencyCode") == F.col("E.CurrencyCode"),
                        "left"
                    )
                    .filter(F.coalesce(F.col("F926.Amount"), F.lit(0)) != 0)
                    .select(
                        F.lit(None).cast("int").alias("SuperParentEntityID"),
                        F.col("K1P.LowerTierEntityID").alias("EntityID"),
                        F.lit(form926_line_type_id).alias("LineTypeID"),
                        F.col("FL.LineID"),
                        _f926_amount_expr(F.col("R.AverageRate")).alias("Amount"),
                        F.lit(None).cast("string").alias("TransactionName"),
                        F.lit(None).cast("int").alias("TransactionEntityID"),
                        F.col("F926.Form926ID").alias("QuicklinkID"),
                        F.lit(None).cast("int").alias("CategoryID"),
                        F.lit(None).cast("int").alias("PeriodID"),
                        F.lit(None).cast("string").alias("LineCode"),
                        F.lit(0).alias("ParentEntityID"),
                        F.lit(None).cast("int").alias("AdjustmentTypeID"),
                        F.lit(None).cast("string").alias("Tag"),
                        F.when(F.lit(is_tracking), F.col("AIW.EntityID").cast("string"))
                         .otherwise(F.lit(None).cast("string")).alias("TrackingKey"),
                        F.lit(None).cast("int").alias("SchID"),
                        F.lit(None).cast("int").alias("OriginalParentEntityID"),
                    )
                )
                parts.append(no_td)

        # Flowup from ReclassFootnoteAllocationData
        f926_flowup = (
            reclass_df.alias("F926")
            .join(
                f926_line_item.alias("FL"),
                (F.col("F926.LineID") == F.col("FL.LineID")) &
                (F.upper(F.col("FL.LineDataType")) == "NUMBER") &
                (F.col("FL.IsAllocated") == True) &
                (F.col("FL.ClientID") == F.col("F926.ClientID")) &
                (F.col("FL.TaxPeriodID") == F.col("F926.TaxPeriodID")) &
                (F.col("FL.IsActive") == True),
                "inner"
            )
            .join(
                f926_package.alias("P"),
                F.col("P.Form926ID") == F.col("F926.FootnoteID"),
                "inner"
            )
            .join(
                k1_pkg_df.alias("K"),
                F.col("K.K1PackageID") == F.col("P.K1PackageID"),
                "inner"
            )
            .filter(F.col("F926.LineTypeID") == form926_line_type_id)
            .groupBy(
                F.col("F926.LineID"),
                F.col("F926.FootnoteID"),
                F.col("K.LowerTierEntityID"),
                F.coalesce(F.col("F926.ParentEntityID"), F.lit(0)).alias("_parent"),
                F.col("F926.SourceEntityID"),
                F.col("F926.LTEntityID"),
                F.coalesce(F.col("F926.TrackingKey"), F.lit("")).alias("_tracking"),
                F.coalesce(F.col("F926.OriginalParentEntityID"), F.lit(entity_id)).alias("_orig_parent"),
            )
            .agg(F.sum("F926.FlowupAmount").alias("Amount"))
            .select(
                F.col("F926.LTEntityID").alias("SuperParentEntityID"),
                F.col("K.LowerTierEntityID").alias("EntityID"),
                F.lit(form926_line_type_id).alias("LineTypeID"),
                F.col("F926.LineID"),
                F.col("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.col("F926.FootnoteID").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                _flowup_parent_entity("_parent", "F926.SourceEntityID", "F926.LTEntityID", "K.LowerTierEntityID").alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.col("_tracking").alias("TrackingKey"),
                F.lit(None).cast("int").alias("SchID"),
                F.col("_orig_parent").alias("OriginalParentEntityID"),
            )
        )
        if not is_pfic_cfc_qfc or not is_blocker_checked:
            parts.append(f926_flowup)

    # ─── Form 199A ────────────────────────────────────────────────────────
    form199a_line_type_id = cfg.get("form199a_line_type_id")
    if form199a_line_type_id:
        f199a_snapshot = (
            read_table(spark, "Form199AInput_Snapshot", cfg)
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            )
        )
        f199a_line_item = F.broadcast(
            scoped(read_table(spark, "Form199ALineItem", cfg), cfg)
        )
        f199a_package = F.broadcast(
            scoped(read_table(spark, "Form199APackage", cfg), cfg)
        )

        f199a_direct = (
            f199a_snapshot.alias("F199A")
            .join(
                f199a_line_item.alias("FL"),
                (F.col("F199A.LineID") == F.col("FL.LineID")) &
                (F.col("FL.IsAllocated") == True) &
                (F.col("FL.ClientID") == F.col("F199A.ClientID")) &
                (F.col("FL.TaxPeriodID") == F.col("F199A.TaxPeriodID")) &
                (F.col("FL.IsActive") == True),
                "inner"
            )
            .join(
                k1_wf_df.alias("KW"),
                F.col("F199A.WorkflowID") == F.col("KW.WorkflowID"),
                "inner"
            )
            .join(
                entity_df.alias("E"),
                F.col("E.EntityID") == F.col("KW.EntityID"),
                "inner"
            )
            .join(
                f199a_package.alias("P"),
                F.col("P.Form199AID") == F.col("F199A.Form199AID"),
                "inner"
            )
            .join(
                k1_pkg_df.alias("K1P"),
                F.col("K1P.K1PackageID") == F.col("P.K1PackageID"),
                "inner"
            )
            .join(
                fx_avg_df.alias("R"),
                F.col("R.CurrencyCode") == F.col("E.CurrencyCode"),
                "left"
            )
            .filter(F.coalesce(F.col("F199A.Amount"), F.lit(0)) != 0)
            .select(
                F.lit(None).cast("int").alias("SuperParentEntityID"),
                F.col("K1P.LowerTierEntityID").alias("EntityID"),
                F.lit(form199a_line_type_id).alias("LineTypeID"),
                F.col("FL.LineID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("F199A.Amount"))
                 .otherwise(F.round(F.col("F199A.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                 .alias("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.col("F199A.Form199AID").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                F.lit(0).alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.when(F.lit(is_tracking), F.col("KW.EntityID").cast("string"))
                 .otherwise(F.lit(None).cast("string")).alias("TrackingKey"),
                F.lit(None).cast("int").alias("SchID"),
                F.lit(None).cast("int").alias("OriginalParentEntityID"),
            )
        )
        if not is_pfic_cfc_qfc or not is_blocker_checked:
            parts.append(f199a_direct)

        f199a_flowup = (
            reclass_df.alias("F")
            .join(
                f199a_line_item.alias("FL"),
                (F.col("F.LineID") == F.col("FL.LineID")) &
                (F.col("FL.IsAllocated") == True) &
                (F.upper(F.col("FL.LineDataType")) == "NUMBER") &
                (F.col("FL.ClientID") == F.col("F.ClientID")) &
                (F.col("FL.TaxPeriodID") == F.col("F.TaxPeriodID")),
                "inner"
            )
            .join(
                f199a_package.alias("P"),
                F.col("P.Form199AID") == F.col("F.FootnoteID"),
                "inner"
            )
            .join(
                k1_pkg_df.alias("K"),
                F.col("K.K1PackageID") == F.col("P.K1PackageID"),
                "inner"
            )
            .filter(F.col("F.LineTypeID") == form199a_line_type_id)
            .groupBy(
                F.col("F.LineID"),
                F.col("F.FootnoteID"),
                F.col("K.LowerTierEntityID"),
                F.coalesce(F.col("F.ParentEntityID"), F.lit(0)).alias("_parent"),
                F.col("F.SourceEntityID"),
                F.col("F.LTEntityID"),
                F.coalesce(F.col("F.TrackingKey"), F.lit("")).alias("_tracking"),
                F.coalesce(F.col("F.OriginalParentEntityID"), F.lit(entity_id)).alias("_orig_parent"),
            )
            .agg(F.sum("F.FlowupAmount").alias("Amount"))
            .select(
                F.col("F.LTEntityID").alias("SuperParentEntityID"),
                F.col("K.LowerTierEntityID").alias("EntityID"),
                F.lit(form199a_line_type_id).alias("LineTypeID"),
                F.col("F.LineID"),
                F.col("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.col("F.FootnoteID").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                _flowup_parent_entity("_parent", "F.SourceEntityID", "F.LTEntityID", "K.LowerTierEntityID").alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.col("_tracking").alias("TrackingKey"),
                F.lit(None).cast("int").alias("SchID"),
                F.col("_orig_parent").alias("OriginalParentEntityID"),
            )
        )
        if not is_pfic_cfc_qfc or not is_blocker_checked:
            parts.append(f199a_flowup)

    # ─── Form 8886 ────────────────────────────────────────────────────────
    # NOTE: Form8886LineItem stores ALL data as LineDataType='TEXT'.
    # The SQL Server SP uses TextValue (not Amount), filters ISNUMERIC(TextValue)=1,
    # and converts to FLOAT. Direct query does NOT filter on IsActive.
    form8886_line_type_id = cfg.get("form8886_line_type_id")
    if form8886_line_type_id:
        f8886_snapshot = (
            read_table(spark, "Form8886Input_Snapshot", cfg)
            .withColumn("_NumericAmount", F.expr("try_cast(TextValue as double)"))
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            )
        )
        f8886_line_item = F.broadcast(
            scoped(read_table(spark, "Form8886LineItem", cfg), cfg)
        )
        f8886_package = F.broadcast(
            scoped(read_table(spark, "Form8886Package", cfg), cfg)
        )

        if not is_pfic_cfc_qfc or not is_blocker_checked:
            f8886_direct = (
                f8886_snapshot.alias("F8886")
                .join(
                    f8886_line_item.alias("FL"),
                    (F.col("F8886.LineID") == F.col("FL.LineID")) &
                    (F.upper(F.col("FL.LineDataType")) == "TEXT") &
                    (F.col("FL.IsAllocated") == True) &
                    (F.col("FL.ClientID") == F.col("F8886.ClientID")) &
                    (F.col("FL.TaxPeriodID") == F.col("F8886.TaxPeriodID")),
                    "inner"
                )
                .join(
                    k1_wf_df.alias("KW"),
                    F.col("F8886.WorkflowID") == F.col("KW.WorkflowID"),
                    "inner"
                )
                .join(
                    entity_df.alias("E"),
                    F.col("E.EntityID") == F.col("KW.EntityID"),
                    "inner"
                )
                .join(
                    f8886_package.alias("P"),
                    F.col("P.Form8886ID") == F.col("F8886.Form8886ID"),
                    "inner"
                )
                .join(
                    k1_pkg_df.alias("K1P"),
                    F.col("K1P.K1PackageID") == F.col("P.K1PackageID"),
                    "inner"
                )
                .join(
                    fx_avg_df.alias("R"),
                    F.col("R.CurrencyCode") == F.col("E.CurrencyCode"),
                    "left"
                )
                .filter(F.col("F8886._NumericAmount").isNotNull())
                .select(
                    F.lit(None).cast("int").alias("SuperParentEntityID"),
                    F.col("K1P.LowerTierEntityID").alias("EntityID"),
                    F.lit(form8886_line_type_id).alias("LineTypeID"),
                    F.col("FL.LineID"),
                    F.round(
                        F.col("F8886._NumericAmount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)),
                        0
                    ).alias("Amount"),
                    F.col("F8886.TransactionName").alias("TransactionName"),
                    F.col("K1P.LowerTierEntityID").alias("TransactionEntityID"),
                    F.col("F8886.Form8886ID").alias("QuicklinkID"),
                    F.lit(None).cast("int").alias("CategoryID"),
                    F.lit(None).cast("int").alias("PeriodID"),
                    F.lit(None).cast("string").alias("LineCode"),
                    F.lit(0).alias("ParentEntityID"),
                    F.lit(None).cast("int").alias("AdjustmentTypeID"),
                    F.lit(None).cast("string").alias("Tag"),
                    F.when(F.lit(is_tracking), F.col("KW.EntityID").cast("string"))
                     .otherwise(F.lit(None).cast("string")).alias("TrackingKey"),
                    F.lit(None).cast("int").alias("SchID"),
                    F.lit(None).cast("int").alias("OriginalParentEntityID"),
                )
            )
            parts.append(f8886_direct)

        # 8886 flowup
        f8886_flowup = (
            reclass_df.alias("F")
            .join(
                f8886_line_item.alias("FL"),
                (F.col("F.LineID") == F.col("FL.LineID")) &
                (F.upper(F.col("FL.LineDataType")) == "TEXT") &
                (F.col("FL.IsAllocated") == True) &
                (F.col("FL.ClientID") == F.col("F.ClientID")) &
                (F.col("FL.TaxPeriodID") == F.col("F.TaxPeriodID")) &
                (F.col("FL.IsActive") == True),
                "inner"
            )
            .join(
                f8886_package.alias("P"),
                F.col("P.Form8886ID") == F.col("F.FootnoteID"),
                "inner"
            )
            .join(
                k1_pkg_df.alias("K"),
                F.col("K.K1PackageID") == F.col("P.K1PackageID"),
                "inner"
            )
            .filter(F.col("F.LineTypeID") == form8886_line_type_id)
            .groupBy(
                F.col("F.LineID"),
                F.col("F.FootnoteID"),
                F.col("K.LowerTierEntityID"),
                F.col("F.TransactionName"),
                F.col("F.TransactionEntityID"),
                F.coalesce(F.col("F.ParentEntityID"), F.lit(0)).alias("_parent"),
                F.col("F.SourceEntityID"),
                F.col("F.LTEntityID"),
                F.coalesce(F.col("F.TrackingKey"), F.lit("")).alias("_tracking"),
                F.coalesce(F.col("F.OriginalParentEntityID"), F.lit(entity_id)).alias("_orig_parent"),
            )
            .agg(F.sum("F.FlowupAmount").alias("Amount"))
            .select(
                F.col("F.LTEntityID").alias("SuperParentEntityID"),
                F.col("K.LowerTierEntityID").alias("EntityID"),
                F.lit(form8886_line_type_id).alias("LineTypeID"),
                F.col("F.LineID"),
                F.col("Amount"),
                F.col("F.TransactionName").alias("TransactionName"),
                F.col("F.TransactionEntityID").alias("TransactionEntityID"),
                F.col("F.FootnoteID").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                _flowup_parent_entity("_parent", "F.SourceEntityID", "F.LTEntityID", "K.LowerTierEntityID").alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.col("_tracking").alias("TrackingKey"),
                F.lit(None).cast("int").alias("SchID"),
                F.col("_orig_parent").alias("OriginalParentEntityID"),
            )
        )
        if not is_pfic_cfc_qfc or not is_blocker_checked:
            parts.append(f8886_flowup)

    # ─── Form 8865 ────────────────────────────────────────────────────────
    form8865_line_type_id = cfg.get("form8865_line_type_id")
    if form8865_line_type_id:
        f8865_snapshot = (
            read_table(spark, "Form8865Input_Snapshot", cfg)
            .withColumn("Amount", F.expr("try_cast(Amount as double)"))
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            )
        )
        f8865_line_item = F.broadcast(
            scoped(read_table(spark, "Form8865LineItem", cfg), cfg)
        )
        f8865_package = F.broadcast(
            scoped(read_table(spark, "Form8865Package", cfg), cfg)
        )

        # Direct from Form8865Input_Snapshot (Schedule N/A, H, E, F)
        f8865_direct = (
            f8865_snapshot.alias("F8865")
            .join(
                f8865_line_item.alias("FL"),
                (F.col("F8865.LineID") == F.col("FL.LineID")) &
                (F.col("FL.IsAllocated") == True) &
                (F.col("FL.ClientID") == F.col("F8865.ClientID")) &
                (F.col("FL.TaxPeriodID") == F.col("F8865.TaxPeriodID")) &
                (F.col("FL.IsActive") == True) &
                (F.upper(F.col("FL.Schedule")).isin("N/A", "H", "E", "F")),
                "inner"
            )
            .join(
                k1_wf_df.alias("KW"),
                F.col("F8865.WorkflowID") == F.col("KW.WorkflowID"),
                "inner"
            )
            .join(
                entity_df.alias("E"),
                F.col("E.EntityID") == F.col("KW.EntityID"),
                "inner"
            )
            .join(
                f8865_package.alias("P"),
                F.col("P.Form8865ID") == F.col("F8865.Form8865ID"),
                "inner"
            )
            .join(
                k1_pkg_df.alias("K1P"),
                F.col("K1P.K1PackageID") == F.col("P.K1PackageID"),
                "inner"
            )
            .join(
                fx_avg_df.alias("R"),
                F.col("R.CurrencyCode") == F.col("E.CurrencyCode"),
                "left"
            )
            .filter(F.coalesce(F.col("F8865.Amount"), F.lit(0)) != 0)
            .select(
                F.lit(None).cast("int").alias("SuperParentEntityID"),
                F.col("K1P.LowerTierEntityID").alias("EntityID"),
                F.lit(form8865_line_type_id).alias("LineTypeID"),
                F.col("FL.LineID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("F8865.Amount"))
                 .otherwise(F.round(F.col("F8865.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                 .alias("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.col("F8865.Form8865ID").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                F.lit(0).alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.when(F.lit(is_tracking), F.col("KW.EntityID").cast("string"))
                 .otherwise(F.lit(None).cast("string")).alias("TrackingKey"),
                F.lit(None).cast("int").alias("SchID"),
                F.lit(None).cast("int").alias("OriginalParentEntityID"),
            )
        )
        if not is_pfic_cfc_qfc or not is_blocker_checked:
            parts.append(f8865_direct)

        # Form 8865 Schedule A (from Form8865SchInput_Snapshot)
        f8865_sch_snapshot = (
            read_table(spark, "Form8865SchInput_Snapshot", cfg)
            .withColumn("Amount", F.expr("try_cast(Amount as double)"))
        )
        f8865_sch_package = F.broadcast(
            scoped(read_table(spark, "Form8865SchPackage", cfg), cfg)
        )

        f8865_sch_a = (
            f8865_sch_snapshot.alias("F8865")
            .join(
                f8865_line_item.alias("FL"),
                (F.col("F8865.LineID") == F.col("FL.LineID")) &
                (F.col("FL.IsAllocated") == True) &
                (F.col("FL.IsActive") == True) &
                (F.upper(F.col("FL.Schedule")) != "N/A"),
                "inner"
            )
            .join(
                k1_wf_df.alias("KW"),
                F.col("F8865.WorkflowID") == F.col("KW.WorkflowID"),
                "inner"
            )
            .join(
                entity_df.alias("E"),
                F.col("E.EntityID") == F.col("KW.EntityID"),
                "inner"
            )
            .join(
                f8865_sch_package.alias("P"),
                F.col("P.SchID") == F.col("F8865.SchID"),
                "inner"
            )
            .join(
                fx_avg_df.alias("R"),
                F.col("R.CurrencyCode") == F.col("E.CurrencyCode"),
                "left"
            )
            .filter(
                (F.col("FL.ClientID") == client_id) &
                (F.col("FL.TaxPeriodID") == tax_period_id) &
                (F.coalesce(F.col("F8865.Amount"), F.lit(0)) != 0)
            )
            .select(
                F.lit(None).cast("int").alias("SuperParentEntityID"),
                F.col("KW.EntityID").alias("EntityID"),
                F.lit(form8865_line_type_id).alias("LineTypeID"),
                F.col("FL.LineID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("F8865.Amount"))
                 .otherwise(F.round(F.col("F8865.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                 .alias("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.col("P.Form8865ID").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                F.lit(0).alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.when(F.lit(is_tracking), F.col("KW.EntityID").cast("string"))
                 .otherwise(F.lit(None).cast("string")).alias("TrackingKey"),
                F.col("F8865.SchID").alias("SchID"),
                F.lit(None).cast("int").alias("OriginalParentEntityID"),
            )
        )
        if not is_pfic_cfc_qfc or not is_blocker_checked:
            parts.append(f8865_sch_a)

        # 8865 flowup (from Form8865AllocationSummary — SQL lines 3115-3163)
        f8865_alloc_summary = (
            prune_to_lower_tier_runs(
                read_table(spark, "Form8865AllocationSummary", cfg), spark, cfg
            )
            .filter(
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id)
            )
        )
        lower_tier_funds = spark.table(f"_lower_tier_funds_{run_id}")

        f8865_flowup = (
            f8865_alloc_summary.alias("F")
            .join(
                f8865_line_item.alias("FL"),
                (F.col("F.LineID") == F.col("FL.LineID")) &
                (F.col("FL.IsAllocated") == True) &
                (F.col("FL.ClientID") == F.col("F.ClientID")) &
                (F.col("FL.TaxPeriodID") == F.col("F.TaxPeriodID")) &
                (F.col("FL.IsActive") == True),
                "inner"
            )
            .join(
                lower_tier_funds.alias("LT"),
                (F.col("F.RunID") == F.col("LT.RunID")) &
                (F.col("F.PartnerNumber") == F.col("LT.PartnerNumber")),
                "inner"
            )
            .join(
                f8865_package.alias("P"),
                F.col("P.Form8865ID") == F.col("F.Form8865ID"),
                "inner"
            )
            .join(
                k1_pkg_df.alias("K"),
                F.col("K.K1PackageID") == F.col("P.K1PackageID"),
                "inner"
            )
            .groupBy(
                F.col("F.LineID"),
                F.col("F.Form8865ID"),
                F.col("K.LowerTierEntityID"),
                F.coalesce(F.col("F.ParentEntityID"), F.lit(0)).alias("_parent"),
                F.col("F.SourceEntityID"),
                F.col("LT.EntityID").alias("_lt_entity"),
                F.coalesce(F.col("F.TrackingKey"), F.lit("")).alias("_tracking"),
                F.col("F.SchID"),
                F.coalesce(F.col("F.OriginalParentEntityID"), F.lit(entity_id)).alias("_orig_parent"),
            )
            .agg(F.sum("F.FlowupAmount").alias("Amount"))
            .select(
                F.col("_lt_entity").alias("SuperParentEntityID"),
                F.col("K.LowerTierEntityID").alias("EntityID"),
                F.lit(form8865_line_type_id).alias("LineTypeID"),
                F.col("F.LineID"),
                F.col("Amount"),
                F.lit(None).cast("string").alias("TransactionName"),
                F.lit(None).cast("int").alias("TransactionEntityID"),
                F.col("F.Form8865ID").alias("QuicklinkID"),
                F.lit(None).cast("int").alias("CategoryID"),
                F.lit(None).cast("int").alias("PeriodID"),
                F.lit(None).cast("string").alias("LineCode"),
                _flowup_parent_entity("_parent", "F.SourceEntityID", "_lt_entity", "K.LowerTierEntityID").alias("ParentEntityID"),
                F.lit(None).cast("int").alias("AdjustmentTypeID"),
                F.lit(None).cast("string").alias("Tag"),
                F.col("_tracking").alias("TrackingKey"),
                F.col("F.SchID").alias("SchID"),
                F.col("_orig_parent").alias("OriginalParentEntityID"),
            )
        )
        if not is_pfic_cfc_qfc or not is_blocker_checked:
            parts.append(f8865_flowup)

    # ─── Union all parts ──────────────────────────────────────────────────
    if not parts:
        result = spark.createDataFrame([], alloc_cols)
    else:
        result = parts[0]
        for p in parts[1:]:
            result = result.unionByName(p, allowMissingColumns=True)

    log_timing("build_all_form_inputs", t0)
    return result
