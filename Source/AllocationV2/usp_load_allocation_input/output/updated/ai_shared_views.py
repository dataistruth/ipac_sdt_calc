"""
ai_shared_views.py

Registers shared lookup tables as session-scoped temp views once at pipeline start.
Eliminates redundant Delta table scans across all service modules.

Tables pre-registered:
  - _entity: Entity (full entity dimension)
  - _fx_avg_rate: ForeignCurrencyAverageRate filtered by ClientID + TransactionID
  - _aiw: AllocationInputWorkflow filtered by RunID
  - _reclass_data: ReclassFootnoteAllocationData filtered by RunID + ClientID + TaxPeriodID
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import table_prefix, read_table, log_section, log_timing
from .checkpoint import pipeline_checkpoint as checkpoint

logger = logging.getLogger(__name__)


def register_shared_views(spark: SparkSession, cfg: dict) -> None:
    """Register shared lookup tables as temp views for downstream queries.

    All shared views are broadcast-hinted since they are small dimension/lookup
    tables (hundreds to low thousands of rows). This eliminates shuffle joins
    across the entire pipeline.
    """
    log_section("register_shared_views")
    t0 = time.time()
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    fx_tid = cfg.get("fx_rate_transaction_id") or 0

    # Entity — entity dimension (small, ~hundreds of rows per client)
    F.broadcast(
        read_table(spark, "Entity", cfg)
        .select("EntityID", "ClientID", "TaxPeriodID", "EntityIdentification", "EIN",
                "CurrencyCode", "IsForeign", "IsPFIC", "IsCFC",
                "IsQualifiedForeignCorporation", "IsDomesticBlocker", "TaxClassID",
                "AllocationTypeID", "DisplayName")
    ).createOrReplaceTempView("_entity")

    # ForeignCurrencyAverageRate — pre-filtered to this run's FX transaction
    F.broadcast(
        read_table(spark, "ForeignCurrencyAverageRate", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TransactionID") == fx_tid))
        .select("CurrencyCode", "AverageRate")
    ).createOrReplaceTempView("_fx_avg_rate")

    # AllocationInputWorkflow — pre-filtered to this RunID (small: one row per entity)
    F.broadcast(
        read_table(spark, "AllocationInputWorkflow", cfg)
        .filter(F.col("RunID") == run_id)
    ).createOrReplaceTempView("_aiw")

    # ReclassFootnoteAllocationData — pre-filtered to RunID + ClientID + TaxPeriodID
    _reclass_df = (
        read_table(spark, "ReclassFootnoteAllocationData", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.col("RunID") == run_id)
        )
    )
    _reclass_df = checkpoint(spark, _reclass_df, "reclass_data", cfg)
    _reclass_df.createOrReplaceTempView("_reclass_data")

    # K1LineItem — small lookup, used in almost every K1/form query
    F.broadcast(
        read_table(spark, "K1LineItem", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
        .select("LineID", "LineDataType", "IsActive", "PFICClassType", "TransactionDate")
    ).createOrReplaceTempView("_k1_line_item")

    # MAP_K1LineItemLineType — small mapping table
    F.broadcast(
        read_table(spark, "MAP_K1LineItemLineType", cfg)
        .select("K1LineItemID", "LineTypeID")
    ).createOrReplaceTempView("_map_k1_line_type")

    # K1Package — pre-filtered, maps K1PackageID to LowerTierEntityID
    F.broadcast(
        read_table(spark, "K1Package", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
        .select("K1PackageID", "LowerTierEntityID", "UpperTierEntityID")
    ).createOrReplaceTempView("_k1_package")

    # ForeignCurrencyRate — pre-filtered to this FX transaction
    F.broadcast(
        read_table(spark, "ForeignCurrencyRate", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TransactionID") == fx_tid))
        .select("CurrencyCode", "Rate", "Range")
    ).createOrReplaceTempView("_fx_rate")

    # PFICFootnoteLineItem — small lookup, used 19× across PFIC/flowup/finalization
    F.broadcast(
        read_table(spark, "PFICFootnoteLineItem", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
        .select("LineID", "ShortName", "LineDataType", "IsActive", "IsAllocated")
    ).createOrReplaceTempView("_pfic_line_item")

    # LowerTierFunds — register for use in write_form_flowups
    is_blocker_checked = cfg.get("is_foreign_blocker_footnotes_flowup_checked", False)
    ltf_df = read_table(spark, "LowerTierFunds", cfg)
    entity_view = spark.table("_entity")
    tax_class_df = read_table(spark, "ENU_TaxClass", cfg)
    _ltf_df = (
        ltf_df.alias("LF")
        .join(entity_view.alias("E"), F.col("LF.EntityID") == F.col("E.EntityID"), "left")
        .join(tax_class_df.alias("T"), F.col("E.TaxClassID") == F.col("T.TaxClassID"), "left")
        .filter(F.col("LF.RunID") == run_id)
        .select(
            F.col("LF.EntityID"),
            F.col("LF.PartnerNumber"),
            F.col("LF.LTRunID").alias("RunID"),
            # Orphan (no Entity) rows keep IsForeign NULL, matching SQL's un-updated rows.
            F.when(F.col("E.EntityID").isNull(), F.lit(None).cast("boolean"))
            .when(
                (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == False) &
                (F.lower(F.coalesce(F.col("T.TaxClassName"), F.lit(""))) == "disregarded entity"),
                F.lit(True)
            ).otherwise(F.coalesce(F.col("E.IsForeign"), F.lit(False))).alias("IsForeign"),
            F.when(F.col("E.EntityID").isNull(), F.lit(None).cast("boolean"))
            .when(
                ((F.coalesce(F.col("E.IsPFIC"), F.lit(False)) == True) |
                 (F.coalesce(F.col("E.IsCFC"), F.lit(False)) == True) |
                 (F.coalesce(F.col("E.IsQualifiedForeignCorporation"), F.lit(False)) == True)) &
                (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == True) &
                F.lit(is_blocker_checked),
                F.lit(True)
            ).otherwise(F.lit(False)).alias("IsPficCfcQfcEntity"),
        )
    )
    F.broadcast(_ltf_df).createOrReplaceTempView(f"_lower_tier_funds_{run_id}")

    log_timing("register_shared_views", t0)
