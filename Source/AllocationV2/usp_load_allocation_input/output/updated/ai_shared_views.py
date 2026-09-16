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

from Common_V2.core.helpers import read_table, log_section, log_timing

from .checkpoint import checkpoint
from .parallel import run_parallel
from .spark_optimizations import cache_for_run, current_run_scoped, scoped

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
    fx_tid = cfg.get("fx_rate_transaction_id") or 0

    def _small(df):
        return cache_for_run(df, cfg, broadcast=True)

    tasks = [
        ("_entity", lambda: _small(
            scoped(read_table(spark, "Entity", cfg), cfg)
            .select(
                "EntityID", "ClientID", "TaxPeriodID", "EntityIdentification",
                "EIN", "CurrencyCode", "IsForeign", "IsPFIC", "IsCFC",
                "IsQualifiedForeignCorporation", "IsDomesticBlocker",
                "TaxClassID", "AllocationTypeID", "DisplayName",
            )
        )),
        ("_fx_avg_rate", lambda: _small(
            scoped(read_table(spark, "ForeignCurrencyAverageRate", cfg), cfg)
            .filter(F.col("TransactionID") == fx_tid)
            .select("CurrencyCode", "AverageRate")
        )),
        ("_aiw", lambda: _small(
            current_run_scoped(
                read_table(spark, "AllocationInputWorkflow", cfg), cfg
            )
        )),
        ("_reclass_data", lambda: current_run_scoped(
            read_table(spark, "ReclassFootnoteAllocationData", cfg), cfg
        )),
        ("_k1_line_item", lambda: _small(
            scoped(read_table(spark, "K1LineItem", cfg), cfg)
            .select(
                "LineID", "LineDataType", "IsActive", "PFICClassType",
                "TransactionDate",
            )
        )),
        ("_map_k1_line_type", lambda: _small(
            read_table(spark, "MAP_K1LineItemLineType", cfg)
            .select("K1LineItemID", "LineTypeID")
        )),
        ("_k1_package", lambda: _small(
            scoped(read_table(spark, "K1Package", cfg), cfg)
            .select("K1PackageID", "LowerTierEntityID", "UpperTierEntityID")
        )),
        ("_fx_rate", lambda: _small(
            scoped(read_table(spark, "ForeignCurrencyRate", cfg), cfg)
            .filter(F.col("TransactionID") == fx_tid)
            .select("CurrencyCode", "Rate", "Range")
        )),
        ("_pfic_line_item", lambda: _small(
            scoped(read_table(spark, "PFICFootnoteLineItem", cfg), cfg)
            .select(
                "LineID", "ShortName", "LineDataType", "IsActive",
                "IsAllocated",
            )
        )),
    ]
    # Spark actions perform the actual loads in a fixed four-thread pool.
    # Temp-view catalog mutation remains deterministic on the caller thread.
    for view_name, frame in run_parallel(tasks, "shared-view-load"):
        if view_name == "_reclass_data":
            # Delta writes and catalog changes stay on the caller thread.
            frame = checkpoint(spark, frame, "reclass_data", cfg)
        frame.createOrReplaceTempView(view_name)

    # LowerTierFunds — register for use in write_form_flowups
    is_blocker_checked = cfg.get("is_foreign_blocker_footnotes_flowup_checked", False)
    ltf_df = current_run_scoped(read_table(spark, "LowerTierFunds", cfg), cfg)
    entity_view = spark.table("_entity")
    tax_class_df = F.broadcast(read_table(spark, "ENU_TaxClass", cfg))
    _ltf_df = (
        ltf_df.alias("LF")
        .join(entity_view.alias("E"), F.col("LF.EntityID") == F.col("E.EntityID"), "left")
        .join(tax_class_df.alias("T"), F.col("E.TaxClassID") == F.col("T.TaxClassID"), "left")
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
    cache_for_run(_ltf_df, cfg, broadcast=True).createOrReplaceTempView(
        f"_lower_tier_funds_{run_id}"
    )

    log_timing("register_shared_views", t0)
