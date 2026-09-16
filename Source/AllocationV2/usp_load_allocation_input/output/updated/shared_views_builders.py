"""Parity-preserving builders extracted from production ai_shared_views."""

from __future__ import annotations

import pyspark.sql.functions as F
from Common_V2.core.helpers import read_table

from .checkpoint import checkpoint
from .parallel_helpers import run_parallel


def _register_entity(spark, cfg):
    F.broadcast(read_table(spark, "Entity", cfg).select(
        "EntityID", "ClientID", "TaxPeriodID", "EntityIdentification", "EIN",
        "CurrencyCode", "IsForeign", "IsPFIC", "IsCFC",
        "IsQualifiedForeignCorporation", "IsDomesticBlocker", "TaxClassID",
        "AllocationTypeID", "DisplayName",
    )).createOrReplaceTempView("_entity")


def _register_fx_average(spark, cfg):
    F.broadcast(read_table(spark, "ForeignCurrencyAverageRate", cfg).filter(
        (F.col("ClientID") == cfg["client_id"])
        & (F.col("TransactionID") == (cfg.get("fx_rate_transaction_id") or 0))
    ).select("CurrencyCode", "AverageRate")).createOrReplaceTempView("_fx_avg_rate")


def _register_aiw(spark, cfg):
    F.broadcast(read_table(spark, "AllocationInputWorkflow", cfg).filter(
        F.col("RunID") == cfg["run_id"]
    )).createOrReplaceTempView("_aiw")


def _register_k1_line(spark, cfg):
    F.broadcast(read_table(spark, "K1LineItem", cfg).filter(
        (F.col("ClientID") == cfg["client_id"])
        & (F.col("TaxPeriodID") == cfg["tax_period_id"])
    ).select(
        "LineID", "LineDataType", "IsActive", "PFICClassType", "TransactionDate"
    )).createOrReplaceTempView("_k1_line_item")


def _register_k1_map(spark, cfg):
    F.broadcast(read_table(spark, "MAP_K1LineItemLineType", cfg).select(
        "K1LineItemID", "LineTypeID"
    )).createOrReplaceTempView("_map_k1_line_type")


def _register_k1_package(spark, cfg):
    F.broadcast(read_table(spark, "K1Package", cfg).filter(
        (F.col("ClientID") == cfg["client_id"])
        & (F.col("TaxPeriodID") == cfg["tax_period_id"])
    ).select(
        "K1PackageID", "LowerTierEntityID", "UpperTierEntityID"
    )).createOrReplaceTempView("_k1_package")


def _register_fx_rate(spark, cfg):
    F.broadcast(read_table(spark, "ForeignCurrencyRate", cfg).filter(
        (F.col("ClientID") == cfg["client_id"])
        & (F.col("TransactionID") == (cfg.get("fx_rate_transaction_id") or 0))
    ).select("CurrencyCode", "Rate", "Range")).createOrReplaceTempView("_fx_rate")


def _register_pfic_line(spark, cfg):
    F.broadcast(read_table(spark, "PFICFootnoteLineItem", cfg).filter(
        (F.col("ClientID") == cfg["client_id"])
        & (F.col("TaxPeriodID") == cfg["tax_period_id"])
    ).select(
        "LineID", "ShortName", "LineDataType", "IsActive", "IsAllocated"
    )).createOrReplaceTempView("_pfic_line_item")


INDEPENDENT = [
    ("entity", _register_entity),
    ("fx_average", _register_fx_average),
    ("allocation_workflow", _register_aiw),
    ("k1_line_item", _register_k1_line),
    ("k1_line_map", _register_k1_map),
    ("k1_package", _register_k1_package),
    ("fx_rate", _register_fx_rate),
    ("pfic_line_item", _register_pfic_line),
]


def _register_reclass(spark, cfg):
    df = read_table(spark, "ReclassFootnoteAllocationData", cfg).filter(
        (F.col("ClientID") == cfg["client_id"])
        & (F.col("TaxPeriodID") == cfg["tax_period_id"])
        & (F.col("RunID") == cfg["run_id"])
    )
    checkpoint(spark, df, "reclass_data", cfg).createOrReplaceTempView("_reclass_data")


def _register_lower_tier(spark, cfg):
    run_id = cfg["run_id"]
    out = (
        read_table(spark, "LowerTierFunds", cfg).alias("LF")
        .join(spark.table("_entity").alias("E"), F.col("LF.EntityID") == F.col("E.EntityID"), "left")
        .join(read_table(spark, "ENU_TaxClass", cfg).alias("T"),
              F.col("E.TaxClassID") == F.col("T.TaxClassID"), "left")
        .filter(F.col("LF.RunID") == run_id)
        .select(
            F.col("LF.EntityID"), F.col("LF.PartnerNumber"),
            F.col("LF.LTRunID").alias("RunID"),
            F.when(F.col("E.EntityID").isNull(), F.lit(None).cast("boolean"))
            .when(
                (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == False)
                & (F.lower(F.coalesce(F.col("T.TaxClassName"), F.lit("")))
                   == "disregarded entity"),
                F.lit(True),
            ).otherwise(F.coalesce(F.col("E.IsForeign"), F.lit(False))).alias("IsForeign"),
            F.when(F.col("E.EntityID").isNull(), F.lit(None).cast("boolean"))
            .when(
                (
                    (F.coalesce(F.col("E.IsPFIC"), F.lit(False)) == True)
                    | (F.coalesce(F.col("E.IsCFC"), F.lit(False)) == True)
                    | (F.coalesce(F.col("E.IsQualifiedForeignCorporation"), F.lit(False)) == True)
                )
                & (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == True)
                & F.lit(cfg.get("is_foreign_blocker_footnotes_flowup_checked", False)),
                F.lit(True),
            ).otherwise(F.lit(False)).alias("IsPficCfcQfcEntity"),
        )
    )
    F.broadcast(out).createOrReplaceTempView(f"_lower_tier_funds_{run_id}")


def register_all(spark, cfg, max_threads):
    tasks = [
        (name, lambda fn=fn: fn(spark, cfg))
        for name, fn in INDEPENDENT
    ]
    run_parallel(tasks, max_threads, "shared_views")
    # Production order and semantics: reclass checkpoint, then lower-tier after _entity.
    _register_reclass(spark, cfg)
    _register_lower_tier(spark, cfg)
