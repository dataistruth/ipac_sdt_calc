"""
Parallel-safe builders extracted from ai_shared_views.register_shared_views.

Uses read_table + broadcast + createOrReplaceTempView (not SQL strings).
"""

from __future__ import annotations

import logging
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from Common_V2.core.helpers import log_section, log_timing, read_table

from .checkpoint import checkpoint

logger = logging.getLogger(__name__)


def _ids(cfg: dict) -> tuple[int, int, int, int, int]:
    client_id = int(cfg["client_id"])
    tax_period_id = int(cfg["tax_period_id"])
    run_id = int(cfg["run_id"])
    entity_id = int(cfg["entity_id"])
    fx_tid = int(cfg.get("fx_rate_transaction_id") or 0)
    return client_id, tax_period_id, run_id, entity_id, fx_tid


def register_entity(spark: SparkSession, cfg: dict) -> None:
    F.broadcast(
        read_table(spark, "Entity", cfg)
        .select(
            "EntityID", "ClientID", "TaxPeriodID", "EntityIdentification", "EIN",
            "CurrencyCode", "IsForeign", "IsPFIC", "IsCFC",
            "IsQualifiedForeignCorporation", "IsDomesticBlocker", "TaxClassID",
            "AllocationTypeID", "DisplayName",
        )
    ).createOrReplaceTempView("_entity")


def register_fx_avg_rate(spark: SparkSession, cfg: dict) -> None:
    client_id, _, _, _, fx_tid = _ids(cfg)
    F.broadcast(
        read_table(spark, "ForeignCurrencyAverageRate", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TransactionID") == fx_tid))
        .select("CurrencyCode", "AverageRate")
    ).createOrReplaceTempView("_fx_avg_rate")


def register_aiw(spark: SparkSession, cfg: dict) -> None:
    _, _, run_id, _, _ = _ids(cfg)
    F.broadcast(
        read_table(spark, "AllocationInputWorkflow", cfg)
        .filter(F.col("RunID") == run_id)
    ).createOrReplaceTempView("_aiw")


def register_reclass_data(spark: SparkSession, cfg: dict) -> None:
    client_id, tax_period_id, run_id, _, _ = _ids(cfg)
    reclass_df = (
        read_table(spark, "ReclassFootnoteAllocationData", cfg)
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
            & (F.col("RunID") == run_id)
        )
    )
    reclass_df = checkpoint(spark, reclass_df, "reclass_data", cfg)
    reclass_df.createOrReplaceTempView("_reclass_data")


def register_k1_line_item(spark: SparkSession, cfg: dict) -> None:
    client_id, tax_period_id, _, _, _ = _ids(cfg)
    F.broadcast(
        read_table(spark, "K1LineItem", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
        .select("LineID", "LineDataType", "IsActive", "PFICClassType", "TransactionDate")
    ).createOrReplaceTempView("_k1_line_item")


def register_map_k1_line_type(spark: SparkSession, cfg: dict) -> None:
    F.broadcast(
        read_table(spark, "MAP_K1LineItemLineType", cfg)
        .select("K1LineItemID", "LineTypeID")
    ).createOrReplaceTempView("_map_k1_line_type")


def register_k1_package(spark: SparkSession, cfg: dict) -> None:
    client_id, tax_period_id, _, _, _ = _ids(cfg)
    F.broadcast(
        read_table(spark, "K1Package", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
        .select("K1PackageID", "LowerTierEntityID", "UpperTierEntityID")
    ).createOrReplaceTempView("_k1_package")


def register_fx_rate(spark: SparkSession, cfg: dict) -> None:
    client_id, _, _, _, fx_tid = _ids(cfg)
    F.broadcast(
        read_table(spark, "ForeignCurrencyRate", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TransactionID") == fx_tid))
        .select("CurrencyCode", "Rate", "Range")
    ).createOrReplaceTempView("_fx_rate")


def register_pfic_line_item(spark: SparkSession, cfg: dict) -> None:
    client_id, tax_period_id, _, _, _ = _ids(cfg)
    F.broadcast(
        read_table(spark, "PFICFootnoteLineItem", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
        .select("LineID", "ShortName", "LineDataType", "IsActive", "IsAllocated")
    ).createOrReplaceTempView("_pfic_line_item")


def register_lower_tier_funds(spark: SparkSession, cfg: dict) -> None:
    """Requires _entity temp view."""
    _, _, run_id, _, _ = _ids(cfg)
    is_blocker_checked = cfg.get("is_foreign_blocker_footnotes_flowup_checked", False)
    ltf_df = read_table(spark, "LowerTierFunds", cfg)
    entity_view = spark.table("_entity")
    tax_class_df = read_table(spark, "ENU_TaxClass", cfg)
    ltf_out = (
        ltf_df.alias("LF")
        .join(entity_view.alias("E"), F.col("LF.EntityID") == F.col("E.EntityID"), "left")
        .join(tax_class_df.alias("T"), F.col("E.TaxClassID") == F.col("T.TaxClassID"), "left")
        .filter(F.col("LF.RunID") == run_id)
        .select(
            F.col("LF.EntityID"),
            F.col("LF.PartnerNumber"),
            F.col("LF.LTRunID").alias("RunID"),
            F.when(F.col("E.EntityID").isNull(), F.lit(None).cast("boolean"))
            .when(
                (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == False)
                & (F.lower(F.coalesce(F.col("T.TaxClassName"), F.lit(""))) == "disregarded entity"),
                F.lit(True),
            )
            .otherwise(F.coalesce(F.col("E.IsForeign"), F.lit(False)))
            .alias("IsForeign"),
            F.when(F.col("E.EntityID").isNull(), F.lit(None).cast("boolean"))
            .when(
                (
                    (F.coalesce(F.col("E.IsPFIC"), F.lit(False)) == True)
                    | (F.coalesce(F.col("E.IsCFC"), F.lit(False)) == True)
                    | (F.coalesce(F.col("E.IsQualifiedForeignCorporation"), F.lit(False)) == True)
                )
                & (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == True)
                & F.lit(is_blocker_checked),
                F.lit(True),
            )
            .otherwise(F.lit(False))
            .alias("IsPficCfcQfcEntity"),
        )
    )
    F.broadcast(ltf_out).createOrReplaceTempView(f"_lower_tier_funds_{run_id}")


# Independent views (safe for parallel batch — no cross-view deps)
INDEPENDENT_VIEW_REGISTRARS: list[tuple[str, object]] = [
    ("_entity", register_entity),
    ("_fx_avg_rate", register_fx_avg_rate),
    ("_aiw", register_aiw),
    ("_k1_line_item", register_k1_line_item),
    ("_map_k1_line_type", register_map_k1_line_type),
    ("_k1_package", register_k1_package),
    ("_fx_rate", register_fx_rate),
    ("_pfic_line_item", register_pfic_line_item),
]


def register_shared_views_parallel_builders(spark: SparkSession, cfg: dict, workers: int) -> None:
    """
    Mirror ai_shared_views.register_shared_views with parallel independent reads.

    Phase 1: 8 broadcast lookups in parallel (max_workers)
    Phase 2: _reclass_data (checkpoint)
    Phase 3: _lower_tier_funds_{run_id} (needs _entity)
    """
    from .parallel_config import run_parallel_tasks

    log_section("register_shared_views_parallel")
    t0 = time.time()

    tasks = [
        (name, lambda fn=fn: fn(spark, cfg) or {})
        for name, fn in INDEPENDENT_VIEW_REGISTRARS
    ]
    run_parallel_tasks(spark, "register_shared_views_independent", tasks, workers)

    register_reclass_data(spark, cfg)
    register_lower_tier_funds(spark, cfg)

    log_timing("register_shared_views_parallel", t0)
