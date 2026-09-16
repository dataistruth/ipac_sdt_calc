# updated-package sync marker v2 (2026-09-01): resync the ENTIRE
# output/updated/ folder as one set.
"""Semantically equivalent source builders without logging-only Spark actions."""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from .parent import output_module

_book = output_module("book_effective")


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def load_line_items(spark: SparkSession, cfg: dict) -> DataFrame:
    """Build line items without the production warning-only ``isEmpty`` job."""
    k1 = _tbl(spark, "K1LineItem", cfg).select(
        "LineID",
        "AllocationTypeRuleId",
        F.lit(cfg["k1_line_type_id"]).cast("int").alias("LineTypeID"),
        "TransactionDate",
        "IsTransactionDate",
        "IsTransfersAdjusted",
    )
    box_jkl = _tbl(spark, "BoxjklLineItem", cfg).select(
        "LineID",
        F.lit(cfg["yearly_allocation_type_id"])
        .cast("int")
        .alias("AllocationTypeRuleId"),
        F.lit(cfg["box_jkl_line_type_id"]).cast("int").alias("LineTypeID"),
        F.lit(None).cast("timestamp").alias("TransactionDate"),
        F.lit(False).alias("IsTransactionDate"),
        F.lit(True).alias("IsTransfersAdjusted"),
    )
    return k1.unionByName(box_jkl)


def load_quarters(spark: SparkSession, cfg: dict) -> DataFrame:
    """Build quarter values without the production warning-only ``isEmpty`` job."""
    if (
        cfg.get("allocation_type_name", "") == "PE Book Allocation"
        and cfg.get("is_dated_transfers_configured", "") == "C"
    ):
        return _tbl(spark, "QuarterDates", cfg).select("Quarter")
    return (
        _tbl(spark, "ENU_DF_DataList", cfg)
        .filter(F.col("Category") == "Quarters")
        .select(F.col("LookUpData").alias("Quarter"))
    )


def build_lookthrough_input_modes14(
    spark: SparkSession,
    cfg: dict,
) -> DataFrame:
    """Build RunID-pruned lookthrough input without a warning-only probe."""
    k1_id = cfg["k1_line_type_id"]
    adjustment_id = cfg["adjustment_line_type_id"]
    box_jkl_id = cfg["box_jkl_line_type_id"]

    return (
        _tbl(spark, "LookThroughAllocationInput", cfg)
        .filter(
            (F.col("RunID") == cfg["run_id"])
            & (F.col("ClientID") == cfg["client_id"])
            & F.col("LineTypeID").isin([k1_id, adjustment_id, box_jkl_id])
            & (
                (F.col("LineTypeID") == box_jkl_id)
                | (
                    F.col("LineTypeID").isin([k1_id, adjustment_id])
                    & (
                        _book._sql_round(
                            F.coalesce(F.col("Amount"), F.lit(0.0)),
                            0,
                        )
                        != 0
                    )
                )
            )
        )
        .select(
            "RunID",
            "ClientID",
            "EntityID",
            "LineTypeID",
            "LineID",
            "Amount",
            "QuicklinkID",
            "Amount704b",
            "TrackingKey",
            "Tag",
        )
    )
