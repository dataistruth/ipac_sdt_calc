"""Equivalent source builders without warning-only Spark actions."""

from __future__ import annotations

import pyspark.sql.functions as F


def _tbl(spark, name: str, cfg: dict):
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def load_line_items(spark, cfg: dict):
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


def load_quarters(spark, cfg: dict):
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


def build_lookthrough_input_modes14(spark, cfg: dict):
    def sql_round(column, scale):
        factor = F.pow(F.lit(10), F.lit(scale))
        return (
            F.signum(column)
            * F.floor(F.abs(column) * factor + F.lit(0.5))
            / factor
        )

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
                        sql_round(
                            F.coalesce(F.col("Amount"), F.lit(0.0)), 0
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


__all__ = [
    "build_lookthrough_input_modes14",
    "load_line_items",
    "load_quarters",
]
