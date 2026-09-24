"""Small, default-off transforms used by isolated outputV3 experiments."""

from __future__ import annotations

import pyspark.sql.functions as F

from .parent import isolated_output_module

_book = isolated_output_module("book_effective")


def _table(spark, cfg, name):
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def load_line_items_without_warning_probe(spark, cfg):
    """Production relation without its logging-only ``isEmpty`` action."""
    k1 = _table(spark, cfg, "K1LineItem").select(
        "LineID",
        "AllocationTypeRuleId",
        F.lit(cfg["k1_line_type_id"]).cast("int").alias("LineTypeID"),
        "TransactionDate",
        "IsTransactionDate",
        "IsTransfersAdjusted",
    )
    box_jkl = _table(spark, cfg, "BoxjklLineItem").select(
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


def load_quarters_without_warning_probe(spark, cfg):
    """Production relation without its logging-only ``isEmpty`` action."""
    if (
        cfg.get("allocation_type_name", "") == "PE Book Allocation"
        and cfg.get("is_dated_transfers_configured", "") == "C"
    ):
        return _table(spark, cfg, "QuarterDates").select("Quarter")
    return (
        _table(spark, cfg, "ENU_DF_DataList")
        .filter(F.col("Category") == "Quarters")
        .select(F.col("LookUpData").alias("Quarter"))
    )


def build_lookthrough_without_warning_probe(spark, cfg):
    """Production relation without its logging-only ``isEmpty`` action."""
    k1_id = cfg["k1_line_type_id"]
    adjustment_id = cfg["adjustment_line_type_id"]
    box_jkl_id = cfg["box_jkl_line_type_id"]
    return (
        _table(spark, cfg, "LookThroughAllocationInput")
        .filter(
            (F.col("RunID") == cfg["run_id"])
            & (F.col("ClientID") == cfg["client_id"])
            & F.col("LineTypeID").isin(
                [k1_id, adjustment_id, box_jkl_id]
            )
            & (
                (F.col("LineTypeID") == box_jkl_id)
                | (
                    F.col("LineTypeID").isin([k1_id, adjustment_id])
                    & (
                        _book._sql_round(
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


WARNING_PROBE_BUILDERS = {
    "line_items": load_line_items_without_warning_probe,
    "quarters": load_quarters_without_warning_probe,
    "lookthrough": build_lookthrough_without_warning_probe,
}

