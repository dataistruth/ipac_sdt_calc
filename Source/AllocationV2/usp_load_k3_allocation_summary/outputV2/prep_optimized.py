"""Parity-safe preparation overrides for the K3 outputV2 candidate."""

import pyspark.sql.functions as F

from Common_V2.core.helpers import read_table


def build_income_attr_rounding_import(spark, cfg):
    """Build S3 without broadcasting the unbounded derived-lines relation."""
    base = (
        read_table(spark, "IncomeAttributeRounding", cfg)
        .filter(
            F.col("TransactionID")
            == F.lit(cfg.get("income_attr_import_trans_id"))
        )
        .select(
            "EntityID",
            "LineID",
            "CountryID",
            "AttributeTypeID",
            "AttributeID",
            "RoundDown",
        )
    )
    derived_lines = (
        read_table(spark, "MAP_DerivedLines", cfg)
        .filter(F.col("DerivedLineID").isNotNull())
        .select("BaseLineID", "DerivedLineID", "AttributeID")
    )
    offset_attr = (
        read_table(spark, "ENU_AttributeType", cfg)
        .filter(
            (F.lower(F.col("AttributeType")) == "offset")
            & (
                F.coalesce(F.col("IsHidden"), F.lit(False))
                == F.lit(False)
            )
        )
        .select("AttributeID")
    )
    offset = (
        base.alias("I")
        .join(
            derived_lines.alias("MD"),
            F.col("MD.BaseLineID") == F.col("I.LineID"),
            "inner",
        )
        .join(
            F.broadcast(offset_attr.alias("A")),
            F.col("A.AttributeID") == F.col("MD.AttributeID"),
            "inner",
        )
        .select(
            F.col("I.EntityID").alias("EntityID"),
            F.col("MD.DerivedLineID").alias("LineID"),
            F.col("I.CountryID").alias("CountryID"),
            F.col("I.AttributeTypeID").alias("AttributeTypeID"),
            F.col("I.AttributeID").alias("AttributeID"),
            F.col("I.RoundDown").alias("RoundDown"),
        )
    )
    return base.unionByName(offset)


__all__ = ["build_income_attr_rounding_import"]
