"""
entities.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Non-dated and dated entity classification.
Conversion date: 2026-05-04

SQL lines: 3370-3420
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

logger = logging.getLogger(__name__)


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _log_timing(name, start):
    logger.info(f"[TIMING] {name}: {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# build_non_dated_entities
# SQL lines: 3373-3382
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_non_dated_entities(
    input_lines: DataFrame, line_items: DataFrame, cfg: dict,
) -> DataFrame:
    """Build #TempNonDatedEntities — lines whose K1LineItem has
    IsTransfersAdjusted=0 (K1) or IsTransactionDate=0 (BoxJKL).

    These lines get Q0 (non-quarterly) effective percentages.
    """
    t0 = time.time()
    logger.info("[SECTION] build_non_dated_entities")

    box_jkl_lt_id = cfg["box_jkl_line_type_id"]

    result = (
        input_lines.alias("L")
        .join(
            line_items.alias("K"),
            (F.col("K.LineID") == F.col("L.LineID"))
            & (
                F.when(
                    F.col("K.LineTypeID") == box_jkl_lt_id,
                    F.coalesce(F.col("K.IsTransactionDate"), F.lit(False)),
                ).otherwise(
                    F.coalesce(F.col("K.IsTransfersAdjusted"), F.lit(False)),
                )
                == False
            ),
        )
        .select(
            F.col("L.UnderlyingEntityID").cast("int").alias("UnderlyingEntityID"),
            F.col("L.LineTypeID"),
            F.col("L.TypeID"),
            F.col("L.TrackingKey"),
            F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    _log_timing("build_non_dated_entities", t0)
    return result


# ---------------------------------------------------------------------------
# build_dated_entities
# SQL lines: 3385-3420
# Row count: POSSIBLY-EMPTY (mode 4 skips this)
# ---------------------------------------------------------------------------
def build_dated_entities(
    spark: SparkSession, cfg: dict,
    input_lines: DataFrame, line_items: DataFrame,
) -> DataFrame:
    """Build #TempDatedEntities — lines with IsTransactionDate=True AND IsTransfersAdjusted=True.

    Maps each line to a quarter via:
    - PE Book + DatedTransfers: QuarterDates (StartDate/EndDate ranges) with Preference
    - Standard: ENU_DF_DataList QuarterMonth lookup

    Mode 4 skips this entirely.
    """
    t0 = time.time()
    logger.info("[SECTION] build_dated_entities")

    mode = cfg.get("mode")
    k1_lt_id = cfg["k1_line_type_id"]
    alloc_type_name = cfg.get("allocation_type_name", "")
    is_dated_transfers = cfg.get("is_dated_transfers_configured", "")
    is_pe_book_dated = (alloc_type_name == "PE Book Allocation" and is_dated_transfers == "C")

    if mode == 4:
        # Mode 4 skips dated entities — return empty DF with correct schema
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType
        schema = StructType([
            StructField("Quarter", StringType()),
            StructField("UnderlyingEntityID", IntegerType()),
            StructField("TypeID", IntegerType()),
            StructField("TrackingKey", StringType()),
            StructField("Tag", StringType()),
            StructField("IsExcludefromTransfer", BooleanType()),
            StructField("LineID", IntegerType()),
            StructField("LineTypeID", IntegerType()),
            StructField("Preference", IntegerType()),
            StructField("transferdate", DateType()),
        ])
        return spark.createDataFrame([], schema)

    if is_pe_book_dated:
        # PE Book + DatedTransfers: join K1LineItem → QuarterDates
        result = (
            input_lines.alias("L")
            .join(
                _tbl(spark, "K1LineItem", cfg).alias("K"),
                (F.col("K.LineID") == F.col("L.LineID"))
                & (F.col("K.IsTransactionDate") == True)
                & (F.col("K.IsTransfersAdjusted") == True),
            )
            .join(
                _tbl(spark, "QuarterDates", cfg).alias("D"),
                F.coalesce(F.col("K.TransactionDate"), F.lit("1900-01-01").cast("timestamp"))
                .between(F.col("D.StartDate"), F.col("D.EndDate")),
            )
            .filter(F.col("L.LineTypeID") == k1_lt_id)
            .select(
                F.coalesce(F.col("D.Quarter"), F.lit("Q0")).alias("Quarter"),
                F.col("L.UnderlyingEntityID").cast("int").alias("UnderlyingEntityID"),
                F.col("L.TypeID"),
                F.col("L.TrackingKey"),
                F.col("L.Tag"),
                F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
                F.col("K.LineID"),
                F.col("L.LineTypeID"),
                F.col("D.Preference"),
                F.lit(None).cast("date").alias("transferdate"),
            )
            .distinct()
        )
    else:
        # Standard: K1LineItem → ENU_DF_DataList QuarterMonth
        result = (
            input_lines.alias("L")
            .join(
                line_items.alias("K"),
                (F.col("K.LineID") == F.col("L.LineID"))
                & (F.col("K.IsTransactionDate") == True)
                & (F.col("K.IsTransfersAdjusted") == True)
                & (F.col("L.LineTypeID") == F.col("K.LineTypeID")),
            )
            .join(
                F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg)).alias("D"),
                (F.col("D.LookUpValue") == F.coalesce(
                    F.month(F.col("K.TransactionDate")), F.lit(0)
                ).cast("string"))
                & (F.col("D.Category") == "QuarterMonth"),
            )
            .select(
                F.col("D.LookUpData").alias("Quarter"),
                F.col("L.UnderlyingEntityID").cast("int").alias("UnderlyingEntityID"),
                F.col("L.TypeID"),
                F.col("L.TrackingKey"),
                F.col("L.Tag"),
                F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
                F.col("K.LineID"),
                F.col("L.LineTypeID"),
                F.lit(None).cast("int").alias("Preference"),
                F.lit(None).cast("date").alias("transferdate"),
            )
            .distinct()
        )

    _log_timing("build_dated_entities", t0)
    return result
