"""_finalize.py — S10 post-branch finalization (SQL 502-541).

  1. 6a/6b "after" adjustment (SQL 502-523, IF mapped): Amount = a.Amount + b.Amount.
  2. Attribute/Country backfill (SQL 525-541, rows with AttributeTypeID & CountryID
     both NULL/0):
       a. overwrite from the rounding-import flags matched on (LineID, EntityID),
          only when @IncomeAttrImportTransID <> 0;
       b. then, for rows still NULL/0, copy Country/AttributeType/Attribute from the
          highest-Amount row of that (LineID, EntityID) (maxValbyLine, rn=1).

Backfill row-level WHERE conditions make the transforms no-ops when no NULL/0 rows
exist, so the SQL `IF EXISTS` guard is folded in (no separate existence action).
"""

import time

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from Common_V2.core.helpers import get_logger, log_timing

from ._prep import _apply_6a6b

logger = get_logger(__name__)

_SUMMARY_COLS = [
    "RunID", "EntityID", "PartnerNumber", "LineID",
    "CountryID", "Amount", "AttributeTypeID", "AttributeID",
]


def finalize_summary(
    spark: SparkSession,
    cfg: dict,
    k3_summary: DataFrame,
    rounding_flags: DataFrame,
    mapped_lines: DataFrame,
    has_mapped: bool,
) -> DataFrame:
    """S10 — 6a/6b 'after' adjustment then attribute/country backfill."""
    trans_id = cfg.get("income_attr_import_trans_id")
    logger.info("[S10] finalize_summary (has_mapped=%s, trans_id=%s)", has_mapped, trans_id)
    t0 = time.time()

    summary = k3_summary.select(*_SUMMARY_COLS)

    # ---- 6a/6b "after" adjustment (SQL 505-513): Amount = a.Amount + b.Amount ----
    if has_mapped:
        summary = _apply_6a6b(summary, mapped_lines, sign=1, match_country=True)

    # ---- Backfill step (a): from rounding-import flags on (LineID, EntityID) ----
    # Only when @IncomeAttrImportTransID <> 0 (SQL 529). KR de-duped on (LineID,
    # EntityID) — row_number picks the specific-import row (non-NULL AT) over the
    # -1 fallback, matching SQL Server hash-join insert-order behavior.
    if trans_id not in (None, 0):
        _w = Window.partitionBy("LineID", "EntityID").orderBy(
            F.when(F.coalesce(F.col("AttributeTypeID"), F.lit(0)) != 0, 0).otherwise(1).asc()
        )
        kr = (rounding_flags
              .select("LineID", "EntityID", "CountryID", "AttributeID", "AttributeTypeID")
              .withColumn("_rn", F.row_number().over(_w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))

        kc_zero = (F.coalesce(F.col("KC.AttributeTypeID"), F.lit(0)) == 0) & (
            F.coalesce(F.col("KC.CountryID"), F.lit(0)) == 0
        )
        do_update = kc_zero & F.col("KR.LineID").isNotNull()

        summary = (
            summary.alias("KC")
            .join(
                kr.alias("KR"),
                (F.col("KC.LineID") == F.col("KR.LineID"))
                & (F.col("KC.EntityID") == F.col("KR.EntityID")),
                "left",
            )
            .select(
                F.col("KC.RunID").alias("RunID"),
                F.col("KC.EntityID").alias("EntityID"),
                F.col("KC.PartnerNumber").alias("PartnerNumber"),
                F.col("KC.LineID").alias("LineID"),
                F.when(do_update, F.col("KR.CountryID")).otherwise(F.col("KC.CountryID")).alias("CountryID"),
                F.col("KC.Amount").alias("Amount"),
                F.when(do_update, F.col("KR.AttributeTypeID")).otherwise(F.col("KC.AttributeTypeID")).alias("AttributeTypeID"),
                F.when(do_update, F.col("KR.AttributeID")).otherwise(F.col("KC.AttributeID")).alias("AttributeID"),
            )
        )

    # ---- Backfill step (b): maxValbyLine (SQL 531-540) ----
    # For rows still NULL/0, copy Country/AttributeType/Attribute from the highest-
    # Amount row of the same (LineID, EntityID).
    w = Window.partitionBy("LineID", "EntityID").orderBy(F.col("Amount").desc())
    max_row = (
        summary.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == F.lit(1))
        .select(
            F.col("LineID").alias("_mLineID"),
            F.col("EntityID").alias("_mEntityID"),
            F.col("CountryID").alias("_mCountryID"),
            F.col("AttributeTypeID").alias("_mAttrType"),
            F.col("AttributeID").alias("_mAttrID"),
        )
    )

    t_zero = (F.coalesce(F.col("T.AttributeTypeID"), F.lit(0)) == 0) & (
        F.coalesce(F.col("T.CountryID"), F.lit(0)) == 0
    )
    summary = (
        summary.alias("T")
        .join(
            max_row.alias("mv"),
            (F.col("T.LineID") == F.col("mv._mLineID"))
            & (F.col("T.EntityID") == F.col("mv._mEntityID")),
            "left",
        )
        .select(
            F.col("T.RunID").alias("RunID"),
            F.col("T.EntityID").alias("EntityID"),
            F.col("T.PartnerNumber").alias("PartnerNumber"),
            F.col("T.LineID").alias("LineID"),
            F.when(t_zero, F.col("mv._mCountryID")).otherwise(F.col("T.CountryID")).alias("CountryID"),
            F.col("T.Amount").alias("Amount"),
            F.when(t_zero, F.col("mv._mAttrType")).otherwise(F.col("T.AttributeTypeID")).alias("AttributeTypeID"),
            F.when(t_zero, F.col("mv._mAttrID")).otherwise(F.col("T.AttributeID")).alias("AttributeID"),
        )
    )

    log_timing("finalize_summary", t0, logger)
    return summary.select(*_SUMMARY_COLS)
