"""_summary.py — K3 rounded summary + rounding difference.

Sections:
  S6  build_k3_summary_rounded   (SQL 201-233): per-row Floor/Ceiling/Round then
      SUM by partner/line/country/attr; 6a/6b summary adjustment; K1 partner sync.
      Also returns the pre-subtraction #TEMP6a/#TEMP6b snapshots consumed by S9.
  S7  build_rounding_difference  (SQL 235-240): per line/partner K1 amount minus
      K3 rounded total.

The K3 summary (#tmpK3LookThroughCompleteAllocationSummary) is an in-memory
working set; all SQL UPDATEs on it are DataFrame transforms here.
"""

import time

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from Common_V2.core.helpers import get_logger, log_timing, sql_round as _sql_round

from ._prep import _apply_6a6b

logger = get_logger(__name__)

# Columns of #tmpK3LookThroughCompleteAllocationSummary (insert order).
_SUMMARY_COLS = [
    "RunID", "EntityID", "PartnerNumber", "LineID",
    "CountryID", "Amount", "AttributeTypeID", "AttributeID",
]


# ---------------------------------------------------------------------------
# S6: build_k3_summary_rounded  (SQL 201-233)
# ---------------------------------------------------------------------------

def build_k3_summary_rounded(
    spark: SparkSession,
    cfg: dict,
    k3_detail: DataFrame,
    rounding_flags: DataFrame,
    mapped_lines: DataFrame,
    has_mapped: bool,
    k1_amounts: DataFrame,
) -> dict:
    """Return {'summary', 'temp6a', 'temp6b'}.

    Base rounded summary (SQL 205-211):
        SUM( CASE WHEN ISNULL(KR.RoundDown,0)=0 THEN ROUND(Amount,0)
                  ELSE (CASE WHEN Amount>0 THEN FLOOR(Amount) ELSE CEILING(Amount) END) END )
        GROUP BY PartnerNumber, LineID, CountryID, AttributeTypeID, AttributeID
        LEFT JOIN rounding flags KR ON LineID, AttributeTypeID
    6a/6b summary adjustment (SQL 213-224, IF mapped): Amount = a.Amount - b.Amount
        (match on entity+partner+country). #TEMP6a/#TEMP6b snapshots are taken from
        the *base* summary (before subtraction) for reuse in S9.
    K1 partner sync (SQL 229-233): append K1 amounts for (partner, line) absent from
        the summary with nonzero amount; CountryID/AttributeTypeID/AttributeID = NULL.
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    logger.info("[S6] build_k3_summary_rounded (has_mapped=%s)", has_mapped)
    t0 = time.time()

    kr = rounding_flags.select("LineID", "AttributeTypeID", "RoundDown")

    # Per-detail-row rounded amount (RoundDown NULL/0 -> ROUND half-away-from-zero;
    # RoundDown 1 -> FLOOR for positive, CEILING for negative). LEFT JOIN mirrors SQL
    # exactly (including any KR fan-out on LineID+AttributeTypeID).
    rounded = (
        F.when(
            F.coalesce(F.col("KR.RoundDown"), F.lit(False)) == False,  # noqa: E712
            _sql_round(F.col("L.Amount"), 0).cast("double"),
        )
        .otherwise(
            F.when(F.col("L.Amount") > 0, F.floor(F.col("L.Amount")))
            .otherwise(F.ceil(F.col("L.Amount")))
            .cast("double")
        )
    )

    summary_base = (
        k3_detail.alias("L")
        .join(
            kr.alias("KR"),
            (F.col("L.LineID") == F.col("KR.LineID"))
            & (F.col("L.AttributeTypeID") == F.col("KR.AttributeTypeID")),
            "left",
        )
        .withColumn("_rounded", rounded)
        .groupBy(
            F.col("L.PartnerNumber").alias("PartnerNumber"),
            F.col("L.LineID").alias("LineID"),
            F.col("L.CountryID").alias("CountryID"),
            F.col("L.AttributeTypeID").alias("AttributeTypeID"),
            F.col("L.AttributeID").alias("AttributeID"),
        )
        .agg(F.sum("_rounded").alias("Amount"))
        .select(
            F.lit(run_id).cast("int").alias("RunID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("PartnerNumber"),
            F.col("LineID"),
            F.col("CountryID"),
            F.col("Amount"),
            F.col("AttributeTypeID"),
            F.col("AttributeID"),
        )
    )

    temp6a = None
    temp6b = None
    summary = summary_base
    if has_mapped:
        # Snapshots taken from the *base* summary (SQL 216-218), before subtraction.
        temp6b = (
            summary_base.alias("k3")
            .join(
                F.broadcast(mapped_lines.alias("k1")),
                F.col("k3.LineID") == F.col("k1.MappedK1LineID"),
                "inner",
            )
            .select("k3.*", F.col("k1.K1LineID").alias("K1LineID"))
        )
        temp6a = (
            summary_base.alias("k3")
            .join(
                F.broadcast(mapped_lines.alias("k1")),
                F.col("k3.LineID") == F.col("k1.K1LineID"),
                "inner",
            )
            .select("k3.*", F.col("k1.K1LineID").alias("K1LineID"))
        )
        # UPDATE I SET Amount = a.Amount - b.Amount (SQL 220-224).
        summary = _apply_6a6b(summary_base, mapped_lines, sign=-1, match_country=True)

    # K1 partner sync (SQL 229-233): rows present in K1 amounts but not in summary.
    sync_candidates = k1_amounts.filter(F.coalesce(F.col("Amount"), F.lit(0.0)) != 0)
    sync_rows = (
        sync_candidates.alias("TK1")
        .join(
            summary.select("PartnerNumber", "LineID", "EntityID").alias("TK3"),
            (F.col("TK1.PartnerNumber") == F.col("TK3.PartnerNumber"))
            & (F.col("TK1.LineID") == F.col("TK3.LineID"))
            & (F.col("TK1.EntityID") == F.col("TK3.EntityID")),
            "left_anti",
        )
        .select(
            F.lit(run_id).cast("int").alias("RunID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("PartnerNumber"),
            F.col("LineID"),
            F.lit(None).cast("int").alias("CountryID"),
            F.col("Amount"),
            F.lit(None).cast("int").alias("AttributeTypeID"),
            F.lit(None).cast("int").alias("AttributeID"),
        )
    )

    summary = summary.select(*_SUMMARY_COLS).unionByName(sync_rows.select(*_SUMMARY_COLS))

    log_timing("build_k3_summary_rounded", t0, logger)
    return {"summary": summary, "temp6a": temp6a, "temp6b": temp6b}


# ---------------------------------------------------------------------------
# S7: build_rounding_difference  (SQL 235-240)
# ---------------------------------------------------------------------------

def build_rounding_difference(
    spark: SparkSession, cfg: dict, k3_summary: DataFrame, k1_amounts: DataFrame
) -> DataFrame:
    """#tmpK3CompleteAllocationDetailRoundedDiff.

    TR = summary SUM(Amount) GROUP BY RunID, EntityID, PartnerNumber, LineID
    LEFT JOIN k1_amounts TA ON EntityID, PartnerNumber, LineID
    DiffAmount = ISNULL(TA.Amount,0) - ISNULL(TR.TotalAmount,0);  Amount = TotalAmount.
    """
    logger.info("[S7] build_rounding_difference")
    t0 = time.time()

    tr = k3_summary.groupBy("RunID", "EntityID", "PartnerNumber", "LineID").agg(
        F.sum("Amount").alias("TotalAmount")
    )

    diff = (
        tr.alias("TR")
        .join(
            k1_amounts.select("EntityID", "PartnerNumber", "LineID", "Amount").alias("TA"),
            (F.col("TA.EntityID") == F.col("TR.EntityID"))
            & (F.col("TA.PartnerNumber") == F.col("TR.PartnerNumber"))
            & (F.col("TA.LineID") == F.col("TR.LineID")),
            "left",
        )
        .select(
            F.col("TR.RunID").alias("RunID"),
            F.col("TR.EntityID").alias("EntityID"),
            F.col("TR.PartnerNumber").alias("PartnerNumber"),
            F.col("TR.LineID").alias("LineID"),
            (
                F.coalesce(F.col("TA.Amount"), F.lit(0.0))
                - F.coalesce(F.col("TR.TotalAmount"), F.lit(0.0))
            ).alias("DiffAmount"),
            F.col("TR.TotalAmount").alias("Amount"),
        )
    )

    log_timing("build_rounding_difference", t0, logger)
    return diff
