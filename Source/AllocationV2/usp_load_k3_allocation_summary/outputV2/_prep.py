"""_prep.py — input preparation for uspLoadK3AllocationSummary.

Sections (see _plan.md / logic_review.md):
  S2  build_country_sic_lines            (SQL 85-103; inlines uspGetK1ForeignLines)
  S3  build_income_attr_rounding_import  (SQL 125-135)
  S4a build_k3_detail                    (SQL 137-140)
  S4b build_rounding_flags               (SQL 143-159)
  S5a build_mapped_lines                 (SQL 183-185)
  S5b build_k1_summary_amounts           (SQL 161-199; incl. 6a/6b K1 adjustment)

Shared helper:
  _apply_6a6b  — 6a/6b look-through mapped-line adjustment used by S5/S6/S10.

All temp-table UPDATEs in the SP target in-memory working sets, so they are
DataFrame transforms here (not Delta merges). AttributeTypeID/AttributeID/
CountryID/LineID are kept as int throughout; the SQL round-trips some of them
through varchar(4) temp columns (lossless implicit conversion) — see logic_review.
"""

import time

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from Common_V2.core.helpers import read_table, get_logger, log_timing

logger = get_logger(__name__)

# K3 attribute types that flag a K1 line as "foreign" (uspGetK1ForeignLines).
_K3_FOREIGN_ATTRS = ("k3 - country attribute", "k3 - sic attribute")


# ---------------------------------------------------------------------------
# Shared: 6a/6b mapped-line adjustment
# ---------------------------------------------------------------------------

def _apply_6a6b(df: DataFrame, mapped: DataFrame, sign: int, match_country: bool) -> DataFrame:
    """Apply the K3 6a/6b mapped-line amount adjustment.

    Mirrors the T-SQL pattern (SQL 189-198 / 216-224 / 505-513):
        #TEMP6b = df JOIN mapped ON df.LineID = mapped.MappedK1LineID   (the "6b" rows)
        #TEMP6a = df JOIN mapped ON df.LineID = mapped.K1LineID         (the "6a" rows)
        UPDATE I SET I.Amount = a.Amount (sign) b.Amount
        FROM #TEMP6b b JOIN #TEMP6a a ON b.k1lineid=a.k1lineid [AND a.CountryID=b.CountryID]
                                     AND a.EntityID=b.EntityID AND a.PartnerNumber=b.PartnerNumber
        JOIN df I ON a.LineID=i.LineID [AND a.CountryID=i.CountryID]
                  AND a.PartnerNumber=I.PartnerNumber AND a.EntityID=I.EntityID

    Because a.LineID = mapped.K1LineID = a.k1lineid and (a) joins (I) on the same
    key, a == I; the target row's own Amount is a.Amount. So for every target row
    whose LineID is a K1LineID that has a mapped 6b line (b), the new amount is
    `t.Amount (sign) b.Amount`.

    sign:          -1 for the pre-branch adjustments, +1 for the post-branch ("after").
    match_country: True adds CountryID to the b↔target match (summary-level);
                   False for the K1-amounts level (no CountryID column).

    NOTE (logic_review divergence): a K1LineID mapping to multiple MappedK1LineIDs
    makes the T-SQL UPDATE non-deterministic (it applies one arbitrary match).
    We de-dup `b` on the join key for a deterministic, single-match result.
    """
    out_cols = df.columns

    b_sel = [
        F.col("m.K1LineID").alias("_k1lineid"),
        F.col("k3.EntityID").alias("_bEntity"),
        F.col("k3.PartnerNumber").alias("_bPartner"),
        F.col("k3.Amount").alias("_bAmount"),
    ]
    if match_country:
        b_sel.append(F.col("k3.CountryID").alias("_bCountry"))

    b = (
        df.alias("k3")
        .join(
            F.broadcast(mapped.select("K1LineID", "MappedK1LineID").alias("m")),
            F.col("k3.LineID") == F.col("m.MappedK1LineID"),
            "inner",
        )
        .select(*b_sel)
    )

    dedup_keys = ["_k1lineid", "_bEntity", "_bPartner"] + (["_bCountry"] if match_country else [])
    b = b.dropDuplicates(dedup_keys)

    cond = (
        (F.col("t.LineID") == F.col("b._k1lineid"))
        & (F.col("t.EntityID") == F.col("b._bEntity"))
        & (F.col("t.PartnerNumber") == F.col("b._bPartner"))
    )
    if match_country:
        cond = cond & (F.col("t.CountryID") == F.col("b._bCountry"))

    joined = df.alias("t").join(b.alias("b"), cond, "left")
    adjusted = joined.withColumn(
        "_newAmount",
        F.when(
            F.col("b._bAmount").isNotNull(),
            F.col("t.Amount") + F.lit(sign) * F.col("b._bAmount"),
        ).otherwise(F.col("t.Amount")),
    )
    return adjusted.select(
        *[
            (F.col("_newAmount").alias("Amount") if c == "Amount" else F.col(f"t.{c}"))
            for c in out_cols
        ]
    )


# ---------------------------------------------------------------------------
# S2: build_country_sic_lines  (SQL 85-103)
# ---------------------------------------------------------------------------

def build_country_sic_lines(spark: SparkSession, cfg: dict) -> DataFrame:
    """#CountryANDSICLines — distinct (LineID, K3AttributeTypeValue).

    #tmpLookThroughAllocationOutput : DISTINCT LineID from LookThroughAllocationOutput
                                      WHERE RunID AND LineTypeID = K1
    #ForeignLines_temp (inline uspGetK1ForeignLines):
        K1LineItem (ClientID, TaxPeriodID, IsVisible=1) JOIN ENU_K3Attribute
        WHERE K3AttributeType IN ('K3 - Country Attribute','K3 - SIC Attribute')
    #CountryANDSICLines:
        #ForeignLines_temp JOIN K1LineItem JOIN ENU_K3Attribute JOIN #tmpLookThroughAllocationOutput
    """
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    k1_line_type_id = cfg["k1_line_type_id"]
    logger.info("[S2] build_country_sic_lines")
    t0 = time.time()

    lto_lines = (
        read_table(spark, "LookThroughAllocationOutput", cfg)
        .filter((F.col("RunID") == run_id) & (F.col("LineTypeID") == k1_line_type_id))
        .select("LineID")
        .distinct()
    )

    k3attr = (
        read_table(spark, "ENU_K3Attribute", cfg)
        .select("K3AttributeTypeID", "K3AttributeType")
        .filter(F.lower(F.col("K3AttributeType")).isin(*_K3_FOREIGN_ATTRS))
    )

    k1li = (
        read_table(spark, "K1LineItem", cfg)
        .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
        .select("LineID", "K3AttributeTypeID", "IsVisible")
    )

    # #ForeignLines_temp — visible K1 lines carrying a K3 country/SIC attribute.
    foreign_lines = (
        k1li.filter(F.col("IsVisible") == True)  # noqa: E712
        .join(F.broadcast(k3attr), "K3AttributeTypeID", "inner")
        .select("LineID")
    )

    # #CountryANDSICLines — foreign lines re-joined to K1LineItem + ENU_K3Attribute,
    # restricted to lines present in the run's LookThroughAllocationOutput.
    country_sic = (
        foreign_lines.alias("K")
        .join(k1li.alias("KL"), F.col("K.LineID") == F.col("KL.LineID"), "inner")
        .join(
            F.broadcast(k3attr.alias("K3")),
            F.col("KL.K3AttributeTypeID") == F.col("K3.K3AttributeTypeID"),
            "inner",
        )
        .join(lto_lines.alias("LA"), F.col("K.LineID") == F.col("LA.LineID"), "inner")
        .select(
            F.col("K.LineID").alias("LineID"),
            F.col("K3.K3AttributeType").alias("K3AttributeTypeValue"),
        )
        .distinct()
    )

    log_timing("build_country_sic_lines", t0, logger)
    return country_sic


# ---------------------------------------------------------------------------
# S3: build_income_attr_rounding_import  (SQL 125-135)
# ---------------------------------------------------------------------------

def build_income_attr_rounding_import(spark: SparkSession, cfg: dict) -> DataFrame:
    """#tmpIncomeAttributeRoundingImport — base import ∪ offset-derived child lines.

    Base   : IncomeAttributeRounding WHERE TransactionID = @IncomeAttrImportTransID
    Offset : base JOIN MAP_DerivedLines (BaseLineID=base.LineID, DerivedLineID NOT NULL)
                  JOIN ENU_AttributeType (AttributeID=MD.AttributeID,
                                          AttributeType='Offset', ISNULL(IsHidden,0)=0)
             emitting MD.DerivedLineID as LineID, base's other columns.

    Columns: EntityID, LineID, CountryID, AttributeTypeID, AttributeID, RoundDown.
    """
    trans_id = cfg.get("income_attr_import_trans_id")
    logger.info("[S3] build_income_attr_rounding_import (trans_id=%s)", trans_id)
    t0 = time.time()

    base = (
        read_table(spark, "IncomeAttributeRounding", cfg)
        .filter(F.col("TransactionID") == F.lit(trans_id))
        .select("EntityID", "LineID", "CountryID", "AttributeTypeID", "AttributeID", "RoundDown")
    )

    md = (
        read_table(spark, "MAP_DerivedLines", cfg)
        .filter(F.col("DerivedLineID").isNotNull())
        .select("BaseLineID", "DerivedLineID", "AttributeID")
    )
    offset_attr = (
        read_table(spark, "ENU_AttributeType", cfg)
        .filter(
            (F.lower(F.col("AttributeType")) == "offset")
            & (F.coalesce(F.col("IsHidden"), F.lit(False)) == False)  # noqa: E712
        )
        .select("AttributeID")
    )

    offset = (
        base.alias("I")
        .join(F.broadcast(md.alias("MD")), F.col("MD.BaseLineID") == F.col("I.LineID"), "inner")
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

    result = base.unionByName(offset)
    log_timing("build_income_attr_rounding_import", t0, logger)
    return result


# ---------------------------------------------------------------------------
# S4a: build_k3_detail  (SQL 137-140)
# ---------------------------------------------------------------------------

def build_k3_detail(spark: SparkSession, cfg: dict) -> DataFrame:
    """#K3LookThroughCompleteAllocationDetail — WHERE RunID (column-pruned).

    The SQL selects 19 columns into the temp table but only 7 are consumed
    downstream (EntityID, LineID, PartnerNumber, CountryID, Amount,
    AttributeTypeID, AttributeID); the rest are pruned (see logic_review).
    """
    run_id = cfg["run_id"]
    logger.info("[S4a] build_k3_detail")
    t0 = time.time()

    df = (
        read_table(spark, "K3LookThroughCompleteAllocationDetail", cfg)
        .filter(F.col("RunID") == run_id)
        .select(
            "EntityID", "LineID", "PartnerNumber", "CountryID",
            "Amount", "AttributeTypeID", "AttributeID",
        )
    )
    log_timing("build_k3_detail", t0, logger)
    return df


# ---------------------------------------------------------------------------
# S4b: build_rounding_flags  (SQL 143-159)
# ---------------------------------------------------------------------------

def build_rounding_flags(
    spark: SparkSession, cfg: dict, k3_detail: DataFrame, income_attr_import: DataFrame
) -> DataFrame:
    """#tmpK3LookThroughCompleteAllocationRounding.

    Insert 1 (specific lines, SQL 143-146):
        DISTINCT @EntityID, L.LineID, IA.(Country/AttrType/Attr/RoundDown)
        FROM detail L LEFT JOIN import IA ON L.EntityID=IA.EntityID AND L.LineID=IA.LineID
                                         AND IA.LineID <> -1
    Insert 2 (-1 fallback, SQL 150-158):
        DISTINCT @EntityID, L.LineID, IA.(...), ISNULL(RoundDown,0)
        FROM detail L LEFT JOIN import IA ON IA.LineID = -1   (uncorrelated cross)
        anti-join Insert-1 on (EntityID, LineID, AttributeTypeID)
    """
    entity_id = cfg["entity_id"]
    logger.info("[S4b] build_rounding_flags")
    t0 = time.time()

    detail_lines = k3_detail.select("EntityID", "LineID").distinct()

    ia_specific = income_attr_import.filter(F.col("LineID") != -1)
    ins1 = (
        detail_lines.alias("L")
        .join(
            ia_specific.alias("IA"),
            (F.col("L.EntityID") == F.col("IA.EntityID"))
            & (F.col("L.LineID") == F.col("IA.LineID")),
            "left",
        )
        .select(
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("L.LineID").alias("LineID"),
            F.col("IA.CountryID").alias("CountryID"),
            F.col("IA.AttributeTypeID").alias("AttributeTypeID"),
            F.col("IA.AttributeID").alias("AttributeID"),
            F.col("IA.RoundDown").alias("RoundDown"),
        )
        .distinct()
    )

    ia_minus1 = income_attr_import.filter(F.col("LineID") == -1)
    # LEFT JOIN ... ON IA.LineID = -1 is uncorrelated: each detail line paired with
    # every -1 import row, else a single NULL row (LEFT-outer). ia_minus1 is tiny.
    res = (
        detail_lines.select("LineID").distinct().alias("L")
        .join(F.broadcast(ia_minus1.alias("IA")), F.col("IA.LineID") == F.lit(-1), "left")
        .select(
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("L.LineID").alias("LineID"),
            F.col("IA.CountryID").alias("CountryID"),
            F.col("IA.AttributeTypeID").alias("AttributeTypeID"),
            F.col("IA.AttributeID").alias("AttributeID"),
            F.coalesce(F.col("IA.RoundDown"), F.lit(False)).alias("RoundDown"),
        )
        .distinct()
    )

    # WHERE KR.LineID IS NULL  ->  left-anti. Use == (not <=>) so NULL AttributeTypeID
    # never matches (mirrors SQL `RES.AttributeTypeID = KR.AttributeTypeID` NULL semantics).
    ins2 = (
        res.alias("RES")
        .join(
            ins1.alias("KR"),
            (F.col("RES.EntityID") == F.col("KR.EntityID"))
            & (F.col("RES.LineID") == F.col("KR.LineID"))
            & (F.col("RES.AttributeTypeID") == F.col("KR.AttributeTypeID")),
            "left_anti",
        )
        .select("EntityID", "LineID", "CountryID", "AttributeTypeID", "AttributeID", "RoundDown")
    )

    result = ins1.unionByName(ins2)
    log_timing("build_rounding_flags", t0, logger)
    return result


# ---------------------------------------------------------------------------
# S5a: build_mapped_lines  (SQL 183-185)
# ---------------------------------------------------------------------------

def build_mapped_lines(spark: SparkSession, cfg: dict) -> DataFrame:
    """#K3MappedAllocableLines — DISTINCT (ClientID, TaxPeriodID, K1LineID, MappedK1LineID)
    from K3MappedAllocableLinesDetail WHERE RunID."""
    run_id = cfg["run_id"]
    logger.info("[S5a] build_mapped_lines")
    t0 = time.time()
    df = (
        read_table(spark, "K3MappedAllocableLinesDetail", cfg)
        .filter(F.col("RunID") == run_id)
        .select("ClientID", "TaxPeriodID", "K1LineID", "MappedK1LineID")
        .distinct()
    )
    log_timing("build_mapped_lines", t0, logger)
    return df


def has_mapped_lines(cfg: dict, mapped_lines: DataFrame) -> bool:
    """SQL IF EXISTS (#K3MappedAllocableLines WHERE ClientID=@C AND TaxPeriodID=@TP)."""
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    guard = mapped_lines.filter(
        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
    )
    return len(guard.limit(1).head(1)) > 0


# ---------------------------------------------------------------------------
# S5b: build_k1_summary_amounts  (SQL 161-199)
# ---------------------------------------------------------------------------

def build_k1_summary_amounts(
    spark: SparkSession,
    cfg: dict,
    country_sic: DataFrame,
    mapped_lines: DataFrame,
    has_mapped: bool,
) -> DataFrame:
    """#tmpK1LookthroughAllocationSummaryAmounts (after 6a/6b K1 adjustment).

    #tmpK1AllocationSummary : K1AllocationSummary (RunID) JOIN #CountryANDSICLines (LineID)
    aggregate              : SUM(Amount) GROUP BY PartnerNumber, LineID
                             RunID/EntityID stamped as @LocalRunID/@LocalEntityID
    6a/6b K1 adjustment    : IF mapped exists, Amount = a.Amount - b.Amount
                             (match on entity+partner, no country).

    Columns: RunID, EntityID, PartnerNumber, LineID, Amount.
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    logger.info("[S5b] build_k1_summary_amounts (has_mapped=%s)", has_mapped)
    t0 = time.time()

    k1_rows = (
        read_table(spark, "K1AllocationSummary", cfg)
        .filter(F.col("RunID") == run_id)
        .select("PartnerNumber", "LineID", "Amount")
        .join(
            F.broadcast(country_sic.select("LineID").distinct()),
            "LineID",
            "inner",
        )
    )

    k1_amounts = (
        k1_rows.groupBy("PartnerNumber", "LineID")
        .agg(F.sum("Amount").alias("Amount"))
        .select(
            F.lit(run_id).cast("int").alias("RunID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("PartnerNumber"),
            F.col("LineID"),
            F.col("Amount"),
        )
    )

    if has_mapped:
        k1_amounts = _apply_6a6b(k1_amounts, mapped_lines, sign=-1, match_country=False)

    log_timing("build_k1_summary_amounts", t0, logger)
    return k1_amounts
