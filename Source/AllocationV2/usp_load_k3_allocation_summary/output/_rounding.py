"""_rounding.py — S9 standard rounding-difference plug (SQL 455-500, ELSE branch).

Plug the per-partner rounding difference to the partner's highest-amount
(Country/SIC) row. Expressed as Spark DataFrame transforms.

(The country-level branch, S8, lives in _country_rounding.py.)
"""

import time

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from Common_V2.core.helpers import read_table, get_logger, log_timing, sql_round as _sql_round

logger = get_logger(__name__)

# ENU_IncomeAttributeType.AttributeType values (SQL @Country / @SIC). Sourced with
# proper casing; compared case-insensitively via F.lower() (UTF8_BINARY collation).
_ATTR_COUNTRY = "Country"
_ATTR_SIC = "SIC"


def apply_standard_rounding(
    spark: SparkSession,
    cfg: dict,
    k3_summary: DataFrame,
    rounding_diff: DataFrame,
    rounding_flags: DataFrame,
    mapped_lines: DataFrame,
    has_mapped: bool,
    temp6a: DataFrame,
    temp6b: DataFrame,
) -> DataFrame:
    """S9 — plug the difference to the partner's highest (Country/SIC) row.

    #roundingexcludedcountries (SQL 462-467, IF mapped): (partner, line, country)
        combos where the 6a line net-of-6b would go negative — excluded from the
        highest-country pick.
    #tmpK1CompleteAllocationSummaryHighestCountryAmounts (SQL 472-482): per
        (partner, line), the highest-Amount Country/SIC row (RowNum=1), excluding
        the rounding-excluded countries for AttributeType='Country'.
    UPDATE highest with rounding import (SQL 486-488): overwrite CountryID/AttributeID.
    UPDATE summary += ROUND(DiffAmount,0) at the highest country row (SQL 492-496).

    NOTE (logic_review divergences): the SQL LEFT JOINs to ENU_CountryListImports
    and ENU_SICCodes reference no columns and are dropped (no-ops). The rounding-
    import overwrite (KR) is de-duped on (LineID, AttributeTypeID) — the T-SQL
    UPDATE is non-deterministic on multi-match.
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    logger.info("[S9] apply_standard_rounding (has_mapped=%s)", has_mapped)
    t0 = time.time()

    # ---- #roundingexcludedcountries (IF mapped) ----
    if has_mapped and temp6a is not None and temp6b is not None:
        excl = (
            temp6b.alias("b")
            .join(
                temp6a.alias("a"),
                (F.col("b.K1LineID") == F.col("a.K1LineID"))
                & (F.col("a.CountryID") == F.col("b.CountryID"))
                & (F.col("a.EntityID") == F.col("b.EntityID"))
                & (F.col("a.PartnerNumber") == F.col("b.PartnerNumber")),
                "inner",
            )
            .join(
                rounding_diff.alias("d"),
                (F.col("d.PartnerNumber") == F.col("a.PartnerNumber"))
                & (F.col("d.LineID") == F.col("a.LineID")),
                "inner",
            )
            .filter(
                (F.col("d.DiffAmount") < 0)
                & ((F.col("d.DiffAmount") + F.col("a.Amount")) < 0)
            )
            .select(
                F.col("a.PartnerNumber").alias("PartnerNumber"),
                F.col("a.LineID").alias("LineID"),
                F.col("a.CountryID").alias("CountryID"),
            )
            .distinct()
        )
    else:
        excl = None

    # ---- highest (Country/SIC) row per (partner, line) ----
    eia = (
        read_table(spark, "ENU_IncomeAttributeType", cfg)
        .select("ID", "AttributeType")
        .filter(F.lower(F.col("AttributeType")).isin(_ATTR_COUNTRY.lower(), _ATTR_SIC.lower()))
    )

    base = k3_summary.alias("L").join(
        F.broadcast(eia.alias("EIA")), F.col("EIA.ID") == F.col("L.AttributeTypeID"), "inner"
    )

    if excl is not None:
        # Drop rows matching an excluded (partner, line, country) when AttributeType='Country'.
        base = base.alias("L").join(
            excl.alias("re"),
            (F.col("L.PartnerNumber") == F.col("re.PartnerNumber"))
            & (F.col("L.LineID") == F.col("re.LineID"))
            & (F.col("L.CountryID") == F.col("re.CountryID"))
            & (F.lower(F.col("L.AttributeType")) == _ATTR_COUNTRY.lower()),
            "left_anti",
        )

    # CountryID = AttributeID when income-attr-type ID = 1 (Country); else NULL (SQL 475).
    proj = base.select(
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("AttributeID"),
        F.col("ID").alias("AttributeTypeID"),
        F.when(F.col("ID") == F.lit(1), F.col("AttributeID"))
        .otherwise(F.lit(None).cast("int"))
        .alias("CountryID"),
    )
    w = Window.partitionBy("PartnerNumber", "LineID").orderBy(
        F.col("Amount").desc(), F.col("AttributeID").asc()
    )
    highest = (
        proj.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == F.lit(1))
        .select(
            F.lit(run_id).cast("int").alias("RunID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("LineID"),
            F.col("PartnerNumber"),
            F.col("CountryID"),
            F.col("AttributeTypeID"),
            F.col("AttributeID"),
        )
    )

    # ---- overwrite highest CountryID/AttributeID from rounding import (SQL 486-488) ----
    kr = rounding_flags.select("LineID", "AttributeTypeID", "CountryID", "AttributeID").dropDuplicates(
        ["LineID", "AttributeTypeID"]
    )
    highest = (
        highest.alias("KC")
        .join(
            kr.alias("KR"),
            (F.col("KC.LineID") == F.col("KR.LineID"))
            & (F.col("KC.AttributeTypeID") == F.col("KR.AttributeTypeID")),
            "left",
        )
        .select(
            F.col("KC.RunID").alias("RunID"),
            F.col("KC.EntityID").alias("EntityID"),
            F.col("KC.LineID").alias("LineID"),
            F.col("KC.PartnerNumber").alias("PartnerNumber"),
            F.when(F.col("KR.LineID").isNotNull(), F.col("KR.CountryID"))
            .otherwise(F.col("KC.CountryID")).alias("CountryID"),
            F.col("KC.AttributeTypeID").alias("AttributeTypeID"),
            F.when(F.col("KR.LineID").isNotNull(), F.col("KR.AttributeID"))
            .otherwise(F.col("KC.AttributeID")).alias("AttributeID"),
        )
    )

    # ---- plug the difference to the highest country row (SQL 492-496) ----
    plug = (
        highest.alias("TC")
        .join(
            rounding_diff.alias("TD"),
            (F.col("TD.PartnerNumber") == F.col("TC.PartnerNumber"))
            & (F.col("TD.LineID") == F.col("TC.LineID"))
            & (F.col("TD.EntityID") == F.col("TC.EntityID")),
            "inner",
        )
        .select(
            F.col("TC.EntityID").alias("EntityID"),
            F.col("TC.PartnerNumber").alias("PartnerNumber"),
            F.col("TC.LineID").alias("LineID"),
            F.col("TC.AttributeTypeID").alias("AttributeTypeID"),
            F.col("TC.AttributeID").alias("AttributeID"),
            _sql_round(F.col("TD.DiffAmount"), 0).alias("_plug"),
        )
    )

    result = (
        k3_summary.alias("TR")
        .join(
            plug.alias("P"),
            (F.col("TR.EntityID") == F.col("P.EntityID"))
            & (F.col("TR.PartnerNumber") == F.col("P.PartnerNumber"))
            & (F.col("TR.LineID") == F.col("P.LineID"))
            & (F.col("TR.AttributeTypeID") == F.col("P.AttributeTypeID"))
            & (F.col("TR.AttributeID") == F.col("P.AttributeID")),
            "left",
        )
        .select(
            F.col("TR.RunID").alias("RunID"),
            F.col("TR.EntityID").alias("EntityID"),
            F.col("TR.PartnerNumber").alias("PartnerNumber"),
            F.col("TR.LineID").alias("LineID"),
            F.col("TR.CountryID").alias("CountryID"),
            F.when(F.col("P._plug").isNotNull(), F.col("TR.Amount") + F.col("P._plug"))
            .otherwise(F.col("TR.Amount")).alias("Amount"),
            F.col("TR.AttributeTypeID").alias("AttributeTypeID"),
            F.col("TR.AttributeID").alias("AttributeID"),
        )
    )

    log_timing("apply_standard_rounding", t0, logger)
    return result
