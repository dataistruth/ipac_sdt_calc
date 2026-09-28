"""_country_rounding.py — S8 country-level rounding (SQL 242-454).

Runs when @IsCountryLevelRounding='C'. The T-SQL block is an order-dependent
WHILE loop: each iteration re-reads the running summary to detect tied
partners, so it cannot be naively vectorized into a single transform -- rank
r's tied-partner set depends on the MUTATED result of ranks 1..r-1, a genuine
fixed-point dependency.

Pure PySpark: every row-level operation is a DataFrame join/filter/groupBy/
Window function, never a `.collect()` into Python objects and never pandas.
The loop structure itself remains (the cross-iteration dependency doesn't
disappear just because it's expressed in DataFrame ops), so each iteration
Delta-checkpoints its updated summary (Common_V2/core/checkpoint.py) before
the next iteration's groupBy reads it -- NOT .cache()/localCheckpoint(), both
confirmed unreliable/unsupported on Databricks Serverless.
"""

import time

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from Common_V2.core.helpers import get_logger, log_timing, sql_round as _sql_round
from Common_V2.core.checkpoint import checkpoint

from ._prep import _apply_6a6b

logger = get_logger(__name__)

_SUMMARY_COLS = [
    "RunID", "EntityID", "PartnerNumber", "LineID",
    "CountryID", "Amount", "AttributeTypeID", "AttributeID",
]


def apply_country_level_rounding(
    spark: SparkSession,
    cfg: dict,
    k3_summary: DataFrame,
    k3_detail: DataFrame,
    rounding_diff: DataFrame,
    k1_amounts: DataFrame,
    mapped_lines: DataFrame,
    has_mapped: bool,
) -> DataFrame:
    """S8 — country-level rounding. Returns the updated summary."""
    logger.info("[S8] apply_country_level_rounding (has_mapped=%s)", has_mapped)
    t0 = time.time()

    # #TempK3LookThroughCompleteAllocationDetail (SQL 254-257) + optional 6a/6b.
    temp_detail = k3_detail.groupBy(
        "EntityID", "PartnerNumber", "LineID", "CountryID", "AttributeTypeID", "AttributeID"
    ).agg(F.sum("Amount").alias("Amount"))
    if has_mapped:
        temp_detail = _apply_6a6b(temp_detail, mapped_lines, sign=-1, match_country=True)

    # #TotalLineAmountsByCountry (SQL 278-280).
    total_by_country = temp_detail.groupBy("LineID", "AttributeTypeID", "AttributeID").agg(
        _sql_round(F.sum("Amount"), 0).alias("TotalAmount")
    )
    total_by_country = checkpoint(spark, total_by_country, "k3sp_total_by_country", cfg)

    diff_rows = rounding_diff.select("EntityID", "PartnerNumber", "LineID", "DiffAmount", "Amount")
    k1 = k1_amounts.select(
        "EntityID", "PartnerNumber", "LineID", F.col("Amount").alias("K1Amount")
    )
    diff_partners = diff_rows.select("EntityID", "PartnerNumber", "LineID").distinct()

    summary = k3_summary.select(*_SUMMARY_COLS)

    # #RoundedLineAmountsByCountry (SQL 283-285): SUM(summary) by line/attr.
    rounded_by_country = summary.groupBy("LineID", "AttributeTypeID", "AttributeID").agg(
        F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("RoundedAmount")
    )

    # #RoundingDiffByCountry (SQL 288-292): total - rounded, keep <> 0.
    diff_by_country = (
        rounded_by_country.alias("RA")
        .join(
            F.broadcast(total_by_country).alias("TA"),
            (F.col("RA.LineID") == F.col("TA.LineID"))
            & (F.col("RA.AttributeTypeID") == F.col("TA.AttributeTypeID"))
            & (F.col("RA.AttributeID") == F.col("TA.AttributeID")),
            "inner",
        )
        .select(
            F.col("RA.LineID").alias("LineID"),
            F.col("RA.AttributeTypeID").alias("AttributeTypeID"),
            F.col("RA.AttributeID").alias("AttributeID"),
            F.coalesce(F.col("RA.RoundedAmount"), F.lit(0.0)).alias("Amount"),
            (
                F.coalesce(F.col("TA.TotalAmount"), F.lit(0.0))
                - F.coalesce(F.col("RA.RoundedAmount"), F.lit(0.0))
            ).alias("DiffAmount"),
        )
        .filter(F.col("DiffAmount") != 0)
    )

    # #CountryAmountsRank (SQL 295-297): rank countries within each line.
    w_rank = Window.partitionBy("LineID").orderBy(
        F.when(F.col("DiffAmount") < 0, F.col("DiffAmount")).otherwise(F.lit(1.0)).asc(),
        F.col("Amount").desc(),
        F.col("AttributeTypeID").asc(),
        F.col("AttributeID").asc(),
    )
    rank_by_line = diff_by_country.withColumn("Rnk", F.row_number().over(w_rank))
    rank_by_line = checkpoint(spark, rank_by_line, "k3sp_rank_by_line", cfg)

    max_rank_row = rank_by_line.agg(F.max("Rnk").alias("m")).first()
    max_rank = max_rank_row["m"] if max_rank_row and max_rank_row["m"] is not None else 0

    # ---- WHILE @MinCountryRnk <= @MaxCountryRnk (SQL 307-409) ----
    for r in range(1, max_rank + 1):
        current_country = rank_by_line.filter(F.col("Rnk") == r).select(
            "LineID", "AttributeTypeID", "AttributeID", "DiffAmount"
        )

        # #AggTotalAmounts (SQL 326-328): SUM(current summary Amount) by
        # (Entity, Partner, Line) -- MUST read the MUTATED state from prior ranks.
        agg = summary.groupBy("EntityID", "PartnerNumber", "LineID").agg(
            F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("TotalAmount")
        )

        # #TiedPartners (SQL 330-336): current total == K1 target, and has a diff row.
        tied = (
            agg.alias("TR")
            .join(
                F.broadcast(k1).alias("TA"),
                (F.col("TR.EntityID") == F.col("TA.EntityID"))
                & (F.col("TR.PartnerNumber") == F.col("TA.PartnerNumber"))
                & (F.col("TR.LineID") == F.col("TA.LineID")),
                "left",
            )
            .join(
                F.broadcast(diff_partners).alias("PR"),
                (F.col("TR.LineID") == F.col("PR.LineID"))
                & (F.col("TR.PartnerNumber") == F.col("PR.PartnerNumber")),
                "inner",
            )
            .filter((F.col("TA.K1Amount") - F.col("TR.TotalAmount")) == 0)
            .select(F.col("TR.LineID").alias("LineID"), F.col("TR.PartnerNumber").alias("PartnerNumber"))
        )

        # #CurrentPartnerAmountsRank candidates (SQL 339-347): sign-matched, tied-excluded.
        cand = (
            diff_rows.alias("P")
            .join(F.broadcast(current_country).alias("CA"), F.col("P.LineID") == F.col("CA.LineID"), "inner")
            .join(
                F.broadcast(tied).alias("T"),
                (F.col("P.LineID") == F.col("T.LineID")) & (F.col("P.PartnerNumber") == F.col("T.PartnerNumber")),
                "left_anti",
            )
            .filter(
                ((F.col("CA.DiffAmount") > 0) & (F.col("P.DiffAmount") > 0))
                | ((F.col("CA.DiffAmount") < 0) & (F.col("P.DiffAmount") < 0))
            )
            .select(
                F.col("P.LineID").alias("LineID"),
                F.col("P.PartnerNumber").alias("PartnerNumber"),
                F.col("P.Amount").alias("Amount"),
                F.col("CA.AttributeTypeID").alias("AttributeTypeID"),
                F.col("CA.AttributeID").alias("AttributeID"),
            )
        )

        # Fallback (SQL 350-361): lines with a nonzero country diff but NO candidate
        # above (all tied) -> rank ALL partners for that line instead.
        cand_lines = cand.select("LineID").distinct()
        needs_fallback = (
            current_country.filter(F.col("DiffAmount") != 0)
            .select("LineID", "AttributeTypeID", "AttributeID")
            .join(F.broadcast(cand_lines), "LineID", "left_anti")
        )
        fallback = (
            diff_rows.alias("P")
            .join(F.broadcast(needs_fallback).alias("CA"), F.col("P.LineID") == F.col("CA.LineID"), "inner")
            .select(
                F.col("P.LineID").alias("LineID"),
                F.col("P.PartnerNumber").alias("PartnerNumber"),
                F.col("P.Amount").alias("Amount"),
                F.col("CA.AttributeTypeID").alias("AttributeTypeID"),
                F.col("CA.AttributeID").alias("AttributeID"),
            )
        )
        cpr = cand.unionByName(fallback)

        # #PartnerCountByLine (SQL 363-365).
        partner_count = cpr.groupBy("LineID").agg(F.count(F.lit(1)).alias("PartnerCount"))

        # #LineModAmounts (SQL 368-373) + base RoundingAmount (SQL 376-378).
        # T-SQL % keeps the sign of the dividend, matching Spark SQL's % directly
        # (unlike Python's builtin %, which keeps the sign of the divisor).
        line_calc = (
            current_country.alias("CA")
            .join(F.broadcast(partner_count).alias("PC"), "LineID", "inner")
            .select(
                F.col("LineID").alias("LineID"),
                (F.col("CA.DiffAmount") % F.col("PC.PartnerCount")).cast("long").alias("ModAmount"),
                (F.col("CA.DiffAmount") / F.col("PC.PartnerCount")).cast("long").alias("BaseRounding"),
            )
        )

        cpr_calc = cpr.join(F.broadcast(line_calc), "LineID", "inner")

        # Distribute the modulo remainder (SQL 380-391): +1/-1 to the top |mod|
        # partners ordered by ABS(Amount) DESC, PartnerNumber ASC.
        w_mod = Window.partitionBy("LineID").orderBy(
            F.abs(F.coalesce(F.col("Amount"), F.lit(0.0))).desc(), F.col("PartnerNumber").asc()
        )
        cpr_final = (
            cpr_calc.withColumn("_ord", F.row_number().over(w_mod))
            .withColumn(
                "RoundingAmount",
                F.col("BaseRounding")
                + F.when(
                    (F.col("ModAmount") != 0) & (F.col("_ord") <= F.abs(F.col("ModAmount"))),
                    F.when(F.col("ModAmount") > 0, F.lit(1)).otherwise(F.lit(-1)),
                ).otherwise(F.lit(0)),
            )
            .select("LineID", "PartnerNumber", "AttributeTypeID", "AttributeID", "RoundingAmount")
        )

        # UPDATE summary Amount += RoundingAmount (SQL 394-397), matched on the
        # SUMMARY ROW'S OWN (PartnerNumber, LineID, AttributeTypeID, AttributeID)
        # -- i.e. the row belonging to the country currently at rank r.
        summary = (
            summary.alias("TR")
            .join(
                F.broadcast(cpr_final).alias("TD"),
                (F.col("TR.PartnerNumber") == F.col("TD.PartnerNumber"))
                & (F.col("TR.LineID") == F.col("TD.LineID"))
                & (F.col("TR.AttributeTypeID") == F.col("TD.AttributeTypeID"))
                & (F.col("TR.AttributeID") == F.col("TD.AttributeID")),
                "left",
            )
            .select(
                *[
                    (F.col("TR.Amount") + F.coalesce(F.col("TD.RoundingAmount"), F.lit(0))).alias("Amount")
                    if c == "Amount" else F.col(f"TR.{c}").alias(c)
                    for c in _SUMMARY_COLS
                ]
            )
        )
        summary = checkpoint(spark, summary, f"k3sp_rank{r}_summary", cfg)

    # ---- Post-loop: plug residual per-partner diff to highest income country ----
    # #AggregatedTotalAmounts (SQL 414-417) + #DiffAmounts (SQL 421-424).
    agg2 = summary.groupBy("EntityID", "PartnerNumber", "LineID").agg(
        F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("TotalAmount")
    )
    diff_amounts = (
        agg2.alias("TR")
        .join(
            F.broadcast(k1).alias("TA"),
            (F.col("TR.EntityID") == F.col("TA.EntityID"))
            & (F.col("TR.PartnerNumber") == F.col("TA.PartnerNumber"))
            & (F.col("TR.LineID") == F.col("TA.LineID")),
            "left",
        )
        .select(
            F.col("TR.PartnerNumber").alias("PartnerNumber"),
            F.col("TR.LineID").alias("LineID"),
            (
                F.coalesce(F.col("TA.K1Amount"), F.lit(0.0))
                - F.coalesce(F.col("TR.TotalAmount"), F.lit(0.0))
            ).alias("Diff"),
        )
        .filter(F.col("Diff") != 0)
    )

    # #PartnerCountByCountry (SQL 426-428): count summary rows by line/attr.
    pcbc = summary.groupBy("LineID", "AttributeTypeID", "AttributeID").agg(
        F.count(F.lit(1)).alias("PartnerCount")
    )

    # #HighestIncomeCountry (SQL 432-438): PartnerCount DESC, Amount DESC — the SQL
    # has no explicit tiebreaker here (T-SQL's hash-aggregate order is effectively
    # arbitrary when counts/amounts tie exactly -- see qa_findings.md § F2 for the
    # full analysis and the 22-row real-data divergence this causes on entities
    # with a genuine 4-way tie). AttributeID ASC below is the deterministic
    # tiebreaker this port uses; it cannot reproduce T-SQL's arbitrary pick in that
    # tied case, but is fully faithful otherwise.
    cand_by_line = (
        total_by_country.alias("TA")
        .join(
            F.broadcast(pcbc).alias("PC"),
            (F.col("TA.LineID") == F.col("PC.LineID"))
            & (F.col("TA.AttributeTypeID") == F.col("PC.AttributeTypeID"))
            & (F.col("TA.AttributeID") == F.col("PC.AttributeID")),
            "inner",
        )
        .select(
            F.col("TA.LineID").alias("LineID"),
            F.col("TA.AttributeTypeID").alias("AttributeTypeID"),
            F.col("TA.AttributeID").alias("AttributeID"),
            F.coalesce(F.col("TA.TotalAmount"), F.lit(0.0)).alias("TotalAmount"),
            F.col("PC.PartnerCount").alias("PartnerCount"),
        )
    )
    w_highest = Window.partitionBy("LineID").orderBy(
        F.col("PartnerCount").desc(),
        F.col("TotalAmount").desc(),
        F.col("AttributeTypeID").asc(),
        F.col("AttributeID").asc(),
    )
    highest_by_line = (
        cand_by_line.withColumn("Rnk", F.row_number().over(w_highest))
        .filter(F.col("Rnk") == 1)
        .select("LineID", "AttributeTypeID", "AttributeID")
    )

    # Final UPDATE (SQL 442-445): Amount += ROUND(Diff, 0) for the highest
    # country row of each (partner, line) with a residual diff.
    plug = (
        diff_amounts.alias("D")
        .join(F.broadcast(highest_by_line).alias("H"), F.col("D.LineID") == F.col("H.LineID"), "inner")
        .select(
            F.col("D.PartnerNumber").alias("PartnerNumber"),
            F.col("D.LineID").alias("LineID"),
            F.col("H.AttributeTypeID").alias("AttributeTypeID"),
            F.col("H.AttributeID").alias("AttributeID"),
            _sql_round(F.col("D.Diff"), 0).alias("Plug"),
        )
        .filter(F.col("Plug") != 0)
    )

    summary = (
        summary.alias("TR")
        .join(
            F.broadcast(plug).alias("PL"),
            (F.col("TR.PartnerNumber") == F.col("PL.PartnerNumber"))
            & (F.col("TR.LineID") == F.col("PL.LineID"))
            & (F.col("TR.AttributeTypeID") == F.col("PL.AttributeTypeID"))
            & (F.col("TR.AttributeID") == F.col("PL.AttributeID")),
            "left",
        )
        .select(
            *[
                (F.col("TR.Amount") + F.coalesce(F.col("PL.Plug"), F.lit(0.0))).alias("Amount")
                if c == "Amount" else F.col(f"TR.{c}").alias(c)
                for c in _SUMMARY_COLS
            ]
        )
    )

    log_timing("apply_country_level_rounding", t0, logger)
    return summary
