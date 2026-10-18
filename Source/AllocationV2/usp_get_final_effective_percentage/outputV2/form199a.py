"""
form199a.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Form199A effective percentage calculation (4 rules).
Conversion date: 2026-05-04

SQL lines: 4680-5700
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
import logging
import time

logger = logging.getLogger(__name__)


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _log_timing(name, start):
    logger.info(f"[TIMING] {name}: {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# compute_form199a_effective_percentage
# SQL lines: 4680-5700
# Row count: POSSIBLY-EMPTY (guarded by @IsForm199AEffectivePercentageLogic)
# ---------------------------------------------------------------------------
def compute_form199a_effective_percentage(
    spark: SparkSession, cfg: dict,
    non_dated_entities: DataFrame,
    book_effective: DataFrame,
    input_lines: DataFrame,
    cost_percentage: DataFrame,
) -> tuple:
    """Compute Form199A effective percentages via 4 rule-based calculations.

    Effective SQL gate (PE Model has been removed):
      (mode == 2 OR mode == 4) AND is_form_199a_enabled

    Per-mode behaviour:
        Mode 1 : SKIP   (mode 1 path is dead due to inner gate)
        Mode 2, enabled : RUN
        Mode 3 : SKIP
        Mode 4, enabled : RUN
        Any mode, enabled=False : SKIP

    Rules:
    - Rule 1: K1 line amounts from LookThroughAllocationOutput for matching lines
    - Rule 2: Q4 quarter amounts + cost percentage fallback for missing entities
    - Rule 3: Taxable income for entities not matched by rules 1/2/4
    - Rule 4: All K1 line amounts (no exclusions, no quarter filter)

    Returns: (updated_non_dated, form199a_eff_pct)
    """
    t0 = time.time()
    logger.info("[SECTION] compute_form199a_effective_percentage")

    is_enabled = cfg.get("is_form_199a_effective_pct_logic", False)
    mode = cfg.get("mode")

    # Effective SQL gate (PE Model removed):
    #   (mode in {2, 4}) AND is_enabled
    mode_eligible = (mode == 2 or mode == 4)

    if not is_enabled or not mode_eligible:
        if not is_enabled:
            reason = "disabled (is_form_199a_effective_pct_logic = False)"
        else:
            reason = f"mode {mode} not in {{2, 4}} (SP outer gate fails)"
        logger.info(f"Form199A effective percentage logic skipped ({reason})")
        return non_dated_entities, spark.createDataFrame([], "InvestmentID int, LineTypeID int")

    run_id = cfg["run_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    cost_alloc_type_id = cfg["cost_allocation_type_id"]

    # --- Load rule config from ENU_DF_DataList ---
    rule_categories = [
        "Form199AEffectivePercentageRule1",
        "Form199AEffectivePercentageRule2",
        "Form199AEffectivePercentageRule3",
        "Form199AEffectivePercentageRule4",
    ]
    rule_config = (
        F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg))
        .filter(F.col("Category").isin(rule_categories))
        .select(
            F.when(F.col("Category") == "Form199AEffectivePercentageRule1", F.lit(1))
            .when(F.col("Category") == "Form199AEffectivePercentageRule2", F.lit(2))
            .when(F.col("Category") == "Form199AEffectivePercentageRule3", F.lit(3))
            .when(F.col("Category") == "Form199AEffectivePercentageRule4", F.lit(4))
            .alias("RuleNumber"),
            F.col("LookUpData"),
            F.col("LookUpValue"),
        )
    )

    # --- Build #tmp199ALine: Form199A line IDs by rule ---
    form199a_li = _tbl(spark, "Form199ALineItem", cfg)
    tmp_199a_line = (
        rule_config.alias("D")
        .join(
            form199a_li.alias("F_"),
            F.trim(F.col("D.LookUpValue")) == F.trim(F.col("F_.LineDescription")),
        )
        .filter(F.col("D.LookUpData") == "199ALine")
        .select(
            F.col("D.RuleNumber"),
            F.col("F_.LineID").alias("Form199ALineID"),
        )
    )

    # --- Build #tmpK1Line: K1 line numbers by rule ---
    tmp_k1_line = (
        rule_config
        .filter(F.col("LookUpData") == "K1Line")
        .select(
            F.col("RuleNumber"),
            F.col("LookUpValue").alias("LineNumber"),
        )
    )

    # --- Build #tmpExcludeLine: excluded K1 LineIDs by rule ---
    k1_li = _tbl(spark, "K1LineItem", cfg)
    tmp_exclude_line = (
        rule_config.alias("D")
        .join(
            k1_li.alias("F_"),
            F.trim(F.col("D.LookUpValue")) == F.trim(F.col("F_.LineDescription")),
        )
        .filter(F.col("D.LookUpData") == "ExcludeLine")
        .select(
            F.col("D.RuleNumber"),
            F.col("F_.LineID").alias("ExcludeLineID"),
        )
    )

    # --- Get @199ALineType ---
    _199a_line_type_row = (
        _tbl(spark, "ENU_LineType", cfg)
        .filter(F.coalesce(F.col("LineType"), F.lit("")) == "Form199A")
        .select("LineTypeID")
        .first()
    )
    _199a_line_type_id = _199a_line_type_row["LineTypeID"] if _199a_line_type_row else None

    if _199a_line_type_id is None:
        logger.warning("Form199A LineType not found — skipping 199A effective percentage")
        return non_dated_entities, spark.createDataFrame([], "InvestmentID int, LineTypeID int")

    # --- Build #tmp199AUnderlying ---
    # Non-dated entities for Form199A that have NO matching book effective entry
    tmp_199a_underlying = (
        non_dated_entities.alias("D")
        .filter(
            (F.col("D.LineTypeID") == _199a_line_type_id)
            & (F.coalesce(F.col("D.IsExcludefromTransfer"), F.lit(False)) == False)
        )
        .join(
            book_effective.select("UnderlyingEntityID", "SourceID").distinct().alias("B"),
            (F.col("D.UnderlyingEntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("D.LineTypeID") == F.col("B.SourceID")),
            "left_anti",
        )
        .select(
            F.col("D.UnderlyingEntityID"),
            F.col("D.LineTypeID"),
            F.col("D.TypeID"),
            F.col("D.TrackingKey"),
            F.col("D.Tag"),
        )
    )

    # --- Build #RuleUnderlyings ---
    rule_underlyings = (
        tmp_199a_underlying.alias("T")
        .join(
            input_lines.alias("I"),
            F.col("I.UnderlyingEntityID") == F.col("T.UnderlyingEntityID"),
        )
        .join(
            tmp_199a_line.alias("A"),
            F.col("A.Form199ALineID") == F.col("I.LineID"),
        )
        .select(
            F.col("T.UnderlyingEntityID"),
            F.col("T.LineTypeID"),
            F.col("T.TypeID"),
            F.col("T.TrackingKey"),
            F.col("T.Tag"),
            F.col("I.LineID"),
            F.col("A.RuleNumber"),
        )
    )

    # --- Load LookThroughAllocationOutput ---
    ltao = (
        _tbl(spark, "LookThroughAllocationOutput", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("LineTypeID") == k1_lt_id)
        )
    )

    # =====================================================================
    # Rule 1: K1 amounts for rule 1 lines, excluding specified lines
    # SQL lines 4912-4955
    # =====================================================================
    rule1_amounts = (
        ltao.alias("L")
        .join(
            rule_underlyings.alias("T"),
            (F.col("T.UnderlyingEntityID") == F.col("L.EntityID"))
            & (F.col("T.RuleNumber") == F.lit(1)),
        )
        .join(
            k1_li.alias("K"),
            F.col("K.LineID") == F.col("L.LineID"),
        )
        .join(
            tmp_k1_line.alias("KL"),
            (F.col("K.LineNumber") == F.col("KL.LineNumber"))
            & (F.col("KL.RuleNumber") == F.col("T.RuleNumber")),
        )
        .join(
            tmp_exclude_line.alias("E"),
            (F.col("K.LineID") == F.col("E.ExcludeLineID"))
            & (F.col("E.RuleNumber") == F.lit(1)),
            "left",
        )
        .filter(F.col("E.RuleNumber").isNull())
        .groupBy(
            F.col("L.EntityID"),
            F.col("L.PartnerNumber"),
            F.col("T.LineTypeID"),
            F.col("T.TypeID"),
            F.col("T.TrackingKey"),
            F.col("T.Tag"),
            F.col("T.LineID"),
        )
        .agg(F.sum("L.Amount").alias("Amount"))
        .select(
            F.col("EntityID"),
            F.col("PartnerNumber"),
            F.col("Amount"),
            F.col("LineTypeID"),
            F.col("TypeID"),
            F.col("TrackingKey"),
            F.col("Tag"),
            F.col("LineID"),
            F.lit(1).alias("RuleNumber"),
            F.lit(True).alias("IsK1LineAmount"),
        )
    )

    # =====================================================================
    # Rule 2: K1 amounts for Q4, plus cost percentage fallback
    # SQL lines 4960-5090
    # =====================================================================
    quarter_month = (
        F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg))
        .filter(F.col("Category") == "QuarterMonth")
    )

    rule2_amounts = (
        ltao.alias("L")
        .join(
            rule_underlyings.alias("T"),
            (F.col("T.UnderlyingEntityID") == F.col("L.EntityID"))
            & (F.col("T.RuleNumber") == 2),
        )
        .join(
            k1_li.alias("K"),
            F.col("K.LineID") == F.col("L.LineID"),
        )
        .join(
            quarter_month.alias("D"),
            F.col("D.LookUpValue") == F.coalesce(
                F.month(F.col("K.TransactionDate")), F.lit(0)
            ).cast("string"),
        )
        .join(
            tmp_k1_line.alias("KL"),
            (F.col("K.LineNumber") == F.col("KL.LineNumber"))
            & (F.col("KL.RuleNumber") == F.col("T.RuleNumber")),
        )
        .join(
            tmp_exclude_line.alias("E"),
            (F.col("K.LineID") == F.col("E.ExcludeLineID"))
            & (F.col("E.RuleNumber") == 2),
            "left",
        )
        .filter(
            (F.col("D.LookUpData") == "Q4")
            & (F.col("E.RuleNumber").isNull())
        )
        .groupBy(
            F.col("L.EntityID"),
            F.col("L.PartnerNumber"),
            F.col("T.LineTypeID"),
            F.col("T.TypeID"),
            F.col("T.TrackingKey"),
            F.col("T.Tag"),
            F.col("T.LineID"),
        )
        .agg(F.sum("L.Amount").alias("Amount"))
        .select(
            F.col("EntityID"),
            F.col("PartnerNumber"),
            F.col("Amount"),
            F.col("LineTypeID"),
            F.col("TypeID"),
            F.col("TrackingKey"),
            F.col("Tag"),
            F.col("LineID"),
            F.lit(2).alias("RuleNumber"),
            F.lit(True).alias("IsK1LineAmount"),
        )
    )

    # Rule 2 cost percentage fallback: for entities with no K1 amounts, use max quarter cost pct
    max_pct = (
        cost_percentage.alias("T")
        .join(
            rule_underlyings.alias("U"),
            (F.col("T.DealId") == F.col("U.UnderlyingEntityID"))
            & (F.col("U.RuleNumber") == 2),
        )
        .join(
            rule2_amounts.alias("R"),
            (F.col("T.DealId") == F.col("R.EntityID"))
            & (F.col("R.LineID") == F.col("U.LineID")),
            "left",
        )
        .filter(
            (F.col("R.EntityID").isNull())
            & (F.col("T.TypeId") == cost_alloc_type_id)
        )
        .groupBy(
            F.col("T.DealId"),
            F.col("T.TypeId"),
            F.col("T.TrackingKey"),
            F.col("T.Tag"),
        )
        .agg(F.max("T.Quarter").alias("MaxQuarter"))
    )

    rule2_cost_pct = (
        cost_percentage.alias("T")
        .join(
            max_pct.alias("M"),
            (F.col("M.DealId") == F.col("T.DealId"))
            & (F.col("M.MaxQuarter") == F.col("T.Quarter"))
            & (F.coalesce(F.col("M.TypeId"), F.lit(0)) == F.coalesce(F.col("T.TypeId"), F.lit(0)))
            & (F.coalesce(F.col("M.TrackingKey"), F.lit("")) == F.coalesce(F.col("T.TrackingKey"), F.lit("")))
            & (F.coalesce(F.col("M.Tag"), F.lit("")) == F.coalesce(F.col("T.Tag"), F.lit(""))),
        )
    )

    # Insert into final effective percentage for rule 2
    rule2_199a_line = tmp_199a_line.filter(F.col("RuleNumber") == 2)

    rule2_eff_pct = (
        rule2_cost_pct.alias("L_")
        .join(
            tmp_199a_underlying.alias("T"),
            (F.col("T.UnderlyingEntityID") == F.col("L_.DealId"))
            & (F.col("L_.TypeId") == F.col("T.TypeID")),
        )
        .crossJoin(rule2_199a_line.alias("A"))
        .select(
            F.col("L_.DealId").alias("InvestmentID"),
            F.col("T.LineTypeID"),
            F.col("T.TypeID").alias("TypeId"),
            F.col("L_.PartnerNumber"),
            F.col("L_.CommitmentPercent").alias("EffPercentage"),
            F.lit("TI").alias("AllocationType"),
            F.col("T.TrackingKey"),
            F.col("T.Tag"),
            F.lit("Q0").alias("Quarter"),
            F.col("A.Form199ALineID").alias("LineID"),
            F.lit(False).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # =====================================================================
    # Rule 4: All K1 amounts (no exclusion, no quarter filter)
    # SQL lines 5220-5260
    # =====================================================================
    rule4_amounts = (
        ltao.alias("L")
        .join(
            rule_underlyings.alias("T"),
            (F.col("T.UnderlyingEntityID") == F.col("L.EntityID"))
            & (F.col("T.RuleNumber") == 4),
        )
        .join(
            k1_li.alias("K"),
            F.col("K.LineID") == F.col("L.LineID"),
        )
        .join(
            tmp_k1_line.alias("KL"),
            (F.col("K.LineNumber") == F.col("KL.LineNumber"))
            & (F.col("KL.RuleNumber") == F.col("T.RuleNumber")),
        )
        .groupBy(
            F.col("L.EntityID"),
            F.col("L.PartnerNumber"),
            F.col("T.LineTypeID"),
            F.col("T.TypeID"),
            F.col("T.TrackingKey"),
            F.col("T.Tag"),
            F.col("T.LineID"),
        )
        .agg(F.sum("L.Amount").alias("Amount"))
        .select(
            F.col("EntityID"),
            F.col("PartnerNumber"),
            F.col("Amount"),
            F.col("LineTypeID"),
            F.col("TypeID"),
            F.col("TrackingKey"),
            F.col("Tag"),
            F.col("LineID"),
            F.lit(4).alias("RuleNumber"),
            F.lit(True).alias("IsK1LineAmount"),
        )
    )

    # --- Combine rules 1 + 4 amounts ---
    all_rule_amounts = rule1_amounts.unionByName(rule4_amounts)

    # --- Effective Percentage from rules 1+4: Amount / TotalAmount ---
    total_amount = (
        all_rule_amounts
        .filter(F.col("IsK1LineAmount") == True)
        .groupBy("EntityID", "LineID", "RuleNumber")
        .agg(F.sum("Amount").alias("TotalAmount"))
    )

    rules_14_eff_pct = (
        all_rule_amounts.alias("I")
        .join(
            total_amount.alias("A"),
            (F.col("I.EntityID") == F.col("A.EntityID"))
            & (F.col("I.LineID") == F.col("A.LineID")),
        )
        .filter(F.col("A.TotalAmount") != 0)
        .select(
            F.col("I.EntityID").alias("InvestmentID"),
            F.col("I.LineTypeID"),
            F.col("I.TypeID").alias("TypeId"),
            F.col("I.PartnerNumber"),
            F.try_divide(F.col("I.Amount"), F.col("A.TotalAmount")).alias("EffPercentage"),
            F.lit("TI").alias("AllocationType"),
            F.col("I.TrackingKey"),
            F.col("I.Tag"),
            F.lit("Q0").alias("Quarter"),
            F.col("I.LineID"),
            F.lit(False).alias("IsExcludefromTransfer"),
        )
    )

    # =====================================================================
    # Rule 3: Taxable income for entities not matched by rules 1/2/4
    # SQL lines 5418-5510
    # =====================================================================

    # Find underlyings NOT yet in final eff pct
    ti_lines_rule3_a = (
        rule_underlyings.alias("R")
        .join(
            rules_14_eff_pct.alias("A"),
            (F.col("R.UnderlyingEntityID") == F.col("A.InvestmentID"))
            & (F.col("R.LineID") == F.col("A.LineID"))
            & (F.col("R.TypeID") == F.col("A.TypeId"))
            & (F.col("R.TrackingKey") == F.col("A.TrackingKey")),
            "left",
        )
        .filter(F.col("A.InvestmentID").isNull())
        .select(
            F.col("R.UnderlyingEntityID"),
            F.col("R.RuleNumber"),
            F.col("R.LineID").alias("Form199ALineID"),
        )
    )

    # Also include input_lines for Form199A that are NOT in tmp_199a_line
    # and have no book effective data
    ti_lines_rule3_b = (
        input_lines.alias("T")
        .filter(F.col("T.LineTypeID") == _199a_line_type_id)
        .join(
            tmp_199a_line.select("Form199ALineID").distinct().alias("L_"),
            F.col("L_.Form199ALineID") == F.col("T.LineID"),
            "left_anti",
        )
        .join(
            book_effective.select(
                "UnderlyingEntityID", "AdjustmentAllocationTypeID", "SourceID"
            ).distinct().alias("B"),
            (F.col("T.UnderlyingEntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("T.TypeID") == F.col("B.AdjustmentAllocationTypeID"))
            & (F.col("B.SourceID") == F.col("T.LineTypeID")),
            "left_anti",
        )
        .select(
            F.col("T.UnderlyingEntityID"),
            F.lit(3).alias("RuleNumber"),
            F.col("T.LineID").alias("Form199ALineID"),
        )
    )

    ti_eff_per_lines = ti_lines_rule3_a.unionByName(ti_lines_rule3_b)

    # Rule 3 amounts from LookThroughTaxableIncome
    enu_lt = F.broadcast(_tbl(spark, "ENU_LineType", cfg))
    rule3_amounts = (
        _tbl(spark, "LookThroughTaxableIncome", cfg).alias("L")
        .filter(F.col("L.RunID") == run_id)
        .join(
            ti_eff_per_lines.alias("A"),
            F.col("A.UnderlyingEntityID") == F.col("L.EntityID"),
        )
        .join(
            input_lines.alias("T"),
            (F.col("T.UnderlyingEntityID") == F.col("A.UnderlyingEntityID"))
            & (F.col("A.Form199ALineID") == F.col("T.LineID")),
        )
        .join(
            enu_lt.alias("LT"),
            (F.col("T.LineTypeID") == F.col("LT.LineTypeID"))
            & (F.col("LT.LineType") == "Form199A"),
        )
        .groupBy(
            F.col("L.EntityID"),
            F.col("L.PartnerNumber"),
            F.col("T.LineTypeID"),
            F.col("T.TypeID"),
            F.col("T.TrackingKey"),
            F.col("T.Tag"),
            F.col("T.LineID"),
        )
        .agg(F.sum("L.TaxableIncome").alias("Amount"))
        .select(
            F.col("EntityID"),
            F.col("PartnerNumber"),
            F.col("Amount"),
            F.col("LineTypeID"),
            F.col("TypeID"),
            F.col("TrackingKey"),
            F.col("Tag"),
            F.col("LineID"),
            F.lit(3).alias("RuleNumber"),
            F.lit(True).alias("IsK1LineAmount"),
        )
    )

    # Taxable income total and effective percentage
    ti_total = (
        rule3_amounts
        .filter(F.col("IsK1LineAmount") == True)
        .groupBy("EntityID", "LineID", "RuleNumber")
        .agg(F.sum("Amount").alias("TotalAmount"))
    )

    rule3_eff_pct = (
        rule3_amounts.alias("I")
        .join(
            ti_total.alias("A"),
            (F.col("I.EntityID") == F.col("A.EntityID"))
            & (F.col("I.LineID") == F.col("A.LineID")),
        )
        .filter(F.col("A.TotalAmount") != 0)
        .select(
            F.col("I.EntityID").alias("InvestmentID"),
            F.col("I.LineTypeID"),
            F.col("I.TypeID").alias("TypeId"),
            F.col("I.PartnerNumber"),
            F.try_divide(F.col("I.Amount"), F.col("A.TotalAmount")).alias("EffPercentage"),
            F.lit("TI").alias("AllocationType"),
            F.col("I.TrackingKey"),
            F.col("I.Tag"),
            F.lit("Q0").alias("Quarter"),
            F.col("I.LineID"),
            F.lit(False).alias("IsExcludefromTransfer"),
        )
    )

    # =====================================================================
    # Combine all rule effective percentages
    # =====================================================================
    form199a_eff_pct = (
        rules_14_eff_pct
        .unionByName(rule2_eff_pct, allowMissingColumns=True)
        .unionByName(rule3_eff_pct, allowMissingColumns=True)
    )

    # =====================================================================
    # Remove matched entries from non-dated entities
    # SQL lines 5580-5595
    # =====================================================================
    entries_to_remove = (
        non_dated_entities.alias("D")
        .join(
            tmp_199a_underlying.alias("F_"),
            (F.col("D.UnderlyingEntityID") == F.col("F_.UnderlyingEntityID"))
            & (F.col("D.TypeID") == F.col("F_.TypeID"))
            & (F.col("D.TrackingKey") == F.col("F_.TrackingKey"))
            & (F.col("D.Tag") == F.col("F_.Tag"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.coalesce(F.col("F_.LineTypeID"), F.lit(-1))),
        )
        .join(
            form199a_eff_pct.alias("T"),
            (F.col("D.UnderlyingEntityID") == F.col("T.InvestmentID"))
            & (F.col("D.TypeID") == F.col("T.TypeId"))
            & (F.col("D.TrackingKey") == F.col("T.TrackingKey"))
            & (F.col("D.Tag") == F.col("T.Tag"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.coalesce(F.col("T.LineTypeID"), F.lit(-1))),
        )
        .filter(F.coalesce(F.col("D.IsExcludefromTransfer"), F.lit(False)) == False)
        .select(
            F.col("D.UnderlyingEntityID"),
            F.col("D.TypeID"),
            F.col("D.TrackingKey"),
            F.col("D.Tag"),
            F.col("D.LineTypeID"),
        )
    )

    # Anti-join to remove
    updated_non_dated = (
        non_dated_entities.alias("D")
        .join(
            entries_to_remove.alias("R"),
            (F.col("D.UnderlyingEntityID") == F.col("R.UnderlyingEntityID"))
            & (F.col("D.TypeID") == F.col("R.TypeID"))
            & (F.col("D.TrackingKey") == F.col("R.TrackingKey"))
            & (F.col("D.Tag") == F.col("R.Tag"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.coalesce(F.col("R.LineTypeID"), F.lit(-1))),
            "left_anti",
        )
    )

    _log_timing("compute_form199a_effective_percentage", t0)
    return updated_non_dated, form199a_eff_pct
