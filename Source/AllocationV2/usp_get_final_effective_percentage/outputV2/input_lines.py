"""
input_lines.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Input line building and amount-based allocation logic.
Conversion date: 2026-05-04

SQL lines: 3000-3370
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
import logging
import time

logger = logging.getLogger(__name__)


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _sql_round(col, scale):
    """SQL Server ROUND() — round half away from zero."""
    factor = F.pow(F.lit(10), F.lit(scale))
    return F.signum(col) * F.floor(F.abs(col) * factor + F.lit(0.5)) / factor


def _log_timing(name, start):
    logger.info(f"[TIMING] {name}: {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# build_input_lines
# SQL lines: 2975-3095
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_input_lines(
    spark: SparkSession, cfg: dict,
    lt_input: DataFrame,
    line_items: DataFrame,
    book_effective: DataFrame,
    entity_alloc_rule: DataFrame,
    all_underlyings: DataFrame,
) -> DataFrame:
    """Build #TempInputLines — maps each input line to allocation type.

    Two passes:
    1. With #TempAllUnderlyings join (for CAR-mapped lines)
    2. Remaining lines via K1LineItem direct lookup

    Then cleans up: deletes matched rows from lt_input,
    deletes K1 source BookEffective entries, and does second pass for unmapped.
    """
    t0 = time.time()
    logger.info("[SECTION] build_input_lines")

    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    book_alloc_type_id = cfg.get("book_allocation_type_id")
    k1_lt_id = cfg["k1_line_type_id"]
    adj_lt_id = cfg["adjustment_line_type_id"]

    # Line type normalization
    adj_to_k1 = F.when(
        F.col("I.LineTypeID") == adj_lt_id, F.lit(k1_lt_id).cast("int")
    ).otherwise(F.col("I.LineTypeID"))

    # Key matching helpers
    def _match_key(col):
        return F.when(
            F.coalesce(col, F.lit("")) == "", F.lit("-1")
        ).otherwise(col)

    # --- Pass 1: Lines with #TempAllUnderlyings match ---
    # Broadcast line_items (~370K rows, ~20MB) to eliminate shuffle against
    # lt_input (4M+ rows). The join is on LineID which is highly selective.
    _line_items_bc = F.broadcast(line_items)

    pass1 = (
        lt_input.alias("I")
        .join(
            _line_items_bc.alias("K"),
            (F.col("I.LineID") == F.col("K.LineID"))
            & (F.col("I.LineTypeID") == F.when(
                F.col("K.LineTypeID") == adj_lt_id, F.lit(k1_lt_id).cast("int")
            ).otherwise(F.col("K.LineTypeID"))),
        )
        .join(
            F.broadcast(book_effective).alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("I.LineID") == F.col("B.LineID"))
            & (F.col("B.LineID") != -1)
            & (F.col("B.SourceID") == F.col("I.LineTypeID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
            "left",
        )
        .join(
            F.broadcast(entity_alloc_rule).alias("ER"),
            F.col("ER.LineId") == F.col("I.LineID"),
            "left",
        )
        .join(
            all_underlyings.alias("AI"),
            (F.col("I.EntityID") == F.col("AI.UnderlyingEntityId"))
            & (F.col("I.TrackingKey") == F.col("AI.TrackingKey"))
            & (F.col("I.LineID") == F.col("AI.LineID"))
            & (F.col("AI.LineTypeID") == adj_to_k1)
            & (F.col("AI.AllocationBy") == "PERCENT"),
            "left",
        )
        .filter(
            (F.col("B.LineID") != -1) | (F.col("AI.LineID") != -1)
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.LineTypeID"),
            F.col("I.LineID"),
            F.coalesce(
                F.col("B.AdjustmentAllocationTypeID"),
                F.when(
                    F.col("K.AllocationTypeRuleId") == book_alloc_type_id,
                    F.lit(cost_alloc_type_id).cast("int"),
                ).otherwise(
                    F.coalesce(
                        F.col("ER.UpdatedAllocationRuleID"),
                        F.coalesce(F.col("AI.AllocationTypeId"), F.col("K.AllocationTypeRuleId")),
                    )
                ),
            ).alias("TypeID"),
            F.coalesce(
                F.col("B.TrackingKey"),
                F.coalesce(F.col("I.TrackingKey"), F.lit("")),
            ).alias("TrackingKey"),
            F.coalesce(
                F.col("B.Tag"),
                F.coalesce(F.col("I.Tag"), F.lit("")),
            ).alias("Tag"),
            F.coalesce(
                F.col("B.IsExcludefromTransfer"),
                F.col("AI.IsExcludefromTransfer"),
            ).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Identify matched input rows for exclusion ---
    matched_keys = (
        lt_input.alias("I")
        .join(
            book_effective.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("I.LineID") == F.col("B.LineID"))
            & (F.col("B.SourceID") == F.col("I.LineTypeID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
            "left",
        )
        .join(
            all_underlyings.alias("AI"),
            (F.col("I.EntityID") == F.col("AI.UnderlyingEntityId"))
            & (F.col("I.TrackingKey") == F.col("AI.TrackingKey"))
            & (F.col("I.LineID") == F.col("AI.LineID"))
            & (F.col("AI.LineTypeID") == F.when(
                F.col("I.LineTypeID") == adj_lt_id, F.lit(k1_lt_id).cast("int")
            ).otherwise(F.col("I.LineTypeID"))),
            "left",
        )
        .filter(
            (F.col("B.LineID") != -1) | (F.col("AI.LineID") != -1)
        )
        .select(F.col("I.EntityID"), F.col("I.LineID"), F.col("I.LineTypeID"), F.col("I.TrackingKey"))
    )

    # Remaining lt_input after removing matched
    lt_input_remaining = (
        lt_input.alias("I")
        .join(
            matched_keys.alias("MK"),
            (F.col("I.EntityID") == F.col("MK.EntityID"))
            & (F.col("I.LineID") == F.col("MK.LineID"))
            & (F.col("I.LineTypeID") == F.col("MK.LineTypeID"))
            & (F.col("I.TrackingKey") == F.col("MK.TrackingKey")),
            "left_anti",
        )
    )

    # Clean book effective: delete K1 source entries with non-null LineID
    book_effective_cleaned = book_effective.filter(
        ~((F.coalesce(F.col("LineID"), F.lit(0)) != -1) & (F.col("SourceID") == k1_lt_id))
    )

    # --- Pass 2: Remaining lines via K1LineItem direct lookup ---
    pass2 = (
        lt_input_remaining.alias("I")
        .join(
            F.broadcast(_tbl(spark, "K1LineItem", cfg)).alias("K"),
            F.col("I.LineID") == F.col("K.LineID"),
        )
        .join(
            F.broadcast(book_effective_cleaned).alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("B.SourceID") == F.col("I.LineTypeID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
            "left",
        )
        .join(
            F.broadcast(entity_alloc_rule).alias("ER"),
            F.col("ER.LineId") == F.col("I.LineID"),
            "left",
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.LineTypeID"),
            F.col("I.LineID"),
            F.coalesce(
                F.col("B.AdjustmentAllocationTypeID"),
                F.when(
                    F.col("K.AllocationTypeRuleId") == book_alloc_type_id,
                    F.lit(cost_alloc_type_id).cast("int"),
                ).otherwise(
                    F.coalesce(F.col("ER.UpdatedAllocationRuleID"), F.col("K.AllocationTypeRuleId"))
                ),
            ).alias("TypeID"),
            F.coalesce(
                F.col("B.TrackingKey"),
                F.coalesce(F.col("I.TrackingKey"), F.lit("")),
            ).alias("TrackingKey"),
            F.coalesce(
                F.col("B.Tag"),
                F.coalesce(F.col("I.Tag"), F.lit("")),
            ).alias("Tag"),
            F.coalesce(F.col("B.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # Combine both passes
    result = pass1.unionByName(pass2)

    _log_timing("build_input_lines", t0)
    return result, lt_input_remaining, book_effective_cleaned


# ---------------------------------------------------------------------------
# compute_amount_based_allocation
# SQL lines: 3095-3370
# Row count: POSSIBLY-EMPTY (only when AllocationBy='AMOUNT' rows exist)
# ---------------------------------------------------------------------------
def compute_amount_based_allocation(
    spark: SparkSession, cfg: dict,
    all_underlyings: DataFrame,
    cost_pct_snapshot: DataFrame,
    lt_input: DataFrame,
    map_dar: DataFrame,
) -> DataFrame:
    """Compute effective amounts for 'By Amount' allocation.

    Two flows:
    1. Mode 1 + 704c mappings (Aggregate or SP-specific): allocates by
       CostPercentageInvestmentID grouping
    2. Default flow: allocates by EntityTotal underlying type

    Then aggregates: #TotalUnderlyingAmounts, #FinalEffectiveAmounts, #FinalAmounts.

    Returns: final_amounts DataFrame (or empty if no AMOUNT rows).
    """
    t0 = time.time()
    logger.info("[SECTION] compute_amount_based_allocation")

    entity_id = cfg["entity_id"]
    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    entity_ut_id = cfg["entity_underlying_type_id"]
    underlying_only_ut_id = cfg.get("underlying_only_type_id")
    entity_total_ut_id = cfg.get("entity_total_underlying_type_id")
    k1_lt_id = cfg["k1_line_type_id"]
    box_jkl_lt_id = cfg["box_jkl_line_type_id"]
    dar_tid = cfg.get("default_alloc_rule_transaction_id")
    gdar_tid = cfg.get("global_default_alloc_rule_transaction_id")
    mode = cfg.get("mode")
    _704c_alloc_type_name = cfg.get("_704c_allocation_type_name", "")
    has_704c_mappings = cfg.get("has_704c_mappings", False)

    valid_tids = [t for t in [dar_tid, gdar_tid, -2] if t is not None]

    enu_ut = F.broadcast(_tbl(spark, "ENU_UnderlyingType", cfg))
    enu_lt = F.broadcast(_tbl(spark, "ENU_LineType", cfg))

    # --- EntityTotal amounts from default (non-704c) flow ---
    # SQL lines 3249-3285 — standard entity total amounts
    entity_total_amounts = (
        enu_ut.alias("U")
        .join(
            all_underlyings.alias("E"),
            F.col("U.UnderlyingTypeID") == F.col("E.Underlyingtype"),
        )
        .join(
            cost_pct_snapshot.alias("C"),
            (F.col("E.EntityId") == F.col("C.InvestmentID"))
            & (F.col("C.UnderlyingType") == F.col("E.Underlyingtype"))
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (
                F.concat(
                    F.lit("~"),
                    F.when(
                        F.coalesce(F.col("C.TrackingKey"), F.lit("")) == "",
                        F.col("C.InvestmentID").cast("string"),
                    ).otherwise(F.col("C.TrackingKey")),
                    F.lit("~"),
                )
                == F.col("E.TrackingMatch")
            ),
        )
        .join(
            lt_input.alias("AI"),
            (F.col("E.UnderlyingEntityId") == F.col("AI.EntityID"))
            & (F.col("E.LineTypeID") == F.col("AI.LineTypeID"))
            & (F.col("AI.LineID") == F.col("E.LineID")),
        )
        .join(
            map_dar.alias("M"),
            (F.col("C.AllocationTypeID") == F.col("M.RuleID"))
            & (
                F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("M.SelectedMappingID"))
                == F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("AI.LineID"))
            ),
        )
        .join(
            enu_lt.alias("EL"),
            F.col("M.SourceID") == F.col("EL.LineTypeID"),
        )
        .filter(
            (F.coalesce(F.col("C.UnderlyingType").cast("string"), F.lit("")) == F.lit(entity_ut_id).cast("string"))
            & (F.col("EL.LineTypeID").isin([k1_lt_id, box_jkl_lt_id]))
            & (F.col("AI.LineTypeID") == F.col("M.SourceID"))
            & (F.coalesce(F.col("C.AllocatedAmount"), F.lit(0.0)) != 0)
            & (F.col("M.TransactionID").isin(valid_tids))
            & (F.col("E.AllocationBy") == "AMOUNT")
        )
        .select(
            F.col("E.UnderlyingEntityId").alias("UnderlyingEntityID"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.coalesce(F.col("C.CommitmentPercent"), F.lit(0.0)).alias("CommitmentPercent"),
            F.coalesce(F.col("C.AllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("AllocationTypeID"),
            F.coalesce(F.col("AI.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("AI.Tag"), F.lit("")).alias("Tag"),
            F.col("AI.LineID"),
            F.coalesce(F.col("AI.Amount"), F.lit(0.0)).alias("InputAmount"),
            F.coalesce(F.col("C.AllocatedAmount"), F.lit(0.0)).alias("AllocatedAmount"),
            F.col("C.InvestmentID").alias("CostEntityID"),
            F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut_id).cast("int")).alias("UnderlyingTypeID"),
            F.col("AI.LineTypeID"),
            F.col("C.GPPartnerReceivingCarry"),
        )
    )

    # ── Gap B: Mode 1 + 704c PE-Book variant entity_total_amounts ────────
    # SQL lines 3137-3251 (Bug 347522 — '704c with PE Book' allocations).
    # This block contributes EXTRA rows to #EntityTotalAmounts BEFORE the
    # standard INSERT runs. We model it as a UNION ALL of variant rows on
    # top of `entity_total_amounts` above (the standard path).
    #
    # Two variants, switched by cfg["_704c_allocation_type_name"]:
    #   - LIKE '%Aggregate 704(c)%'  → filter lt_input to mapped LineIDs;
    #     CostPercentageInvestmentID is the local entity_id.
    #   - == '704(c) - SP'           → join lt_input with all_underlyings
    #     and a custom-allocation-restricted snapshot to derive
    #     CostPercentageInvestmentID.
    #
    # The variant's allocated amount uses the SQL formula
    #     (C.AllocatedAmount / LT.Amount) * AI.Amount
    # via #LookThroughAllocationInputGrouped LT — i.e. amount is
    # apportioned by the per-investment line total.
    if (
        mode == 1
        and has_704c_mappings
        and cfg.get("_704c_mappings_df") is not None
        and underlying_only_ut_id is not None
        and entity_total_ut_id is not None
    ):
        mappings_df = cfg["_704c_mappings_df"]

        # SQL line 3109: SELECT * INTO #LookThroughAllocationInput
        #   WHERE Round(ISNULL(Amount,0),0) <> 0
        # Unlike #TempLookThroughAllocationInput (line 2712, which exempts
        # BoxJKL from the amount check per Bug 340444), this temp table
        # applies the Round(Amount,0)!=0 filter to ALL line types — K1,
        # Adjustment, AND BoxJKL. We re-filter `lt_input` here because the
        # caller passes the BoxJKL-exempted variant.
        lt_input = lt_input.filter(
            _sql_round(F.coalesce(F.col("Amount"), F.lit(0.0)), 0) != 0
        )

        # --- variant-filtered LookThroughAllocationInputAmounts ---
        if "Aggregate 704(c)" in _704c_alloc_type_name:
            distinct_mapped_lines = (
                mappings_df.select(F.col("RegisterLineID")).distinct()
            )
            lt_input_amounts = (
                lt_input.alias("LT").join(
                    F.broadcast(distinct_mapped_lines).alias("MS"),
                    F.col("MS.RegisterLineID") == F.col("LT.LineID"),
                )
                .select(
                    F.col("LT.EntityID"),
                    F.lit(entity_id).cast("int").alias("CostPercentageInvestmentID"),
                    F.col("LT.LineTypeID"),
                    F.col("LT.LineID"),
                    F.col("LT.Amount"),
                    F.col("LT.TrackingKey"),
                    F.col("LT.Tag"),
                )
            )
        elif _704c_alloc_type_name == "704(c) - SP":
            # SQL joins #CostPercentage_Snapshot → ENU_CustomAllocations →
            # #Mappings to filter to rows whose AllocationTypeID matches a
            # 'Special <DatabaseName>' custom allocation created in Phase 2.
            # Since we now persist real IDs into ENU_CustomAllocations, we
            # look them up by name — matching the SQL exactly.
            enu_ca = _tbl(spark, "ENU_CustomAllocations", cfg)
            custom_alloc_ids = (
                mappings_df.alias("MS")
                .join(
                    F.broadcast(enu_ca).alias("ET"),
                    F.concat(F.lit("Special "), F.col("MS.DatabaseName")) == F.col("ET.AllocationType"),
                )
                .select(F.col("ET.AllocationTypeID"))
                .distinct()
            )

            distinct_cps = (
                cost_pct_snapshot.alias("CS").join(
                    F.broadcast(custom_alloc_ids).alias("CA"),
                    F.col("CS.AllocationTypeID") == F.col("CA.AllocationTypeID"),
                )
                .select(
                    F.col("CS.EntityID"),
                    F.col("CS.InvestmentID"),
                    F.col("CS.AllocationTypeID"),
                )
                .distinct()
            )
            lt_input_amounts = (
                lt_input.alias("LT").join(
                    all_underlyings.alias("T"),
                    (F.col("T.UnderlyingEntityId") == F.col("LT.EntityID"))
                    & (F.col("T.LineID") == F.col("LT.LineID"))
                    & (
                        F.coalesce(F.col("T.TrackingKey"), F.lit(""))
                        == F.coalesce(F.col("LT.TrackingKey"), F.lit(""))
                    ),
                )
                .join(
                    distinct_cps.alias("CS"),
                    (F.col("CS.InvestmentID") == F.col("T.EntityId"))
                    & (F.col("CS.AllocationTypeID") == F.col("T.AllocationTypeId")),
                )
                .select(
                    F.col("LT.EntityID"),
                    F.col("CS.InvestmentID").alias("CostPercentageInvestmentID"),
                    F.col("LT.LineTypeID"),
                    F.col("LT.LineID"),
                    F.col("LT.Amount"),
                    F.col("LT.TrackingKey"),
                    F.col("LT.Tag"),
                )
                .distinct()
            )
        else:
            lt_input_amounts = None

        if lt_input_amounts is not None:
            # #LookThroughAllocationInputGrouped — sum by investment/line
            lt_input_grouped = (
                lt_input_amounts
                .groupBy("CostPercentageInvestmentID", "LineID", "LineTypeID")
                .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("Amount"))
            )

            # Variant #EntityTotalAmounts contributions (SQL 3204-3251).
            entity_total_amounts_704c = (
                enu_ut.alias("U")
                .join(
                    all_underlyings.alias("E"),
                    F.col("U.UnderlyingTypeID") == F.col("E.Underlyingtype"),
                )
                .join(
                    cost_pct_snapshot.alias("C"),
                    (F.col("E.EntityId") == F.col("C.InvestmentID"))
                    & (F.col("C.UnderlyingType") == F.col("E.Underlyingtype"))
                    & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
                    & (
                        F.concat(
                            F.lit("~"),
                            F.when(
                                F.coalesce(F.col("C.TrackingKey"), F.lit("")) == "",
                                F.col("C.InvestmentID").cast("string"),
                            ).otherwise(F.col("C.TrackingKey")),
                            F.lit("~"),
                        )
                        == F.col("E.TrackingMatch")
                    ),
                )
                .join(
                    lt_input_amounts.alias("AI"),
                    (F.col("E.UnderlyingEntityId") == F.col("AI.EntityID"))
                    & (F.col("E.LineTypeID") == F.col("AI.LineTypeID"))
                    & (F.col("AI.LineID") == F.col("E.LineID"))
                    & (
                        F.coalesce(F.col("AI.TrackingKey"), F.lit(""))
                        == F.coalesce(F.col("E.TrackingKey"), F.lit(""))
                    ),
                )
                .join(
                    lt_input_grouped.alias("LTG"),
                    (F.col("LTG.CostPercentageInvestmentID") == F.col("E.EntityId"))
                    & (F.col("LTG.LineID") == F.col("AI.LineID"))
                    & (F.col("LTG.LineTypeID") == F.col("AI.LineTypeID")),
                )
                .join(
                    map_dar.alias("M"),
                    (F.col("C.AllocationTypeID") == F.col("M.RuleID"))
                    & (
                        F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                        .otherwise(F.col("M.SelectedMappingID"))
                        == F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                        .otherwise(F.col("AI.LineID"))
                    ),
                )
                .join(
                    enu_lt.alias("EL"),
                    F.col("M.SourceID") == F.col("EL.LineTypeID"),
                )
                .filter(
                    F.coalesce(F.col("C.UnderlyingType").cast("string"), F.lit("")).isin(
                        [str(underlying_only_ut_id), str(entity_total_ut_id)]
                    )
                    & (F.col("EL.LineTypeID").isin([k1_lt_id, box_jkl_lt_id]))
                    & (F.col("AI.LineTypeID") == F.col("M.SourceID"))
                    & (F.coalesce(F.col("C.AllocatedAmount"), F.lit(0.0)) != 0)
                    & (F.col("M.TransactionID").isin(valid_tids))
                    & (F.col("E.AllocationBy") == "AMOUNT")
                )
                .select(
                    F.col("E.UnderlyingEntityId").alias("UnderlyingEntityID"),
                    F.col("C.PartnerNumber"),
                    F.col("C.Quarter"),
                    F.coalesce(F.col("C.CommitmentPercent"), F.lit(0.0)).alias("CommitmentPercent"),
                    F.coalesce(F.col("C.AllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("AllocationTypeID"),
                    F.coalesce(F.col("AI.TrackingKey"), F.lit("")).alias("TrackingKey"),
                    F.coalesce(F.col("AI.Tag"), F.lit("")).alias("Tag"),
                    F.col("AI.LineID"),
                    F.coalesce(F.col("AI.Amount"), F.lit(0.0)).alias("InputAmount"),
                    # AllocatedAmount = (C.AllocatedAmount / LTG.Amount) * AI.Amount
                    (
                        F.coalesce(
                            F.try_divide(
                                F.col("C.AllocatedAmount"), F.col("LTG.Amount"),
                            ),
                            F.lit(0.0),
                        )
                        * F.coalesce(F.col("AI.Amount"), F.lit(0.0))
                    ).alias("AllocatedAmount"),
                    F.col("C.InvestmentID").alias("CostEntityID"),
                    F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut_id).cast("int")).alias("UnderlyingTypeID"),
                    F.col("AI.LineTypeID"),
                    F.col("C.GPPartnerReceivingCarry"),
                )
            )

            entity_total_amounts = entity_total_amounts.unionByName(
                entity_total_amounts_704c, allowMissingColumns=True,
            )
            logger.info(
                f"[704c-PE-Book] entity_total_amounts augmented with variant '{_704c_alloc_type_name}'"
            )

    # --- Aggregate and compute effective amounts ---
    # #TotalUnderlyingAmounts
    total_underlying = (
        entity_total_amounts
        .groupBy(
            "LineID", "PartnerNumber", "CostEntityID", "AllocationTypeID",
            "TrackingKey", "Tag", "LineTypeID",
        )
        .agg(
            F.sum(F.coalesce(F.col("InputAmount"), F.lit(0.0))).alias("TotalAmount"),
        )
    )

    # #FinalEffectiveAmounts
    final_eff = (
        total_underlying.alias("T")
        .join(
            entity_total_amounts.alias("C"),
            (F.col("C.CostEntityID") == F.col("T.CostEntityID"))
            & (F.col("C.AllocationTypeID") == F.col("T.AllocationTypeID"))
            & (F.col("C.TrackingKey") == F.col("T.TrackingKey"))
            & (F.col("C.Tag") == F.col("T.Tag"))
            & (F.col("C.LineID") == F.col("T.LineID"))
            & (F.col("C.PartnerNumber") == F.col("T.PartnerNumber"))
            & (F.col("T.LineTypeID") == F.col("C.LineTypeID")),
        )
        .select(
            F.col("C.UnderlyingEntityID"),
            F.col("C.LineID"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.col("C.AllocationTypeID").alias("TypeId"),
            F.col("C.TrackingKey"),
            F.col("C.Tag"),
            F.when(
                F.col("T.TotalAmount") != 0,
                F.try_divide(F.col("C.InputAmount"), F.col("T.TotalAmount"))
                * F.col("C.AllocatedAmount"),
            ).otherwise(F.lit(0.0)).alias("EffectiveAmount"),
            F.col("C.UnderlyingTypeID").alias("UnderlyingTypeId"),
            F.col("C.LineTypeID"),
            F.col("C.GPPartnerReceivingCarry"),
        )
    )

    # #FinalAmounts — final output with 'Cost' allocation type marker
    final_amounts = final_eff.select(
        F.col("UnderlyingEntityID").alias("InvestmentID"),
        F.col("PartnerNumber").alias("Partnernumber"),
        F.lit("Cost").alias("AllocationType"),
        F.col("Quarter"),
        F.col("TypeId"),
        F.coalesce(F.col("TrackingKey"), F.lit("")).alias("TrackingKey"),
        F.coalesce(F.col("Tag"), F.lit("")).alias("Tag"),
        F.lit(-1).cast("int").alias("LineID"),
        F.lit(0.0).alias("EffPercentage"),
        F.col("EffectiveAmount").alias("EffAmount"),
        F.col("UnderlyingTypeId"),
        F.coalesce(F.col("LineTypeID"), F.lit(-1)).alias("LineTypeID"),
        F.lit(False).alias("IsExcludefromTransfer"),
        F.lit(0).cast("int").alias("704cAllocationTypeID"),
        F.lit("").alias("704cPercentageType"),
        F.col("GPPartnerReceivingCarry"),
    ).distinct()

    # Clean up: delete AMOUNT-type rows from #TempAllUnderlyings if CAR active
    is_car = cfg.get("is_custom_allocation_rule_enabled", "U") == "C"
    if is_car:
        all_underlyings_cleaned = all_underlyings.filter(F.col("AllocationBy") != "AMOUNT")
    else:
        all_underlyings_cleaned = all_underlyings

    _log_timing("compute_amount_based_allocation", t0)
    return final_amounts, all_underlyings_cleaned
