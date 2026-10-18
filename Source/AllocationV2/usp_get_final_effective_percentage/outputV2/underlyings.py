"""
underlyings.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Asset class filtering, HLevel ordering, underlying pickup order logic.
Conversion date: 2026-05-04

SQL lines: 2500-2960
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
# filter_asset_class_underlyings
# SQL lines: 2500-2630
# Row count: ALWAYS-NON-EMPTY (input minus deleted rows)
# ---------------------------------------------------------------------------
def filter_asset_class_underlyings(
    spark: SparkSession, cfg: dict,
    underlyings_combined: DataFrame,
    entity_asset_class_rel: DataFrame,
) -> DataFrame:
    """Filter #TempAllUnderlyingsCombined by asset class matching rules.

    When OverrideIndirectLookthroughAssetClass != 'C':
        Build #MatchingAssetClass via 3 passes, then delete non-matching Asset Class rows.
    Else (== 'C'):
        Delete Asset Class rows where AssetClassID != entity's AssetClassID.

    Also handles IgnoreAssetclassForPartnershipLevel = 'C':
        Delete Asset Class rows where UnderlyingEntityId = @LocalEntityID.
    """
    t0 = time.time()
    logger.info("[SECTION] filter_asset_class_underlyings")

    override_flag = cfg.get("override_indirect_lookthrough_asset_class", "")
    ignore_flag = cfg.get("ignore_asset_class_for_partnership_level", "")
    entity_id = cfg["entity_id"]

    enu_ut = F.broadcast(_tbl(spark, "ENU_UnderlyingType", cfg))
    entity = F.broadcast(_tbl(spark, "Entity", cfg))

    # Alias for readability
    ac = underlyings_combined

    if override_flag != "C":
        # --- Pass 1: EAR rows with TrackingKey IS NULL ---
        ear_null_tk = entity_asset_class_rel.filter(F.col("TrackingKey").isNull())
        ear_notnull_tk = entity_asset_class_rel.filter(F.col("TrackingKey").isNotNull())

        matching1 = None
        if not ear_null_tk.isEmpty():
            matching1 = (
                ac.alias("AI")
                .join(
                    ear_null_tk.alias("EAR"),
                    F.col("AI.UnderlyingEntityID") == F.col("EAR.LowerTierEntityID"),
                )
                .join(
                    entity.alias("E"),
                    F.col("E.EntityID") == F.col("AI.UnderlyingEntityID"),
                )
                .join(
                    enu_ut.alias("U"),
                    F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"),
                )
                .filter(
                    (F.col("U.UnderlyingType") == "ASSET CLASS")
                    & (
                        F.when(
                            F.coalesce(F.col("EAR.AssetClassID"), F.lit(0)) == 0,
                            F.col("E.AssetClassID"),
                        ).otherwise(F.col("EAR.AssetClassID"))
                        == F.col("AI.AssetClassID")
                    )
                )
                .select(
                    F.col("AI.UnderlyingEntityID"), F.col("AI.EntityID"),
                    F.col("AI.HLevel"), F.col("AI.UnderlyingType"),
                    F.col("AI.AllocationTypeID"), F.col("AI.TrackingKey"),
                    F.col("AI.AssetClassID"), F.col("AI.ImmediateLowerTierEntityID"),
                )
                .distinct()
            )

        # --- Pass 2: EAR rows with TrackingKey IS NOT NULL ---
        matching2 = None
        if not ear_notnull_tk.isEmpty():
            matching2 = (
                ac.alias("AI")
                .join(
                    ear_notnull_tk.alias("EAR"),
                    (F.col("AI.UnderlyingEntityID") == F.col("EAR.LowerTierEntityID"))
                    & (
                        F.concat(F.lit("~"), F.col("EAR.TrackingKey"), F.lit("~"))
                        .contains(F.col("AI.TrackingKey"))
                    ),
                )
                .join(
                    entity.alias("E"),
                    F.col("E.EntityID") == F.col("AI.UnderlyingEntityID"),
                )
                .join(
                    enu_ut.alias("U"),
                    F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"),
                )
                .filter(
                    (F.col("U.UnderlyingType") == "ASSET CLASS")
                    & (
                        F.when(
                            F.coalesce(F.col("EAR.AssetClassID"), F.lit(0)) == 0,
                            F.col("E.AssetClassID"),
                        ).otherwise(F.col("EAR.AssetClassID"))
                        == F.col("AI.AssetClassID")
                    )
                )
                .select(
                    F.col("AI.UnderlyingEntityID"), F.col("AI.EntityID"),
                    F.col("AI.HLevel"), F.col("AI.UnderlyingType"),
                    F.col("AI.AllocationTypeID"), F.col("AI.TrackingKey"),
                    F.col("AI.AssetClassID"), F.col("AI.ImmediateLowerTierEntityID"),
                )
                .distinct()
            )

        # --- Combine matching1 + matching2 ---
        matching = None
        if matching1 is not None and matching2 is not None:
            matching = matching1.unionByName(matching2)
        elif matching1 is not None:
            matching = matching1
        elif matching2 is not None:
            matching = matching2

        # --- Pass 3: Remaining Asset Class rows with direct AssetClassID match ---
        if matching is not None:
            remaining = (
                ac.alias("AI")
                .join(
                    entity.alias("E"),
                    (F.col("E.EntityID") == F.col("AI.UnderlyingEntityID"))
                    & (F.col("AI.AssetClassID") == F.col("E.AssetClassID")),
                )
                .join(
                    enu_ut.alias("U"),
                    F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"),
                )
                .join(
                    matching.alias("M"),
                    (F.col("M.UnderlyingEntityID") == F.col("AI.UnderlyingEntityID"))
                    & (F.col("AI.TrackingKey") == F.col("M.TrackingKey"))
                    & (F.col("AI.ImmediateLowerTierEntityID") == F.col("M.ImmediateLowerTierEntityID")),
                    "left",
                )
                .filter(
                    (F.col("U.UnderlyingType") == "ASSET CLASS")
                    & (F.col("M.UnderlyingEntityID").isNull())
                )
                .select(
                    F.col("AI.UnderlyingEntityID"), F.col("AI.EntityID"),
                    F.col("AI.HLevel"), F.col("AI.UnderlyingType"),
                    F.col("AI.AllocationTypeID"), F.col("AI.TrackingKey"),
                    F.col("AI.AssetClassID"), F.col("AI.ImmediateLowerTierEntityID"),
                )
                .distinct()
            )
            matching = matching.unionByName(remaining)
        else:
            matching = (
                ac.alias("AI")
                .join(
                    entity.alias("E"),
                    (F.col("E.EntityID") == F.col("AI.UnderlyingEntityID"))
                    & (F.col("AI.AssetClassID") == F.col("E.AssetClassID")),
                )
                .join(
                    enu_ut.alias("U"),
                    F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"),
                )
                .filter(F.col("U.UnderlyingType") == "ASSET CLASS")
                .select(
                    F.col("AI.UnderlyingEntityID"), F.col("AI.EntityID"),
                    F.col("AI.HLevel"), F.col("AI.UnderlyingType"),
                    F.col("AI.AllocationTypeID"), F.col("AI.TrackingKey"),
                    F.col("AI.AssetClassID"), F.col("AI.ImmediateLowerTierEntityID"),
                )
                .distinct()
            )

        # --- Delete non-matching Asset Class rows ---
        if not entity_asset_class_rel.isEmpty():
            ac = (
                ac.alias("AI")
                .join(
                    enu_ut.alias("U"),
                    F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"),
                )
                .join(
                    matching.alias("M"),
                    (F.col("M.UnderlyingEntityID") == F.col("AI.UnderlyingEntityID"))
                    & (F.col("AI.TrackingKey") == F.col("M.TrackingKey"))
                    & (F.col("AI.AssetClassID") == F.col("M.AssetClassID"))
                    & (F.col("AI.ImmediateLowerTierEntityID") == F.col("M.ImmediateLowerTierEntityID")),
                    "left",
                )
                .filter(
                    (F.col("U.UnderlyingType") != "ASSET CLASS")
                    | (F.col("M.UnderlyingEntityID").isNotNull())
                )
                .select("AI.*")
            )
    else:
        # OverrideIndirectLookthroughAssetClass == 'C'
        # Simple delete: non-matching AssetClassID via ImmediateLowerTierEntityID
        ac = (
            ac.alias("AI")
            .join(
                entity_asset_class_rel.alias("EAR"),
                F.col("AI.ImmediateLowerTierEntityID") == F.col("EAR.LowerTierEntityID"),
                "left",
            )
            .join(
                entity.alias("E"),
                F.col("E.EntityID") == F.col("AI.ImmediateLowerTierEntityID"),
                "left",
            )
            .join(
                enu_ut.alias("U"),
                F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"),
                "left",
            )
            .filter(
                (F.col("U.UnderlyingType") != "ASSET CLASS")
                | (F.col("EAR.LowerTierEntityID").isNull())
                | (
                    F.when(
                        F.coalesce(F.col("EAR.AssetClassID"), F.lit(0)) == 0,
                        F.col("E.AssetClassID"),
                    ).otherwise(F.col("EAR.AssetClassID"))
                    == F.col("AI.AssetClassID")
                )
            )
            .select("AI.*")
        )

    # Handle IgnoreAssetclassForPartnershipLevel = 'C'
    if ignore_flag == "C":
        ac = (
            ac.alias("AI")
            .join(
                enu_ut.alias("U"),
                F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"),
                "left",
            )
            .filter(
                ~(
                    (F.col("AI.UnderlyingEntityID") == entity_id)
                    & (F.col("U.UnderlyingType") == "ASSET CLASS")
                )
            )
            .select("AI.*")
        )

    _log_timing("filter_asset_class_underlyings", t0)
    return ac


# ---------------------------------------------------------------------------
# build_underlyings_hlevel_ordered
# SQL lines: 2643-2660
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_underlyings_hlevel_ordered(
    underlyings_combined: DataFrame,
) -> DataFrame:
    """Update HLevel to MAX per group in #TempAllUnderlyingsCombined.

    Groups by: UnderlyingEntityId, Entityid, Underlyingtype, AllocationTypeId, TrackingKey.
    Sets HLevel to the max within each group.
    """
    t0 = time.time()
    logger.info("[SECTION] build_underlyings_hlevel_ordered")

    w = Window.partitionBy(
        "UnderlyingEntityID", "EntityID", "UnderlyingType",
        "AllocationTypeID", "TrackingKey",
    )

    result = underlyings_combined.withColumn(
        "HLevel", F.max("HLevel").over(w)
    )

    _log_timing("build_underlyings_hlevel_ordered", t0)
    return result


# ---------------------------------------------------------------------------
# build_underlying_mod
# SQL lines: 2663-2672
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_underlying_mod(
    underlyings_combined: DataFrame,
    cost_pct_snapshot: DataFrame,
) -> DataFrame:
    """Build #tempunderlyingMod from #TempAllUnderlyingsCombined JOIN #CostPercentage_Snapshot.

    Joins on Cost_* columns to bring in Tag from cost percentage snapshot.
    """
    t0 = time.time()
    logger.info("[SECTION] build_underlying_mod")

    result = (
        underlyings_combined.alias("AI")
        .join(
            cost_pct_snapshot.alias("C"),
            (F.col("AI.Cost_Entity") == F.col("C.EntityID"))
            & (F.col("AI.Cost_InvestmentID") == F.col("C.InvestmentID"))
            & (F.col("AI.Cost_Quarter") == F.col("C.Quarter"))
            & (F.col("AI.Cost_AllocationTypeID") == F.col("C.AllocationTypeID"))
            & (
                F.coalesce(F.col("AI.Cost_TrackingKey"), F.lit(""))
                == F.coalesce(F.col("C.TrackingKey"), F.lit(""))
            )
            & (F.col("AI.Cost_UnderlyingType") == F.col("C.UnderlyingType")),
        )
        .select(
            F.col("AI.UnderlyingType"),
            F.col("AI.UnderlyingEntityID"),
            F.col("AI.EntityID"),
            F.col("AI.TrackingKey"),
            F.col("AI.AllocationTypeID"),
            F.col("AI.HLevel").alias("hlevel"),
            F.col("C.Tag"),
        )
        .distinct()
    )

    if result.isEmpty():
        logger.warning("build_underlying_mod produced 0 rows")

    _log_timing("build_underlying_mod", t0)
    return result


# ---------------------------------------------------------------------------
# build_all_underlyings_ordered
# SQL lines: 2800-2960
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_all_underlyings_ordered(
    spark: SparkSession, cfg: dict,
    underlying_mod: DataFrame,
    lt_input: DataFrame,
    book_effective: DataFrame,
    entity_alloc_rule: DataFrame,
    dar_setup: DataFrame,
    map_dar: DataFrame,
    cost_pct_snapshot: DataFrame,
) -> tuple:
    """Build #TempAllUnderlyingsOrdered with ROW_NUMBER for pickup order.

    Two paths:
      1. CAR (IsCustomAllocationRuleEnabled='C'): Uses K1LineItem for AllocationTypeRuleId
      2. DAR/default: Uses MapDefaultAllocRuleToLineItem + DefaultAllocationRuleSetup

    After building ordered, filters to RankForUnderlyingPickup=1 → #TempAllUnderlyings.
    Also builds #TempDefaultAllocationRule.

    Returns: (all_underlyings, default_alloc_rule)
    """
    t0 = time.time()
    logger.info("[SECTION] build_all_underlyings_ordered")

    entity_id = cfg["entity_id"]
    is_car = cfg.get("is_custom_allocation_rule_enabled", "U") == "C"
    override_flag = cfg.get("override_indirect_lookthrough_asset_class", "")
    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    adj_lt_id = cfg["adjustment_line_type_id"]
    _704c_alloc_type_id = cfg.get("_704c_allocation_type_id")
    mode = cfg.get("mode")
    dar_tid = cfg.get("default_alloc_rule_transaction_id")
    gdar_tid = cfg.get("global_default_alloc_rule_transaction_id")

    enu_ut = F.broadcast(_tbl(spark, "ENU_UnderlyingType", cfg))

    # Build line type normalization expression
    adj_to_k1 = F.when(
        F.col("L.LineTypeID") == adj_lt_id, F.lit(k1_lt_id).cast("int")
    ).otherwise(F.col("L.LineTypeID"))

    # Tracking key matching expression
    def _tracking_match(ai_entity_col, ai_underlying_col, ai_tracking_col, l_tracking_col, u_type_col):
        """CASE WHEN entity=@local OR (entity=@local AND ut!='Asset Class')
        OR (override!='C' AND ut='Asset Class') THEN '-1'
        ELSE '~' + TrackingKey + '~' END"""
        direct = (
            (ai_underlying_col == entity_id)
            | ((ai_entity_col == entity_id) & (u_type_col != "ASSET CLASS"))
            | ((F.lit(override_flag) != "C") & (u_type_col == "ASSET CLASS"))
        )
        ai_side = F.when(direct, F.lit("-1")).otherwise(
            F.concat(F.lit("%"), ai_tracking_col, F.lit("%"))
        )
        l_side = F.when(direct, F.lit("-1")).otherwise(
            F.concat(F.lit("~"), l_tracking_col, F.lit("~"))
        )
        return l_side.like(ai_side)

    # Tag matching
    def _tag_match(tag1, tag2):
        cond = F.coalesce(tag1, F.lit("")) == ""
        k1 = F.when(cond, F.lit("-1")).otherwise(tag1)
        k2 = F.when(cond, F.lit("-1")).otherwise(tag2)
        return k1 == k2

    ordered_parts = []

    # --- Path 1: CAR (Custom Allocation Rule) ---
    if is_car:
        # Build #K1LineItem
        k1_line_item = (
            _tbl(spark, "K1LineItem", cfg).alias("K")
            .join(lt_input.alias("I"), F.col("K.LineID") == F.col("I.LineID"))
            .select(
                F.col("K.LineID"),
                F.when(
                    (F.lit(mode) == 4) & (F.col("K.AllocationTypeRuleId") == cost_alloc_type_id),
                    F.lit(_704c_alloc_type_id).cast("int"),
                ).otherwise(F.col("K.AllocationTypeRuleId")).alias("AllocationTypeRuleID"),
            )
            .distinct()
        )

        # Build ordered with K1LineItem
        car_part = (
            underlying_mod.alias("AI")
            .join(
                enu_ut.alias("U"),
                F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"),
            )
            .join(
                lt_input.alias("L"),
                (F.col("L.EntityID") == F.col("AI.UnderlyingEntityID"))
                & (
                    _tracking_match(
                        F.col("AI.EntityID"), F.col("AI.UnderlyingEntityID"),
                        F.col("AI.TrackingKey"), F.col("L.TrackingKey"),
                        F.col("U.UnderlyingType"),
                    )
                )
                & (_tag_match(F.col("AI.Tag"), F.col("L.Tag"))),
            )
            .join(
                book_effective.alias("B"),
                (F.col("L.EntityID") == F.col("B.UnderlyingEntityID"))
                & (F.col("L.LineID") == F.col("B.LineID"))
                & (F.col("B.LineID") != -1)
                & (F.col("B.SourceID") == F.col("L.LineTypeID"))
                & (
                    F.when(F.coalesce(F.col("B.TrackingKey"), F.lit("")) == "", F.lit("-1"))
                    .otherwise(F.col("B.TrackingKey"))
                    == F.when(F.coalesce(F.col("B.TrackingKey"), F.lit("")) == "", F.lit("-1"))
                    .otherwise(F.col("L.TrackingKey"))
                )
                & (
                    F.when(F.coalesce(F.col("B.Tag"), F.lit("")) == "", F.lit("-1"))
                    .otherwise(F.col("B.Tag"))
                    == F.when(F.coalesce(F.col("B.Tag"), F.lit("")) == "", F.lit("-1"))
                    .otherwise(F.col("L.Tag"))
                ),
                "left",
            )
            .join(
                k1_line_item.alias("K"),
                (F.col("L.LineID") == F.col("K.LineID"))
                & (
                    F.coalesce(
                        F.col("B.AdjustmentAllocationTypeID"),
                        F.coalesce(F.col("K.AllocationTypeRuleID"), F.lit(cost_alloc_type_id).cast("int")),
                    )
                    == F.col("AI.AllocationTypeID")
                ),
            )
            .select(
                F.col("AI.UnderlyingType").alias("Underlyingtype"),
                F.col("AI.UnderlyingEntityID").alias("UnderlyingEntityId"),
                F.col("AI.EntityID").alias("EntityId"),
                F.col("L.TrackingKey"),
                F.col("AI.TrackingKey").alias("TrackingMatch"),
                F.coalesce(
                    F.col("B.AdjustmentAllocationTypeID"),
                    F.coalesce(F.col("K.AllocationTypeRuleID"), F.lit(cost_alloc_type_id).cast("int")),
                ).alias("AllocationTypeId"),
                F.col("L.LineID"),
                F.row_number().over(
                    Window.partitionBy(
                        F.col("AI.UnderlyingEntityID"),
                        F.col("L.TrackingKey"),
                        F.col("L.LineID"),
                        adj_to_k1,
                        F.col("AI.AllocationTypeID"),
                    ).orderBy(
                        F.col("AI.hlevel"),
                        F.col("U.DisplayOrder"),
                        F.col("AI.TrackingKey"),
                    )
                ).alias("RankForUnderlyingPickup"),
                adj_to_k1.alias("LineTypeID"),
                F.lit("PERCENT").alias("AllocationBy"),
                F.lit(False).alias("IsExcludefromTransfer"),
            )
        )
        ordered_parts.append(car_part)

    # --- Path 2: DAR (Default Allocation Rule) / Non-CAR ---
    # Also runs when CAR is active AND 704c mappings exist
    has_704c_mappings = cfg.get("has_704c_mappings", False)

    if not is_car or has_704c_mappings:
        # Filter transaction IDs
        valid_tids = [t for t in [dar_tid, gdar_tid, -2] if t is not None]

        dar_part = (
            underlying_mod.alias("AI")
            .join(
                enu_ut.alias("U"),
                F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"),
            )
            .join(
                lt_input.alias("L"),
                (F.col("L.EntityID") == F.col("AI.UnderlyingEntityID"))
                & (
                    _tracking_match(
                        F.col("AI.EntityID"), F.col("AI.UnderlyingEntityID"),
                        F.col("AI.TrackingKey"), F.col("L.TrackingKey"),
                        F.col("U.UnderlyingType"),
                    )
                ),
            )
            .join(
                map_dar.alias("M"),
                (
                    F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                    .otherwise(F.col("L.LineID"))
                    == F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                    .otherwise(F.col("M.SelectedMappingID"))
                )
                & (F.col("M.RuleID") == F.col("AI.AllocationTypeID"))
                & (F.col("M.SourceID") == adj_to_k1),
            )
            .join(
                dar_setup.alias("D"),
                (F.col("D.RuleID") == F.col("AI.AllocationTypeID"))
                & (F.col("AI.UnderlyingType") == F.col("D.UnderlyingTypeID")),
            )
            .join(
                F.broadcast(_tbl(spark, "ENU_RuleType", cfg)).alias("R"),
                F.col("D.RuleTypeID") == F.col("R.RuleTypeID"),
            )
            .join(
                F.broadcast(_tbl(spark, "ENU_AllocationBy", cfg)).alias("EA"),
                F.col("D.AllocationByID") == F.col("EA.AllocationByID"),
            )
            .filter(
                (F.col("M.TransactionID").isin(valid_tids))
                & (F.col("D.TransactionID").isin(valid_tids))
            )
            .select(
                F.col("AI.UnderlyingType").alias("Underlyingtype"),
                F.col("AI.UnderlyingEntityID").alias("UnderlyingEntityId"),
                F.col("AI.EntityID").alias("EntityId"),
                F.col("L.TrackingKey"),
                F.col("AI.TrackingKey").alias("TrackingMatch"),
                F.col("AI.AllocationTypeID").alias("AllocationTypeId"),
                F.col("L.LineID"),
                F.row_number().over(
                    Window.partitionBy(
                        F.col("AI.UnderlyingEntityID"),
                        F.col("L.TrackingKey"),
                        F.col("L.LineID"),
                        adj_to_k1,
                        F.col("EA.DisplayOrder"),
                    ).orderBy(
                        F.col("AI.hlevel"),
                        F.col("R.DisplayOrder").desc(),
                        F.col("U.DisplayOrder"),
                        F.col("M.SelectedMappingID").desc(),
                        F.col("EA.DisplayOrder"),
                        F.col("AI.TrackingKey"),
                    )
                ).alias("RankForUnderlyingPickup"),
                adj_to_k1.alias("LineTypeID"),
                F.col("EA.AllocationBy"),
                F.coalesce(F.col("M.ExcludeFromTransfers"), F.lit(0))
                .cast("boolean")
                .alias("IsExcludefromTransfer"),
            )
        )
        ordered_parts.append(dar_part)

    # Union all ordered parts
    if len(ordered_parts) == 1:
        all_ordered = ordered_parts[0]
    else:
        all_ordered = ordered_parts[0].unionByName(ordered_parts[1])

    # #TempAllUnderlyings = rank 1 only
    all_underlyings = all_ordered.filter(F.col("RankForUnderlyingPickup") == 1)

    # --- Build #TempDefaultAllocationRule ---
    # Part 1: Entity-specific DAR
    map_rules_underlyings = _tbl(spark, "MapRulesToUnderlyings", cfg)

    dar_rule_p1 = (
        map_dar.alias("M")
        .join(
            all_underlyings.alias("L"),
            F.col("M.SelectedMappingID") == F.col("L.LineID"),
        )
        .join(
            enu_ut.alias("U"),
            F.col("L.Underlyingtype") == F.col("U.UnderlyingTypeID"),
        )
        .join(
            map_rules_underlyings.alias("MU"),
            (
                F.when(F.col("U.UnderlyingType") == "ASSET CLASS", F.lit("1"))
                .otherwise(F.col("M.RuleID").cast("string"))
                == F.when(F.col("U.UnderlyingType") == "ASSET CLASS", F.lit("1"))
                .otherwise(F.col("MU.RuleID").cast("string"))
            )
            & (
                F.when(F.col("U.UnderlyingType") == "ASSET CLASS", F.lit("1"))
                .otherwise(F.col("L.EntityId").cast("string"))
                == F.when(F.col("U.UnderlyingType") == "ASSET CLASS", F.lit("1"))
                .otherwise(F.col("MU.UnderlyingID").cast("string"))
            ),
        )
        .filter(
            (F.col("M.TransactionID") == dar_tid)
            & (F.col("MU.TransactionID") == dar_tid)
        )
        .select(
            F.col("M.SelectedMappingID").alias("LineId"),
            F.col("M.RuleID").alias("AllocationRuleID"),
            F.col("L.UnderlyingEntityId").alias("EntityID"),
        )
    )

    # Part 2: Global DAR
    dar_rule_p2 = (
        map_dar.alias("M")
        .filter(F.col("M.TransactionID") == gdar_tid)
        .select(
            F.col("M.SelectedMappingID").alias("LineId"),
            F.col("M.RuleID").alias("AllocationRuleID"),
            F.col("M.EntityID"),
        )
    )

    default_alloc_rule = dar_rule_p1.unionByName(dar_rule_p2)

    _log_timing("build_all_underlyings_ordered", t0)
    return all_underlyings, default_alloc_rule
