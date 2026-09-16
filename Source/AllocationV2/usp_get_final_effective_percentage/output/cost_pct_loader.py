"""
cost_pct_loader.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Cost percentage by underlying type (3-tier), transfer adjusted cost,
entity matching, missing entity fallback, minimum quarter calculation.
Conversion date: 2026-05-04

SQL lines: 6210-7440
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


def _iif_inv(inv_col, ent_col):
    """IIF(InvestmentID = -1, EntityID, InvestmentID) pattern."""
    return F.when(inv_col == -1, ent_col).otherwise(inv_col)


def _tracking_match_expr(tracking_key_col, inv_col, ent_col):
    """Build '~' + CASE WHEN TrackingKey='' THEN InvestmentID ELSE TrackingKey END + '~'."""
    return F.concat(
        F.lit("~"),
        F.when(
            F.coalesce(tracking_key_col, F.lit("")) == "",
            _iif_inv(inv_col, ent_col).cast("string"),
        ).otherwise(tracking_key_col),
        F.lit("~"),
    )


# ---------------------------------------------------------------------------
# build_entity_underlyings
# SQL lines: 6215-6240
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_entity_underlyings(
    spark: SparkSession, cfg: dict,
    input_lines: DataFrame,
    underlyings_combined: DataFrame,
    asset_class_rel: DataFrame,
) -> DataFrame:
    """Build #TempEntityUnderlying for asset class cost percentage matching.

    Two paths:
    - OverrideIndirectLookthrough != 'C': Direct from input_lines + Entity
    - OverrideIndirectLookthrough == 'C': Through underlyings_combined hierarchy
    """
    t0 = time.time()
    logger.info("[SECTION] build_entity_underlyings")

    override_flag = cfg.get("override_indirect_lookthrough_asset_class", "")
    entity_id = cfg["entity_id"]
    ignore_ac = cfg.get("ignore_asset_class_for_partnership_level", "")

    entity = F.broadcast(_tbl(spark, "Entity", cfg))

    if override_flag != "C":
        result = (
            input_lines.alias("L")
            .join(
                asset_class_rel.alias("EAR"),
                (F.col("L.UnderlyingEntityID") == F.col("EAR.LowerTierEntityID"))
                & (
                    F.when(
                        F.coalesce(F.col("EAR.TrackingKey"), F.lit("")) == "",
                        F.coalesce(F.col("L.TrackingKey"), F.lit("")),
                    ).otherwise(F.col("EAR.TrackingKey"))
                    == F.coalesce(F.col("L.TrackingKey"), F.lit(""))
                ),
                "left",
            )
            .join(
                entity.alias("E"),
                F.col("L.UnderlyingEntityID") == F.col("E.EntityID"),
            )
            .filter(
                ((F.lit(ignore_ac) == "C") & (F.col("E.EntityID") != entity_id))
                | (F.lit(ignore_ac) != "C")
            )
            .select(
                F.col("L.UnderlyingEntityID").alias("UnderlyingEntityId"),
                F.when(
                    F.coalesce(F.col("EAR.AssetClassID"), F.lit(0)) == 0,
                    F.col("E.AssetClassID"),
                ).otherwise(F.col("EAR.AssetClassID")).alias("AssetClassId"),
                F.col("L.TrackingKey"),
            )
            .distinct()
        )
    else:
        uc_distinct = underlyings_combined.select(
            "UnderlyingEntityId", "ImmediateLowerTierEntityID",
        ).distinct()

        result = (
            input_lines.alias("L")
            .join(
                uc_distinct.alias("TC"),
                (F.col("TC.UnderlyingEntityId") == F.col("L.UnderlyingEntityID"))
                & (
                    F.col("TC.ImmediateLowerTierEntityID")
                    == F.when(
                        ~F.col("L.TrackingKey").contains("~"),
                        F.col("L.TrackingKey"),
                    ).otherwise(
                        F.regexp_extract(F.col("L.TrackingKey"), r"([^~]+)$", 1),
                    )
                ),
            )
            .join(
                asset_class_rel.alias("EAR"),
                F.col("TC.ImmediateLowerTierEntityID") == F.col("EAR.LowerTierEntityID"),
                "left",
            )
            .join(
                entity.alias("E"),
                F.col("E.EntityID") == F.col("TC.ImmediateLowerTierEntityID"),
            )
            .select(
                F.col("L.UnderlyingEntityID").alias("UnderlyingEntityId"),
                F.when(
                    F.coalesce(F.col("EAR.AssetClassID"), F.lit(0)) == 0,
                    F.col("E.AssetClassID"),
                ).otherwise(F.col("EAR.AssetClassID")).alias("AssetClassId"),
                F.col("L.TrackingKey"),
            )
            .distinct()
        )

    # Note: legitimate emptiness after Path A.
    # The orchestrator builds this chain twice in Common Phase 2 — once with
    # populated lt_input (mode 1) and once with empty lt_input (modes 2/3,
    # per SP `IF @LocalMode IN (1, 4)`). For the empty-lt chain, input_lines
    # has no K1/Adj/BoxJKL rows from LookThroughAllocationInput, so this
    # function naturally returns 0 rows. That's correct, not a bug.
    if result.isEmpty():
        if input_lines.isEmpty():
            logger.warning(
                "build_entity_underlyings: 0 rows because input_lines is empty "
                "(SP-correct for the without-lt chain serving modes 2/3)."
            )
        else:
            # Genuinely unexpected — input_lines has data but the join produced
            # nothing. Likely a join-condition or asset-class-rel data issue.
            logger.warning(
                "build_entity_underlyings returned 0 rows but input_lines is "
                "non-empty — investigate join with asset_class_rel / Entity."
            )

    _log_timing("build_entity_underlyings", t0)
    return result


# ---------------------------------------------------------------------------
# load_transfers_adj_cost
# SQL lines: 6350-6445
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def load_transfers_adj_cost(
    spark: SparkSession, cfg: dict,
    all_underlyings: DataFrame,
    entity_underlyings: DataFrame,
) -> DataFrame:
    """Load TransfersAdjCostDefaultPercentage and match to underlyings.

    Only for mode != 4. Returns matched transfer adj cost data.
    """
    t0 = time.time()
    logger.info("[SECTION] load_transfers_adj_cost")

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    asset_class_ut = cfg.get("asset_class_underlying_type_id")

    raw = (
        _tbl(spark, "TransfersAdjCostDefaultPercentage", cfg).alias("T")
        .filter(
            (F.col("T.RunID") == run_id)
            & (F.col("T.ClientID") == client_id)
        )
        .select(
            "*",
            F.when(
                F.coalesce(F.col("isEODTransfer").cast("int"), F.lit(0)) == F.lit(1),
                F.date_add(F.col("TransferDate"), 1),
            ).otherwise(F.col("TransferDate")).alias("AdjTransferDate"),
            _tracking_match_expr(F.col("TrackingKey"),
                                 F.col("InvestmentID"), F.col("EntityID")).alias("FormattedTrackingKey"),
            _iif_inv(F.col("InvestmentID"), F.col("EntityID")).alias("FormattedEntityID"),
        )
    )

    # Match 1: Direct entity match (non-asset-class)
    match1 = (
        raw.alias("T")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == F.col("T.FormattedEntityID"))
            & (F.col("E.AllocationTypeId") == F.col("T.AllocationTypeID"))
            & (F.col("T.TrackingKey") == F.col("E.TrackingKey"))
            & (F.col("E.Underlyingtype") == F.col("T.Underlyingtype")),
        )
        .filter(F.col("T.Underlyingtype") != asset_class_ut)
        .select(
            F.col("E.UnderlyingEntityId").alias("InvestmentID"),
            F.col("T.TransferPartnerNumber"),
            F.col("T.AdjTransferDate").alias("TransferDate"),
            F.col("T.EndingCostPercent"),
            F.col("T.PartnerNumber"),
            F.col("T.EffectivePercent"),
            F.coalesce(F.col("T.AllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("TypeID"),
            F.coalesce(F.col("T.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("T.Tag"), F.lit("")).alias("Tag"),
            F.lit(None).cast("string").alias("TrackingKeyMatch"),
            F.col("E.Underlyingtype"),
        )
        .distinct()
    )

    # Match 2: TrackingMatch pattern match for entities not in match1
    defined_deals = match1.select(
        F.col("InvestmentID"),
        F.col("TypeID").alias("TypeId"),
        F.col("Tag"),
        F.col("TrackingKey"),
    ).distinct()

    match2 = (
        raw.alias("T")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == F.col("T.FormattedEntityID"))
            & (F.col("E.AllocationTypeId") == F.col("T.AllocationTypeID"))
            & (F.col("T.FormattedTrackingKey") == F.col("E.TrackingMatch")),
        )
        .join(
            defined_deals.alias("D"),
            (F.col("E.UnderlyingEntityId") == F.col("D.InvestmentID"))
            & (F.col("D.Tag") == F.coalesce(F.col("T.Tag"), F.lit("")))
            & (F.col("D.TypeId") == F.coalesce(F.col("T.AllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")))
            & (F.col("D.TrackingKey") == F.coalesce(F.col("T.TrackingKey"), F.lit(""))),
            "left",
        )
        .filter(
            (F.col("D.InvestmentID").isNull())
            & (F.col("T.Underlyingtype") != asset_class_ut)
        )
        .select(
            F.col("E.UnderlyingEntityId").alias("InvestmentID"),
            F.col("T.TransferPartnerNumber"),
            F.col("T.AdjTransferDate").alias("TransferDate"),
            F.col("T.EndingCostPercent"),
            F.col("T.PartnerNumber"),
            F.col("T.EffectivePercent"),
            F.coalesce(F.col("T.AllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("TypeID"),
            F.coalesce(F.col("T.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("T.Tag"), F.lit("")).alias("Tag"),
            F.when(
                (F.col("T.FormattedEntityID") != F.col("T.EntityID"))
                & (F.coalesce(F.col("T.TrackingKey"), F.lit("")) == ""),
                F.col("E.TrackingMatch"),
            ).otherwise(F.lit(None).cast("string")).alias("TrackingKeyMatch"),
            F.col("E.Underlyingtype"),
        )
        .distinct()
    )

    # Match 3: Asset class underlyings
    match3 = (
        raw.alias("T")
        .join(
            entity_underlyings.alias("EU"),
            F.col("EU.AssetClassId") == F.col("T.InvestmentID"),
        )
        .join(
            all_underlyings.alias("E"),
            (F.col("EU.UnderlyingEntityId") == F.col("E.UnderlyingEntityId"))
            & (F.col("EU.TrackingKey") == F.col("E.TrackingKey"))
            & (F.col("T.Underlyingtype") == F.col("E.Underlyingtype")),
        )
        .filter(F.col("T.Underlyingtype") == asset_class_ut)
        .select(
            F.col("E.UnderlyingEntityId").alias("InvestmentID"),
            F.col("T.TransferPartnerNumber"),
            F.col("T.AdjTransferDate").alias("TransferDate"),
            F.col("T.EndingCostPercent"),
            F.col("T.PartnerNumber"),
            F.col("T.EffectivePercent"),
            F.coalesce(F.col("T.AllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("TypeID"),
            F.coalesce(F.col("E.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("T.Tag"), F.lit("")).alias("Tag"),
            F.when(
                (F.col("T.FormattedEntityID") != F.col("T.EntityID"))
                & (F.coalesce(F.col("T.TrackingKey"), F.lit("")) == ""),
                F.col("E.TrackingMatch"),
            ).otherwise(F.lit(None).cast("string")).alias("TrackingKeyMatch"),
            F.col("E.Underlyingtype"),
        )
        .distinct()
    )

    result = match1.unionByName(match2).unionByName(match3)

    _log_timing("load_transfers_adj_cost", t0)
    return result


# ---------------------------------------------------------------------------
# build_cost_percentage_by_type
# SQL lines: 6450-6950
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_cost_percentage_by_type(
    spark: SparkSession, cfg: dict,
    cost_pct_snapshot: DataFrame,
    temp_cost_pct: DataFrame,
    all_underlyings: DataFrame,
    entity_underlyings: DataFrame,
    non_dated: DataFrame,
    dated: DataFrame,
    transfers_adj: DataFrame,
    checkpoint_fn=None,
) -> tuple:
    """3-tier cost percentage loading + entity parent matching.

    Priority order:
    1. Underlying Only type (tracking key match → tracking match)
    2. Entity Total type (tracking key match → tracking match)
    3. Asset Class type (asset class id → InvestmentID=-1 cross join)
    4. Parent hierarchy matching with TrackingKeyMatch
    5. Parent hierarchy matching without TrackingKeyMatch
    6. Tag-only matching
    7. Empty tracking key + tag matching

    Returns: (updated_temp_cost_pct, updated_transfers_adj)
    """
    t0 = time.time()
    logger.info("[SECTION] build_cost_percentage_by_type")

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    entity_ut = cfg.get("entity_underlying_type_id")
    uo_ut = cfg.get("underlying_only_type_id")
    et_ut = cfg.get("entity_total_underlying_type_id")
    ac_ut = cfg.get("asset_class_underlying_type_id")

    # Broadcast cps — it's a small per-(client, period) lookup table
    # joined 6+ times against all_underlyings / entity_underlyings.
    # Broadcasting converts every sort-merge join to a broadcast hash join,
    # eliminating 6 shuffles in the UO + ET + AC priority steps.
    cps = F.broadcast(
        cost_pct_snapshot.filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
    )

    cost_pct_cols = [
        "DealId", "PartnerNumber", "Quarter", "CommitmentPercent", "TypeId",
        "TrackingKey", "Tag", "UnderlyingType",
        F.col("`704cAllocationTypeID`"), F.col("`704cPercentageType`"),
        "GPPartnerReceivingCarry",
    ]

    def _select_cost_pct(df, underlying_entity_col, tracking_key_col, underlying_type_col=None):
        """Standard select for cost percentage inserts.

        Phase 2a (_mode-aware):  Projects ``E._mode`` so unioned-mode rows in
        ``temp_cost_pct`` keep their source-mode tag. ``cps`` does not have a
        ``_mode`` column — joins inherit ``_mode`` from the right side ``E``
        (``all_underlyings`` / ``entity_underlyings``).
        """
        cols = [
            underlying_entity_col.alias("DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.coalesce(F.col("C.CommitmentPercent"), F.lit(0)).alias("CommitmentPercent"),
            F.coalesce(F.col("C.AllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("TypeId"),
            tracking_key_col.alias("TrackingKey"),
            F.coalesce(F.col("C.Tag"), F.lit("")).alias("Tag"),
        ]
        if underlying_type_col is not None:
            cols.append(underlying_type_col.alias("UnderlyingType"))
        else:
            cols.append(F.col("C.UnderlyingType"))
        cols.extend([
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("E._mode"),
        ])
        return df.select(*cols).distinct()

    def _dedup_key():
        return ["DealId", "TypeId", "Tag", "TrackingKey"]

    # Helper: get already-defined deals from current cost_pct
    def _existing_deals(cp):
        # Used only as the right side of left_anti joins below — anti-joins do not
        # require unique right-side rows for correctness. Skipping .distinct() saves
        # one shuffle per priority-match step (called 6+ times in this function).
        # Phase 2a: project _mode so anti-dedup can match by mode.
        return cp.select(
            F.col("DealId").alias("DealID"),
            F.col("TypeId"),
            F.col("Tag"),
            F.col("TrackingKey"),
            F.col("_mode"),
        )

    def _anti_dedup(new_rows, existing):
        # Phase 2a: add _mode equality so a match in mode 1 doesn't suppress
        # an otherwise-identical row coming from mode 2 (or vice versa). When
        # the function is called per-mode (Phase 2a), all rows on both sides
        # have the same _mode and this predicate is trivially true; the
        # safety net is for Phase 2b's fused execution.
        return (
            new_rows.alias("N")
            .join(
                existing.alias("D"),
                (F.col("N.DealId") == F.col("D.DealID"))
                & (F.col("D.Tag") == F.coalesce(F.col("N.Tag"), F.lit("")))
                & (F.col("D.TypeId") == F.col("N.TypeId"))
                & (F.col("D.TrackingKey") == F.coalesce(F.col("N.TrackingKey"), F.lit("")))
                & (F.col("D._mode") == F.col("N._mode")),
                "left_anti",
            )
        )

    # ── Underlying Only: Priority 1 (tracking key match) ──
    existing1 = _existing_deals(temp_cost_pct)
    uo_p1 = _select_cost_pct(
        cps.alias("C")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == _iif_inv(F.col("C.InvestmentID"), F.col("C.EntityID")))
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (F.coalesce(F.col("C.TrackingKey"), F.lit("")) == F.col("E.TrackingKey")),
        )
        .filter(F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")) == uo_ut),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("E.TrackingKey"), F.lit("")),
    )
    uo_p1 = _anti_dedup(uo_p1, existing1)
    temp_cost_pct = temp_cost_pct.unionByName(uo_p1, allowMissingColumns=True)

    # ── Underlying Only: Priority 2 (tracking match) ──
    existing2 = _existing_deals(temp_cost_pct)
    uo_p2 = _select_cost_pct(
        cps.alias("C")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == _iif_inv(F.col("C.InvestmentID"), F.col("C.EntityID")))
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (_tracking_match_expr(F.col("C.TrackingKey"), F.col("C.InvestmentID"), F.col("C.EntityID"))
               == F.col("E.TrackingMatch")),
        )
        .filter(F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")) == uo_ut),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("E.TrackingKey"), F.lit("")),
    )
    uo_p2 = _anti_dedup(uo_p2, existing2)
    temp_cost_pct = temp_cost_pct.unionByName(uo_p2, allowMissingColumns=True)

    # ── Entity Total: Priority 1 (tracking key match) ──
    existing_et1 = _existing_deals(temp_cost_pct)
    et_p1 = _select_cost_pct(
        cps.alias("C")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == _iif_inv(F.col("C.InvestmentID"), F.col("C.EntityID")))
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (F.col("C.TrackingKey") == F.col("E.TrackingKey")),
        )
        .filter(F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")) == et_ut),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("C.TrackingKey"), F.lit("")),
    )
    et_p1 = _anti_dedup(et_p1, existing_et1)
    temp_cost_pct = temp_cost_pct.unionByName(et_p1, allowMissingColumns=True)

    # ── Entity Total: Priority 2 (tracking match, with TrackingKeyMatch) ──
    existing_et2 = _existing_deals(temp_cost_pct)
    et_p2 = (
        cps.alias("C")
        .join(
            all_underlyings.select(
                "EntityId", "AllocationTypeId", "TrackingMatch",
                "UnderlyingEntityId", "TrackingKey", "_mode",
            ).distinct().alias("E"),
            (F.col("E.EntityId") == _iif_inv(F.col("C.InvestmentID"), F.col("C.EntityID")))
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (_tracking_match_expr(F.col("C.TrackingKey"), F.col("C.InvestmentID"), F.col("C.EntityID"))
               == F.col("E.TrackingMatch")),
        )
        .filter(F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")) == et_ut)
        .select(
            F.col("E.UnderlyingEntityId").alias("DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.coalesce(F.col("C.CommitmentPercent"), F.lit(0)).alias("CommitmentPercent"),
            F.coalesce(F.col("C.AllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("TypeId"),
            F.coalesce(F.col("C.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("C.Tag"), F.lit("")).alias("Tag"),
            F.when(
                (_iif_inv(F.col("C.InvestmentID"), F.col("C.EntityID")) != F.col("C.EntityID"))
                & (F.coalesce(F.col("C.TrackingKey"), F.lit("")) == ""),
                F.col("E.TrackingMatch"),
            ).otherwise(F.lit(None).cast("string")).alias("TrackingKeyMatch"),
            F.col("C.UnderlyingType"),
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("E._mode"),
        )
        .distinct()
    )
    et_p2 = _anti_dedup(et_p2, existing_et2)

    temp_cost_pct = temp_cost_pct.unionByName(et_p2, allowMissingColumns=True)

    # ── Intermediate checkpoint: break DAG after UO+ET steps (4 joins) ──
    # Steps 1-4 each join cps × all_underlyings with anti-dedup chains.
    # Without this, the Asset Class steps must optimize through the full
    # nested 4-union plan to evaluate _existing_deals(temp_cost_pct).
    if checkpoint_fn is not None:
        mode = cfg.get("_current_mode", 1)
        temp_cost_pct = checkpoint_fn(spark, temp_cost_pct, f"tcp_post_et_m{mode}", cfg)
        logger.info("[CHECKPOINT] temp_cost_pct after UO+ET (4 steps)")

    # ── Asset Class: by AssetClassId ──
    existing_ac1 = _existing_deals(temp_cost_pct)
    ac_p1 = _select_cost_pct(
        cps.alias("C")
        .join(
            entity_underlyings.alias("E"),
            F.col("E.AssetClassId") == F.col("C.InvestmentID"),
        )
        .filter(
            (F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")) == ac_ut)
            & (F.col("C.InvestmentID") != -1)
        ),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("E.TrackingKey"), F.lit("")),
        F.col("C.UnderlyingType"),
    )
    ac_p1 = _anti_dedup(ac_p1, existing_ac1)
    temp_cost_pct = temp_cost_pct.unionByName(ac_p1, allowMissingColumns=True)

    # ── Asset Class: InvestmentID = -1 cross join ──
    existing_ac2 = _existing_deals(temp_cost_pct)
    # Phase 2a: include _mode so the cross-join's E side carries the mode tag,
    # which _select_cost_pct then projects into the unioned temp_cost_pct.
    eu_distinct = entity_underlyings.select("UnderlyingEntityId", "_mode").distinct()
    ac_p2 = _select_cost_pct(
        cps.alias("C")
        .crossJoin(eu_distinct.alias("E"))
        .filter(
            (F.col("C.InvestmentID") == -1)
            & (F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")) == entity_ut)
        ),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("C.TrackingKey"), F.lit("")),
    )
    ac_p2 = _anti_dedup(ac_p2, existing_ac2)
    temp_cost_pct = temp_cost_pct.unionByName(ac_p2, allowMissingColumns=True)

    # ── (tcp_post_et checkpoint above already broke the main lineage;
    # the 2 AC steps only add 2 shallow unions — checkpoint removed.) ──

    # ═══════════════════════════════════════════════════════════
    # Build #TempAllEntities (union of dated + non-dated entities)
    # SQL lines: 6630-6660
    # ═══════════════════════════════════════════════════════════
    # Phase 2a: include _mode so the unioned all_entities preserves the per-mode
    # tag through downstream parent-hierarchy and tag matching.
    dated_subset = dated.select("UnderlyingEntityID", "TypeID", "TrackingKey", "Tag", "_mode").distinct()
    nondated_subset = non_dated.select("UnderlyingEntityID", "TypeID", "TrackingKey", "Tag", "_mode").distinct()

    all_entities = (
        dated_subset
        .unionByName(nondated_subset)
        # Also include CostAllocationTypeID versions where TypeID differs
        .unionByName(
            dated_subset
            .filter(F.col("TypeID") != cost_alloc_type_id)
            .withColumn("TypeID", F.lit(cost_alloc_type_id).cast("int"))
        )
        .unionByName(
            nondated_subset
            .filter(F.col("TypeID") != cost_alloc_type_id)
            .withColumn("TypeID", F.lit(cost_alloc_type_id).cast("int"))
        )
        .distinct()
    )

    # Delete entities already matched in cost_pct
    # Phase 2a: add _mode equality so a mode-1 entity isn't suppressed by a
    # mode-2 cost_pct row (only meaningful in Phase 2b's fused execution).
    all_entities = all_entities.join(
        temp_cost_pct,
        (all_entities["UnderlyingEntityID"] == temp_cost_pct["DealId"])
        & (all_entities["TypeID"] == temp_cost_pct["TypeId"])
        & (all_entities["TrackingKey"] == temp_cost_pct["TrackingKey"])
        & (all_entities["Tag"] == temp_cost_pct["Tag"])
        & (all_entities["_mode"] == temp_cost_pct["_mode"]),
        "left_anti",
    )

    # Checkpoint all_entities — it's a 4-union + distinct + anti-join that is
    # otherwise recomputed inside parent_ranked (expensive window join) and
    # again for tag/nothing matching. Materializing here prevents the
    # DENSE_RANK window from triggering a full recomputation of the entity set.
    if checkpoint_fn is not None:
        mode = cfg.get("_current_mode", 1)
        all_entities = checkpoint_fn(spark, all_entities, f"all_ent_m{mode}", cfg)
        logger.info("[CHECKPOINT] all_entities (initial anti-join)")

    # ═══════════════════════════════════════════════════════════
    # Parent hierarchy matching (DENSE_RANK)
    # SQL lines: 6700-6850
    # ═══════════════════════════════════════════════════════════
    # Phase 2a: add _mode equality to the join AND _mode to the Window
    # partitionBy so each (UnderlyingEntityID, TypeID, TrackingKey, Tag, _mode)
    # tuple gets its own DENSE_RANK ordering. Without _mode in the partition,
    # a fused-mode plan would mix entities across modes when computing rank.
    parent_ranked = (
        all_entities.alias("E")
        .join(
            all_underlyings.alias("U"),
            (F.col("E.UnderlyingEntityID") == F.col("U.UnderlyingEntityId"))
            & (F.col("E.TrackingKey") == F.col("U.TrackingKey"))
            & (F.col("E.TypeID") == F.col("U.AllocationTypeId"))
            & (F.col("E._mode") == F.col("U._mode")),
            "left",
        )
        .select(
            F.col("E.UnderlyingEntityID").alias("UnderlyingEntityId"),
            F.col("E.TypeID"),
            F.col("E.TrackingKey"),
            F.col("E.Tag"),
            F.col("U.EntityId"),
            F.col("U.Underlyingtype"),
            F.col("E._mode"),
            F.dense_rank().over(
                Window.partitionBy(
                    F.col("E.UnderlyingEntityID"), F.col("E.TypeID"),
                    F.col("E.TrackingKey"), F.col("E.Tag"), F.col("E._mode"),
                ).orderBy(F.col("U.Underlyingtype"))
            ).alias("RuleRank"),
        )
    )

    parent_ordered = parent_ranked.filter(F.col("RuleRank") == F.lit(1)).select(
        "UnderlyingEntityId", "TypeID", "TrackingKey", "Tag", "EntityId", "Underlyingtype", "_mode",
    ).distinct()

    # Checkpoint parent_ordered — it's used 4 times (TK/no-TK × temp_cost_pct/transfers_adj).
    # Without this, the expensive DENSE_RANK window over all_entities × all_underlyings
    # is recomputed for each use. For mode 2, all_entities includes footnote entities
    # making this O(N²) recomputation the primary bottleneck.
    if checkpoint_fn is not None:
        mode = cfg.get("_current_mode", 1)
        parent_ordered = checkpoint_fn(spark, parent_ordered, f"parent_ord_m{mode}", cfg)
        logger.info("[CHECKPOINT] parent_ordered (hierarchy rank=1)")

    # ── Match through parent WITH TrackingKeyMatch ──
    # Phase 2a: both C (temp_cost_pct) and E (parent_ordered) carry _mode.
    # Add _mode equality to keep mode-1 cost rows from matching mode-2
    # parents (only matters in Phase 2b's fused plan).
    parent_match_tk = (
        temp_cost_pct.alias("C")
        .join(
            parent_ordered.alias("E"),
            (
                F.when(
                    (F.coalesce(F.col("E.EntityId"), F.col("C.DealId")) == F.col("C.DealId"))
                    & (F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")) == et_ut),
                    F.col("E.UnderlyingEntityId"),
                ).otherwise(F.col("C.DealId"))
                == F.col("E.UnderlyingEntityId")
            )
            & (F.col("C.TypeId") == F.col("E.TypeID"))
            & (F.col("C.Tag") == F.col("E.Tag"))
            & (F.col("C._mode") == F.col("E._mode"))
            & (
                F.regexp_replace(F.col("C.TrackingKeyMatch").cast("string"), "~", "")
                == F.col("E.EntityId").cast("string")
            )
            & (F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int"))
               == F.coalesce(F.col("E.Underlyingtype"), F.lit(entity_ut).cast("int")))
            & (F.col("C.TrackingKey") == "")
            & (F.concat(F.lit("~"), F.col("E.TrackingKey"), F.lit("~"))
               .like(F.concat(F.lit("%"), F.col("C.TrackingKeyMatch"), F.lit("%")))),
        )
        .filter(F.coalesce(F.col("C.TrackingKeyMatch"), F.lit("")) != "")
        .select(
            F.col("E.UnderlyingEntityId").alias("DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.col("C.CommitmentPercent"),
            F.col("C.TypeId"),
            F.col("E.TrackingKey"),
            F.col("C.Tag"),
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("C._mode"),
        )
        .distinct()
    )
    temp_cost_pct = temp_cost_pct.unionByName(parent_match_tk, allowMissingColumns=True)

    # ── Intermediate checkpoint: break DAG after parent TK matching ──
    # REMOVED — temp_cost_pct is consumed sequentially (noTK match next).
    # parent_ordered checkpoint (#3) breaks the expensive DENSE_RANK lineage.
    # Removing saves one localCheckpoint job (~1.5s overhead).
    logger.info("[SKIP-CHECKPOINT] tcp_post_ptk (single consumer)")

    # Similarly for transfers_adj with TrackingKeyMatch — same pattern
    # Phase 2a: both C (transfers_adj) and E (parent_ordered) have _mode;
    # add _mode equality + project _mode through.
    if transfers_adj is not None:
        parent_ordered_adj = parent_ordered  # same hierarchy
        adj_match_tk = (
            transfers_adj.alias("C")
            .join(
                parent_ordered_adj.alias("E"),
                (
                    F.when(
                        (F.coalesce(F.col("E.EntityId"), F.col("C.InvestmentID")) == F.col("C.InvestmentID"))
                        & (F.coalesce(F.col("C.Underlyingtype"), F.lit(entity_ut).cast("int")) == et_ut),
                        F.col("E.UnderlyingEntityId"),
                    ).otherwise(F.col("C.InvestmentID"))
                    == F.col("E.UnderlyingEntityId")
                )
                & (F.col("C.TypeID") == F.col("E.TypeID"))
                & (F.col("C.Tag") == F.col("E.Tag"))
                & (F.col("C._mode") == F.col("E._mode"))
                & (F.coalesce(F.col("C.Underlyingtype"), F.lit(entity_ut).cast("int"))
                   == F.coalesce(F.col("E.Underlyingtype"), F.lit(entity_ut).cast("int")))
                & (F.col("C.TrackingKey") == "")
                & (F.concat(F.lit("~"), F.col("E.TrackingKey"), F.lit("~"))
                   .like(F.concat(F.lit("%"), F.col("C.TrackingKeyMatch"), F.lit("%")))),
            )
            .filter(F.coalesce(F.col("C.TrackingKeyMatch"), F.lit("")) != "")
            .select(
                F.col("E.UnderlyingEntityId").alias("InvestmentID"),
                F.col("C.TransferPartnerNumber"),
                F.col("C.TransferDate"),
                F.col("C.EndingCostPercent"),
                F.col("C.PartnerNumber"),
                F.col("C.EffectivePercent"),
                F.col("C.TypeID"),
                F.col("E.TrackingKey"),
                F.col("C.Tag"),
                F.col("C._mode"),
            )
            .distinct()
        )
        transfers_adj = transfers_adj.unionByName(adj_match_tk, allowMissingColumns=True)

    # (transfers_adj checkpoint skipped — linear usage, shallow lineage
    # from checkpointed parent_ordered. Saves ~1.5s Delta I/O.)

    # Delete matched from parent_ordered
    # Phase 2a: include _mode in the anti-join key.
    parent_ordered = parent_ordered.join(
        temp_cost_pct,
        (parent_ordered["UnderlyingEntityId"] == temp_cost_pct["DealId"])
        & (parent_ordered["TypeID"] == temp_cost_pct["TypeId"])
        & (parent_ordered["TrackingKey"] == temp_cost_pct["TrackingKey"])
        & (parent_ordered["Tag"] == temp_cost_pct["Tag"])
        & (parent_ordered["_mode"] == temp_cost_pct["_mode"]),
        "left_anti",
    )

    # ── Match through parent WITHOUT TrackingKeyMatch ──
    # Phase 2a: same _mode treatment as parent_match_tk above.
    parent_match_notk = (
        temp_cost_pct.alias("C")
        .join(
            parent_ordered.alias("E"),
            (
                F.when(
                    (F.coalesce(F.col("E.EntityId"), F.col("C.DealId")) == F.col("C.DealId"))
                    & (F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")) == et_ut),
                    F.col("E.UnderlyingEntityId"),
                ).otherwise(F.col("C.DealId"))
                == F.col("E.UnderlyingEntityId")
            )
            & (F.col("C.TypeId") == F.col("E.TypeID"))
            & (F.col("C.Tag") == F.col("E.Tag"))
            & (F.col("C._mode") == F.col("E._mode"))
            & (F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int"))
               == F.coalesce(F.col("E.Underlyingtype"), F.lit(entity_ut).cast("int")))
            & (F.col("C.TrackingKey") == ""),
        )
        .filter(F.coalesce(F.col("C.TrackingKeyMatch"), F.lit("")) == "")
        .select(
            F.col("E.UnderlyingEntityId").alias("DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.col("C.CommitmentPercent"),
            F.col("C.TypeId"),
            F.col("E.TrackingKey"),
            F.col("C.Tag"),
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("C._mode"),
        )
        .distinct()
    )
    temp_cost_pct = temp_cost_pct.unionByName(parent_match_notk, allowMissingColumns=True)

    if transfers_adj is not None:
        adj_match_notk = (
            transfers_adj.alias("C")
            .join(
                parent_ordered.alias("E"),
                (
                    F.when(
                        (F.coalesce(F.col("E.EntityId"), F.col("C.InvestmentID")) == F.col("C.InvestmentID"))
                        & (F.coalesce(F.col("C.Underlyingtype"), F.lit(entity_ut).cast("int")) == et_ut),
                        F.col("E.UnderlyingEntityId"),
                    ).otherwise(F.col("C.InvestmentID"))
                    == F.col("E.UnderlyingEntityId")
                )
                & (F.col("C.TypeID") == F.col("E.TypeID"))
                & (F.col("C.Tag") == F.col("E.Tag"))
                & (F.col("C._mode") == F.col("E._mode"))
                & (F.coalesce(F.col("C.Underlyingtype"), F.lit(entity_ut).cast("int"))
                   == F.coalesce(F.col("E.Underlyingtype"), F.lit(entity_ut).cast("int")))
                & (F.col("C.TrackingKey") == ""),
            )
            .filter(F.coalesce(F.col("C.TrackingKeyMatch"), F.lit("")) == "")
            .select(
                F.col("E.UnderlyingEntityId").alias("InvestmentID"),
                F.col("C.TransferPartnerNumber"),
                F.col("C.TransferDate"),
                F.col("C.EndingCostPercent"),
                F.col("C.PartnerNumber"),
                F.col("C.EffectivePercent"),
                F.col("C.TypeID"),
                F.col("E.TrackingKey"),
                F.col("C.Tag"),
                F.col("C._mode"),
            )
            .distinct()
        )
        transfers_adj = transfers_adj.unionByName(adj_match_notk, allowMissingColumns=True)

    # ── Intermediate checkpoint: break DAG lineage after parent matching ──
    # REMOVED — temp_cost_pct flows sequentially into TrackingKeyMatch cleanup
    # (filter) then pre-tag anti-join. No multi-consumer. Removing saves one
    # localCheckpoint job (~1.5s overhead).
    # transfers_adj: skip checkpoint — linear usage, shallow lineage
    # from checkpointed parent_ordered.
    logger.info("[SKIP-CHECKPOINT] tcp_post_parent (single consumer)")

    # ── Cleanup: Delete rows with empty TrackingKey but non-empty TrackingKeyMatch ──
    if "TrackingKeyMatch" in temp_cost_pct.columns:
        temp_cost_pct = temp_cost_pct.filter(
            ~(
                (F.coalesce(F.col("TrackingKey"), F.lit("")) == "")
                & (F.coalesce(F.col("TrackingKeyMatch"), F.lit("")) != "")
            )
        )

    if transfers_adj is not None and "TrackingKeyMatch" in transfers_adj.columns:
        transfers_adj = transfers_adj.filter(
            ~(
                (F.coalesce(F.col("TrackingKey"), F.lit("")) == "")
                & (F.coalesce(F.col("TrackingKeyMatch"), F.lit("")) != "")
            )
        )

    # Recompute remaining all_entities
    # Phase 2a: include _mode in the anti-join key.
    all_entities = all_entities.join(
        temp_cost_pct,
        (all_entities["UnderlyingEntityID"] == temp_cost_pct["DealId"])
        & (all_entities["TypeID"] == temp_cost_pct["TypeId"])
        & (all_entities["TrackingKey"] == temp_cost_pct["TrackingKey"])
        & (all_entities["Tag"] == temp_cost_pct["Tag"])
        & (all_entities["_mode"] == temp_cost_pct["_mode"]),
        "left_anti",
    )

    # Checkpoint all_entities before tag matching — both sides of the anti-join
    # are checkpointed, but the result is used in tag_match AND nothing_match
    # joins. Without this checkpoint, the anti-join plan is re-evaluated twice.
    if checkpoint_fn is not None:
        mode = cfg.get("_current_mode", 1)
        all_entities = checkpoint_fn(spark, all_entities, f"all_ent_pre_tag_m{mode}", cfg)
        logger.info("[CHECKPOINT] all_entities (pre-tag matching)")

    # ── Tag matching: cost % TrackingKey matches, Tag = '' → input Tag ──
    # Phase 2a: both C and E carry _mode; add _mode equality + project _mode.
    tag_match = (
        temp_cost_pct.alias("C")
        .join(
            all_entities.alias("E"),
            (F.col("C.DealId") == F.col("E.UnderlyingEntityID"))
            & (F.col("C.TypeId") == F.col("E.TypeID"))
            & (F.col("C.TrackingKey") == F.col("E.TrackingKey"))
            & (F.col("C._mode") == F.col("E._mode"))
            & (F.col("C.Tag") == ""),
        )
        .select(
            F.col("C.DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.col("C.CommitmentPercent"),
            F.col("C.TypeId"),
            F.col("C.TrackingKey"),
            F.col("E.Tag"),
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("C._mode"),
        )
        .distinct()
    )
    temp_cost_pct = temp_cost_pct.unionByName(tag_match, allowMissingColumns=True)

    if transfers_adj is not None:
        all_entities_adj = all_entities  # same remaining set
        adj_tag_match = (
            transfers_adj.alias("C")
            .join(
                all_entities_adj.alias("E"),
                (F.col("C.InvestmentID") == F.col("E.UnderlyingEntityID"))
                & (F.col("C.TypeID") == F.col("E.TypeID"))
                & (F.col("C.TrackingKey") == F.col("E.TrackingKey"))
                & (F.col("C._mode") == F.col("E._mode"))
                & (F.col("C.Tag") == ""),
            )
            .select(
                F.col("C.InvestmentID"),
                F.col("C.TransferPartnerNumber"),
                F.col("C.TransferDate"),
                F.col("C.EndingCostPercent"),
                F.col("C.PartnerNumber"),
                F.col("C.EffectivePercent"),
                F.col("C.TypeID"),
                F.col("C.TrackingKey"),
                F.col("E.Tag"),
                F.col("C._mode"),
            )
            .distinct()
        )
        transfers_adj = transfers_adj.unionByName(adj_tag_match, allowMissingColumns=True)

    # ── Intermediate checkpoint: break DAG after tag matching ──
    if checkpoint_fn is not None:
        mode = cfg.get("_current_mode", 1)
        temp_cost_pct = checkpoint_fn(spark, temp_cost_pct, f"tcp_post_tag_m{mode}", cfg)
        logger.info("[CHECKPOINT] temp_cost_pct after tag matching")
        # transfers_adj: skip checkpoint — caller checkpoints the return
        # value. Saves ~1.5s Delta I/O.

    # Recompute remaining all_entities again
    # Phase 2a: include _mode in the anti-join key.
    all_entities = all_entities.join(
        temp_cost_pct,
        (all_entities["UnderlyingEntityID"] == temp_cost_pct["DealId"])
        & (all_entities["TypeID"] == temp_cost_pct["TypeId"])
        & (all_entities["TrackingKey"] == temp_cost_pct["TrackingKey"])
        & (all_entities["Tag"] == temp_cost_pct["Tag"])
        & (all_entities["_mode"] == temp_cost_pct["_mode"]),
        "left_anti",
    )

    # ── Nothing matching: cost % TrackingKey = '' AND Tag = '' → input TrackingKey + Tag ──
    # Phase 2a: both C and E carry _mode; add _mode equality + project _mode.
    nothing_match = (
        temp_cost_pct.alias("C")
        .join(
            all_entities.alias("E"),
            (F.col("C.DealId") == F.col("E.UnderlyingEntityID"))
            & (F.col("C.TypeId") == F.col("E.TypeID"))
            & (F.col("C._mode") == F.col("E._mode"))
            & (F.col("C.TrackingKey") == "")
            & (F.col("C.Tag") == ""),
        )
        .select(
            F.col("C.DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.col("C.CommitmentPercent"),
            F.col("C.TypeId"),
            F.col("E.TrackingKey"),
            F.col("E.Tag"),
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("C._mode"),
        )
        .distinct()
    )
    temp_cost_pct = temp_cost_pct.unionByName(nothing_match, allowMissingColumns=True)

    if transfers_adj is not None:
        adj_nothing_match = (
            transfers_adj.alias("C")
            .join(
                all_entities.alias("E"),
                (F.col("C.InvestmentID") == F.col("E.UnderlyingEntityID"))
                & (F.col("C.TypeID") == F.col("E.TypeID"))
                & (F.col("C._mode") == F.col("E._mode"))
                & (F.col("C.TrackingKey") == "")
                & (F.col("C.Tag") == ""),
            )
            .select(
                F.col("C.InvestmentID"),
                F.col("C.TransferPartnerNumber"),
                F.col("C.TransferDate"),
                F.col("C.EndingCostPercent"),
                F.col("C.PartnerNumber"),
                F.col("C.EffectivePercent"),
                F.col("C.TypeID"),
                F.col("E.TrackingKey"),
                F.col("E.Tag"),
                F.col("C._mode"),
            )
            .distinct()
        )
        transfers_adj = transfers_adj.unionByName(adj_nothing_match, allowMissingColumns=True)

    _log_timing("build_cost_percentage_by_type", t0)
    return temp_cost_pct, transfers_adj


# ---------------------------------------------------------------------------
# compute_missing_entities
# SQL lines: 7150-7195
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def compute_missing_entities(
    cfg: dict,
    non_dated: DataFrame,
    dated: DataFrame,
    cost_pct: DataFrame,
) -> tuple:
    """For entities with non-cost TypeID that have no matching cost %,
    fall back to CostAllocationTypeID.

    Returns: (updated_non_dated, updated_dated)
    """
    t0 = time.time()
    logger.info("[SECTION] compute_missing_entities")

    cost_alloc_type_id = cfg["cost_allocation_type_id"]

    # Non-dated: find entities with TypeID != CostAlloc that have no cost %
    # NOTE: SQL bug uses D.UnderlyingEntityID IS NULL (left table, always non-NULL)
    # instead of C.DealId IS NULL. This makes nd_missing always empty. We replicate for parity.
    # Phase 3a-1: add _mode equality to the join + propagate _mode through select.
    nd_missing = (
        non_dated.alias("D")
        .join(
            cost_pct.alias("C"),
            (F.col("D.UnderlyingEntityID") == F.col("C.DealId"))
            & (F.col("D.TypeID") == F.col("C.TypeId"))
            & (F.col("D.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("D.Tag") == F.col("C.Tag"))
            & (F.col("D._mode") == F.col("C._mode")),
            "left",
        )
        .filter(
            (F.col("D.TypeID") != cost_alloc_type_id)
            & (F.col("D.UnderlyingEntityID").isNull())
        )
        .select(
            F.col("D.UnderlyingEntityID"),
            F.coalesce(F.col("D.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("D.TypeID"),
            F.col("D.TrackingKey"),
            F.col("D.Tag"),
            F.col("D.IsExcludefromTransfer"),
            F.col("D._mode"),
        )
    )

    # Dated: same SQL bug — D.UnderlyingEntityID IS NULL (always false)
    # Phase 3a-1: add _mode equality + propagate _mode through select.
    dt_missing = (
        dated.alias("D")
        .join(
            cost_pct.alias("C"),
            (F.col("D.UnderlyingEntityID") == F.col("C.DealId"))
            & (F.col("D.TypeID") == F.col("C.TypeId"))
            & (F.col("D.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("D.Tag") == F.col("C.Tag"))
            & (F.col("D._mode") == F.col("C._mode"))
            & (F.col("C.Quarter") <= F.col("D.Quarter")),
            "left",
        )
        .filter(
            (F.col("D.TypeID") != cost_alloc_type_id)
            & (F.col("D.UnderlyingEntityID").isNull())
        )
        .select(
            F.col("D.Quarter"),
            F.col("D.UnderlyingEntityID"),
            F.coalesce(F.col("D.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("D.TypeID"),
            F.col("D.TrackingKey"),
            F.col("D.Tag"),
            F.col("D.IsExcludefromTransfer"),
            F.col("D._mode"),
        )
    )

    # Update TypeID to CostAllocationTypeID for missing entities
    # Dated update
    updated_dated = (
        dated.alias("D")
        .join(
            dt_missing.alias("C"),
            (F.col("D.UnderlyingEntityID") == F.col("C.UnderlyingEntityID"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.col("C.LineTypeID"))
            & (F.col("D.Quarter") == F.col("C.Quarter"))
            & (F.col("D.TypeID") == F.col("C.TypeID"))
            & (F.col("D.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("D.Tag") == F.col("C.Tag"))
            & (F.col("D.IsExcludefromTransfer") == F.col("C.IsExcludefromTransfer")),
            "left",
        )
        .select(
            F.col("D.*"),
        )
        .withColumn(
            "TypeID",
            F.when(F.col("C.UnderlyingEntityID").isNotNull(),
                   F.lit(cost_alloc_type_id).cast("int"))
            .otherwise(F.col("D.TypeID")),
        )
        .drop("C.*")
    )

    # For the update, use a simpler approach: replace matching rows
    # Phase 3a-1: add _mode equality to all 4 semi/anti joins.
    # Broadcast dt_missing / nd_missing: they are always empty due to the
    # replicated SQL bug (filter on D.UnderlyingEntityID IS NULL, which is
    # always false since D is the left table). Broadcasting avoids shuffling
    # the large dated/non_dated DFs for these no-op joins.
    dated_matched = (
        dated.alias("D")
        .join(
            F.broadcast(dt_missing).alias("C"),
            (F.col("D.UnderlyingEntityID") == F.col("C.UnderlyingEntityID"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.col("C.LineTypeID"))
            & (F.col("D.Quarter") == F.col("C.Quarter"))
            & (F.col("D.TypeID") == F.col("C.TypeID"))
            & (F.col("D.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("D.Tag") == F.col("C.Tag"))
            & (F.col("D._mode") == F.col("C._mode"))
            & (F.col("D.IsExcludefromTransfer") == F.col("C.IsExcludefromTransfer")),
            "left_semi",
        )
    )
    dated_unmatched = (
        dated.alias("D")
        .join(
            F.broadcast(dt_missing).alias("C"),
            (F.col("D.UnderlyingEntityID") == F.col("C.UnderlyingEntityID"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.col("C.LineTypeID"))
            & (F.col("D.Quarter") == F.col("C.Quarter"))
            & (F.col("D.TypeID") == F.col("C.TypeID"))
            & (F.col("D.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("D.Tag") == F.col("C.Tag"))
            & (F.col("D._mode") == F.col("C._mode"))
            & (F.col("D.IsExcludefromTransfer") == F.col("C.IsExcludefromTransfer")),
            "left_anti",
        )
    )
    dated_matched_updated = dated_matched.withColumn(
        "TypeID", F.lit(cost_alloc_type_id).cast("int"),
    )
    updated_dated = dated_unmatched.unionByName(dated_matched_updated, allowMissingColumns=True)

    # Non-dated update
    nd_matched = (
        non_dated.alias("D")
        .join(
            F.broadcast(nd_missing).alias("C"),
            (F.col("D.UnderlyingEntityID") == F.col("C.UnderlyingEntityID"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.col("C.LineTypeID"))
            & (F.col("D.TypeID") == F.col("C.TypeID"))
            & (F.col("D.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("D.Tag") == F.col("C.Tag"))
            & (F.col("D._mode") == F.col("C._mode"))
            & (F.col("D.IsExcludefromTransfer") == F.col("C.IsExcludefromTransfer")),
            "left_semi",
        )
    )
    nd_unmatched = (
        non_dated.alias("D")
        .join(
            F.broadcast(nd_missing).alias("C"),
            (F.col("D.UnderlyingEntityID") == F.col("C.UnderlyingEntityID"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.col("C.LineTypeID"))
            & (F.col("D.TypeID") == F.col("C.TypeID"))
            & (F.col("D.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("D.Tag") == F.col("C.Tag"))
            & (F.col("D._mode") == F.col("C._mode"))
            & (F.col("D.IsExcludefromTransfer") == F.col("C.IsExcludefromTransfer")),
            "left_anti",
        )
    )
    nd_matched_updated = nd_matched.withColumn(
        "TypeID", F.lit(cost_alloc_type_id).cast("int"),
    )
    updated_non_dated = nd_unmatched.unionByName(nd_matched_updated, allowMissingColumns=True)

    # Store missing entity data in cfg for apply_type_id_update (SQL #TempNonDatedEntitiesCost / #TempDatedEntitiesCost)
    # NOTE: nd_missing and dt_missing are ALWAYS empty due to the replicated
    # SQL bug (D.UnderlyingEntityID IS NULL is always false for a left-table
    # column). Setting None avoids an expensive isEmpty() action in
    # apply_type_id_update that would otherwise evaluate the lazy join plan.
    cfg["_non_dated_entities_cost"] = None
    cfg["_dated_entities_cost"] = None

    _log_timing("compute_missing_entities", t0)
    return updated_non_dated, updated_dated


# ---------------------------------------------------------------------------
# build_final_cost_percentage
# SQL lines: 7200-7215
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_final_cost_percentage(
    cost_pct: DataFrame,
    entity_partners: DataFrame,
) -> DataFrame:
    """Build #FinalCostPercentage: cross join of cost % deals × partners.

    Ensures every partner has a row (0 if no cost %).
    """
    t0 = time.time()
    logger.info("[SECTION] build_final_cost_percentage")

    # Phase 3a-1: project _mode through deals → cross-join → left-join.
    # The cross join (deals × entity_partners) inherits _mode from D.
    # The subsequent left-join needs _mode equality so a mode-1 deal
    # doesn't get matched against a mode-2 cost row (only meaningful when
    # build_final_cost_percentage is called on fused data in Phase 3b).
    deals = cost_pct.select(
        "DealId", "Quarter", "TypeId", "TrackingKey", "Tag", "_mode",
    ).distinct()

    result = (
        deals.alias("D")
        .crossJoin(entity_partners.alias("P"))
        .join(
            cost_pct.alias("C"),
            (F.col("C.PartnerNumber") == F.col("P.PartnerNumber"))
            & (F.col("C.DealId") == F.col("D.DealId"))
            & (F.col("D.Quarter") == F.col("C.Quarter"))
            & (F.col("D.TypeId") == F.col("C.TypeId"))
            & (F.col("D.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("D.Tag") == F.col("C.Tag"))
            & (F.col("D._mode") == F.col("C._mode")),
            "left",
        )
        .select(
            F.col("D.DealId"),
            F.col("P.PartnerNumber"),
            F.col("D.Quarter"),
            F.coalesce(F.col("C.CommitmentPercent"), F.lit(0)).alias("CommitmentPercent"),
            F.col("D.TypeId"),
            F.col("D.TrackingKey"),
            F.col("D.Tag"),
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("D._mode"),
        )
    )

    _log_timing("build_final_cost_percentage", t0)
    return result


# ---------------------------------------------------------------------------
# validate_cost_percentage_sum
# SQL lines: 7240-7270
# ---------------------------------------------------------------------------
def validate_cost_percentage_sum(
    spark: SparkSession, cfg: dict,
    final_cost_pct: DataFrame,
    dar_setup: DataFrame,
) -> bool:
    """For mode 1: validate cost % sums to 100%. Write error if not.

    Returns True if valid, False if error written.
    """
    mode = cfg.get("mode")
    if mode != 1:
        return True

    from Common_V2.core.helpers import sql_round as _sql_round

    error_deals = (
        final_cost_pct.alias("F")
        .join(
            dar_setup.alias("E"),
            F.col("F.TypeId") == F.col("E.RuleID"),
            "left",
        )
        .filter(F.col("E.RuleID").isNull())
        .groupBy("DealId", "Quarter", "TypeId", "TrackingKey", "Tag")
        .agg(F.sum("CommitmentPercent").alias("TotalPct"))
        .filter(_sql_round(F.col("TotalPct"), 8).cast("decimal(24,8)") != F.lit(1.0).cast("decimal(24,8)"))
        .select("DealId")
        .distinct()
    )

    if error_deals.isEmpty():
        return True

    # Write error to AllocationRunErrors
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    log_id = cfg.get("log_id")
    entity = F.broadcast(_tbl(spark, "Entity", cfg))

    names = (
        error_deals.alias("U")
        .join(entity.alias("E"), F.col("U.DealId") == F.col("E.EntityID"))
        .select("E.DisplayName")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    name_str = ", ".join(names) if names else ""

    error_msg = f"The sum of Cost Percentage does not sum to 100% for following deals -{name_str}"
    logger.error(f"[VALIDATION] {error_msg}")

    # Write to AllocationRunErrors
    error_df = spark.createDataFrame(
        [(run_id, entity_id, error_msg, log_id, "Error")],
        ["RunID", "EntityID", "ErrorMessage", "LogID", "ErrororWarning"],
    )
    error_df.write.format("delta").mode("append").saveAsTable(
        f"{cfg['catalog']}.{cfg['schema']}.AllocationRunErrors"
    )

    # Update AllocationRun to FAIL
    spark.sql(f"""
        UPDATE {cfg['catalog']}.{cfg['schema']}.AllocationRun
        SET RunStatus = 'FAIL', RunEndDate = current_timestamp()
        WHERE RunID = {run_id}
    """)

    return False


# ---------------------------------------------------------------------------
# compute_minimum_quarter
# SQL lines: 7270-7440
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def compute_minimum_quarter(
    spark: SparkSession, cfg: dict,
    final_cost_pct: DataFrame,
    dated_entities: DataFrame,
) -> tuple:
    """Compute minimum quarter for each deal + quarter type dedup.

    PE Book: QuarterDates Preference-based min.
    Standard: ENU_DF_DataList DisplayOrder-based min + Q/M dedup.

    Returns: (min_quarter_df, cost_pct_min_quarter_df, updated_dated_entities)
    """
    t0 = time.time()
    logger.info("[SECTION] compute_minimum_quarter")

    alloc_type_name = cfg.get("allocation_type_name", "")
    is_dated_transfers = cfg.get("is_dated_transfers_configured", "")
    is_pe_book_dated = (alloc_type_name == "PE Book Allocation" and is_dated_transfers == "C")

    # Phase 3a-1: include _mode in every groupBy key, anti-join key, and select
    # so per-mode min-quarter calculations don't bleed across modes.
    # quarter_dates / quarters_ref tables don't have _mode (shared dim), so the
    # T × D joins inherit _mode from T (final_cost_pct).
    if is_pe_book_dated:
        quarter_dates = _tbl(spark, "QuarterDates", cfg)

        min_quarter = (
            final_cost_pct.alias("T")
            .join(quarter_dates.alias("D"), F.col("T.Quarter") == F.col("D.Quarter"))
            .groupBy("T.DealId", "T.TypeId", "T.TrackingKey", "T.Tag", "T._mode")
            .agg(F.min("D.Preference").alias("MinQuarter"))
            .select(
                F.col("DealId").alias("DealID"),
                F.col("TypeId").alias("TypeID"),
                F.col("TrackingKey"),
                F.col("Tag"),
                F.col("MinQuarter"),
                F.col("_mode"),
            )
        )

        cost_pct_min_quarter = (
            min_quarter.alias("T")
            .join(
                quarter_dates.alias("D"),
                F.col("T.MinQuarter") == F.col("D.Preference"),
            )
            .select(
                F.col("T.DealID"),
                F.col("T.TypeID"),
                F.col("T.TrackingKey"),
                F.col("T.Tag"),
                F.col("D.Quarter"),
                F.col("D.Preference"),
                F.col("T._mode"),
            )
        )
    else:
        enu_df = F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg))
        quarters_ref = enu_df.filter(F.col("Category") == "Quarters")

        min_quarter = (
            final_cost_pct.alias("T")
            .join(
                quarters_ref.alias("D"),
                (F.col("T.Quarter") == F.col("D.LookUpData"))
                & (F.col("T.Quarter").like(
                    F.concat(F.coalesce(F.col("D.Comments"), F.lit("")), F.lit("%"))
                )),
            )
            .groupBy("T.DealId", "T.TypeId", "T.TrackingKey", "T.Tag", "D.Comments", "T._mode")
            .agg(F.min("D.DisplayOrder").alias("MinQuarter"))
            .select(
                F.col("DealId").alias("DealID"),
                F.col("TypeId").alias("TypeID"),
                F.col("TrackingKey"),
                F.col("Tag"),
                F.col("MinQuarter"),
                F.col("Comments").alias("QuarterType"),
                F.col("_mode"),
            )
        )

        cost_pct_min_quarter = (
            min_quarter.alias("T")
            .join(
                quarters_ref.alias("D"),
                (F.col("D.Category") == "Quarters")
                & (F.col("T.MinQuarter") == F.col("D.DisplayOrder"))
                & (F.coalesce(F.col("D.Comments"), F.lit(""))
                   == F.coalesce(F.col("T.QuarterType"), F.lit(""))),
            )
            .select(
                F.col("T.DealID"),
                F.col("T.TypeID"),
                F.col("T.TrackingKey"),
                F.col("T.Tag"),
                F.col("D.LookUpData").alias("Quarter"),
                F.lit(None).cast("int").alias("Preference"),
                F.col("T._mode"),
            )
        )

        # If a deal has Q0, retain Q0 and delete non-Q0
        # Phase 3a-1: scope the Q0-retention check by _mode too.
        has_q0 = cost_pct_min_quarter.filter(F.col("Quarter") == "Q0")
        cost_pct_min_quarter = cost_pct_min_quarter.join(
            has_q0.select("DealID", "TypeID", "TrackingKey", "Tag", "_mode").distinct().alias("Q"),
            (cost_pct_min_quarter["DealID"] == F.col("Q.DealID"))
            & (cost_pct_min_quarter["TypeID"] == F.col("Q.TypeID"))
            & (cost_pct_min_quarter["TrackingKey"] == F.col("Q.TrackingKey"))
            & (cost_pct_min_quarter["Tag"] == F.col("Q.Tag"))
            & (cost_pct_min_quarter["_mode"] == F.col("Q._mode"))
            & (cost_pct_min_quarter["Quarter"] != "Q0"),
            "left_anti",
        )

        # Quarter type dedup: pick Q vs M preference
        # Phase 3a-1: include _mode in the dedup key to avoid mixing
        # mode-1 quarter types with mode-2 in the multi_qt detection.
        quarter_type_by_deal = (
            final_cost_pct.alias("T")
            .join(
                quarters_ref.alias("D"),
                (F.col("T.Quarter") == F.col("D.LookUpData")),
            )
            .select(
                F.col("T.DealId"),
                F.col("T.TypeId"),
                F.col("T.TrackingKey"),
                F.col("T.Tag"),
                F.col("D.Comments").alias("QuarterType"),
                F.col("T._mode"),
            )
            .distinct()
        )

        multi_qt = (
            quarter_type_by_deal
            .groupBy("DealId", "TypeId", "TrackingKey", "Tag", "_mode")
            .agg(F.countDistinct("QuarterType").alias("cnt"))
            .filter(F.col("cnt") > 1)
            .select("DealId", "TypeId", "TrackingKey", "Tag", "_mode")
        )

        # Delete Q type if both Q and M exist
        # Phase 3a-1: include _mode in both anti-join keys.
        quarter_type_by_deal = quarter_type_by_deal.join(
            multi_qt.alias("M"),
            (quarter_type_by_deal["DealId"] == F.col("M.DealId"))
            & (quarter_type_by_deal["TypeId"] == F.col("M.TypeId"))
            & (quarter_type_by_deal["TrackingKey"] == F.col("M.TrackingKey"))
            & (quarter_type_by_deal["Tag"] == F.col("M.Tag"))
            & (quarter_type_by_deal["_mode"] == F.col("M._mode"))
            & (quarter_type_by_deal["QuarterType"] == "Q"),
            "left_anti",
        ).unionByName(
            quarter_type_by_deal.join(
                multi_qt,
                ["DealId", "TypeId", "TrackingKey", "Tag", "_mode"],
                "left_anti",
            ),
            allowMissingColumns=True,
        ).distinct()

        # Delete dated entities not matching retained quarter types
        # Phase 3a-1: add _mode equality so a mode-1 dated row only stays
        # in if mode-1's quarter_type_by_deal contains a matching entry.
        updated_dated = (
            dated_entities.alias("D")
            .join(
                quarters_ref.alias("DQ"),
                (F.col("D.Quarter") == F.col("DQ.LookUpData")),
            )
            .join(
                quarter_type_by_deal.alias("F"),
                (F.col("D.UnderlyingEntityID") == F.col("F.DealId"))
                & (F.col("DQ.Comments") == F.col("F.QuarterType"))
                & (F.col("D.TypeID") == F.col("F.TypeId"))
                & (F.col("D.TrackingKey") == F.col("F.TrackingKey"))
                & (F.col("D.Tag") == F.col("F.Tag"))
                & (F.col("D._mode") == F.col("F._mode")),
                "left_semi",
            )
        )
        dated_entities = updated_dated

    _log_timing("compute_minimum_quarter", t0)
    return min_quarter, cost_pct_min_quarter, dated_entities
