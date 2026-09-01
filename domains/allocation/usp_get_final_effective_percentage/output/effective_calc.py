"""
effective_calc.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Core effective percentage calculation (dated + non-dated), transfer-adjusted
cost percentage, plugging logic, type ID update, and final output assembly.
Conversion date: 2026-05-04

SQL lines: 7443-8850
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


def _sql_round(col, scale=0):
    """SQL Server ROUND() — half-away-from-zero."""
    from pyspark.sql.functions import abs as _abs, signum, floor
    factor = F.lit(10 ** scale)
    return signum(col) * floor(_abs(col) * factor + F.lit(0.5)) / factor


def _iif_inv(inv_col, ent_col):
    """IIF(InvestmentID = -1, EntityID, InvestmentID)."""
    return F.when(inv_col == -1, ent_col).otherwise(inv_col)


def _transfer_join_cond(t_prefix, l_prefix):
    """Build the CASE WHEN T.InvestmentID=-1 THEN 1 ELSE ... END matching pattern.

    Phase 3a-2: include unconditional ``_mode`` equality. Default rows
    (T.InvestmentID = -1) are replicated per-mode by the caller before this
    join fires, so each default row already carries a specific _mode that
    must match L._mode like any other row.
    """
    return (
        (
            F.when(F.col(f"{t_prefix}.InvestmentID") == -1, F.lit(1))
            .otherwise(F.col(f"{t_prefix}.InvestmentID"))
            ==
            F.when(F.col(f"{t_prefix}.InvestmentID") == -1, F.lit(1))
            .otherwise(F.col(f"{l_prefix}.UnderlyingEntityID"))
        )
        & (
            F.when(F.col(f"{t_prefix}.InvestmentID") == -1, F.lit(1))
            .otherwise(F.col(f"{t_prefix}.TypeID"))
            ==
            F.when(F.col(f"{t_prefix}.InvestmentID") == -1, F.lit(1))
            .otherwise(F.col(f"{l_prefix}.TypeID"))
        )
        & (
            F.when(F.col(f"{t_prefix}.InvestmentID") == -1, F.lit("1"))
            .otherwise(F.col(f"{t_prefix}.TrackingKey"))
            ==
            F.when(F.col(f"{t_prefix}.InvestmentID") == -1, F.lit("1"))
            .otherwise(F.col(f"{l_prefix}.TrackingKey"))
        )
        & (
            F.when(F.col(f"{t_prefix}.InvestmentID") == -1, F.lit("1"))
            .otherwise(F.col(f"{t_prefix}.Tag"))
            ==
            F.when(F.col(f"{t_prefix}.InvestmentID") == -1, F.lit("1"))
            .otherwise(F.col(f"{l_prefix}.Tag"))
        )
        & (F.col(f"{t_prefix}._mode") == F.col(f"{l_prefix}._mode"))
    )


# ---------------------------------------------------------------------------
# compute_effective_percentage_dated
# SQL lines: 7443-7960
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def compute_effective_percentage_dated(
    spark: SparkSession, cfg: dict,
    dated_entities: DataFrame,
    final_cost_pct: DataFrame,
    cost_pct_min_quarter: DataFrame,
    transfers_adj: DataFrame,
    entity_partners: DataFrame,
    line_items: DataFrame,
    checkpoint_fn=None,
) -> tuple:
    """Compute dated effective percentages:
    1. Cost % for dated entities (PickUpOrder=1)
    2. Transfer-affected dated cost % (PickUpOrder=2)
    3. Exclude-from-transfer cost % (PickUpOrder=2)
    4. Pickup order selection
    5. No-transfer entities → cost-adjusted or ProRata
    6. Yearly ProRata fallback (PickUpOrder=3)
    7. Missing partners insertion

    Returns: (eff_pct_dated, pickup_order_dated, remaining_dated_entities)
    """
    t0 = time.time()
    logger.info("[SECTION] compute_effective_percentage_dated")

    alloc_type_name = cfg.get("allocation_type_name", "")
    is_dated_transfers = cfg.get("is_dated_transfers_configured", "")
    is_pe_book_dated = (alloc_type_name == "PE Book Allocation" and is_dated_transfers == "C")
    k1_lt_id = cfg["k1_line_type_id"]
    pfic_fn_lt_id = cfg.get("pfic_footnote_line_type_id")
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    part_v_allocated = cfg.get("part_v_allocated", 0)

    # Broadcast cost_pct_min_quarter — small per-(entity, quarter) lookup
    # joined 4+ times against dated_entities. Broadcasting eliminates shuffles.
    if cost_pct_min_quarter is not None:
        cost_pct_min_quarter = F.broadcast(cost_pct_min_quarter)

    # ── Step 1: Cost percentage for dated entities ──
    # SQL: INSERT INTO #TempFinalEffectivePercentageDated ... FROM #TempDatedEntities JOIN #FinalCostPercentage
    # Phase 3a-2: add _mode equality + project _mode through.
    eff_pct_dated = (
        dated_entities.alias("L")
        .join(
            final_cost_pct.alias("C"),
            (F.col("L.UnderlyingEntityID") == F.col("C.DealId"))
            & (F.col("C.Quarter") == F.col("L.Quarter"))
            & (F.col("L.TypeID") == F.col("C.TypeId"))
            & (F.col("L.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("L.Tag") == F.col("C.Tag"))
            & (F.col("L._mode") == F.col("C._mode")),
        )
        .select(
            F.col("L.UnderlyingEntityID").alias("InvestmentID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("C.PartnerNumber"),
            F.coalesce(F.col("C.CommitmentPercent"), F.lit(0)).alias("EffPercentage"),
            F.col("L.Quarter"),
            F.lit("Cost").alias("AllocationType"),
            F.lit(1).alias("PickUpOrder"),
            F.col("C.TypeId"),
            F.col("C.TrackingKey"),
            F.col("C.Tag"),
            F.col("L.IsExcludefromTransfer"),
            F.coalesce(F.col("C.GPPartnerReceivingCarry"), F.lit(False)).alias("GPPartnerReceivingCarry"),
            F.lit(None).cast("int").alias("LineID"),
            F.lit(None).cast("int").alias("704cAllocationTypeId"),
            F.lit(None).cast("string").alias("704cPercentageType"),
            F.lit(None).cast("int").alias("StateID"),
            F.col("L._mode"),
        )
        .distinct()
    )

    # Delete matched dated entities
    # Phase 3a-2: add _mode equality.
    dated_entities = dated_entities.join(
        eff_pct_dated.select("InvestmentID", "Quarter", "TypeId", "TrackingKey", "Tag", "_mode").distinct().alias("F"),
        (dated_entities["UnderlyingEntityID"] == F.col("F.InvestmentID"))
        & (dated_entities["Quarter"] == F.col("F.Quarter"))
        & (dated_entities["TypeID"] == F.col("F.TypeId"))
        & (dated_entities["TrackingKey"] == F.col("F.TrackingKey"))
        & (dated_entities["Tag"] == F.col("F.Tag"))
        & (dated_entities["_mode"] == F.col("F._mode")),
        "left_anti",
    )

    # ── Step 2: Transfer-affected dated cost % ──
    # Build #TempTransferAdjDatedPercentages
    # Phase 3a-2: project _mode through. Default rows (InvestmentID=-1) apply
    # to all modes — replicate per-mode by cross-joining with distinct modes
    # so the unioned frame is _mode-tagged consistently.
    if transfers_adj is not None and not transfers_adj.isEmpty():
        transfer_adj_dated = transfers_adj.filter(F.col("TransferDate").isNotNull()).select(
            "InvestmentID", "TransferPartnerNumber", "TransferDate",
            "EndingCostPercent", "PartnerNumber", "TypeID", "TrackingKey", "Tag",
            "_mode",
        ).distinct()

        _distinct_modes_for_defaults = transfers_adj.select("_mode").distinct()

        trans_adj_default = (
            F.broadcast(_tbl(spark, "TransfersAdjDefaultPercentage", cfg)).alias("T")
            .filter(
                (F.col("T.RunID") == run_id)
                & (F.col("T.ClientID") == client_id)
                & (F.col("T.TransferDate").isNotNull())
            )
            .select(
                F.lit(-1).alias("InvestmentID"),
                F.col("T.TransferPartnerNumber"),
                F.when(
                    F.coalesce(F.col("T.isEODTransfer").cast("int"), F.lit(0)) == F.lit(1),
                    F.date_add(F.col("TransferDate"), 1),
                ).otherwise(F.col("TransferDate")).alias("TransferDate"),
                F.col("T.EndingCommitmentPercent").alias("EndingCostPercent"),
                F.col("T.PartnerNumber"),
                F.lit(-1).alias("TypeID"),
                F.lit("").alias("TrackingKey"),
                F.lit("").alias("Tag"),
            )
            .crossJoin(F.broadcast(_distinct_modes_for_defaults))
        )
        transfer_adj_dated = transfer_adj_dated.unionByName(trans_adj_default, allowMissingColumns=True)
    else:
        transfer_adj_dated = None

    # ── Step 2b: Build #TempTransferDate (matching dated entities to transfers) ──
    if transfer_adj_dated is not None and not transfer_adj_dated.isEmpty():
        if is_pe_book_dated:
            # PE Book: QuarterDates + K1LineItem TransactionDate check.
            # QuarterDates is a small calendar dim (~4-5 rows/period) — always broadcast.
            # K1LineItem is small per (entity,client,period) — broadcast.
            quarter_dates = F.broadcast(_tbl(spark, "QuarterDates", cfg))
            k1_items = F.broadcast(_tbl(spark, "K1LineItem", cfg))

            transfer_date_k1 = (
                dated_entities.alias("L")
                .join(
                    transfer_adj_dated.alias("T"),
                    _transfer_join_cond("T", "L"),
                )
                .join(
                    quarter_dates.alias("D"),
                    F.coalesce(F.col("T.TransferDate"), F.lit("1900-01-01").cast("date"))
                    .between(F.col("D.StartDate"), F.col("D.EndDate")),
                )
                .join(
                    k1_items.alias("K"),
                    F.col("K.LineID") == F.col("L.LineID"),
                )
                .filter(
                    (F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)) == False)
                    & (F.coalesce(F.col("K.TransactionDate"), F.lit("1900-01-01").cast("date"))
                       >= F.coalesce(F.col("T.TransferDate"), F.lit("1900-01-01").cast("date")))
                    & (F.col("L.LineTypeID") == k1_lt_id)
                )
                .groupBy(
                    F.col("L.UnderlyingEntityID"), F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                    F.col("L.Quarter"), F.col("L.TypeID").alias("UnderlyingTypeID"),
                    F.col("L.TrackingKey").alias("UnderlyingTrackingKey"),
                    F.col("L.Tag").alias("UnderlyingTag"),
                    F.col("T.TypeID"), F.col("T.TrackingKey"), F.col("T.Tag"),
                    F.col("T.InvestmentID"), F.col("T.TransferPartnerNumber"),
                    F.col("L._mode"),
                )
                .agg(F.max("T.TransferDate").alias("TransferDate"))
            )

            # Form926 line type
            form926_lt = (
                _tbl(spark, "ENU_LineType", cfg)
                .filter(F.col("LineType") == "Form926")
                .select("LineTypeID")
                .first()
            )
            form926_lt_id = form926_lt["LineTypeID"] if form926_lt else None

            if form926_lt_id is not None:
                transfer_date_926 = (
                    dated_entities.alias("L")
                    .join(
                        transfer_adj_dated.alias("T"),
                        _transfer_join_cond("T", "L"),
                    )
                    .join(
                        quarter_dates.alias("D"),
                        F.coalesce(F.col("T.TransferDate"), F.lit("1900-01-01").cast("date"))
                        .between(F.col("D.StartDate"), F.col("D.EndDate")),
                    )
                    .filter(
                        (F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)) == False)
                        & (F.col("L.LineTypeID") == form926_lt_id)
                        & (F.coalesce(F.col("L.transferdate"), F.lit("1900-01-01").cast("date"))
                           >= F.coalesce(F.col("T.TransferDate"), F.lit("1900-01-01").cast("date")))
                    )
                    .groupBy(
                        F.col("L.UnderlyingEntityID"), F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                        F.col("L.Quarter"), F.col("L.TypeID").alias("UnderlyingTypeID"),
                        F.col("L.TrackingKey").alias("UnderlyingTrackingKey"),
                        F.col("L.Tag").alias("UnderlyingTag"),
                        F.col("T.TypeID"), F.col("T.TrackingKey"), F.col("T.Tag"),
                        F.col("T.InvestmentID"), F.col("T.TransferPartnerNumber"),
                        F.col("L._mode"),
                    )
                    .agg(F.max("T.TransferDate").alias("TransferDate"))
                )
                transfer_dates = transfer_date_k1.unionByName(transfer_date_926, allowMissingColumns=True)
            else:
                transfer_dates = transfer_date_k1

        else:
            # Standard: ENU_DF_DataList QuarterMonth matching
            enu_df = F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg))
            qm_ref = enu_df.filter(F.col("Category") == "QuarterMonth")

            transfer_dates = (
                dated_entities.alias("L")
                .join(
                    transfer_adj_dated.alias("T"),
                    _transfer_join_cond("T", "L"),
                )
                .join(
                    qm_ref.alias("D"),
                    (F.col("D.LookUpValue") == F.coalesce(F.month(F.col("T.TransferDate")), F.lit(0)).cast("string"))
                    & (F.col("L.Quarter").like(F.concat(F.coalesce(F.col("D.Comments"), F.lit("")), F.lit("%")))),
                )
                .join(
                    qm_ref.alias("DF"),
                    (F.col("L.Quarter") == F.col("DF.LookUpData"))
                    & (F.col("D.LookUpValue").cast("int") <= F.col("DF.LookUpValue").cast("int")),
                )
                .filter(F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)) == False)
                .groupBy(
                    F.col("L.UnderlyingEntityID"), F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                    F.col("L.Quarter"), F.col("L.TypeID").alias("UnderlyingTypeID"),
                    F.col("L.TrackingKey").alias("UnderlyingTrackingKey"),
                    F.col("L.Tag").alias("UnderlyingTag"),
                    F.col("T.TypeID"), F.col("T.TrackingKey"), F.col("T.Tag"),
                    F.col("T.InvestmentID"), F.col("T.TransferPartnerNumber"),
                    F.col("L._mode"),
                )
                .agg(F.max("T.TransferDate").alias("TransferDate"))
                .distinct()
            )

        # PartV allocated: PFIC footnote lines
        if part_v_allocated == 1 and pfic_fn_lt_id is not None:
            pfic_transfer_dates = (
                dated_entities.alias("L")
                .join(
                    transfer_adj_dated.alias("T"),
                    _transfer_join_cond("T", "L"),
                )
                .join(
                    F.broadcast(_tbl(spark, "QuarterDates", cfg)).alias("D") if is_pe_book_dated else qm_ref.alias("D"),
                    F.coalesce(F.col("T.TransferDate"), F.lit("1900-01-01").cast("date"))
                    .between(F.col("D.StartDate"), F.col("D.EndDate")) if is_pe_book_dated
                    else (F.col("D.LookUpValue") == F.coalesce(F.month(F.col("T.TransferDate")), F.lit(0)).cast("string")),
                )
                .join(
                    transfer_dates.alias("TD"),
                    (F.col("TD.UnderlyingEntityID") == F.col("L.UnderlyingEntityID"))
                    & (F.col("TD.LineTypeID") == F.coalesce(F.col("L.LineTypeID"), F.lit(-1)))
                    & (F.col("TD.Quarter") == F.col("L.Quarter"))
                    & (F.col("L.TypeID") == F.col("TD.UnderlyingTypeID"))
                    & (F.col("L.TrackingKey") == F.col("TD.UnderlyingTrackingKey"))
                    & (F.coalesce(F.col("L.Tag"), F.lit("")) == F.coalesce(F.col("TD.UnderlyingTag"), F.lit("")))
                    & (F.col("TD.TypeID") == F.col("T.TypeID"))
                    & (F.col("T.TrackingKey") == F.col("TD.TrackingKey"))
                    & (F.col("T.Tag") == F.col("TD.Tag"))
                    & (F.col("T.InvestmentID") == F.col("TD.InvestmentID"))
                    & (F.col("T.TransferPartnerNumber") == F.col("TD.TransferPartnerNumber"))
                    & (F.col("L._mode") == F.col("TD._mode")),
                    "left",
                )
                .filter(
                    (F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)) == False)
                    & (F.col("L.LineTypeID") == pfic_fn_lt_id)
                    & (F.coalesce(F.col("L.transferdate"), F.lit("1900-01-01").cast("date"))
                       >= F.coalesce(F.col("T.TransferDate"), F.lit("1900-01-01").cast("date")))
                    & (F.col("TD.InvestmentID").isNull())
                )
                .groupBy(
                    F.col("L.UnderlyingEntityID"), F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                    F.col("L.Quarter"), F.col("L.TypeID").alias("UnderlyingTypeID"),
                    F.col("L.TrackingKey").alias("UnderlyingTrackingKey"),
                    F.col("L.Tag").alias("UnderlyingTag"),
                    F.col("T.TypeID"), F.col("T.TrackingKey"), F.col("T.Tag"),
                    F.col("T.InvestmentID"), F.col("T.TransferPartnerNumber"),
                    F.col("L._mode"),
                )
                .agg(F.max("T.TransferDate").alias("TransferDate"))
            )
            transfer_dates = transfer_dates.unionByName(pfic_transfer_dates, allowMissingColumns=True)

            # Delete quarters where transfer date was not applied for PFIC footnote
            # Phase 3a-2: scope per-mode so a mode-1 PFIC entity isn't deleted because
            # mode-2 had a different applied_quarters set.
            applied_quarters = transfer_dates.select("Quarter", "_mode").distinct()
            dated_entities = (
                dated_entities.alias("L")
                .join(
                    applied_quarters.alias("T"),
                    (F.col("L.Quarter") == F.col("T.Quarter"))
                    & (F.col("L._mode") == F.col("T._mode")),
                    "left",
                )
                .filter(
                    (F.col("T.Quarter").isNotNull())
                    | (F.coalesce(F.col("L.LineTypeID"), F.lit(-1)) != pfic_fn_lt_id)
                )
                .select("L.*")
            )

        # ── Insert transfer-affected percentages into eff_pct_dated ──
        # Phase 3a-2: add _mode equality + project _mode through groupBy/select.
        transfer_eff = (
            transfer_adj_dated.alias("T")
            .join(
                transfer_dates.alias("P"),
                (F.col("T.InvestmentID") == F.col("P.InvestmentID"))
                & (F.col("T.TransferPartnerNumber") == F.col("P.TransferPartnerNumber"))
                & (F.coalesce(F.col("T.TransferDate"), F.lit("9999-01-01").cast("date"))
                   == F.coalesce(F.col("P.TransferDate"), F.lit("9999-01-01").cast("date")))
                & (F.col("T.TypeID") == F.col("P.TypeID"))
                & (F.col("T.TrackingKey") == F.col("P.TrackingKey"))
                & (F.col("T.Tag") == F.col("P.Tag"))
                & (F.col("T._mode") == F.col("P._mode")),
            )
            .groupBy(
                F.col("P.UnderlyingEntityID"), F.coalesce(F.col("P.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.col("T.PartnerNumber"), F.col("P.Quarter"),
                F.col("T.InvestmentID"), F.col("T.TypeID"), F.col("T.TrackingKey"), F.col("T.Tag"),
                F.col("P.UnderlyingTypeID"), F.col("P.UnderlyingTrackingKey"), F.col("P.UnderlyingTag"),
                F.col("P._mode"),
            )
            .agg(F.sum(F.coalesce(F.col("T.EndingCostPercent"), F.lit(0))).alias("EffPercentage"))
            .select(
                F.col("UnderlyingEntityID").alias("InvestmentID"),
                F.col("LineTypeID"),
                F.col("PartnerNumber"),
                F.col("EffPercentage"),
                F.col("Quarter"),
                F.when(F.col("T.InvestmentID") == -1, F.lit("ProRata"))
                .otherwise(F.lit("CostAdjustedDatedTransfer")).alias("AllocationType"),
                F.when(F.col("T.InvestmentID") == -1, F.lit(3)).otherwise(F.lit(2)).alias("PickUpOrder"),
                F.col("UnderlyingTypeID").alias("TypeId"),
                F.col("UnderlyingTrackingKey").alias("TrackingKey"),
                F.col("UnderlyingTag").alias("Tag"),
                F.lit(False).alias("IsExcludefromTransfer"),
                F.lit(None).cast("boolean").alias("GPPartnerReceivingCarry"),
                F.lit(None).cast("int").alias("LineID"),
                F.lit(None).cast("int").alias("704cAllocationTypeId"),
                F.lit(None).cast("string").alias("704cPercentageType"),
                F.lit(None).cast("int").alias("StateID"),
                F.col("_mode"),
            )
        )
        eff_pct_dated = eff_pct_dated.unionByName(transfer_eff, allowMissingColumns=True)

        # ── Exclude-from-transfer entities: cost % without transfer adjustment ──
        # Phase 3a-2: add _mode equality to all 3 joins + project _mode through select.
        if is_pe_book_dated:
            exclude_transfer = (
                final_cost_pct.alias("Y")
                .join(
                    cost_pct_min_quarter.alias("M"),
                    (F.col("Y.DealId") == F.col("M.DealID"))
                    & (F.col("Y.Quarter") == F.col("M.Quarter"))
                    & (F.col("Y.TypeId") == F.col("M.TypeID"))
                    & (F.col("Y.TrackingKey") == F.col("M.TrackingKey"))
                    & (F.col("Y.Tag") == F.col("M.Tag"))
                    & (F.col("Y._mode") == F.col("M._mode")),
                )
                .join(
                    dated_entities.alias("D"),
                    (F.col("D.UnderlyingEntityID") == F.col("M.DealID"))
                    & (F.col("D.TypeID") == F.col("M.TypeID"))
                    & (F.col("D.Tag") == F.col("M.Tag"))
                    & (F.col("D.TrackingKey") == F.col("M.TrackingKey"))
                    & (F.col("D._mode") == F.col("M._mode"))
                    & (F.col("M.Preference") < F.col("D.Preference")),
                )
                .filter(F.col("D.IsExcludefromTransfer") == True)
                .select(
                    F.col("D.UnderlyingEntityID").alias("InvestmentID"),
                    F.coalesce(F.col("D.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                    F.col("Y.PartnerNumber"),
                    F.col("Y.CommitmentPercent").alias("EffPercentage"),
                    F.col("D.Quarter"),
                    F.lit("Cost without Transfer Adj %").alias("AllocationType"),
                    F.lit(2).alias("PickUpOrder"),
                    F.col("D.TypeID").alias("TypeId"),
                    F.col("D.TrackingKey"),
                    F.col("D.Tag"),
                    F.col("D.IsExcludefromTransfer"),
                    F.col("Y.GPPartnerReceivingCarry"),
                    F.lit(None).cast("int").alias("LineID"),
                    F.lit(None).cast("int").alias("704cAllocationTypeId"),
                    F.lit(None).cast("string").alias("704cPercentageType"),
                    F.lit(None).cast("int").alias("StateID"),
                    F.col("D._mode"),
                )
                .distinct()
            )
        else:
            exclude_transfer = (
                final_cost_pct.alias("Y")
                .join(
                    cost_pct_min_quarter.alias("M"),
                    (F.col("Y.DealId") == F.col("M.DealID"))
                    & (F.col("Y.Quarter") == F.col("M.Quarter"))
                    & (F.col("Y.TypeId") == F.col("M.TypeID"))
                    & (F.col("Y.TrackingKey") == F.col("M.TrackingKey"))
                    & (F.col("Y.Tag") == F.col("M.Tag"))
                    & (F.col("Y._mode") == F.col("M._mode")),
                )
                .join(
                    dated_entities.alias("D"),
                    (F.col("D.UnderlyingEntityID") == F.col("M.DealID"))
                    & (F.col("D.TypeID") == F.col("M.TypeID"))
                    & (F.col("D.Tag") == F.col("M.Tag"))
                    & (F.col("D.TrackingKey") == F.col("M.TrackingKey"))
                    & (F.col("D._mode") == F.col("M._mode"))
                    & (F.col("M.Quarter") < F.col("D.Quarter")),
                )
                .filter(F.col("D.IsExcludefromTransfer") == True)
                .select(
                    F.col("D.UnderlyingEntityID").alias("InvestmentID"),
                    F.coalesce(F.col("D.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                    F.col("Y.PartnerNumber"),
                    F.col("Y.CommitmentPercent").alias("EffPercentage"),
                    F.col("D.Quarter"),
                    F.lit("Cost without Transfer Adj %").alias("AllocationType"),
                    F.lit(2).alias("PickUpOrder"),
                    F.col("D.TypeID").alias("TypeId"),
                    F.col("D.TrackingKey"),
                    F.col("D.Tag"),
                    F.col("D.IsExcludefromTransfer"),
                    F.col("Y.GPPartnerReceivingCarry"),
                    F.lit(None).cast("int").alias("LineID"),
                    F.lit(None).cast("int").alias("704cAllocationTypeId"),
                    F.lit(None).cast("string").alias("704cPercentageType"),
                    F.lit(None).cast("int").alias("StateID"),
                    F.col("D._mode"),
                )
                .distinct()
            )
        eff_pct_dated = eff_pct_dated.unionByName(exclude_transfer, allowMissingColumns=True)

    else:
        transfer_dates = None

    # Checkpoint eff_pct_dated after transfer steps — Steps 3-7 reference
    # eff_pct_dated repeatedly (groupBy, anti-joins, unions). Without this,
    # each downstream use re-evaluates the full Steps 1-2 transfer lineage.
    # Critical for entities with substantial transfer data (e.g. 5940).
    if checkpoint_fn is not None:
        mode_val = cfg.get("_current_mode", cfg.get("mode", 0))
        eff_pct_dated = checkpoint_fn(spark, eff_pct_dated, f"eff_pct_dated_post_transfer", cfg)

    # ── Step 3: Pickup order selection ──
    # Phase 3a-2: include _mode in groupBy.
    pickup_order_dated = (
        eff_pct_dated
        .groupBy(
            "InvestmentID", F.coalesce(F.col("LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            "Quarter", "TypeId", "TrackingKey", "Tag", "IsExcludefromTransfer", "_mode",
        )
        .agg(F.min("PickUpOrder").alias("PickUpOrder"))
    )

    # ── Step 4: No-transfer entities ──
    # Entities with cost % at a lesser quarter but no partners in transfer
    # Phase 3a-2: include _mode in pickup_non3 distinct.
    part_v_quarters = cfg.get("_part_v_quarters_df")  # set by pfic_footnotes if applicable
    pickup_non3 = pickup_order_dated.filter(F.col("PickUpOrder") != 3).select(
        "InvestmentID", F.col("LineTypeID"), "Quarter", "TypeId".upper() if False else "TypeId",
        "TrackingKey", "Tag", "IsExcludefromTransfer", "_mode",
    ).distinct()

    # Comparison depends on PE Book vs Standard
    if is_pe_book_dated:
        compare_expr = F.col("M.Preference") < F.col("D.Preference")
    else:
        compare_expr = F.col("M.Quarter") < F.col("D.Quarter")

    # Phase 3a-2: add _mode equality to D × M and D × U joins, project _mode.
    no_transfer_base = (
        dated_entities.alias("D")
        .join(
            cost_pct_min_quarter.alias("M"),
            (F.col("D.UnderlyingEntityID") == F.col("M.DealID"))
            & (F.col("D.TypeID") == F.col("M.TypeID"))
            & (F.col("D.TrackingKey") == F.col("M.TrackingKey"))
            & (F.col("D.Tag") == F.col("M.Tag"))
            & (F.col("D._mode") == F.col("M._mode"))
            & compare_expr,
        )
        .join(
            pickup_non3.alias("U"),
            (F.col("D.UnderlyingEntityID") == F.col("U.InvestmentID"))
            & (F.col("D.Quarter") == F.col("U.Quarter"))
            & (F.col("D.TypeID") == F.col("U.TypeId"))
            & (F.col("D.TrackingKey") == F.col("U.TrackingKey"))
            & (F.col("D.Tag") == F.col("U.Tag"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.col("U.LineTypeID"))
            & (F.col("D.IsExcludefromTransfer") == F.col("U.IsExcludefromTransfer"))
            & (F.col("D._mode") == F.col("U._mode")),
            "left",
        )
    )

    # Also exclude PartV quarters if applicable
    if part_v_quarters is not None:
        no_transfer_base = (
            no_transfer_base
            .join(
                part_v_quarters.alias("QD"),
                F.col("QD.Quarter") == F.col("D.Quarter"),
                "left",
            )
            .filter(
                (F.col("D.IsExcludefromTransfer") == False)
                & (F.col("QD.Quarter").isNull())
                & (F.col("U.InvestmentID").isNull())
            )
            .select(
                F.col("D.UnderlyingEntityID").alias("InvestmentID"),
                F.coalesce(F.col("D.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.col("D.Quarter"),
                F.col("D.TypeID").alias("TypeId"),
                F.col("D.TrackingKey"),
                F.col("D.Tag"),
                F.col("D.IsExcludefromTransfer"),
                F.col("D._mode"),
            )
            .distinct()
        )
    else:
        no_transfer_base = (
            no_transfer_base
            .filter(
                (F.col("D.IsExcludefromTransfer") == False)
                & (F.col("U.InvestmentID").isNull())
            )
            .select(
                F.col("D.UnderlyingEntityID").alias("InvestmentID"),
                F.coalesce(F.col("D.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.col("D.Quarter"),
                F.col("D.TypeID").alias("TypeId"),
                F.col("D.TrackingKey"),
                F.col("D.Tag"),
                F.col("D.IsExcludefromTransfer"),
                F.col("D._mode"),
            )
            .distinct()
        )

    no_transfer_entities = no_transfer_base

    # Delete ProRata (pickup=3) for these entities from pickup_order
    # Phase 3a-2: add _mode equality to both anti-joins.
    pickup_order_dated = pickup_order_dated.join(
        no_transfer_entities.alias("D"),
        (F.col("D.InvestmentID") == pickup_order_dated["InvestmentID"])
        & (F.col("D.TypeId") == pickup_order_dated["TypeId"])
        & (F.col("D.TrackingKey") == pickup_order_dated["TrackingKey"])
        & (F.col("D.Tag") == pickup_order_dated["Tag"])
        & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.coalesce(pickup_order_dated["LineTypeID"], F.lit(-1)))
        & (F.col("D.IsExcludefromTransfer") == pickup_order_dated["IsExcludefromTransfer"])
        & (F.col("D._mode") == pickup_order_dated["_mode"])
        & (pickup_order_dated["PickUpOrder"] == 3),
        "left_anti",
    ).unionByName(
        pickup_order_dated.join(
            no_transfer_entities.alias("D2"),
            (F.col("D2.InvestmentID") == pickup_order_dated["InvestmentID"])
            & (F.col("D2.TypeId") == pickup_order_dated["TypeId"])
            & (F.col("D2.TrackingKey") == pickup_order_dated["TrackingKey"])
            & (F.col("D2.Tag") == pickup_order_dated["Tag"])
            & (F.coalesce(F.col("D2.LineTypeID"), F.lit(-1)) == F.coalesce(pickup_order_dated["LineTypeID"], F.lit(-1)))
            & (F.col("D2.IsExcludefromTransfer") == pickup_order_dated["IsExcludefromTransfer"])
            & (F.col("D2._mode") == pickup_order_dated["_mode"]),
            "left_anti",
        ).filter(F.col("PickUpOrder") == 3),
        allowMissingColumns=True,
    )

    # Checkpoint pickup_order_dated after Step 4 modifications
    if checkpoint_fn is not None:
        mode_val = cfg.get("_current_mode", cfg.get("mode", 0))
        pickup_order_dated = checkpoint_fn(spark, pickup_order_dated, f"pickup_s4_m{mode_val}", cfg)
        logger.info("[CHECKPOINT] pickup_order_dated after Step 4")

    # ── Step 5: No-transfer pickup quarter cost % ──
    mode = cfg.get("mode")
    if mode != 4 and no_transfer_entities is not None:
        # Phase 3a-2: add _mode equality to Y × P join, include _mode in groupBy.
        if is_pe_book_dated:
            notransfer_pickup = (
                final_cost_pct.alias("Y")
                .join(
                    no_transfer_entities.alias("P"),
                    (F.col("P.InvestmentID") == F.col("Y.DealId"))
                    & (F.col("Y.TypeId") == F.col("P.TypeId"))
                    & (F.col("Y.TrackingKey") == F.col("P.TrackingKey"))
                    & (F.col("Y.Tag") == F.col("P.Tag"))
                    & (F.col("Y._mode") == F.col("P._mode"))
                    & (
                        F.replace(F.replace(F.col("Y.Quarter"), F.lit("M"), F.lit("")), F.lit("Q"), F.lit("")).cast("int")
                        <= F.replace(F.replace(F.col("P.Quarter"), F.lit("M"), F.lit("")), F.lit("Q"), F.lit("")).cast("int")
                    ),
                )
                .groupBy(
                    F.col("P.InvestmentID"), F.coalesce(F.col("P.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                    F.col("P.Quarter"),
                    F.col("Y.TypeId"), F.col("Y.TrackingKey"), F.col("Y.Tag"),
                    F.col("P.IsExcludefromTransfer"),
                    F.col("P._mode"),
                )
                .agg(F.min("Y.Quarter").alias("PickUpQuarter"))
            )
        else:
            notransfer_pickup = (
                final_cost_pct.alias("Y")
                .join(
                    no_transfer_entities.alias("P"),
                    (F.col("P.InvestmentID") == F.col("Y.DealId"))
                    & (F.col("Y.TypeId") == F.col("P.TypeId"))
                    & (F.col("Y.TrackingKey") == F.col("P.TrackingKey"))
                    & (F.col("Y.Tag") == F.col("P.Tag"))
                    & (F.col("Y._mode") == F.col("P._mode"))
                    & (F.col("Y.Quarter") <= F.col("P.Quarter")),
                )
                .groupBy(
                    F.col("P.InvestmentID"), F.coalesce(F.col("P.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                    F.col("P.Quarter"),
                    F.col("Y.TypeId"), F.col("Y.TrackingKey"), F.col("Y.Tag"),
                    F.col("P.IsExcludefromTransfer"),
                    F.col("P._mode"),
                )
                .agg(F.max("Y.Quarter").alias("PickUpQuarter"))
            )

        # Phase 3a-2: add _mode equality + project _mode through select.
        notransfer_eff = (
            final_cost_pct.alias("Y")
            .join(
                notransfer_pickup.alias("M"),
                (F.col("Y.DealId") == F.col("M.InvestmentID"))
                & (F.col("Y.Quarter") == F.col("M.PickUpQuarter"))
                & (F.col("Y.TypeId") == F.col("M.TypeId"))
                & (F.col("Y.TrackingKey") == F.col("M.TrackingKey"))
                & (F.col("Y.Tag") == F.col("M.Tag"))
                & (F.col("Y._mode") == F.col("M._mode")),
            )
            .select(
                F.col("M.InvestmentID"),
                F.coalesce(F.col("M.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.col("Y.PartnerNumber"),
                F.col("Y.CommitmentPercent").alias("EffPercentage"),
                F.col("M.Quarter"),
                F.lit("CostAdjustedDatedTransfer").alias("AllocationType"),
                F.lit(2).alias("PickUpOrder"),
                F.col("M.TypeId"),
                F.col("M.TrackingKey"),
                F.col("M.Tag"),
                F.col("M.IsExcludefromTransfer"),
                F.col("Y.GPPartnerReceivingCarry"),
                F.lit(None).cast("int").alias("LineID"),
                F.lit(None).cast("int").alias("704cAllocationTypeId"),
                F.lit(None).cast("string").alias("704cPercentageType"),
                F.lit(None).cast("int").alias("StateID"),
                F.col("M._mode"),
            )
            .distinct()
        )
        eff_pct_dated = eff_pct_dated.unionByName(notransfer_eff, allowMissingColumns=True)

    # Checkpoint between Step 5 and Step 6 — splits the largest single-step span.
    # By this point eff_pct_dated has accumulated: Step 1 (cost%), Step 2-3 (transfer
    # adjustments), Step 4 (no-transfer entities), Step 5 (notransfer cost). Step 6
    # below adds 3 wide joins (final_cost_pct + cost_pct_min_quarter + pickup_order
    # + dated_entities) and a left_anti against eff_pct_dated itself. Without this
    # checkpoint Step 6 re-evaluates the entire 5-step lineage.
    if checkpoint_fn is not None:
        mode_val = cfg.get("_current_mode", cfg.get("mode", 0))
        eff_pct_dated = checkpoint_fn(spark, eff_pct_dated, f"eff_dated_s5_m{mode_val}", cfg)
        logger.info("[CHECKPOINT] eff_pct_dated after Step 5 (pre-Step-6)")

    # ── Step 6: Missing partners for transfer adjusted cost % ──
    # Phase 3a-2: add _mode equality to all 4 joins + project _mode through.
    if transfer_dates is not None:
        temp_cost = (
            final_cost_pct.alias("Y")
            .join(
                cost_pct_min_quarter.alias("M"),
                (F.col("Y.DealId") == F.col("M.DealID"))
                & (F.col("Y.Quarter") == F.col("M.Quarter"))
                & (F.col("Y.TypeId") == F.col("M.TypeID"))
                & (F.col("Y.TrackingKey") == F.col("M.TrackingKey"))
                & (F.col("Y.Tag") == F.col("M.Tag"))
                & (F.col("Y._mode") == F.col("M._mode")),
            )
            .join(
                pickup_order_dated.alias("P"),
                (F.col("P.InvestmentID") == F.col("Y.DealId"))
                & (F.col("P.PickUpOrder") == 2)
                & (F.col("Y.TypeId") == F.col("P.TypeId"))
                & (F.col("Y.TrackingKey") == F.col("P.TrackingKey"))
                & (F.col("Y.Tag") == F.col("P.Tag"))
                & (F.col("Y._mode") == F.col("P._mode")),
            )
            .join(
                dated_entities.alias("D"),
                (F.col("D.UnderlyingEntityID") == F.col("P.InvestmentID"))
                & (F.col("D.TypeID") == F.col("P.TypeId"))
                & (F.col("D.Quarter") == F.col("P.Quarter"))
                & (F.col("D.Tag") == F.col("P.Tag"))
                & (F.col("D.TrackingKey") == F.col("P.TrackingKey"))
                & (F.col("D._mode") == F.col("P._mode")),
            )
            .select(
                F.col("P.InvestmentID"),
                F.coalesce(F.col("P.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.col("P.Quarter"),
                F.col("Y.PartnerNumber"),
                F.col("Y.CommitmentPercent"),
                F.col("Y.TypeId"),
                F.col("Y.TrackingKey"),
                F.col("Y.Tag"),
                F.col("D.IsExcludefromTransfer"),
                F.col("P._mode"),
            )
            .distinct()
        )

        missing_partners = (
            temp_cost.alias("Y")
            .join(
                eff_pct_dated.alias("F"),
                (F.col("F.PartnerNumber") == F.col("Y.PartnerNumber"))
                & (F.col("F.InvestmentID") == F.col("Y.InvestmentID"))
                & (F.col("F.Quarter") == F.col("Y.Quarter"))
                & (F.col("F.TypeId") == F.col("Y.TypeId"))
                & (F.col("F.TrackingKey") == F.col("Y.TrackingKey"))
                & (F.col("F.Tag") == F.col("Y.Tag"))
                & (F.coalesce(F.col("F.LineTypeID"), F.lit(-1)) == F.coalesce(F.col("Y.LineTypeID"), F.lit(-1)))
                & (F.col("F.IsExcludefromTransfer") == F.col("Y.IsExcludefromTransfer"))
                & (F.col("F._mode") == F.col("Y._mode")),
                "left_anti",
            )
            .select(
                F.col("Y.InvestmentID"),
                F.coalesce(F.col("Y.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.col("Y.PartnerNumber"),
                F.coalesce(F.col("Y.CommitmentPercent"), F.lit(0)).alias("EffPercentage"),
                F.col("Y.Quarter"),
                F.when(F.col("Y.IsExcludefromTransfer") == True, F.lit("Cost without Transfer Adj %"))
                .otherwise(F.lit("CostAdjustedDatedTransfer")).alias("AllocationType"),
                F.lit(2).alias("PickUpOrder"),
                F.col("Y.TypeId"),
                F.col("Y.TrackingKey"),
                F.col("Y.Tag"),
                F.when(F.col("Y.IsExcludefromTransfer") == True, F.lit(True)).otherwise(F.lit(False)).alias("IsExcludefromTransfer"),
                F.lit(None).cast("boolean").alias("GPPartnerReceivingCarry"),
                F.lit(None).cast("int").alias("LineID"),
                F.lit(None).cast("int").alias("704cAllocationTypeId"),
                F.lit(None).cast("string").alias("704cPercentageType"),
                F.lit(None).cast("int").alias("StateID"),
                F.col("Y._mode"),
            )
        )
        eff_pct_dated = eff_pct_dated.unionByName(missing_partners, allowMissingColumns=True)

    # Add no-transfer entities to pickup order
    # Phase 3a-2: include _mode in projection.
    notransfer_pickup_order = no_transfer_entities.select(
        "InvestmentID", "LineTypeID", "Quarter", "TypeId", "TrackingKey", "Tag",
        F.lit(2).alias("PickUpOrder"), "IsExcludefromTransfer", "_mode",
    )
    pickup_order_dated = pickup_order_dated.unionByName(notransfer_pickup_order, allowMissingColumns=True)

    # ── Step 7: Remaining dated entities (PickUpOrder=3 → Yearly ProRata) ──
    # Phase 3a-2: include _mode in remaining_pickup distinct + anti-join + select.
    remaining_pickup = pickup_order_dated.select(
        "InvestmentID", F.coalesce(F.col("LineTypeID"), F.lit(-1)).alias("LineTypeID"),
        "Quarter", "TypeId", "TrackingKey", "Tag", "IsExcludefromTransfer", "_mode",
    ).distinct()

    remaining_dated = (
        dated_entities.alias("D")
        .join(
            remaining_pickup.alias("U"),
            (F.col("D.UnderlyingEntityID") == F.col("U.InvestmentID"))
            & (F.col("D.Quarter") == F.col("U.Quarter"))
            & (F.col("D.TypeID") == F.col("U.TypeId"))
            & (F.col("D.TrackingKey") == F.col("U.TrackingKey"))
            & (F.col("D.Tag") == F.col("U.Tag"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.col("U.LineTypeID"))
            & (F.col("D.IsExcludefromTransfer") == F.col("U.IsExcludefromTransfer"))
            & (F.col("D._mode") == F.col("U._mode")),
            "left_anti",
        )
        .select(
            F.col("D.UnderlyingEntityID").alias("InvestmentID"),
            F.coalesce(F.col("D.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("D.Quarter"),
            F.col("D.TypeID").alias("TypeId"),
            F.col("D.TrackingKey"),
            F.col("D.Tag"),
            F.lit(3).alias("PickUpOrder"),
            F.col("D.IsExcludefromTransfer"),
            F.col("D._mode"),
        )
        .distinct()
    )
    pickup_order_dated = pickup_order_dated.unionByName(remaining_dated, allowMissingColumns=True)

    pickup_order_dated = checkpoint_fn(spark, pickup_order_dated, "pickup_order_dated_pre_yearly", cfg)

    # Yearly percentage (ProRata fallback)
    yearly_workflow_id = cfg.get("yearly_workflow_id")
    yearly_snapshot = F.broadcast(
        _tbl(spark, "Yearly_Snapshot", cfg).filter(F.col("WorkflowID") == yearly_workflow_id)
    )

    # Phase 3a-2: project _mode through (yearly_snapshot has no _mode; rides
    # from P side via the cross-join).
    yearly_dated = (
        yearly_snapshot.alias("Y")
        .crossJoin(
            pickup_order_dated.filter(F.col("PickUpOrder") == 3).alias("P"),
        )
        .select(
            F.col("P.InvestmentID"),
            F.coalesce(F.col("P.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("P.Quarter"),
            F.col("Y.PartnerNumber"),
            F.coalesce(F.col("Y.ProRataEffOwnPercent"), F.lit(0)).alias("EffPercentage"),
            F.col("P.TypeId"),
            F.col("P.TrackingKey"),
            F.col("P.Tag"),
            F.col("P.IsExcludefromTransfer"),
            F.col("P._mode"),
        )
    )

    # Checkpoint eff_pct_dated to break DAG lineage before yearly check
    if checkpoint_fn is not None:
        mode_val = cfg.get("_current_mode", cfg.get("mode", 0))
        eff_pct_dated = checkpoint_fn(spark, eff_pct_dated, f"eff_dated_s6_m{mode_val}", cfg)
        logger.info("[CHECKPOINT] eff_pct_dated after missing partners")

    # Check if yearly data exists — if not, validate error
    # Phase 3a-2: add _mode equality to anti-join. The cross-join inherits
    # _mode from P (entity_partners has no _mode — shared dim).
    if yearly_dated.isEmpty():
        partner_check = (
            entity_partners.alias("Y")
            .crossJoin(
                pickup_order_dated.filter(F.col("PickUpOrder") == 3).alias("P"),
            )
        )
        missing_yearly = (
            partner_check.alias("Y")
            .join(
                eff_pct_dated.alias("F"),
                (F.col("F.PartnerNumber") == F.col("Y.PartnerNumber"))
                & (F.col("F.InvestmentID") == F.col("Y.InvestmentID"))
                & (F.col("F.Quarter") == F.col("Y.Quarter"))
                & (F.col("F.TypeId") == F.col("Y.TypeId"))
                & (F.col("F.TrackingKey") == F.col("Y.TrackingKey"))
                & (F.col("F.Tag") == F.col("Y.Tag"))
                & (F.coalesce(F.col("F.LineTypeID"), F.lit(-1)) == F.coalesce(F.col("Y.LineTypeID"), F.lit(-1)))
                & (F.col("F.IsExcludefromTransfer") == F.col("Y.IsExcludefromTransfer"))
                & (F.col("F._mode") == F.col("Y._mode")),
                "left_anti",
            )
        )
        if not missing_yearly.isEmpty():
            # Write error
            error_df = spark.createDataFrame(
                [(run_id, cfg["entity_id"], "Please update yearly prorata percentages", cfg.get("log_id"), "Error")],
                ["RunID", "EntityID", "ErrorMessage", "LogID", "ErrororWarning"],
            )
            error_df.write.format("delta").mode("append").saveAsTable(
                f"{cfg['catalog']}.{cfg['schema']}.AllocationRunErrors"
            )
            spark.sql(f"""
                UPDATE {cfg['catalog']}.{cfg['schema']}.AllocationRun
                SET RunStatus = 'FAIL', RunEndDate = current_timestamp()
                WHERE RunID = {run_id}
            """)
            _log_timing("compute_effective_percentage_dated", t0)
            return None, None, None  # signal FAIL

    # Insert yearly ProRata for missing partners
    # Phase 3a-2: add _mode equality + project _mode through.
    yearly_missing = (
        yearly_dated.alias("Y")
        .join(
            eff_pct_dated.alias("F"),
            (F.col("F.PartnerNumber") == F.col("Y.PartnerNumber"))
            & (F.col("F.InvestmentID") == F.col("Y.InvestmentID"))
            & (F.col("F.Quarter") == F.col("Y.Quarter"))
            & (F.col("F.TypeId") == F.col("Y.TypeId"))
            & (F.col("F.TrackingKey") == F.col("Y.TrackingKey"))
            & (F.col("F.Tag") == F.col("Y.Tag"))
            & (F.coalesce(F.col("F.LineTypeID"), F.lit(-1)) == F.coalesce(F.col("Y.LineTypeID"), F.lit(-1)))
            & (F.col("F.IsExcludefromTransfer") == F.col("Y.IsExcludefromTransfer"))
            & (F.col("F._mode") == F.col("Y._mode")),
            "left_anti",
        )
        .select(
            F.col("Y.InvestmentID"),
            F.coalesce(F.col("Y.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("Y.PartnerNumber"),
            F.coalesce(F.col("Y.EffPercentage"), F.lit(0)).alias("EffPercentage"),
            F.col("Y.Quarter"),
            F.when(F.col("Y.IsExcludefromTransfer") == True, F.lit("Cost without Transfer Adj %"))
            .otherwise(F.lit("ProRata")).alias("AllocationType"),
            F.lit(3).alias("PickUpOrder"),
            F.col("Y.TypeId"),
            F.col("Y.TrackingKey"),
            F.col("Y.Tag"),
            F.col("Y.IsExcludefromTransfer"),
            F.lit(None).cast("boolean").alias("GPPartnerReceivingCarry"),
            F.lit(None).cast("int").alias("LineID"),
            F.lit(None).cast("int").alias("704cAllocationTypeId"),
            F.lit(None).cast("string").alias("704cPercentageType"),
            F.lit(None).cast("int").alias("StateID"),
            F.col("Y._mode"),
        )
    )
    eff_pct_dated = eff_pct_dated.unionByName(yearly_missing, allowMissingColumns=True)

    # Update pickup order for newly added rows
    # Phase 3a-2: add _mode equality + include _mode in groupBy.
    new_pickup = (
        eff_pct_dated.alias("F")
        .join(
            pickup_order_dated.alias("P"),
            (F.col("P.InvestmentID") == F.col("F.InvestmentID"))
            & (F.col("P.Quarter") == F.col("F.Quarter"))
            & (F.col("P.TypeId") == F.col("F.TypeId"))
            & (F.col("P.TrackingKey") == F.col("F.TrackingKey"))
            & (F.col("P.Tag") == F.col("F.Tag"))
            & (F.col("P.IsExcludefromTransfer") == F.col("F.IsExcludefromTransfer"))
            & (F.col("P._mode") == F.col("F._mode")),
            "left_anti",
        )
        .groupBy("InvestmentID", "LineTypeID", "Quarter", "TypeId", "TrackingKey", "Tag", "IsExcludefromTransfer", "_mode")
        .agg(F.min("PickUpOrder").alias("PickUpOrder"))
    )
    pickup_order_dated = pickup_order_dated.unionByName(new_pickup, allowMissingColumns=True)

    _log_timing("compute_effective_percentage_dated", t0)
    return eff_pct_dated, pickup_order_dated, dated_entities


# ---------------------------------------------------------------------------
# compute_effective_percentage_non_dated
# SQL lines: 8147-8280
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def compute_effective_percentage_non_dated(
    spark: SparkSession, cfg: dict,
    non_dated_entities: DataFrame,
    final_cost_pct: DataFrame,
    cost_pct_min_quarter: DataFrame,
    transfers_adj: DataFrame,
) -> DataFrame:
    """Compute non-dated effective percentages:
    1. Transfer-affected cost % (from transfers_adj)
    2. Missing partner insertion from FinalCostPercentage
    3. Cost % from FinalCostPercentage + MinQuarter
    4. Yearly ProRata fallback

    Returns: eff_pct_non_dated
    """
    t0 = time.time()
    logger.info("[SECTION] compute_effective_percentage_non_dated")

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    mode = cfg.get("mode")

    # Broadcast cost_pct_min_quarter — small per-(entity, quarter) lookup
    # joined multiple times in Steps 1-2. Eliminates shuffles.
    if cost_pct_min_quarter is not None:
        cost_pct_min_quarter = F.broadcast(cost_pct_min_quarter)

    # ── Step 1: Transfer-affected cost % (non-dated) ──
    eff_pct_nd = None

    # Phase 3a-3: add _mode equality across all joins/anti-joins + project _mode
    # through every select/groupBy.
    if transfers_adj is not None and not transfers_adj.isEmpty() and mode != 4:
        # Broadcast transfers_adj — small per-entity transfer data (few hundred
        # rows across all modes). Eliminates shuffle in non_dated_entities join.
        transfers_adj_bc = F.broadcast(transfers_adj)
        transfer_nd = (
            non_dated_entities.alias("L")
            .join(
                transfers_adj_bc.alias("T"),
                (F.col("L.UnderlyingEntityID") == F.col("T.InvestmentID"))
                & (F.col("L.TypeID") == F.col("T.TypeID"))
                & (F.col("L.TrackingKey") == F.col("T.TrackingKey"))
                & (F.col("L.Tag") == F.col("T.Tag"))
                & (F.col("L._mode") == F.col("T._mode")),
            )
            .filter(F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)) == False)
            .groupBy(
                F.col("L.UnderlyingEntityID"), F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.col("T.PartnerNumber"), F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
                F.col("L._mode"),
            )
            .agg(F.sum(F.coalesce(F.col("T.EffectivePercent"), F.lit(0))).alias("EffPercentage"))
            .select(
                F.col("UnderlyingEntityID").alias("InvestmentID"),
                F.col("LineTypeID"),
                F.col("TypeID").alias("TypeId"),
                F.col("PartnerNumber"),
                F.col("EffPercentage"),
                F.lit("Cost").alias("AllocationType"),
                F.col("TrackingKey"),
                F.col("Tag"),
                F.lit("Q0").alias("Quarter"),
                F.lit(False).alias("IsExcludefromTransfer"),
                F.lit(None).cast("boolean").alias("GPPartnerReceivingCarry"),
                F.lit(None).cast("int").alias("LineID"),
                F.lit(None).cast("int").alias("PickUpOrder"),
                F.lit(None).cast("int").alias("704cAllocationTypeId"),
                F.lit(None).cast("string").alias("704cPercentageType"),
                F.lit(None).cast("int").alias("StateID"),
                F.col("_mode"),
            )
            .distinct()
        )
        eff_pct_nd = transfer_nd

        # Selected non-dated lines (for deletion)
        selected_nd = eff_pct_nd.select(
            F.col("InvestmentID"),
            F.coalesce(F.col("LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("TypeId").alias("TypeID"),
            F.col("TrackingKey"),
            F.col("Tag"),
            F.col("_mode"),
        ).distinct()

        # Insert missing partners from FinalCostPercentage
        missing_nd = (
            selected_nd.alias("S")
            .join(
                final_cost_pct.alias("Y"),
                (F.col("S.InvestmentID") == F.col("Y.DealId"))
                & (F.col("S.TypeID") == F.col("Y.TypeId"))
                & (F.col("S.TrackingKey") == F.col("Y.TrackingKey"))
                & (F.col("S.Tag") == F.col("Y.Tag"))
                & (F.col("S._mode") == F.col("Y._mode")),
            )
            .join(
                cost_pct_min_quarter.alias("M"),
                (F.col("Y.DealId") == F.col("M.DealID"))
                & (F.col("Y.Quarter") == F.col("M.Quarter"))
                & (F.col("Y.TypeId") == F.col("M.TypeID"))
                & (F.col("Y.TrackingKey") == F.col("M.TrackingKey"))
                & (F.col("Y.Tag") == F.col("M.Tag"))
                & (F.col("Y._mode") == F.col("M._mode")),
            )
            .join(
                eff_pct_nd.alias("F"),
                (F.col("Y.DealId") == F.col("F.InvestmentID"))
                & (F.col("Y.PartnerNumber") == F.col("F.PartnerNumber"))
                & (F.col("Y.TypeId") == F.col("F.TypeId"))
                & (F.col("Y.TrackingKey") == F.col("F.TrackingKey"))
                & (F.col("Y.Tag") == F.col("F.Tag"))
                & (F.coalesce(F.col("S.LineTypeID"), F.lit(-1)) == F.coalesce(F.col("F.LineTypeID"), F.lit(-1)))
                & (F.col("Y._mode") == F.col("F._mode")),
                "left",
            )
            .filter(F.col("F.PartnerNumber").isNull())
            .select(
                F.col("Y.DealId").alias("InvestmentID"),
                F.coalesce(F.col("S.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.col("Y.PartnerNumber"),
                F.coalesce(F.col("Y.CommitmentPercent"), F.lit(0)).alias("EffPercentage"),
                F.lit("Q0").alias("Quarter"),
                F.lit("Cost").alias("AllocationType"),
                F.col("Y.TypeId"),
                F.col("Y.TrackingKey"),
                F.col("Y.Tag"),
                F.lit(False).alias("IsExcludefromTransfer"),
                F.col("Y.GPPartnerReceivingCarry"),
                F.lit(None).cast("int").alias("LineID"),
                F.lit(None).cast("int").alias("PickUpOrder"),
                F.lit(None).cast("int").alias("704cAllocationTypeId"),
                F.lit(None).cast("string").alias("704cPercentageType"),
                F.lit(None).cast("int").alias("StateID"),
                F.col("Y._mode"),
            )
            .distinct()
        )
        eff_pct_nd = eff_pct_nd.unionByName(missing_nd, allowMissingColumns=True)

        # Delete non-dated entities that are now covered
        non_dated_entities = non_dated_entities.join(
            selected_nd.alias("F"),
            (non_dated_entities["UnderlyingEntityID"] == F.col("F.InvestmentID"))
            & (non_dated_entities["TypeID"] == F.col("F.TypeID"))
            & (non_dated_entities["TrackingKey"] == F.col("F.TrackingKey"))
            & (non_dated_entities["Tag"] == F.col("F.Tag"))
            & (F.coalesce(non_dated_entities["LineTypeID"], F.lit(-1)) == F.coalesce(F.col("F.LineTypeID"), F.lit(-1)))
            & (non_dated_entities["_mode"] == F.col("F._mode"))
            & (F.coalesce(non_dated_entities["IsExcludefromTransfer"], F.lit(False)) == False),
            "left_anti",
        )

    # ── Step 2: Cost % from FinalCostPercentage + MinQuarter ──
    # Phase 3a-3: add _mode equality + project _mode through.

    cost_nd = (
        non_dated_entities.alias("L")
        .join(
            cost_pct_min_quarter.alias("M"),
            (F.col("L.UnderlyingEntityID") == F.col("M.DealID"))
            & (F.col("L.TypeID") == F.col("M.TypeID"))
            & (F.col("L.TrackingKey") == F.col("M.TrackingKey"))
            & (F.col("L.Tag") == F.col("M.Tag"))
            & (F.col("L._mode") == F.col("M._mode")),
        )
        .join(
            final_cost_pct.alias("C"),
            (F.col("L.UnderlyingEntityID") == F.col("C.DealId"))
            & (F.col("C.Quarter") == F.col("M.Quarter"))
            & (F.col("L.TypeID") == F.col("C.TypeId"))
            & (F.col("L.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("L.Tag") == F.col("C.Tag"))
            & (F.col("L._mode") == F.col("C._mode")),
        )
        .select(
            F.col("L.UnderlyingEntityID").alias("InvestmentID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("C.PartnerNumber"),
            F.coalesce(F.col("C.CommitmentPercent"), F.lit(0)).alias("EffPercentage"),
            F.when(F.col("L.IsExcludefromTransfer") == True, F.lit("Cost without Transfer Adj %"))
            .otherwise(F.lit("Cost")).alias("AllocationType"),
            F.lit("Q0").alias("Quarter"),
            F.col("C.TypeId"),
            F.col("C.TrackingKey"),
            F.col("C.Tag"),
            F.when(F.col("L.IsExcludefromTransfer") == True, F.lit(True)).otherwise(F.lit(False)).alias("IsExcludefromTransfer"),
            F.col("C.`704cAllocationTypeId`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.lit(None).cast("int").alias("LineID"),
            F.lit(None).cast("int").alias("PickUpOrder"),
            F.lit(None).cast("int").alias("StateID"),
            F.col("L._mode"),
        )
        .distinct()
    )

    if eff_pct_nd is not None:
        eff_pct_nd = eff_pct_nd.unionByName(cost_nd, allowMissingColumns=True)
    else:
        eff_pct_nd = cost_nd

    # Delete matched non-dated entities
    # Phase 3a-3: add _mode equality.
    non_dated_entities = non_dated_entities.join(
        eff_pct_nd.select(
            F.col("InvestmentID").alias("_InvID"),
            F.col("TypeId").alias("_TypeId"),
            F.col("TrackingKey").alias("_TK"),
            F.col("Tag").alias("_Tag"),
            F.coalesce(F.col("LineTypeID"), F.lit(-1)).alias("_LTID"),
            F.col("_mode").alias("_M"),
        ).distinct().alias("F"),
        (non_dated_entities["UnderlyingEntityID"] == F.col("F._InvID"))
        & (non_dated_entities["TypeID"] == F.col("F._TypeId"))
        & (non_dated_entities["TrackingKey"] == F.col("F._TK"))
        & (non_dated_entities["Tag"] == F.col("F._Tag"))
        & (F.coalesce(non_dated_entities["LineTypeID"], F.lit(-1)) == F.col("F._LTID"))
        & (non_dated_entities["_mode"] == F.col("F._M")),
        "left_anti",
    )

    # ── Step 3: Yearly ProRata (non-dated) ──
    # Phase 3a-3: include _mode in groupBy + select. The TransfersAdj
    # default table has no _mode (lookup) — _mode rides from L (entities).
    yearly_nd = (
        non_dated_entities.alias("L")
        .join(
            F.broadcast(_tbl(spark, "TransfersAdjDefaultPercentage", cfg)).alias("D"),
            (F.col("D.RunID") == run_id) & (F.col("D.ClientID") == client_id),
        )
        .groupBy(
            F.col("L.UnderlyingEntityID"), F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("D.PartnerNumber"), F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.col("L.IsExcludefromTransfer"),
            F.col("L._mode"),
        )
        .agg(F.sum(F.coalesce(F.col("D.EffectivePercent"), F.lit(0))).alias("EffPercentage"))
        .select(
            F.col("UnderlyingEntityID").alias("InvestmentID"),
            F.col("LineTypeID"),
            F.col("PartnerNumber"),
            F.col("EffPercentage"),
            F.when(F.col("IsExcludefromTransfer") == True, F.lit("Cost without Transfer Adj %"))
            .otherwise(F.lit("ProRata")).alias("AllocationType"),
            F.lit("Q0").alias("Quarter"),
            F.col("TypeID").alias("TypeId"),
            F.col("TrackingKey"),
            F.col("Tag"),
            F.when(F.col("IsExcludefromTransfer") == True, F.lit(True)).otherwise(F.lit(False)).alias("IsExcludefromTransfer"),
            F.lit(None).cast("boolean").alias("GPPartnerReceivingCarry"),
            F.lit(None).cast("int").alias("LineID"),
            F.lit(None).cast("int").alias("PickUpOrder"),
            F.lit(None).cast("int").alias("704cAllocationTypeId"),
            F.lit(None).cast("string").alias("704cPercentageType"),
            F.lit(None).cast("int").alias("StateID"),
            F.col("_mode"),
        )
    )
    eff_pct_nd = eff_pct_nd.unionByName(yearly_nd, allowMissingColumns=True)

    _log_timing("compute_effective_percentage_non_dated", t0)
    return eff_pct_nd


# ---------------------------------------------------------------------------
# apply_plugging
# SQL lines: 8299-8640
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def apply_plugging(
    spark: SparkSession, cfg: dict,
    eff_pct_dated: DataFrame,
    eff_pct_non_dated: DataFrame,
    dar_setup: DataFrame,
) -> tuple:
    """Plugging logic: ensure percentages sum to 100% per group.

    For each (InvestmentID, LineTypeID, AllocationType, Quarter, TypeId,
    TrackingKey, Tag, IsExcludefromTransfer):
    1. Round percentages to 8 decimal places
    2. Compute excess = 1.0 - SUM(EffPercentage)
    3. Find "big partner" (max EffPercentage)
    4. Add excess to big partner's percentage

    Returns: (rounded_dated, rounded_non_dated)
    """
    t0 = time.time()
    logger.info("[SECTION] apply_plugging")

    dar_tx_id = cfg.get("default_alloc_rule_transaction_id")
    global_dar_tx_id = cfg.get("global_default_alloc_rule_transaction_id")

    def _plug(df, group_cols, extra_filter=None, has_704c=False):
        """Apply plugging to a DataFrame.

        Steps:
        1. Round EffPercentage to 8 decimals
        2. Compute excess = ROUND(ROUND(1,8) - ROUND(SUM,8), 8)
        3. Find max EffPercentage partner
        4. Add excess to big partner
        """
        # Round
        rounded = df.withColumn("EffPercentage", _sql_round(F.col("EffPercentage"), 8))

        # DAR setup exclusion: skip plugging for rules with AllocationPercentageType in certain types.
        # dar_setup is small per (run, mode); broadcast to avoid sort-merge against rounded.
        dar_exclude = (
            F.broadcast(dar_setup).alias("E")
            .filter(
                F.col("E.TransactionID").isin(dar_tx_id, global_dar_tx_id)
            )
            .join(
                F.broadcast(_tbl(spark, "ENU_AllocationPercentageType", cfg)).alias("EA"),
                (F.col("E.AllocationPercentageTypeID") == F.col("EA.AllocationPercentageTypeID")),
            )
        )

        if not has_704c:
            # Standard plugging: exclude rules with 'Allocate > 100%' or 'Allocate < 100%'
            dar_skip = dar_exclude.filter(
                F.col("EA.AllocationPercentageType").isin("Allocate > 100%", "Allocate < 100%")
            ).select("E.RuleID").distinct()

            plug_filter = extra_filter if extra_filter is not None else F.lit(True)

            plug = (
                rounded
                .join(dar_skip.alias("E"), F.col("TypeId") == F.col("E.RuleID"), "left")
                .filter(F.col("E.RuleID").isNull())
                .drop("E.RuleID")
            )
            if extra_filter is not None:
                plug = plug.filter(extra_filter)
        else:
            # 704c plugging: exclude rules with 'Allocate 100%'
            dar_skip = dar_exclude.filter(
                F.col("EA.AllocationPercentageType") == "Allocate 100%"
            ).select("E.RuleID").distinct()

            plug = (
                rounded
                .join(dar_skip.alias("E"), F.col("TypeId") == F.col("E.RuleID"), "left")
                .filter(F.col("E.RuleID").isNull())
                .drop("E.RuleID")
            )

        # Compute excess: HAVING SUM > 0 via window pre-filter
        w_sum = Window.partitionBy(*group_cols)
        excess = (
            plug
            .withColumn("_sum_eff", F.sum("EffPercentage").over(w_sum))
            .filter(_sql_round(F.col("_sum_eff"), 8) > 0)
            .groupBy(*group_cols)
            .agg(
                _sql_round(
                    _sql_round(F.lit(1.00000000), 8) - _sql_round(F.sum("EffPercentage"), 8),
                    8,
                ).alias("ExcessAllocationPercentage"),
            )
        )

        # Max commitment partner
        max_commit = (
            plug
            .groupBy(*group_cols)
            .agg(F.max("EffPercentage").alias("InvestmentPercentage"))
        )

        # Find big partner
        excess_with_partner = (
            excess.alias("T")
            .join(
                rounded.alias("S"),
                [F.col(f"T.{c}") == F.col(f"S.{c if c != 'TypeId' else 'TypeId'}") for c in group_cols],
            )
            .join(
                max_commit.alias("C"),
                [F.col(f"T.{c}") == F.col(f"C.{c}") for c in group_cols]
                + [F.col("C.InvestmentPercentage") == F.col("S.EffPercentage")],
            )
            .filter(F.col("T.ExcessAllocationPercentage") != 0)
            .select(
                *[F.col(f"T.{c}") for c in group_cols],
                F.col("S.PartnerNumber").alias("BigPartner"),
                F.col("T.ExcessAllocationPercentage"),
            )
            .distinct()
        )

        # Apply plug to big partner
        t_cols = rounded.columns
        rounded = (
            rounded.alias("T")
            .join(
                excess_with_partner.alias("S"),
                [F.col(f"T.{c}") == F.col(f"S.{c}") for c in group_cols]
                + [F.col("T.PartnerNumber") == F.col("S.BigPartner")],
                "left",
            )
            .withColumn(
                "EffPercentage",
                F.when(
                    F.col("S.BigPartner").isNotNull(),
                    _sql_round(F.col("T.EffPercentage") + F.col("S.ExcessAllocationPercentage"), 8),
                ).otherwise(F.col("T.EffPercentage")),
            )
            .select([F.col(f"T.`{c}`") if c != "EffPercentage" else F.col("EffPercentage") for c in t_cols])
        )

        return rounded

    # ── Plug dated ──
    # Phase 3a-3: include _mode in every group_cols list so windows/groupBys
    # partition per-mode and joins inside _plug carry _mode equality.
    dated_group = ["InvestmentID", "LineTypeID", "AllocationType", "Quarter", "TypeId", "TrackingKey", "Tag", "IsExcludefromTransfer", "_mode"]
    eff_pct_dated_rounded = _plug(eff_pct_dated, dated_group)

    # ── Plug non-dated (standard: 704cPercentageType='') ──
    nd_group = ["InvestmentID", "LineTypeID", "AllocationType", "Quarter", "TypeId", "TrackingKey", "Tag", "IsExcludefromTransfer", "_mode"]
    if "LineID" in eff_pct_non_dated.columns:
        nd_group.append("LineID")

    eff_pct_nd_rounded = _plug(
        eff_pct_non_dated,
        nd_group,
        extra_filter=F.coalesce(F.col("`704cPercentageType`"), F.lit("")) == "",
    )

    # ── Plug non-dated (704c: 704cPercentageType != '') ──
    nd_704c_group = nd_group + ["`704cAllocationTypeID`", "`704cPercentageType`"]
    # Re-plug for 704c rows
    nd_704c = eff_pct_non_dated.filter(
        F.coalesce(F.col("`704cPercentageType`"), F.lit("")) != ""
    )
    if not nd_704c.isEmpty():
        nd_704c_rounded = _plug(nd_704c, nd_704c_group, has_704c=True)
        # Merge: replace 704c rows in rounded with re-plugged ones
        eff_pct_nd_rounded = (
            eff_pct_nd_rounded
            .filter(F.coalesce(F.col("`704cPercentageType`"), F.lit("")) == "")
            .unionByName(nd_704c_rounded, allowMissingColumns=True)
        )

    _log_timing("apply_plugging", t0)
    return eff_pct_dated_rounded, eff_pct_nd_rounded


# ---------------------------------------------------------------------------
# apply_type_id_update
# SQL lines: 8640-8680
# Row count: N/A (in-place update)
# ---------------------------------------------------------------------------
def apply_type_id_update(
    cfg: dict,
    eff_pct_dated_rounded: DataFrame,
    eff_pct_non_dated_rounded: DataFrame,
    non_dated_entities_cost: DataFrame,
    dated_entities_cost: DataFrame,
) -> tuple:
    """Update TypeID for non-cost types using cost allocation percentage.

    When entities used CostAllocationTypeID as fallback but original TypeID
    was different, restore the original TypeID and rename AllocationType.

    Returns: (updated_dated, updated_non_dated)
    """
    t0 = time.time()
    logger.info("[SECTION] apply_type_id_update")

    cost_alloc_type_id = cfg["cost_allocation_type_id"]

    alloc_type_map = {
        "COST": "DEFAULT",
        "CostAdjustedDatedTransfer": "DefaultAdjustedDatedTransfer",
        "Cost without Transfer Adj %": "Default without Transfer Adj %",
    }

    def _update_type(df, cost_df, has_quarter=False):
        # Phase 3a-3: add _mode equality. nd_missing/dt_missing carry _mode
        # (from compute_missing_entities); eff_pct_*_rounded carry _mode from
        # the heavy effective_calc functions.
        join_cond = (
            (F.col("D.InvestmentID") == F.col("C.UnderlyingEntityID"))
            & (F.coalesce(F.col("D.LineTypeID"), F.lit(-1)) == F.coalesce(F.col("C.LineTypeID"), F.lit(-1)))
            & (F.col("D.TypeId") == cost_alloc_type_id)
            & (F.col("D.TrackingKey") == F.col("C.TrackingKey"))
            & (F.col("D.Tag") == F.col("C.Tag"))
            & (F.col("D.IsExcludefromTransfer") == F.col("C.IsExcludefromTransfer"))
            & (F.col("D._mode") == F.col("C._mode"))
        )
        if has_quarter:
            join_cond = join_cond & (F.col("D.Quarter") == F.col("C.Quarter"))

        matched = df.alias("D").join(cost_df.alias("C"), join_cond, "left_semi")
        unmatched = df.alias("D").join(cost_df.alias("C"), join_cond, "left_anti")

        # For matched: update TypeId and AllocationType
        updated = (
            df.alias("D")
            .join(cost_df.alias("C"), join_cond, "inner")
            .select(
                "D.*",
                F.col("C.TypeID").alias("_NewTypeId"),
            )
            .withColumn(
                "TypeId",
                F.col("_NewTypeId"),
            )
            .withColumn(
                "AllocationType",
                F.when(F.col("AllocationType") == "COST", F.lit("DEFAULT"))
                .when(F.col("AllocationType") == "CostAdjustedDatedTransfer", F.lit("DefaultAdjustedDatedTransfer"))
                .when(F.col("AllocationType") == "Cost without Transfer Adj %", F.lit("Default without Transfer Adj %"))
                .otherwise(F.lit("ProRata")),
            )
            .drop("_NewTypeId")
        )

        return unmatched.unionByName(updated, allowMissingColumns=True)

    # Non-dated update
    if non_dated_entities_cost is not None and not non_dated_entities_cost.isEmpty():
        eff_pct_non_dated_rounded = _update_type(eff_pct_non_dated_rounded, non_dated_entities_cost, has_quarter=False)

    # Dated update
    if dated_entities_cost is not None and not dated_entities_cost.isEmpty():
        eff_pct_dated_rounded = _update_type(eff_pct_dated_rounded, dated_entities_cost, has_quarter=True)

    _log_timing("apply_type_id_update", t0)
    return eff_pct_dated_rounded, eff_pct_non_dated_rounded


# ---------------------------------------------------------------------------
# build_final_output
# SQL lines: 8679-8850
# ---------------------------------------------------------------------------
def build_final_output(
    spark: SparkSession, cfg: dict,
    eff_pct_dated_rounded: DataFrame,
    eff_pct_non_dated_rounded: DataFrame,
    pickup_order_dated: DataFrame,
    entity_underlyings: DataFrame,
    final_amounts: DataFrame,
) -> DataFrame:
    """Mode-dependent final output assembly.

    Mode 1 (not PE): NonDated + Dated(with pickup) + FinalAmounts, with AssetClassId
    Mode 1 (PE): NonDated + Dated(with pickup), with LineID
    Mode 2: NonDated + Dated(with pickup), with AssetClassId
    Mode 3: NonDated + Dated(with pickup) + FinalAmounts, with AssetClassId
    Mode 4: NonDated only, 704c rows only

    Returns: combined output DataFrame for the mode in cfg["mode"].

    Phase 4 note: this function is still per-mode-aware via cfg["mode"]. The
    orchestrator's Pass C calls it once per mode, filtering fused inputs by
    _mode first. Output schema differs per mode, so per-mode invocation is
    correct; what's fused upstream are all the heavy join/aggregation passes.
    """
    t0 = time.time()
    logger.info("[SECTION] build_final_output")

    mode = cfg.get("mode")

    # Add AssetClassId from entity_underlyings
    # entity_underlyings has _mode (Phase 2a contract); we don't add _mode to
    # the join here because eff_pct rows already have a _mode and the lookup
    # of AssetClassId is mode-agnostic (asset class is a global property).
    # If we ever need per-mode asset class lookup, add _mode equality here.
    eu = entity_underlyings.select("UnderlyingEntityId", "TrackingKey", "AssetClassId").distinct() if entity_underlyings is not None else None

    def _add_asset_class(df, alias_prefix="L"):
        if eu is None:
            return df.withColumn("AssetClassId", F.lit(0))
        return (
            df.alias(alias_prefix)
            .join(
                eu.alias("E"),
                (F.col(f"{alias_prefix}.InvestmentID") == F.col("E.UnderlyingEntityId"))
                & (F.col(f"{alias_prefix}.TrackingKey") == F.col("E.TrackingKey")),
                "left",
            )
            .withColumn("AssetClassId", F.coalesce(F.col("E.AssetClassId"), F.lit(0)))
            .select(f"{alias_prefix}.*", "AssetClassId")
        )

    # Filter non-zero
    nd_nonzero = eff_pct_non_dated_rounded.filter(F.coalesce(F.col("EffPercentage"), F.lit(0)) != 0)
    dt_nonzero = eff_pct_dated_rounded.filter(F.coalesce(F.col("EffPercentage"), F.lit(0)) != 0)

    # Dated with pickup order join.
    # build_final_output is called PER MODE in orchestrator Pass D — inputs
    # are already filtered to a single mode and _mode is dropped before the
    # call, so a _mode equality predicate would reference a missing column.
    # If you ever switch to a fused single call here, re-add `& (F.col("L._mode") == F.col("T._mode"))`.
    dated_with_pickup = (
        dt_nonzero.alias("L")
        .join(
            pickup_order_dated.alias("T"),
            (F.col("L.InvestmentID") == F.col("T.InvestmentID"))
            & (F.col("L.PickUpOrder") == F.col("T.PickUpOrder"))
            & (F.col("L.TypeId") == F.col("T.TypeId"))
            & (F.col("L.TrackingKey") == F.col("T.TrackingKey"))
            & (F.col("L.Tag") == F.col("T.Tag"))
            & (F.col("T.Quarter") == F.col("L.Quarter"))
            & (F.col("L.IsExcludefromTransfer") == F.col("T.IsExcludefromTransfer"))
            & (F.coalesce(F.col("L.LineTypeID"), F.lit(-1)) == F.coalesce(F.col("T.LineTypeID"), F.lit(-1))),
        )
        .select("L.*")
    )

    
    # Ensure final_amounts has EffPercentage=0.0 — amount-based rows use
    # EffAmount instead; the SQL target table requires non-NULL EffPercentage.
    if final_amounts is not None and "EffPercentage" not in final_amounts.columns:
        final_amounts = final_amounts.withColumn("EffPercentage", F.lit(0.0))

    
    if mode == 4:
        # Mode 4: only non-dated 704c rows
        result = (
            nd_nonzero
            .filter(F.coalesce(F.col("`704cPercentageType`"), F.lit("")) != "")
        )
        result = _add_asset_class(result)

    elif mode == 1:
        nd_out = _add_asset_class(nd_nonzero, "L")
        dt_out = _add_asset_class(dated_with_pickup, "L")
        result = nd_out.unionByName(dt_out, allowMissingColumns=True)
        if final_amounts is not None and not final_amounts.isEmpty():
            fa_out = _add_asset_class(final_amounts, "L")
            result = result.unionByName(fa_out, allowMissingColumns=True)

    elif mode == 2:
        nd_out = _add_asset_class(nd_nonzero, "L")
        dt_out = _add_asset_class(dated_with_pickup, "L")
        result = nd_out.unionByName(dt_out, allowMissingColumns=True)

    elif mode == 3:
        nd_out = _add_asset_class(nd_nonzero, "L")
        dt_out = _add_asset_class(dated_with_pickup, "L")
        result = nd_out.unionByName(dt_out, allowMissingColumns=True)
        if final_amounts is not None and not final_amounts.isEmpty():
            fa_out = _add_asset_class(final_amounts, "L")
            result = result.unionByName(fa_out, allowMissingColumns=True)

    else:
        result = nd_nonzero.unionByName(dated_with_pickup, allowMissingColumns=True)

    _log_timing("build_final_output", t0)
    return result

