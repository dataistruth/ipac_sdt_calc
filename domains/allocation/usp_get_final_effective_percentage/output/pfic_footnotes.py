"""
pfic_footnotes.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
PFIC footnote, At Risk, Form926/8865/1042S/8886/199A processing.
UDF inlining: udfGetLatestCustomFootnoteTransactionIDs.
Conversion date: 2026-05-04

SQL lines: 3430-5700
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
import logging
import time

logger = logging.getLogger(__name__)


def _parse_textvalue_date(col, default: str = "1900-01-01"):
    """Parse a free-form date string from PF.TextValue (Form926Flowup).

    The column is varchar and stores dates in US format (e.g. '06/09/2025'
    or '6/9/2025'). PySpark's `cast("date")` only accepts ISO `yyyy-MM-dd`
    and throws CAST_INVALID_INPUT on US format. This helper accepts both
    US and ISO formats and falls back to `default` for nulls / unparseable
    strings — matching the original SQL Server behaviour where COALESCE +
    implicit cast was lenient.
    """
    return F.coalesce(
        F.to_date(col, "M/d/yyyy"),     # 06/09/2025 or 6/9/2025
        F.to_date(col, "yyyy-MM-dd"),   # ISO fallback
        F.lit(default).cast("date"),
    )


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _log_timing(name, start):
    logger.info(f"[TIMING] {name}: {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# _get_custom_footnote_line_types (inline udfGetLatestCustomFootnoteTransactionIDs)
# SQL lines: 3443-3446
# ---------------------------------------------------------------------------
def _get_custom_footnote_line_types(
    spark: SparkSession, cfg: dict,
) -> DataFrame:
    """Inline dbo.udfGetLatestCustomFootnoteTransactionIDs (LineTypeID subset).

    The full UDF returns (EntityID, TransactionID, LineTypeID, EventTypeID,
    RegisterTypeID, K1PackageID), but the orchestrator only consumes
    LineTypeID (used to detect custom-footnote line types in alloc_input
    at build_footnote_underlyings_ordered). So we port only the LineTypeID
    derivation, which is independent of the other output columns.

    SQL chain (from udfGetLatestCustomFootnoteTransactionIDs body):
        SELECT DISTINCT EL.LineTypeID
        FROM CustomImportDetail CD
        JOIN ENU_LineType EL ON EL.LineType = CD.ImportName
        WHERE CD.IsCustomFootnote = 1
          -- ENU_LineType scoped to current (ClientID, TaxPeriodID)

    Note: the previous conversion incorrectly joined CustomImportDetail
    to TransactionLog and selected CI.LineTypeID — but CustomImportDetail
    has no LineTypeID column, and TransactionLog isn't part of the chain.
    """
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]

    cd_footnote = (
        _tbl(spark, "CustomImportDetail", cfg)
        .filter(F.col("IsCustomFootnote") == True)
        .select(F.col("ImportName"))
        .distinct()
        .alias("CD")
    )
    enu_lt = (
        _tbl(spark, "ENU_LineType", cfg)
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .alias("EL")
    )
    return (
        F.broadcast(cd_footnote)
        .join(enu_lt, F.col("EL.LineType") == F.col("CD.ImportName"))
        .select(F.col("EL.LineTypeID"))
        .distinct()
    )


# ---------------------------------------------------------------------------
# _resolve_pfic_alloc_type
# Helper for allocation type resolution with LP/GP offset logic
# ---------------------------------------------------------------------------
def _resolve_pfic_alloc_type(
    adj_alloc_col, ai_alloc_col, line_type_col, line_desc_col, cfg: dict,
):
    """Resolve allocation type for PFIC/footnote lines.

    Priority: AdjustmentAllocationTypeID → AI.AllocationTypeId →
    LP/GP Offset check → CostAllocationTypeID
    """
    pfic_lt_id = cfg.get("pfic_footnote_line_type_id")
    at_risk_lt_id = cfg.get("at_risk_line_type_id")
    lp_offset_id = cfg.get("lp_offset_allocation_type_id")
    gp_offset_id = cfg.get("gp_offset_allocation_type_id")
    cost_alloc_id = cfg["cost_allocation_type_id"]

    offset_check = F.when(
        (line_type_col.isin([pfic_lt_id, at_risk_lt_id]))
        & (F.coalesce(line_desc_col, F.lit("")).contains("- LP - Offset")),
        F.lit(lp_offset_id).cast("int"),
    ).when(
        (line_type_col.isin([pfic_lt_id, at_risk_lt_id]))
        & (F.coalesce(line_desc_col, F.lit("")).contains("- GP - Offset")),
        F.lit(gp_offset_id).cast("int"),
    ).otherwise(F.lit(cost_alloc_id).cast("int"))

    return F.coalesce(adj_alloc_col, F.coalesce(ai_alloc_col, offset_check))


# ---------------------------------------------------------------------------
# build_footnote_underlyings_ordered
# SQL lines: 3450-3615
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_footnote_underlyings_ordered(
    spark: SparkSession, cfg: dict,
    underlying_mod: DataFrame,
    underlyings_combined: DataFrame,
    alloc_input: DataFrame,
    book_effective: DataFrame,
    all_underlyings: DataFrame,
    dar_setup: DataFrame,
    map_dar: DataFrame,
    custom_fn_line_types: DataFrame,
) -> DataFrame:
    """Build #TempAllUnderlyingsFNOrdered + merge into #TempAllUnderlyings.

    Two paths (CAR vs DAR) similar to build_all_underlyings_ordered,
    but filtered to footnote line types (PFIC, Form926, Form8865, etc.).
    Also handles At Risk backfill for missing DAR entries.

    Returns: updated all_underlyings with footnote rows appended.
    """
    t0 = time.time()
    logger.info("[SECTION] build_footnote_underlyings_ordered")

    # If alloc_input is None (Mode 1 non-PE), no footnote underlyings to add
    if alloc_input is None:
        _log_timing("build_footnote_underlyings_ordered (skipped — no alloc_input)", t0)
        return all_underlyings

    entity_id = cfg["entity_id"]
    is_car = cfg.get("is_custom_allocation_rule_enabled", "U") == "C"
    override_flag = cfg.get("override_indirect_lookthrough_asset_class", "")
    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    adj_lt_id = cfg["adjustment_line_type_id"]
    at_risk_lt_id = cfg.get("at_risk_line_type_id")
    mode = cfg.get("mode")
    _704c_alloc_type_id = cfg.get("_704c_allocation_type_id")
    dar_tid = cfg.get("default_alloc_rule_transaction_id")
    gdar_tid = cfg.get("global_default_alloc_rule_transaction_id")

    enu_ut = F.broadcast(_tbl(spark, "ENU_UnderlyingType", cfg))
    enu_lt = F.broadcast(_tbl(spark, "ENU_LineType", cfg))

    # Footnote line type filter
    fn_line_types = ["PFIC Footnote", "Form926", "Form8865", "Form1042S", "Form8886", "Form199A", "At Risk"]

    # Tracking key matching
    def _tracking_match(ai_entity_col, ai_underlying_col, ai_tracking_col, l_tracking_col, u_type_col):
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

    def _match_key(col):
        return F.when(F.coalesce(col, F.lit("")) == "", F.lit("-1")).otherwise(col)

    # Filter alloc_input to footnote line types + custom footnote types
    fn_input = (
        alloc_input.alias("L")
        .join(enu_lt.alias("EL"), F.col("L.LineTypeID") == F.col("EL.LineTypeID"))
        .join(
            custom_fn_line_types.alias("CF"),
            F.col("L.LineTypeID") == F.col("CF.LineTypeID"),
            "left",
        )
        .filter(
            F.col("EL.LineType").isin(fn_line_types)
            | F.col("CF.LineTypeID").isNotNull()
        )
        .select("L.*")
    )

    ordered_parts = []

    if is_car:
        # CAR path — similar to K1 but with footnote lines
        adj_to_k1 = F.when(
            F.col("L.LineTypeID") == adj_lt_id, F.lit(k1_lt_id).cast("int")
        ).otherwise(F.col("L.LineTypeID"))

        # Resolve allocation type for mode 4 with 704c
        alloc_type_expr = F.when(
            (F.coalesce(F.col("B.AdjustmentAllocationTypeID").cast("string"), F.lit("")) == "")
            & (F.lit(mode) == 4),
            F.lit(_704c_alloc_type_id).cast("int"),
        ).otherwise(
            F.coalesce(
                F.col("B.AdjustmentAllocationTypeID"),
                F.coalesce(F.col("D.RuleID"), F.lit(cost_alloc_type_id).cast("int")),
            )
        )

        valid_tids = [t for t in [dar_tid, gdar_tid] if t is not None]

        car_fn = (
            underlying_mod.alias("AI")
            .join(enu_ut.alias("U"), F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"))
            .join(
                fn_input.alias("L"),
                (F.col("L.EntityID") == F.col("AI.UnderlyingEntityID"))
                & (_tracking_match(
                    F.col("AI.EntityID"), F.col("AI.UnderlyingEntityID"),
                    F.col("AI.TrackingKey"), F.col("L.TrackingKey"),
                    F.col("U.UnderlyingType"),
                )),
            )
            .join(
                book_effective.alias("B"),
                (F.col("L.EntityID") == F.col("B.UnderlyingEntityID"))
                & (F.col("L.LineID") == F.col("B.LineID"))
                & (F.col("B.LineID") != -1)
                & (F.col("B.SourceID") == F.col("L.LineTypeID"))
                & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("L.TrackingKey")))
                & (_match_key(F.col("B.Tag")) == _match_key(F.col("L.Tag")))
                & (
                    F.when(
                        (F.coalesce(F.col("B.AdjustmentAllocationTypeID").cast("string"), F.lit("")) == "")
                        & (F.lit(mode) == 4),
                        F.lit(_704c_alloc_type_id),
                    ).otherwise(
                        F.coalesce(F.col("B.AdjustmentAllocationTypeID"), F.lit(cost_alloc_type_id)),
                    ).cast("int")
                    == F.col("AI.AllocationTypeID")
                ),
                "left",
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
                & (F.col("M.SourceID") == F.col("L.LineTypeID"))
                & (F.col("M.TransactionID").isin(valid_tids)),
                "left",
            )
            .join(
                dar_setup.alias("D"),
                (F.col("D.RuleID") == F.col("AI.AllocationTypeID"))
                & (F.col("AI.UnderlyingType") == F.col("D.UnderlyingTypeID"))
                & (F.col("D.TransactionID").isin(valid_tids)),
                "left",
            )
            .select(
                F.col("AI.UnderlyingType").alias("Underlyingtype"),
                F.col("AI.UnderlyingEntityID").alias("UnderlyingEntityId"),
                F.col("AI.EntityID").alias("EntityId"),
                F.col("L.TrackingKey"),
                F.col("AI.TrackingKey").alias("TrackingMatch"),
                alloc_type_expr.alias("AllocationTypeId"),
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
                F.col("L.LineTypeID"),
                F.lit("PERCENT").alias("AllocationBy"),
                F.coalesce(F.col("M.ExcludeFromTransfers"), F.lit(0))
                .cast("boolean").alias("IsExcludefromTransfer"),
            )
        )
        ordered_parts.append(car_fn)

    else:
        # DAR path for footnotes
        valid_tids = [t for t in [dar_tid, gdar_tid] if t is not None]

        dar_fn = (
            underlying_mod.alias("AI")
            .join(enu_ut.alias("U"), F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"))
            .join(
                fn_input.alias("L"),
                (F.col("L.EntityID") == F.col("AI.UnderlyingEntityID"))
                & (_tracking_match(
                    F.col("AI.EntityID"), F.col("AI.UnderlyingEntityID"),
                    F.col("AI.TrackingKey"), F.col("L.TrackingKey"),
                    F.col("U.UnderlyingType"),
                )),
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
                & (F.col("M.SourceID") == F.col("L.LineTypeID")),
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
                        F.col("L.LineTypeID"),
                    ).orderBy(
                        F.col("AI.hlevel"),
                        F.col("R.DisplayOrder").desc(),
                        F.col("U.DisplayOrder"),
                        F.col("M.SelectedMappingID").desc(),
                        F.col("AI.TrackingKey"),
                    )
                ).alias("RankForUnderlyingPickup"),
                F.col("L.LineTypeID"),
                F.lit("PERCENT").alias("AllocationBy"),
                F.coalesce(F.col("M.ExcludeFromTransfers"), F.lit(0))
                .cast("boolean").alias("IsExcludefromTransfer"),
            )
        )
        ordered_parts.append(dar_fn)

    # At Risk backfill — insert K1 rule if At Risk not present in DAR
    # SQL lines 3577-3610
    valid_tids_full = [t for t in [dar_tid, gdar_tid] if t is not None]
    at_risk_backfill = (
        underlyings_combined.alias("AI")
        .join(enu_ut.alias("U"), F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"))
        .join(
            fn_input.alias("L"),
            (F.col("L.EntityID") == F.col("AI.UnderlyingEntityID"))
            & (_tracking_match(
                F.col("AI.EntityID"), F.col("AI.UnderlyingEntityID"),
                F.col("AI.TrackingKey"), F.col("L.TrackingKey"),
                F.col("U.UnderlyingType"),
            )),
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
            & (F.col("M.SourceID") == k1_lt_id),
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
        .join(enu_lt.alias("EL"), F.col("L.LineTypeID") == F.col("EL.LineTypeID"))
        .filter(
            (F.col("M.TransactionID").isin(valid_tids_full))
            & (F.col("D.TransactionID").isin(valid_tids_full))
            & (F.col("EL.LineType") == "At Risk")
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
                    F.col("L.LineTypeID"),
                    F.col("EA.DisplayOrder"),
                ).orderBy(
                    F.col("AI.HLevel"),
                    F.col("R.DisplayOrder").desc(),
                    F.col("U.DisplayOrder"),
                    F.col("M.SelectedMappingID").desc(),
                    F.col("EA.DisplayOrder"),
                    F.col("AI.TrackingKey"),
                )
            ).alias("RankForUnderlyingPickup"),
            F.col("L.LineTypeID"),
            F.col("EA.AllocationBy"),
            F.coalesce(F.col("M.ExcludeFromTransfers"), F.lit(0))
            .cast("boolean").alias("IsExcludefromTransfer"),
        )
    )

    # Union all ordered parts
    fn_ordered = ordered_parts[0]
    for part in ordered_parts[1:]:
        fn_ordered = fn_ordered.unionByName(part)

    # Left anti join to exclude At Risk already present
    at_risk_new = (
        at_risk_backfill.alias("AB")
        .join(
            fn_ordered.alias("TFO"),
            (F.col("AB.UnderlyingEntityId") == F.col("TFO.UnderlyingEntityId"))
            & (F.col("AB.EntityId") == F.col("TFO.EntityId"))
            & (F.col("AB.LineID") == F.col("TFO.LineID"))
            & (F.col("AB.LineTypeID") == F.col("TFO.LineTypeID"))
            & (F.col("AB.LineTypeID") == at_risk_lt_id),
            "left_anti",
        )
    )

    fn_ordered = fn_ordered.unionByName(at_risk_new)

    # Take rank 1 only, append to all_underlyings
    fn_rank1 = fn_ordered.filter(F.col("RankForUnderlyingPickup") == F.lit(1))

    # Select matching columns for union
    fn_for_union = fn_rank1.select(
        "Underlyingtype", "UnderlyingEntityId", "EntityId", "TrackingKey",
        "TrackingMatch", "AllocationTypeId", "LineID",
        "RankForUnderlyingPickup", "LineTypeID", "IsExcludefromTransfer",
    )

    # Ensure all_underlyings has matching columns
    au_cols = set(all_underlyings.columns)
    fn_cols = set(fn_for_union.columns)
    missing_in_fn = au_cols - fn_cols
    for c in missing_in_fn:
        fn_for_union = fn_for_union.withColumn(c, F.lit(None).cast("string"))
    missing_in_au = fn_cols - au_cols
    for c in missing_in_au:
        all_underlyings = all_underlyings.withColumn(c, F.lit(None).cast("string"))

    result = all_underlyings.unionByName(fn_for_union, allowMissingColumns=True)

    _log_timing("build_footnote_underlyings_ordered", t0)
    return result


# ---------------------------------------------------------------------------
# build_footnote_input_lines
# SQL lines: 3660-3930
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_footnote_input_lines(
    spark: SparkSession, cfg: dict,
    alloc_input: DataFrame,
    book_effective: DataFrame,
    all_underlyings: DataFrame,
    map_dar: DataFrame,
) -> tuple:
    """Build #TempInputLines for footnote types.

    Processes in order:
    1. At Risk lines (with K1/AtRisk BookEffective join)
    2. PFIC/other footnote lines (FootNoteID match)
    3. FootNoteID-only match (LineID = -1)
    4. FootNoteID = -1, LineID match
    5. Remaining unmatched

    Returns: (input_lines_fn, alloc_input_remaining, book_effective_remaining)
    """
    t0 = time.time()
    logger.info("[SECTION] build_footnote_input_lines")

    # If alloc_input is None (Mode 1 non-PE), return None
    if alloc_input is None:
        _log_timing("build_footnote_input_lines (skipped — no alloc_input)", t0)
        return None

    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    at_risk_lt_id = cfg.get("at_risk_line_type_id")
    pfic_lt_id = cfg.get("pfic_footnote_line_type_id")
    k1_lt_id = cfg["k1_line_type_id"]

    def _match_key(col):
        return F.when(F.coalesce(col, F.lit("")) == "", F.lit("-1")).otherwise(col)

    # --- Pass 1: At Risk lines ---
    at_risk_input = (
        alloc_input.alias("I")
        .join(
            F.broadcast(_tbl(spark, "K1LineItem", cfg)).alias("P"),
            F.col("P.LineID") == F.col("I.LineID"),
        )
        .join(
            F.broadcast(_tbl(spark, "MAP_K1LineItemLineType", cfg)).alias("MK"),
            (F.col("I.LineID") == F.col("MK.K1LineItemID"))
            & (F.col("I.LineTypeID") == at_risk_lt_id),
            "left",
        )
        .join(
            book_effective.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.coalesce(F.col("I.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("B.FootNoteID"), F.lit(0)))
            & (F.coalesce(F.col("I.LineID"), F.lit(0)) == F.coalesce(F.col("B.LineID"), F.lit(0)))
            & (F.col("B.SourceID") == at_risk_lt_id)
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
            "left",
        )
        .join(
            book_effective.alias("BK"),
            (F.col("I.EntityID") == F.col("BK.UnderlyingEntityID"))
            & (F.coalesce(F.col("I.LineID"), F.lit(0)) == F.coalesce(F.col("BK.LineID"), F.lit(0)))
            & (F.col("BK.SourceID") == k1_lt_id)
            & (_match_key(F.col("BK.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("BK.Tag")) == _match_key(F.col("I.Tag"))),
            "left",
        )
        .join(
            all_underlyings.alias("AI"),
            (F.col("I.EntityID") == F.col("AI.UnderlyingEntityId"))
            & (F.col("I.LineID") == F.col("AI.LineID"))
            & (F.col("I.LineTypeID") == F.col("AI.LineTypeID"))
            & (F.col("I.TrackingKey") == F.col("AI.TrackingKey")),
            "left",
        )
        .filter(
            (F.col("I.LineTypeID") == at_risk_lt_id)
            & (
                ((F.coalesce(F.col("B.FootNoteID"), F.lit(0)) != -1)
                 & (F.coalesce(F.col("B.LineID"), F.lit(0)) != -1))
                | (F.col("AI.LineID") != -1)
                | (F.coalesce(F.col("BK.LineID"), F.lit(0)) != -1)
            )
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.LineID"),
            F.col("I.QuicklinkID").alias("QuickLinkID"),
            _resolve_pfic_alloc_type(
                F.col("B.AdjustmentAllocationTypeID"),
                F.coalesce(F.col("BK.AdjustmentAllocationTypeID"), F.col("AI.AllocationTypeId")),
                F.col("I.LineTypeID"),
                F.col("P.LineDescription"),
                cfg,
            ).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.coalesce(F.col("I.TrackingKey"), F.lit(""))).alias("TrackingKey"),
            F.coalesce(F.col("B.Tag"), F.coalesce(F.col("I.Tag"), F.lit(""))).alias("Tag"),
            F.col("I.LineTypeID"),
            F.coalesce(F.col("B.IsExcludefromTransfer"), F.col("AI.IsExcludefromTransfer")).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Pass 2: PFIC/other footnotes with FootNoteID + LineID match ---
    pfic_input_p2 = (
        alloc_input.alias("I")
        .join(
            F.broadcast(_tbl(spark, "PFICFootnoteLineItem", cfg)).alias("P"),
            (F.col("I.LineID") == F.col("P.LineID"))
            & (F.col("I.LineTypeID") == pfic_lt_id),
            "left",
        )
        .join(
            book_effective.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.coalesce(F.col("I.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("B.FootNoteID"), F.lit(0)))
            & (F.coalesce(F.col("I.LineID"), F.lit(0)) == F.coalesce(F.col("B.LineID"), F.lit(0)))
            & (F.col("I.LineTypeID") == F.col("B.SourceID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
            "left",
        )
        .join(
            all_underlyings.alias("AI"),
            (F.col("I.EntityID") == F.col("AI.UnderlyingEntityId"))
            & (F.col("I.LineID") == F.col("AI.LineID"))
            & (F.col("I.LineTypeID") == F.col("AI.LineTypeID"))
            & (F.col("I.TrackingKey") == F.col("AI.TrackingKey")),
            "left",
        )
        .filter(
            ((F.coalesce(F.col("B.FootNoteID"), F.lit(0)) != -1)
             & (F.coalesce(F.col("B.LineID"), F.lit(0)) != -1))
            | (F.col("AI.LineID") != -1)
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.LineID"),
            F.col("I.QuicklinkID").alias("QuickLinkID"),
            _resolve_pfic_alloc_type(
                F.col("B.AdjustmentAllocationTypeID"),
                F.col("AI.AllocationTypeId"),
                F.col("I.LineTypeID"),
                F.col("P.LineDescription"),
                cfg,
            ).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.coalesce(F.col("I.TrackingKey"), F.lit(""))).alias("TrackingKey"),
            F.coalesce(F.col("B.Tag"), F.coalesce(F.col("I.Tag"), F.lit(""))).alias("Tag"),
            F.col("I.LineTypeID"),
            F.coalesce(F.col("B.IsExcludefromTransfer"), F.col("AI.IsExcludefromTransfer")).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Pass 3: FootNoteID match, LineID = -1 ---
    pfic_input_p3 = (
        alloc_input.alias("I")
        .join(
            F.broadcast(_tbl(spark, "PFICFootnoteLineItem", cfg)).alias("P"),
            (F.col("I.LineID") == F.col("P.LineID"))
            & (F.col("I.LineTypeID") == pfic_lt_id),
            "left",
        )
        .join(
            book_effective.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.coalesce(F.col("I.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("B.FootNoteID"), F.lit(0)))
            & (F.col("I.LineTypeID") == F.col("B.SourceID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
        )
        .filter(
            (F.coalesce(F.col("B.FootNoteID"), F.lit(0)) != -1)
            & (F.coalesce(F.col("B.LineID"), F.lit(0)) == -1)
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.LineID"),
            F.col("I.QuicklinkID").alias("QuickLinkID"),
            _resolve_pfic_alloc_type(
                F.col("B.AdjustmentAllocationTypeID"),
                F.lit(None).cast("int"),
                F.col("I.LineTypeID"),
                F.col("P.LineDescription"),
                cfg,
            ).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.coalesce(F.col("I.TrackingKey"), F.lit(""))).alias("TrackingKey"),
            F.coalesce(F.col("B.Tag"), F.coalesce(F.col("I.Tag"), F.lit(""))).alias("Tag"),
            F.col("I.LineTypeID"),
            F.coalesce(F.col("B.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Pass 4: FootNoteID = -1, LineID match ---
    # SQL: WHERE ISNULL(B.FootNoteID, 0) = -1 AND ISNULL(B.LineID, 0) <> -1
    # B has no specific footnote but DOES have a specific line match.
    pfic_input_p4 = (
        alloc_input.alias("I")
        .join(
            F.broadcast(_tbl(spark, "PFICFootnoteLineItem", cfg)).alias("P"),
            (F.col("I.LineID") == F.col("P.LineID"))
            & (F.col("I.LineTypeID") == pfic_lt_id),
            "left",
        )
        .join(
            book_effective.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.coalesce(F.col("I.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("B.FootNoteID"), F.lit(0)))
            & (F.coalesce(F.col("B.LineID"), F.lit(0)) == F.coalesce(F.col("I.LineID"), F.lit(0)))
            & (F.col("I.LineTypeID") == F.col("B.SourceID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
        )
        .filter(
            (F.coalesce(F.col("B.FootNoteID"), F.lit(0)) == -1)
            & (F.coalesce(F.col("B.LineID"), F.lit(0)) != -1)
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.LineID"),
            F.col("I.QuicklinkID").alias("QuickLinkID"),
            _resolve_pfic_alloc_type(
                F.col("B.AdjustmentAllocationTypeID"),
                F.lit(None).cast("int"),
                F.col("I.LineTypeID"),
                F.col("P.LineDescription"),
                cfg,
            ).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.coalesce(F.col("I.TrackingKey"), F.lit(""))).alias("TrackingKey"),
            F.coalesce(F.col("B.Tag"), F.coalesce(F.col("I.Tag"), F.lit(""))).alias("Tag"),
            F.col("I.LineTypeID"),
            F.coalesce(F.col("B.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Pass 5: FootNoteID = -1, LineID = -1 (blanket book effective) ---
    # SQL: WHERE ISNULL(B.FootNoteID, 0) = -1 AND ISNULL(B.LineID, 0) = -1
    pfic_input_p5 = (
        alloc_input.alias("I")
        .join(
            F.broadcast(_tbl(spark, "PFICFootnoteLineItem", cfg)).alias("P"),
            (F.col("I.LineID") == F.col("P.LineID"))
            & (F.col("I.LineTypeID") == pfic_lt_id),
            "left",
        )
        .join(
            book_effective.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("I.LineTypeID") == F.col("B.SourceID"))
            & (F.coalesce(F.col("I.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("B.FootNoteID"), F.lit(0)))
            & (F.coalesce(F.col("I.LineID"), F.lit(0)) == F.coalesce(F.col("B.LineID"), F.lit(0)))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
        )
        .filter(
            (F.coalesce(F.col("B.FootNoteID"), F.lit(0)) == -1)
            & (F.coalesce(F.col("B.LineID"), F.lit(0)) == -1)
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.LineID"),
            F.col("I.QuicklinkID").alias("QuickLinkID"),
            _resolve_pfic_alloc_type(
                F.col("B.AdjustmentAllocationTypeID"),
                F.lit(None).cast("int"),
                F.col("I.LineTypeID"),
                F.col("P.LineDescription"),
                cfg,
            ).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.coalesce(F.col("I.TrackingKey"), F.lit(""))).alias("TrackingKey"),
            F.coalesce(F.col("B.Tag"), F.coalesce(F.col("I.Tag"), F.lit(""))).alias("Tag"),
            F.col("I.LineTypeID"),
            F.coalesce(F.col("B.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Pass 6: Remaining unmatched ---
    pfic_input_p6 = (
        alloc_input.alias("I")
        .join(
            F.broadcast(_tbl(spark, "PFICFootnoteLineItem", cfg)).alias("P"),
            (F.col("I.LineID") == F.col("P.LineID"))
            & (F.col("I.LineTypeID") == pfic_lt_id),
            "left",
        )
        .join(
            book_effective.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("I.LineTypeID") == F.col("B.SourceID"))
            & (F.coalesce(F.col("I.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("B.FootNoteID"), F.lit(0)))
            & (F.coalesce(F.col("I.LineID"), F.lit(0)) == F.coalesce(F.col("B.LineID"), F.lit(0)))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
            "left",
        )
        .filter(F.col("B.UnderlyingEntityID").isNull())
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.LineID"),
            F.col("I.QuicklinkID").alias("QuickLinkID"),
            _resolve_pfic_alloc_type(
                F.col("B.AdjustmentAllocationTypeID"),
                F.lit(None).cast("int"),
                F.col("I.LineTypeID"),
                F.col("P.LineDescription"),
                cfg,
            ).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.coalesce(F.col("I.TrackingKey"), F.lit(""))).alias("TrackingKey"),
            F.coalesce(F.col("B.Tag"), F.coalesce(F.col("I.Tag"), F.lit(""))).alias("Tag"),
            F.col("I.LineTypeID"),
            F.coalesce(F.col("B.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    result = (
        at_risk_input
        .unionByName(pfic_input_p2)
        .unionByName(pfic_input_p3)
        .unionByName(pfic_input_p4)
        .unionByName(pfic_input_p5)
        .unionByName(pfic_input_p6)
    )

    # --- UPDATE IsExcludefromTransfer from MapDefaultAllocRuleToLineItem ---
    # SQL: UPDATE T SET T.IsExcludefromTransfer=M.ExcludefromTransfers
    #      FROM #TempInputLines T JOIN #MapDefaultAllocRuleToLineItem M
    #      ON CASE WHEN M.SelectedMappingID=-1 THEN 1 ELSE T.LineID END
    #       = CASE WHEN M.SelectedMappingID=-1 THEN 1 ELSE M.SelectedMappingID END
    #      AND T.LineTypeID=M.SourceID AND T.TypeID=M.RuleID
    #      WHERE M.ClientID=@LocalClientID AND M.TaxPeriodID=@LocalTaxPeriodID AND @IsDARSetup=1
    is_dar_setup = cfg.get("is_dar_setup", False)
    if is_dar_setup:
        client_id = cfg["client_id"]
        tax_period_id = cfg["tax_period_id"]
        map_dar_filtered = (
            map_dar
            .filter(
                (F.col("ClientID") == client_id)
                & (F.col("TaxPeriodID") == tax_period_id)
            )
            .alias("M")
        )
        join_cond = (
            (
                F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("T.LineID"))
                == F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("M.SelectedMappingID"))
            )
            & (F.col("T.LineTypeID") == F.col("M.SourceID"))
            & (F.col("T.TypeID") == F.col("M.RuleID"))
        )
        result = (
            result.alias("T")
            .join(F.broadcast(map_dar_filtered), join_cond, "left")
            .select(
                F.col("T.UnderlyingEntityID"),
                F.col("T.LineID"),
                F.col("T.QuickLinkID"),
                F.col("T.TypeID"),
                F.col("T.TrackingKey"),
                F.col("T.Tag"),
                F.col("T.LineTypeID"),
                F.coalesce(
                    F.col("M.ExcludeFromTransfers").cast("boolean"),
                    F.col("T.IsExcludefromTransfer"),
                ).alias("IsExcludefromTransfer"),
            )
        )

    _log_timing("build_footnote_input_lines", t0)
    return result


# ---------------------------------------------------------------------------
# build_footnote_dated_entities
# SQL lines: 4140-4610
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_footnote_dated_entities(
    spark: SparkSession, cfg: dict,
    input_lines: DataFrame,
    non_dated: DataFrame,
    dated: DataFrame,
) -> tuple:
    """Map footnote input lines to dated/non-dated entities via package/flowup joins.

    Handles: PFIC (QuarterAllocations, DatesOfDistribution, Part V, PFICAllocationByQuarter),
    Form926, Form8865, Form1042S, Form8886, Form199A, At Risk, Custom Footnotes.

    Returns: (non_dated_updated, dated_updated)
    """
    t0 = time.time()
    logger.info("[SECTION] build_footnote_dated_entities")

    # If input_lines is None (Mode 1 non-PE, no alloc_input), return unchanged
    if input_lines is None:
        _log_timing("build_footnote_dated_entities (skipped — no input_lines)", t0)
        return non_dated, dated

    run_id = cfg["run_id"]
    alloc_type_name = cfg.get("allocation_type_name", "")
    is_dated_transfers = cfg.get("is_dated_transfers_configured", "")
    is_pe_book_dated = (alloc_type_name == "PE Book Allocation" and is_dated_transfers == "C")
    is_pfic_by_quarter = cfg.get("is_pfic_allocation_by_quarter", "U") == "C"
    part_v_allocated = cfg.get("part_v_allocated", 0)
    pfic_lt_id = cfg.get("pfic_footnote_line_type_id")
    at_risk_lt_id = cfg.get("at_risk_line_type_id")

    enu_lt = F.broadcast(_tbl(spark, "ENU_LineType", cfg))

    # --- Lookup quarter line IDs ---
    def _get_quarter_line_id(table_name, short_name):
        row = (
            _tbl(spark, table_name, cfg)
            .filter(
                (F.col("ShortName") == short_name)
                & (F.col("IsActive") == True)
            )
            .select("LineID")
            .first()
        )
        return row["LineID"] if row else None

    pfic_quarter_line_id = _get_quarter_line_id("PFICFootnoteLineItem", "QuarterAllocations")
    form199a_quarter_line_id = _get_quarter_line_id("Form199ALineItem", "QuarterAllocations")
    form8886_quarter_line_id = _get_quarter_line_id("Form8886LineItem", "QuarterAllocations")
    form926_quarter_line_id = _get_quarter_line_id("Form926LineItem", "TransferDate")
    form8865_quarter_line_id = _get_quarter_line_id("Form8865LineItem", "TransferDate")
    pfic_dist_date_line_id = _get_quarter_line_id("PFICFootnoteLineItem", "DatesofDistribution")

    # Helper: join through Package → K1Package → get LowerTierEntityID
    def _package_entity_join(il, pkg_table, pkg_id_col, ql_col, line_type_filter):
        return (
            il.alias("L")
            .join(enu_lt.alias("EL"), F.col("L.LineTypeID") == F.col("EL.LineTypeID"))
            .join(
                F.broadcast(_tbl(spark, pkg_table, cfg)).alias("P"),
                F.col("L.QuickLinkID") == F.col(f"P.{pkg_id_col}"),
            )
            .join(
                F.broadcast(_tbl(spark, "K1Package", cfg)).alias("K"),
                F.col("P.K1PackageID") == F.col("K.K1PackageID"),
            )
            .filter(F.col("EL.LineType") == line_type_filter)
        )

    # --- PFIC Dated (via QuarterAllocations mapped lines) ---
    pfic_dated_mapped = (
        _package_entity_join(input_lines, "PFICFootnotePackage", "PFICFootnoteID", "QuicklinkID", "PFIC Footnote")
        .join(
            F.broadcast(_tbl(spark, "PFICFootnoteLineItem", cfg)).alias("PL"),
            F.col("L.LineID") == F.col("PL.LineID"),
        )
        .join(
            F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg)).alias("EDF"),
            (F.col("EDF.Category") == "DatedFootnoteLines")
            & (F.col("PL.ShortName") == F.col("EDF.LookUpData")),
        )
        .select(
            F.col("EDF.LookUpValue").alias("Quarter"),
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.col("L.TypeID"),
            F.col("L.TrackingKey"),
            F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
            F.col("L.LineID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.lit(None).cast("int").alias("Preference"),
            F.lit(None).cast("date").alias("transferdate"),
        )
        .distinct()
    )

    # --- PFIC Dated (via PFICFootnoteFlowup quarter text) ---
    pfic_dated_flowup = (
        _package_entity_join(input_lines, "PFICFootnotePackage", "PFICFootnoteID", "QuicklinkID", "PFIC Footnote")
        .join(
            _tbl(spark, "PFICFootnoteFlowup", cfg).alias("PF"),
            (F.col("PF.PFICFootnoteID") == F.col("P.PFICFootnoteID"))
            & (F.col("PF.RunID") == run_id)
            & (F.col("PF.LineID") == pfic_quarter_line_id)
            & (F.coalesce(F.col("PF.TextValue"), F.lit("")) != ""),
        )
        .select(
            F.col("PF.TextValue").alias("Quarter"),
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.col("L.TypeID"),
            F.col("L.TrackingKey"),
            F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
            F.lit(None).cast("int").alias("LineID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.lit(None).cast("int").alias("Preference"),
            F.lit(None).cast("date").alias("transferdate"),
        )
        .distinct()
    )

    # --- PFIC Non-Dated (no quarter flowup) ---
    pfic_non_dated = (
        _package_entity_join(input_lines, "PFICFootnotePackage", "PFICFootnoteID", "QuicklinkID", "PFIC Footnote")
        .join(
            _tbl(spark, "PFICFootnoteFlowup", cfg)
            .filter(
                (F.col("RunID") == run_id)
                & (F.col("LineID") == pfic_quarter_line_id)
                & (F.coalesce(F.col("TextValue"), F.lit("")) != "")
            )
            .select("PFICFootnoteID")
            .distinct()
            .alias("PF"),
            F.col("P.PFICFootnoteID") == F.col("PF.PFICFootnoteID"),
            "left_anti",
        )
        .select(
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("L.TypeID"),
            F.col("L.TrackingKey"),
            F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Form926 Dated/Non-Dated ---
    if is_pe_book_dated and form926_quarter_line_id:
        form926_dated = (
            _package_entity_join(input_lines, "Form926Package", "Form926ID", "QuicklinkID", "Form926")
            .join(
                _tbl(spark, "Form926Flowup", cfg).alias("PF"),
                (F.col("PF.Form926ID") == F.col("P.Form926ID"))
                & (F.col("PF.RunID") == run_id)
                & (F.col("PF.LineID") == form926_quarter_line_id)
                & (F.lower(F.coalesce(F.col("PF.TextValue"), F.lit(""))) != "various"),
            )
            .join(
                F.broadcast(_tbl(spark, "QuarterDates", cfg)).alias("D"),
                _parse_textvalue_date(F.col("PF.TextValue"))
                .between(F.col("D.StartDate"), F.col("D.EndDate")),
            )
            .select(
                F.coalesce(F.col("D.Quarter"), F.lit("Q0")).alias("Quarter"),
                F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
                F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
                F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
                F.col("L.LineID"),
                F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.col("D.Preference"),
                _parse_textvalue_date(F.col("PF.TextValue")).alias("transferdate"),
            )
            .distinct()
        )
    elif form926_quarter_line_id:
        form926_dated = (
            _package_entity_join(input_lines, "Form926Package", "Form926ID", "QuicklinkID", "Form926")
            .join(
                _tbl(spark, "Form926Flowup", cfg).alias("PF"),
                (F.col("PF.Form926ID") == F.col("P.Form926ID"))
                & (F.col("PF.RunID") == run_id)
                & (F.col("PF.LineID") == form926_quarter_line_id)
                & (F.lower(F.coalesce(F.col("PF.TextValue"), F.lit(""))) != "various"),
            )
            .select(
                F.concat(F.lit("Q"), F.coalesce(F.quarter(_parse_textvalue_date(F.col("PF.TextValue"))), F.lit(0)).cast("string")).alias("Quarter"),
                F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
                F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
                F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
                F.col("L.LineID"),
                F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.lit(None).cast("int").alias("Preference"),
                _parse_textvalue_date(F.col("PF.TextValue")).alias("transferdate"),
            )
            .distinct()
        )
    else:
        form926_dated = None

    form926_non_dated = (
        _package_entity_join(input_lines, "Form926Package", "Form926ID", "QuicklinkID", "Form926")
        .join(
            _tbl(spark, "Form926Flowup", cfg)
            .filter(
                (F.col("RunID") == run_id)
                & (F.col("LineID") == form926_quarter_line_id)
                & (F.lower(F.coalesce(F.col("TextValue"), F.lit(""))) != "various")
            )
            .select("Form926ID")
            .distinct()
            .alias("PF"),
            F.col("P.Form926ID") == F.col("PF.Form926ID"),
            "left_anti",
        )
        .select(
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Form8865 Dated/Non-Dated ---
    form8865_dated = (
        _package_entity_join(input_lines, "Form8865Package", "Form8865ID", "QuicklinkID", "Form8865")
        .join(
            _tbl(spark, "Form8865Flowup", cfg).alias("PF"),
            (F.col("PF.Form8865ID") == F.col("P.Form8865ID"))
            & (F.col("PF.RunID") == run_id)
            & (F.col("PF.LineID") == form8865_quarter_line_id)
            & (F.lower(F.coalesce(F.col("PF.TextValue"), F.lit(""))) != "various"),
        )
        .select(
            F.concat(F.lit("Q"), F.coalesce(F.quarter(_parse_textvalue_date(F.col("PF.TextValue"))), F.lit(0)).cast("string")).alias("Quarter"),
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
            F.lit(None).cast("int").alias("LineID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.lit(None).cast("int").alias("Preference"),
            F.lit(None).cast("date").alias("transferdate"),
        )
        .distinct()
    ) if form8865_quarter_line_id else None

    form8865_non_dated = (
        _package_entity_join(input_lines, "Form8865Package", "Form8865ID", "QuicklinkID", "Form8865")
        .join(
            _tbl(spark, "Form8865Flowup", cfg).alias("PF"),
            (F.col("PF.Form8865ID") == F.col("P.Form8865ID"))
            & (F.col("PF.RunID") == run_id)
            & (F.col("PF.LineID") == form8865_quarter_line_id)
            & (F.lower(F.coalesce(F.col("PF.TextValue"), F.lit(""))) != "various"),
            "left",
        )
        .filter(F.col("PF.Form8865ID").isNull())
        .select(
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Form1042S (Non-Dated only) ---
    form1042s_non_dated = (
        _package_entity_join(input_lines, "Form1042SPackage", "Form1042SID", "QuicklinkID", "Form1042S")
        .select(
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Form8886 Dated/Non-Dated ---
    form8886_dated = (
        _package_entity_join(input_lines, "Form8886Package", "Form8886ID", "QuicklinkID", "Form8886")
        .join(
            _tbl(spark, "Form8886FlowUp", cfg).alias("PF"),
            (F.col("PF.Form8886ID") == F.col("P.Form8886ID"))
            & (F.col("PF.RunID") == run_id)
            & (F.col("PF.LineID") == form8886_quarter_line_id)
            & (F.coalesce(F.col("PF.TextValue"), F.lit("")) != ""),
        )
        .select(
            F.col("PF.TextValue").alias("Quarter"),
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
            F.lit(None).cast("int").alias("LineID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.lit(None).cast("int").alias("Preference"),
            F.lit(None).cast("date").alias("transferdate"),
        )
        .distinct()
    ) if form8886_quarter_line_id else None

    form8886_non_dated = (
        _package_entity_join(input_lines, "Form8886Package", "Form8886ID", "QuicklinkID", "Form8886")
        .join(
            _tbl(spark, "Form8886FlowUp", cfg)
            .filter(
                (F.col("RunID") == run_id)
                & (F.col("LineID") == form8886_quarter_line_id)
                & (F.coalesce(F.col("TextValue"), F.lit("")) != "")
            )
            .select("Form8886ID")
            .distinct()
            .alias("PF"),
            F.col("P.Form8886ID") == F.col("PF.Form8886ID"),
            "left_anti",
        )
        .select(
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Form199A Dated/Non-Dated ---
    form199a_dated = (
        _package_entity_join(input_lines, "Form199APackage", "Form199AID", "QuicklinkID", "Form199A")
        .join(
            _tbl(spark, "Form199AFlowUp", cfg).alias("PF"),
            (F.col("PF.Form199AID") == F.col("P.Form199AID"))
            & (F.col("PF.RunID") == run_id)
            & (F.col("PF.LineID") == form199a_quarter_line_id)
            & (F.coalesce(F.col("PF.TextValue"), F.lit("")) != ""),
        )
        .select(
            F.col("PF.TextValue").alias("Quarter"),
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
            F.lit(None).cast("int").alias("LineID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.lit(None).cast("int").alias("Preference"),
            F.lit(None).cast("date").alias("transferdate"),
        )
        .distinct()
    ) if form199a_quarter_line_id else None

    form199a_non_dated = (
        _package_entity_join(input_lines, "Form199APackage", "Form199AID", "QuicklinkID", "Form199A")
        .join(
            _tbl(spark, "Form199AFlowUp", cfg).alias("PF"),
            (F.col("PF.Form199AID") == F.col("P.Form199AID"))
            & (F.col("PF.RunID") == run_id)
            & (F.col("PF.LineID") == form199a_quarter_line_id)
            & (F.coalesce(F.col("PF.TextValue"), F.lit("")) != ""),
            "left",
        )
        .filter(F.col("PF.Form199AID").isNull())
        .select(
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- At Risk (Non-Dated only) ---
    at_risk_non_dated = (
        input_lines.alias("L")
        .join(enu_lt.alias("EL"), F.col("L.LineTypeID") == F.col("EL.LineTypeID"))
        .join(
            _tbl(spark, "AtRiskPackage", cfg).alias("P"),
            F.col("L.QuickLinkID") == F.col("P.AtRiskID"),
        )
        .join(
            _tbl(spark, "K1Package", cfg).alias("K"),
            F.col("P.K1PackageID") == F.col("K.K1PackageID"),
        )
        .join(
            _tbl(spark, "AtRiskFlowup", cfg).alias("PF"),
            (F.col("PF.AtRiskID") == F.col("P.AtRiskID"))
            & (F.col("PF.RunID") == run_id),
            "left",
        )
        .filter(F.col("L.LineTypeID") == at_risk_lt_id)
        .select(
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Custom Footnotes (Non-Dated only) ---
    custom_fn_non_dated = (
        input_lines.alias("L")
        .join(enu_lt.alias("EL"), F.col("L.LineTypeID") == F.col("EL.LineTypeID"))
        .join(
            _get_custom_footnote_line_types(spark, cfg).alias("CF"),
            F.col("CF.LineTypeID") == F.col("EL.LineTypeID"),
        )
        .join(
            _tbl(spark, "CustomFootNotePackage", cfg).alias("P"),
            F.col("L.QuickLinkID") == F.col("P.CustomFootnoteID"),
        )
        .join(
            _tbl(spark, "K1Package", cfg).alias("K"),
            F.col("P.K1PackageID") == F.col("K.K1PackageID"),
        )
        .join(
            _tbl(spark, "CustomFootnoteFlowup", cfg).alias("PF"),
            (F.col("PF.CustomFootnoteID") == F.col("P.CustomFootnoteID"))
            & (F.col("PF.RunID") == run_id),
            "left",
        )
        .select(
            F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
            F.col("L.TypeID"), F.col("L.TrackingKey"), F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # --- Assemble non-dated ---
    all_non_dated = [pfic_non_dated, form926_non_dated, form8865_non_dated,
                     form1042s_non_dated, form8886_non_dated, form199a_non_dated,
                     at_risk_non_dated, custom_fn_non_dated]
    non_dated_new = all_non_dated[0]
    for nd in all_non_dated[1:]:
        non_dated_new = non_dated_new.unionByName(nd, allowMissingColumns=True)

    non_dated_result = non_dated.unionByName(non_dated_new, allowMissingColumns=True)

    # --- PFIC Part V Dated (distribution date → quarter mapping) ---
    # SQL: IF(@PartVAllocated = 1) ... INSERT INTO #TempDatedEntities
    pfic_part_v_dated = None
    if part_v_allocated:
        # Step 1: Build #tmpPartVQuarters equivalent
        # JOIN PFICFootnoteFlowup → QuarterDates on parsed date BETWEEN Start/End
        part_v_quarters = (
            _tbl(spark, "PFICFootnoteFlowup", cfg).alias("PF")
            .filter(
                (F.col("PF.RunID") == run_id)
                & (F.col("PF.LineID") == pfic_dist_date_line_id)
                & (F.coalesce(F.col("PF.TextValue"), F.lit("")) != "")
                & (F.lower(F.coalesce(F.col("PF.TextValue"), F.lit(""))) != "various")
            )
            .join(
                F.broadcast(_tbl(spark, "QuarterDates", cfg)).alias("D"),
                _parse_textvalue_date(F.col("PF.TextValue"))
                .between(F.col("D.StartDate"), F.col("D.EndDate")),
            )
            .select(
                F.col("PF.PFICFootnoteID"),
                F.coalesce(F.col("D.Quarter"), F.lit("Q0")).alias("Quarter"),
                F.col("PF.TextValue"),
            )
        )

        # Step 2: Join input_lines → Package → K1Package → part_v_quarters → dated entities
        pfic_part_v_dated = (
            _package_entity_join(input_lines, "PFICFootnotePackage", "PFICFootnoteID", "QuicklinkID", "PFIC Footnote")
            .join(
                part_v_quarters.alias("D"),
                F.col("D.PFICFootnoteID") == F.col("P.PFICFootnoteID"),
            )
            .select(
                F.coalesce(F.col("D.Quarter"), F.lit("Q0")).alias("Quarter"),
                F.col("K.LowerTierEntityID").alias("UnderlyingEntityID"),
                F.col("L.TypeID"),
                F.col("L.TrackingKey"),
                F.col("L.Tag"),
                F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
                F.col("L.LineID"),
                F.coalesce(F.col("L.LineTypeID"), F.lit(-1)).alias("LineTypeID"),
                F.lit(None).cast("int").alias("Preference"),
                _parse_textvalue_date(F.col("D.TextValue")).alias("transferdate"),
            )
            .distinct()
        )

        # Store part_v_quarters in cfg for downstream use (effective_calc)
        cfg["_part_v_quarters_df"] = part_v_quarters

    # --- Assemble dated ---
    all_dated = [pfic_dated_mapped, pfic_dated_flowup]
    if pfic_part_v_dated is not None:
        all_dated.append(pfic_part_v_dated)
    if form926_dated is not None:
        all_dated.append(form926_dated)
    if form8865_dated is not None:
        all_dated.append(form8865_dated)
    if form8886_dated is not None:
        all_dated.append(form8886_dated)
    if form199a_dated is not None:
        all_dated.append(form199a_dated)

    if all_dated:
        dated_new = all_dated[0]
        for d in all_dated[1:]:
            dated_new = dated_new.unionByName(d, allowMissingColumns=True)
        dated_result = dated.unionByName(dated_new, allowMissingColumns=True)
    else:
        dated_result = dated

    # --- PFIC Allocation by Quarter (distribution date / max quarter / Q0) ---
    # SQL: IF(ISNULL(@IsPFICAllocationbyQuarter, 'U') = 'C')
    if is_pfic_by_quarter and input_lines is not None:
        # Build #TempInputLinesPFIC equivalent
        pfic_input_lines = (
            input_lines
            .filter(F.col("LineTypeID") == pfic_lt_id)
            .select(
                "UnderlyingEntityID",
                F.lit(pfic_lt_id).alias("LineTypeID"),
                F.col("QuickLinkID").alias("PFICFootnoteID"),
                "LineID", "TypeID", "TrackingKey", "Tag",
                F.coalesce(F.col("IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
            )
            .distinct()
        )

        # --- 1. Distribution Date Allocation → #TempDatedEntities ---
        # Month from PFICFootnoteFlowup.TextValue → QuarterMonth lookup
        pfic_by_quarter_dated = (
            pfic_input_lines.alias("L")
            .join(
                _tbl(spark, "PFICFootnoteFlowup", cfg).alias("PF"),
                (F.col("PF.PFICFootnoteID") == F.col("L.PFICFootnoteID"))
                & (F.col("PF.RunID") == run_id)
                & (F.col("PF.LineID") == pfic_dist_date_line_id)
                & (F.lower(F.coalesce(F.col("PF.TextValue"), F.lit("various"))) != "various"),
            )
            .join(
                F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg)).alias("D"),
                (F.col("D.LookUpValue") == F.coalesce(
                    F.month(_parse_textvalue_date(F.col("PF.TextValue"))), F.lit(0)
                ).cast("string"))
                & (F.col("D.Category") == "QuarterMonth"),
            )
            .join(
                F.broadcast(_tbl(spark, "PFICFootnoteLineItem", cfg)).alias("PL"),
                F.col("L.LineID") == F.col("PL.LineID"),
            )
            .filter(F.col("PL.DefaultAllocationRule") == "Distribution Date Allocation")
            .join(
                dated_result.alias("TD"),
                (F.col("TD.UnderlyingEntityID") == F.col("L.UnderlyingEntityID"))
                & (F.col("TD.LineTypeID") == F.lit(pfic_lt_id))
                & (F.col("TD.Quarter") == F.col("D.LookUpData"))
                & (F.col("TD.TypeID") == F.col("L.TypeID"))
                & (F.col("TD.TrackingKey") == F.col("L.TrackingKey"))
                & (F.col("TD.Tag") == F.col("L.Tag"))
                & (F.col("L.IsExcludefromTransfer") == F.col("TD.IsExcludefromTransfer")),
                "left_anti",
            )
            .select(
                F.col("L.UnderlyingEntityID"),
                F.lit(pfic_lt_id).alias("LineTypeID"),
                F.col("D.LookUpData").alias("Quarter"),
                F.col("L.TypeID"),
                F.col("L.TrackingKey"),
                F.col("L.Tag"),
                F.col("L.IsExcludefromTransfer"),
            )
            .distinct()
        )
        dated_result = dated_result.unionByName(pfic_by_quarter_dated, allowMissingColumns=True)

        # --- 2. Max Quarter Allocation → #TempDatedEntities with Q4 ---
        pfic_max_quarter_dated = (
            pfic_input_lines.alias("L")
            .join(
                F.broadcast(_tbl(spark, "PFICFootnoteLineItem", cfg)).alias("PL"),
                F.col("L.LineID") == F.col("PL.LineID"),
            )
            .filter(F.col("PL.DefaultAllocationRule") == "Max Quarter Allocation")
            .join(
                dated_result.alias("TD"),
                (F.col("TD.UnderlyingEntityID") == F.col("L.UnderlyingEntityID"))
                & (F.col("TD.LineTypeID") == F.lit(pfic_lt_id))
                & (F.col("TD.Quarter") == "Q4")
                & (F.col("TD.TypeID") == F.col("L.TypeID"))
                & (F.col("TD.TrackingKey") == F.col("L.TrackingKey"))
                & (F.col("TD.Tag") == F.col("L.Tag"))
                & (F.col("L.IsExcludefromTransfer") == F.col("TD.IsExcludefromTransfer")),
                "left_anti",
            )
            .select(
                F.col("L.UnderlyingEntityID"),
                F.lit(pfic_lt_id).alias("LineTypeID"),
                F.lit("Q4").alias("Quarter"),
                F.col("L.TypeID"),
                F.col("L.TrackingKey"),
                F.col("L.Tag"),
                F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
            )
            .distinct()
        )
        dated_result = dated_result.unionByName(pfic_max_quarter_dated, allowMissingColumns=True)

        # --- 3. Q0 Allocation → #TempNonDatedEntities ---
        pfic_q0_non_dated = (
            pfic_input_lines.alias("L")
            .join(
                F.broadcast(_tbl(spark, "PFICFootnoteLineItem", cfg)).alias("PL"),
                F.col("L.LineID") == F.col("PL.LineID"),
            )
            .filter(F.col("PL.DefaultAllocationRule") == "Q0 Allocation")
            .join(
                non_dated_result.alias("TD"),
                (F.col("TD.UnderlyingEntityID") == F.col("L.UnderlyingEntityID"))
                & (F.col("TD.LineTypeID") == F.lit(pfic_lt_id))
                & (F.col("TD.TypeID") == F.col("L.TypeID"))
                & (F.col("TD.TrackingKey") == F.col("L.TrackingKey"))
                & (F.col("TD.Tag") == F.col("L.Tag"))
                & (F.col("L.IsExcludefromTransfer") == F.col("TD.IsExcludefromTransfer")),
                "left_anti",
            )
            .select(
                F.col("L.UnderlyingEntityID"),
                F.lit(pfic_lt_id).alias("LineTypeID"),
                F.col("L.TypeID"),
                F.col("L.TrackingKey"),
                F.col("L.Tag"),
                F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
            )
            .distinct()
        )
        non_dated_result = non_dated_result.unionByName(pfic_q0_non_dated, allowMissingColumns=True)

    # --- Update IsExcludefromTransfer from MapDefaultAllocRuleToLineItem ---
    # SQL line 4133: Fix for 331593
    # NOTE: This UPDATE is now applied inside build_footnote_input_lines()
    # before returning `result`, so no action needed here.

    _log_timing("build_footnote_dated_entities", t0)
    return non_dated_result, dated_result
