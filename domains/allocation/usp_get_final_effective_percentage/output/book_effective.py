"""
book_effective.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Book effective data, line items, allocation rules, yearly/quarterly loading.
Conversion date: 2026-05-04

SQL lines: 1700-1810, 2710-2800
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
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
# load_allocation_rules
# SQL lines: 1701-1716
# Row count: POSSIBLY-EMPTY (depends on workflow config)
# ---------------------------------------------------------------------------
def load_allocation_rules(
    spark: SparkSession, cfg: dict
) -> tuple:
    """Load DefaultAllocationRuleSetup, MapDefaultAllocRuleToLineItem,
    EntityAllocationRule_Snapshot.

    Returns: (dar_setup_df, map_dar_df, entity_alloc_rule_df)
    """
    t0 = time.time()
    logger.info("[SECTION] load_allocation_rules")

    dar_tid = cfg.get("default_alloc_rule_transaction_id")
    gdar_tid = cfg.get("global_default_alloc_rule_transaction_id")

    filter_tids = [t for t in [dar_tid, gdar_tid] if t is not None]

    # #DefaultAllocationRuleSetup
    dar_setup = (
        _tbl(spark, "DefaultAllocationRuleSetup", cfg)
        .filter(F.col("TransactionID").isin(filter_tids))
        .select(
            "TransactionID", "RuleID", "AllocationPercentageTypeID",
            "AllocationByID", "UnderlyingTypeID", "RuleTypeID",
            "RuleGroupID", "ClientID", "TaxPeriodID", "EntityID",
        )
    )

    # #MapDefaultAllocRuleToLineItem
    map_dar = (
        _tbl(spark, "MapDefaultAllocRuleToLineItem", cfg)
        .filter(F.col("TransactionID").isin(filter_tids))
        .select(
            "TransactionID", "SourceID", "StateID", "SelectedMappingID",
            "RuleID", "ExcludeFromTransfers", "ClientID", "TaxPeriodID",
            "EntityID",
        )
    )

    # #TempEnitityAllocationRule (from EntityAllocationRule_Snapshot)
    ear_wf_id = cfg.get("entity_allocation_rule_workflow_id")
    entity_alloc_rule = (
        _tbl(spark, "EntityAllocationRule_Snapshot", cfg)
        .filter(F.col("WorkflowID") == ear_wf_id)
        .select("LineID", "UpdatedAllocationRuleID")
        .withColumnRenamed("LineID", "LineId")
    )

    _log_timing("load_allocation_rules", t0)
    return dar_setup, map_dar, entity_alloc_rule


# ---------------------------------------------------------------------------
# load_line_items
# SQL lines: 1718-1722
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def load_line_items(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load #LineItem from K1LineItem UNION BoxjklLineItem.

    K1LineItem: LineID, AllocationTypeRuleId, @K1LineTypeID, TransactionDate,
                IsTransactionDate, IsTransfersAdjusted
    BoxjklLineItem: LineID, @YearlyAllocationTypeID, @BoxJKLLineTypeID,
                    NULL, 0, 1
    """
    t0 = time.time()
    logger.info("[SECTION] load_line_items")

    k1_line_type_id = cfg["k1_line_type_id"]
    yearly_alloc_type_id = cfg["yearly_allocation_type_id"]
    box_jkl_line_type_id = cfg["box_jkl_line_type_id"]

    k1 = (
        _tbl(spark, "K1LineItem", cfg)
        .select(
            F.col("LineID"),
            F.col("AllocationTypeRuleId"),
            F.lit(k1_line_type_id).cast("int").alias("LineTypeID"),
            F.col("TransactionDate"),
            F.col("IsTransactionDate"),
            F.col("IsTransfersAdjusted"),
        )
    )

    bjkl = (
        _tbl(spark, "BoxjklLineItem", cfg)
        .select(
            F.col("LineID"),
            F.lit(yearly_alloc_type_id).cast("int").alias("AllocationTypeRuleId"),
            F.lit(box_jkl_line_type_id).cast("int").alias("LineTypeID"),
            F.lit(None).cast("timestamp").alias("TransactionDate"),
            F.lit(False).alias("IsTransactionDate"),
            F.lit(True).alias("IsTransfersAdjusted"),
        )
    )

    result = k1.unionByName(bjkl)

    if result.isEmpty():
        logger.warning("load_line_items produced 0 rows — no K1 or BoxJKL lines found")

    _log_timing("load_line_items", t0)
    return result


# ---------------------------------------------------------------------------
# load_book_effective_data
# SQL lines: 1743-1763
# Row count: POSSIBLY-EMPTY (no custom allocation rules configured)
# ---------------------------------------------------------------------------
def load_book_effective_data(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load #TempBookEffectiveData from BookEffective_Snapshot.

    Also applies the AdjustmentAllocationTypeID → CostAllocationTypeID update
    where the value equals BookAllocationTypeID.
    """
    t0 = time.time()
    logger.info("[SECTION] load_book_effective_data")

    custom_alloc_wf_id = cfg.get("custom_allocation_workflow_id")
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    book_alloc_type_id = cfg.get("book_allocation_type_id")
    cost_alloc_type_id = cfg["cost_allocation_type_id"]

    bed = (
        _tbl(spark, "BookEffective_Snapshot", cfg)
        .filter(
            (F.col("WorkflowID") == custom_alloc_wf_id)
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select(
            "UnderlyingEntityID", "LineID", "FootNoteID", "SourceID",
            F.col("AllocationTypeID").alias("AllocationTypeid"),
            F.when(
                F.col("AdjustmentAllocationTypeID") == book_alloc_type_id,
                F.lit(cost_alloc_type_id).cast("int"),
            ).otherwise(F.col("AdjustmentAllocationTypeID")).alias("AdjustmentAllocationTypeID"),
            "TrackingKey", "Tag",
            F.coalesce(F.col("IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
    )

    _log_timing("load_book_effective_data", t0)
    return bed


# ---------------------------------------------------------------------------
# load_yearly_lines
# SQL lines: 1763
# Row count: POSSIBLY-EMPTY (only if yearly allocation exists in book effective)
# ---------------------------------------------------------------------------
def load_yearly_lines(book_effective: DataFrame, cfg: dict) -> DataFrame:
    """Extract #TempYearlyLines from #TempBookEffectiveData.

    Filters to rows where AdjustmentAllocationTypeID = yearly.
    """
    yearly_alloc_type_id = cfg["yearly_allocation_type_id"]

    return (
        book_effective
        .filter(F.col("AdjustmentAllocationTypeID") == yearly_alloc_type_id)
        .select("UnderlyingEntityID", "AllocationTypeid",
                "AdjustmentAllocationTypeID", "TrackingKey", "Tag")
        .distinct()
    )


# ---------------------------------------------------------------------------
# load_quarters
# SQL lines: 1765-1779
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def load_quarters(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load #Quarters — from QuarterDates (PE Book + DatedTransfers) or ENU_DF_DataList."""
    t0 = time.time()
    logger.info("[SECTION] load_quarters")

    alloc_type_name = cfg.get("allocation_type_name", "")
    is_dated_transfers = cfg.get("is_dated_transfers_configured", "")

    if alloc_type_name == "PE Book Allocation" and is_dated_transfers == "C":
        result = (
            _tbl(spark, "QuarterDates", cfg)
            .select(F.col("Quarter"))
        )
    else:
        result = (
            _tbl(spark, "ENU_DF_DataList", cfg)
            .filter(F.col("Category") == "Quarters")
            .select(F.col("LookUpData").alias("Quarter"))
        )

    if result.isEmpty():
        logger.warning("load_quarters produced 0 rows")

    _log_timing("load_quarters", t0)
    return result


# ---------------------------------------------------------------------------
# load_yearly_data
# SQL lines: 1781
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def load_yearly_data(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load #YearlyData from Yearly_Snapshot."""
    yearly_wf_id = cfg.get("yearly_workflow_id")
    return (
        _tbl(spark, "Yearly_Snapshot", cfg)
        .filter(F.col("WorkflowId") == yearly_wf_id)
    )


# ---------------------------------------------------------------------------
# build_lookthrough_input_modes14
# SQL lines: 2710-2740
# Row count: ALWAYS-NON-EMPTY (for modes 1,4)
# ---------------------------------------------------------------------------
def build_lookthrough_input_modes14(
    spark: SparkSession, cfg: dict
) -> DataFrame:
    """Load #TempLookThroughAllocationInput for modes 1,4.

    From LookThroughAllocationInput where ROUND(Amount,0)!=0 + BoxJKL filter.
    """
    t0 = time.time()
    logger.info("[SECTION] build_lookthrough_input_modes14")

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]

    k1_lt_id = cfg["k1_line_type_id"]
    adj_lt_id = cfg["adjustment_line_type_id"]
    box_jkl_lt_id = cfg["box_jkl_line_type_id"]

    result = (
        _tbl(spark, "LookThroughAllocationInput", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
            & (F.col("LineTypeID").isin([k1_lt_id, adj_lt_id, box_jkl_lt_id]))
            & (
                (F.col("LineTypeID") == box_jkl_lt_id)
                | (
                    F.col("LineTypeID").isin([k1_lt_id, adj_lt_id])
                    & (_sql_round(F.coalesce(F.col("Amount"), F.lit(0.0)), 0) != 0)
                )
            )
        )
        .select(
            "RunID", "ClientID", "EntityID", "LineTypeID", "LineID",
            "Amount", "QuicklinkID", "Amount704b", "TrackingKey", "Tag",
        )
    )

    if result.isEmpty():
        logger.warning("build_lookthrough_input_modes14 produced 0 rows")

    _log_timing("build_lookthrough_input_modes14", t0)
    return result


# ---------------------------------------------------------------------------
# build_footnote_lines
# SQL lines: 2740-2760
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_footnote_lines(spark: SparkSession, cfg: dict) -> DataFrame:
    """Build #TempFootnoteLines from MAP_DerivedLines + ENU_AttributeType.

    Returns DF with columns: FedLine (BaseLineID), FootnoteLine (DerivedLineID).
    """
    t0 = time.time()
    logger.info("[SECTION] build_footnote_lines")

    result = (
        _tbl(spark, "MAP_DerivedLines", cfg).alias("M")
        .join(
            F.broadcast(_tbl(spark, "ENU_AttributeType", cfg)).alias("EA"),
            F.col("M.AttributeID") == F.col("EA.AttributeID"),
        )
        .filter(
            (F.col("EA.AttributeType") == "FN")
            & (F.col("M.DerivedLineID").isNotNull())
            & (F.col("M.BaseLineID").isNotNull())
            & (F.coalesce(F.col("EA.IsHidden"), F.lit(False)) == False)
        )
        .select(
            F.col("M.BaseLineID").alias("FedLine"),
            F.col("M.DerivedLineID").alias("FootnoteLine"),
        )
        .distinct()
    )

    _log_timing("build_footnote_lines", t0)
    return result


# ---------------------------------------------------------------------------
# build_footnote_book_effective
# SQL lines: 2769-2800
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_footnote_book_effective(
    lt_input: DataFrame, footnote_lines: DataFrame,
    book_effective: DataFrame, cfg: dict
) -> DataFrame:
    """Mark footnote lines to follow fed lines custom allocation.

    Joins TempLookThroughAllocationInput → FootnoteLines → BookEffective
    to create new book effective entries for footnote lines,
    excluding those that already exist in book effective.
    """
    t0 = time.time()
    logger.info("[SECTION] build_footnote_book_effective")

    k1_line_type_id = cfg["k1_line_type_id"]

    # Key matching: ISNULL(TrackingKey,'')='' → '-1', else value
    def _match_key(col):
        return F.when(
            F.coalesce(col, F.lit("")) == "", F.lit("-1")
        ).otherwise(col)

    # Footnote lines that need book effective entries
    fn_bed = (
        lt_input.alias("I")
        .join(
            footnote_lines.alias("M"),
            F.col("I.LineID") == F.col("M.FootnoteLine"),
        )
        .join(
            book_effective.alias("B"),
            (F.col("B.LineID") == F.col("M.FedLine"))
            & (F.col("B.SourceID") == k1_line_type_id)
            & (F.col("B.UnderlyingEntityID") == F.col("I.EntityID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
        )
        # Anti-join: exclude rows that already exist in book_effective for
        # this (FootnoteLine, EntityID, TrackingKey, Tag). SQL uses
        # LEFT JOIN B2 ... WHERE B2.UnderlyingEntityID IS NULL — equivalent
        # to LEFT ANTI JOIN since no B2 columns are projected.
        .join(
            book_effective.alias("B2"),
            (F.col("B2.LineID") == F.col("M.FootnoteLine"))
            & (F.col("B2.SourceID") == k1_line_type_id)
            & (F.col("B2.UnderlyingEntityID") == F.col("I.EntityID"))
            & (_match_key(F.col("B2.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B2.Tag")) == _match_key(F.col("I.Tag"))),
            "left_anti",
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.LineID").alias("LineID"),
            F.col("B.FootNoteID"),
            F.col("B.SourceID"),
            F.col("B.AllocationTypeid"),
            F.col("B.AdjustmentAllocationTypeID"),
            F.col("B.TrackingKey"),
            F.col("B.Tag"),
            F.coalesce(F.col("B.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # Union with existing book effective
    result = book_effective.unionByName(fn_bed)

    _log_timing("build_footnote_book_effective", t0)
    return result
