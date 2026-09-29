"""
add_lookthrough_allocation_detail_step01.py

Converted from: uspAddLookThroughAllocationDetail_Step_01.sql
Original procedure: dbo.uspAddLookThroughAllocationDetail_Step_01
Conversion date: 2026-05-04

NOTE: UBTI/UBTI-DF logic (SQL lines 521-2251) is intentionally excluded per requirements.

Usage (standalone):
    from add_lookthrough_allocation_detail_step01 import run_add_lookthrough_allocation_detail_step01

    run_add_lookthrough_allocation_detail_step01(
        spark,
        entity_id=123, client_id=456, tax_period_id=789, run_id=1001,
        catalog="dev7", schema="iPC_2025_dev7_15349",
    )
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType,
)
from datetime import datetime
from functools import reduce
import operator
import logging
import time

from Common_V2.core.helpers import (
    get_logger,
    tbl as _tbl,
    tbl_name as _tbl_name,
    table_prefix as _table_prefix,
    ns as _ns,
    ns0 as _ns0,
)
from Common_V2.core.observability import log_section, log_timing
from Common_V2.core.config import load_common_config
from Common_V2.core.writers import write_output, collect_parquet_result
from Common_V2.core.generic_result_storer import GenericResultStorer
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.execution_profiles import resolve_execution_profile

from .parallel_helpers import (
    normalize_workers,
    parse_enabled_groups,
    run_parallel,
)

logger = get_logger("add_lookthrough_allocation_detail_step01")


def _blank(value):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _as_int(value):
    if _blank(value) is None:
        return None
    return int(value)


def _checkpoint_frame(spark, df, name, cfg):
    if df is None:
        return None
    if not hasattr(df, "columns"):
        return df
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = checkpoint(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    return result


def _apply_execution_profile(
    spark,
    cfg,
    profile_name,
    profile,
    workers,
    checkpoint_mode,
    CheckpointMode,
    SqlShufflePartitions,
):
    shuffle_override = _as_int(SqlShufflePartitions)
    if shuffle_override is None:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            str(profile["shuffle_partitions"]),
        )
    else:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            str(shuffle_override),
        )
    explicit_checkpoint = (
        CheckpointMode if CheckpointMode is not None else checkpoint_mode
    )
    mode = resolve_checkpoint_mode(
        cfg,
        checkpoint_mode=(
            explicit_checkpoint
            if _blank(explicit_checkpoint) is not None
            else profile["checkpoint_mode"]
        ),
        CheckpointMode=CheckpointMode,
    )
    cfg.update(
        {
            "checkpoint_mode": mode,
            "max_threads": workers,
            "execution_profile": profile_name,
        }
    )
    initialize_checkpoint_V2(cfg, mode)
    print(
        f"[lt-detail-step01] ExecutionProfile={profile_name} "
        f"CheckpointMode={mode} shuffle="
        f"{shuffle_override or profile['shuffle_partitions']} "
        f"MaxThreads={workers}"
    )
    return mode


def _isolated_cfg(cfg):
    local = {**cfg}
    local["_parquet_results"] = {}
    return local


def _merge_parquet_results(target, branch):
    for key, df in (branch or {}).items():
        if key in target:
            target[key] = target[key].unionByName(df)
        else:
            target[key] = df


def _log_timing(section_name, start):
    log_timing(logger, section_name, start)


def _set_query_tag(spark, tag):
    if tag:
        log_section(logger, tag)


def _write_parquet_if_enabled(cfg: dict, df, table_name: str) -> None:
    """Collect DataFrame for batch write via GenericResultStorer at end of SP.
    If the same table_name is written more than once (e.g. two INSERTs into the
    same physical table), the DataFrames are unioned automatically."""
    if "_parquet_results" not in cfg:
        cfg["_parquet_results"] = {}
    if table_name in cfg["_parquet_results"]:
        cfg["_parquet_results"][table_name] = cfg["_parquet_results"][table_name].unionByName(df)
    else:
        cfg["_parquet_results"][table_name] = df


# ---------------------------------------------------------------------------
# Tracking Key helper
# ---------------------------------------------------------------------------

def _build_tracking_key(entity_id: int):
    """
    TrackingKey is always on — CASE WHEN TrackingKey IS NULL
        THEN CONVERT(VARCHAR(4000), EntityID)
        ELSE TrackingKey + '~' + CONVERT(VARCHAR(4000), @LocalEntityID)
    END
    """
    return F.when(
        F.col("TrackingKey").isNull(),
        F.col("EntityID").cast("string")
    ).otherwise(
        F.concat(F.col("TrackingKey"), F.lit("~"), F.lit(entity_id).cast("string"))
    )


# ---------------------------------------------------------------------------
# Section 1: Config & Lookups
# SQL lines: 250-520
# ---------------------------------------------------------------------------

def _load_config(spark: SparkSession, cfg: dict) -> dict:
    """Alias common cfg scalars to the legacy SP-internal names used downstream.

    All scalar config (AllocationRun, line types, event types, entity allocation
    type name, entity type IDs) comes from load_common_config.
    """
    _set_query_tag(spark, "load_config")
    t0 = time.time()
    logger.info(f"[START] load_config | RunID={cfg['run_id']}")

    if cfg.get("run_status") == "FAIL":
        _log_timing("load_config", t0)
        return cfg

    # Legacy SP-internal aliases. run_status/run_type/phase_id/k1_line_type_id/
    # m1_line_type_id are already on cfg from load_common_config under the same
    # names downstream code reads — no aliasing needed.
    cfg["box_jkl_line_type_id"] = cfg.get("boxjkl_line_type_id", -1) or -1
    cfg["book_k1_adj_line_type_id"] = cfg.get("book_k1_adjustments_line_type_id", -1) or -1
    cfg["k1_event_type_id"] = cfg.get("event_type_id_k1_input", -1) or -1
    cfg["allocation_type_name"] = cfg.get("entity_allocation_type_name")
    cfg["inv_entity_type_id"] = cfg.get("entity_type_id_investment", -1) or -1

    _log_timing("load_config", t0)
    return cfg


# ---------------------------------------------------------------------------
# Section 2: M1 SidePocket
# SQL lines: 2252-2269
# ---------------------------------------------------------------------------

def _write_m1_sidepocket(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_m1_sidepocket")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    m1_lt_id = cfg["m1_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == m1_lt_id)
            & (F.lower(_ns(F.col("AllocationType"))) == "sidepocket")
        )
    )

    write_df = df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("PeriodID"),
        _ns(F.col("LineCode")).alias("WorkPaperCode"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
    )

    _write_parquet_if_enabled(cfg, write_df, "M1AdjLookThroughSidePocketAllocationDetail")
    logger.info("M1 SidePocket: written.")
    _log_timing("write_m1_sidepocket", t0)


# ---------------------------------------------------------------------------
# Section 3: Book
# SQL lines: 2271-2303
# ---------------------------------------------------------------------------

def _write_book(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_book")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == k1_lt_id)
            & (F.lower(_ns(F.col("AllocationType"))) == "book")
        )
    )

    agg_df = (
        df.groupBy(
            "ParentEntityID", "EntityID",
            _ns(F.col("ShareClass")).alias("ShareClass"),
            "PartnerNumber", "LineID", "PeriodID",
            "TrackingKey",
            _ns0(F.col("SuperParentEntityID")).alias("SuperParentEntityID"),
        )
        .agg(F.sum(_ns0(F.col("Amount"))).alias("Amount"))
    )

    write_df = agg_df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("PeriodID"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
        F.col("SuperParentEntityID"),
    )

    _write_parquet_if_enabled(cfg, write_df, "K1LookThroughBookAllocationDetail")
    logger.info("Book: written.")
    _log_timing("write_book", t0)


# ---------------------------------------------------------------------------
# Section 4: BookK1Adjustment
# SQL lines: 2305-2329
# ---------------------------------------------------------------------------

def _write_book_k1_adjustment(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_book_k1_adjustment")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == k1_lt_id)
            & (F.lower(_ns(F.col("AllocationType"))) == "bookk1adjustment")
        )
    )

    agg_df = (
        df.groupBy(
            "ParentEntityID", "EntityID",
            _ns(F.col("ShareClass")).alias("ShareClass"),
            "PartnerNumber", "LineID", "PeriodID",
            "TrackingKey",
            _ns0(F.col("SuperParentEntityID")).alias("SuperParentEntityID"),
        )
        .agg(F.sum(_ns0(F.col("Amount"))).alias("Amount"))
    )

    write_df = agg_df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("PeriodID"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
        F.col("SuperParentEntityID"),
    )

    _write_parquet_if_enabled(cfg, write_df, "K1LookThroughBookK1AdjustmentAllocationDetail")
    logger.info("BookK1Adjustment: written.")
    _log_timing("write_book_k1_adjustment", t0)


# ---------------------------------------------------------------------------
# Section 5: Offset
# SQL lines: 2331-2353
# ---------------------------------------------------------------------------

def _write_offset(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_offset")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == k1_lt_id)
            & (F.lower(_ns(F.col("AllocationType"))) == "offset")
        )
    )

    agg_df = (
        df.groupBy(
            "ParentEntityID", "EntityID",
            _ns(F.col("ShareClass")).alias("ShareClass"),
            "PartnerNumber", "LineID", "PeriodID",
            "TrackingKey",
            _ns0(F.col("SuperParentEntityID")).alias("SuperParentEntityID"),
        )
        .agg(F.sum(_ns0(F.col("Amount"))).alias("Amount"))
    )

    write_df = agg_df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("PeriodID"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
        F.col("SuperParentEntityID"),
    )

    _write_parquet_if_enabled(cfg, write_df, "K1LookThroughOffsetAllocationDetail")
    logger.info("Offset: written.")
    _log_timing("write_offset", t0)


# ---------------------------------------------------------------------------
# Section 6: DatedTransfer
# SQL lines: 2355-2383
# ---------------------------------------------------------------------------

def _write_dated_transfer(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_dated_transfer")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == k1_lt_id)
            & (F.lower(_ns(F.col("AllocationType"))).like("%datedtransfer"))
        )
    )

    agg_df = (
        df.groupBy(
            "ParentEntityID", "EntityID",
            _ns(F.col("ShareClass")).alias("ShareClass"),
            "PartnerNumber", "LineID", "PeriodID",
            "TrackingKey",
            _ns0(F.col("SuperParentEntityID")).alias("SuperParentEntityID"),
            "AllocationType", "Tag",
        )
        .agg(F.sum(_ns0(F.col("Amount"))).alias("Amount"))
    )

    write_df = agg_df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("PeriodID"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
        F.col("SuperParentEntityID"),
        F.col("AllocationType"),
        F.col("Tag"),
    )

    _write_parquet_if_enabled(cfg, write_df, "K1LookThroughDatedTransferAllocationDetail")
    logger.info("DatedTransfer: written.")
    _log_timing("write_dated_transfer", t0)


# ---------------------------------------------------------------------------
# Section 7: DatedTransfer Without Transfer Adjusted %
# SQL lines: 2385-2413
# ---------------------------------------------------------------------------

def _write_dated_transfer_without_adj(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_dated_transfer_without_adj")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    # SQL: AllocationType like '%without Transfer Adj [%]'
    # The [%] in SQL LIKE is a character class matching a literal % character.
    # Spark LIKE has no [%] syntax, so we use LIKE + endsWith("%") to enforce
    # the string ends with a literal percent sign.
    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == k1_lt_id)
            & (F.lower(F.col("AllocationType")).like("%without transfer adj %"))
            & (F.col("AllocationType").endswith("%"))
        )
    )

    agg_df = (
        df.groupBy(
            "ParentEntityID", "EntityID",
            _ns(F.col("ShareClass")).alias("ShareClass"),
            "PartnerNumber", "LineID", "PeriodID",
            "TrackingKey",
            _ns0(F.col("SuperParentEntityID")).alias("SuperParentEntityID"),
            "AllocationType", "Tag",
        )
        .agg(F.sum(_ns0(F.col("Amount"))).alias("Amount"))
    )

    write_df = agg_df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("PeriodID"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
        F.col("SuperParentEntityID"),
        F.col("AllocationType"),
        F.col("Tag"),
    )

    _write_parquet_if_enabled(cfg, write_df, "K1LookThroughDatedTransferAllocationDetail")
    logger.info("DatedTransfer (without Adj %): written.")
    _log_timing("write_dated_transfer_without_adj", t0)


# ---------------------------------------------------------------------------
# Section 8: Special Allocation
# SQL lines: 2415-2439
# ---------------------------------------------------------------------------

def _write_special_allocation(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_special_allocation")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    special_types = ["special allocation", "special allocation with incentive",
                     "special allocation - partial netting"]

    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == k1_lt_id)
            & (F.lower(_ns(F.col("AllocationType"))).isin(special_types))
        )
    )

    agg_df = (
        df.groupBy(
            "EntityID",
            _ns(F.col("ShareClass")).alias("ShareClass"),
            "PartnerNumber", "LineID", "TrackingKey", "ParentEntityID",
        )
        .agg(F.sum(_ns0(F.col("Amount"))).alias("Amount"))
    )

    write_df = agg_df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
    )

    _write_parquet_if_enabled(cfg, write_df, "K1LOOKTHROUGHSPECIALALLOCATIONDETAIL")
    logger.info("Special Allocation: written.")
    _log_timing("write_special_allocation", t0)


# ---------------------------------------------------------------------------
# Section 9: M1 Residual (non-SidePocket)
# SQL lines: 2441-2471
# ---------------------------------------------------------------------------

def _write_m1_residual(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_m1_residual")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    m1_lt_id = cfg["m1_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == m1_lt_id)
            & (F.lower(_ns(F.col("AllocationType"))) != "sidepocket")
        )
    )

    agg_df = (
        df.groupBy(
            "ParentEntityID", "EntityID",
            _ns(F.col("ShareClass")).alias("ShareClass"),
            "PartnerNumber", "LineID", "PeriodID",
            _ns(F.col("LineCode")).alias("WorkPaperCode"),
            "TrackingKey",
        )
        .agg(F.sum(_ns0(F.col("Amount"))).alias("Amount"))
    )

    write_df = agg_df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("PeriodID"),
        F.col("WorkPaperCode"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
    )

    _write_parquet_if_enabled(cfg, write_df, "M1AdjLookThroughSidePocketResidualAllocationDetail")
    logger.info("M1 Residual: written.")
    _log_timing("write_m1_residual", t0)


# ---------------------------------------------------------------------------
# Section 10: BoxJKL
# SQL lines: 2473-2501
# ---------------------------------------------------------------------------

def _write_box_jkl(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_box_jkl")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    box_jkl_lt_id = cfg["box_jkl_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == box_jkl_lt_id)
        )
    )

    # No GROUP BY in original SQL — straight insert
    write_df = df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
        F.coalesce(F.col("AllocationType"), F.lit("ProRata")).alias("AllocationType"),
        F.col("OriginalParentEntityID"),
    )

    _write_parquet_if_enabled(cfg, write_df, "BoxJKLLookThroughAllocationDetail")
    logger.info("BoxJKL: written.")
    _log_timing("write_box_jkl", t0)


# ---------------------------------------------------------------------------
# Section 11: K1 Complete
# SQL lines: 2503-2535
# ---------------------------------------------------------------------------

def _write_k1_complete(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_k1_complete")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    lt_out = cfg["_base_lt_out"]

    df = (
        lt_out
        .filter(
            (F.col("LineTypeID") == k1_lt_id)
        )
    )

    agg_df = (
        df.groupBy(
            "ParentEntityID", "EntityID",
            _ns(F.col("ShareClass")).alias("ShareClass"),
            "PartnerNumber", "LineID", "PeriodID",
            "TrackingKey",
            _ns0(F.col("SuperParentEntityID")).alias("SuperParentEntityID"),
            "AllocationType",
            "Tag",
            "OriginalParentEntityID",
        )
        .agg(F.sum(_ns0(F.col("Amount"))).alias("Amount"))
    )

    # HAVING SUM(ISNULL(Amount,0)) <> 0
    agg_df = agg_df.filter(F.col("Amount") != 0)

    write_df = agg_df.select(
        F.lit(run_id).cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("ParentEntityID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.col("Amount").alias("FlowupAmount"),
        F.col("PeriodID"),
        _build_tracking_key(entity_id).alias("TrackingKey"),
        F.col("SuperParentEntityID"),
        F.coalesce(F.col("AllocationType"), F.lit("ProRata")).alias("AllocationType"),
        F.col("Tag"),
        F.col("OriginalParentEntityID"),
    )

    _write_parquet_if_enabled(cfg, write_df, "K1LookThroughCompleteAllocationDetail")
    logger.info("K1 Complete: written.")
    _log_timing("write_k1_complete", t0)


# ---------------------------------------------------------------------------
# Section 12: CY Adjustment
# SQL lines: 2537-2695
# ---------------------------------------------------------------------------

def _write_cy_adjustment(spark: SparkSession, cfg: dict) -> None:
    """
    Conditional on GlobalMenu configs:
    - 'Reports' / 'Underlying Investment Basis Report'
    - 'Schedule K Equivalent Configurations' / 'Show Tax Capital'
    """
    _set_query_tag(spark, "write_cy_adjustment")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    inv_entity_type_id = cfg["inv_entity_type_id"]

    prefix = _table_prefix(cfg)

    # --- Check if configured (uspIsGlobalMenuConfigured equivalent) ---
    # Two pre-resolved GlobalMenu flag scalars from load_common_config:
    #   Reports / "Underlying Investment Basis Report"
    #   Schedule K Equivalent Configurations / "Show Tax Capital"
    # CY Adjustment runs when either flag is in ('C', 'CG').
    uibr_state = (cfg.get("flag_underlying_investment_basis_report") or "").strip().upper()
    stc_state = (cfg.get("flag_show_tax_capital") or "").strip().upper()
    is_configured = uibr_state in ("C", "CG") or stc_state in ("C", "CG")

    if not is_configured:
        logger.info("CY Adjustment: not configured — skipping.")
        _log_timing("write_cy_adjustment", t0)
        return

    # --- IsDomesticBlocker, IsPFIC from pre-resolved Entity scalars ---
    is_domestic_blocker = int(bool(cfg.get("entity_is_domestic_blocker")))
    is_pfic = int(bool(cfg.get("entity_is_pfic")))

    # --- Load K1Workflows ---
    k1_workflows = (
        _tbl(spark, "AllocationInputWorkflow", cfg)
        .filter(F.col("RunID") == run_id)
        .select("EntityID", "K1WorkflowID")
    )

    # --- Load AllocationPercentage (Prorata <> 0) ---
    alloc_pct = (
        _tbl(spark, "AllocationPercentage", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("EntityID") == entity_id)
            & (F.col("ClientID") == client_id)
            & (_ns0(F.col("Prorata")) != 0)
        )
        .select("PartnerNumber", "Prorata", "ShareClass")
    )

    # --- Direct investments: CYAdjustmentInput_Snapshot × AllocationPercentage ---
    cy_input = _tbl(spark, "CYAdjustmentInput_Snapshot", cfg)

    direct_df = (
        k1_workflows.alias("AI")
        .join(
            cy_input.alias("CYS"),
            (F.col("AI.K1WorkflowID") == F.col("CYS.WorkflowID"))
            & (F.col("CYS.Amount") != 0),
        )
        .crossJoin(alloc_pct.alias("AP"))
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.col("AI.EntityID").alias("EntityID"),
            F.col("AP.ShareClass").alias("ShareClass"),
            F.col("AP.PartnerNumber").alias("PartnerNumber"),
            F.col("CYS.LineID").alias("LineID"),
            (_ns0(F.col("CYS.Amount")) * _ns0(F.col("AP.Prorata"))).alias("Amount"),
            _ns0(F.col("AP.Prorata")).alias("ProrataPercentage"),
            F.concat(
                F.col("AI.EntityID").cast("string"),
                F.lit("~"),
                F.lit(entity_id).cast("string"),
            ).alias("TrackingKey"),
        )
        .distinct()  # SQL: SELECT DISTINCT
    )

    # --- Indirect investments: ReclassBoxJKLLookThroughAllocationData ---
    reclass = (
        _tbl(spark, "ReclassBoxJKLLookThroughAllocationData", cfg).alias("CY")
        .join(
            _tbl(spark, "Entity", cfg).alias("E"),
            F.col("CY.EntityID") == F.col("E.EntityID"),
        )
        .filter(
            (F.col("CY.RunID") == run_id)
            & (F.lower(F.col("CY.BoxJKLBox")) == "l")
            & (F.col("E.FundOrInvestmentID") == inv_entity_type_id)
            & (F.col("CY.Amount") != 0)
        )
        .select(
            F.col("CY.EntityID"),
            F.col("CY.LineID"),
            F.col("CY.Amount"),
            F.col("CY.TrackingKey"),
        )
    )

    indirect_df = (
        reclass.alias("CY")
        .crossJoin(alloc_pct.alias("AP"))
        .groupBy(
            F.col("CY.EntityID"),
            F.col("AP.ShareClass"),
            F.col("AP.PartnerNumber"),
            F.col("CY.LineID"),
            F.when(
                F.col("CY.TrackingKey").isNull(),
                F.col("CY.EntityID").cast("string")
            ).otherwise(
                F.concat(F.col("CY.TrackingKey"), F.lit("~"), F.lit(entity_id).cast("string"))
            ).alias("TrackingKey"),
        )
        .agg(
            F.sum(_ns0(F.col("CY.Amount")) * _ns0(F.col("AP.Prorata"))).alias("Amount"),
        )
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.col("EntityID"),
            F.col("ShareClass"),
            F.col("PartnerNumber"),
            F.col("LineID"),
            F.col("Amount"),
            F.lit(0.0).alias("ProrataPercentage"),
            F.col("TrackingKey"),
        )
    )

    # --- Combine direct + indirect ---
    cy_combined = direct_df.unionByName(indirect_df)

    # --- If DomesticBlocker or PFIC: keep only local entity ---
    if is_domestic_blocker == 1 or is_pfic == 1:
        cy_combined = cy_combined.filter(F.col("EntityID") == entity_id)

    # --- Write to CYAdjustmentLookThroughAllocationDetail ---
    write_df = cy_combined.select(
        F.col("RunID"),
        F.col("ClientID"),
        F.col("TaxPeriodID"),
        F.col("EntityID"),
        F.col("ShareClass"),
        F.col("PartnerNumber"),
        F.col("LineID"),
        F.col("Amount"),
        F.lit(0.0).alias("ProrataPercentage"),
        F.col("TrackingKey"),
    )

    _write_parquet_if_enabled(cfg, write_df, "CYAdjustmentLookThroughAllocationDetail")
    logger.info("CY Adjustment: written.")
    _log_timing("write_cy_adjustment", t0)


# ---------------------------------------------------------------------------
# Section 13: K1 Text Allocation Detail
# ---------------------------------------------------------------------------

def _write_k1_text_allocation_detail(spark: SparkSession, cfg: dict) -> None:
    _set_query_tag(spark, "write_k1_text_allocation_detail")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    # @K1WorkflowID — already resolved on cfg by load_common_config
    # (AllocationRun.K1WorkflowID); reuse it instead of re-reading.
    k1_workflow_id = cfg["k1_workflow_id"]

    # #TmpK1InputSnapshot: TEXT-type K1 lines for the workflow
    tmp_k1_input = (
        _tbl(spark, "K1Input_Snapshot", cfg).alias("K1")
        .join(
            _tbl(spark, "K1LineItem", cfg).alias("KL"),
            (F.col("K1.LineID") == F.col("KL.LineID"))
            & (F.lower(F.col("KL.LineDataType")).like("%text%")),
        )
        .filter(
            (F.col("K1.WorkflowID") == k1_workflow_id)
            & (F.col("K1.ClientID") == client_id)
            & (F.col("K1.TaxPeriodID") == tax_period_id)
        )
        .select(
            F.col("K1.LineID").alias("lineId"),
            F.col("K1.TextValue"),
        )
    )

    # #TmpAllocOutputPartner: distinct partners from LookThroughAllocationOutput
    lt_out = cfg["_base_lt_out"]
    tmp_alloc_partner = (
        lt_out.alias("AO")
        .filter(F.col("LineTypeID") == k1_lt_id)
        .groupBy(
            F.col("AO.EntityID"),
            F.col("AO.PartnerNumber"),
            F.col("AO.ParentEntityID"),
        )
        .agg(F.max(_ns(F.col("AO.ShareClass"))).alias("ShareClass"))
        .select(
            F.col("AO.EntityID"),
            F.col("ShareClass"),
            F.col("AO.PartnerNumber"),
            F.col("AO.ParentEntityID"),
        )
    )

    # Fallback when no partners found — SQL:
    #   SELECT @LocalEntityID, ShareClass, PartnerNumber, 0
    #   FROM dbo.udf_PE_GetPartnersList(@LocalClientID,@LocalTaxPeriodID,@LocalEntityID,@PhaseID)
    # Partners load from Partner_Snapshot filtered by the partner workflow,
    # same as usp_load_lookthrough_cost_alloc_to_output._data_loading.load_partners.
    if tmp_alloc_partner.isEmpty():
        tmp_alloc_partner = (
            _tbl(spark, "Partner_Snapshot", cfg)
            .filter(
                (F.col("WorkflowID") == cfg["partner_workflow_id"])
                & (F.col("ClientID") == client_id)
                & (F.col("TaxPeriodID") == tax_period_id)
                & (F.col("EntityID") == entity_id)
            )
            .select(
                F.lit(entity_id).cast("long").alias("EntityID"),
                F.col("ShareClass"),
                F.col("PartnerNumber"),
                F.lit(0).cast("long").alias("ParentEntityID"),
            )
        )

    # INSERT INTO K1AllocationDetail: cross join, GROUP BY acts as DISTINCT
    write_df = (
        tmp_k1_input.alias("K1")
        .crossJoin(tmp_alloc_partner.alias("AP"))
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.col("AP.EntityID").cast("int").alias("EntityID"),
            F.col("AP.ShareClass"),
            F.col("AP.PartnerNumber"),
            F.col("K1.lineId").alias("LineID"),
            F.lit(None).cast("int").alias("PeriodID"),
            F.coalesce(F.col("AP.ParentEntityID"), F.lit(0)).cast("int").alias("ParentEntityID"),
            F.col("K1.TextValue"),
        )
        .distinct()
    )

    _write_parquet_if_enabled(cfg, write_df, "K1AllocationDetail")
    logger.info("K1 Text Allocation Detail: written.")
    _log_timing("write_k1_text_allocation_detail", t0)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_add_lookthrough_allocation_detail_step01(
    spark: SparkSession,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = "dev7",
    SchemaName: str = "iPC_2025_dev7_15349",
    CallFrom: str = None,
    cfg: dict = None,
    verbose: bool = False,
    ResultType: str = "Parquet",
    VolumePath: str = None,
    ExecutionID: str = None,
    max_threads: int = None,
    MaxThreads: int = None,
    parallel_groups: str = "all",
    ParallelGroups: str = None,
    execution_profile: str = "low",
    ExecutionProfile: str = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    SqlShufflePartitions=None,
    **kwargs,
):
    """Loads lookthrough allocated amounts into detail tables."""
    del kwargs
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName
    call_from = CallFrom
    result_type = ResultType
    volume_path = VolumePath
    execution_id = ExecutionID
    t0 = time.time()
    parallel_activity = []
    enabled_groups = parse_enabled_groups(parallel_groups, ParallelGroups)
    profile_name = (
        _blank(ExecutionProfile) or _blank(execution_profile) or "low"
    )
    profile = resolve_execution_profile(profile_name)
    workers = normalize_workers(
        max_threads=(
            MaxThreads
            if MaxThreads is not None
            else max_threads
            if max_threads is not None
            else profile["max_threads"]
        ),
        MaxThreads=MaxThreads,
    )
    mode = None
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    status = {
        "sp_name": "uspAddLookThroughAllocationDetail_Step_01",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
        "skip_reason": None,
    }
    save_return_value = ""

    try:
        if cfg is None:
            cfg = load_common_config(
                spark,
                run_id=run_id,
                entity_id=entity_id,
                client_id=client_id,
                tax_period_id=tax_period_id,
                catalog=catalog,
                schema=schema,
                call_from=call_from,
            )
        elif call_from is not None:
            cfg["call_from"] = call_from
        cfg = {**cfg, "_checkpoint_tables": []}
        if result_type is not None:
            cfg.setdefault("result_type", result_type)
        if volume_path is not None:
            cfg["volume_path"] = volume_path
        if execution_id is not None:
            cfg["execution_id"] = execution_id
        cfg.setdefault("_parquet_results", {})
        cfg.setdefault("_checkpoint_paths", [])
        cfg.setdefault("_checkpoint_v2_activity", [])
        mode = _apply_execution_profile(
            spark,
            cfg,
            profile_name,
            profile,
            workers,
            checkpoint_mode,
            CheckpointMode,
            SqlShufflePartitions,
        )
        status["run_id"] = cfg.get("run_id")
        status["entity_id"] = cfg.get("entity_id")
        _load_config(spark, cfg)
        if cfg.get("run_status") == "FAIL":
            logger.error(
                "RunStatus=FAIL — aborting. RunID=%s EntityID=%s",
                cfg["run_id"],
                cfg["entity_id"],
            )
            status["status"] = "FAIL"
            status["error"] = "RunStatus=FAIL at entry"
            status["skip_reason"] = "run_status_fail"
            return status
        status["sections_completed"] = 1

        base_lt_out = (
            _tbl(spark, "LookThroughAllocationOutput", cfg)
            .filter(
                (F.col("RunID") == cfg["run_id"])
                & (F.col("ClientID") == cfg["client_id"])
            )
        )
        cfg["_base_lt_out"] = _checkpoint_frame(
            spark, base_lt_out, "base_lt_out", cfg
        )

        def _wrap(name, fn):
            def task():
                local = _isolated_cfg(cfg)
                fn(spark, local)
                return dict(local.get("_parquet_results") or {})

            return name, task

        def _dated_pair(spark_session, local):
            _write_dated_transfer(spark_session, local)
            _write_dated_transfer_without_adj(spark_session, local)

        write_tasks = [
            _wrap("m1_sidepocket", _write_m1_sidepocket),
            _wrap("book", _write_book),
            _wrap("book_k1_adjustment", _write_book_k1_adjustment),
            _wrap("offset", _write_offset),
            _wrap("dated_transfer_pair", _dated_pair),
            _wrap("special_allocation", _write_special_allocation),
            _wrap("m1_residual", _write_m1_residual),
            _wrap("box_jkl", _write_box_jkl),
            _wrap("k1_complete", _write_k1_complete),
            _wrap("cy_adjustment", _write_cy_adjustment),
            _wrap("k1_text", _write_k1_text_allocation_detail),
        ]
        branches = run_parallel(
            write_tasks,
            workers,
            parallel_activity,
            "output_writes",
            enabled_groups,
        )
        merged = {}
        for branch in branches:
            _merge_parquet_results(merged, branch)
        cfg["_parquet_results"] = merged
        status["sections_completed"] = 13

        parquet_results = {
            key: value
            for key, value in cfg.get("_parquet_results", {}).items()
            if value.limit(1).first() is not None
        }
        if parquet_results:
            result_storer = GenericResultStorer(spark, None)
            save_return_value = result_storer.save_results(
                result=parquet_results,
                result_type=cfg.get("result_type", "deltalake"),
                catalog_name=cfg["catalog"],
                database_name=cfg["schema"],
                run_id=cfg["run_id"],
                client_id=cfg["client_id"],
                entity_id=cfg["entity_id"],
                execution_id=cfg.get("execution_id", "1"),
                volume_path=cfg.get("volume_path", ""),
                sql_url_path=None,
                sql_username=None,
                sql_password=None,
            )
        status["elapsed_seconds"] = round(time.time() - t0, 1)
    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error("[FAIL] %s", e, exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        if isinstance(cfg, dict):
            drop_checkpoints_V2(spark, cfg)
            _drop_checkpoints(spark, cfg)

    logger.info(
        "[DONE] add_lookthrough_allocation_detail_step01 | %ss | RunID=%s EntityID=%s",
        status["elapsed_seconds"],
        cfg["run_id"],
        cfg["entity_id"],
    )
    return save_return_value if save_return_value else status

# ---------------------------------------------------------------------------
# Checkpoint cleanup (even though no checkpoints are used here,
# keep the pattern for consistency)
# ---------------------------------------------------------------------------

def _drop_checkpoints(spark, cfg):
    for fqn in cfg.get("_checkpoint_tables", []):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:
            pass
    cfg["_checkpoint_tables"] = []


# ════════════════════════════════════════════════════════════════
# __main__: standalone execution (Mode 3).
# Read widget params → call run_add_lookthrough_allocation_detail_step01(...).
# The function's `if cfg is None` branch is the single point that calls
# load_common_config. Job/Orchestrator modes pass cfg in directly and skip
# this block.
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import json
    spark = SparkSession.builder.getOrCreate()

    try:
        result = run_add_lookthrough_allocation_detail_step01(
            spark,
            RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
            EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
            ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
            TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
            CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
            SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
        )
    except Exception as exc:
        print(f"Usage: provide run_id, entity_id, etc. as widget parameters ({exc})")
        import sys
        sys.exit(1)

    try:
        dbutils.notebook.exit(json.dumps(result) if not isinstance(result, str) else result)  # noqa: F821
    except Exception:
        print(json.dumps(result, indent=2) if not isinstance(result, str) else result)
