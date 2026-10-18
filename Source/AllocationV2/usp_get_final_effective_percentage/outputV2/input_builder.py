"""
input_builder.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Allocation input loading — mode-dependent.
Conversion date: 2026-05-04

SQL lines: 604-730
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

logger = logging.getLogger(__name__)


def _sql_round(col, scale):
    """SQL Server ROUND() — round half away from zero."""
    factor = F.pow(F.lit(10), F.lit(scale))
    return F.signum(col) * F.floor(F.abs(col) * factor + F.lit(0.5)) / factor


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _log_timing(name, start):
    logger.info(f"[TIMING] {name}: {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# build_allocation_input
# SQL lines: 607-680 (modes 2, PE+1, 4 → AllocationInput / PE_AllocationInput)
# Row count: POSSIBLY-EMPTY (early return if no rows for modes 2/3)
# ---------------------------------------------------------------------------
def build_allocation_input(
    spark: SparkSession, cfg: dict, modes: list[int]
) -> DataFrame:
    """Load #TempAllocationInput for modes 2 and 4.

    Converted from SQL lines 607-680.
    Modes 2 and 4 load from AllocationInput.
    Returns None if no rows and mode != 4 (early return in SQL).
    """
    t0 = time.time()
    logger.info("[SECTION] build_allocation_input")

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    k1_lt = cfg["k1_line_type_id"]
    adj_lt = cfg["adjustment_line_type_id"]

    # Only modes 2 and 4 load AllocationInput
    needs_alloc_input = any(m in (2, 4) for m in modes)

    if not needs_alloc_input:
        _log_timing("build_allocation_input (skipped)", t0)
        return None

    # SQL lines 614-619: AllocationInput, filter non-zero amounts
    df = (
        _tbl(spark, "AllocationInput", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
            & (~F.col("LineTypeID").isin([k1_lt, adj_lt]))
            & (
                F.when(
                    F.coalesce(F.col("Amount"), F.lit(0.0))
                    == F.coalesce(F.col("Amount704b"), F.lit(0.0)),
                    F.col("Amount"),
                ).otherwise(
                    _sql_round(F.coalesce(F.col("Amount"), F.lit(0.0)), 0)
                )
                != 0
            )
        )
        .select(
            "RunID", "ClientID", "EntityID", "LineTypeID", "LineID",
            "Amount", "QuicklinkID", "Amount704b", "Tag", "TrackingKey",
        )
    )

    _log_timing("build_allocation_input", t0)
    return df


# ---------------------------------------------------------------------------
# build_sm_lookthrough_allocation_input
# SQL lines: 685-730 (mode 3 → SM_LookThroughAllocationInput)
# Row count: POSSIBLY-EMPTY (early return if no rows)
# ---------------------------------------------------------------------------
def build_sm_lookthrough_allocation_input(
    spark: SparkSession, cfg: dict
) -> DataFrame:
    """Load #TempSMLookThroughAllocationInput for mode 3 (state allocation).

    Converted from SQL lines 685-730.
    Returns None if no rows (early return in SQL).
    """
    t0 = time.time()
    logger.info("[SECTION] build_sm_lookthrough_allocation_input")

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]

    # SQL lines 695-700
    df = (
        _tbl(spark, "SM_LookThroughAllocationInput", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
            & (_sql_round(F.coalesce(F.col("Amount"), F.lit(0.0)), 0) != 0)
        )
        .select(
            "RunID", "ClientID", "EntityID", "LineTypeID",
            "StateID", "StateLineID", "Amount", "QuicklinkID",
            "TrackingKey", "Tag",
        )
    )

    _log_timing("build_sm_lookthrough_allocation_input", t0)
    return df


# ---------------------------------------------------------------------------
# build_lookthrough_allocation_input
# SQL lines: 2707-2770 (modes 1,4 → LookThroughAllocationInput)
# Row count: ALWAYS-NON-EMPTY (for modes 1,4)
# ---------------------------------------------------------------------------
def build_lookthrough_allocation_input(
    spark: SparkSession, cfg: dict
) -> DataFrame:
    """Load #TempLookThroughAllocationInput for modes 1 and 4.

    Converted from SQL lines 2707-2770.
    """
    t0 = time.time()
    logger.info("[SECTION] build_lookthrough_allocation_input")

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    k1_lt = cfg["k1_line_type_id"]
    adj_lt = cfg["adjustment_line_type_id"]
    box_jkl_lt = cfg["box_jkl_line_type_id"]

    valid_line_types = [k1_lt, adj_lt, box_jkl_lt]

    df = (
        _tbl(spark, "LookThroughAllocationInput", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
            & (F.col("LineTypeID").isin(valid_line_types))
            & (
                (F.col("LineTypeID") == box_jkl_lt)
                | (
                    F.col("LineTypeID").isin([k1_lt, adj_lt])
                    & (_sql_round(F.coalesce(F.col("Amount"), F.lit(0.0)), 0) != 0)
                )
            )
        )
        .select(
            "RunID", "ClientID", "EntityID", "LineTypeID", "LineID",
            "Amount", "QuicklinkID", "Amount704b", "TrackingKey", "Tag",
        )
    )

    _log_timing("build_lookthrough_allocation_input", t0)
    return df
