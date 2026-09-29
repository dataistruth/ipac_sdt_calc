"""Effective percentage calculation for SM LookThrough Effective Allocation Percentage.

Functions:
    compute_effective_percentages  — SQL lines 1790-1810: Core effective % calculation
    apply_exclude_from_residual    — SQL lines 1837-2040: Recalc for excluded partners
    apply_pe_book_unmapped_lines   — SQL lines 2046-2090: Prorata for unmapped lines
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from Common_V2.core.helpers import read_table, ns, ns0, sql_round, table_prefix
from Common_V2.core.checkpoint_V2 import checkpoint_V2 as checkpoint
from Common_V2.core.observability import log_section, log_timing
from Common_V2.core.assertions import warn_if_empty
import logging
import time

logger = logging.getLogger(__name__)


def compute_effective_percentages(
    spark: SparkSession, cfg: dict,
    total_amounts: DataFrame, sm_lt_input: DataFrame,
) -> tuple:
    """Compute effective amounts = (AllocAmount/InputAmount) * StateAmount.

    Converted from: SQL lines 1790-1810.
    Row count: ALWAYS-NON-EMPTY — core pipeline result.

    Returns:
        (effective_amounts, temp_effective_amounts) — both DataFrames.
        temp_effective_amounts is a copy used for self-join avoidance.
    """
    log_section("compute_effective_percentages")
    t0 = time.time()

    entity_id = cfg["entity_id"]

    # PERF L2: Pre-compute the TrackingKey concat on sm_lt_input for equi-join
    sm_lt_keyed = sm_lt_input.withColumn(
        "_join_tk", F.concat(F.col("TrackingKey"), F.lit("~"), F.lit(str(entity_id)))
    )

    # ── S10-line 1790: #EffectiveAmounts ──
    # CASE WHEN InputAmount <> 0 THEN (AllocAmount/InputAmount) * StateAmount END
    # WHERE AllocAmount <> 0 AND InputAmount <> 0
    # PERF L5: broadcast sm_lt_keyed (aggregated, no PartnerNumber → much smaller than total_amounts)
    effective_amounts = (
        total_amounts.alias("A")
        .join(
            F.broadcast(sm_lt_keyed).alias("S"),
            (F.col("A.StateID") == F.col("S.StateID"))
            & (F.col("A.StateFieldID") == F.col("S.StateLineID"))
            & (F.col("A.LineTypeID") == F.col("S.LineTypeID"))
            & (F.col("A.EntityID") == F.col("S.EntityID"))
            & (F.col("A.TrackingKey") == F.col("S._join_tk")),
        )
        .filter((F.col("A.AllocAmount") != 0) & (F.col("A.InputAmount") != 0))
        .select(
            F.col("S.EntityID"),
            F.col("A.StateID"),
            F.col("S.LineTypeID"),
            F.col("A.StateFieldID"),
            F.col("A.PartnerNumber"),
            F.when(
                F.col("A.InputAmount") != 0,
                (F.col("A.AllocAmount") / F.col("A.InputAmount")) * F.col("S.StateAmount"),
            ).alias("EffectiveAmount"),
            F.col("S.ParentEntityID"),
            F.col("S.SuperParentEntityID"),
            F.col("S.TrackingKey"),
            F.col("A.AllocType"),
            F.col("S.OriginalParentEntityID"),
        )
    )

    # localCheckpoint = SQL #TempTable equivalent. Eager so data is materialized
    # BEFORE the 3 downstream consumers (exclude_from_residual, pe_book_unmapped,
    # write_output) use it. Without eager, lazy checkpoint causes Spark to
    # evaluate the full lineage tree on first action, potentially recomputing.
    effective_amounts = effective_amounts.localCheckpoint(eager=True)

    # ── S10-line 1812: #TempEffectiveAmounts (copy for self-join avoidance) ──
    temp_effective = effective_amounts.select(
        "EntityID", "StateID", "LineTypeID", "StateFieldID",
        "PartnerNumber", "EffectiveAmount", "ParentEntityID",
        "SuperParentEntityID", "TrackingKey", "AllocType",
    )

    log_timing("compute_effective_percentages", t0)
    return effective_amounts, temp_effective


def apply_exclude_from_residual(
    spark: SparkSession, cfg: dict,
    effective_amounts: DataFrame, total_amounts: DataFrame,
) -> DataFrame:
    """Recalculate effective amounts for partners excluded from residual allocation.

    Converted from: SQL lines 1837-2040.
    Row count: POSSIBLY-EMPTY — only when excluded partners exist.

    NOTE: In this SP, #ExcludeFromResidualProrataPercentage is CREATEd but never
    INSERTed into, so #EffectiveAmountExcludeFromResidual is always empty and
    the IF EXISTS block never executes. The logic is preserved for completeness.

    Returns:
        Updated effective_amounts (unchanged in practice).
    """
    log_section("apply_exclude_from_residual")
    t0 = time.time()

    # #ExcludeFromResidualProrataPercentage is never populated in this SP.
    # The following logic would execute if it were:
    #
    # 1. JOIN TotalAmounts with ExcludeFromResidual to get AllocatedAmount per partner
    # 2. SUM excluded partner amounts (IsExcluded=1)
    # 3. SUM total effective amounts from #EffectiveAmounts
    # 4. Recalculate: if excluded → 0; else → TEA.EffectiveAmount * (AllocAmount / (Total - Excluded))
    # 5. UPDATE #EffectiveAmounts with recalculated values via ROUND(...,0)
    #
    # Since no data ever reaches #ExcludeFromResidualProrataPercentage in this SP,
    # we skip execution but log for visibility.

    logger.info(
        "[SKIP] apply_exclude_from_residual: "
        "ExcludeFromResidualProrataPercentage is never populated in this SP"
    )

    log_timing("apply_exclude_from_residual", t0)
    return effective_amounts


def apply_pe_book_unmapped_lines(
    spark: SparkSession, cfg: dict,
    effective_amounts: DataFrame, temp_effective: DataFrame,
    sm_lt_input: DataFrame, mappings: dict,
) -> DataFrame:
    """Add prorata amounts for unmapped state lines (non-PE Book only).

    Converted from: SQL lines 2046-2090.
    Row count: POSSIBLY-EMPTY — only when non-PE-Book AND excluded partners exist.

    NOTE: This section is gated by:
    1. AllocationTypeName <> 'PE Book Allocation'
    2. IF EXISTS(#ExcludeFromResidualProrataPercentage)
    Since #ExcludeFromResidualProrataPercentage is always empty in this SP,
    this section never executes.

    Returns:
        effective_amounts (unchanged in practice).
    """
    log_section("apply_pe_book_unmapped_lines")
    t0 = time.time()

    alloc_type = (cfg.get("allocation_type_name") or "").strip().lower()
    if alloc_type == "pe book allocation":
        logger.info("[SKIP] apply_pe_book_unmapped_lines: PE Book Allocation")
        log_timing("apply_pe_book_unmapped_lines", t0)
        return effective_amounts

    # Gate: #ExcludeFromResidualProrataPercentage is never populated → skip
    # If it were populated, the logic would:
    # 1. Find state lines in SM_LookThroughAllocationInput that have NO matching
    #    rows in #TempEffectiveAmounts (LEFT JOIN ... IS NULL)
    # 2. For those unmapped lines, compute: StateAmount * Prorata
    # 3. INSERT into #EffectiveAmounts

    logger.info(
        "[SKIP] apply_pe_book_unmapped_lines: "
        "ExcludeFromResidualProrataPercentage is never populated in this SP"
    )

    log_timing("apply_pe_book_unmapped_lines", t0)
    return effective_amounts
