"""
Write operations for uspLoadFootnotesAllocationToOutput.

Handles the final INSERT into AllocationOutput and the UPDATE operations
to deduct allocated amounts from AllocationInput.
"""
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0
from Common_V2.core.writers import write_insert, write_merge_custom
from Common_V2.core.observability import log_section, log_timing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# write_allocation_output
# SQL lines: 2653–2670
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def write_allocation_output(
    spark: SparkSession, cfg: dict,
    df_tmp_alloc_output: DataFrame,
) -> int:
    """INSERT allocated rows into permanent AllocationOutput table.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 2653-2670.
    Row count: ALWAYS-NON-EMPTY — final INSERT must produce rows for valid input.
    Filters: Amount<>0 before writing.
    Returns row count written.
    """
    log_section("write_allocation_output")
    t0 = time.time()

    if df_tmp_alloc_output is None:
        logger.warning("[SKIP] No allocation output to write")
        log_timing("write_allocation_output", t0)
        return 0

    # Filter Amount<>0
    df_write = df_tmp_alloc_output.filter(
        F.coalesce(F.col("Amount"), F.lit(0)) != 0
    )

    columns = [
        "RunID", "ClientID", "EntityID", "ShareClass", "PartnerNumber",
        "LineTypeID", "QuicklinkID", "LineID", "Amount", "AllocationType",
        "ParentEntityID", "SuperParentEntityID", "AllocationTypeID",
        "TrackingKey", "SchID", "OriginalParentEntityID",
    ]

    # Ensure SchID column exists
    if "SchID" not in df_write.columns:
        df_write = df_write.withColumn("SchID", F.lit(None).cast("int"))

    row_count = write_insert(
        spark, cfg, df_write, "AllocationOutput", columns,
        description="Footnote allocation output",
    )

    log_timing("write_allocation_output", t0)
    return row_count


# ---------------------------------------------------------------------------
# apply_deduction
# SQL lines: 2671–2790
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def apply_deduction(
    spark: SparkSession, cfg: dict,
    df_tmp_alloc_output: DataFrame,
    df_alloc_input: DataFrame,
    df_fn_allocated_lines: DataFrame,
    df_zero_exclude: DataFrame,
) -> None:
    """UPDATE AllocationInput to deduct allocated amounts and zero-out residuals.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 2671-2790.
    Row count: ALWAYS-NON-EMPTY — must match write output scope.

    Operations:
    1. Aggregate #tmpAllocationOutput → #AllocationOutputTotal (Footnote% groups)
    2. UPDATE AllocationInput.Amount -= allocated (with Allocate>100% cap logic)
    3. Zero-out residuals (Amount BETWEEN -0.99 AND 0.99), excluding ZeroExcludeLines
    4. Accumulate into #FNAllocatedLines (for zero-out scope)

    Uses Delta MERGE for the permanent table updates.
    """
    log_section("apply_deduction")
    t0 = time.time()

    if df_tmp_alloc_output is None:
        logger.info("[SKIP] No allocation output — skipping deduction")
        log_timing("apply_deduction", t0)
        return

    run_id = cfg["run_id"]
    dar_tid = cfg.get("default_allocation_rule_transaction_id")
    gdar_tid = cfg.get("global_default_allocation_rule_transaction_id")

    # Step 1: Aggregate → AllocationOutputTotal
    # WHERE AllocationType LIKE 'Footnote%'
    # SQL #tmpAllocationOutput.AdjustmentTypeID is never populated by INSERTs, so SQL
    # groups/joins on NULL. We mirror that by emitting AdjustmentTypeID = NULL and NOT
    # grouping by AllocationTypeID (SQL doesn't either).
    # SchID is always present by this point (added as NULL if missing upstream)
    _filtered = (
        df_tmp_alloc_output
        .filter(
            (F.col("RunID") == run_id)
            & F.col("AllocationType").like("Footnote%")
        )
        .withColumn("TrackingKey_ns", ns(F.col("TrackingKey")))
    )

    _group_cols = [
        "RunID", "ClientID", "EntityID", "LineTypeID",
        "QuicklinkID", "LineID", "ParentEntityID",
        "SuperParentEntityID", "TrackingKey_ns",
    ]
    if "SchID" in df_tmp_alloc_output.columns:
        _group_cols.append("SchID")

    df_aot = (
        _filtered
        .groupBy(*_group_cols)
        .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("AllocatedAmount"))
        .withColumnRenamed("TrackingKey_ns", "TrackingKey")
        .withColumn("AdjustmentTypeID", F.lit(None).cast("int"))
    )

    # Step 2: Build deduction source (join to DAR for Allocate>100% logic)
    dar = F.broadcast(
        read_table(spark, "DefaultAllocationRuleSetup", cfg)
        .filter(F.col("TransactionID").isin([dar_tid, gdar_tid]))
        .select(F.col("RuleID").alias("dar_RuleID"))
        .distinct()
    )

    # Build UPDATE source using temp view + SQL MERGE
    # Join AllocationOutputTotal with DAR to determine cap condition
    df_update_source = (
        df_aot.alias("AO")
        .join(
            df_alloc_input.select("TypeID", "EntityID", "LineID", "TrackingKey", "Tag", "QuicklinkID").distinct().alias("AI_types"),
            (F.col("AO.EntityID") == F.col("AI_types.EntityID"))
            & (F.col("AO.LineID") == F.col("AI_types.LineID"))
            & (ns(F.col("AO.TrackingKey")) == ns(F.col("AI_types.TrackingKey")))
            & (F.col("AO.QuicklinkID") == F.col("AI_types.QuicklinkID")),
            "left",
        )
        .join(dar, F.col("AI_types.TypeID") == F.col("dar_RuleID"), "left")
        .select(
            F.col("AO.RunID"), F.col("AO.ClientID"), F.col("AO.EntityID"),
            F.col("AO.LineTypeID"), F.col("AO.QuicklinkID"), F.col("AO.LineID"),
            F.col("AO.ParentEntityID"), F.col("AO.SuperParentEntityID"),
            F.col("AO.AdjustmentTypeID"),
            F.col("AO.TrackingKey"), F.col("AI_types.Tag").alias("Tag"),
            F.col("AO.AllocatedAmount"),
            F.col("dar_RuleID"),
        )
    )

    # Add SchID if available
    if "SchID" in df_aot.columns:
        df_update_source = (
            df_aot.alias("AO")
            .join(
                df_alloc_input.select("TypeID", "EntityID", "LineID", "TrackingKey", "Tag", "QuicklinkID").distinct().alias("AI_types"),
                (F.col("AO.EntityID") == F.col("AI_types.EntityID"))
                & (F.col("AO.LineID") == F.col("AI_types.LineID"))
                & (ns(F.col("AO.TrackingKey")) == ns(F.col("AI_types.TrackingKey")))
                & (F.col("AO.QuicklinkID") == F.col("AI_types.QuicklinkID")),
                "left",
            )
            .join(dar, F.col("AI_types.TypeID") == F.col("dar_RuleID"), "left")
            .select(
                F.col("AO.RunID"), F.col("AO.ClientID"), F.col("AO.EntityID"),
                F.col("AO.LineTypeID"), F.col("AO.QuicklinkID"), F.col("AO.LineID"),
                F.col("AO.ParentEntityID"), F.col("AO.SuperParentEntityID"),
                F.col("AO.AdjustmentTypeID"),
                F.col("AO.TrackingKey"), F.col("AI_types.Tag").alias("Tag"),
                F.col("AO.SchID"),
                F.col("AO.AllocatedAmount"),
                F.col("dar_RuleID"),
            )
        )

    # MERGE into permanent AllocationInput
    merge_on = f"""
        t.RunID = s.RunID
        AND t.EntityID = s.EntityID
        AND t.ClientID = s.ClientID
        AND COALESCE(t.ParentEntityID, 0) = COALESCE(s.ParentEntityID, 0)
        AND COALESCE(t.SuperParentEntityID, 0) = COALESCE(s.SuperParentEntityID, 0)
        AND COALESCE(t.AdjustmentTypeID, 0) = COALESCE(s.AdjustmentTypeID, 0)
        AND COALESCE(t.TrackingKey, '') = COALESCE(s.TrackingKey, '')
        AND COALESCE(t.Tag, '') = COALESCE(s.Tag, '')
        AND t.LineID = s.LineID
        AND t.LineTypeID = s.LineTypeID
        AND t.QuicklinkID = s.QuicklinkID
        AND COALESCE(t.SchID, 0) = COALESCE(s.SchID, 0)
        AND t.RunID = {run_id}
    """

    write_merge_custom(
        spark, cfg, df_update_source, "AllocationInput",
        merge_on=merge_on,
        update_exprs={
            "Amount": (
                "CASE WHEN s.dar_RuleID IS NOT NULL AND ABS(s.AllocatedAmount) > ABS(t.Amount) "
                "THEN 0 ELSE t.Amount - s.AllocatedAmount END"
            ),
            "Amount704b": (
                "CASE WHEN s.dar_RuleID IS NOT NULL AND ABS(s.AllocatedAmount) > ABS(t.Amount) "
                "THEN ROUND(s.AllocatedAmount, 0) ELSE t.Amount704b END"
            ),
        },
        description="Footnote deduction",
    )

    # Step 3: Accumulate FNAllocatedLines (for zero-out)
    df_new_fn_lines = (
        df_aot
        .select("LineID", "LineTypeID", "RunID")
        .distinct()
    )

    # Combine with existing fn_allocated_lines (from 704c deduction)
    if df_fn_allocated_lines is not None:
        df_all_fn = df_fn_allocated_lines.unionByName(df_new_fn_lines).distinct()
    else:
        df_all_fn = df_new_fn_lines

    # Step 4: Zero-out residuals on PERMANENT AllocationInput
    # WHERE ISNULL(Amount,0) BETWEEN -0.99 AND 0.99
    # AND NOT IN ZeroExcludeLines
    ze_view = f"_zero_exclude_{run_id}"
    fn_view = f"_fn_alloc_{run_id}"
    df_all_fn.createOrReplaceTempView(fn_view)
    df_zero_exclude.createOrReplaceTempView(ze_view)

    zero_source_sql = (
        f"SELECT DISTINCT fn.LineID, fn.LineTypeID, fn.RunID "
        f"FROM {fn_view} fn "
        f"LEFT JOIN {ze_view} ze ON ze.LineTypeID = fn.LineTypeID AND ze.LineID = fn.LineID "
        f"WHERE ze.LineID IS NULL"
    )

    zero_merge_on = (
        f"t.LineID = s.LineID AND t.LineTypeID = s.LineTypeID "
        f"AND t.RunID = s.RunID AND t.RunID = {run_id} "
        f"AND COALESCE(t.Amount, 0) BETWEEN -0.99 AND 0.99"
    )

    write_merge_custom(
        spark, cfg, None, "AllocationInput",
        merge_on=zero_merge_on,
        update_exprs={"Amount": "0"},
        source_sql=zero_source_sql,
        description="Zero-out residuals",
    )

    log_timing("apply_deduction", t0)
    return df_all_fn
