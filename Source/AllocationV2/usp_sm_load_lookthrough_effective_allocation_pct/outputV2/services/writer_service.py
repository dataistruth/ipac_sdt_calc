"""Final write operations for SM LookThrough Effective Allocation Percentage.

Functions:
    write_allocation_output  — SQL lines 2155-2167
    update_allocation_input  — SQL lines 2169-2175
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0, table_prefix

from Common_V2.core.observability import log_section, log_timing
from Common_V2.core.generic_result_storer import GenericResultStorer

logger = logging.getLogger(__name__)


def write_allocation_output(
    spark: SparkSession, cfg: dict,
    effective_amounts: DataFrame, partner_snapshot: DataFrame,
) -> int:
    """Write effective amounts to SM_LookThroughAllocationOutput.

    Converted from: SQL lines 2155-2167.
    Row count: ALWAYS-NON-EMPTY — final output write.

    SQL pattern:
        INSERT INTO SM_LookThroughAllocationOutput
        SELECT @RunID, @ClientID, ea.EntityID, P.ShareClass, ea.PartnerNumber,
               ea.LineTypeID, StateID, StateFieldId, EffectiveAmount, AllocType,
               NULL, ea.ParentEntityID, SuperParentEntityID, TrackingKey,
               ea.OriginalParentEntityID, 0, ''
        FROM #EffectiveAmounts ea
        JOIN #Partner_Snapshot P ON EA.PartnerNumber = P.PartnerNumber
          AND P.EntityID = @EntityID AND P.Clientid = @ClientID
          AND P.TaxperiodID = @TaxPeriodID
    """
    log_section("write_allocation_output")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    entity_id = cfg["entity_id"]
    #fqn_output = f"{table_prefix(cfg)}.SM_LookThroughAllocationOutput"

    assert run_id is not None, "run_id must not be None"
    assert client_id is not None, "client_id must not be None"

    # ── JOIN #EffectiveAmounts ea JOIN #Partner_Snapshot P ──
    # Partner_Snapshot casing: Clientid, TaxperiodID, EntityID (from _columns.md)
    output_df = (
        effective_amounts.alias("EA")
        .join(
            F.broadcast(partner_snapshot).alias("P"),
            (F.col("EA.PartnerNumber") == F.col("P.PartnerNumber"))
            & (F.col("P.EntityID") == entity_id)
            & (F.col("P.Clientid") == client_id)
            & (F.col("P.TaxperiodID") == cfg["tax_period_id"]),
        )
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.col("EA.EntityID"),
            F.col("P.ShareClass"),
            F.col("EA.PartnerNumber"),
            F.col("EA.LineTypeID"),
            F.col("EA.StateID"),
            F.col("EA.StateFieldID").alias("StateLineID"),
            F.col("EA.EffectiveAmount").alias("Amount"),
            F.col("EA.AllocType").alias("AllocationType"),
            F.lit(None).cast("string").alias("LineCode"),
            F.col("EA.ParentEntityID"),
            F.col("EA.SuperParentEntityID"),
            F.col("EA.TrackingKey"),
            F.col("EA.OriginalParentEntityID"),
            F.lit(0).alias("AdjustmentTypeID"),
            F.lit("").alias("Tag"),
        )
    )

    # Write via GenericResultStorer (Delta + Parquet in parallel)
    return_value = None
    result_type = cfg.get("result_type", "Parquet")
    if result_type:
        # Cast DataFrame to match target Delta table schema (saveAsTable is strict)
        table_name = "SM_LookThroughAllocationOutput"
        fqn = f"{cfg['catalog']}.{cfg['schema']}.{table_name}"
        target_schema = spark.table(fqn).schema
        for field in target_schema:
            if field.name in output_df.columns:
                output_df = output_df.withColumn(field.name, F.col(field.name).cast(field.dataType))

        storer = GenericResultStorer(spark)
        return_value = storer.save_results(
            result={table_name: output_df},
            result_type=result_type,
            catalog_name=cfg["catalog"],
            database_name=cfg["schema"],
            run_id=cfg["run_id"],
            client_id=cfg["client_id"],
            entity_id=cfg["entity_id"],
            execution_id=cfg.get("execution_id"),
            volume_path=cfg.get("volume_path"),
            sql_url_path=None,
            sql_username=None,
            sql_password=None,
        )
        if return_value:
            print(f"[PARQUET] SM_LookThroughAllocationOutput path: {return_value}")

    logger.info(f"[WRITE] SM_LookThroughAllocationOutput: write complete")
    log_timing("write_allocation_output", t0)
    return return_value


def update_allocation_input(
    spark: SparkSession, cfg: dict,
    effective_amounts: DataFrame,
) -> int:
    """Set Amount=0 on SM_LookThroughAllocationInput for processed state lines.

    Converted from: SQL lines 2169-2175.
    Row count: ALWAYS-NON-EMPTY — zeroes out processed inputs.

    SQL pattern:
        SELECT StateId, StateFieldId, LineTypeId, EntityID, ParentEntityID,
               SuperParentEntityID, TrackingKey, SUM(EffectiveAmount)
        INTO #TempAllocatedAmounts FROM #EffectiveAmounts GROUP BY ...

        UPDATE SM_LookThroughAllocationInput SET Amount = 0
        FROM SAI JOIN #TempAllocatedAmounts EA
        ON SAI.StateID = EA.StateId AND SAI.StateLineID = EA.StateFieldId
          AND SAI.LineTypeID = EA.LineTypeID AND SAI.EntityID = EA.EntityID
          AND SAI.ParentEntityID = EA.ParentEntityID
          AND SAI.SuperParentEntityID = EA.SuperParentEntityID
          AND SAI.TrackingKey = EA.TrackingKey
        WHERE RunID = @RunID AND ClientID = @ClientID
    """
    log_section("update_allocation_input")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    fqn_input = f"{table_prefix(cfg)}.SM_LookThroughAllocationInput"

    assert run_id is not None, "run_id must not be None for UPDATE"
    assert client_id is not None, "client_id must not be None for UPDATE"

    # ── S13-line 2169: #TempAllocatedAmounts ──
    # PERF: The EXISTS subquery only matches on key columns — EffectiveAmount
    # is never referenced. Use select+distinct instead of groupBy+agg(sum)
    # to skip the SUM computation and produce a smaller temp view.
    temp_allocated = (
        effective_amounts
        .select(
            "StateID", "StateFieldID", "LineTypeID", "EntityID",
            "ParentEntityID", "SuperParentEntityID", "TrackingKey",
        )
        .distinct()
    )

    update_view = f"_temp_alloc_amounts_{run_id}"
    temp_allocated.createOrReplaceTempView(update_view)

    # ── S13-line 2175: UPDATE ... SET Amount = 0 ──
    # UPDATE WHERE EXISTS is simpler than MERGE for single-column SET.
    # Note: SQL uses direct = on ParentEntityID/SuperParentEntityID/TrackingKey
    # (no ISNULL wrapping), so NULLs will NOT match — same behavior preserved.
    spark.sql(f"""
        UPDATE {fqn_input} AS SAI
        SET Amount = 0
        WHERE SAI.RunID = {run_id}
          AND SAI.ClientID = {client_id}
          AND EXISTS (
            SELECT 1 FROM {update_view} EA
            WHERE SAI.StateID = EA.StateID
              AND SAI.StateLineID = EA.StateFieldID
              AND SAI.LineTypeID = EA.LineTypeID
              AND SAI.EntityID = EA.EntityID
              AND SAI.ParentEntityID = EA.ParentEntityID
              AND SAI.SuperParentEntityID = EA.SuperParentEntityID
              AND SAI.TrackingKey = EA.TrackingKey
          )
    """)

    logger.info("[UPDATE] SM_LookThroughAllocationInput: zeroed processed rows")

    spark.catalog.dropTempView(update_view)
    log_timing("update_allocation_input", t0)
    return 1
