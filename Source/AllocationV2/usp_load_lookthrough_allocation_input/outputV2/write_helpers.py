"""Distinct-table final writes for look-through allocation input outputV2.

Same filters, aggregations, casts, and appends as production
``write_final_output``. Each function writes one table and builds its
Spark writer inside the task.
"""

from __future__ import annotations

import time

import pyspark.sql.functions as F

from .parent import output_module

_helpers = output_module("lt_helpers")
logger = _helpers.logger
tbl = _helpers.tbl
tbl_name = _helpers.tbl_name
log_section = _helpers.log_section
log_timing = _helpers.log_timing


def write_lookthrough_allocation_input(spark, cfg, alloc_input_df):
    allocation_type = cfg.get("allocation_type", "")
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    final_input = (
        alloc_input_df.filter(F.coalesce(F.col("Amount"), F.lit(0)) != 0)
        .groupBy(
            "ParentEntityID",
            "EntityID",
            "LineTypeID",
            "LineID",
            "QuicklinkID",
            F.coalesce(F.col("CategoryID"), F.lit(0)).alias("CategoryID"),
            "PeriodID",
            F.coalesce(F.col("LineCode"), F.lit("")).alias("LineCode"),
            F.coalesce(F.col("SuperParentEntityID"), F.lit(0)).alias(
                "SuperParentEntityID"
            ),
            F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias(
                "AdjustmentTypeID"
            ),
            "TrackingKey",
            F.when(
                F.lower(F.lit(allocation_type)) == "pro rata", F.lit("")
            )
            .otherwise(F.coalesce(F.col("Tag"), F.lit("")))
            .alias("Tag"),
            "OriginalParentEntityID",
        )
        .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("Amount"))
        .withColumn("RunID", F.lit(run_id).cast("bigint"))
        .withColumn("ClientID", F.lit(client_id).cast("bigint"))
        .withColumn("Amount704b", F.col("Amount"))
    )
    target_schema = spark.table(
        tbl_name("LookThroughAllocationInput", cfg)
    ).schema
    for field in target_schema:
        if field.name in final_input.columns:
            final_input = final_input.withColumn(
                field.name, F.col(field.name).cast(field.dataType)
            )
    final_input.write.mode("append").saveAsTable(
        tbl_name("LookThroughAllocationInput", cfg)
    )
    logger.info(
        "Wrote %s rows to LookThroughAllocationInput.",
        final_input.count(),
    )


def write_sch_k_taxable_income(
    spark, cfg, alloc_input_df, lower_tier_funds_df
):
    entity_id = cfg["entity_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    k1_line_item = tbl(spark, "K1LineItem", cfg)
    distinct_lt_funds = lower_tier_funds_df.select("RunID", "EntityID").distinct()
    sch_k_taxable = (
        alloc_input_df.alias("LI")
        .filter(F.col("LI.LineTypeID") == k1_lt)
        .join(k1_line_item.alias("KL"), F.col("LI.LineID") == F.col("KL.LineID"))
        .join(
            distinct_lt_funds.alias("LT"),
            F.col("LT.EntityID") == F.col("LI.LTEntityID"),
        )
        .groupBy(F.col("LI.LTEntityID"), F.col("LT.RunID"))
        .agg(
            F.sum(
                F.when(
                    F.lower(F.col("KL.TaxableIncomeRule")) == "subtract",
                    -1 * F.col("LI.Amount"),
                )
                .when(
                    F.lower(F.col("KL.TaxableIncomeRule")) == "add",
                    F.col("LI.Amount"),
                )
                .otherwise(F.lit(0))
            ).alias("TaxableIncome")
        )
        .select(
            F.lit(run_id).alias("UpperTierRunID"),
            F.lit(entity_id).alias("UpperTierEntityID"),
            F.col("LT.RunID").alias("LowerTierRunID"),
            F.col("LI.LTEntityID").alias("LowerTierEntityID"),
            F.col("TaxableIncome"),
        )
    )
    sch_k_taxable.write.mode("append").saveAsTable(
        tbl_name("SchKTaxableIncome", cfg)
    )
    logger.info("Wrote SchKTaxableIncome rows.")


def write_pfic_income_attributes(spark, cfg, pfic_income_attr_df):
    if pfic_income_attr_df is None:
        return
    pfic_income_attr_df.write.mode("append").saveAsTable(
        tbl_name("PFICtoK1IncomeAttributePercentages", cfg)
    )
    logger.info("Wrote PFICtoK1IncomeAttributePercentages.")


def write_final_output_parallel(
    spark,
    cfg,
    alloc_input_df,
    lower_tier_funds_df,
    pfic_income_attr_df,
    workers,
    activity,
    enabled_groups,
    run_parallel,
):
    """Append independent result tables concurrently. K3/AllocationRun stay sequential."""
    log_section("write_final_output")
    started = time.time()
    tasks = [
        (
            "LookThroughAllocationInput",
            lambda: write_lookthrough_allocation_input(
                spark, {**cfg}, alloc_input_df
            ),
        ),
        (
            "SchKTaxableIncome",
            lambda: write_sch_k_taxable_income(
                spark, {**cfg}, alloc_input_df, lower_tier_funds_df
            ),
        ),
    ]
    if pfic_income_attr_df is not None:
        tasks.append(
            (
                "PFICtoK1IncomeAttributePercentages",
                lambda: write_pfic_income_attributes(
                    spark, {**cfg}, pfic_income_attr_df
                ),
            )
        )
    run_parallel(
        tasks,
        workers,
        activity,
        "output_writes",
        enabled_groups,
    )
    log_timing("write_final_output", started)
