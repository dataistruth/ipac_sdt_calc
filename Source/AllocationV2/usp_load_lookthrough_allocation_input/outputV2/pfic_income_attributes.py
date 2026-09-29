"""Development-only PFIC %: window total instead of self-join.

Same filters, unions, and output columns as production
``build_pfic_income_attributes``. Inner-join on EntityID/LineID/TrackingKey
drops null keys; the window path applies the same null-key drop so hashes
stay exact.
"""

from __future__ import annotations

import time

import pyspark.sql.functions as F
from pyspark.sql import Window

from .parent import output_module

_helpers = output_module("lt_helpers")
tbl = _helpers.tbl
log_section = _helpers.log_section
log_timing = _helpers.log_timing
logger = _helpers.logger


def build_pfic_income_attributes(
    spark,
    cfg,
    converted_pfic_amounts_df,
    lt_amounts_df,
    pfic_mapped_df,
    lower_tier_funds_df,
):
    log_section("build_pfic_income_attributes")
    t0 = time.time()

    if cfg["is_foreign_entity"]:
        log_timing("build_pfic_income_attributes", t0)
        return None

    if converted_pfic_amounts_df is None:
        log_timing("build_pfic_income_attributes", t0)
        return None

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]

    attr_type_id = (
        tbl(spark, "ENU_IncomeAttributeType", cfg)
        .filter(F.lower(F.col("AttributeType")) == "country")
        .select("ID")
        .first()
    )
    attribute_type_id = attr_type_id["ID"] if attr_type_id else 0

    lt_run_ids = [
        row["RunID"]
        for row in lower_tier_funds_df.select("RunID").distinct().collect()
        if row["RunID"] is not None
    ]

    if lt_run_ids:
        existing_pct = tbl(spark, "PFICtoK1IncomeAttributePercentages", cfg).filter(
            F.col("RunID").isin(lt_run_ids)
        )
        flowup_pfic_amounts = (
            lt_amounts_df.alias("LI")
            .join(
                pfic_mapped_df.alias("PK"),
                F.col("LI.LineID") == F.col("PK.K1LineID"),
            )
            .join(
                existing_pct.alias("IA"),
                (F.col("LI.LineID") == F.col("IA.LineID"))
                & (F.col("LI.TrackingKey") == F.col("IA.TrackingKey")),
            )
            .groupBy(
                F.col("LI.EntityID"),
                F.col("LI.LineID"),
                F.col("IA.CountryCode"),
                F.col("IA.AttributeID"),
                F.col("LI.TrackingKey"),
            )
            .agg(
                F.sum(F.col("IA.EffPercentage") * F.col("LI.FlowupAmount")).alias(
                    "Amount"
                )
            )
        )
        recalc_amounts = converted_pfic_amounts_df.unionByName(
            flowup_pfic_amounts, allowMissingColumns=True
        )
    else:
        logger.info("No lower-tier funds for this run — skipping PFIC flow-up read.")
        recalc_amounts = converted_pfic_amounts_df

    recalc_grouped = recalc_amounts.groupBy(
        "EntityID", "LineID", "CountryCode", "AttributeID", "TrackingKey"
    ).agg(F.sum("Amount").alias("Amount"))

    totals = Window.partitionBy("EntityID", "LineID", "TrackingKey")
    result = (
        recalc_grouped.filter(
            F.col("EntityID").isNotNull()
            & F.col("LineID").isNotNull()
            & F.col("TrackingKey").isNotNull()
        )
        .withColumn("TotalAmount", F.sum("Amount").over(totals))
        .filter(F.coalesce(F.col("TotalAmount"), F.lit(0)) != 0)
        .select(
            F.lit(run_id).alias("RunID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("LineID"),
            F.col("Amount"),
            (F.col("Amount") / F.col("TotalAmount")).alias("EffPercentage"),
            F.col("CountryCode"),
            F.col("AttributeID"),
            F.lit(attribute_type_id).alias("AttributeTypeID"),
            F.lit(client_id).alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.concat(
                F.col("TrackingKey"), F.lit("~"), F.lit(str(entity_id))
            ).alias("TrackingKey"),
            F.col("EntityID").alias("SourceEntityID"),
        )
    )

    log_timing("build_pfic_income_attributes", t0)
    return result
