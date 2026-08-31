"""
RunID partition pruning for flowup and run-scoped tables.

Use before joins that would otherwise scan full tables partitioned on RunID.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from Common_V2.core.helpers import read_table


def lower_tier_run_ids(spark: SparkSession, cfg: dict) -> DataFrame:
    """LTRunID values from _lower_tier_funds_{run_id} (lower-tier allocation runs)."""
    run_id = int(cfg["run_id"])
    return (
        spark.table(f"_lower_tier_funds_{run_id}")
        .select(F.col("RunID").cast("long").alias("RunID"))
        .distinct()
    )


def _client_tax_filter(df: DataFrame, cfg: dict) -> DataFrame:
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    cols = set(df.columns)
    if "ClientID" in cols:
        df = df.filter(F.col("ClientID") == client_id)
    if "TaxPeriodID" in cols:
        df = df.filter(F.col("TaxPeriodID") == tax_period_id)
    return df


def _prune_run_ids(df: DataFrame, run_ids: DataFrame) -> DataFrame:
    return df.join(F.broadcast(run_ids), "RunID", "left_semi")


def read_local_run_table(spark: SparkSession, table_name: str, cfg: dict) -> DataFrame:
    """Current entity run (cfg run_id) — e.g. PficForeignCorpClassificationInput."""
    run_id = cfg["run_id"]
    df = read_table(spark, table_name, cfg).filter(F.col("RunID") == run_id)
    return _client_tax_filter(df, cfg)


def read_lower_tier_flowup(spark: SparkSession, table_name: str, cfg: dict) -> DataFrame:
    """Lower-tier LTRunIDs — e.g. PFICFootnoteFlowupWithTrackingKey, Form*Flowup pass-through."""
    df = read_table(spark, table_name, cfg)
    df = _prune_run_ids(df, lower_tier_run_ids(spark, cfg))
    return _client_tax_filter(df, cfg)
