"""Conservative helpers for run-scoped fact reads."""

import pyspark.sql.functions as F


def current_run(df, cfg):
    """Push only columns that exist; callers retain control of business predicates."""
    if "RunID" in df.columns:
        df = df.filter(F.col("RunID") == cfg["run_id"])
    if "ClientID" in df.columns:
        df = df.filter(F.col("ClientID") == cfg["client_id"])
    if "TaxPeriodID" in df.columns:
        df = df.filter(F.col("TaxPeriodID") == cfg["tax_period_id"])
    return df


def lower_tier_runs(spark, cfg):
    return F.broadcast(
        spark.table(f"_lower_tier_funds_{cfg['run_id']}")
        .select(F.col("RunID").cast("long").alias("RunID"))
        .where(F.col("RunID").isNotNull())
        .distinct()
    )


def prune_to_lower_tier_runs(df, spark, cfg):
    """Bound a historical RunID fact by the lower-tier key set."""
    if "RunID" not in df.columns:
        return df
    return df.join(lower_tier_runs(spark, cfg), "RunID", "left_semi")
