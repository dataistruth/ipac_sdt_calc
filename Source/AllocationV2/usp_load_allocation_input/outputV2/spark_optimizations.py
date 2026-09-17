"""Serverless-safe pruning and broadcast helpers."""

import pyspark.sql.functions as F


def current_run(df, cfg):
    if "RunID" in df.columns:
        return df.filter(F.col("RunID") == cfg["run_id"])
    return df


def scoped(df, cfg):
    if "ClientID" in df.columns:
        df = df.filter(F.col("ClientID") == cfg["client_id"])
    if "TaxPeriodID" in df.columns:
        df = df.filter(F.col("TaxPeriodID") == cfg["tax_period_id"])
    return df


def current_run_scoped(df, cfg):
    return current_run(scoped(df, cfg), cfg)


def lower_tier_runs(spark, cfg):
    return F.broadcast(
        spark.table(f"_lower_tier_funds_{cfg['run_id']}")
        .select(F.col("RunID").cast("long").alias("RunID"))
        .where(F.col("RunID").isNotNull())
        .distinct()
    )


def prune_to_lower_tier_runs(df, spark, cfg):
    if "RunID" not in df.columns:
        return df
    return df.join(lower_tier_runs(spark, cfg), "RunID", "left_semi")


def cache_for_run(df, cfg, *, broadcast: bool = False):
    """Compatibility name: no persist/cache on serverless."""
    del cfg
    if not hasattr(df, "columns"):
        return df
    if broadcast:
        print(f"[broadcast] columns={len(df.columns)}", flush=True)
        return F.broadcast(df)
    return df


def unpersist_cached(cfg):
    """Compatibility no-op because this package never persists DataFrames."""
    cfg["_cached_dataframes"] = []


__all__ = [
    "cache_for_run",
    "current_run",
    "current_run_scoped",
    "lower_tier_runs",
    "prune_to_lower_tier_runs",
    "scoped",
    "unpersist_cached",
]
