"""Conservative pruning, caching, and broadcast helpers."""

from __future__ import annotations

import threading

import pyspark.sql.functions as F
from pyspark import StorageLevel

_CACHE_LOCK = threading.Lock()


def current_run(df, cfg):
    """Push the current RunID predicate when the table exposes RunID."""
    if "RunID" in df.columns:
        return df.filter(F.col("RunID") == cfg["run_id"])
    return df


def scoped(df, cfg):
    """Push client/tax-period predicates when those columns are available."""
    if "ClientID" in df.columns:
        df = df.filter(F.col("ClientID") == cfg["client_id"])
    if "TaxPeriodID" in df.columns:
        df = df.filter(F.col("TaxPeriodID") == cfg["tax_period_id"])
    return df


def current_run_scoped(df, cfg):
    return current_run(scoped(df, cfg), cfg)


def lower_tier_runs(spark, cfg):
    """Small set of child RunIDs reached by the current parent run."""
    return F.broadcast(
        spark.table(f"_lower_tier_funds_{cfg['run_id']}")
        .select(F.col("RunID").cast("long").alias("RunID"))
        .where(F.col("RunID").isNotNull())
        .distinct()
    )


def prune_to_lower_tier_runs(df, spark, cfg):
    """Prune historical flow-up facts to child runs used by this invocation."""
    if "RunID" not in df.columns:
        return df
    return df.join(lower_tier_runs(spark, cfg), "RunID", "left_semi")


def cache_for_run(df, cfg, *, broadcast: bool = False):
    """Persist and materialize a reused, already-pruned DataFrame."""
    cached = df.persist(StorageLevel.MEMORY_AND_DISK)
    cached.count()
    with _CACHE_LOCK:
        cfg.setdefault("_cached_dataframes", []).append(cached)
    return F.broadcast(cached) if broadcast else cached


def unpersist_cached(cfg):
    """Release only DataFrames cached by this package."""
    for df in cfg.get("_cached_dataframes", ()):
        try:
            df.unpersist(blocking=False)
        except Exception:
            pass
    cfg["_cached_dataframes"] = []
