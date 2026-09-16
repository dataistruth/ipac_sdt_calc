"""Allocation-input checkpoints optimized for temporary Delta materialization."""

from __future__ import annotations

import contextvars
import logging
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyspark.sql.functions as F
from pyspark import StorageLevel

try:
    from .plan_profiler import profile_dataframe
except Exception:
    def profile_dataframe(label, df, cfg, *, kind="checkpoint"):
        del label, cfg, kind
        return df

logger = logging.getLogger(__name__)
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_]")
_VALID_BACKENDS = frozenset({"local", "delta"})
MAX_THREADS = 4
_CACHE_LOCK = threading.Lock()


def _safe_name(value: object) -> str:
    return _SAFE_NAME.sub("_", str(value))


def _quoted_fqn(catalog: str, schema: str, table: str) -> str:
    return ".".join(
        f"`{part.replace('`', '``')}`" for part in (catalog, schema, table)
    )


def normalize_checkpoint_backend(value: object) -> str:
    backend = str(value or "delta").strip().lower()
    if backend not in _VALID_BACKENDS:
        raise ValueError("CheckpointBackend must be 'local' or 'delta'")
    return backend


def normalize_local_denylist(value: object, mode: object = "extend") -> frozenset[str]:
    """Parse LocalDeltaDenylist. ``mode`` is accepted for older notebook copies."""
    del mode
    if not value:
        return frozenset()
    tokens = re.split(r"[,\s]+", value) if isinstance(value, str) else value
    return frozenset(str(token).strip() for token in tokens if str(token).strip())


normalize_local_delta_denylist = normalize_local_denylist


def _local_is_denied(name: str, cfg: dict) -> bool:
    denylist = cfg.get("_local_delta_denylist", ())
    return any(name == token or name.startswith(token) for token in denylist)


def checkpoint(spark, df, name: str, cfg: dict):
    """Write and immediately reread a temporary Delta table with stats disabled.

    The table option is scoped to this write, avoiding mutation of Spark-wide
    configuration while other jobs are running in the four-thread pool.
    """
    profile_dataframe(name, df, cfg, kind="checkpoint")
    backend = normalize_checkpoint_backend(cfg.get("_checkpoint_backend", "delta"))
    started = time.time()

    if backend == "local" and not _local_is_denied(name, cfg):
        result = df.localCheckpoint(eager=True).toDF(*df.columns)
        elapsed = round(time.time() - started, 3)
        cfg.setdefault("_checkpoint_elapsed", []).append(
            {
                "name": name,
                "elapsed_seconds": elapsed,
                "backend": "local",
            }
        )
        logger.info("[checkpoint] %s: %.3fs (local)", name, elapsed)
        return result

    run_id = _safe_name(cfg.get("run_id", "0"))
    table_name = (
        f"_tmp_alloc_input_updated_{_safe_name(name)}_{run_id}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    fqn = _quoted_fqn(cfg["catalog"], cfg["schema"], table_name)
    cfg.setdefault("_checkpoint_tables", []).append(fqn)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.dataSkippingNumIndexedCols", "0")
        .saveAsTable(fqn)
    )
    result = spark.read.table(fqn)
    elapsed = round(time.time() - started, 3)
    cfg.setdefault("_checkpoint_elapsed", []).append(
        {
            "name": name,
            "elapsed_seconds": elapsed,
            "backend": "delta",
            "column_stats": "off",
            "local_denylist_fallback": backend == "local",
        }
    )
    logger.info("[checkpoint] %s: %.3fs (Delta stats off)", name, elapsed)
    return result


def pipeline_checkpoint(spark, df, name: str, cfg: dict):
    """Alias used by shared-view and PFIC copies of the production helpers."""
    return checkpoint(spark, df, name, cfg)


def log_checkpoint_plan(cfg: dict) -> None:
    backend = normalize_checkpoint_backend(
        cfg.get("_checkpoint_backend", cfg.get("checkpoint_backend"))
    )
    denylist = ",".join(sorted(cfg.get("_local_delta_denylist", ()))) or "none"
    print(f"[checkpoint] backend={backend} local_deny_list={denylist}")


def drop_checkpoints(spark, cfg: dict) -> None:
    """Drop checkpoint tables created by this invocation."""
    for fqn in dict.fromkeys(cfg.get("_checkpoint_tables", ())):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:
            logger.warning("Failed to drop checkpoint %s", fqn, exc_info=True)
    cfg["_checkpoint_tables"] = []


def run_parallel(tasks, label: str):
    """Execute named callables with an invariant maximum of four threads."""
    if not tasks:
        return []
    workers = min(MAX_THREADS, len(tasks))
    started = time.time()
    values = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for name, task in tasks:
            context = contextvars.copy_context()
            futures[pool.submit(context.run, task)] = name
        for future in as_completed(futures):
            name = futures[future]
            values[name] = future.result()
    logger.info(
        "[parallel] %s: tasks=%d workers=%d wall=%.2fs",
        label,
        len(tasks),
        workers,
        time.time() - started,
    )
    return [(name, values[name]) for name, _ in tasks]


def isolated_collector_cfg(cfg: dict) -> dict:
    local = dict(cfg)
    local["_parquet_results"] = {}
    local["_schema_cache"] = {}
    return local


def merge_collector_cfg(target: dict, local: dict) -> None:
    target_results = target.setdefault("_parquet_results", {})
    for table_name, df in local.get("_parquet_results", {}).items():
        if table_name in target_results:
            target_results[table_name] = target_results[table_name].unionByName(
                df, allowMissingColumns=True
            )
        else:
            target_results[table_name] = df
    target.setdefault("_schema_cache", {}).update(local.get("_schema_cache", {}))


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
    if not hasattr(df, "persist"):
        return df
    cached = df.persist(StorageLevel.MEMORY_AND_DISK)
    cached.count()
    with _CACHE_LOCK:
        cfg.setdefault("_cached_dataframes", []).append(cached)
    return F.broadcast(cached) if broadcast else cached


def unpersist_cached(cfg):
    for df in cfg.get("_cached_dataframes", ()):
        try:
            df.unpersist(blocking=False)
        except Exception:
            pass
    cfg["_cached_dataframes"] = []
