"""
checkpoint_updated.py — local sibling of Common_V2.core.checkpoint for A/B testing.

COPY TO:
  Source/AllocationV2/usp_load_allocation_input/output/checkpoint_updated.py

Import in load_allocation_input_updated.py:
  from .checkpoint_updated import checkpoint, drop_checkpoints

Improvements vs Common_V2.core.checkpoint:
  - auto: uncompressed Parquet files on volume_path when set
  - delta: same as production (UC temp table _tmp_{name}_{run_id}_{uniq})
  - optimizeWrite on Delta checkpoints
  - tracks volume paths in cfg["_checkpoint_paths"] for cleanup

Set in cfg (optional):
  checkpoint_backend: "auto" | "delta" | "volume"  (default "auto")
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from Common_V2.core.helpers import table_prefix

logger = logging.getLogger(__name__)

_CHECKPOINT_MAX_RETRIES = 3
_CHECKPOINT_RETRY_DELAY = 5


def _ensure_checkpoint_lists(cfg: dict) -> None:
    cfg.setdefault("_checkpoint_tables", [])
    cfg.setdefault("_checkpoint_paths", [])


def _is_transient_uc_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    needles = (
        "metadata",
        "concurrent",
        "staging",
        "temporarily",
        "timeout",
        "throttle",
        "already exists",
    )
    return any(n in msg for n in needles)


def _resolve_backend(cfg: dict) -> str:
    mode = str(cfg.get("checkpoint_backend", "auto")).strip().lower()
    volume = str(cfg.get("volume_path") or "").strip()
    if mode == "volume":
        if not volume:
            raise ValueError(
                "checkpoint_backend=volume requires volume_path "
                "(e.g. /Volumes/qa7/datavolume/databrickdata/checkpoint)"
            )
        return "volume"
    if mode == "delta":
        return "delta"
    return "volume" if volume else "delta"


def _volume_checkpoint_path(cfg: dict, name: str, uniq: str) -> str:
    base = str(cfg.get("volume_path") or "").rstrip("/")
    run_id = cfg.get("run_id", 0)
    return f"{base}/_checkpoints/{run_id}/{name}_{uniq}"


def _dbutils(spark: SparkSession) -> Any | None:
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except Exception:
        return None


def _rm_path(spark: SparkSession, path: str) -> None:
    dbu = _dbutils(spark)
    if dbu is not None:
        dbu.fs.rm(path, recurse=True)
        return
    logger.warning(f"[CHECKPOINT] No dbutils — could not remove path: {path}")


def _write_delta_checkpoint(spark: SparkSession, df: DataFrame, fqn: str) -> DataFrame:
    for attempt in range(1, _CHECKPOINT_MAX_RETRIES + 1):
        try:
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("optimizeWrite", "true")
                .saveAsTable(fqn)
            )
            return spark.table(fqn)
        except Exception as exc:
            if _is_transient_uc_error(exc) and attempt < _CHECKPOINT_MAX_RETRIES:
                logger.warning(
                    f"[CHECKPOINT] Transient UC error on attempt "
                    f"{attempt}/{_CHECKPOINT_MAX_RETRIES}: {exc}"
                )
                spark.sql(f"DROP TABLE IF EXISTS {fqn}")
                time.sleep(_CHECKPOINT_RETRY_DELAY * attempt)
                continue
            raise


def _write_volume_checkpoint(spark: SparkSession, df: DataFrame, path: str) -> DataFrame:
    """Write uncompressed Parquet files to volume (faster CPU; larger temp files)."""
    for attempt in range(1, _CHECKPOINT_MAX_RETRIES + 1):
        try:
            _rm_path(spark, path)
            (
                df.write.mode("overwrite")
                .option("compression", "uncompressed")
                .parquet(path)
            )
            return spark.read.parquet(path)
        except Exception as exc:
            if attempt < _CHECKPOINT_MAX_RETRIES:
                logger.warning(
                    f"[CHECKPOINT] Volume write retry "
                    f"{attempt}/{_CHECKPOINT_MAX_RETRIES}: {exc}"
                )
                time.sleep(_CHECKPOINT_RETRY_DELAY * attempt)
                continue
            raise


def checkpoint(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
) -> DataFrame:
    """
    Materialize df to break lineage. Same contract as Common_V2.core.checkpoint.

    auto + volume_path → uncompressed Parquet files under volume (no UC temp table).
    Otherwise → Delta temp table in catalog (production behavior).
    """
    _ensure_checkpoint_lists(cfg)
    run_id = cfg.get("run_id", 0)
    uniq = uuid.uuid4().hex[:8]
    backend = _resolve_backend(cfg)

    if backend == "volume":
        path = _volume_checkpoint_path(cfg, name, uniq)
        cfg["_checkpoint_paths"].append(path)
        logger.info(f"[CHECKPOINT] volume write: {path}")
        return _write_volume_checkpoint(spark, df, path)

    fqn = f"{table_prefix(cfg)}._tmp_{name}_{run_id}_{uniq}"
    cfg["_checkpoint_tables"].append(fqn)
    logger.info(f"[CHECKPOINT] delta write: {fqn}")
    return _write_delta_checkpoint(spark, df, fqn)


def drop_checkpoints(spark: SparkSession, cfg: dict) -> None:
    """Drop UC temp tables and volume checkpoint dirs. Call at end of run."""
    for fqn in cfg.get("_checkpoint_tables", []):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
            logger.debug(f"[CHECKPOINT] Dropped table: {fqn}")
        except Exception as exc:
            logger.warning(f"[CHECKPOINT] Failed to drop table {fqn}: {exc}")

    for path in cfg.get("_checkpoint_paths", []):
        try:
            _rm_path(spark, path)
            logger.debug(f"[CHECKPOINT] Removed path: {path}")
        except Exception as exc:
            logger.warning(f"[CHECKPOINT] Failed to remove path {path}: {exc}")

    cfg["_checkpoint_tables"] = []
    cfg["_checkpoint_paths"] = []
