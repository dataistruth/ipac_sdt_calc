"""
checkpoint.py — volume / Delta checkpoints for updated package.

Matches production checkpoint placement (same steps as monolith /
load_allocation_input_updated), using volume uncompressed Parquet when
volume_path is set.

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

# Production-aligned step names (see output/load_allocation_input_updated.py).
CHECKPOINT_STEPS: frozenset[str] = frozenset(
    {
        "reclass_data",  # shared_views_builders.register_reclass_data
        "pfic_snapshot",
        "alloc_input",
        "pfic_raw",
        "base_flowup",  # inner PFIC flowup (monolith ai_pfic_flowup_service)
        "pfic_flowup",
        "alloc_filtered",
        "alloc_tagged",
    }
)


def should_checkpoint(cfg: dict, step_name: str) -> bool:
    if step_name not in CHECKPOINT_STEPS:
        return False
    if step_name == "alloc_tagged":
        return int(cfg.get("investment_tag_workflow_id", 0) or 0) != 0
    return True


def log_checkpoint_plan(cfg: dict) -> None:
    enabled = sorted(name for name in CHECKPOINT_STEPS if should_checkpoint(cfg, name))
    print(f"[checkpoint] steps={enabled} (production-aligned)")


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
