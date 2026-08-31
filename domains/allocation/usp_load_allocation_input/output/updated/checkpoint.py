"""
checkpoint.py — lineage breaks for updated package.

Default steps match original output.load_allocation_input:
  alloc_input, base_flowup, pfic_flowup, alloc_filtered (+ alloc_tagged if tagged).

Set in cfg (optional):
  checkpoint_backend: "auto" | "local" | "delta" | "volume"
    auto / default → local (executor disk, fast; not fault-tolerant — full job restart on failure)
  volume_path: final flow-up outputs (GenericResultStorer), not checkpoints by default.
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

CHECKPOINT_STEPS: frozenset[str] = frozenset(
    {
        "alloc_input",
        "base_flowup",  # inner PFIC flowup (post-reclass / post-zero in ai_pfic_flowup_service)
        "pfic_flowup",
        "alloc_filtered",
        "alloc_tagged",
    }
)

OPT_IN_CHECKPOINT_STEPS: frozenset[str] = frozenset(
    {
        "reclass_data",
        "pfic_snapshot",
        "pfic_raw",
    }
)

_OPT_IN_CFG_KEYS: dict[str, str] = {
    "reclass_data": "checkpoint_reclass_data",
    "pfic_snapshot": "checkpoint_pfic_snapshot",
    "pfic_raw": "checkpoint_pfic_raw",
}


def should_checkpoint(cfg: dict, step_name: str) -> bool:
    if step_name in OPT_IN_CHECKPOINT_STEPS:
        key = _OPT_IN_CFG_KEYS[step_name]
        return bool(cfg.get(key, False))

    if step_name not in CHECKPOINT_STEPS:
        return False

    if step_name == "alloc_tagged":
        return int(cfg.get("investment_tag_workflow_id", 0) or 0) != 0

    return True


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
    if mode == "local":
        return "local"
    # auto: executor localCheckpoint — fast lineage break; job restart if executor loses data.
    return "local"


def log_checkpoint_plan(cfg: dict) -> None:
    backend = _resolve_backend(cfg)
    all_steps = CHECKPOINT_STEPS | OPT_IN_CHECKPOINT_STEPS
    enabled = sorted(name for name in all_steps if should_checkpoint(cfg, name))
    print(f"[checkpoint] backend={backend} steps={enabled}")


def _ensure_checkpoint_lists(cfg: dict) -> None:
    cfg.setdefault("_checkpoint_tables", [])
    cfg.setdefault("_checkpoint_paths", [])
    cfg.setdefault("_checkpoint_local_count", 0)


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


def _local_checkpoint(df: DataFrame, name: str, cfg: dict) -> DataFrame:
    """
    Spark localCheckpoint on executor local disk — cuts lineage without UC / cloud I/O.
    Not fault-tolerant: executor loss requires full job restart.
    """
    logger.info(f"[CHECKPOINT] localCheckpoint (executor disk): {name}")
    cfg["_checkpoint_local_count"] = int(cfg.get("_checkpoint_local_count", 0)) + 1
    return df.localCheckpoint(eager=True)


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
    Materialize df to break lineage.

    local (default): executor localCheckpoint — fastest; not fault-tolerant.
    delta: UC temp Delta table (original / Common_V2.core.checkpoint).
    volume: uncompressed Parquet on volume_path (opt-in).
    """
    _ensure_checkpoint_lists(cfg)
    run_id = cfg.get("run_id", 0)
    uniq = uuid.uuid4().hex[:8]
    backend = _resolve_backend(cfg)

    if backend == "local":
        return _local_checkpoint(df, name, cfg)

    if backend == "volume":
        path = _volume_checkpoint_path(cfg, name, uniq)
        cfg["_checkpoint_paths"].append(path)
        logger.info(f"[CHECKPOINT] volume write: {path}")
        return _write_volume_checkpoint(spark, df, path)

    fqn = f"{table_prefix(cfg)}._tmp_{name}_{run_id}_{uniq}"
    cfg["_checkpoint_tables"].append(fqn)
    logger.info(f"[CHECKPOINT] delta write: {fqn}")
    return _write_delta_checkpoint(spark, df, fqn)
