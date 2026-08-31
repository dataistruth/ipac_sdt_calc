"""
checkpoint.py — lineage breaks for updated package.

Default steps match sdt_d output.load_allocation_input:
  pfic_snapshot, alloc_input, base_flowup, pfic_raw, pfic_flowup,
  alloc_filtered (+ alloc_tagged if tagged).

Set in cfg (optional):
  checkpoint_backend: "auto" | "delta" | "local" | "volume"
    auto / default → delta (UC temp tables)
  checkpoint_compression: parquet codec for updated.checkpoint() only (default: uncompressed)
  checkpoint_use_production: use Common_V2.core.checkpoint for pipeline breaks (default: True)
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
        "pfic_snapshot",
        "alloc_input",
        "base_flowup",  # inner PFIC flowup (post-reclass / post-zero)
        "pfic_raw",
        "pfic_flowup",
        "alloc_filtered",
        "alloc_tagged",
    }
)

OPT_IN_CHECKPOINT_STEPS: frozenset[str] = frozenset(
    {
        "reclass_data",
    }
)

_OPT_IN_CFG_KEYS: dict[str, str] = {
    "reclass_data": "checkpoint_reclass_data",
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
    if mode in ("delta", "local"):
        return mode
    # auto: Delta temp tables (matches original / Common_V2.core.checkpoint).
    return "delta"


def _checkpoint_compression(cfg: dict) -> str | None:
    raw = str(
        cfg.get("checkpoint_compression")
        or cfg.get("write_compression")
        or "uncompressed"
    ).strip().lower()
    if raw in ("uncompressed", "none"):
        return "uncompressed"
    if raw in ("", "default"):
        return None
    return raw


def log_checkpoint_plan(cfg: dict) -> None:
    backend = _resolve_backend(cfg)
    all_steps = CHECKPOINT_STEPS | OPT_IN_CHECKPOINT_STEPS
    enabled = sorted(name for name in all_steps if should_checkpoint(cfg, name))
    comp = _checkpoint_compression(cfg) if backend in ("delta", "volume") else None
    comp_note = f" compression={comp}" if comp else ""
    print(f"[checkpoint] backend={backend}{comp_note} steps={enabled}")


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


def _write_delta_checkpoint(
    spark: SparkSession,
    df: DataFrame,
    fqn: str,
    cfg: dict,
) -> DataFrame:
    compression = _checkpoint_compression(cfg)
    optimize = bool(cfg.get("checkpoint_delta_optimize_write", False))
    for attempt in range(1, _CHECKPOINT_MAX_RETRIES + 1):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
            writer = (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
            )
            if optimize:
                writer = writer.option("optimizeWrite", "true")
            if compression:
                writer = writer.option("compression", compression)
            writer.saveAsTable(fqn)
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


def _use_production_checkpoint(cfg: dict) -> bool:
    return bool(cfg.get("checkpoint_use_production", True))


def checkpoint_production(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
) -> DataFrame:
    """
    Match sdt_d Common_V2.core.checkpoint — snappy Delta, DROP before write.
    Falls back to local implementation when Common_V2 is not on sys.path.
    """
    try:
        from Common_V2.core.checkpoint import checkpoint as core_checkpoint

        return core_checkpoint(spark, df, name, cfg)
    except ImportError:
        _ensure_checkpoint_lists(cfg)
        run_id = cfg.get("run_id", "0")
        uniq = uuid.uuid4().hex[:8]
        fqn = f"{table_prefix(cfg)}._tmp_{name}_{run_id}_{uniq}"
        cfg["_checkpoint_tables"].append(fqn)
        logger.info(f"[CHECKPOINT] production fallback write: {fqn}")
        prod_cfg = dict(cfg)
        prod_cfg["checkpoint_compression"] = "default"
        prod_cfg["write_compression"] = "default"
        return _write_delta_checkpoint(spark, df, fqn, prod_cfg)


def drop_checkpoints(spark: SparkSession, cfg: dict) -> None:
    """Drop UC temp tables and volume checkpoint paths tracked in cfg."""
    try:
        from Common_V2.core.checkpoint import drop_checkpoints as core_drop

        core_drop(spark, cfg)
    except ImportError:
        for fqn in cfg.get("_checkpoint_tables", []):
            try:
                spark.sql(f"DROP TABLE IF EXISTS {fqn}")
            except Exception as exc:
                logger.warning(f"[CHECKPOINT] Failed to drop table {fqn}: {exc}")
        cfg["_checkpoint_tables"] = []

    for path in cfg.get("_checkpoint_paths", []):
        try:
            _rm_path(spark, path)
        except Exception as exc:
            logger.warning(f"[CHECKPOINT] Failed to remove path {path}: {exc}")
    cfg["_checkpoint_paths"] = []


def _write_volume_checkpoint(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    cfg: dict,
) -> DataFrame:
    compression = _checkpoint_compression(cfg) or "uncompressed"
    for attempt in range(1, _CHECKPOINT_MAX_RETRIES + 1):
        try:
            _rm_path(spark, path)
            (
                df.write.mode("overwrite")
                .option("compression", compression)
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

    delta (default): UC temp Delta table with uncompressed Parquet (cfg-driven).
    local: executor localCheckpoint (opt-in).
    volume: Parquet files on volume_path (opt-in).
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
        return _write_volume_checkpoint(spark, df, path, cfg)

    fqn = f"{table_prefix(cfg)}._tmp_{name}_{run_id}_{uniq}"
    cfg["_checkpoint_tables"].append(fqn)
    comp = _checkpoint_compression(cfg)
    comp_note = f" compression={comp}" if comp else ""
    logger.info(f"[CHECKPOINT] delta write: {fqn}{comp_note}")
    return _write_delta_checkpoint(spark, df, fqn, cfg)
