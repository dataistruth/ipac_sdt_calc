"""
checkpoint.py — lineage breaks for updated package.

Pipeline checkpoints use UC Volume Parquet when volume_path is set (serverless-safe,
faster than UC Delta temp tables). Set checkpoint_backend=delta only to force UC Delta.

Default steps match sdt_d output.load_allocation_input:
  pfic_snapshot, alloc_input, base_flowup, pfic_raw, pfic_flowup,
  alloc_filtered (+ alloc_tagged if tagged).

Cfg (optional):
  volume_path: required for volume checkpoints + final flow-up outputs
  checkpoint_backend: "auto" | "volume" | "delta"
    auto + volume_path → volume (default for updated package)
  checkpoint_compression: parquet codec (default: uncompressed)
  checkpoint_use_production: UC Delta via Common_V2 when backend=delta (default: False)
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
    if mode == "delta":
        return "delta"
    if mode == "volume" or (mode == "auto" and volume):
        if not volume:
            raise ValueError(
                "Volume checkpoint requires volume_path "
                "(e.g. /Volumes/qa7/datavolume/databrickdata/checkpoint)"
            )
        return "volume"
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
    vol = str(cfg.get("volume_path") or "").strip()
    vol_note = f" volume_path={vol}" if backend == "volume" and vol else ""
    print(f"[checkpoint] backend={backend}{comp_note}{vol_note} steps={enabled}")


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
    return bool(cfg.get("checkpoint_use_production", False))


def pipeline_checkpoint(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
) -> DataFrame:
    """Volume Parquet when volume_path is set; optional UC Delta via Common_V2."""
    if _use_production_checkpoint(cfg) and _resolve_backend(cfg) == "delta":
        return checkpoint_production(spark, df, name, cfg)
    return checkpoint(spark, df, name, cfg)


def inner_base_flowup_checkpoint(
    spark: SparkSession,
    df: DataFrame,
    cfg: dict,
    label: str,
) -> DataFrame:
    """Mid-pipeline PFIC flowup break (post-reclass / post-zero)."""
    if not should_checkpoint(cfg, "base_flowup"):
        return df
    return pipeline_checkpoint(spark, df, f"base_flowup_{label}", cfg)


def checkpoint_production(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
) -> DataFrame:
    """
    Match sdt_d Common_V2.core.checkpoint — snappy Delta, DROP before write.
    Falls back to updated Delta when Common_V2 is not on sys.path.
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
    """Materialize df: volume Parquet (default when volume_path set) or UC Delta."""
    _ensure_checkpoint_lists(cfg)
    run_id = cfg.get("run_id", 0)
    uniq = uuid.uuid4().hex[:8]
    backend = _resolve_backend(cfg)

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
