"""
checkpoint.py — volume / Delta checkpoints for updated package.

- auto: uncompressed Parquet files on volume_path when set
- delta: same as production (UC temp table _tmp_{name}_{run_id}_{uniq})
- optimizeWrite on Delta checkpoints
- tracks volume paths in cfg["_checkpoint_paths"] (audit only; no end-of-run cleanup)

Set in cfg (optional):
  checkpoint_backend: "auto" | "delta" | "volume"  (default "auto")
  checkpoint_level: "minimal" | "default" | "full"  (default "default")
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

CHECKPOINT_LEVELS = ("minimal", "default", "full")

# Every named checkpoint in the pipeline (for logging).
_ALL_STEPS: frozenset[str] = frozenset(
    {
        "reclass_data",
        "lower_tier_funds",
        "alloc_post_k1",
        "pfic_snapshot",
        "alloc_input",
        "pfic_raw",
        "alloc_post_7b",
        "pfic_flowup",
        "alloc_filtered",
        "alloc_tagged",
    }
)

# Step names must match checkpoint(..., name, ...) calls in the pipeline.
_STEPS_BY_LEVEL: dict[str, frozenset[str]] = {
    "minimal": frozenset({"pfic_snapshot", "alloc_input"}),
    "default": frozenset(
        {
            "reclass_data",
            "lower_tier_funds",
            "alloc_post_k1",
            "pfic_snapshot",
            "alloc_input",
            "pfic_raw",
            "alloc_post_7b",
            "pfic_flowup",
            "alloc_filtered",
        }
    ),
    "full": _ALL_STEPS,
}


def normalize_checkpoint_level(raw: str | None, default: str = "default") -> str:
    level = str(raw or default).strip().lower()
    if level not in CHECKPOINT_LEVELS:
        raise ValueError(
            f"checkpoint_level must be one of {CHECKPOINT_LEVELS}, got '{raw}'"
        )
    return level


def checkpoint_enabled(cfg: dict, step_name: str) -> bool:
    level = normalize_checkpoint_level(cfg.get("checkpoint_level"), default="default")
    allowed = _STEPS_BY_LEVEL[level]
    if step_name not in allowed:
        return False
    if step_name == "alloc_tagged":
        return int(cfg.get("investment_tag_workflow_id", 0) or 0) != 0
    return True


def log_checkpoint_level(cfg: dict) -> None:
    level = normalize_checkpoint_level(cfg.get("checkpoint_level"), default="default")
    enabled = sorted(
        name for name in _ALL_STEPS if checkpoint_enabled(cfg, name)
    )
    workers = int(cfg.get("parallel_checkpoint_workers", 1) or 1)
    print(f"[checkpoint] level={level} steps={enabled}")
    if workers > 1:
        print(f"[checkpoint] parallel_checkpoint_workers={workers}")


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


def parallel_checkpoint_workers(cfg: dict) -> int:
    raw = (
        cfg.get("parallel_checkpoint_workers")
        or cfg.get("parallel_write_workers")
        or cfg.get("parallel_config_workers")
        or 1
    )
    try:
        return max(1, int(raw or 1))
    except (TypeError, ValueError):
        return 1


def checkpoint_parallel(
    spark: SparkSession,
    cfg: dict,
    named_dfs: list[tuple[str, DataFrame]],
    max_workers: int | None = None,
) -> dict[str, DataFrame]:
    """
    Materialize multiple independent DataFrames concurrently.

    Safe when checkpoints do not depend on each other's outputs (e.g. alloc_post_7b
    + pfic_flowup after phase 7b). Falls back to sequential for a single item.
    """
    if not named_dfs:
        return {}

    workers = max(1, int(max_workers or parallel_checkpoint_workers(cfg)))
    if len(named_dfs) == 1:
        name, df = named_dfs[0]
        return {name: checkpoint(spark, df, name, cfg)}

    from concurrent.futures import ThreadPoolExecutor, as_completed

    print(
        f"[checkpoint] parallel write: {len(named_dfs)} table(s), "
        f"max_workers={min(workers, len(named_dfs))}"
    )

    def _one(name: str, df: DataFrame) -> tuple[str, DataFrame]:
        return name, checkpoint(spark, df, name, cfg)

    results: dict[str, DataFrame] = {}
    errors: list[str] = []
    pool_workers = min(workers, len(named_dfs))
    with ThreadPoolExecutor(max_workers=pool_workers) as executor:
        futures = {
            executor.submit(_one, name, df): name for name, df in named_dfs
        }
        for fut in as_completed(futures):
            step_name = futures[fut]
            try:
                name, out_df = fut.result()
                results[name] = out_df
                print(f"[checkpoint] parallel ok {name}")
            except Exception as exc:
                errors.append(f"{step_name}: {exc}")
    if errors:
        raise RuntimeError(
            "Parallel checkpoint failed: " + "; ".join(errors)
        )
    return results
