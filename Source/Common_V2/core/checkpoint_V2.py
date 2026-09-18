"""Configurable checkpoint strategies for Databricks Spark pipelines.

This module is additive: ``Common_V2.core.checkpoint`` remains unchanged.

Modes:
    1 - Delta with column statistics disabled.
    2 - Odd calls localCheckpoint; even calls stats-off Delta (default).
    3 - Odd calls localCheckpoint; even calls uncompressed Volume Parquet.
    4 - All calls localCheckpoint.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
import uuid

from pyspark.sql import DataFrame, SparkSession

try:
    from Common_V2.core.helpers import table_prefix
except ImportError:
    def table_prefix(cfg: dict) -> str:
        catalog = cfg.get("catalog") or cfg.get("Catalog") or "hive_metastore"
        schema = cfg.get("schema") or cfg.get("Schema") or "default"
        return f"{catalog}.{schema}"

logger = logging.getLogger(__name__)

DEFAULT_CHECKPOINT_MODE = 2
VALID_CHECKPOINT_MODES = frozenset({1, 2, 3, 4})
DELTA_STATS_KEY = "spark.databricks.delta.stats.collect"

_STATE_CREATION_LOCK = threading.Lock()
_DELTA_STATS_LOCK = threading.Lock()


def normalize_checkpoint_mode(value=None) -> int:
    """Validate and return a checkpoint mode in the range 1..4."""
    raw = DEFAULT_CHECKPOINT_MODE if value is None else value
    try:
        mode = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("CheckpointMode must be one of 1, 2, 3, 4") from exc
    if mode not in VALID_CHECKPOINT_MODES:
        raise ValueError("CheckpointMode must be one of 1, 2, 3, 4")
    return mode


def resolve_checkpoint_mode(
    cfg: dict | None = None,
    checkpoint_mode=None,
    CheckpointMode=None,
) -> int:
    """Resolve an SP override before falling back to the shared default.

    Priority:
      1. Explicit ``CheckpointMode`` (notebook/schedule convention)
      2. Explicit ``checkpoint_mode`` (Python/SP-local convention)
      3. ``cfg["CheckpointMode"]``
      4. ``cfg["checkpoint_mode"]``
      5. ``DEFAULT_CHECKPOINT_MODE``
    """
    value = CheckpointMode
    if value is None:
        value = checkpoint_mode
    if value is None and isinstance(cfg, dict):
        value = cfg.get("CheckpointMode")
    if value is None and isinstance(cfg, dict):
        value = cfg.get("checkpoint_mode")
    return normalize_checkpoint_mode(value)


def initialize_checkpoint_V2(cfg: dict, checkpoint_mode=None) -> dict:
    """Initialize the shared per-invocation counter before copying ``cfg``.

    Shallow cfg copies keep the same state object, making sequence assignment
    atomic when independent stages call checkpoint_V2 from worker threads.
    """
    mode = resolve_checkpoint_mode(cfg, checkpoint_mode=checkpoint_mode)
    with _STATE_CREATION_LOCK:
        state = cfg.get("_checkpoint_v2_state")
        if not isinstance(state, dict) or "lock" not in state:
            state = {"count": 0, "lock": threading.Lock()}
            cfg["_checkpoint_v2_state"] = state
    cfg["checkpoint_mode"] = mode
    cfg.setdefault("_checkpoint_tables", [])
    cfg.setdefault("_checkpoint_paths", [])
    cfg.setdefault("_checkpoint_v2_activity", [])
    return state


def _next_sequence(cfg: dict, checkpoint_mode=None) -> tuple[int, int]:
    mode = resolve_checkpoint_mode(cfg, checkpoint_mode=checkpoint_mode)
    state = initialize_checkpoint_V2(cfg, mode)
    with state["lock"]:
        state["count"] += 1
        sequence = state["count"]
    return mode, sequence


def _select_backend(mode: int, sequence: int) -> str:
    if mode == 1:
        return "delta"
    if mode == 2:
        return "local" if sequence % 2 else "delta"
    if mode == 3:
        return "local" if sequence % 2 else "volume"
    return "local"


def _track_plan(name: str, df: DataFrame, cfg: dict) -> None:
    """Record the incoming plan when AllocationV2.plan_profiler is deployed."""
    if not cfg.get("profile_plan"):
        return
    try:
        from AllocationV2.plan_profiler import track_checkpoint_plan

        track_checkpoint_plan(name, df, cfg)
    except Exception:
        logger.debug("[CHECKPOINT_V2] Plan tracking unavailable", exc_info=True)


def _delta_checkpoint(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
    sequence: int,
) -> DataFrame:
    run_id = cfg.get("run_id", "0")
    fqn = (
        f"{table_prefix(cfg)}._tmp_v2_{name}_{run_id}_{sequence}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    cfg["_checkpoint_tables"].append(fqn)

    # The stats setting is session-scoped. Serialize set/write/restore so
    # concurrent Delta checkpoints cannot restore each other's values.
    with _DELTA_STATS_LOCK:
        previous = None
        existed = True
        try:
            previous = spark.conf.get(DELTA_STATS_KEY)
        except Exception:
            existed = False
        try:
            spark.conf.set(DELTA_STATS_KEY, "false")
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("delta.dataSkippingNumIndexedCols", "0")
                .saveAsTable(fqn)
            )
        finally:
            try:
                if existed:
                    spark.conf.set(DELTA_STATS_KEY, previous)
                else:
                    spark.conf.unset(DELTA_STATS_KEY)
            except Exception:
                logger.warning(
                    "[CHECKPOINT_V2] Failed to restore %s", DELTA_STATS_KEY
                )
    return spark.read.table(fqn)


def _volume_checkpoint(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
    sequence: int,
) -> DataFrame:
    volume_path = (
        cfg.get("checkpoint_volume_path")
        or cfg.get("volume_path")
        or cfg.get("VolumePath")
    )
    if not volume_path:
        raise ValueError(
            "CheckpointMode 3 requires checkpoint_volume_path or volume_path"
        )
    path = (
        f"{str(volume_path).rstrip('/')}/_checkpoints/"
        f"{cfg.get('client_id', '0')}/{cfg.get('run_id', '0')}/"
        f"{cfg.get('execution_id', '1')}/"
        f"{name}_{sequence}_{uuid.uuid4().hex[:8]}"
    )
    cfg["_checkpoint_paths"].append(path)
    (
        df.write.mode("overwrite")
        .option("compression", "uncompressed")
        .parquet(path)
    )
    return spark.read.parquet(path)


def checkpoint_V2(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
    checkpoint_mode=None,
) -> DataFrame:
    """Materialize ``df`` according to checkpoint mode (default: mode 2).

    A localCheckpoint failure falls back to stats-off Delta so Spark Connect or
    serverless limitations do not fail the SP solely because of its mode.
    """
    mode, sequence = _next_sequence(cfg, checkpoint_mode)
    requested_backend = _select_backend(mode, sequence)
    _track_plan(f"{name}#{sequence}", df, cfg)
    started = time.time()
    print(
        f"[CHECKPOINT_V2] start name={name} sequence={sequence} "
        f"mode={mode} backend={requested_backend}",
        flush=True,
    )

    actual_backend = requested_backend
    if requested_backend == "local":
        try:
            result = df.localCheckpoint(eager=True)
        except Exception as exc:
            actual_backend = "delta"
            print(
                f"[CHECKPOINT_V2] fallback name={name} sequence={sequence} "
                f"local->delta reason={type(exc).__name__}: {exc}",
                flush=True,
            )
            result = _delta_checkpoint(spark, df, name, cfg, sequence)
    elif requested_backend == "delta":
        result = _delta_checkpoint(spark, df, name, cfg, sequence)
    else:
        result = _volume_checkpoint(spark, df, name, cfg, sequence)

    elapsed = round(time.time() - started, 3)
    cfg["_checkpoint_v2_activity"].append(
        {
            "name": name,
            "sequence": sequence,
            "mode": mode,
            "requested_backend": requested_backend,
            "backend": actual_backend,
            "elapsed_seconds": elapsed,
        }
    )
    print(
        f"[CHECKPOINT_V2] done name={name} sequence={sequence} mode={mode} "
        f"backend={actual_backend} elapsed={elapsed:.3f}s",
        flush=True,
    )
    return result


def _remove_volume_path(spark: SparkSession, path: str) -> None:
    dbutils = None
    with contextlib.suppress(Exception):
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    if dbutils is None:
        with contextlib.suppress(Exception):
            from IPython import get_ipython

            shell = get_ipython()
            dbutils = shell.user_ns.get("dbutils") if shell else None
    if dbutils is None:
        raise RuntimeError("DBUtils is unavailable for Volume cleanup")
    dbutils.fs.rm(path, True)


def drop_checkpoints_V2(spark: SparkSession, cfg: dict) -> None:
    """Drop Delta tables and remove Volume paths created by checkpoint_V2."""
    for fqn in dict.fromkeys(cfg.get("_checkpoint_tables", ())):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:
            logger.warning(
                "[CHECKPOINT_V2] Failed to drop table: %s", fqn, exc_info=True
            )
    cfg["_checkpoint_tables"] = []

    for path in dict.fromkeys(cfg.get("_checkpoint_paths", ())):
        try:
            _remove_volume_path(spark, path)
        except Exception:
            logger.warning(
                "[CHECKPOINT_V2] Failed to remove path: %s", path, exc_info=True
            )
    cfg["_checkpoint_paths"] = []


# Convenient aliases for updated orchestrators that already import these names.
checkpoint = checkpoint_V2
drop_checkpoints = drop_checkpoints_V2


__all__ = [
    "DEFAULT_CHECKPOINT_MODE",
    "VALID_CHECKPOINT_MODES",
    "checkpoint",
    "checkpoint_V2",
    "drop_checkpoints",
    "drop_checkpoints_V2",
    "initialize_checkpoint_V2",
    "normalize_checkpoint_mode",
    "resolve_checkpoint_mode",
]
