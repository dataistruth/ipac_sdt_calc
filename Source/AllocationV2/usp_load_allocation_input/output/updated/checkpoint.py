"""Profiled Allocation Input checkpoints.

Delta is the default backend. Delta data-skipping column statistics are
disabled while writing temporary checkpoint tables and the prior Spark
configuration is restored afterward.
"""

from __future__ import annotations

import logging
import re
import time
import uuid

from .plan_profiler import track_checkpoint_plan

logger = logging.getLogger(__name__)

DEFAULT_CHECKPOINT_BACKEND = "delta"
_VALID_BACKENDS = frozenset({"delta", "local"})
_STATS_KEY = "spark.databricks.delta.stats.collect"
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_]")


def normalize_checkpoint_backend(value: object) -> str:
    backend = str(value or DEFAULT_CHECKPOINT_BACKEND).strip().lower()
    if backend not in _VALID_BACKENDS:
        choices = ", ".join(sorted(_VALID_BACKENDS))
        raise ValueError(
            f"Unknown checkpoint backend {value!r}; expected one of: {choices}"
        )
    return backend


def should_checkpoint(cfg: dict, name: str) -> bool:
    """Production checkpoints remain enabled unless explicitly bypassed."""
    bypass = cfg.get("_checkpoint_bypass", ())
    return name not in set(bypass or ())


def _safe_name(value: object) -> str:
    return _SAFE_NAME.sub("_", str(value))


def _quoted_fqn(catalog: str, schema: str, table: str) -> str:
    return ".".join(
        f"`{part.replace('`', '``')}`" for part in (catalog, schema, table)
    )


def _get_conf(spark, key: str) -> tuple[bool, str | None]:
    try:
        return True, spark.conf.get(key)
    except Exception:
        return False, None


def _restore_conf(
    spark, key: str, existed: bool, previous: str | None
) -> None:
    try:
        if existed and previous is not None:
            spark.conf.set(key, previous)
        else:
            spark.conf.unset(key)
    except Exception:
        logger.debug("Could not restore Spark config %s", key, exc_info=True)


def checkpoint(spark, df, name: str, cfg: dict):
    """Materialize and profile a lineage break."""
    if not should_checkpoint(cfg, name):
        return df

    track_checkpoint_plan(name, df)
    backend = normalize_checkpoint_backend(
        cfg.get("_checkpoint_backend", cfg.get("checkpoint_backend"))
    )
    started = time.time()

    if backend == "local":
        result = df.localCheckpoint(eager=True).toDF(*df.columns)
        elapsed = round(time.time() - started, 3)
        cfg.setdefault("_checkpoint_elapsed", []).append(
            {"name": name, "elapsed_seconds": elapsed, "backend": "local"}
        )
        logger.info(
            "[updated checkpoint] %s: %.3fs (backend=local)", name, elapsed
        )
        return result

    run_id = _safe_name(cfg.get("run_id", "0"))
    table_name = (
        f"_tmp_alloc_input_updated_{_safe_name(name)}_{run_id}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    fqn = _quoted_fqn(cfg["catalog"], cfg["schema"], table_name)
    cfg.setdefault("_checkpoint_tables", []).append(fqn)

    existed, previous = _get_conf(spark, _STATS_KEY)
    try:
        spark.conf.set(_STATS_KEY, "false")
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .saveAsTable(fqn)
        )
    finally:
        _restore_conf(spark, _STATS_KEY, existed, previous)

    elapsed = round(time.time() - started, 3)
    cfg.setdefault("_checkpoint_elapsed", []).append(
        {
            "name": name,
            "elapsed_seconds": elapsed,
            "backend": "delta",
            "column_stats": "off",
        }
    )
    logger.info(
        "[updated checkpoint] %s: %.3fs "
        "(backend=delta, column_stats=off)",
        name,
        elapsed,
    )
    return spark.table(fqn)


def drop_checkpoints(spark, cfg: dict) -> None:
    """Drop only temporary tables created by this updated invocation."""
    if cfg.get("_skip_cleanup"):
        return
    for fqn in dict.fromkeys(cfg.get("_checkpoint_tables", [])):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:
            logger.warning("Failed to drop checkpoint %s", fqn, exc_info=True)
    cfg["_checkpoint_tables"] = []


def log_checkpoint_plan(cfg: dict) -> None:
    backend = normalize_checkpoint_backend(
        cfg.get("_checkpoint_backend", cfg.get("checkpoint_backend"))
    )
    line = (
        f"[checkpoint] backend={backend}"
        + (", column_stats=off" if backend == "delta" else "")
    )
    print(line)
    logger.info(line)
