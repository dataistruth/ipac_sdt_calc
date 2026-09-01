"""Fast UC Delta checkpoints for the optimized FEP pipeline."""

from __future__ import annotations

import logging
import re
import time

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_STATS_KEY = "spark.databricks.delta.stats.collect"
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_]")


def _safe_name(value: object) -> str:
    return _SAFE_NAME.sub("_", str(value))


def _get_conf(spark: SparkSession, key: str) -> tuple[bool, str | None]:
    try:
        return True, spark.conf.get(key)
    except Exception:
        return False, None


def _restore_conf(
    spark: SparkSession,
    key: str,
    existed: bool,
    value: str | None,
) -> None:
    try:
        if existed and value is not None:
            spark.conf.set(key, value)
        else:
            spark.conf.unset(key)
    except Exception:
        logger.debug("Could not restore Spark config %s", key, exc_info=True)


def checkpoint(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
) -> DataFrame:
    """Materialize a lineage break with Delta data-skipping stats disabled."""
    started = time.time()
    run_id = _safe_name(cfg.get("run_id", "0"))
    fqn = (
        f"{cfg['catalog']}.{cfg['schema']}."
        f"_tmp_fep_updated_{_safe_name(name)}_{run_id}"
    )
    checkpoint_tables = cfg.setdefault("_checkpoint_tables", [])
    if fqn not in checkpoint_tables:
        checkpoint_tables.append(fqn)

    existed, previous = _get_conf(spark, _STATS_KEY)
    try:
        spark.conf.set(_STATS_KEY, "false")
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .option("compression", "uncompressed")
            .saveAsTable(fqn)
        )
    finally:
        _restore_conf(spark, _STATS_KEY, existed, previous)

    elapsed = time.time() - started
    cfg.setdefault("_updated_checkpoint_timings", []).append(
        {"name": name, "elapsed_seconds": round(elapsed, 3)}
    )
    print(f"[updated checkpoint] {name}: {elapsed:.3f}s (stats=off)")
    return spark.table(fqn)


def drop_checkpoints(spark: SparkSession, cfg: dict) -> None:
    """Drop only temporary tables registered by this run."""
    if cfg.get("_skip_cleanup"):
        return
    for fqn in dict.fromkeys(cfg.get("_checkpoint_tables", [])):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:
            logger.warning("Failed to drop checkpoint %s", fqn, exc_info=True)
    cfg["_checkpoint_tables"] = []
