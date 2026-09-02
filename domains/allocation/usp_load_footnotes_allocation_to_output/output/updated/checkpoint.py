"""Fast, namespaced UC Delta checkpoints for footnote allocation."""

from __future__ import annotations

import logging
import re
import time
import uuid

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_STATS_KEY = "spark.databricks.delta.stats.collect"
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_]")
_MAX_RETRIES = 3
_RETRY_DELAY_SECONDS = 5
_TRANSIENT_ERRORS = (
    "RESOURCE_DOES_NOT_EXIST",
    "Staging Table",
    "TABLE_OR_VIEW_ALREADY_EXISTS",
    "TableAlreadyExistsException",
)


def _safe_name(value: object) -> str:
    return _SAFE_NAME.sub("_", str(value))


def _quoted_fqn(catalog: str, schema: str, table: str) -> str:
    return ".".join(
        f"`{part.replace('`', '``')}`" for part in (catalog, schema, table)
    )


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
    """Materialize a lineage break without Delta data-skipping statistics."""
    started = time.time()
    run_id = _safe_name(cfg.get("run_id", "0"))
    table_name = (
        f"_tmp_footnote_updated_{_safe_name(name)}_{run_id}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    fqn = _quoted_fqn(cfg["catalog"], cfg["schema"], table_name)
    checkpoint_tables = cfg.setdefault("_checkpoint_tables", [])
    checkpoint_tables.append(fqn)

    existed, previous = _get_conf(spark, _STATS_KEY)
    try:
        spark.conf.set(_STATS_KEY, "false")
        last_error = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                spark.sql(f"DROP TABLE IF EXISTS {fqn}")
                (
                    df.write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .option("delta.dataSkippingNumIndexedCols", "0")
                    .option("compression", "uncompressed")
                    .saveAsTable(fqn)
                )
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                is_transient = any(
                    marker in str(exc) for marker in _TRANSIENT_ERRORS
                )
                if not is_transient or attempt == _MAX_RETRIES:
                    raise
                logger.warning(
                    "[updated checkpoint] transient UC error %s/%s for %s: %s",
                    attempt,
                    _MAX_RETRIES,
                    fqn,
                    str(exc)[:200],
                )
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {fqn}")
                except Exception:
                    pass
                time.sleep(_RETRY_DELAY_SECONDS * attempt)
        if last_error is not None:
            raise last_error
    finally:
        _restore_conf(spark, _STATS_KEY, existed, previous)

    elapsed = round(time.time() - started, 3)
    cfg.setdefault("_updated_checkpoint_timings", []).append(
        {"step": f"checkpoint:{name}", "elapsed_seconds": elapsed}
    )
    logger.info(
        "[updated checkpoint] %s: %.3fs (stats=off)", name, elapsed
    )
    return spark.table(fqn)


def drop_checkpoints(spark: SparkSession, cfg: dict) -> None:
    """Drop only temporary tables registered by this updated invocation."""
    if cfg.get("_skip_cleanup"):
        return
    for fqn in dict.fromkeys(cfg.get("_checkpoint_tables", [])):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:
            logger.warning("Failed to drop checkpoint %s", fqn, exc_info=True)
    cfg["_checkpoint_tables"] = []
