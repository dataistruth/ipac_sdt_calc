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

# Checkpoint backend: "delta" (durable UC Delta temp table) or "local"
# (df.localCheckpoint(eager=True) -- no metastore commit / small-file I/O, so
# materially faster). Selected per-run via cfg["_checkpoint_backend"].
#
# CAVEAT (learned in usp_get_final_effective_percentage): localCheckpoint returns
# a LogicalRDD that Spark cannot re-resolve with fresh attribute IDs, so any
# builder that self-joins a checkpointed DataFrame against something derived from
# it fails with UNRESOLVED_COLUMN. The Delta round-trip returns a catalog
# relation that re-resolves cleanly. To stay correct while defaulting to local,
# checkpoint names matching cfg["_local_delta_denylist"] (prefix match) are
# forced back to "delta". The denylist starts empty here; if a local run crashes
# with UNRESOLVED_COLUMN at some checkpoint, add that name via the notebook
# widget (no redeploy) and, once known-stable, promote it into the default.
DEFAULT_CHECKPOINT_BACKEND = "delta"
_VALID_BACKENDS = frozenset({"delta", "local"})
_LOCAL_DELTA_DENYLIST_DEFAULT: frozenset[str] = frozenset()


def normalize_checkpoint_backend(value: object) -> str:
    backend = str(value or DEFAULT_CHECKPOINT_BACKEND).strip().lower()
    if backend not in _VALID_BACKENDS:
        choices = ", ".join(sorted(_VALID_BACKENDS))
        raise ValueError(
            f"Unknown checkpoint backend {value!r}; expected one of: {choices}"
        )
    return backend


def normalize_local_denylist(extra: object) -> frozenset[str]:
    """Merge caller-supplied delta-denylist prefixes into the built-in default.

    Accepts a comma/space separated string or any iterable of strings.
    """
    prefixes: set[str] = set(_LOCAL_DELTA_DENYLIST_DEFAULT)
    if extra:
        tokens = re.split(r"[,\s]+", extra) if isinstance(extra, str) else list(extra)
        for token in tokens:
            cleaned = str(token).strip()
            if cleaned:
                prefixes.add(cleaned)
    return frozenset(prefixes)


def _forces_delta_backend(name: object, cfg: dict) -> bool:
    """True when `name` matches a denylist prefix and must stay on Delta."""
    denylist = cfg.get("_local_delta_denylist")
    if denylist is None:
        denylist = _LOCAL_DELTA_DENYLIST_DEFAULT
    safe = str(name)
    return any(safe.startswith(prefix) for prefix in denylist)


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
    backend = normalize_checkpoint_backend(
        cfg.get("_checkpoint_backend", cfg.get("checkpoint_backend"))
    )
    # Hybrid: honor "local" except for the self-join denylist, forced to delta.
    forced_to_delta = backend == "local" and _forces_delta_backend(name, cfg)
    if backend == "local" and not forced_to_delta:
        started = time.time()
        cp = df.localCheckpoint(eager=True)
        # localCheckpoint drops table-qualifier metadata; re-wrap to mimic the
        # fresh schema a spark.table() read would give (parity with delta path).
        cp = cp.toDF(*cp.columns)
        elapsed = round(time.time() - started, 3)
        cfg.setdefault("_updated_checkpoint_timings", []).append(
            {"step": f"checkpoint:{name}", "elapsed_seconds": elapsed}
        )
        logger.info(
            "[updated checkpoint] %s: %.3fs (backend=local)", name, elapsed
        )
        return cp

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
    forced_note = " forced-from-local" if forced_to_delta else ""
    logger.info(
        "[updated checkpoint] %s: %.3fs (backend=delta%s, stats=off)",
        name,
        elapsed,
        forced_note,
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
