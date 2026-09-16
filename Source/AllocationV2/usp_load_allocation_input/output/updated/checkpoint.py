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
_LOCAL_DELTA_DENYLIST_DEFAULT: frozenset[str] = frozenset()
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


def normalize_local_denylist(extra: object, mode: object = "extend") -> frozenset[str]:
    """Resolve prefixes that must stay on Delta even when backend is local.

    `localCheckpoint` cannot re-resolve self-joins (UNRESOLVED_COLUMN). Names
    matching these prefixes are forced back to a Delta round-trip.

    `extra` is a comma/space-separated string or any iterable of strings.
    `mode`:
      * "extend" (default) -> built-in defaults UNION `extra`
      * "replace" -> use only `extra`; empty extra falls back to defaults so a
        blank widget never drops every self-join guard
    """
    normalized_mode = str(mode or "extend").strip().lower()
    supplied: set[str] = set()
    if extra:
        tokens = re.split(r"[,\s]+", extra) if isinstance(extra, str) else list(extra)
        supplied = {str(token).strip() for token in tokens if str(token).strip()}
    if normalized_mode == "replace":
        return frozenset(supplied) if supplied else _LOCAL_DELTA_DENYLIST_DEFAULT
    return frozenset(set(_LOCAL_DELTA_DENYLIST_DEFAULT) | supplied)


def _forces_delta_backend(name: object, cfg: dict) -> bool:
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
    if cfg.get("_local_delta_denylist") is None:
        cfg["_local_delta_denylist"] = normalize_local_denylist(
            cfg.get("local_denylist")
        )
    forced_to_delta = backend == "local" and _forces_delta_backend(name, cfg)
    started = time.time()

    if backend == "local" and not forced_to_delta:
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
        "(backend=delta%s, column_stats=off)",
        name,
        elapsed,
        " forced-from-local" if forced_to_delta else "",
    )
    return spark.table(fqn)


def pipeline_checkpoint(spark, df, name: str, cfg: dict):
    """Public lineage-break entry used by the orchestrator. Same as checkpoint()."""
    return checkpoint(spark, df, name, cfg)


def use_inner_base_flowup_local_checkpoint(cfg: dict) -> bool:
    """True when inner 7a breaks should follow the run's local backend."""
    flag = cfg.get("checkpoint_inner_base_flowup_local")
    if flag is not None:
        return bool(flag)
    backend = normalize_checkpoint_backend(
        cfg.get("_checkpoint_backend", cfg.get("checkpoint_backend"))
    )
    return backend == "local"


def inner_base_flowup_checkpoint(spark, df, cfg: dict, label: str):
    """Inner PFIC 7a lineage break (post-reclass / post-zero).

    Signature used by ``ai_pfic_flowup_service._flowup_checkpoint``.
    """
    name = f"base_flowup_{_safe_name(label)}"
    return checkpoint(spark, df, name, cfg)


def _use_production_checkpoint(cfg: dict) -> bool:
    """Updated packages default to stats-off Delta, not Common_V2."""
    return bool(cfg.get("checkpoint_use_production", False))


def checkpoint_production(spark, df, name: str, cfg: dict):
    """Compatibility alias. Still uses stats-off Delta, not Common_V2."""
    return checkpoint(spark, df, name, cfg)


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
