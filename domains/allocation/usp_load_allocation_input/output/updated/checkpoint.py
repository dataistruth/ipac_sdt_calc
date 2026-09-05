"""
checkpoint.py — lineage breaks for updated package.

Default: fast UC temp Delta — DROP + overwrite with Delta data-skipping stats
collection DISABLED (delta.dataSkippingNumIndexedCols=0 + stats.collect=false) and
uncompressed Parquet. Checkpoints are throwaway lineage breaks, so min/max column
stats add write cost with zero benefit. Set checkpoint_use_production=True to match
sdt_d Common_V2.core.checkpoint exactly (stats on first 32 cols).

Steps: pfic_snapshot, alloc_input, base_flowup, pfic_raw, pfic_flowup,
alloc_filtered (+ alloc_tagged if tagged).

Cfg (optional):
  checkpoint_use_production: use Common_V2.core.checkpoint (default: False)
  checkpoint_disable_stats: skip Delta stats collection on temp writes (default: True)
  checkpoint_compression: temp Delta parquet codec (default: uncompressed)
"""

from __future__ import annotations

import logging
import time
import uuid

from pyspark.sql import DataFrame, SparkSession

from Common_V2.core.helpers import table_prefix

logger = logging.getLogger(__name__)

_CHECKPOINT_MAX_RETRIES = 3
_CHECKPOINT_RETRY_DELAY = 5

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
# forced back to "delta". The denylist starts empty; if a local run crashes with
# UNRESOLVED_COLUMN at some checkpoint, add that name via the notebook widget
# (no redeploy) and, once known-stable, promote it into the default.
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
    import re

    prefixes: set[str] = set(_LOCAL_DELTA_DENYLIST_DEFAULT)
    if extra:
        tokens = re.split(r"[,\s]+", extra) if isinstance(extra, str) else list(extra)
        for token in tokens:
            cleaned = str(token).strip()
            if cleaned:
                prefixes.add(cleaned)
    return frozenset(prefixes)


def _resolve_backend(cfg: dict) -> str:
    return normalize_checkpoint_backend(
        cfg.get("_checkpoint_backend", cfg.get("checkpoint_backend"))
    )


def _forces_delta_backend(name: object, cfg: dict) -> bool:
    """True when `name` matches a denylist prefix and must stay on Delta."""
    denylist = cfg.get("_local_delta_denylist")
    if denylist is None:
        denylist = _LOCAL_DELTA_DENYLIST_DEFAULT
    safe = str(name)
    return any(safe.startswith(prefix) for prefix in denylist)

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
    all_steps = CHECKPOINT_STEPS | OPT_IN_CHECKPOINT_STEPS
    enabled = sorted(name for name in all_steps if should_checkpoint(cfg, name))
    if _use_production_checkpoint(cfg):
        print(f"[checkpoint] backend=Common_V2 (stats on) steps={enabled}")
        return
    comp = _checkpoint_compression(cfg)
    comp_note = f" compression={comp}" if comp else ""
    stats_note = " stats=off" if _disable_stats(cfg) else " stats=on"
    print(f"[checkpoint] backend=fast_delta{comp_note}{stats_note} steps={enabled}")


def _ensure_checkpoint_lists(cfg: dict) -> None:
    cfg.setdefault("_checkpoint_tables", [])


def _safe_name(name: str) -> str:
    """Sanitize a checkpoint label for use in an unquoted Delta table identifier."""
    cleaned = "".join(c if (c.isalnum() or c == "_") else "_" for c in str(name))
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "ckpt"


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


_DELTA_STATS_COLLECT_KEY = "spark.databricks.delta.stats.collect"


def _disable_stats(cfg: dict) -> bool:
    return bool(cfg.get("checkpoint_disable_stats", True))


def _write_delta_checkpoint(
    spark: SparkSession,
    df: DataFrame,
    fqn: str,
    cfg: dict,
) -> DataFrame:
    compression = _checkpoint_compression(cfg)
    optimize = bool(cfg.get("checkpoint_delta_optimize_write", False))
    disable_stats = _disable_stats(cfg)

    prev_stats: str | None = None
    stats_conf_set = False
    if disable_stats:
        try:
            prev_stats = spark.conf.get(_DELTA_STATS_COLLECT_KEY)
        except Exception:
            prev_stats = None
        try:
            spark.conf.set(_DELTA_STATS_COLLECT_KEY, "false")
            stats_conf_set = True
        except Exception:
            stats_conf_set = False

    try:
        for attempt in range(1, _CHECKPOINT_MAX_RETRIES + 1):
            try:
                spark.sql(f"DROP TABLE IF EXISTS {fqn}")
                writer = (
                    df.write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                )
                if disable_stats:
                    # Table property: index 0 columns → no min/max/null stats on write.
                    writer = writer.option("delta.dataSkippingNumIndexedCols", "0")
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
    finally:
        if stats_conf_set:
            try:
                if prev_stats is not None:
                    spark.conf.set(_DELTA_STATS_COLLECT_KEY, prev_stats)
                else:
                    spark.conf.unset(_DELTA_STATS_COLLECT_KEY)
            except Exception:
                pass


def _use_production_checkpoint(cfg: dict) -> bool:
    return bool(cfg.get("checkpoint_use_production", False))


def pipeline_checkpoint(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
) -> DataFrame:
    """Lineage break — local checkpoint by default, or fast/production Delta.

    Backend precedence: an explicit "local" backend wins (in-memory, no metastore
    commit) unless `name` is on the self-join delta-denylist; otherwise fall back
    to the production or fast-Delta path.
    """
    backend = _resolve_backend(cfg)
    forced_to_delta = backend == "local" and _forces_delta_backend(name, cfg)
    if backend == "local" and not forced_to_delta:
        started = time.time()
        cp = df.localCheckpoint(eager=True)
        # localCheckpoint drops table-qualifier metadata; re-wrap to mimic the
        # fresh schema a spark.table() read would give (parity with delta path).
        cp = cp.toDF(*cp.columns)
        logger.info(
            "[CHECKPOINT] %s: %.3fs (backend=local)",
            name,
            time.time() - started,
        )
        return cp

    if _use_production_checkpoint(cfg):
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
        fqn = f"{table_prefix(cfg)}._tmp_{_safe_name(name)}_{run_id}_{uniq}"
        cfg["_checkpoint_tables"].append(fqn)
        logger.info(f"[CHECKPOINT] production fallback write: {fqn}")
        prod_cfg = dict(cfg)
        prod_cfg["checkpoint_compression"] = "default"
        prod_cfg["write_compression"] = "default"
        prod_cfg["checkpoint_disable_stats"] = False  # match production (stats on)
        return _write_delta_checkpoint(spark, df, fqn, prod_cfg)


def drop_checkpoints(spark: SparkSession, cfg: dict) -> None:
    """Drop UC temp checkpoint tables tracked in cfg."""
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


def checkpoint(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    cfg: dict,
) -> DataFrame:
    """Fast UC temp Delta break: DROP + overwrite, stats off, uncompressed."""
    _ensure_checkpoint_lists(cfg)
    run_id = cfg.get("run_id", 0)
    uniq = uuid.uuid4().hex[:8]
    fqn = f"{table_prefix(cfg)}._tmp_{_safe_name(name)}_{run_id}_{uniq}"
    cfg["_checkpoint_tables"].append(fqn)
    comp = _checkpoint_compression(cfg)
    comp_note = f" compression={comp}" if comp else ""
    stats_note = " stats=off" if _disable_stats(cfg) else ""
    logger.info(f"[CHECKPOINT] fast delta write: {fqn}{comp_note}{stats_note}")
    return _write_delta_checkpoint(spark, df, fqn, cfg)
