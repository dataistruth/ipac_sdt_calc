# updated-package sync marker v2 (2026-09-01): resync the ENTIRE
# output/updated/ folder as one set.
"""Fast UC Delta checkpoints for the optimized FEP pipeline."""

from __future__ import annotations

import logging
import re
import time
from contextvars import ContextVar
from typing import Any

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_STATS_KEY = "spark.databricks.delta.stats.collect"
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_]")
# Prod-copy default: "full" materializes every production seam (no bypass),
# which keeps the advised checkpoint at nde_post_miss_fused / eff_nd_fused that
# feeds compute_effective_percentage_non_dated. The old "conservative" default
# bypassed nde_post_miss_fused, forcing a 759-node plan to replay into the
# non-dated effective-percentage stage and regressing the run vs production.
DEFAULT_CHECKPOINT_PROFILE = "full"

_CONSERVATIVE_BYPASSES = frozenset(
    {
        "underlyings_common",
        "nde_post_miss_fused",
    }
)
CHECKPOINT_PROFILES = {
    "full": frozenset(),
    "conservative": _CONSERVATIVE_BYPASSES,
    "balanced": _CONSERVATIVE_BYPASSES
    | {
        "all_ent_pre_tag_m0",
        "eff_dated_s6_m0",
    },
}

_ACTIVE_PROFILE: ContextVar[str] = ContextVar(
    "fep_checkpoint_profile",
    default=DEFAULT_CHECKPOINT_PROFILE,
)
_ACTIVE_ACTIVITY: ContextVar[dict[str, Any] | None] = ContextVar(
    "fep_checkpoint_activity",
    default=None,
)

# Checkpoint backend: "delta" (durable UC Delta table, default) or "local"
# (df.localCheckpoint(eager=True), no metastore commit).
#
# !! KNOWN-INCOMPATIBLE: "local" is EXPERIMENTAL and fails on this pipeline. !!
# Several builders (e.g. compute_missing_entities) self-join a checkpointed
# DataFrame against something derived from it. The Delta round-trip returns a
# catalog relation, which Spark re-resolves with fresh attribute IDs so the
# self-join disambiguates. localCheckpoint returns a LogicalRDD that CANNOT be
# re-resolved that way, so self-join relation-dedup fails with
# UNRESOLVED_COLUMN (it "suggests" the very column it claims is missing). The
# toDF(*columns) re-wrap does not help -- the issue is attribute-ID dedup, not
# column names. This is why production ships _USE_LOCAL_CHECKPOINT = False.
# Kept only to document the failure mode; leave the default on "delta".
DEFAULT_CHECKPOINT_BACKEND = "delta"
_VALID_BACKENDS = frozenset({"delta", "local"})
_ACTIVE_BACKEND: ContextVar[str] = ContextVar(
    "fep_checkpoint_backend",
    default=DEFAULT_CHECKPOINT_BACKEND,
)


def normalize_checkpoint_backend(value: object) -> str:
    backend = str(value or DEFAULT_CHECKPOINT_BACKEND).strip().lower()
    if backend not in _VALID_BACKENDS:
        choices = ", ".join(sorted(_VALID_BACKENDS))
        raise ValueError(
            f"Unknown checkpoint backend {value!r}; expected one of: {choices}"
        )
    return backend


def _safe_name(value: object) -> str:
    return _SAFE_NAME.sub("_", str(value))


def normalize_checkpoint_profile(value: object) -> str:
    profile = str(value or DEFAULT_CHECKPOINT_PROFILE).strip().lower()
    if profile not in CHECKPOINT_PROFILES:
        choices = ", ".join(CHECKPOINT_PROFILES)
        raise ValueError(
            f"Unknown checkpoint profile {value!r}; expected one of: {choices}"
        )
    return profile


def start_checkpoint_run(profile: object, backend: object = None):
    """Activate a checkpoint profile + backend and collect activity."""
    normalized = normalize_checkpoint_profile(profile)
    normalized_backend = normalize_checkpoint_backend(backend)
    activity: dict[str, Any] = {
        "profile": normalized,
        "backend": normalized_backend,
        "written": [],
        "bypassed": [],
    }
    return (
        _ACTIVE_PROFILE.set(normalized),
        _ACTIVE_ACTIVITY.set(activity),
        _ACTIVE_BACKEND.set(normalized_backend),
        activity,
    )


def finish_checkpoint_run(profile_token, activity_token, backend_token=None) -> None:
    _ACTIVE_ACTIVITY.reset(activity_token)
    _ACTIVE_PROFILE.reset(profile_token)
    if backend_token is not None:
        _ACTIVE_BACKEND.reset(backend_token)


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
    profile = normalize_checkpoint_profile(
        cfg.get("_checkpoint_profile", _ACTIVE_PROFILE.get())
    )
    cfg["_checkpoint_profile"] = profile
    activity = _ACTIVE_ACTIVITY.get()

    if name in CHECKPOINT_PROFILES[profile]:
        cfg.setdefault("_updated_checkpoint_bypasses", []).append(name)
        if activity is not None:
            activity["bypassed"].append(name)
        print(f"[updated checkpoint] {name}: bypassed (profile={profile})")
        return df

    backend = normalize_checkpoint_backend(
        cfg.get("_checkpoint_backend", _ACTIVE_BACKEND.get())
    )
    cfg["_checkpoint_backend"] = backend

    # --- local backend: in-memory lineage break, no metastore Delta commit ---
    if backend == "local":
        started = time.time()
        cp = df.localCheckpoint(eager=True)
        # localCheckpoint strips table-qualifier metadata from columns; re-wrap
        # to mimic the fresh schema a spark.table() read would give (parity with
        # the delta path, and required by aliased downstream joins).
        cp = cp.toDF(*cp.columns)
        elapsed = time.time() - started
        cfg.setdefault("_updated_checkpoint_timings", []).append(
            {"name": name, "elapsed_seconds": round(elapsed, 3)}
        )
        if activity is not None:
            activity["written"].append(
                {"name": name, "elapsed_seconds": round(elapsed, 3)}
            )
        print(
            f"[updated checkpoint] {name}: {elapsed:.3f}s "
            f"(backend=local, profile={profile})"
        )
        return cp

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
    if activity is not None:
        activity["written"].append(
            {"name": name, "elapsed_seconds": round(elapsed, 3)}
        )
    print(
        f"[updated checkpoint] {name}: {elapsed:.3f}s "
        f"(backend=delta, stats=off, profile={profile})"
    )
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
