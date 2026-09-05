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
# WHY A HYBRID: localCheckpoint is materially faster than the Delta round-trip
# (no metastore commit, no small-file write/read), but a few builders (starting
# with compute_missing_entities) self-join a checkpointed DataFrame against
# something derived from it. The Delta round-trip returns a *catalog relation*,
# which Spark re-resolves with fresh attribute IDs so the self-join
# disambiguates. localCheckpoint returns a LogicalRDD that CANNOT be re-resolved
# that way, so self-join relation-dedup fails with UNRESOLVED_COLUMN (it
# "suggests" the very column it claims is missing). toDF(*columns) does not help
# -- the issue is attribute-ID dedup, not column names.
#
# HYBRID STRATEGY: run backend="local" for the many safe, expensive seams, but
# force "delta" for the small denylist of seams whose result later feeds a
# self-join (see _LOCAL_DELTA_DENYLIST_DEFAULT). This keeps the big local
# speedups while preserving correctness. The denylist is prefix-matched (names
# carry a mode suffix, e.g. de_pre_cpbt_m1) and can be extended at runtime via
# start_checkpoint_run(local_denylist=[...]) after a validation run confirms it.
DEFAULT_CHECKPOINT_BACKEND = "delta"
# "parquet" = write plain Parquet to a volume/DBFS path and read it back. The
# read-back is a fresh, re-resolvable file relation (so self-joins work, unlike
# local) but it skips Delta's transaction-log commit + metastore registration
# (the slow part), so it's the "faster-than-delta, still-correct" backend for
# the self-join denylist. Requires a volume path (else it falls back to delta).
_VALID_BACKENDS = frozenset({"delta", "local", "parquet"})
_ACTIVE_BACKEND: ContextVar[str] = ContextVar(
    "fep_checkpoint_backend",
    default=DEFAULT_CHECKPOINT_BACKEND,
)

# Checkpoint-name PREFIXES that must stay on Delta even when backend="local",
# because their result is self-joined downstream (localCheckpoint -> LogicalRDD
# -> UNRESOLVED_COLUMN on self-join relation-dedup).
#   * nde_pre_cpbt / de_pre_cpbt : unioned into the non_dated/dated inputs of
#     compute_missing_entities, which self-joins them (crash at the checkpoint
#     right after txfr_adj_fused when left on local). CONFIRMED-critical.
#
# SPEED-OVER-SAFETY DEFAULT (requested 2026-09-05): only the two pre_cpbt seams
# are forced to Delta; nde_post_miss / de_post_miss / final_cost_pct run on
# local to chase the ~104s number. CAVEAT: localCheckpoint self-join resolution
# is NON-DETERMINISTIC -- this exact set passed once (~104s) but a rerun crashed
# with UNRESOLVED_COLUMN(`DealID`) at the effective_calc + plugging self-join
# (aliased T./Q. on DealID/Quarter/Tag/TypeID), whose input derives from
# final_cost_pct. If that crash reappears, add "final_cost_pct" (and, if still
# unstable, "nde_post_miss"/"de_post_miss") back via the LocalDeltaDenylist
# notebook widget -- no redeploy needed.
_LOCAL_DELTA_DENYLIST_DEFAULT = frozenset(
    {
        "nde_pre_cpbt",
        "de_pre_cpbt",
    }
)
_ACTIVE_LOCAL_DENYLIST: ContextVar[frozenset[str]] = ContextVar(
    "fep_local_delta_denylist",
    default=_LOCAL_DELTA_DENYLIST_DEFAULT,
)


def normalize_checkpoint_backend(value: object) -> str:
    backend = str(value or DEFAULT_CHECKPOINT_BACKEND).strip().lower()
    if backend not in _VALID_BACKENDS:
        choices = ", ".join(sorted(_VALID_BACKENDS))
        raise ValueError(
            f"Unknown checkpoint backend {value!r}; expected one of: {choices}"
        )
    return backend


def normalize_local_denylist(extra: object, mode: object = "extend") -> frozenset[str]:
    """Resolve the effective local-backend delta-denylist prefixes.

    `extra` is a comma/space separated string or any iterable of strings.
    `mode`:
      * "extend"  (default) -> built-in defaults UNION `extra`.
      * "replace" -> use ONLY `extra` (lets a validation run TRIM the preemptive
        built-ins down to the confirmed-critical seams). Guarded: an empty
        `extra` in replace mode falls back to the defaults so a blank widget
        never silently drops every self-join guard.
    """
    normalized_mode = str(mode or "extend").strip().lower()
    supplied: set[str] = set()
    if extra:
        tokens = re.split(r"[,\s]+", extra) if isinstance(extra, str) else list(extra)
        supplied = {str(token).strip() for token in tokens if str(token).strip()}
    if normalized_mode == "replace":
        return frozenset(supplied) if supplied else _LOCAL_DELTA_DENYLIST_DEFAULT
    return frozenset(set(_LOCAL_DELTA_DENYLIST_DEFAULT) | supplied)


def _forces_delta_backend(name: object) -> bool:
    """True when `name` matches a denylist prefix and must stay on Delta."""
    safe = str(name)
    return any(safe.startswith(prefix) for prefix in _ACTIVE_LOCAL_DENYLIST.get())


# Optional coalesce applied to the Delta checkpoint write. On this small dataset
# (and with shuffle.partitions capped at 4) a full-parallel write still emits
# several tiny files per checkpoint; coalescing to a handful reduces the file
# count and the per-commit overhead. It targets the Delta write path only, so in
# backend="local" mode it applies exactly to the forced-delta self-join seams
# (nde_pre_cpbt / de_pre_cpbt). None = leave partitioning as-is (current
# behavior). coalesce() is a narrow op (no shuffle); keep the value >= 1.
DEFAULT_CHECKPOINT_COALESCE: int | None = None
_ACTIVE_COALESCE: ContextVar[int | None] = ContextVar(
    "fep_checkpoint_coalesce",
    default=DEFAULT_CHECKPOINT_COALESCE,
)


def normalize_coalesce(value: object) -> int | None:
    """Coerce a coalesce setting to a positive int, or None to disable it."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in ("", "0", "none", "off", "false"):
        return None
    try:
        count = int(float(text))
    except (TypeError, ValueError):
        raise ValueError(
            f"Invalid checkpoint coalesce {value!r}; expected a positive integer"
        )
    return count if count > 0 else None


# Base directory for the parquet backend. When set (and backend forces a
# denylist seam off "local"), the forced write goes to parquet-on-volume
# instead of delta. Blank/None disables it (forced seams fall back to delta).
DEFAULT_CHECKPOINT_VOLUME_PATH: str | None = None
_ACTIVE_VOLUME_PATH: ContextVar[str | None] = ContextVar(
    "fep_checkpoint_volume_path",
    default=DEFAULT_CHECKPOINT_VOLUME_PATH,
)


def normalize_volume_path(value: object) -> str | None:
    """Coerce a volume/DBFS base path, or None to disable the parquet backend."""
    if not value:
        return None
    text = str(value).strip()
    if text.lower() in ("", "none", "off", "false"):
        return None
    return text.rstrip("/") or None


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


def start_checkpoint_run(
    profile: object,
    backend: object = None,
    local_denylist: object = None,
    local_denylist_mode: object = "extend",
    coalesce: object = None,
    volume_path: object = None,
):
    """Activate a checkpoint profile + backend and collect activity.

    `local_denylist` (str or iterable) tunes the set of checkpoint prefixes that
    stay on Delta even when backend="local"; `local_denylist_mode` selects
    "extend" (add to built-ins) or "replace" (use only the supplied set).
    `coalesce` (int or None) coalesces each Delta checkpoint write to that many
    output files (None = leave as-is). Use it to tune the hybrid after a
    validation run without a redeploy.
    """
    normalized = normalize_checkpoint_profile(profile)
    normalized_backend = normalize_checkpoint_backend(backend)
    denylist = normalize_local_denylist(local_denylist, local_denylist_mode)
    normalized_coalesce = normalize_coalesce(coalesce)
    normalized_volume = normalize_volume_path(volume_path)
    activity: dict[str, Any] = {
        "profile": normalized,
        "backend": normalized_backend,
        "local_delta_denylist": sorted(denylist),
        "coalesce": normalized_coalesce,
        "volume_path": normalized_volume,
        "written": [],
        "bypassed": [],
    }
    return (
        _ACTIVE_PROFILE.set(normalized),
        _ACTIVE_ACTIVITY.set(activity),
        _ACTIVE_BACKEND.set(normalized_backend),
        _ACTIVE_LOCAL_DENYLIST.set(denylist),
        _ACTIVE_COALESCE.set(normalized_coalesce),
        _ACTIVE_VOLUME_PATH.set(normalized_volume),
        activity,
    )


def finish_checkpoint_run(
    profile_token,
    activity_token,
    backend_token=None,
    denylist_token=None,
    coalesce_token=None,
    volume_token=None,
) -> None:
    _ACTIVE_ACTIVITY.reset(activity_token)
    _ACTIVE_PROFILE.reset(profile_token)
    if backend_token is not None:
        _ACTIVE_BACKEND.reset(backend_token)
    if denylist_token is not None:
        _ACTIVE_LOCAL_DENYLIST.reset(denylist_token)
    if coalesce_token is not None:
        _ACTIVE_COALESCE.reset(coalesce_token)
    if volume_token is not None:
        _ACTIVE_VOLUME_PATH.reset(volume_token)


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

    # Optional coalesce to shrink file-count / commit overhead on the write.
    # Resolved up-front so both the delta and parquet write paths share it.
    coalesce = normalize_coalesce(
        cfg.get("_checkpoint_coalesce", _ACTIVE_COALESCE.get())
    )
    volume_base = normalize_volume_path(
        cfg.get("_checkpoint_volume_path", _ACTIVE_VOLUME_PATH.get())
    )

    # Hybrid: honor "local" everywhere except the self-join denylist, which is
    # forced onto a durable, re-resolvable relation for correctness. Prefer the
    # cheaper parquet-on-volume round-trip when a volume path is configured;
    # otherwise fall back to the full Delta round-trip.
    effective_backend = backend
    forced_off_local = backend == "local" and _forces_delta_backend(name)
    if forced_off_local:
        effective_backend = "parquet" if volume_base else "delta"
    elif backend == "parquet":
        # Explicit global parquet backend still needs a volume; else use delta.
        effective_backend = "parquet" if volume_base else "delta"

    # --- parquet backend: write plain Parquet to a volume path + read back. ---
    # Skips the Delta commit/metastore step (faster) while still returning a
    # fresh file relation that Spark can re-resolve for self-joins.
    if effective_backend == "parquet" and volume_base:
        started = time.time()
        run_id = _safe_name(cfg.get("run_id", "0"))
        path = f"{volume_base}/_tmp_fep_updated_{_safe_name(name)}_{run_id}"
        checkpoint_paths = cfg.setdefault("_checkpoint_paths", [])
        if path not in checkpoint_paths:
            checkpoint_paths.append(path)
        writer_source = df.coalesce(coalesce) if coalesce else df
        writer_source.write.mode("overwrite").parquet(path)
        cp = spark.read.parquet(path)
        elapsed = time.time() - started
        timing = {
            "name": name,
            "elapsed_seconds": round(elapsed, 3),
            "backend": "parquet",
        }
        if forced_off_local:
            timing["forced_from_local"] = True
        if coalesce:
            timing["coalesce"] = coalesce
        cfg.setdefault("_updated_checkpoint_timings", []).append(dict(timing))
        if activity is not None:
            activity["written"].append(dict(timing))
        forced_note = " forced-from-local" if forced_off_local else ""
        coalesce_note = f" coalesce={coalesce}" if coalesce else ""
        print(
            f"[updated checkpoint] {name}: {elapsed:.3f}s "
            f"(backend=parquet-volume{forced_note}{coalesce_note}, profile={profile})"
        )
        return cp

    # --- local backend: in-memory lineage break, no metastore Delta commit ---
    if effective_backend == "local":
        started = time.time()
        cp = df.localCheckpoint(eager=True)
        # localCheckpoint strips table-qualifier metadata from columns; re-wrap
        # to mimic the fresh schema a spark.table() read would give (parity with
        # the delta path, and required by aliased downstream joins).
        cp = cp.toDF(*cp.columns)
        elapsed = time.time() - started
        cfg.setdefault("_updated_checkpoint_timings", []).append(
            {"name": name, "elapsed_seconds": round(elapsed, 3), "backend": "local"}
        )
        if activity is not None:
            activity["written"].append(
                {"name": name, "elapsed_seconds": round(elapsed, 3), "backend": "local"}
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

    writer_source = df.coalesce(coalesce) if coalesce else df

    existed, previous = _get_conf(spark, _STATS_KEY)
    try:
        spark.conf.set(_STATS_KEY, "false")
        (
            writer_source.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .option("compression", "uncompressed")
            .saveAsTable(fqn)
        )
    finally:
        _restore_conf(spark, _STATS_KEY, existed, previous)

    elapsed = time.time() - started
    timing = {"name": name, "elapsed_seconds": round(elapsed, 3), "backend": "delta"}
    if forced_off_local:
        timing["forced_from_local"] = True
    if coalesce:
        timing["coalesce"] = coalesce
    cfg.setdefault("_updated_checkpoint_timings", []).append(dict(timing))
    if activity is not None:
        activity["written"].append(dict(timing))
    forced_note = " forced-from-local" if forced_off_local else ""
    coalesce_note = f" coalesce={coalesce}" if coalesce else ""
    print(
        f"[updated checkpoint] {name}: {elapsed:.3f}s "
        f"(backend=delta{forced_note}{coalesce_note}, stats=off, profile={profile})"
    )
    return spark.table(fqn)


def drop_checkpoints(spark: SparkSession, cfg: dict) -> None:
    """Drop only temporary tables registered by this run.

    Parquet-backend paths are intentionally NOT deleted here: they are written
    run-scoped with mode("overwrite"), so each run reuses (and overwrites) the
    same location. Skipping the recursive volume delete avoids adding extra
    wall time to the run.
    """
    if cfg.get("_skip_cleanup"):
        return
    for fqn in dict.fromkeys(cfg.get("_checkpoint_tables", [])):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:
            logger.warning("Failed to drop checkpoint %s", fqn, exc_info=True)
    cfg["_checkpoint_tables"] = []
