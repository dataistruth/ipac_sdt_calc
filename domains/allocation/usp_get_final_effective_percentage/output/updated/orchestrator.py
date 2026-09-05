# updated-package sync marker v2 (2026-09-01): resync the ENTIRE
# output/updated/ folder as one set. Imports .checkpoint, .cost_pct_loader,
# .parent, .read_optimizations relatively; a partial upload breaks the import.
"""Optimized, isolated orchestrator for Final Effective Percentage.

The production orchestrator is executed in a private updated-module namespace.
Only that private copy has selected globals replaced; the original
``output/orchestrator.py`` module and all production files remain unchanged.
"""

from __future__ import annotations

import contextvars
import functools
import logging
import re
import time
from collections import defaultdict
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession

from .checkpoint import (
    DEFAULT_CHECKPOINT_BACKEND,
    DEFAULT_CHECKPOINT_PROFILE,
    checkpoint,
    drop_checkpoints,
    finish_checkpoint_run,
    normalize_checkpoint_backend,
    normalize_checkpoint_profile,
    normalize_coalesce,
    start_checkpoint_run,
)
try:
    from .cost_pct_loader import (
        OPTIMIZATION_PROFILE_MARKER as CPBT_OPTIMIZATION_PROFILE,
        build_cost_percentage_by_type as build_cost_percentage_by_type_optimized,
    )
except Exception as _cpbt_import_error:  # pragma: no cover - deploy safety net
    # A partial workspace sync (e.g. cost_pct_loader.py not uploaded) must not
    # crash the whole updated pipeline. Fall back to the base builder and make
    # the degraded state obvious in the logs and in the returned profile.
    _cpbt_error_text = str(_cpbt_import_error).replace("\n", " ")[:240]
    CPBT_OPTIMIZATION_PROFILE = (
        f"base_fallback ({type(_cpbt_import_error).__name__}: "
        f"{_cpbt_error_text})"
    )
    build_cost_percentage_by_type_optimized = None
    logging.getLogger(__name__).warning(
        "[updated] optimized build_cost_percentage_by_type unavailable (%s); "
        "using base builder. Re-sync output/updated/cost_pct_loader.py to "
        "enable the candidate-claim optimization.",
        _cpbt_import_error,
    )
from .parent import isolated_output_module
from .plan_profiler import (
    finish_plan_profile,
    plan_profile_report,
    start_plan_profile,
    track_plan,
)
from .read_optimizations import (
    build_lookthrough_input_modes14,
    load_line_items,
    load_quarters,
)

logger = logging.getLogger(__name__)

# Prod-copy revert: the optimized cost_pct builder is no longer swapped in, so
# report the base builder regardless of whether cost_pct_loader imported.
_ACTIVE_CPBT_PROFILE = "base (prod-copy revert)"

_base = isolated_output_module("orchestrator")
_ORIGINAL_RUN_MODES = _base.run_modes
_ORIGINAL_RUN_FINAL = _base.run_final_effective_percentages

_ACTIVE_TIMINGS: contextvars.ContextVar[list[dict[str, Any]] | None] = (
    contextvars.ContextVar("fep_updated_timings", default=None)
)
_LAST_RUN_PROFILE: dict[str, Any] = {}


def _record(name: str, elapsed: float) -> None:
    sink = _ACTIVE_TIMINGS.get()
    if sink is not None:
        sink.append(
            {
                "step": name,
                "elapsed_seconds": round(elapsed, 3),
            }
        )


def _timed(name: str, fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        started = time.time()
        try:
            return fn(*args, **kwargs)
        finally:
            _record(name, time.time() - started)

    return wrapped


def _checkpoint(spark, df, name, cfg):
    started = time.time()
    try:
        return checkpoint(spark, df, name, cfg)
    finally:
        _record(f"checkpoint:{name}", time.time() - started)


# --- New: post-builder checkpoints for hot (deep + slow) seams -------------
# Builders whose OUTPUT should get an extra lineage break beyond production's
# built-in seams. Populate from the plan-profile + timing logs: a builder
# qualifies when its plan is BOTH tall (high depth/delta) AND slow (high
# elapsed). Applied as a transparent post-builder wrapper -- prod calculation
# logic stays untouched. Override per-run via the ``extra_checkpoint_builders``
# kwarg (list, or comma-separated string from a notebook widget).
_DEFAULT_POST_BUILDER_CHECKPOINTS: frozenset[str] = frozenset()

_ACTIVE_POST_CHECKPOINTS: contextvars.ContextVar[frozenset[str]] = (
    contextvars.ContextVar("fep_post_checkpoints", default=frozenset())
)


def _extract_spark_cfg(args, kwargs):
    """Best-effort locate the SparkSession and cfg dict in builder args."""
    spark = None
    cfg = None
    for value in (*args, *kwargs.values()):
        if spark is None and isinstance(value, SparkSession):
            spark = value
        elif (
            cfg is None
            and isinstance(value, dict)
            and "catalog" in value
            and "schema" in value
        ):
            cfg = value
        if spark is not None and cfg is not None:
            break
    return spark, cfg


def _checkpoint_output(name: str, fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap ``fn`` so its returned DataFrame is checkpointed when selected.

    No-op unless ``name`` is in the active post-checkpoint set AND the return
    value is a DataFrame AND both spark/cfg can be located in the call args.
    """

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        result = fn(*args, **kwargs)
        if name in _ACTIVE_POST_CHECKPOINTS.get() and isinstance(
            result, DataFrame
        ):
            spark, cfg = _extract_spark_cfg(args, kwargs)
            if spark is not None and cfg is not None:
                return _checkpoint(spark, result, f"post_{name}", cfg)
        return result

    return wrapped


# --- Reverted to prod copy (2026-09-04) ------------------------------------
# The updated pipeline now mirrors production: it runs the BASE builders
# unchanged and only keeps (a) the fast Delta checkpoint writer (stats-off,
# uncompressed -- same result as prod, cheaper I/O) and (b) the transparent
# timing / plan-profiler instrumentation. The earlier builder swaps
# (cost_pct_loader / read_optimizations) are removed, and the "conservative"
# bypass of nde_post_miss_fused -- which replayed a 759-node plan into
# compute_effective_percentage_non_dated and slowed the run -- is gone
# (checkpoint.py now defaults to the "full" profile).
#
# Advised checkpoint: with the "full" profile every production seam is
# materialized, including nde_post_miss_fused / eff_nd_fused feeding
# compute_effective_percentage_non_dated.
#
# These replacements affect only the isolated module object loaded above.
_base._checkpoint = _checkpoint
_base._drop_checkpoints = drop_checkpoints
# Prod-copy: base read/cost builders are intentionally NOT swapped. Re-enable
# one at a time only after a measured, parity-verified win:
#   _base.load_line_items = load_line_items
#   _base.load_quarters = load_quarters
#   _base.build_lookthrough_input_modes14 = build_lookthrough_input_modes14
#   if build_cost_percentage_by_type_optimized is not None:
#       _base.build_cost_percentage_by_type = build_cost_percentage_by_type_optimized

_TIMED_GLOBALS = (
    "load_config",
    "build_cost_percentage_snapshot_modes123",
    "build_cost_percentage_snapshot_mode4",
    "build_mode1_704c_pe_book_allocations",
    "build_entity_partners",
    "build_cost_underlying_types",
    "build_entity_hierarchy",
    "build_asset_class_relationship",
    "build_underlyings_combined",
    "load_allocation_rules",
    "load_line_items",
    "load_book_effective_data",
    "load_quarters",
    "load_yearly_data",
    "filter_asset_class_underlyings",
    "build_underlyings_hlevel_ordered",
    "build_lookthrough_input_modes14",
    "build_footnote_lines",
    "build_footnote_book_effective",
    "build_temp_cost_percentage",
    "build_underlying_mod",
    "build_all_underlyings_ordered",
    "build_input_lines",
    "compute_amount_based_allocation",
    "build_non_dated_entities",
    "build_dated_entities",
    "build_footnote_underlyings_ordered",
    "build_footnote_input_lines",
    "build_footnote_dated_entities",
    "compute_form199a_effective_percentage",
    "build_state_allocation_input",
    "build_state_entities",
    "build_entity_underlyings",
    "load_transfers_adj_cost",
    "build_cost_percentage_by_type",
    "compute_missing_entities",
    "build_final_cost_percentage",
    "validate_cost_percentage_sum",
    "compute_minimum_quarter",
    "compute_effective_percentage_dated",
    "compute_effective_percentage_non_dated",
    "apply_plugging",
    "apply_type_id_update",
    "build_final_output",
    "_save_results",
)

for _name in _TIMED_GLOBALS:
    _fn = getattr(_base, _name, None)
    if callable(_fn):
        setattr(_base, _name, _timed(_name, _fn))

# Wrap the same builders with the plan-size profiler (outer of _timed, so it
# observes the real DataFrame args/return). ``track_plan`` is a transparent
# passthrough unless a plan sink is active (see plan_profiler), so this adds
# zero overhead outside profiling runs. Builders that don't return a DataFrame
# are recorded as no-ops automatically.
for _name in _TIMED_GLOBALS:
    _fn = getattr(_base, _name, None)
    if callable(_fn):
        setattr(_base, _name, track_plan(_fn))

# Outermost wrapper: checkpoint a builder's OUTPUT when it is selected as a
# hot (deep + slow) seam. Layered after track_plan so the profiler still sees
# the true pre-checkpoint plan; the extra materialization time is recorded
# separately as ``checkpoint:post_<builder>`` in the timings.
for _name in _TIMED_GLOBALS:
    _fn = getattr(_base, _name, None)
    if callable(_fn):
        setattr(_base, _name, _checkpoint_output(_name, _fn))


def _summarize(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: dict[str, float] = defaultdict(float)
    calls: dict[str, int] = defaultdict(int)
    for event in events:
        name = str(event["step"])
        totals[name] += float(event["elapsed_seconds"])
        calls[name] += 1
    return [
        {
            "step": name,
            "calls": calls[name],
            "elapsed_seconds": round(elapsed, 3),
        }
        for name, elapsed in sorted(
            totals.items(),
            key=lambda item: item[1],
            reverse=True,
        )
    ]


def _run_with_timings(
    fn: Callable[..., Any],
    *args,
    checkpoint_profile: str = DEFAULT_CHECKPOINT_PROFILE,
    checkpoint_backend: str = DEFAULT_CHECKPOINT_BACKEND,
    local_delta_denylist: tuple[str, ...] = (),
    local_delta_denylist_mode: str = "extend",
    checkpoint_coalesce: int | None = None,
    profile_plan: bool = False,
    plan_checkpoint_threshold: int = 30,
    extra_checkpoint_builders: tuple[str, ...] = (),
    **kwargs,
):
    profile = normalize_checkpoint_profile(checkpoint_profile)
    backend = normalize_checkpoint_backend(checkpoint_backend)
    coalesce = normalize_coalesce(checkpoint_coalesce)
    post_checkpoints = frozenset(_DEFAULT_POST_BUILDER_CHECKPOINTS).union(
        extra_checkpoint_builders
    )
    supplied_cfg = kwargs.get("cfg")
    if isinstance(supplied_cfg, dict):
        supplied_cfg["_checkpoint_profile"] = profile
        supplied_cfg["_checkpoint_backend"] = backend
        supplied_cfg["_checkpoint_coalesce"] = coalesce
        if profile_plan:
            supplied_cfg["profile_plan"] = True
            supplied_cfg["plan_checkpoint_threshold"] = plan_checkpoint_threshold
    events: list[dict[str, Any]] = []
    token = _ACTIVE_TIMINGS.set(events)
    post_token = _ACTIVE_POST_CHECKPOINTS.set(post_checkpoints)
    (
        profile_token,
        activity_token,
        backend_token,
        denylist_token,
        coalesce_token,
        checkpoint_activity,
    ) = start_checkpoint_run(
        profile,
        backend,
        local_denylist=local_delta_denylist,
        local_denylist_mode=local_delta_denylist_mode,
        coalesce=coalesce,
    )
    plan_token = None
    plan_records: list[dict[str, Any]] = []
    if profile_plan:
        plan_token, plan_records = start_plan_profile()
    started = time.time()
    try:
        result = fn(*args, **kwargs)
    finally:
        if plan_token is not None:
            finish_plan_profile(plan_token)
        finish_checkpoint_run(
            profile_token,
            activity_token,
            backend_token,
            denylist_token,
            coalesce_token,
        )
        _ACTIVE_POST_CHECKPOINTS.reset(post_token)
        _ACTIVE_TIMINGS.reset(token)

    summary = _summarize(events)
    wall = round(time.time() - started, 3)
    checkpoint_summary = {
        "profile": profile,
        "backend": backend,
        "written_count": len(checkpoint_activity["written"]),
        "bypassed_count": len(checkpoint_activity["bypassed"]),
        "written_names": [
            item["name"] for item in checkpoint_activity["written"]
        ],
        "bypassed_names": list(checkpoint_activity["bypassed"]),
        "post_builder_checkpoints": sorted(post_checkpoints),
        "local_delta_denylist": list(
            checkpoint_activity.get("local_delta_denylist", [])
        ),
        "forced_delta_names": [
            item["name"]
            for item in checkpoint_activity["written"]
            if item.get("forced_from_local")
        ],
        "coalesce": coalesce,
    }
    print(f"[updated timing] wall={wall:.3f}s")
    if post_checkpoints:
        print(
            "[updated checkpoints] post-builder="
            + ", ".join(sorted(post_checkpoints))
        )
    print(
        f"[updated checkpoints] profile={profile} backend={backend} "
        f"written={checkpoint_summary['written_count']} "
        f"bypassed={checkpoint_summary['bypassed_count']} "
        f"coalesce={coalesce if coalesce else 'off'}"
    )
    if backend == "local" and checkpoint_summary["forced_delta_names"]:
        print(
            "[updated checkpoints] forced-to-delta (self-join denylist): "
            + ", ".join(checkpoint_summary["forced_delta_names"])
        )
    for item in summary:
        print(
            f"[updated timing] {item['step']}: "
            f"{item['elapsed_seconds']:.3f}s "
            f"(calls={item['calls']})"
        )

    plan_profile: list[dict[str, Any]] = []
    if profile_plan:
        try:
            plan_profile = plan_profile_report(
                plan_records, plan_checkpoint_threshold
            )
        except Exception:
            logger.warning("[PLAN] report failed", exc_info=True)

    _LAST_RUN_PROFILE.clear()
    _LAST_RUN_PROFILE.update(
        {
            "updated_wall_seconds": wall,
            "timings": summary,
            "checkpoint_summary": checkpoint_summary,
            "cpbt_profile": _ACTIVE_CPBT_PROFILE,
            "plan_profile": plan_profile,
        }
    )
    if isinstance(result, dict):
        result["timings"] = summary
        result["updated_wall_seconds"] = wall
        result["checkpoint_summary"] = checkpoint_summary
        result["optimization_profile"] = {
            "checkpoint_backend": backend,
            "checkpoint_profile": profile,
            "cpbt_profile": _ACTIVE_CPBT_PROFILE,
            "builders": "prod-copy (no swaps)",
            "post_builder_checkpoints": sorted(post_checkpoints),
        }
        if profile_plan:
            result["plan_profile"] = plan_profile
    return result


def _pop_checkpoint_profile(kwargs: dict[str, Any]) -> str:
    if "CheckpointProfile" in kwargs:
        return kwargs.pop("CheckpointProfile")
    return kwargs.pop("checkpoint_profile", DEFAULT_CHECKPOINT_PROFILE)


def _pop_checkpoint_backend(kwargs: dict[str, Any]) -> str:
    if "CheckpointBackend" in kwargs:
        return kwargs.pop("CheckpointBackend")
    return kwargs.pop("checkpoint_backend", DEFAULT_CHECKPOINT_BACKEND)


def _pop_plan_flags(kwargs: dict[str, Any]) -> tuple[bool, int]:
    """Extract plan-profiler flags from caller kwargs.

    ``profile_plan`` (default ``False``) is the master on/off switch;
    ``plan_checkpoint_threshold`` (default ``30``) is the minimum node-count
    growth for a builder to be flagged as a checkpoint candidate.
    """
    profile_plan = bool(kwargs.pop("profile_plan", False))
    threshold = int(kwargs.pop("plan_checkpoint_threshold", 30))
    return profile_plan, threshold


def _pop_extra_checkpoints(kwargs: dict[str, Any]) -> tuple[str, ...]:
    """Extract the extra post-builder checkpoint seams from caller kwargs.

    Accepts a list/tuple of builder names, or a comma-separated string (handy
    for a notebook text widget). Empty / unset -> no extra checkpoints.
    """
    value = kwargs.pop("extra_checkpoint_builders", None)
    if not value:
        return ()
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    return tuple(str(part).strip() for part in value if str(part).strip())


def _pop_local_denylist(kwargs: dict[str, Any]) -> tuple[str, ...]:
    """Extract EXTRA local-backend delta-denylist prefixes from caller kwargs.

    These are added on top of the built-in denylist in checkpoint.py. Accepts a
    list/tuple or a comma/space-separated string (handy for a notebook widget).
    Only consulted when the checkpoint backend is "local".
    """
    value = kwargs.pop("LocalDeltaDenylist", None)
    if value is None:
        value = kwargs.pop("local_delta_denylist", None)
    else:
        kwargs.pop("local_delta_denylist", None)
    if not value:
        return ()
    if isinstance(value, str):
        parts = re.split(r"[,\s]+", value)
        return tuple(part.strip() for part in parts if part.strip())
    return tuple(str(part).strip() for part in value if str(part).strip())


def _pop_local_denylist_mode(kwargs: dict[str, Any]) -> str:
    """Extract the denylist merge mode ("extend" or "replace") from kwargs."""
    value = kwargs.pop("LocalDeltaDenylistMode", None)
    if value is None:
        value = kwargs.pop("local_delta_denylist_mode", None)
    else:
        kwargs.pop("local_delta_denylist_mode", None)
    return str(value or "extend").strip().lower()


def _pop_checkpoint_coalesce(kwargs: dict[str, Any]) -> int | None:
    """Extract the Delta checkpoint-write coalesce count from kwargs."""
    value = kwargs.pop("CheckpointCoalesce", None)
    if value is None:
        value = kwargs.pop("checkpoint_coalesce", None)
    else:
        kwargs.pop("checkpoint_coalesce", None)
    return normalize_coalesce(value)


def run_modes(*args, **kwargs):
    """Run modes with optimized checkpoints and detailed step timings."""
    profile = _pop_checkpoint_profile(kwargs)
    backend = _pop_checkpoint_backend(kwargs)
    local_denylist = _pop_local_denylist(kwargs)
    local_denylist_mode = _pop_local_denylist_mode(kwargs)
    checkpoint_coalesce = _pop_checkpoint_coalesce(kwargs)
    profile_plan, plan_threshold = _pop_plan_flags(kwargs)
    extra_checkpoints = _pop_extra_checkpoints(kwargs)
    return _run_with_timings(
        _ORIGINAL_RUN_MODES,
        *args,
        checkpoint_profile=profile,
        checkpoint_backend=backend,
        local_delta_denylist=local_denylist,
        local_delta_denylist_mode=local_denylist_mode,
        checkpoint_coalesce=checkpoint_coalesce,
        profile_plan=profile_plan,
        plan_checkpoint_threshold=plan_threshold,
        extra_checkpoint_builders=extra_checkpoints,
        **kwargs,
    )


def run_final_effective_percentages(*args, **kwargs):
    """Updated entry point matching the production callable."""
    profile = _pop_checkpoint_profile(kwargs)
    backend = _pop_checkpoint_backend(kwargs)
    local_denylist = _pop_local_denylist(kwargs)
    local_denylist_mode = _pop_local_denylist_mode(kwargs)
    checkpoint_coalesce = _pop_checkpoint_coalesce(kwargs)
    profile_plan, plan_threshold = _pop_plan_flags(kwargs)
    extra_checkpoints = _pop_extra_checkpoints(kwargs)
    return _run_with_timings(
        _ORIGINAL_RUN_FINAL,
        *args,
        checkpoint_profile=profile,
        checkpoint_backend=backend,
        local_delta_denylist=local_denylist,
        local_delta_denylist_mode=local_denylist_mode,
        checkpoint_coalesce=checkpoint_coalesce,
        profile_plan=profile_plan,
        plan_checkpoint_threshold=plan_threshold,
        extra_checkpoint_builders=extra_checkpoints,
        **kwargs,
    )


def get_last_run_profile() -> dict[str, Any]:
    """Return benchmark metadata even when the result storer returns a string."""
    return {
        **_LAST_RUN_PROFILE,
        "timings": list(_LAST_RUN_PROFILE.get("timings", [])),
        "checkpoint_summary": dict(
            _LAST_RUN_PROFILE.get("checkpoint_summary", {})
        ),
        "plan_profile": list(_LAST_RUN_PROFILE.get("plan_profile", [])),
    }


# Compatibility with existing notebooks that use the historical name.
run_mode = run_final_effective_percentages

__all__ = [
    "run_final_effective_percentages",
    "run_mode",
    "run_modes",
    "get_last_run_profile",
]
