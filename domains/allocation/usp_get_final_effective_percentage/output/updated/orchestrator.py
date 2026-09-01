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
import time
from collections import defaultdict
from typing import Any, Callable

from .checkpoint import (
    DEFAULT_CHECKPOINT_PROFILE,
    checkpoint,
    drop_checkpoints,
    finish_checkpoint_run,
    normalize_checkpoint_profile,
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
from .read_optimizations import (
    build_lookthrough_input_modes14,
    load_line_items,
    load_quarters,
)

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


# These replacements affect only the isolated module object loaded above.
_base._checkpoint = _checkpoint
_base._drop_checkpoints = drop_checkpoints
_base.load_line_items = load_line_items
_base.load_quarters = load_quarters
_base.build_lookthrough_input_modes14 = build_lookthrough_input_modes14
if build_cost_percentage_by_type_optimized is not None:
    _base.build_cost_percentage_by_type = build_cost_percentage_by_type_optimized

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
    **kwargs,
):
    profile = normalize_checkpoint_profile(checkpoint_profile)
    supplied_cfg = kwargs.get("cfg")
    if isinstance(supplied_cfg, dict):
        supplied_cfg["_checkpoint_profile"] = profile
    events: list[dict[str, Any]] = []
    token = _ACTIVE_TIMINGS.set(events)
    profile_token, activity_token, checkpoint_activity = start_checkpoint_run(
        profile
    )
    started = time.time()
    try:
        result = fn(*args, **kwargs)
    finally:
        finish_checkpoint_run(profile_token, activity_token)
        _ACTIVE_TIMINGS.reset(token)

    summary = _summarize(events)
    wall = round(time.time() - started, 3)
    checkpoint_summary = {
        "profile": profile,
        "written_count": len(checkpoint_activity["written"]),
        "bypassed_count": len(checkpoint_activity["bypassed"]),
        "written_names": [
            item["name"] for item in checkpoint_activity["written"]
        ],
        "bypassed_names": list(checkpoint_activity["bypassed"]),
    }
    print(f"[updated timing] wall={wall:.3f}s")
    print(
        f"[updated checkpoints] profile={profile} "
        f"written={checkpoint_summary['written_count']} "
        f"bypassed={checkpoint_summary['bypassed_count']}"
    )
    for item in summary:
        print(
            f"[updated timing] {item['step']}: "
            f"{item['elapsed_seconds']:.3f}s "
            f"(calls={item['calls']})"
        )

    _LAST_RUN_PROFILE.clear()
    _LAST_RUN_PROFILE.update(
        {
            "updated_wall_seconds": wall,
            "timings": summary,
            "checkpoint_summary": checkpoint_summary,
            "cpbt_profile": CPBT_OPTIMIZATION_PROFILE,
        }
    )
    if isinstance(result, dict):
        result["timings"] = summary
        result["updated_wall_seconds"] = wall
        result["checkpoint_summary"] = checkpoint_summary
        result["optimization_profile"] = {
            "checkpoint_backend": "uc_delta_stats_off",
            "checkpoint_profile": profile,
            "cpbt_profile": CPBT_OPTIMIZATION_PROFILE,
            "logging_only_probe_actions_removed": 3,
        }
    return result


def _pop_checkpoint_profile(kwargs: dict[str, Any]) -> str:
    if "CheckpointProfile" in kwargs:
        return kwargs.pop("CheckpointProfile")
    return kwargs.pop("checkpoint_profile", DEFAULT_CHECKPOINT_PROFILE)


def run_modes(*args, **kwargs):
    """Run modes with optimized checkpoints and detailed step timings."""
    profile = _pop_checkpoint_profile(kwargs)
    return _run_with_timings(
        _ORIGINAL_RUN_MODES,
        *args,
        checkpoint_profile=profile,
        **kwargs,
    )


def run_final_effective_percentages(*args, **kwargs):
    """Updated entry point matching the production callable."""
    profile = _pop_checkpoint_profile(kwargs)
    return _run_with_timings(
        _ORIGINAL_RUN_FINAL,
        *args,
        checkpoint_profile=profile,
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
    }


# Compatibility with existing notebooks that use the historical name.
run_mode = run_final_effective_percentages

__all__ = [
    "run_final_effective_percentages",
    "run_mode",
    "run_modes",
    "get_last_run_profile",
]
