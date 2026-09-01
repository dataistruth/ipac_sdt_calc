"""Optimized, isolated orchestrator for Final Effective Percentage.

The production orchestrator is executed in a private updated-module namespace.
Only that private copy has selected globals replaced; the original
``output/orchestrator.py`` module and all production files remain unchanged.
"""

from __future__ import annotations

import contextvars
import functools
import time
from collections import defaultdict
from typing import Any, Callable

from .checkpoint import checkpoint, drop_checkpoints
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


def _run_with_timings(fn: Callable[..., Any], *args, **kwargs):
    events: list[dict[str, Any]] = []
    token = _ACTIVE_TIMINGS.set(events)
    started = time.time()
    try:
        result = fn(*args, **kwargs)
    finally:
        _ACTIVE_TIMINGS.reset(token)

    summary = _summarize(events)
    wall = round(time.time() - started, 3)
    print(f"[updated timing] wall={wall:.3f}s")
    for item in summary:
        print(
            f"[updated timing] {item['step']}: "
            f"{item['elapsed_seconds']:.3f}s "
            f"(calls={item['calls']})"
        )

    if isinstance(result, dict):
        result["timings"] = summary
        result["updated_wall_seconds"] = wall
        result["optimization_profile"] = {
            "checkpoint_backend": "uc_delta_stats_off",
            "logging_only_probe_actions_removed": 3,
        }
    return result


def run_modes(*args, **kwargs):
    """Run modes with optimized checkpoints and detailed step timings."""
    return _run_with_timings(_ORIGINAL_RUN_MODES, *args, **kwargs)


def run_final_effective_percentages(*args, **kwargs):
    """Updated entry point matching the production callable."""
    return _run_with_timings(_ORIGINAL_RUN_FINAL, *args, **kwargs)


# Compatibility with existing notebooks that use the historical name.
run_mode = run_final_effective_percentages

__all__ = [
    "run_final_effective_percentages",
    "run_mode",
    "run_modes",
]
