"""Conservative V2 wrapper around the unchanged production FEP orchestrator."""

from __future__ import annotations

import contextvars
import functools
import inspect
import logging
import re
import time
from collections import defaultdict
from typing import Any, Callable

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2,
    initialize_checkpoint_V2,
    normalize_checkpoint_mode,
)

from .parent import isolated_output_module
from .plan_profiler import (
    finish_action_profile,
    finish_checkpoint_plan_profile,
    finish_plan_profile,
    plan_profile_report,
    start_action_profile,
    start_checkpoint_plan_profile,
    start_plan_profile,
    track_checkpoint_plan,
    track_plan,
)

logger = logging.getLogger(__name__)
_base = isolated_output_module("orchestrator")
_PRODUCTION_RUN_MODES = _base.run_modes
_PRODUCTION_RUN_FINAL = _base.run_final_effective_percentages

_ACTIVE_TIMINGS: contextvars.ContextVar[list[dict[str, Any]] | None] = (
    contextvars.ContextVar("fep_output_v2_timings", default=None)
)
_ACTIVE_CHECKPOINT_MODE: contextvars.ContextVar[int] = contextvars.ContextVar(
    "fep_output_v2_checkpoint_mode", default=2
)
_ACTIVE_MAX_THREADS: contextvars.ContextVar[int] = contextvars.ContextVar(
    "fep_output_v2_max_threads", default=4
)
_ACTIVE_PROFILE_PLAN: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "fep_output_v2_profile_plan", default=False
)
_ACTIVE_RUN_CFG: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "fep_output_v2_run_cfg", default=None
)
_ACTIVE_CHECKPOINT_PROFILE: contextvars.ContextVar[str] = contextvars.ContextVar(
    "fep_output_v2_checkpoint_profile", default="lean"
)
_ACTIVE_LOCAL_DENYLIST: contextvars.ContextVar[frozenset[str]] = (
    contextvars.ContextVar(
        "fep_output_v2_local_delta_denylist",
        default=frozenset({"final_cost_pct"}),
    )
)
_ACTIVE_SPLIT_CPBT: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "fep_output_v2_split_cpbt_inputs", default=True
)
_LAST_RUN_PROFILE: dict[str, Any] = {}

_BUILTIN_LOCAL_DELTA_DENYLIST = frozenset({"final_cost_pct"})
CHECKPOINT_PROFILES = {
    "full": frozenset(),
    "lean": frozenset(
        {
            "eff_dt_fused",
            "uc_ordered_common",
            "all_und_common_nolt",
            "input_lines_nolt",
            "eff_dated_s6_m0",
            "entity_und_common_nolt",
        }
    ),
    "conservative": frozenset(
        {"underlyings_common", "nde_post_miss_fused"}
    ),
    "balanced": frozenset(
        {
            "underlyings_common",
            "nde_post_miss_fused",
            "all_ent_pre_tag_m0",
            "eff_dated_s6_m0",
        }
    ),
}


def _normalize_workers(max_threads: Any = 4, MaxThreads: Any = None) -> int:
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 4))


def _record(name: str, elapsed: float) -> None:
    sink = _ACTIVE_TIMINGS.get()
    if sink is not None:
        sink.append({"step": name, "elapsed_seconds": round(elapsed, 3)})


def _timed(name: str, fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        started = time.time()
        try:
            return fn(*args, **kwargs)
        finally:
            _record(name, time.time() - started)

    return wrapped


def _normalize_profile(value: Any) -> str:
    profile = str(value or "lean").strip().lower()
    if profile not in CHECKPOINT_PROFILES:
        raise ValueError(
            "CheckpointProfile must be one of full, lean, conservative, balanced"
        )
    return profile


def _normalize_denylist(value: Any, mode: Any) -> frozenset[str]:
    supplied: set[str] = set()
    if value:
        tokens = re.split(r"[,\s]+", value) if isinstance(value, str) else value
        supplied = {
            str(token).strip() for token in tokens if str(token).strip()
        }
    if str(mode or "extend").strip().lower() == "replace":
        return frozenset(supplied) if supplied else _BUILTIN_LOCAL_DELTA_DENYLIST
    return frozenset(set(_BUILTIN_LOCAL_DELTA_DENYLIST) | supplied)


def _is_dataframe(value: Any) -> bool:
    """Duck-type classic and Spark Connect DataFrames."""
    return all(
        hasattr(value, attribute)
        for attribute in ("schema", "columns", "explain")
    )


def _checkpoint(spark, df, name, cfg):
    """Apply the measured profile and V2 backend safety policy."""
    started = time.time()
    profile = _ACTIVE_CHECKPOINT_PROFILE.get()
    if name in CHECKPOINT_PROFILES[profile]:
        if cfg.get("profile_plan"):
            track_checkpoint_plan(name, df, cfg)
        cfg.setdefault("_output_v2_checkpoint_bypasses", []).append(name)
        print(
            f"[outputV2 checkpoint] bypass name={name} profile={profile}",
            flush=True,
        )
        _record(f"checkpoint-bypass:{name}", time.time() - started)
        return df

    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    forced_delta = any(
        str(name).startswith(prefix)
        for prefix in _ACTIVE_LOCAL_DENYLIST.get()
    )
    effective_mode = 1 if forced_delta else _ACTIVE_CHECKPOINT_MODE.get()
    try:
        result = checkpoint_V2(
            spark, df, name, cfg, checkpoint_mode=effective_mode
        )
        activity = cfg.get("_checkpoint_v2_activity", ())
        actual_backend = (
            activity[-1].get("backend")
            if len(activity) > activity_start
            else None
        )
        if actual_backend == "local":
            result = result.toDF(*result.columns)
        if forced_delta:
            cfg.setdefault("_output_v2_forced_delta", []).append(name)
        return result
    finally:
        _record(f"checkpoint:{name}", time.time() - started)


def _drop_checkpoints_noop(spark, cfg):
    """Leave V2 checkpoint hygiene to unique names and EOD cleanup."""
    return None


def _build_cfg_for_run_modes(bound: inspect.BoundArguments) -> dict:
    cfg = bound.arguments.get("cfg")
    if cfg is None:
        cfg = _base.load_common_config(
            bound.arguments["spark"],
            entity_id=bound.arguments.get("entity_id"),
            client_id=bound.arguments.get("client_id"),
            tax_period_id=bound.arguments.get("tax_period_id"),
            run_id=bound.arguments.get("run_id"),
            catalog=bound.arguments.get("catalog"),
            schema=bound.arguments.get("schema"),
        )
        bound.arguments["cfg"] = cfg
    return cfg


def _delegated_run_modes(*args, **kwargs):
    """Initialize V2 once, then invoke the production run_modes implementation."""
    bound = inspect.signature(_PRODUCTION_RUN_MODES).bind_partial(*args, **kwargs)
    bound.apply_defaults()
    cfg = _build_cfg_for_run_modes(bound)
    cfg.pop("_checkpoint_v2_state", None)
    cfg["_checkpoint_v2_activity"] = []
    cfg["_output_v2_checkpoint_bypasses"] = []
    cfg["_output_v2_forced_delta"] = []
    cfg["profile_plan"] = _ACTIVE_PROFILE_PLAN.get()
    _ACTIVE_RUN_CFG.set(cfg)
    initialize_checkpoint_V2(cfg, _ACTIVE_CHECKPOINT_MODE.get())
    return _PRODUCTION_RUN_MODES(*bound.args, **bound.kwargs)


# The private production run_final_effective_percentages resolves run_modes from
# its own globals. Redirect only that private global so cfg is initialized
# before its first production checkpoint.
_base._checkpoint = _checkpoint
_base._drop_checkpoints = _drop_checkpoints_noop
_base.run_modes = _delegated_run_modes


_CPBT_INPUT_SPLIT_SPECS = (
    (6, "nde_pre_cpbt_cpbtin"),
    (7, "de_pre_cpbt_cpbtin"),
)


def _split_cpbt_inputs(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Pre-checkpoint only CPBT's positional non-dated and dated inputs."""
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        checkpoint_fn = kwargs.get("checkpoint_fn")
        cfg = args[1] if len(args) >= 2 else None
        if (
            _ACTIVE_SPLIT_CPBT.get()
            and callable(checkpoint_fn)
            and len(args) >= 9
            and isinstance(cfg, dict)
        ):
            mutable_args = list(args)
            mode = cfg.get("_current_mode", 1)
            for index, prefix in _CPBT_INPUT_SPLIT_SPECS:
                if _is_dataframe(mutable_args[index]):
                    mutable_args[index] = checkpoint_fn(
                        args[0],
                        mutable_args[index],
                        f"{prefix}_m{mode}",
                        cfg,
                    )
            args = tuple(mutable_args)
        return fn(*args, **kwargs)

    return wrapped


# Install the proven CPBT input split as the innermost wrapper, so timing and
# plan profiling observe the post-split builder. The production builder itself
# remains unchanged; warning-only reader swaps and cost_pct_loader stay off.
_base_build_cpbt = getattr(_base, "build_cost_percentage_by_type", None)
if callable(_base_build_cpbt):
    _base.build_cost_percentage_by_type = _split_cpbt_inputs(_base_build_cpbt)

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
        setattr(_base, _name, track_plan(_timed(_name, _fn)))


def _summarize(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    elapsed: dict[str, float] = defaultdict(float)
    calls: dict[str, int] = defaultdict(int)
    for event in events:
        name = str(event["step"])
        elapsed[name] += float(event["elapsed_seconds"])
        calls[name] += 1
    return [
        {
            "step": name,
            "calls": calls[name],
            "elapsed_seconds": round(total, 3),
        }
        for name, total in sorted(
            elapsed.items(), key=lambda item: item[1], reverse=True
        )
    ]


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "on", "true", "yes"}
    return bool(value)


def _pop_options(kwargs: dict[str, Any]) -> dict[str, Any]:
    checkpoint_raw = kwargs.pop(
        "CheckpointMode", kwargs.pop("checkpoint_mode", 2)
    )
    checkpoint_mode = normalize_checkpoint_mode(checkpoint_raw)
    max_threads = _normalize_workers(
        kwargs.pop("max_threads", 4), kwargs.pop("MaxThreads", None)
    )
    profile_raw = kwargs.pop("ProfilePlan", kwargs.pop("profile_plan", False))
    profile_plan = _as_bool(profile_raw)
    threshold = int(
        kwargs.pop(
            "PlanCheckpointThreshold",
            kwargs.pop("plan_checkpoint_threshold", 30),
        )
    )
    checkpoint_profile = _normalize_profile(
        kwargs.pop("CheckpointProfile", kwargs.pop("checkpoint_profile", "lean"))
    )
    denylist_value = kwargs.pop(
        "LocalDeltaDenylist", kwargs.pop("local_delta_denylist", None)
    )
    denylist_mode = str(
        kwargs.pop(
            "LocalDeltaDenylistMode",
            kwargs.pop("local_delta_denylist_mode", "extend"),
        )
        or "extend"
    ).strip().lower()
    local_denylist = _normalize_denylist(denylist_value, denylist_mode)
    split_cpbt_inputs = _as_bool(
        kwargs.pop(
            "SplitCpbtInputs",
            kwargs.pop("split_cpbt_inputs", True),
        ),
        default=True,
    )
    return {
        "checkpoint_mode": checkpoint_mode,
        "max_threads": max_threads,
        "profile_plan": profile_plan,
        "threshold": threshold,
        "checkpoint_profile": checkpoint_profile,
        "local_denylist": local_denylist,
        "local_denylist_mode": denylist_mode,
        "split_cpbt_inputs": split_cpbt_inputs,
    }


def _run_profiled(fn: Callable[..., Any], *args, **kwargs):
    options = _pop_options(kwargs)
    checkpoint_mode = options["checkpoint_mode"]
    max_threads = options["max_threads"]
    profile_plan = options["profile_plan"]
    threshold = options["threshold"]
    events: list[dict[str, Any]] = []
    timing_token = _ACTIVE_TIMINGS.set(events)
    mode_token = _ACTIVE_CHECKPOINT_MODE.set(checkpoint_mode)
    workers_token = _ACTIVE_MAX_THREADS.set(max_threads)
    profile_flag_token = _ACTIVE_PROFILE_PLAN.set(profile_plan)
    checkpoint_profile_token = _ACTIVE_CHECKPOINT_PROFILE.set(
        options["checkpoint_profile"]
    )
    denylist_token = _ACTIVE_LOCAL_DENYLIST.set(options["local_denylist"])
    split_cpbt_token = _ACTIVE_SPLIT_CPBT.set(options["split_cpbt_inputs"])
    run_cfg_token = _ACTIVE_RUN_CFG.set(None)
    plan_token = checkpoint_token = action_token = None
    builder_records: list[dict[str, Any]] = []
    checkpoint_records: list[dict[str, Any]] = []
    action_records: list[dict[str, Any]] = []
    if profile_plan:
        plan_token, builder_records = start_plan_profile()
        checkpoint_token, checkpoint_records = start_checkpoint_plan_profile()
        action_token, action_records = start_action_profile()

    print(
        f"[outputV2] CheckpointMode={checkpoint_mode} "
        f"CheckpointProfile={options['checkpoint_profile']} "
        f"SplitCpbtInputs={'on' if options['split_cpbt_inputs'] else 'off'} "
        f"MaxThreads={max_threads} execution=sequential"
    )
    started = time.time()
    run_cfg = None
    try:
        result = fn(*args, **kwargs)
    finally:
        if action_token is not None:
            finish_action_profile(action_token)
        if checkpoint_token is not None:
            finish_checkpoint_plan_profile(checkpoint_token)
        if plan_token is not None:
            finish_plan_profile(plan_token)
        run_cfg = _ACTIVE_RUN_CFG.get()
        _ACTIVE_RUN_CFG.reset(run_cfg_token)
        _ACTIVE_MAX_THREADS.reset(workers_token)
        _ACTIVE_CHECKPOINT_MODE.reset(mode_token)
        _ACTIVE_PROFILE_PLAN.reset(profile_flag_token)
        _ACTIVE_CHECKPOINT_PROFILE.reset(checkpoint_profile_token)
        _ACTIVE_LOCAL_DENYLIST.reset(denylist_token)
        _ACTIVE_SPLIT_CPBT.reset(split_cpbt_token)
        _ACTIVE_TIMINGS.reset(timing_token)

    wall = round(time.time() - started, 3)
    timings = _summarize(events)
    reports = {"builder": [], "checkpoint": [], "action": []}
    if profile_plan:
        for label, records, key in (
            ("BUILDER", builder_records, "builder"),
            ("CHECKPOINT", checkpoint_records, "checkpoint"),
            ("ACTION", action_records, "action"),
        ):
            print(f"\n===== {label}-LEVEL PLAN PROFILE =====")
            try:
                reports[key] = plan_profile_report(
                    records, threshold, label=label
                )
            except Exception:
                logger.warning("[PLAN] %s report failed", label, exc_info=True)

    activity = (
        list(run_cfg.get("_checkpoint_v2_activity", ()))
        if isinstance(run_cfg, dict)
        else []
    )
    bypassed = (
        list(run_cfg.get("_output_v2_checkpoint_bypasses", ()))
        if isinstance(run_cfg, dict)
        else []
    )
    forced_delta = (
        list(run_cfg.get("_output_v2_forced_delta", ()))
        if isinstance(run_cfg, dict)
        else []
    )
    _LAST_RUN_PROFILE.clear()
    _LAST_RUN_PROFILE.update(
        {
            "updated_wall_seconds": wall,
            "effective_max_threads": max_threads,
            "execution_strategy": "sequential",
            "checkpoint_mode": checkpoint_mode,
            "checkpoint_profile": options["checkpoint_profile"],
            "local_delta_denylist": sorted(options["local_denylist"]),
            "local_delta_denylist_mode": options["local_denylist_mode"],
            "split_cpbt_inputs": options["split_cpbt_inputs"],
            "checkpoint_activity": activity,
            "checkpoint_bypasses": bypassed,
            "forced_delta_names": forced_delta,
            "timings": timings,
            "plan_profile": reports["builder"],
            "checkpoint_plan_profile": reports["checkpoint"],
            "action_profile": reports["action"],
        }
    )
    print(f"[outputV2 timing] wall={wall:.3f}s")
    for item in timings:
        print(
            f"[outputV2 timing] {item['step']}: "
            f"{item['elapsed_seconds']:.3f}s (calls={item['calls']})"
        )
    return result


def run_modes(*args, **kwargs):
    """Run production modes with V2 checkpoints and wrapper instrumentation."""
    return _run_profiled(_delegated_run_modes, *args, **kwargs)


def run_final_effective_percentages(*args, **kwargs):
    """Run the unchanged production FEP entry through the isolated namespace."""
    return _run_profiled(_PRODUCTION_RUN_FINAL, *args, **kwargs)


def get_last_run_profile() -> dict[str, Any]:
    """Return a defensive copy of the latest wrapper profile."""
    return {
        **_LAST_RUN_PROFILE,
        "timings": list(_LAST_RUN_PROFILE.get("timings", ())),
        "checkpoint_activity": list(
            _LAST_RUN_PROFILE.get("checkpoint_activity", ())
        ),
        "plan_profile": list(_LAST_RUN_PROFILE.get("plan_profile", ())),
        "checkpoint_plan_profile": list(
            _LAST_RUN_PROFILE.get("checkpoint_plan_profile", ())
        ),
        "action_profile": list(_LAST_RUN_PROFILE.get("action_profile", ())),
    }


run_mode = run_final_effective_percentages

__all__ = [
    "get_last_run_profile",
    "run_final_effective_percentages",
    "run_mode",
    "run_modes",
]
