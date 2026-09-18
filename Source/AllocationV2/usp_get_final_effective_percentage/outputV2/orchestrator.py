"""Conservative V2 wrapper around the unchanged production FEP orchestrator."""

from __future__ import annotations

import contextvars
import functools
import inspect
import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)

from .parent import isolated_output_module
from .plan_profiler import (
    finish_action_profile,
    finish_checkpoint_plan_profile,
    finish_plan_profile,
    plan_profile_report,
    profile_action,
    start_action_profile,
    start_checkpoint_plan_profile,
    start_plan_profile,
    track_plan,
)

logger = logging.getLogger(__name__)
_base = isolated_output_module("orchestrator")
_PRODUCTION_RUN_MODES = _base.run_modes
_PRODUCTION_RUN_FINAL = _base.run_final_effective_percentages

_ACTIVE_TIMINGS: contextvars.ContextVar[list[dict[str, Any]] | None] = (
    contextvars.ContextVar("fep_output_v2_timings", default=None)
)
_ACTIVE_CHECKPOINT_MODE: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "fep_output_v2_checkpoint_mode", default=None
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
_ACTIVE_PARALLEL_COORDINATOR: contextvars.ContextVar[
    "_ParallelCoordinator | None"
] = contextvars.ContextVar("fep_output_v2_parallel_coordinator", default=None)
_LAST_RUN_PROFILE: dict[str, Any] = {}

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


class _ParallelCoordinator:
    """Run named, read-only planning and distinct-table write tasks safely."""

    def __init__(self, workers: int):
        self.workers = workers
        self._executor = (
            ThreadPoolExecutor(
                max_workers=workers, thread_name_prefix="fep-output-v2"
            )
            if workers > 1
            else None
        )
        self._futures = {}
        self._groups: set[str] = set()
        self._activity: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def submit_group(self, group: str, tasks) -> None:
        if self._executor is None:
            return
        with self._lock:
            first_submission = group not in self._groups
            self._groups.add(group)
        if first_submission:
            print(
                f"[outputV2 parallel] group={group} "
                f"tasks={[name for name, _, _, _ in tasks]} "
                f"workers={self.workers}",
                flush=True,
            )
        for name, fn, args, kwargs in tasks:
            key = (group, name)
            with self._lock:
                if key in self._futures:
                    continue
                context = contextvars.copy_context()
                self._futures[key] = self._executor.submit(
                    context.run,
                    self._execute,
                    group,
                    name,
                    fn,
                    args,
                    kwargs,
                )

    def _execute(self, group, name, fn, args, kwargs):
        started = time.time()
        status = "PASS"
        try:
            return fn(*args, **kwargs)
        except Exception:
            status = "FAIL"
            raise
        finally:
            event = {
                "group": group,
                "task": name,
                "status": status,
                "elapsed_seconds": round(time.time() - started, 3),
                "thread": threading.current_thread().name,
            }
            with self._lock:
                self._activity.append(event)

    def result(self, group, name, fn, *args, **kwargs):
        if self._executor is None:
            return fn(*args, **kwargs)
        self.submit_group(group, ((name, fn, args, kwargs),))
        return self._futures[(group, name)].result()

    def shutdown(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)

    @property
    def activity(self) -> list[dict[str, Any]]:
        with self._lock:
            return sorted(
                (dict(item) for item in self._activity),
                key=lambda item: (item["group"], item["task"]),
            )


def _checkpoint(spark, df, name, cfg):
    """Preserve every production seam and use the initialized shared V2 mode."""
    started = time.time()
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    try:
        result = checkpoint_V2(spark, df, name, cfg)
        activity = cfg.get("_checkpoint_v2_activity", ())
        actual_backend = (
            activity[-1].get("backend")
            if len(activity) > activity_start
            else None
        )
        if actual_backend == "local":
            result = result.toDF(*result.columns)
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


_PARALLEL_ORIGINALS = {
    name: getattr(_base, name)
    for name in (
        "build_cost_percentage_snapshot_modes123",
        "build_cost_percentage_snapshot_mode4",
        "build_entity_partners",
        "build_asset_class_relationship",
        "load_line_items",
        "load_book_effective_data",
        "load_quarters",
        "load_yearly_data",
        "build_lookthrough_input_modes14",
        "build_footnote_lines",
    )
}


def _parallel_result(group, name, *args, **kwargs):
    coordinator = _ACTIVE_PARALLEL_COORDINATOR.get()
    original = _PARALLEL_ORIGINALS[name]
    if coordinator is None:
        return original(*args, **kwargs)
    return coordinator.result(group, name, original, *args, **kwargs)


def _snapshot_with_common_dimensions(name):
    @functools.wraps(_PARALLEL_ORIGINALS[name])
    def wrapped(spark, cfg, *args, **kwargs):
        coordinator = _ACTIVE_PARALLEL_COORDINATOR.get()
        if coordinator is not None:
            coordinator.submit_group(
                "common_dimensions",
                (
                    (
                        "build_entity_partners",
                        _PARALLEL_ORIGINALS["build_entity_partners"],
                        (spark, cfg),
                        {},
                    ),
                    (
                        "build_asset_class_relationship",
                        _PARALLEL_ORIGINALS[
                            "build_asset_class_relationship"
                        ],
                        (spark, cfg),
                        {},
                    ),
                    (
                        name,
                        _PARALLEL_ORIGINALS[name],
                        (spark, cfg, *args),
                        kwargs,
                    ),
                ),
            )
        return _parallel_result(
            "common_dimensions", name, spark, cfg, *args, **kwargs
        )

    return wrapped


def _line_items_with_common_inputs(spark, cfg, *args, **kwargs):
    coordinator = _ACTIVE_PARALLEL_COORDINATOR.get()
    if coordinator is not None:
        coordinator.submit_group(
            "common_inputs",
            tuple(
                (
                    name,
                    _PARALLEL_ORIGINALS[name],
                    (spark, cfg),
                    {},
                )
                for name in (
                    "load_line_items",
                    "load_book_effective_data",
                    "load_quarters",
                    "load_yearly_data",
                )
            ),
        )
    return _parallel_result(
        "common_inputs", "load_line_items", spark, cfg, *args, **kwargs
    )


def _lookthrough_with_footnote_lines(spark, cfg, *args, **kwargs):
    coordinator = _ACTIVE_PARALLEL_COORDINATOR.get()
    if coordinator is not None:
        coordinator.submit_group(
            "lookthrough_metadata",
            tuple(
                (
                    name,
                    _PARALLEL_ORIGINALS[name],
                    (spark, cfg),
                    {},
                )
                for name in (
                    "build_lookthrough_input_modes14",
                    "build_footnote_lines",
                )
            ),
        )
    return _parallel_result(
        "lookthrough_metadata",
        "build_lookthrough_input_modes14",
        spark,
        cfg,
        *args,
        **kwargs,
    )


for _snapshot_name in (
    "build_cost_percentage_snapshot_modes123",
    "build_cost_percentage_snapshot_mode4",
):
    setattr(
        _base,
        _snapshot_name,
        _snapshot_with_common_dimensions(_snapshot_name),
    )

for _parallel_name, _parallel_group in (
    ("build_entity_partners", "common_dimensions"),
    ("build_asset_class_relationship", "common_dimensions"),
    ("load_book_effective_data", "common_inputs"),
    ("load_quarters", "common_inputs"),
    ("load_yearly_data", "common_inputs"),
    ("build_footnote_lines", "lookthrough_metadata"),
):
    setattr(
        _base,
        _parallel_name,
        functools.partial(_parallel_result, _parallel_group, _parallel_name),
    )

_base.load_line_items = _line_items_with_common_inputs
_base.build_lookthrough_input_modes14 = _lookthrough_with_footnote_lines


_PRODUCTION_RESULT_STORER = _base.GenericResultStorer


class _ParallelResultStorer(_PRODUCTION_RESULT_STORER):
    """Write FEP's distinct output tables concurrently."""

    def _store_profiled_table(
        self, df, catalog_name, database_name, table_name, run_id
    ):
        cfg = _ACTIVE_RUN_CFG.get()
        return profile_action(
            f"write:{table_name}",
            df,
            lambda: self.store_output_to_delta_table(
                df,
                catalog_name,
                database_name,
                table_name,
                run_id,
            ),
            cfg,
        )

    def store_output_to_delta_lake(
        self, result, catalog_name, database_name, run_id
    ):
        coordinator = _ACTIVE_PARALLEL_COORDINATOR.get()
        if coordinator is None or coordinator.workers <= 1 or len(result) <= 1:
            return super().store_output_to_delta_lake(
                result, catalog_name, database_name, run_id
            )

        print(f"🔄 Storing to Delta Tables in parallel: {datetime.now()}")
        tasks = tuple(
            (
                table_name,
                self._store_profiled_table,
                (
                    df,
                    catalog_name,
                    database_name,
                    table_name,
                    run_id,
                ),
                {},
            )
            for table_name, df in result.items()
        )
        coordinator.submit_group("output_writes", tasks)
        failures = []
        for table_name in result:
            try:
                coordinator.result(
                    "output_writes",
                    table_name,
                    self._store_profiled_table,
                )
                print(f"   ✓ {table_name}")
            except Exception as exc:
                failures.append((table_name, str(exc)))
                print(f"   ✗ {table_name}: {exc}")
        if failures:
            raise RuntimeError(
                f"Failed to store {len(failures)} FEP output table(s): "
                f"{failures}"
            )
        print(f"✅ Stored to Delta Tables: {datetime.now()}")
        return None


_base.GenericResultStorer = _ParallelResultStorer


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
    checkpoint_pascal = kwargs.pop("CheckpointMode", None)
    checkpoint_snake = kwargs.pop("checkpoint_mode", None)
    checkpoint_mode = resolve_checkpoint_mode(
        kwargs.get("cfg"),
        checkpoint_mode=checkpoint_snake,
        CheckpointMode=checkpoint_pascal,
    )
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
    return {
        "checkpoint_mode": checkpoint_mode,
        "max_threads": max_threads,
        "profile_plan": profile_plan,
        "threshold": threshold,
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
    coordinator = _ParallelCoordinator(max_threads)
    parallel_token = _ACTIVE_PARALLEL_COORDINATOR.set(coordinator)
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
        f"MaxThreads={max_threads} "
        f"execution={'bounded_parallel' if max_threads > 1 else 'sequential'}"
    )
    started = time.time()
    run_cfg = None
    try:
        result = fn(*args, **kwargs)
    finally:
        coordinator.shutdown()
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
        _ACTIVE_PARALLEL_COORDINATOR.reset(parallel_token)
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
    _LAST_RUN_PROFILE.clear()
    _LAST_RUN_PROFILE.update(
        {
            "updated_wall_seconds": wall,
            "effective_max_threads": max_threads,
            "execution_strategy": (
                "bounded_parallel" if max_threads > 1 else "sequential"
            ),
            "checkpoint_mode": checkpoint_mode,
            "checkpoint_activity": activity,
            "parallel_activity": coordinator.activity,
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
        "parallel_activity": list(
            _LAST_RUN_PROFILE.get("parallel_activity", ())
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
