"""Isolated, contract-driven wrapper around the production FEP pipeline."""

from __future__ import annotations

import contextvars
import functools
import inspect
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable

from .checkpoint_policy import (
    drop_failed_run_checkpoints,
    initialize_named_checkpoint_policy,
    named_checkpoint,
)
from .parent import isolated_output_module
from .pipeline import run_modes_parallel
from .stages import FUNCTION_STAGE, StageName, stage_contracts

_base = isolated_output_module("orchestrator")
_PRODUCTION_RUN_MODES = _base.run_modes
_PRODUCTION_RUN_FINAL = _base.run_final_effective_percentages
_PRODUCTION_RESULT_STORER = _base.GenericResultStorer

_ACTIVE_EVENTS: contextvars.ContextVar[list | None] = contextvars.ContextVar(
    "fep_output_v3_events", default=None
)
_ACTIVE_COORDINATOR = contextvars.ContextVar(
    "fep_output_v3_coordinator", default=None
)
_ACTIVE_RUN_CFG: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "fep_output_v3_run_cfg", default=None
)
_ACTIVE_STAGE_OVERRIDE: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("fep_output_v3_stage_override", default=None)
)
_EVENT_LOCK = threading.Lock()
_LAST_RUN_PROFILE: dict[str, Any] = {}
_ALL_PARALLEL_GROUPS = frozenset(
    {
        "common_dimensions",
        "common_inputs",
        "lookthrough_metadata",
        "lt_nolt_branches",
        "mode_prep",
        "output_build",
        "output_writes",
    }
)


def _workers(value: Any) -> int:
    try:
        return max(1, min(int(value), 4))
    except (TypeError, ValueError):
        return 4


def _record(stage: str, operation: str, elapsed: float) -> None:
    sink = _ACTIVE_EVENTS.get()
    if sink is not None:
        with _EVENT_LOCK:
            sink.append(
                {
                    "stage": stage,
                    "operation": operation,
                    "elapsed_seconds": round(elapsed, 3),
                }
            )


def _timed(stage: str, operation: str, fn: Callable) -> Callable:
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        started = time.time()
        try:
            return fn(*args, **kwargs)
        finally:
            _record(
                _ACTIVE_STAGE_OVERRIDE.get() or stage,
                operation,
                time.time() - started,
            )

    return wrapped


class _Coordinator:
    """Bounded executor for explicitly approved independent operations."""

    def __init__(self, workers: int, enabled_groups=frozenset()):
        self.workers = workers
        self.enabled_groups = frozenset(enabled_groups)
        self._executor = (
            ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fep-v3")
            if workers > 1
            else None
        )
        self._futures = {}
        self._events = []
        self._lock = threading.Lock()

    def submit_group(self, group, tasks) -> None:
        if self._executor is None or group not in self.enabled_groups:
            return
        for name, fn, args, kwargs in tasks:
            key = (group, name)
            with self._lock:
                if key in self._futures:
                    continue
                context = contextvars.copy_context()
                self._futures[key] = self._executor.submit(
                    context.run, self._execute, group, name, fn, args, kwargs
                )

    def _execute(self, group, name, fn, args, kwargs):
        started = time.time()
        status = "PASS"
        stage = {
            "common_dimensions": StageName.COMMON_READS.value,
            "common_inputs": StageName.COMMON_READS.value,
            "lookthrough_metadata": StageName.COMMON_READS.value,
            "mode_prep": StageName.MODE_PREP.value,
            "output_build": StageName.OUTPUT_BUILD.value,
            "output_writes": StageName.OUTPUT_WRITE.value,
        }.get(group)
        if group == "lt_nolt_branches":
            stage = (
                StageName.NO_LT_BRANCH.value
                if name == "nolt"
                else StageName.WITH_LT_BRANCH.value
            )
        stage_token = _ACTIVE_STAGE_OVERRIDE.set(stage)
        try:
            return fn(*args, **kwargs)
        except Exception:
            status = "FAIL"
            raise
        finally:
            _ACTIVE_STAGE_OVERRIDE.reset(stage_token)
            with self._lock:
                self._events.append(
                    {
                        "group": group,
                        "task": name,
                        "status": status,
                        "elapsed_seconds": round(time.time() - started, 3),
                        "thread": threading.current_thread().name,
                    }
                )

    def result(self, group, name, fn, *args, **kwargs):
        if self._executor is None or group not in self.enabled_groups:
            return fn(*args, **kwargs)
        self.submit_group(group, ((name, fn, args, kwargs),))
        return self._futures[(group, name)].result()

    def run_group(self, group, tasks):
        """Run every task, observe every future, then raise on the main thread."""
        tasks = tuple(tasks)
        if self._executor is None or group not in self.enabled_groups:
            results = {}
            for name, fn, args, kwargs in tasks:
                results[name] = self._execute(
                    group, name, fn, args, kwargs
                )
            return results
        self.submit_group(group, tasks)
        results = {}
        failures = []
        for name, _, _, _ in tasks:
            try:
                results[name] = self._futures[(group, name)].result()
            except Exception as exc:
                failures.append((name, exc))
        if failures:
            detail = [(name, f"{type(exc).__name__}: {exc}") for name, exc in failures]
            error = RuntimeError(f"Parallel group {group!r} failed: {detail}")
            raise error from failures[0][1]
        return results

    def close(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)

    @property
    def events(self):
        with self._lock:
            return sorted(
                (dict(item) for item in self._events),
                key=lambda item: (item["group"], item["task"]),
            )


def _checkpoint(spark, df, name, cfg):
    return named_checkpoint(spark, df, name, cfg)


def _drop_checkpoints_noop(spark, cfg):
    # Names include a sequence and UUID in Common_V2. Cleanup is intentionally
    # outside the hot path so returned lazy relations stay valid.
    return None


def _build_cfg(bound: inspect.BoundArguments) -> dict:
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
    bound = inspect.signature(_PRODUCTION_RUN_MODES).bind_partial(*args, **kwargs)
    bound.apply_defaults()
    cfg = _build_cfg(bound)
    cfg.pop("_checkpoint_v2_state", None)
    initialize_named_checkpoint_policy(cfg)
    _ACTIVE_RUN_CFG.set(cfg)
    coordinator = _ACTIVE_COORDINATOR.get()
    if coordinator is None:
        return _PRODUCTION_RUN_MODES(*bound.args, **bound.kwargs)
    return run_modes_parallel(
        _base,
        coordinator.run_group,
        _PRODUCTION_RUN_MODES,
        *bound.args,
        **bound.kwargs,
    )


_base._checkpoint = _checkpoint
_base._drop_checkpoints = _drop_checkpoints_noop
_base.run_modes = _delegated_run_modes

# Time production functions without replacing or copying their business logic.
for _function_name, _stage_name in FUNCTION_STAGE.items():
    _function = getattr(_base, _function_name, None)
    if callable(_function):
        setattr(
            _base,
            _function_name,
            _timed(_stage_name, _function_name, _function),
        )


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
    coordinator = _ACTIVE_COORDINATOR.get()
    fn = _PARALLEL_ORIGINALS[name]
    if coordinator is None:
        return fn(*args, **kwargs)
    return coordinator.result(group, name, fn, *args, **kwargs)


def _snapshot_wrapper(name):
    @functools.wraps(_PARALLEL_ORIGINALS[name])
    def wrapped(spark, cfg, *args, **kwargs):
        coordinator = _ACTIVE_COORDINATOR.get()
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
                        _PARALLEL_ORIGINALS["build_asset_class_relationship"],
                        (spark, cfg),
                        {},
                    ),
                    (name, _PARALLEL_ORIGINALS[name], (spark, cfg, *args), kwargs),
                ),
            )
        return _parallel_result(
            "common_dimensions", name, spark, cfg, *args, **kwargs
        )

    return wrapped


def _line_items_wrapper(spark, cfg, *args, **kwargs):
    coordinator = _ACTIVE_COORDINATOR.get()
    names = (
        "load_line_items",
        "load_book_effective_data",
        "load_quarters",
        "load_yearly_data",
    )
    if coordinator is not None:
        coordinator.submit_group(
            "common_inputs",
            tuple(
                (name, _PARALLEL_ORIGINALS[name], (spark, cfg), {})
                for name in names
            ),
        )
    return _parallel_result(
        "common_inputs", "load_line_items", spark, cfg, *args, **kwargs
    )


def _lookthrough_wrapper(spark, cfg, *args, **kwargs):
    coordinator = _ACTIVE_COORDINATOR.get()
    names = ("build_lookthrough_input_modes14", "build_footnote_lines")
    if coordinator is not None:
        coordinator.submit_group(
            "lookthrough_metadata",
            tuple(
                (name, _PARALLEL_ORIGINALS[name], (spark, cfg), {})
                for name in names
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


for _name in (
    "build_cost_percentage_snapshot_modes123",
    "build_cost_percentage_snapshot_mode4",
):
    setattr(_base, _name, _snapshot_wrapper(_name))
for _name, _group in (
    ("build_entity_partners", "common_dimensions"),
    ("build_asset_class_relationship", "common_dimensions"),
    ("load_book_effective_data", "common_inputs"),
    ("load_quarters", "common_inputs"),
    ("load_yearly_data", "common_inputs"),
    ("build_footnote_lines", "lookthrough_metadata"),
):
    setattr(_base, _name, functools.partial(_parallel_result, _group, _name))
_base.load_line_items = _line_items_wrapper
_base.build_lookthrough_input_modes14 = _lookthrough_wrapper


class _ParallelResultStorer(_PRODUCTION_RESULT_STORER):
    """Write only distinct output tables concurrently."""

    def _write(self, df, catalog, database, table, run_id):
        started = time.time()
        try:
            return self.store_output_to_delta_table(
                df, catalog, database, table, run_id
            )
        finally:
            _record(
                StageName.OUTPUT_WRITE.value,
                f"write:{table}",
                time.time() - started,
            )

    def store_output_to_delta_lake(
        self, result, catalog_name, database_name, run_id
    ):
        coordinator = _ACTIVE_COORDINATOR.get()
        if (
            coordinator is None
            or coordinator.workers <= 1
            or "output_writes" not in coordinator.enabled_groups
            or len(result) <= 1
        ):
            return super().store_output_to_delta_lake(
                result, catalog_name, database_name, run_id
            )
        print(f"Storing distinct FEP tables in parallel: {datetime.now()}")
        tasks = tuple(
            (
                table,
                self._write,
                (df, catalog_name, database_name, table, run_id),
                {},
            )
            for table, df in result.items()
        )
        coordinator.submit_group("output_writes", tasks)
        failures = []
        for table in result:
            try:
                coordinator.result("output_writes", table, self._write)
            except Exception as exc:
                failures.append((table, str(exc)))
        if failures:
            raise RuntimeError(f"FEP output write failures: {failures}")
        return None


_base.GenericResultStorer = _ParallelResultStorer


def _stage_summary(events):
    totals = defaultdict(float)
    calls = defaultdict(int)
    for event in events:
        totals[event["stage"]] += event["elapsed_seconds"]
        calls[event["stage"]] += 1
    return [
        {
            "stage": stage.value,
            "calls": calls[stage.value],
            "elapsed_seconds": round(totals[stage.value], 3),
        }
        for stage in StageName
    ]


def _run_profiled(fn, *args, **kwargs):
    raw_threads = kwargs.pop("MaxThreads", kwargs.pop("max_threads", 4))
    raw_groups = kwargs.pop(
        "ParallelGroups",
        kwargs.pop("parallel_groups", ",".join(sorted(_ALL_PARALLEL_GROUPS))),
    )
    # Accepted for schedule compatibility; backend selection is always named.
    kwargs.pop("CheckpointMode", None)
    kwargs.pop("checkpoint_mode", None)
    kwargs.pop("ProfilePlan", None)
    kwargs.pop("profile_plan", None)
    kwargs.pop("PlanCheckpointThreshold", None)
    kwargs.pop("plan_checkpoint_threshold", None)
    max_threads = _workers(raw_threads)
    if raw_groups is None:
        requested_groups = set(_ALL_PARALLEL_GROUPS)
    elif isinstance(raw_groups, str):
        requested_groups = {
            item.strip() for item in raw_groups.split(",") if item.strip()
        }
    else:
        requested_groups = {str(item).strip() for item in raw_groups}
    if requested_groups == {"all"}:
        requested_groups = set(_ALL_PARALLEL_GROUPS)
    elif requested_groups == {"none"}:
        requested_groups = set()
    unknown_groups = requested_groups - _ALL_PARALLEL_GROUPS
    if unknown_groups:
        raise ValueError(
            f"Unknown ParallelGroups: {sorted(unknown_groups)}; "
            f"valid groups are {sorted(_ALL_PARALLEL_GROUPS)}"
        )
    events = []
    event_token = _ACTIVE_EVENTS.set(events)
    coordinator = _Coordinator(max_threads, requested_groups)
    coordinator_token = _ACTIVE_COORDINATOR.set(coordinator)
    cfg_token = _ACTIVE_RUN_CFG.set(None)
    started = time.time()
    succeeded = False
    try:
        result = fn(*args, **kwargs)
        succeeded = True
    finally:
        coordinator.close()
        cfg = _ACTIVE_RUN_CFG.get()
        if not succeeded and isinstance(cfg, dict):
            try:
                spark = args[0] if args else kwargs.get("spark")
                if spark is not None:
                    drop_failed_run_checkpoints(spark, cfg)
            except Exception as cleanup_error:
                print(
                    "[outputV3] failed-run checkpoint cleanup also failed: "
                    f"{cleanup_error}"
                )
        wall = round(time.time() - started, 3)
        pipeline_strategy = (
            cfg.get("_output_v3_pipeline_strategy")
            if isinstance(cfg, dict)
            else None
        )
        uses_parallel_pipeline = (
            pipeline_strategy == "parallel_modes_123_control_flow"
        )
        _LAST_RUN_PROFILE.clear()
        _LAST_RUN_PROFILE.update(
            {
                "updated_wall_seconds": wall,
                "effective_max_threads": max_threads,
                "enabled_parallel_groups": sorted(requested_groups),
                "execution_strategy": (
                    "bounded_parallel_staged_pipeline"
                    if max_threads > 1
                    else "sequential"
                ),
                "branch_strategy": (
                    "parallel_isolated_cfg"
                    if uses_parallel_pipeline and max_threads > 1
                    else (
                        "sequential_isolated_cfg"
                        if uses_parallel_pipeline
                        else "production_control_flow"
                    )
                ),
                "pass_a_strategy": (
                    "parallel_isolated_cfg"
                    if uses_parallel_pipeline and max_threads > 1
                    else (
                        "sequential_isolated_cfg"
                        if uses_parallel_pipeline
                        else "production_control_flow"
                    )
                ),
                "output_build_strategy": (
                    "parallel_isolated_cfg"
                    if uses_parallel_pipeline and max_threads > 1
                    else (
                        "sequential_isolated_cfg"
                        if uses_parallel_pipeline
                        else "production_control_flow"
                    )
                ),
                "pipeline_strategy": pipeline_strategy,
                "checkpoint_policy": "named_semantic_v1",
                "checkpoint_activity": list(
                    cfg.get("_checkpoint_policy_activity", ())
                ) if isinstance(cfg, dict) else [],
                "parallel_activity": coordinator.events,
                "stage_timings": _stage_summary(events),
                "operation_timings": list(events),
                "stage_contracts": stage_contracts(),
                "artifact_merges": list(
                    cfg.get("_output_v3_artifact_merges", ())
                ) if isinstance(cfg, dict) else [],
            }
        )
        _ACTIVE_RUN_CFG.reset(cfg_token)
        _ACTIVE_COORDINATOR.reset(coordinator_token)
        _ACTIVE_EVENTS.reset(event_token)
    return result


def run_modes(*args, **kwargs):
    return _run_profiled(_delegated_run_modes, *args, **kwargs)


def run_final_effective_percentages(*args, **kwargs):
    return _run_profiled(_PRODUCTION_RUN_FINAL, *args, **kwargs)


def get_last_run_profile() -> dict[str, Any]:
    return {
        **_LAST_RUN_PROFILE,
        **{
            key: [dict(item) for item in _LAST_RUN_PROFILE.get(key, ())]
            for key in (
                "checkpoint_activity",
                "parallel_activity",
                "stage_timings",
                "operation_timings",
                "stage_contracts",
                "artifact_merges",
            )
        },
        "enabled_parallel_groups": list(
            _LAST_RUN_PROFILE.get("enabled_parallel_groups", ())
        ),
    }


run_mode = run_final_effective_percentages

__all__ = [
    "get_last_run_profile",
    "run_final_effective_percentages",
    "run_mode",
    "run_modes",
]
