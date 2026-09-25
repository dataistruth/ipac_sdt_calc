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

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from .checkpoint_policy import (
    drop_failed_run_checkpoints,
    initialize_named_checkpoint_policy,
    named_checkpoint,
)
from .parent import isolated_output_module
from .pipeline import run_modes_parallel
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
from .stages import FUNCTION_STAGE, StageName, stage_contracts

_base = isolated_output_module("orchestrator")
_book_effective = isolated_output_module("book_effective")
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
_ACTIVE_CHECKPOINT_MODE: contextvars.ContextVar[int] = contextvars.ContextVar(
    "fep_output_v3_checkpoint_mode", default=1
)
_ACTIVE_PROFILE_PLAN: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "fep_output_v3_profile_plan", default=False
)
_ACTIVE_PLAN_THRESHOLD: contextvars.ContextVar[int] = contextvars.ContextVar(
    "fep_output_v3_plan_threshold", default=30
)
_ACTIVE_EXPERIMENT: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "fep_output_v3_experiment", default=None
)
_NAMED_ACTION_DEPTH: contextvars.ContextVar[int] = contextvars.ContextVar(
    "fep_output_v3_named_action_depth", default=0
)
_EVENT_LOCK = threading.Lock()
_PROCESS_PRINT_LOCK = threading.Lock()
_LAST_RUN_PROFILE: dict[str, Any] = {}
_ALL_PARALLEL_GROUPS = frozenset(
    {
        "common_dimensions",
        "common_inputs",
        "lookthrough_metadata",
        "lt_nolt_branches",
        "mode_prep",
        "mode_prep_boundaries",
        "cpbt_boundaries",
        "effective_inputs",
        "fused_effective",
        "effective_boundaries",
        "output_build",
        "output_writes",
    }
)


def _table(spark, cfg, name):
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _line_items_without_warning_probe(spark, cfg):
    k1 = _table(spark, cfg, "K1LineItem").select(
        "LineID",
        "AllocationTypeRuleId",
        F.lit(cfg["k1_line_type_id"]).cast("int").alias("LineTypeID"),
        "TransactionDate",
        "IsTransactionDate",
        "IsTransfersAdjusted",
    )
    box_jkl = _table(spark, cfg, "BoxjklLineItem").select(
        "LineID",
        F.lit(cfg["yearly_allocation_type_id"])
        .cast("int")
        .alias("AllocationTypeRuleId"),
        F.lit(cfg["box_jkl_line_type_id"]).cast("int").alias("LineTypeID"),
        F.lit(None).cast("timestamp").alias("TransactionDate"),
        F.lit(False).alias("IsTransactionDate"),
        F.lit(True).alias("IsTransfersAdjusted"),
    )
    return k1.unionByName(box_jkl)


def _quarters_without_warning_probe(spark, cfg):
    if (
        cfg.get("allocation_type_name", "") == "PE Book Allocation"
        and cfg.get("is_dated_transfers_configured", "") == "C"
    ):
        return _table(spark, cfg, "QuarterDates").select("Quarter")
    return (
        _table(spark, cfg, "ENU_DF_DataList")
        .filter(F.col("Category") == "Quarters")
        .select(F.col("LookUpData").alias("Quarter"))
    )


def _lookthrough_without_warning_probe(spark, cfg):
    k1_id = cfg["k1_line_type_id"]
    adjustment_id = cfg["adjustment_line_type_id"]
    box_jkl_id = cfg["box_jkl_line_type_id"]
    return (
        _table(spark, cfg, "LookThroughAllocationInput")
        .filter(
            (F.col("RunID") == cfg["run_id"])
            & (F.col("ClientID") == cfg["client_id"])
            & F.col("LineTypeID").isin(
                [k1_id, adjustment_id, box_jkl_id]
            )
            & (
                (F.col("LineTypeID") == box_jkl_id)
                | (
                    F.col("LineTypeID").isin([k1_id, adjustment_id])
                    & (
                        _book_effective._sql_round(
                            F.coalesce(F.col("Amount"), F.lit(0.0)), 0
                        )
                        != 0
                    )
                )
            )
        )
        .select(
            "RunID",
            "ClientID",
            "EntityID",
            "LineTypeID",
            "LineID",
            "Amount",
            "QuicklinkID",
            "Amount704b",
            "TrackingKey",
            "Tag",
        )
    )


WARNING_PROBE_BUILDERS = {
    "line_items": _line_items_without_warning_probe,
    "quarters": _quarters_without_warning_probe,
    "lookthrough": _lookthrough_without_warning_probe,
}


def _workers(value: Any) -> int:
    try:
        return max(1, min(int(value), 4))
    except (TypeError, ValueError):
        return 4


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "on", "true", "yes"}
    return bool(value)


def _record(
    stage: str, operation: str, elapsed: float, **details: Any
) -> None:
    sink = _ACTIVE_EVENTS.get()
    if sink is not None:
        with _EVENT_LOCK:
            sink.append(
                {
                    "stage": stage,
                    "operation": operation,
                    "elapsed_seconds": round(elapsed, 3),
                    **details,
                }
            )


def _print_process(
    event: str,
    kind: str,
    name: str,
    stage: str | None,
    *,
    elapsed: float | None = None,
    status: str | None = None,
) -> None:
    """Print one compact, thread-safe live process event."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    thread_name = threading.current_thread().name
    details = [
        f"[{timestamp}]",
        "[outputV3 process]",
        event,
        f"kind={kind}",
        f"name={name}",
        f"stage={stage or 'n/a'}",
        f"thread={thread_name}",
    ]
    if status is not None:
        details.append(f"status={status}")
    if elapsed is not None:
        details.append(f"elapsed={elapsed:.3f}s")
    with _PROCESS_PRINT_LOCK:
        # Databricks can drop stdout from imported modules and worker threads,
        # while the production orchestrator's configured logger is consistently
        # rendered in the notebook cell output.
        _base.logger.info(" ".join(details))


def _relation_metrics(df, enabled: bool) -> dict:
    if df is None or not enabled:
        return {
            "incoming_plan_nodes": None,
            "incoming_plan_depth": None,
            "incoming_partitions": None,
        }
    try:
        tree = (
            df._jdf.queryExecution()
            .optimizedPlan()
            .numberedTreeString()
        )
        lines = [line for line in tree.splitlines() if line.strip()]
        depths = [
            max(0, (len(line) - len(line.lstrip(" |:+-"))) // 2)
            for line in lines
        ]
    except Exception:
        lines, depths = [], []
    try:
        partitions = int(
            df._jdf.queryExecution()
            .sparkPlan()
            .outputPartitioning()
            .numPartitions()
        )
    except Exception:
        partitions = None
    return {
        "incoming_plan_nodes": len(lines) or None,
        "incoming_plan_depth": max(depths, default=0) if lines else None,
        "incoming_partitions": partitions,
    }


def _timed(stage: str, operation: str, fn: Callable) -> Callable:
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        active_stage = _ACTIVE_STAGE_OVERRIDE.get() or stage
        started = time.time()
        status = "PASS"
        _print_process("START", "helper", operation, active_stage)
        try:
            return fn(*args, **kwargs)
        except Exception:
            status = "FAIL"
            raise
        finally:
            elapsed = time.time() - started
            _record(
                active_stage,
                operation,
                elapsed,
            )
            _print_process(
                "DONE",
                "helper",
                operation,
                active_stage,
                elapsed=elapsed,
                status=status,
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
        self._next_wave = 0

    def submit_group(self, group, tasks) -> None:
        if self._executor is None or group not in self.enabled_groups:
            return
        with self._lock:
            pending = [
                task for task in tasks
                if (group, task[0]) not in self._futures
            ]
            if not pending:
                return
            self._next_wave += 1
            wave = self._next_wave
            for name, fn, args, kwargs in pending:
                key = (group, name)
                context = contextvars.copy_context()
                self._futures[key] = self._executor.submit(
                    context.run,
                    self._execute,
                    wave,
                    group,
                    name,
                    fn,
                    args,
                    kwargs,
                )

    def _execute(self, wave, group, name, fn, args, kwargs):
        started = time.time()
        status = "PASS"
        stage = {
            "common_dimensions": StageName.COMMON_READS.value,
            "common_inputs": StageName.COMMON_READS.value,
            "lookthrough_metadata": StageName.COMMON_READS.value,
            "mode_prep": StageName.MODE_PREP.value,
            "mode_prep_boundaries": StageName.MODE_PREP.value,
            "cpbt_boundaries": StageName.FUSED_CPBT.value,
            "effective_inputs": StageName.FUSED_EFFECTIVE.value,
            "fused_effective": StageName.FUSED_EFFECTIVE.value,
            "effective_boundaries": StageName.FUSED_EFFECTIVE.value,
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
        _print_process("START", "task", f"{group}.{name}", stage)
        try:
            return fn(*args, **kwargs)
        except Exception:
            status = "FAIL"
            raise
        finally:
            elapsed = time.time() - started
            _ACTIVE_STAGE_OVERRIDE.reset(stage_token)
            with self._lock:
                self._events.append(
                    {
                        "group": group,
                        "wave": wave,
                        "task": name,
                        "status": status,
                        "elapsed_seconds": round(elapsed, 3),
                        "thread": threading.current_thread().name,
                    }
                )
            _print_process(
                "DONE",
                "task",
                f"{group}.{name}",
                stage,
                elapsed=elapsed,
                status=status,
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
            with self._lock:
                self._next_wave += 1
                wave = self._next_wave
            for name, fn, args, kwargs in tasks:
                results[name] = self._execute(
                    wave, group, name, fn, args, kwargs
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


def _profile_pipeline_action(name, stage, df, action, cfg):
    started = time.time()
    status = "PASS"
    _print_process("START", "action", name, stage)
    metrics = _relation_metrics(
        df, bool(isinstance(cfg, dict) and cfg.get("profile_plan"))
    )
    depth_token = _NAMED_ACTION_DEPTH.set(_NAMED_ACTION_DEPTH.get() + 1)
    try:
        return profile_action(name, df, action, cfg)
    except Exception:
        status = "FAIL"
        raise
    finally:
        elapsed = time.time() - started
        _NAMED_ACTION_DEPTH.reset(depth_token)
        _record(
            stage,
            f"action:{name}",
            elapsed,
            **metrics,
        )
        _print_process(
            "DONE",
            "action",
            name,
            stage,
            elapsed=elapsed,
            status=status,
        )


_ORIGINAL_DF_ISEMPTY = DataFrame.isEmpty


def _profiled_dataframe_is_empty(self):
    """Name helper-internal probes while preserving the original action."""
    if _NAMED_ACTION_DEPTH.get() > 0:
        return _ORIGINAL_DF_ISEMPTY(self)
    frame = inspect.currentframe()
    caller_name = "unknown"
    try:
        frame = frame.f_back if frame is not None else None
        while frame is not None:
            module_name = str(frame.f_globals.get("__name__", ""))
            if (
                "usp_get_final_effective_percentage.output" in module_name
                and not module_name.endswith("outputV3.orchestrator")
            ):
                caller_name = frame.f_code.co_name
                break
            frame = frame.f_back
    finally:
        del frame
    stage = (
        _ACTIVE_STAGE_OVERRIDE.get()
        or FUNCTION_STAGE.get(caller_name)
        or StageName.MODE_PREP.value
    )
    cfg = _ACTIVE_RUN_CFG.get()
    return _profile_pipeline_action(
        f"{caller_name}.isEmpty",
        stage,
        self,
        lambda: _ORIGINAL_DF_ISEMPTY(self),
        cfg,
    )


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
    cfg["profile_plan"] = _ACTIVE_PROFILE_PLAN.get()
    cfg["plan_checkpoint_threshold"] = _ACTIVE_PLAN_THRESHOLD.get()
    cfg.update(_ACTIVE_EXPERIMENT.get() or {})
    cfg.setdefault("result_type", bound.arguments.get("ResultType"))
    if bound.arguments.get("VolumePath") is not None:
        cfg["volume_path"] = bound.arguments["VolumePath"]
    if bound.arguments.get("ExecutionID") is not None:
        cfg["execution_id"] = bound.arguments["ExecutionID"]
    initialize_named_checkpoint_policy(cfg, _ACTIVE_CHECKPOINT_MODE.get())
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
_base._profile_pipeline_action = _profile_pipeline_action
_base._drop_checkpoints = _drop_checkpoints_noop
_base.run_modes = _delegated_run_modes

# Time production functions without replacing or copying their business logic.
for _function_name, _stage_name in FUNCTION_STAGE.items():
    _function = getattr(_base, _function_name, None)
    if callable(_function):
        setattr(
            _base,
            _function_name,
            track_plan(_timed(_stage_name, _function_name, _function)),
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
    cfg = args[1] if len(args) > 1 and isinstance(args[1], dict) else {}
    if cfg.get("_output_v3_warning_probe_removal") == {
        "load_line_items": "line_items",
        "load_quarters": "quarters",
        "build_lookthrough_input_modes14": "lookthrough",
    }.get(name):
        fn = WARNING_PROBE_BUILDERS[
            cfg["_output_v3_warning_probe_removal"]
        ]
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
        line_items_fn = (
            WARNING_PROBE_BUILDERS["line_items"]
            if cfg.get("_output_v3_warning_probe_removal") == "line_items"
            else _PARALLEL_ORIGINALS["load_line_items"]
        )
        coordinator.submit_group(
            "common_inputs",
            tuple(
                (
                    name,
                    (
                        line_items_fn
                        if name == "load_line_items"
                        else (
                            WARNING_PROBE_BUILDERS["quarters"]
                            if name == "load_quarters"
                            and cfg.get(
                                "_output_v3_warning_probe_removal"
                            )
                            == "quarters"
                            else _PARALLEL_ORIGINALS[name]
                        )
                    ),
                    (spark, cfg),
                    {},
                )
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
        lookthrough_fn = (
            WARNING_PROBE_BUILDERS["lookthrough"]
            if cfg.get("_output_v3_warning_probe_removal") == "lookthrough"
            else _PARALLEL_ORIGINALS[
                "build_lookthrough_input_modes14"
            ]
        )
        coordinator.submit_group(
            "lookthrough_metadata",
            tuple(
                (
                    name,
                    (
                        lookthrough_fn
                        if name == "build_lookthrough_input_modes14"
                        else _PARALLEL_ORIGINALS[name]
                    ),
                    (spark, cfg),
                    {},
                )
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
        status = "PASS"
        stage = StageName.OUTPUT_WRITE.value
        _print_process("START", "write", table, stage)
        try:
            return profile_action(
                f"write:{table}",
                df,
                lambda: self.store_output_to_delta_table(
                    df, catalog, database, table, run_id
                ),
                _ACTIVE_RUN_CFG.get(),
            )
        except Exception:
            status = "FAIL"
            raise
        finally:
            elapsed = time.time() - started
            _record(
                stage,
                f"write:{table}",
                elapsed,
            )
            _print_process(
                "DONE",
                "write",
                table,
                stage,
                elapsed=elapsed,
                status=status,
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


def _performance_summary(wall, cfg, events, parallel_events):
    checkpoint_rows = (
        list(cfg.get("_checkpoint_policy_activity", ()))
        if isinstance(cfg, dict)
        else []
    )
    checkpoint_seconds = round(
        sum(float(row.get("elapsed_seconds", 0) or 0) for row in checkpoint_rows),
        3,
    )
    action_events = [
        row for row in events if str(row.get("operation", "")).startswith("action:")
    ]
    action_seconds = round(
        sum(float(row.get("elapsed_seconds", 0) or 0) for row in action_events),
        3,
    )
    parallel_by_group = defaultdict(list)
    for row in parallel_events:
        parallel_by_group[row["group"]].append(
            float(row.get("elapsed_seconds", 0) or 0)
        )
    parallel_critical = {
        group: round(max(values), 3)
        for group, values in sorted(parallel_by_group.items())
        if values
    }
    parallel_by_wave = defaultdict(list)
    for row in parallel_events:
        parallel_by_wave[int(row.get("wave", 0))].append(row)
    wave_critical_path = []
    for wave, rows in sorted(parallel_by_wave.items()):
        elapsed = max(
            float(row.get("elapsed_seconds", 0) or 0) for row in rows
        )
        wave_critical_path.append(
            {
                "wave": wave,
                "elapsed_seconds": round(elapsed, 3),
                "groups": sorted({row["group"] for row in rows}),
                "tasks": sorted(row["task"] for row in rows),
            }
        )
    critical_actions = sorted(
        [
            {
                "kind": "checkpoint",
                "name": row.get("name"),
                "stage": row.get("stage"),
                "elapsed_seconds": float(
                    row.get("elapsed_seconds", 0) or 0
                ),
                "incoming_plan_nodes": row.get("incoming_plan_nodes"),
                "incoming_plan_depth": row.get("incoming_plan_depth"),
                "incoming_partitions": row.get("incoming_partitions"),
            }
            for row in checkpoint_rows
        ]
        + [
            {
                "kind": "action",
                "name": str(row.get("operation", "")).removeprefix(
                    "action:"
                ),
                "stage": row.get("stage"),
                "elapsed_seconds": float(
                    row.get("elapsed_seconds", 0) or 0
                ),
                "incoming_plan_nodes": row.get("incoming_plan_nodes"),
                "incoming_plan_depth": row.get("incoming_plan_depth"),
                "incoming_partitions": row.get("incoming_partitions"),
            }
            for row in action_events
        ],
        key=lambda row: row["elapsed_seconds"],
        reverse=True,
    )
    return {
        "target_wall_seconds": 50.0,
        "wall_seconds": wall,
        "seconds_over_target": round(max(0.0, wall - 50.0), 3),
        "checkpoint_action_seconds": checkpoint_seconds,
        "explicit_action_seconds": action_seconds,
        "checkpoint_count": len(checkpoint_rows),
        "explicit_action_count": len(action_events),
        "parallel_group_critical_seconds": parallel_critical,
        "parallel_wave_critical_path": wave_critical_path,
        "parallel_wave_total_seconds": round(
            sum(row["elapsed_seconds"] for row in wave_critical_path), 3
        ),
        "critical_actions": critical_actions,
    }


def _run_profiled(fn, *args, **kwargs):
    raw_threads = kwargs.pop("MaxThreads", kwargs.pop("max_threads", 4))
    raw_groups = kwargs.pop(
        "ParallelGroups",
        kwargs.pop("parallel_groups", ",".join(sorted(_ALL_PARALLEL_GROUPS))),
    )
    checkpoint_mode = int(
        kwargs.pop("CheckpointMode", kwargs.pop("checkpoint_mode", 4))
    )
    if checkpoint_mode not in {1, 2, 3, 4, 5}:
        raise ValueError("CheckpointMode must be one of 1, 2, 3, 4, 5")
    profile_plan = _as_bool(
        kwargs.pop("ProfilePlan", kwargs.pop("profile_plan", False))
    )
    plan_threshold = int(
        kwargs.pop(
            "PlanCheckpointThreshold",
            kwargs.pop("plan_checkpoint_threshold", 30),
        )
    )
    experiment = {
        "_output_v3_experiment_id": str(
            kwargs.pop(
                "ExperimentID",
                kwargs.pop("experiment_id", "baseline"),
            )
        ).strip() or "baseline",
        "_output_v3_shuffle_partitions": int(
            kwargs.pop(
                "SqlShufflePartitions",
                kwargs.pop("sql_shuffle_partitions", 32),
            )
        ),
        "_output_v3_warning_probe_removal": str(
            kwargs.pop(
                "WarningProbeRemoval",
                kwargs.pop("warning_probe_removal", "off"),
            )
        ).strip().lower(),
        "_output_v3_missing_entity_identity": _as_bool(
            kwargs.pop(
                "MissingEntityIdentity",
                kwargs.pop("missing_entity_identity", True),
            )
        ),
        "_output_v3_cpbt_input_break": str(
            kwargs.pop(
                "CpbtInputBreak",
                kwargs.pop("cpbt_input_break", "both"),
            )
        ).strip().lower(),
        "_output_v3_cpbt_post_tag_entity_break": _as_bool(
            kwargs.pop(
                "CpbtPostTagEntityBreak",
                kwargs.pop("cpbt_post_tag_entity_break", True),
            )
        ),
        "_output_v3_compact_all_entities": _as_bool(
            kwargs.pop(
                "CompactAllEntities",
                kwargs.pop("compact_all_entities", True),
            )
        ),
        "_output_v3_cpbt_narrow_anti_keys": _as_bool(
            kwargs.pop(
                "CpbtNarrowAntiKeys",
                kwargs.pop("cpbt_narrow_anti_keys", True),
            )
        ),
        "_output_v3_cpbt_transfer_prefilter": _as_bool(
            kwargs.pop(
                "CpbtTransferPrefilter",
                kwargs.pop("cpbt_transfer_prefilter", True),
            )
        ),
        "_output_v3_cpbt_drop_tracking_match": _as_bool(
            kwargs.pop(
                "CpbtDropTrackingMatch",
                kwargs.pop("cpbt_drop_tracking_match", True),
            )
        ),
        "_output_v3_batch_footnote_line_ids": _as_bool(
            kwargs.pop(
                "BatchFootnoteLineIds",
                kwargs.pop("batch_footnote_line_ids", True),
            )
        ),
        "_output_v3_footnote_shared_lineage": _as_bool(
            kwargs.pop(
                "FootnoteSharedLineage",
                kwargs.pop("footnote_shared_lineage", True),
            )
        ),
        "_output_v3_single_pickup_antijoin": _as_bool(
            kwargs.pop(
                "SinglePickupAntiJoin",
                kwargs.pop("single_pickup_antijoin", True),
            )
        ),
        "_output_v3_materialize_effective_inputs": _as_bool(
            kwargs.pop(
                "MaterializeEffectiveInputs",
                kwargs.pop("materialize_effective_inputs", True),
            )
        ),
        "_output_v3_parallel_effective": _as_bool(
            kwargs.pop(
                "ParallelEffective",
                kwargs.pop("parallel_effective", True),
            )
        ),
        "_output_v3_target_checkpoint": str(
            kwargs.pop(
                "TargetCheckpoint",
                kwargs.pop("target_checkpoint", ""),
            )
        ).strip(),
        "_output_v3_target_partition_strategy": str(
            kwargs.pop(
                "TargetPartitionStrategy",
                kwargs.pop("target_partition_strategy", "off"),
            )
        ).strip().lower(),
        "_output_v3_target_partitions": int(
            kwargs.pop(
                "TargetPartitions",
                kwargs.pop("target_partitions", 0),
            )
        ),
        "_output_v3_target_partition_keys": [
            item.strip()
            for item in str(
                kwargs.pop(
                    "TargetPartitionKeys",
                    kwargs.pop("target_partition_keys", ""),
                )
            ).split(",")
            if item.strip()
        ],
        "_output_v3_business_optimization": str(
            kwargs.pop(
                "BusinessOptimization",
                kwargs.pop(
                    "business_optimization",
                    "broadcast_entity_partners",
                ),
            )
        ).strip().lower(),
    }
    if experiment["_output_v3_shuffle_partitions"] < 1:
        raise ValueError("SqlShufflePartitions must be >= 1")
    if experiment["_output_v3_warning_probe_removal"] not in {
        "off",
        "line_items",
        "quarters",
        "lookthrough",
    }:
        raise ValueError(
            "WarningProbeRemoval must be off, line_items, quarters, "
            "or lookthrough"
        )
    if experiment["_output_v3_cpbt_input_break"] not in {
        "off",
        "non_dated",
        "dated",
        "both",
    }:
        raise ValueError(
            "CpbtInputBreak must be off, non_dated, dated, or both"
        )
    if experiment["_output_v3_target_partition_strategy"] not in {
        "off",
        "coalesce",
        "repartition",
    }:
        raise ValueError(
            "TargetPartitionStrategy must be off, coalesce, or repartition"
        )
    if (
        experiment["_output_v3_target_partition_strategy"] != "off"
        and (
            not experiment["_output_v3_target_checkpoint"]
            or experiment["_output_v3_target_partitions"] < 1
        )
    ):
        raise ValueError(
            "TargetCheckpoint and TargetPartitions >= 1 are required for "
            "targeted partitioning"
        )
    if (
        experiment["_output_v3_target_partition_strategy"] == "repartition"
        and not experiment["_output_v3_target_partition_keys"]
    ):
        raise ValueError(
            "TargetPartitionKeys is required for keyed repartition"
        )
    if experiment["_output_v3_business_optimization"] not in {
        "off",
        "broadcast_entity_partners",
    }:
        raise ValueError(
            "BusinessOptimization must be off or broadcast_entity_partners"
        )
    call_arguments = inspect.signature(fn).bind_partial(
        *args, **kwargs
    ).arguments
    supplied_cfg = call_arguments.get("cfg")
    volume_path = call_arguments.get("VolumePath")
    if checkpoint_mode == 3 and not (
        volume_path
        or (
            isinstance(supplied_cfg, dict)
            and (
                supplied_cfg.get("checkpoint_volume_path")
                or supplied_cfg.get("volume_path")
                or supplied_cfg.get("VolumePath")
            )
        )
    ):
        raise ValueError("CheckpointMode 3 requires VolumePath")
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
    mode_token = _ACTIVE_CHECKPOINT_MODE.set(checkpoint_mode)
    profile_token = _ACTIVE_PROFILE_PLAN.set(profile_plan)
    threshold_token = _ACTIVE_PLAN_THRESHOLD.set(plan_threshold)
    experiment_token = _ACTIVE_EXPERIMENT.set(experiment)
    coordinator = _Coordinator(max_threads, requested_groups)
    coordinator_token = _ACTIVE_COORDINATOR.set(coordinator)
    cfg_token = _ACTIVE_RUN_CFG.set(None)
    plan_token = checkpoint_plan_token = action_token = None
    builder_records = []
    checkpoint_records = []
    action_records = []
    if profile_plan:
        plan_token, builder_records = start_plan_profile()
        checkpoint_plan_token, checkpoint_records = (
            start_checkpoint_plan_profile()
        )
        action_token, action_records = start_action_profile()
    started = time.time()
    succeeded = False
    run_name = getattr(fn, "__name__", "outputV3")
    _print_process("START", "run", run_name, "pipeline")
    previous_is_empty = DataFrame.isEmpty
    if profile_plan:
        DataFrame.isEmpty = _profiled_dataframe_is_empty
    try:
        result = fn(*args, **kwargs)
        succeeded = True
    finally:
        if profile_plan:
            DataFrame.isEmpty = previous_is_empty
        coordinator.close()
        if action_token is not None:
            finish_action_profile(action_token)
        if checkpoint_plan_token is not None:
            finish_checkpoint_plan_profile(checkpoint_plan_token)
        if plan_token is not None:
            finish_plan_profile(plan_token)
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
        performance = _performance_summary(
            wall, cfg, events, coordinator.events
        )
        reports = {"builder": [], "checkpoint": [], "action": []}
        if profile_plan:
            for label, records, key in (
                ("BUILDER", builder_records, "builder"),
                ("CHECKPOINT", checkpoint_records, "checkpoint"),
                ("ACTION", action_records, "action"),
            ):
                reports[key] = plan_profile_report(
                    records, plan_threshold, label=label
                )
        _LAST_RUN_PROFILE.clear()
        _LAST_RUN_PROFILE.update(
            {
                "updated_wall_seconds": wall,
                "checkpoint_mode": checkpoint_mode,
                "profile_plan": profile_plan,
                "plan_checkpoint_threshold": plan_threshold,
                "experiment_id": experiment["_output_v3_experiment_id"],
                "requested_shuffle_partitions": experiment[
                    "_output_v3_shuffle_partitions"
                ],
                "experiment_settings": {
                    key.removeprefix("_output_v3_"): value
                    for key, value in experiment.items()
                },
                "effective_spark_config": (
                    dict(cfg.get("_output_v3_effective_spark_config", {}))
                    if isinstance(cfg, dict)
                    else {}
                ),
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
                "checkpoint_policy": f"checkpoint_v2_mode_{checkpoint_mode}",
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
                "plan_profile": reports["builder"],
                "checkpoint_plan_profile": reports["checkpoint"],
                "action_profile": reports["action"],
                "performance_summary": performance,
            }
        )
        _print_process(
            "DONE",
            "run",
            run_name,
            "pipeline",
            elapsed=wall,
            status="PASS" if succeeded else "FAIL",
        )
        _ACTIVE_RUN_CFG.reset(cfg_token)
        _ACTIVE_COORDINATOR.reset(coordinator_token)
        _ACTIVE_EVENTS.reset(event_token)
        _ACTIVE_PROFILE_PLAN.reset(profile_token)
        _ACTIVE_CHECKPOINT_MODE.reset(mode_token)
        _ACTIVE_PLAN_THRESHOLD.reset(threshold_token)
        _ACTIVE_EXPERIMENT.reset(experiment_token)
    print(
        f"[outputV3 timing] wall={wall:.3f}s "
        f"checkpoint_mode={checkpoint_mode} profile_plan={profile_plan} "
        f"experiment={experiment['_output_v3_experiment_id']} "
        f"threads={max_threads} "
        f"groups={','.join(sorted(requested_groups)) or 'none'}"
    )
    effective_config = _LAST_RUN_PROFILE.get("effective_spark_config", {})
    print(
        "[outputV3 config] "
        f"shuffle_partitions={effective_config.get('spark.sql.shuffle.partitions')} "
        f"aqe_enabled={effective_config.get('spark.sql.adaptive.enabled')} "
        "advisory_partition_bytes="
        f"{effective_config.get('spark.sql.adaptive.advisoryPartitionSizeInBytes')}"
    )
    for stage in _LAST_RUN_PROFILE.get("stage_timings", ()):
        print(
            f"[outputV3 timing] stage={stage['stage']} "
            f"elapsed={stage['elapsed_seconds']:.3f}s "
            f"calls={stage['calls']}"
        )
    perf = _LAST_RUN_PROFILE.get("performance_summary", {})
    print(
        "[outputV3 budget] "
        f"target={perf.get('target_wall_seconds', 50.0):.1f}s "
        f"over={perf.get('seconds_over_target', 0.0):.3f}s "
        f"checkpoint_actions={perf.get('checkpoint_action_seconds', 0.0):.3f}s "
        f"explicit_actions={perf.get('explicit_action_seconds', 0.0):.3f}s"
    )
    for row in perf.get("critical_actions", ())[:10]:
        print(
            "[outputV3 critical] "
            f"kind={row['kind']} name={row['name']} stage={row['stage']} "
            f"elapsed={row['elapsed_seconds']:.3f}s "
            f"nodes={row.get('incoming_plan_nodes')} "
            f"depth={row.get('incoming_plan_depth')} "
            f"partitions={row.get('incoming_partitions')}"
        )
    for row in perf.get("parallel_wave_critical_path", ()):
        print(
            "[outputV3 wave] "
            f"wave={row['wave']} elapsed={row['elapsed_seconds']:.3f}s "
            f"groups={','.join(row['groups'])} "
            f"tasks={','.join(row['tasks'])}"
        )
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
                "plan_profile",
                "checkpoint_plan_profile",
                "action_profile",
            )
        },
        "enabled_parallel_groups": list(
            _LAST_RUN_PROFILE.get("enabled_parallel_groups", ())
        ),
        "performance_summary": {
            **_LAST_RUN_PROFILE.get("performance_summary", {}),
            "parallel_group_critical_seconds": dict(
                _LAST_RUN_PROFILE.get("performance_summary", {}).get(
                    "parallel_group_critical_seconds", {}
                )
            ),
            "parallel_wave_critical_path": [
                dict(item)
                for item in _LAST_RUN_PROFILE.get(
                    "performance_summary", {}
                ).get("parallel_wave_critical_path", ())
            ],
            "critical_actions": [
                dict(item)
                for item in _LAST_RUN_PROFILE.get(
                    "performance_summary", {}
                ).get("critical_actions", ())
            ],
        },
    }


run_mode = run_final_effective_percentages

__all__ = [
    "get_last_run_profile",
    "run_final_effective_percentages",
    "run_mode",
    "run_modes",
]
