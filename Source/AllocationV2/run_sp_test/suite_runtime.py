"""Shared helpers for the 12-SP suite notebooks."""

from __future__ import annotations

import importlib
import inspect
import json
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from AllocationV2.run_sp_test.sp_catalog import SP_SPECS

IMPORT_LOCK = threading.Lock()


def selected_specs(sp_filter):
    names = [part.strip() for part in (sp_filter or "").split(",") if part.strip()]
    if not names:
        return list(SP_SPECS)
    wanted = {name.lower() for name in names}
    found = [spec for spec in SP_SPECS if spec["name"].lower() in wanted]
    missing = wanted - {spec["name"].lower() for spec in found}
    if missing:
        raise ValueError(f"unknown SP names: {sorted(missing)}")
    return found


def batches(items, size):
    width = max(1, int(size or 1))
    for index in range(0, len(items), width):
        yield items[index : index + width]


def workspace_notebook_path(source_path, relative):
    root = (source_path or "").rstrip("/")
    if root.startswith("/Workspace/"):
        root = root[len("/Workspace") :]
    return f"{root}/{relative}"


def reported_seconds(result):
    parsed = result
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except (TypeError, ValueError):
            return None
    if not isinstance(parsed, dict):
        return None
    for key in ("elapsed_seconds", "elapsed", "ElapsedSeconds", "total_time_seconds"):
        value = parsed.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None


def filter_kwargs(fn, kwargs):
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return kwargs
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
        return kwargs
    return {key: value for key, value in kwargs.items() if key in params}


def clear_modules(package):
    shared = "AllocationV2.plan_profiler"
    common = "Common_V2"
    for name in list(sys.modules):
        if (
            name == package
            or name.startswith(package + ".")
            or name == shared
            or name.startswith(shared + ".")
            or name == common
            or name.startswith(common + ".")
        ):
            del sys.modules[name]
    importlib.invalidate_caches()


def prepare_source(source_path):
    sys.path[:] = [entry for entry in sys.path if entry != source_path]
    sys.path.insert(0, source_path)
    with IMPORT_LOCK:
        importlib.import_module("Common_V2.core.checkpoint_V2")


def load_runner(package, module_name):
    with IMPORT_LOCK:
        clear_modules(package)
        importlib.import_module("Common_V2.core.checkpoint_V2")
        try:
            importlib.import_module("AllocationV2.plan_profiler")
        except Exception as exc:
            print(f"[suite] AllocationV2.plan_profiler not loaded: {exc}")
        return importlib.import_module(module_name)


def identity_for(spec, overrides):
    use_locked = overrides.get("use_sp_identity", True)
    return {
        "EntityID": spec["entity_id"] if use_locked else int(overrides["entity_id"]),
        "ClientID": int(overrides["client_id"]),
        "TaxPeriodID": int(overrides["tax_period_id"]),
        "RunID": spec["run_id"] if use_locked else int(overrides["run_id"]),
        "CatalogName": spec["catalog"] if use_locked else overrides["catalog"],
        "SchemaName": spec["schema"] if use_locked else overrides["schema"],
    }


def call_entry(spark, spec, module_name, identity, extra, updated, execution_profile, profile_plan):
    runner = load_runner(spec["package"], module_name)
    fn = getattr(runner, spec["entry"])
    kwargs = dict(identity, **extra)
    if updated:
        kwargs["ExecutionProfile"] = execution_profile
        if profile_plan:
            kwargs["profile_plan"] = True
            kwargs["ProfilePlan"] = True
    else:
        kwargs.setdefault("ExecutionProfile", execution_profile)
    kwargs = filter_kwargs(fn, kwargs)
    started = time.time()
    result = fn(spark, **kwargs)
    wall = round(time.time() - started, 3)
    return wall, reported_seconds(result)


def notebook_args(spec, identity, execution_profile, number_of_runs, profile_plan, source_path):
    args = {
        "source_path": source_path,
        "EntityID": str(identity["EntityID"]),
        "ClientID": str(identity["ClientID"]),
        "TaxPeriodID": str(identity["TaxPeriodID"]),
        "RunID": str(identity["RunID"]),
        "CatalogName": identity["CatalogName"],
        "SchemaName": identity["SchemaName"],
        "ExecutionProfile": execution_profile,
    }
    if number_of_runs is not None:
        args["number_of_runs"] = str(number_of_runs)
    if profile_plan is not None:
        args["ProfilePlan"] = "on" if profile_plan else "off"
    return args


def run_via_notebook(dbutils, source_path, relative, args, timeout_seconds):
    path = workspace_notebook_path(source_path, relative)
    started = time.time()
    dbutils.notebook.run(path, timeout_seconds, args)
    return round(time.time() - started, 3)


def run_specs(specs, worker, parallel):
    rows = []
    for batch_no, batch in enumerate(batches(specs, parallel), start=1):
        print(
            f"[suite] batch {batch_no} workers={min(len(batch), max(1, int(parallel)))} "
            f"sps={[spec['name'] for spec in batch]}",
            flush=True,
        )
        if int(parallel) <= 1 or len(batch) == 1:
            for spec in batch:
                rows.append(worker(spec))
            continue
        with ThreadPoolExecutor(max_workers=int(parallel)) as pool:
            futures = {pool.submit(worker, spec): spec["name"] for spec in batch}
            for future in as_completed(futures):
                try:
                    rows.append(future.result())
                except Exception as exc:
                    name = futures[future]
                    rows.append(
                        {
                            "sp": name,
                            "status": "FAIL",
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    print(f"[suite] {name} failed\n{traceback.format_exc()}", flush=True)
    order = {spec["name"]: index for index, spec in enumerate(specs)}
    rows.sort(key=lambda row: order.get(row.get("sp"), 999))
    return rows
