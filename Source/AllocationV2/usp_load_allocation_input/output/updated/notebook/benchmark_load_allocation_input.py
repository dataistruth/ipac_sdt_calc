# Databricks notebook source
# MAGIC %md
# MAGIC # A/B benchmark — `usp_load_allocation_input`
# MAGIC
# MAGIC Compares production `output.load_allocation_input` with
# MAGIC `output.updated.load_allocation_input`.
# MAGIC
# MAGIC This notebook lives in `output/updated/notebook/`. Sync Python modules
# MAGIC into `output/updated/`, not this folder.
# MAGIC
# MAGIC **Do not run another process for this RunID during the benchmark.**

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Monolith Source/",
)
dbutils.widgets.text("number_of_runs", "1", "2. A/B passes")
dbutils.widgets.dropdown(
    "ExecutionOrder",
    "original_first",
    ["original_first", "updated_first", "alternate"],
    "3. Execution order",
)
dbutils.widgets.text("EntityID", "115", "4. EntityID")
dbutils.widgets.text("ClientID", "15348", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "16560", "7. RunID")
dbutils.widgets.text("CatalogName", "QA7", "8. Catalog")
dbutils.widgets.text("SchemaName", "IPC_2025_QA7_15348", "9. Schema")
dbutils.widgets.text(
    "VolumePath",
    "/Volumes/qa7/datavolume/databrickdata/checkpoint",
    "10. Volume path",
)
dbutils.widgets.text("MaxThreads", "4", "11. Updated parallel workers")
dbutils.widgets.dropdown(
    "ProfilePlan",
    "on",
    ["off", "on"],
    "12. Plan profiler",
)
dbutils.widgets.text(
    "PlanCheckpointThreshold",
    "30",
    "13. Plan checkpoint threshold",
)
dbutils.widgets.dropdown(
    "CheckpointBackend",
    "delta",
    ["delta", "local"],
    "14. Checkpoint backend",
)
dbutils.widgets.text(
    "SqlShufflePartitions",
    "",
    "15. spark.sql.shuffle.partitions (blank=unchanged)",
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs").strip() or "1")
execution_order = dbutils.widgets.get("ExecutionOrder").strip().lower()
entity_id = int(dbutils.widgets.get("EntityID").strip())
client_id = int(dbutils.widgets.get("ClientID").strip())
tax_period_id = int(dbutils.widgets.get("TaxPeriodID").strip())
run_id = int(dbutils.widgets.get("RunID").strip())
catalog_name = dbutils.widgets.get("CatalogName").strip()
schema_name = dbutils.widgets.get("SchemaName").strip()
volume_path = dbutils.widgets.get("VolumePath").strip()
parallel_workers = int(dbutils.widgets.get("MaxThreads").strip() or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
plan_checkpoint_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold").strip() or "30"
)
checkpoint_backend = dbutils.widgets.get("CheckpointBackend").strip().lower()
sql_shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if parallel_workers < 1:
    raise ValueError("MaxThreads must be >= 1")
if checkpoint_backend not in ("delta", "local"):
    raise ValueError("CheckpointBackend must be 'delta' or 'local'")
if execution_order not in ("original_first", "updated_first", "alternate"):
    raise ValueError("ExecutionOrder is invalid")

if sql_shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", sql_shuffle_partitions)
    print(
        "[config] spark.sql.shuffle.partitions = "
        f"{spark.conf.get('spark.sql.shuffle.partitions')}"
    )

print(f"source_path         : {source_path}")
print(f"number_of_runs      : {number_of_runs}")
print(f"ExecutionOrder      : {execution_order}")
print(f"EntityID            : {entity_id}")
print(f"ClientID            : {client_id}")
print(f"TaxPeriodID         : {tax_period_id}")
print(f"RunID               : {run_id}")
print(f"CatalogName         : {catalog_name}")
print(f"SchemaName          : {schema_name}")
print(f"MaxThreads          : {parallel_workers}")
print(f"ProfilePlan         : {profile_plan}")
print(f"CheckpointBackend   : {checkpoint_backend}")

# COMMAND ----------

import importlib
import json
import os
import sys
import time
from datetime import datetime
from typing import Any

if not os.path.isdir(source_path):
    raise RuntimeError(f"Source path does not exist: {source_path}")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_load_allocation_input"
ORIGINAL_MODULE = f"{PACKAGE}.output.load_allocation_input"
UPDATED_MODULE = f"{PACKAGE}.output.updated.load_allocation_input"
_SHARED_PKGS = ("AllocationV2.plan_profiler",)


def _assert_updated_package_synced() -> None:
    updated_dir = os.path.join(
        source_path, *PACKAGE.split("."), "output", "updated"
    )
    required = [
        "__init__.py",
        "load_allocation_input.py",
        "checkpoint.py",
        "plan_profiler.py",
        "parent.py",
        "step_timer.py",
    ]
    print(f"[sync check] package dir: {updated_dir}")
    missing = [
        name
        for name in required
        if not os.path.isfile(os.path.join(updated_dir, name))
    ]
    if missing:
        raise ModuleNotFoundError(
            f"output/updated is missing: {', '.join(missing)}. "
            "Sync the entire output/updated folder (not notebook/) "
            "and restart Python."
        )


def _import_fresh(module_name: str):
    for loaded in list(sys.modules):
        if (
            loaded == PACKAGE
            or loaded.startswith(PACKAGE + ".")
            or any(
                loaded == pkg or loaded.startswith(pkg + ".")
                for pkg in _SHARED_PKGS
            )
        ):
            del sys.modules[loaded]
    importlib.invalidate_caches()
    module = importlib.import_module(module_name)
    print(f"imported {module_name}: {module.__file__}")
    return module


def _extract_elapsed(result: Any) -> float | None:
    if isinstance(result, dict):
        val = result.get("elapsed_seconds")
        if val is not None:
            return float(val)
    if isinstance(result, str) and result.strip().startswith("{"):
        try:
            parsed = json.loads(result)
            val = parsed.get("elapsed_seconds")
            if val is not None:
                return float(val)
        except json.JSONDecodeError:
            pass
    return None


def _order_for_pass(pass_num: int) -> list[str]:
    if execution_order == "updated_first":
        return ["updated", "original"]
    if execution_order == "alternate" and pass_num % 2 == 0:
        return ["updated", "original"]
    return ["original", "updated"]


def _run_pipeline(variant: str, pass_num: int) -> dict:
    module_name = ORIGINAL_MODULE if variant == "original" else UPDATED_MODULE
    runner = _import_fresh(module_name)
    run_kwargs = {
        "EntityID": entity_id,
        "ClientID": client_id,
        "TaxPeriodID": tax_period_id,
        "RunID": run_id,
        "CatalogName": catalog_name,
        "SchemaName": schema_name,
    }
    if variant == "updated":
        run_kwargs["VolumePath"] = volume_path
        run_kwargs["parallel_config_workers"] = parallel_workers
        run_kwargs["parallel_write_workers"] = parallel_workers
        run_kwargs["CheckpointBackend"] = checkpoint_backend
        if profile_plan:
            run_kwargs["profile_plan"] = True
            run_kwargs["plan_checkpoint_threshold"] = plan_checkpoint_threshold

    started_at = datetime.now()
    t0 = time.time()
    print(f"\n=== pass {pass_num} | {variant} | start {started_at} ===")
    try:
        result = runner.run_load_allocation_input(spark, **run_kwargs)
        status = "SUCCESS"
        error = None
    except Exception as exc:
        result = None
        status = "FAIL"
        error = str(exc)
        print(f"ERROR ({variant}): {exc}")

    wall_seconds = round(time.time() - t0, 3)
    ended_at = datetime.now()
    row = {
        "pass": pass_num,
        "variant": variant,
        "module": module_name,
        "parallel_workers": parallel_workers if variant == "updated" else None,
        "status": status,
        "wall_seconds": wall_seconds,
        "reported_elapsed_seconds": _extract_elapsed(result),
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "error": error,
    }
    print(
        f"=== pass {pass_num} | {variant} | wall={wall_seconds}s "
        f"| reported={row['reported_elapsed_seconds']} | end {ended_at} ==="
    )
    return row


_assert_updated_package_synced()

# COMMAND ----------

benchmark_rows: list[dict] = []

for pass_num in range(1, number_of_runs + 1):
    print(f"\n{'#' * 60}")
    print(f"# BENCHMARK PASS {pass_num} / {number_of_runs}")
    print(f"{'#' * 60}")
    for variant in _order_for_pass(pass_num):
        benchmark_rows.append(_run_pipeline(variant, pass_num))

print(f"\nCompleted {number_of_runs} pass(es) — {len(benchmark_rows)} runs recorded")

# COMMAND ----------

import pandas as pd

results_df = pd.DataFrame(benchmark_rows)
display(results_df)

# COMMAND ----------

summary = (
    results_df.groupby("variant", as_index=False)
    .agg(
        runs=("wall_seconds", "count"),
        wall_seconds_min=("wall_seconds", "min"),
        wall_seconds_mean=("wall_seconds", "mean"),
        wall_seconds_max=("wall_seconds", "max"),
        wall_seconds_total=("wall_seconds", "sum"),
    )
    .sort_values("variant")
)
summary["wall_seconds_mean"] = summary["wall_seconds_mean"].round(3)
summary["wall_seconds_total"] = summary["wall_seconds_total"].round(3)
display(summary)

if len(summary) == 2:
    orig_mean = summary.loc[summary["variant"] == "original", "wall_seconds_mean"].iloc[0]
    upd_mean = summary.loc[summary["variant"] == "updated", "wall_seconds_mean"].iloc[0]
    delta = round(upd_mean - orig_mean, 3)
    pct = round((delta / orig_mean) * 100, 1) if orig_mean else None
    print(
        f"Mean wall time — original: {orig_mean}s | updated: {upd_mean}s | "
        f"delta: {delta}s ({pct}% vs original)"
    )

# COMMAND ----------

pass_compare = results_df.pivot_table(
    index="pass",
    columns="variant",
    values="wall_seconds",
    aggfunc="first",
)
if "original" in pass_compare.columns and "updated" in pass_compare.columns:
    pass_compare["delta_seconds"] = pass_compare["updated"] - pass_compare["original"]
    pass_compare["delta_pct"] = (
        (pass_compare["delta_seconds"] / pass_compare["original"]) * 100
    ).round(1)
display(pass_compare.round(3))
