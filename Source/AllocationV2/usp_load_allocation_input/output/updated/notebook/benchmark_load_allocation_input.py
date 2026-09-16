# Databricks notebook source
# MAGIC %md
# MAGIC # A/B benchmark — `usp_load_allocation_input`
# MAGIC
# MAGIC Runs unchanged production and `output.updated` against identical inputs.
# MAGIC Each variant starts with clean RunID output partitions. Parity compares
# MAGIC schemas, row counts, business aggregates, and order-independent hashes.
# MAGIC
# MAGIC **Do not run another process for this RunID during the benchmark.**

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Monolith Source/",
)
dbutils.widgets.text("number_of_runs", "2", "2. A/B passes")
dbutils.widgets.dropdown(
    "ExecutionOrder",
    "alternate",
    ["alternate", "original_first", "updated_first"],
    "3. Execution order",
)
dbutils.widgets.text("EntityID", "115", "4. EntityID")
dbutils.widgets.text("ClientID", "15348", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "16560", "7. RunID")
dbutils.widgets.text("CatalogName", "QA7", "8. Catalog")
dbutils.widgets.text("SchemaName", "IPC_2025_QA7_15348", "9. Schema")
dbutils.widgets.dropdown(
    "ResultType", "deltalake", ["deltalake"], "10. Result type"
)
dbutils.widgets.text(
    "VolumePath",
    "/Volumes/qa7/datavolume/databrickdata/checkpoint",
    "11. Volume path",
)
dbutils.widgets.text("MaxThreads", "4", "12. Updated max threads")
dbutils.widgets.dropdown(
    "ProfilePlan", "on", ["off", "on"], "13. Plan profiler"
)
dbutils.widgets.text(
    "PlanCheckpointThreshold", "30", "14. Plan threshold"
)
dbutils.widgets.dropdown(
    "CheckpointBackend",
    "delta",
    ["delta", "local"],
    "15. Checkpoint backend",
)
dbutils.widgets.text(
    "SqlShufflePartitions",
    "",
    "16. spark.sql.shuffle.partitions (blank=unchanged)",
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs").strip() or "2")
execution_order = dbutils.widgets.get("ExecutionOrder").strip().lower()
entity_id = int(dbutils.widgets.get("EntityID").strip())
client_id = int(dbutils.widgets.get("ClientID").strip())
tax_period_id = int(dbutils.widgets.get("TaxPeriodID").strip())
run_id = int(dbutils.widgets.get("RunID").strip())
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
result_type = dbutils.widgets.get("ResultType").strip()
volume_path = dbutils.widgets.get("VolumePath").strip()
max_threads = int(dbutils.widgets.get("MaxThreads").strip() or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
plan_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold").strip() or "30"
)
checkpoint_backend = dbutils.widgets.get("CheckpointBackend").strip().lower()
shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if not 1 <= max_threads <= 8:
    raise ValueError("MaxThreads must be between 1 and 8")
if checkpoint_backend not in {"delta", "local"}:
    raise ValueError("CheckpointBackend must be delta or local")
if execution_order not in {"alternate", "original_first", "updated_first"}:
    raise ValueError("Invalid ExecutionOrder")
if result_type.lower() != "deltalake":
    raise ValueError("Parity benchmark requires ResultType=deltalake")
if shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

print(f"source_path             : {source_path}")
print(f"number_of_runs          : {number_of_runs}")
print(f"ExecutionOrder          : {execution_order}")
print(f"RunID                   : {run_id}")
print(f"MaxThreads              : {max_threads}")
print(f"ProfilePlan             : {profile_plan}")
print(f"PlanCheckpointThreshold : {plan_threshold}")
print(f"CheckpointBackend       : {checkpoint_backend}")
print(
    "SqlShufflePartitions    : "
    + (
        spark.conf.get("spark.sql.shuffle.partitions")
        if shuffle_partitions
        else "unchanged"
    )
)

# COMMAND ----------

import importlib
import json
import os
import sys
import time
from datetime import datetime

if not os.path.isdir(source_path):
    raise RuntimeError(f"Source path does not exist: {source_path}")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_load_allocation_input"
ORIGINAL_MODULE = f"{PACKAGE}.output.load_allocation_input"
UPDATED_MODULE = f"{PACKAGE}.output.updated.load_allocation_input"
RECONCILE_MODULE = f"{PACKAGE}.output.updated.output_reconcile"
SHARED_PACKAGE = "AllocationV2.plan_profiler"


def _clear_modules():
    for loaded in list(sys.modules):
        if (
            loaded == PACKAGE
            or loaded.startswith(PACKAGE + ".")
            or loaded == SHARED_PACKAGE
            or loaded.startswith(SHARED_PACKAGE + ".")
        ):
            del sys.modules[loaded]
    importlib.invalidate_caches()


def _assert_updated_package_synced():
    updated_dir = os.path.join(
        source_path, *PACKAGE.split("."), "output", "updated"
    )
    required = [
        "__init__.py",
        "load_allocation_input.py",
        "ai_shared_views.py",
        "ai_pfic_flowup_service.py",
        "checkpoint.py",
        "plan_profiler.py",
        "parent.py",
        "parallel_helpers.py",
        "shared_views.py",
        "validation_parallel.py",
        "finalize_parallel.py",
        "output_reconcile.py",
    ]
    missing = [
        name
        for name in required
        if not os.path.isfile(os.path.join(updated_dir, name))
    ]
    print(f"[sync check] package dir: {updated_dir}")
    if missing:
        raise ModuleNotFoundError(
            f"output/updated is missing: {', '.join(missing)}. "
            "Sync the entire output/updated directory and restart Python."
        )


def _import_fresh(module_name):
    _clear_modules()
    module = importlib.import_module(module_name)
    print(f"imported {module_name}: {module.__file__}")
    return module


def _reported_seconds(result):
    if isinstance(result, dict):
        value = result.get("elapsed_seconds")
        return float(value) if value is not None else None
    if isinstance(result, str) and result.strip().startswith("{"):
        try:
            value = json.loads(result).get("elapsed_seconds")
            return float(value) if value is not None else None
        except Exception:
            return None
    return None


def _order(pass_number):
    if execution_order == "updated_first":
        return ["updated", "original"]
    if execution_order == "alternate" and pass_number % 2 == 0:
        return ["updated", "original"]
    return ["original", "updated"]


_assert_updated_package_synced()
_clear_modules()
reconcile = importlib.import_module(RECONCILE_MODULE)
purge_run = reconcile.purge_output_partitions_for_run
capture_metrics = reconcile.capture_output_metrics
compare_metrics = reconcile.compare_output_metrics


def _run_variant(variant, pass_number, order_number):
    module_name = ORIGINAL_MODULE if variant == "original" else UPDATED_MODULE
    runner = _import_fresh(module_name)
    purge_run(spark, catalog, schema, run_id)

    kwargs = dict(
        EntityID=entity_id,
        ClientID=client_id,
        TaxPeriodID=tax_period_id,
        RunID=run_id,
        CatalogName=catalog,
        SchemaName=schema,
        ResultType=result_type,
        VolumePath=volume_path,
        ExecutionID=f"benchmark-{pass_number}-{variant}",
    )
    if variant == "updated":
        kwargs.update(
            MaxThreads=max_threads,
            ProfilePlan=profile_plan,
            PlanCheckpointThreshold=plan_threshold,
            CheckpointBackend=checkpoint_backend,
        )

    print(
        f"\n{'=' * 72}\n"
        f"PASS {pass_number} | {variant.upper()} | order={order_number} | "
        f"{datetime.now().isoformat()}\n"
        f"{'=' * 72}"
    )
    started = time.time()
    try:
        result = runner.run_load_allocation_input(spark, **kwargs)
        status = "SUCCESS"
        error = None
    except Exception as exc:
        result = None
        status = "FAIL"
        error = f"{type(exc).__name__}: {exc}"
        print(f"[benchmark] {variant} failed: {error}")
    wall = round(time.time() - started, 3)
    metrics = (
        capture_metrics(spark, catalog, schema, run_id)
        if status == "SUCCESS"
        else {}
    )
    rows = sum(
        int(value.get("rows") or 0) for value in metrics.values()
    )
    print(
        f"[benchmark] {variant}: wall={wall}s "
        f"reported={_reported_seconds(result)} rows={rows}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "order": order_number,
        "status": status,
        "wall_seconds": wall,
        "reported_seconds": _reported_seconds(result),
        "rows": rows,
        "error": error,
        "metrics": metrics,
        "result": result,
    }

# COMMAND ----------

benchmark_runs = []
parity_rows = []

for pass_number in range(1, number_of_runs + 1):
    variants = _order(pass_number)
    print(
        f"[benchmark] pass {pass_number} execution order: "
        + " -> ".join(variants)
    )
    pass_runs = {}
    for order_number, variant in enumerate(variants, start=1):
        run = _run_variant(variant, pass_number, order_number)
        benchmark_runs.append(run)
        pass_runs[variant] = run

    if set(pass_runs) == {"original", "updated"}:
        comparisons = compare_metrics(
            pass_runs["original"]["metrics"],
            pass_runs["updated"]["metrics"],
        )
        for row in comparisons:
            parity_rows.append({"pass": pass_number, **row})
        mismatches = [row for row in comparisons if not row["matched"]]
        if mismatches:
            first = mismatches[0]
            print(
                f"[reconcile] FAIL PASS {pass_number}: "
                f"first mismatch={first['table']} ({first['reason']})"
            )
        else:
            print(
                f"[reconcile] PASS {pass_number}: "
                "all output fingerprints match"
            )

# COMMAND ----------

import pandas as pd

timing_df = pd.DataFrame(
    [
        {key: value for key, value in run.items() if key not in {"metrics", "result"}}
        for run in benchmark_runs
    ]
)
display(timing_df)

if parity_rows:
    parity_df = pd.DataFrame(parity_rows)
    display(parity_df)

comparison_df = timing_df.pivot_table(
    index="pass",
    columns="variant",
    values="wall_seconds",
    aggfunc="first",
)
if {"original", "updated"}.issubset(comparison_df.columns):
    comparison_df["delta_seconds"] = (
        comparison_df["updated"] - comparison_df["original"]
    )
    comparison_df["improvement_pct"] = (
        (comparison_df["original"] - comparison_df["updated"])
        / comparison_df["original"]
        * 100
    ).round(1)
display(comparison_df.reset_index())

updated_results = [
    run["result"]
    for run in benchmark_runs
    if run["variant"] == "updated" and isinstance(run["result"], dict)
]
if updated_results:
    latest = updated_results[-1]
    if latest.get("plan_profile"):
        display(pd.DataFrame(latest["plan_profile"]))
    if latest.get("checkpoint_plan_profile"):
        display(pd.DataFrame(latest["checkpoint_plan_profile"]))
    if latest.get("action_plan_profile"):
        display(pd.DataFrame(latest["action_plan_profile"]))

if parity_rows and any(not row["matched"] for row in parity_rows):
    raise AssertionError("A/B output parity failed; inspect the first mismatch above")
