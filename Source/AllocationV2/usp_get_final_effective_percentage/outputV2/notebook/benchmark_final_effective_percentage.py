# Databricks notebook source
# MAGIC %md
# MAGIC # Final Effective Percentage: production vs outputV2
# MAGIC
# MAGIC Conservative A/B acceptance notebook. It purges the selected RunID
# MAGIC before each variant and requires parity across all three output tables.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("number_of_runs", "1", "2. Number of runs")
dbutils.widgets.dropdown(
    "ExecutionOrder",
    "alternate",
    ["alternate", "original_first", "updated_first"],
    "3. Execution order",
)
dbutils.widgets.dropdown("Mode", "0", ["0", "4"], "4. Mode")
dbutils.widgets.text("EntityID", "4137", "5. EntityID")
dbutils.widgets.text("ClientID", "15348", "6. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "7. TaxPeriodID")
dbutils.widgets.text("RunID", "17376", "8. RunID")
dbutils.widgets.text("CatalogName", "QA7", "9. Catalog")
dbutils.widgets.text("SchemaName", "iPC_2025_QA7_15348", "10. Schema")
dbutils.widgets.dropdown(
    "ResultType", "deltalake", ["deltalake"], "11. Result type"
)
dbutils.widgets.text("VolumePath", "", "12. Volume path")
dbutils.widgets.text("MaxThreads", "4", "13. Max threads")
dbutils.widgets.dropdown(
    "ProfilePlan", "on", ["off", "on"], "14. Plan profile"
)
dbutils.widgets.text(
    "PlanCheckpointThreshold", "30", "15. Plan threshold"
)
dbutils.widgets.dropdown(
    # Screenshot used CheckpointBackend=delta; V2 mode 1 is all Delta.
    "CheckpointMode", "1", ["1", "2", "3", "4"], "16. Checkpoint mode"
)
dbutils.widgets.text(
    "SqlShufflePartitions", "4", "17. Shuffle partitions"
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs").strip() or "1")
execution_order_setting = dbutils.widgets.get("ExecutionOrder").strip()
mode = int(dbutils.widgets.get("Mode"))
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
result_type = dbutils.widgets.get("ResultType").strip()
volume_path = dbutils.widgets.get("VolumePath").strip()
max_threads = int(dbutils.widgets.get("MaxThreads").strip() or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
plan_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold").strip() or "30"
)
checkpoint_mode = int(dbutils.widgets.get("CheckpointMode"))
shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if result_type.lower() != "deltalake":
    raise ValueError("Reconciliation requires ResultType=deltalake")
if shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

# COMMAND ----------

import importlib
import sys
import time

# Workspace files are importable from Source root even when local os.path
# probes cannot see them. Always make the selected root the first import path.
sys.path[:] = [entry for entry in sys.path if entry != source_path]
sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_get_final_effective_percentage"
PRODUCTION_ROOT = f"{PACKAGE}.output"
OUTPUT_V2_ROOT = f"{PACKAGE}.outputV2"
PRODUCTION_MODULE = f"{PRODUCTION_ROOT}.orchestrator"
OUTPUT_V2_MODULE = f"{OUTPUT_V2_ROOT}.orchestrator"


def _evict_fep_modules():
    """Evict production, outputV2, profiler, and shared checkpoint roots."""
    roots = (
        PRODUCTION_ROOT,
        OUTPUT_V2_ROOT,
        "AllocationV2.plan_profiler",
        "Common_V2",
    )
    for name in list(sys.modules):
        if any(name == root or name.startswith(root + ".") for root in roots):
            del sys.modules[name]
    importlib.invalidate_caches()


def _import_fresh(module_name):
    _evict_fep_modules()
    # Prove this fresh Common_V2 import resolves from the selected Source root.
    checkpoint_module = importlib.import_module("Common_V2.core.checkpoint_V2")
    print(f"[import] checkpoint_V2={checkpoint_module.__file__}")
    module = importlib.import_module(module_name)
    print(f"[import] runner={module.__file__}")
    return module


# Keep reconciliation functions in this notebook after later module eviction.
_evict_fep_modules()
reconcile = importlib.import_module(f"{OUTPUT_V2_ROOT}.output_reconcile")
capture_outputs = reconcile.capture_outputs
compare_outputs = reconcile.compare_outputs
purge_run = reconcile.purge_run
summarize_outputs = reconcile.summarize_outputs

# COMMAND ----------


def _reported_seconds(result, runner, variant):
    if isinstance(result, dict):
        value = result.get("elapsed_seconds")
        if value is not None:
            return float(value)
    if variant == "updated" and hasattr(runner, "get_last_run_profile"):
        value = runner.get_last_run_profile().get("updated_wall_seconds")
        return None if value is None else float(value)
    return None


def _run_variant(variant, pass_number):
    module_name = (
        PRODUCTION_MODULE if variant == "original" else OUTPUT_V2_MODULE
    )
    runner = _import_fresh(module_name)
    purge_run(spark, catalog, schema, run_id)

    run_kwargs = {
        "Mode": mode,
        "EntityID": entity_id,
        "ClientID": client_id,
        "TaxPeriodID": tax_period_id,
        "RunID": run_id,
        "CatalogName": catalog,
        "SchemaName": schema,
        "ResultType": result_type,
        "VolumePath": volume_path or None,
        "ExecutionID": f"fep-ab-{pass_number}",
    }
    if variant == "updated":
        # These are the only outputV2-only parameters.
        run_kwargs.update(
            {
                "MaxThreads": max_threads,
                "ProfilePlan": profile_plan,
                "PlanCheckpointThreshold": plan_threshold,
                "CheckpointMode": checkpoint_mode,
            }
        )

    started = time.time()
    result = runner.run_final_effective_percentages(spark, **run_kwargs)
    wall = round(time.time() - started, 3)
    outputs = capture_outputs(spark, catalog, schema, run_id)
    summary = summarize_outputs(outputs)
    reported = _reported_seconds(result, runner, variant)
    profile = (
        runner.get_last_run_profile()
        if variant == "updated" and hasattr(runner, "get_last_run_profile")
        else {}
    )
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s "
        f"reported={reported} rows={summary['total_rows']}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "rows": summary["total_rows"],
        "outputs": outputs,
        "profile": profile,
        "status": "PASS",
    }


def _order_for_pass(pass_number):
    if execution_order_setting == "original_first":
        return ("original", "updated")
    if execution_order_setting == "updated_first":
        return ("updated", "original")
    return (
        ("original", "updated")
        if pass_number % 2
        else ("updated", "original")
    )


records = []
parity_rows = []
for pass_number in range(1, number_of_runs + 1):
    order = _order_for_pass(pass_number)
    print(
        f"[benchmark] pass {pass_number} execution order: "
        f"{' -> '.join(order)}"
    )
    by_variant = {
        variant: _run_variant(variant, pass_number) for variant in order
    }
    records.extend(by_variant.values())
    mismatches = compare_outputs(
        by_variant["original"]["outputs"],
        by_variant["updated"]["outputs"],
    )
    for table in reconcile.OUTPUT_TABLES:
        parity_rows.append(
            {
                "pass": pass_number,
                "table": table,
                "matches": not any(
                    mismatch["table"] == table for mismatch in mismatches
                ),
            }
        )
    if mismatches:
        first = mismatches[0]
        raise AssertionError(
            f"First mismatch: pass={pass_number} table={first['table']} "
            f"original={first['original']} updated={first['updated']}"
        )
    print(
        f"[reconcile] PASS {pass_number}: "
        "all output fingerprints match"
    )

# COMMAND ----------

summary_rows = [
    {
        "pass": row["pass"],
        "variant": row["variant"],
        "wall_seconds": row["wall_seconds"],
        "reported_seconds": row["reported_seconds"],
        "rows": row["rows"],
        "status": row["status"],
    }
    for row in records
]
display(spark.createDataFrame(summary_rows).orderBy("pass", "variant"))
display(spark.createDataFrame(parity_rows).orderBy("pass", "table"))

delta_rows = []
for pass_number in range(1, number_of_runs + 1):
    original = next(
        row for row in records
        if row["pass"] == pass_number and row["variant"] == "original"
    )
    updated = next(
        row for row in records
        if row["pass"] == pass_number and row["variant"] == "updated"
    )
    delta = original["wall_seconds"] - updated["wall_seconds"]
    delta_rows.append(
        {
            "pass": pass_number,
            "original_wall_seconds": original["wall_seconds"],
            "updated_wall_seconds": updated["wall_seconds"],
            "delta_seconds": round(delta, 3),
            "improvement_percent": (
                round(100.0 * delta / original["wall_seconds"], 2)
                if original["wall_seconds"]
                else None
            ),
        }
    )
display(spark.createDataFrame(delta_rows).orderBy("pass"))

# COMMAND ----------

if profile_plan:
    for row in records:
        if row["variant"] != "updated":
            continue
        profile = row["profile"]
        for label, key in (
            ("BUILDER", "plan_profile"),
            ("CHECKPOINT", "checkpoint_plan_profile"),
            ("ACTION", "action_profile"),
        ):
            values = profile.get(key, [])
            print(f"[profile] pass={row['pass']} {label} records={len(values)}")
            if values:
                display(spark.createDataFrame(values))
