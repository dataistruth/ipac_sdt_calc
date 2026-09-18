# Databricks notebook source
# MAGIC %md
# MAGIC # K3AllocationSummary: production vs outputV2
# MAGIC
# MAGIC The selected RunID is snapshotted and restored. Do not run another
# MAGIC process for the same RunID while this benchmark is active.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("number_of_runs", "2", "2. Number of runs")
dbutils.widgets.dropdown(
    "ExecutionOrder",
    "alternate",
    ["alternate", "original_first", "updated_first"],
    "3. Execution order",
)
dbutils.widgets.text("EntityID", "", "4. EntityID")
dbutils.widgets.text("ClientID", "", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "", "7. RunID")
dbutils.widgets.text("CatalogName", "", "8. Catalog")
dbutils.widgets.text("SchemaName", "", "9. Schema")
dbutils.widgets.dropdown(
    "ResultType", "deltalake", ["deltalake", "Parquet"], "10. Result type"
)
dbutils.widgets.text("VolumePath", "", "11. Volume path")
dbutils.widgets.text("MaxThreads", "4", "12. Max threads")
dbutils.widgets.dropdown(
    "ProfilePlan", "off", ["off", "on"], "13. Plan profile"
)
dbutils.widgets.text(
    "PlanCheckpointThreshold", "30", "14. Plan threshold"
)
dbutils.widgets.dropdown(
    "CheckpointMode",
    "default",
    ["default", "1", "2", "3", "4"],
    "15. Checkpoint mode",
)
dbutils.widgets.text(
    "SqlShufflePartitions", "", "16. Shuffle partitions"
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs") or "2")
execution_order = dbutils.widgets.get("ExecutionOrder").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
result_type = dbutils.widgets.get("ResultType").strip()
volume_path = dbutils.widgets.get("VolumePath").strip()
max_threads = int(dbutils.widgets.get("MaxThreads") or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").lower() == "on"
plan_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold") or "30"
)
checkpoint_mode_raw = dbutils.widgets.get("CheckpointMode").strip().lower()
checkpoint_mode = (
    int(checkpoint_mode_raw)
    if checkpoint_mode_raw in {"1", "2", "3", "4"}
    else None
)
shuffle_partitions = dbutils.widgets.get(
    "SqlShufflePartitions"
).strip()

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if not 1 <= max_threads <= 4:
    raise ValueError("MaxThreads must be between 1 and 4")
if checkpoint_mode == 3 and not volume_path:
    raise ValueError("CheckpointMode 3 requires VolumePath")
if shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

# COMMAND ----------

import importlib
import json
import sys
import time
import uuid

sys.path[:] = [item for item in sys.path if item != source_path]
sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_load_k3_allocation_summary"
PRODUCTION_ROOT = f"{PACKAGE}.output"
OUTPUT_V2_ROOT = f"{PACKAGE}.outputV2"
PRODUCTION_MODULE = (
    f"{PRODUCTION_ROOT}.usp_load_k3_allocation_summary"
)
OUTPUT_V2_MODULE = (
    f"{OUTPUT_V2_ROOT}.usp_load_k3_allocation_summary"
)


def _clear_modules():
    roots = (
        PRODUCTION_ROOT,
        OUTPUT_V2_ROOT,
        "AllocationV2.plan_profiler",
        "Common_V2",
    )
    for name in list(sys.modules):
        if any(
            name == root or name.startswith(root + ".")
            for root in roots
        ):
            del sys.modules[name]
    importlib.invalidate_caches()


def _import_fresh(module_name):
    _clear_modules()
    checkpoint = importlib.import_module(
        "Common_V2.core.checkpoint_V2"
    )
    print(f"[import] checkpoint_V2={checkpoint.__file__}")
    module = importlib.import_module(module_name)
    print(f"[import] runner={module.__file__}")
    return module


_clear_modules()
reconcile = importlib.import_module(
    f"{OUTPUT_V2_ROOT}.output_reconcile"
)

# COMMAND ----------


def _order(pass_number):
    if execution_order == "original_first":
        return ("original", "updated")
    if execution_order == "updated_first":
        return ("updated", "original")
    return (
        ("original", "updated")
        if pass_number % 2
        else ("updated", "original")
    )


def _find_parquet_path(result):
    try:
        value = json.loads(result) if isinstance(result, str) else result
    except Exception:
        return None
    paths = []

    def visit(item):
        if isinstance(item, dict):
            for key, child in item.items():
                if "path" in str(key).lower() and isinstance(child, str):
                    paths.append(child)
                else:
                    visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return next(
        (path for path in paths if "k3allocationsummary" in path.lower()),
        paths[0] if paths else None,
    )


def _capture(result, snapshot):
    if result_type.lower() == "parquet":
        path = _find_parquet_path(result)
        if not path:
            raise RuntimeError(
                "Could not resolve K3AllocationSummary Parquet path "
                "from FilePathInfo"
            )
        return reconcile.fingerprint_df(spark.read.parquet(path))
    return reconcile.capture_table(spark, snapshot)


def _run_variant(variant, pass_number, snapshot):
    reconcile.purge_before_variant(spark, snapshot)
    module_name = (
        PRODUCTION_MODULE if variant == "original" else OUTPUT_V2_MODULE
    )
    module = _import_fresh(module_name)
    kwargs = {
        "EntityID": entity_id,
        "ClientID": client_id,
        "TaxPeriodID": tax_period_id,
        "RunID": run_id,
        "CatalogName": catalog,
        "SchemaName": schema,
        "ResultType": result_type,
        "VolumePath": volume_path or None,
        "ExecutionID": (
            f"k3_ab_{pass_number}_{variant}_{uuid.uuid4().hex[:8]}"
        ),
    }
    if variant == "updated":
        kwargs.update(
            {
                "MaxThreads": max_threads,
                "ProfilePlan": profile_plan,
                "PlanCheckpointThreshold": plan_threshold,
            }
        )
        if checkpoint_mode is not None:
            kwargs["CheckpointMode"] = checkpoint_mode
    started = time.time()
    result = module.run_usp_load_k3_allocation_summary(spark, **kwargs)
    wall = round(time.time() - started, 3)
    metrics = _capture(result, snapshot)
    diagnostics = (
        module.get_last_run_profile() if variant == "updated" else {}
    )
    reported = diagnostics.get("elapsed_seconds")
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s "
        f"reported={reported} rows={metrics.get('rows')}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "metrics": metrics,
        "diagnostics": diagnostics,
        "status": "PASS",
    }


records = []
parity_rows = []
snapshot = reconcile.create_snapshot(
    spark,
    catalog,
    schema,
    run_id,
    f"{entity_id}_{uuid.uuid4().hex[:12]}",
)
try:
    for pass_number in range(1, number_of_runs + 1):
        order = _order(pass_number)
        print(
            f"[benchmark] pass {pass_number} execution order: "
            f"{' -> '.join(order)}"
        )
        by_variant = {
            variant: _run_variant(variant, pass_number, snapshot)
            for variant in order
        }
        records.extend(by_variant.values())
        mismatches = reconcile.compare_metrics(
            by_variant["original"]["metrics"],
            by_variant["updated"]["metrics"],
        )
        parity_rows.append(
            {
                "pass": pass_number,
                "table": "K3AllocationSummary",
                "matches": not mismatches,
            }
        )
        if mismatches:
            raise AssertionError(
                f"First mismatch pass={pass_number}: {mismatches[0]}"
            )
        print(
            f"[reconcile] PASS {pass_number}: "
            "all output fingerprints match"
        )
finally:
    try:
        reconcile.restore_snapshot(spark, snapshot)
    except Exception:
        print(
            "[reconcile] RESTORE FAILED; backup retained: "
            f"{snapshot['backup']}"
        )
        raise
    else:
        reconcile.drop_snapshot(spark, snapshot)

# COMMAND ----------

summary_rows = [
    {
        "pass": row["pass"],
        "variant": row["variant"],
        "wall_seconds": row["wall_seconds"],
        "reported_seconds": row["reported_seconds"],
        "rows": row["metrics"].get("rows"),
        "status": row["status"],
    }
    for row in records
]
display(spark.createDataFrame(summary_rows).orderBy("pass", "variant"))
display(spark.createDataFrame(parity_rows).orderBy("pass"))

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
                if original["wall_seconds"] else None
            ),
        }
    )
display(spark.createDataFrame(delta_rows).orderBy("pass"))

checkpoint_rows = [
    {"pass": row["pass"], **item}
    for row in records if row["variant"] == "updated"
    for item in row["diagnostics"].get("checkpoint_activity", [])
]
pool_rows = [
    {"pass": row["pass"], **item}
    for row in records if row["variant"] == "updated"
    for item in row["diagnostics"].get("pools", [])
]
if checkpoint_rows:
    display(spark.createDataFrame(checkpoint_rows).orderBy("pass", "sequence"))
if pool_rows:
    display(spark.createDataFrame(pool_rows).orderBy("pass", "pool"))

if profile_plan:
    for row in records:
        if row["variant"] != "updated":
            continue
        for label in ("BUILDER", "CHECKPOINT", "ACTION"):
            values = row["diagnostics"].get(
                "reports", {}
            ).get(label.lower(), [])
            print(
                f"[profile] pass={row['pass']} "
                f"{label} records={len(values)}"
            )
            if values:
                display(spark.createDataFrame(values))
