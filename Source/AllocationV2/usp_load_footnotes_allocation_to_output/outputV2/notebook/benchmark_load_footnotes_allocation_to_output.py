# Databricks notebook source
# MAGIC %md
# MAGIC # Footnote Allocation: production vs outputV2
# MAGIC
# MAGIC Snapshots and restores both AllocationInput and generated footnote
# MAGIC AllocationOutput rows. Do not run another process for this RunID.

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
dbutils.widgets.text("EntityID", "115", "4. EntityID")
dbutils.widgets.text("ClientID", "15348", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "16560", "7. RunID")
dbutils.widgets.text("CatalogName", "QA7", "8. Catalog")
dbutils.widgets.text(
    "SchemaName", "IPC_2025_QA7_15348", "9. Schema"
)
dbutils.widgets.dropdown(
    "RankForRulePickup", "1", ["1", "2"], "10. Rule rank"
)
dbutils.widgets.text("MaxThreads", "4", "11. Max threads")
dbutils.widgets.dropdown(
    "ProfilePlan", "off", ["off", "on"], "12. Plan profile"
)
dbutils.widgets.text(
    "PlanCheckpointThreshold", "30", "13. Plan threshold"
)
dbutils.widgets.dropdown(
    "CheckpointMode", "2", ["1", "2", "3", "4"], "14. Checkpoint mode"
)
dbutils.widgets.text(
    "SqlShufflePartitions", "4", "15. Shuffle partitions"
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(
    dbutils.widgets.get("number_of_runs").strip() or "1"
)
execution_order = dbutils.widgets.get("ExecutionOrder").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
rank = int(dbutils.widgets.get("RankForRulePickup"))
max_threads = int(dbutils.widgets.get("MaxThreads").strip() or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").lower() == "on"
plan_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold").strip() or "30"
)
checkpoint_mode = int(dbutils.widgets.get("CheckpointMode"))
shuffle_partitions = dbutils.widgets.get(
    "SqlShufflePartitions"
).strip()

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if not 1 <= max_threads <= 4:
    raise ValueError("MaxThreads must be between 1 and 4")
if shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

# COMMAND ----------

import importlib
import sys
import time
import uuid

sys.path[:] = [item for item in sys.path if item != source_path]
sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_load_footnotes_allocation_to_output"
PRODUCTION_ROOT = f"{PACKAGE}.output"
OUTPUT_V2_ROOT = f"{PACKAGE}.outputV2"
PRODUCTION_MODULE = f"{PRODUCTION_ROOT}.orchestrator"
OUTPUT_V2_MODULE = f"{OUTPUT_V2_ROOT}.orchestrator"


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
create_snapshot = reconcile.create_benchmark_snapshot
reset_variant = reconcile.reset_before_variant
capture_metrics = reconcile.capture_metrics
compare_metrics = reconcile.compare_metrics
summarize_metrics = reconcile.summarize_metrics
restore_state = reconcile.restore_original_state
drop_snapshot = reconcile.drop_benchmark_snapshot

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


def _run_variant(variant, pass_number, snapshot):
    reset_variant(spark, snapshot)
    module_name = (
        PRODUCTION_MODULE
        if variant == "original"
        else OUTPUT_V2_MODULE
    )
    runner = _import_fresh(module_name)
    kwargs = {
        "EntityID": entity_id,
        "ClientID": client_id,
        "TaxPeriodID": tax_period_id,
        "RunID": run_id,
        "CatalogName": catalog,
        "SchemaName": schema,
        "RankForRulePickup": rank,
    }
    if variant == "updated":
        kwargs.update(
            {
                "MaxThreads": max_threads,
                "ProfilePlan": profile_plan,
                "PlanCheckpointThreshold": plan_threshold,
                "CheckpointMode": checkpoint_mode,
            }
        )
    started = time.time()
    result = runner.run_load_footnotes_allocation_to_output(
        spark, **kwargs
    )
    wall = round(time.time() - started, 3)
    metrics = capture_metrics(spark, catalog, schema, run_id)
    summary = summarize_metrics(metrics)
    reported = (
        result.get("elapsed_seconds")
        if isinstance(result, dict)
        else None
    )
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s "
        f"reported={reported} "
        f"output_rows={summary['allocation_output_rows']} "
        f"input_rows={summary['allocation_input_rows']}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "metrics": metrics,
        "summary": summary,
        "profiles": (
            result.get("plan_profiles", {})
            if isinstance(result, dict)
            else {}
        ),
        "checkpoint_activity": (
            result.get("checkpoint_activity", [])
            if isinstance(result, dict)
            else []
        ),
        "status": "PASS",
    }


records = []
parity_rows = []
snapshot = create_snapshot(
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
        mismatches = compare_metrics(
            by_variant["original"]["metrics"],
            by_variant["updated"]["metrics"],
        )
        for table in ("AllocationInput", "AllocationOutput"):
            parity_rows.append(
                {
                    "pass": pass_number,
                    "table": table,
                    "matches": not any(
                        item.startswith(table + ".")
                        for item in mismatches
                    ),
                }
            )
        if mismatches:
            raise AssertionError(
                f"First mismatch pass={pass_number}: {mismatches[0]}"
            )
        print(
            f"[reconcile] PASS {pass_number}: "
            "both affected tables match"
        )
finally:
    try:
        restore_state(spark, snapshot)
    except Exception:
        print(
            "[reconcile] RESTORE FAILED; backups retained: "
            f"{snapshot['input_backup']}, {snapshot['output_backup']}"
        )
        raise
    else:
        drop_snapshot(spark, snapshot)

# COMMAND ----------

summary_rows = [
    {
        "pass": row["pass"],
        "variant": row["variant"],
        "wall_seconds": row["wall_seconds"],
        "reported_seconds": row["reported_seconds"],
        "allocation_input_rows": row["summary"][
            "allocation_input_rows"
        ],
        "allocation_output_rows": row["summary"][
            "allocation_output_rows"
        ],
        "status": row["status"],
    }
    for row in records
]
display(spark.createDataFrame(summary_rows).orderBy("pass", "variant"))
display(spark.createDataFrame(parity_rows).orderBy("pass", "table"))

delta_rows = []
for pass_number in range(1, number_of_runs + 1):
    original = next(
        row
        for row in records
        if row["pass"] == pass_number and row["variant"] == "original"
    )
    updated = next(
        row
        for row in records
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
                round(
                    100.0 * delta / original["wall_seconds"], 2
                )
                if original["wall_seconds"]
                else None
            ),
        }
    )
display(spark.createDataFrame(delta_rows).orderBy("pass"))

checkpoint_rows = [
    {
        "pass": row["pass"],
        "name": item.get("name"),
        "sequence": item.get("sequence"),
        "mode": item.get("mode"),
        "backend": item.get("backend"),
        "elapsed_seconds": item.get("elapsed_seconds"),
    }
    for row in records
    if row["variant"] == "updated"
    for item in row["checkpoint_activity"]
]
if checkpoint_rows:
    display(
        spark.createDataFrame(checkpoint_rows).orderBy(
            "pass", "sequence"
        )
    )

if profile_plan:
    for row in records:
        if row["variant"] != "updated":
            continue
        for label, key in (
            ("BUILDER", "builder"),
            ("CHECKPOINT", "checkpoint"),
            ("ACTION", "action"),
        ):
            values = row["profiles"].get(key, [])
            print(
                f"[profile] pass={row['pass']} "
                f"{label} records={len(values)}"
            )
            if values:
                display(spark.createDataFrame(values))
