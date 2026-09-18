# Databricks notebook source
# MAGIC %md
# MAGIC # SM look-through effective allocation: production vs outputV2
# MAGIC
# MAGIC Restores the RunID-scoped allocation input before each variant, purges
# MAGIC only that RunID's output, and compares both resulting tables.

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
dbutils.widgets.text("EntityID", "123", "4. EntityID")
dbutils.widgets.text("ClientID", "456", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "789", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "1001", "7. RunID")
dbutils.widgets.text("CatalogName", "dev7", "8. Catalog")
dbutils.widgets.text("SchemaName", "Qa7testschema", "9. Schema")
dbutils.widgets.dropdown(
    "ResultType",
    "deltalake",
    ["deltalake"],
    "10. Result type",
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
execution_order_setting = dbutils.widgets.get("ExecutionOrder").strip()
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
    None if checkpoint_mode_raw in {"", "default"} else int(checkpoint_mode_raw)
)
shuffle_partitions = dbutils.widgets.get(
    "SqlShufflePartitions"
).strip()

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if not 1 <= max_threads <= 4:
    raise ValueError("MaxThreads must be between 1 and 4")
if result_type.lower() != "deltalake":
    raise ValueError("Mutation reconciliation requires deltalake output")
if checkpoint_mode is not None and checkpoint_mode not in (1, 2, 3, 4):
    raise ValueError("CheckpointMode must be 1, 2, 3, or 4")
if shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

# COMMAND ----------

import importlib
import sys
import time

sys.path[:] = [item for item in sys.path if item != source_path]
sys.path.insert(0, source_path)

PACKAGE = (
    "AllocationV2."
    "usp_sm_load_lookthrough_effective_allocation_pct"
)
PRODUCTION_ROOT = f"{PACKAGE}.output"
OUTPUT_V2_ROOT = f"{PACKAGE}.outputV2"
PRODUCTION_MODULE = f"{PRODUCTION_ROOT}.orchestrator"
OUTPUT_V2_MODULE = f"{OUTPUT_V2_ROOT}.orchestrator"


def _evict_modules():
    roots = (
        PRODUCTION_ROOT,
        OUTPUT_V2_ROOT,
        "AllocationV2.plan_profiler",
        "Common_V2",
        "services",
    )
    for name in list(sys.modules):
        if any(name == root or name.startswith(root + ".") for root in roots):
            del sys.modules[name]
    importlib.invalidate_caches()


def _import_fresh(module_name):
    _evict_modules()
    checkpoint_module = importlib.import_module(
        "Common_V2.core.checkpoint_V2"
    )
    print(f"[import] checkpoint_V2={checkpoint_module.__file__}")
    module = importlib.import_module(module_name)
    print(f"[import] runner={module.__file__}")
    return module


_evict_modules()
reconcile = importlib.import_module(
    f"{OUTPUT_V2_ROOT}.output_reconcile"
)
capture_outputs = reconcile.capture_outputs
compare_outputs = reconcile.compare_outputs
create_input_snapshot = reconcile.create_input_snapshot
drop_input_snapshot = reconcile.drop_input_snapshot
create_run_snapshot = reconcile.create_run_snapshot
drop_run_snapshot = reconcile.drop_run_snapshot
purge_run = reconcile.purge_run
restore_input_snapshot = reconcile.restore_input_snapshot
restore_run_snapshot = reconcile.restore_run_snapshot
summarize_outputs = reconcile.summarize_outputs

# COMMAND ----------


def _reported_seconds(result):
    if isinstance(result, dict) and result.get("elapsed_seconds") is not None:
        return float(result["elapsed_seconds"])
    return None


def _run_variant(variant, pass_number, input_snapshot):
    runner = _import_fresh(
        PRODUCTION_MODULE if variant == "original" else OUTPUT_V2_MODULE
    )
    restore_input_snapshot(
        spark, catalog, schema, run_id, input_snapshot
    )
    purge_run(spark, catalog, schema, run_id)
    kwargs = {
        "EntityID": entity_id,
        "ClientID": client_id,
        "TaxPeriodID": tax_period_id,
        "RunID": run_id,
        "CatalogName": catalog,
        "SchemaName": schema,
        "ResultType": result_type,
        "VolumePath": volume_path or None,
        "ExecutionID": f"sm-effective-ab-{pass_number}",
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
    started = time.perf_counter()
    result = runner.run_sm_load_lt_effective_alloc_pct(
        spark, **kwargs
    )
    wall = round(time.perf_counter() - started, 3)
    outputs = capture_outputs(spark, catalog, schema, run_id)
    profile = (
        runner.get_last_run_profile()
        if variant == "updated"
        and hasattr(runner, "get_last_run_profile")
        else {}
    )
    summary = summarize_outputs(outputs)
    reported = _reported_seconds(result)
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
        "status": "PASS",
        "outputs": outputs,
        "profile": profile,
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
input_snapshot = create_input_snapshot(
    spark, catalog, schema, run_id
)
output_snapshot = None
try:
    output_snapshot = create_run_snapshot(
        spark,
        catalog,
        schema,
        "SM_LookThroughAllocationOutput",
        run_id,
    )
    for pass_number in range(1, number_of_runs + 1):
        order = _order_for_pass(pass_number)
        print(
            f"[benchmark] pass {pass_number} execution order: "
            f"{' -> '.join(order)}"
        )
        by_variant = {
            variant: _run_variant(
                variant, pass_number, input_snapshot
            )
            for variant in order
        }
        records.extend(by_variant.values())
        mismatches = compare_outputs(
            by_variant["original"]["outputs"],
            by_variant["updated"]["outputs"],
        )
        for table in reconcile.MUTATED_TABLES:
            parity_rows.append(
                {
                    "pass": pass_number,
                    "table": table,
                    "matches": not any(
                        item["table"] == table
                        for item in mismatches
                    ),
                }
            )
        if mismatches:
            first = mismatches[0]
            raise AssertionError(
                f"First mismatch: pass={pass_number} "
                f"table={first['table']} "
                f"original={first['original']} "
                f"updated={first['updated']}"
            )
        print(
            f"[reconcile] PASS {pass_number}: "
            "all output fingerprints match"
        )
finally:
    try:
        restore_input_snapshot(
            spark, catalog, schema, run_id, input_snapshot
        )
    finally:
        try:
            if output_snapshot:
                restore_run_snapshot(
                    spark,
                    catalog,
                    schema,
                    "SM_LookThroughAllocationOutput",
                    run_id,
                    output_snapshot,
                )
        finally:
            try:
                drop_input_snapshot(
                    spark, catalog, schema, input_snapshot
                )
            finally:
                if output_snapshot:
                    drop_run_snapshot(
                        spark, catalog, schema, output_snapshot
                    )

# COMMAND ----------

summary_rows = [
    {
        key: row[key]
        for key in (
            "pass",
            "variant",
            "wall_seconds",
            "reported_seconds",
            "rows",
            "status",
        )
    }
    for row in records
]
display(spark.createDataFrame(summary_rows).orderBy("pass", "variant"))
display(spark.createDataFrame(parity_rows).orderBy("pass", "table"))

delta_rows = []
for pass_number in range(1, number_of_runs + 1):
    original = next(
        row for row in records
        if row["pass"] == pass_number
        and row["variant"] == "original"
    )
    updated = next(
        row for row in records
        if row["pass"] == pass_number
        and row["variant"] == "updated"
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

# COMMAND ----------


def _profile_rows(key):
    return [
        {"pass": row["pass"], **item}
        for row in records
        if row["variant"] == "updated"
        for item in row["profile"].get(key, [])
    ]


checkpoint_activity = _profile_rows("checkpoint_activity")
parallel_activity = _profile_rows("parallel_activity")
timing_activity = _profile_rows("timings")
builder_profile = _profile_rows("plan_profile")
checkpoint_profile = _profile_rows("checkpoint_plan_profile")
action_profile = _profile_rows("action_profile")

for title, values, ordering in (
    ("CHECKPOINT ACTIVITY", checkpoint_activity, ["pass", "sequence"]),
    ("PARALLEL ACTIVITY", parallel_activity, ["pass", "group", "task"]),
    ("TIMING ACTIVITY", timing_activity, ["pass", "elapsed_seconds"]),
    ("BUILDER PROFILE", builder_profile, ["pass", "delta"]),
    ("CHECKPOINT PROFILE", checkpoint_profile, ["pass", "delta"]),
    ("ACTION PROFILE", action_profile, ["pass", "delta"]),
):
    print(f"===== {title} =====")
    if values:
        display(spark.createDataFrame(values).orderBy(*ordering))
    else:
        print("No records captured")

# COMMAND ----------

consumer_counts = {
    "mapping_distinct_tagged_union": 8,
    "sm_lt_filtered": 2,
    "sm_lt_input": 2,
    "k1_amount": 2,
    "effective_amounts": 3,
}
checkpoint_times = {
    (item["pass"], item["name"]): item.get("elapsed_seconds")
    for item in checkpoint_activity
}
recommendation_rows = []
for item in checkpoint_profile:
    base_name = str(item.get("func", "")).split("#", 1)[0]
    recommendation_rows.append(
        {
            "pass": item["pass"],
            "name": base_name,
            "checkpoint_nodes": item.get("nodes"),
            "depth": item.get("depth"),
            "operator_mix": str(item.get("ops") or {}),
            "consumer_count": consumer_counts.get(base_name),
            "fan_out": (consumer_counts.get(base_name) or 0) > 1,
            "measured_checkpoint_seconds": checkpoint_times.get(
                (item["pass"], base_name)
            ),
            "recommendation": item.get("recommendation"),
            "rationale": (
                "Production seam retained; decide only after parity and "
                "measured materialization-versus-recomputation cost."
            ),
        }
    )
print("===== CHECKPOINT RECOMMENDATIONS =====")
if recommendation_rows:
    display(
        spark.createDataFrame(recommendation_rows).orderBy(
            "pass", "checkpoint_nodes", ascending=[True, False]
        )
    )
else:
    print("Enable ProfilePlan to populate recommendations")
