# Databricks notebook source
# ruff: noqa: E402, F821
# MAGIC %md
# MAGIC # Look-through cost allocation output: production vs outputV2
# MAGIC
# MAGIC This notebook snapshots and restores both mutated RunID partitions.
# MAGIC Do not run another process for the same RunID during the benchmark.

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
dbutils.widgets.text("EntityID", "115", "4. EntityID")
dbutils.widgets.text("ClientID", "15348", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "16560", "7. RunID")
dbutils.widgets.text("CatalogName", "QA7", "8. Catalog")
dbutils.widgets.text("SchemaName", "IPC_2025_QA7_15348", "9. Schema")
dbutils.widgets.dropdown(
    "LineType",
    "K1 with Cost",
    ["K1 with Cost", "K1 with 704c", "K1", "BoxJKL", "704c"],
    "10. Line type",
)
dbutils.widgets.text("RankForRule", "0", "11. Rule rank")
dbutils.widgets.dropdown(
    "ResultType", "deltalake", ["deltalake"], "12. Result type"
)
dbutils.widgets.text("VolumePath", "", "13. Volume path")
dbutils.widgets.text("MaxThreads", "4", "14. Max threads")
dbutils.widgets.dropdown(
    "ProfilePlan", "off", ["off", "on"], "15. Plan profile"
)
dbutils.widgets.text(
    "PlanCheckpointThreshold", "30", "16. Plan threshold"
)
dbutils.widgets.dropdown(
    "CheckpointMode",
    "default",
    ["default", "1", "2", "3", "4"],
    "17. Checkpoint mode",
)
dbutils.widgets.text(
    "SqlShufflePartitions", "", "18. Shuffle partitions"
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs") or "1")
execution_order = dbutils.widgets.get("ExecutionOrder").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
line_type = dbutils.widgets.get("LineType")
rank_for_rule = int(dbutils.widgets.get("RankForRule") or "0")
result_type = dbutils.widgets.get("ResultType")
volume_path = dbutils.widgets.get("VolumePath")
max_threads = int(dbutils.widgets.get("MaxThreads") or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").lower() == "on"
plan_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold") or "30"
)
checkpoint_mode_raw = dbutils.widgets.get("CheckpointMode").strip().lower()
checkpoint_mode = (
    None if checkpoint_mode_raw in {"", "default"}
    else int(checkpoint_mode_raw)
)
shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()

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

PACKAGE = "AllocationV2.usp_load_lookthrough_cost_alloc_to_output"
PRODUCTION_ROOT = f"{PACKAGE}.output"
OUTPUT_V2_ROOT = f"{PACKAGE}.outputV2"
ENTRY = "load_lookthrough_cost_alloc_to_output"
PRODUCTION_MODULE = f"{PRODUCTION_ROOT}.{ENTRY}"
OUTPUT_V2_MODULE = f"{OUTPUT_V2_ROOT}.{ENTRY}"


def _clear_modules():
    roots = (
        PRODUCTION_ROOT,
        OUTPUT_V2_ROOT,
        "AllocationV2.plan_profiler",
        "Common_V2",
    )
    legacy_helpers = {"_data_loading", "_hierarchy", "_allocation"}
    for name in list(sys.modules):
        if name in legacy_helpers or any(
            name == root or name.startswith(root + ".") for root in roots
        ):
            del sys.modules[name]
    importlib.invalidate_caches()


def _register_production_helpers():
    for helper in ("_data_loading", "_hierarchy", "_allocation"):
        sys.modules[helper] = importlib.import_module(
            f"{PRODUCTION_ROOT}.{helper}"
        )


def _import_fresh(module_name):
    _clear_modules()
    checkpoint_module = importlib.import_module(
        "Common_V2.core.checkpoint_V2"
    )
    print(f"[import] checkpoint_V2={checkpoint_module.__file__}")
    _register_production_helpers()
    module = importlib.import_module(module_name)
    print(f"[import] runner={module.__file__}")
    return module


_clear_modules()
reconcile = importlib.import_module(f"{OUTPUT_V2_ROOT}.output_reconcile")

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
    reconcile.reset_before_variant(spark, snapshot)
    module_name = (
        PRODUCTION_MODULE if variant == "original" else OUTPUT_V2_MODULE
    )
    runner = _import_fresh(module_name)
    kwargs = {
        "EntityID": entity_id,
        "ClientID": client_id,
        "TaxPeriodID": tax_period_id,
        "RunID": run_id,
        "CatalogName": catalog,
        "SchemaName": schema,
        "line_type": line_type,
        "rank_for_rule": rank_for_rule,
        "result_type": result_type,
        "VolumePath": volume_path,
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
    result = runner.run_load_lookthrough_cost_alloc(spark, **kwargs)
    wall = round(time.perf_counter() - started, 3)
    metrics = reconcile.capture_metrics(spark, catalog, schema, run_id)
    rows = reconcile.summarize_metrics(metrics)
    reported = result.get("elapsed") if isinstance(result, dict) else None
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s reported={reported} "
        f"output_rows={rows['LookThroughAllocationOutput']} "
        f"input_rows={rows['LookThroughAllocationInput']}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "metrics": metrics,
        "rows": rows,
        "profiles": (
            result.get("plan_profiles", {})
            if isinstance(result, dict)
            else {}
        ),
        "checkpoints": (
            result.get("checkpoint_activity", [])
            if isinstance(result, dict)
            else []
        ),
        "pools": (
            result.get("parallel_activity", [])
            if isinstance(result, dict)
            else []
        ),
        "status": result.get("status") if isinstance(result, dict) else None,
    }


records = []
parity_rows = []
snapshot = reconcile.create_benchmark_snapshot(
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
        for table in reconcile.TABLES:
            parity_rows.append(
                {
                    "pass": pass_number,
                    "table": table,
                    "matches": not any(
                        item.startswith(table + ".") for item in mismatches
                    ),
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
        reconcile.restore_original_state(spark, snapshot)
    except Exception:
        backups = [
            spec["backup"] for spec in snapshot["tables"].values()
        ]
        print(f"[reconcile] RESTORE FAILED; backups retained: {backups}")
        raise
    else:
        reconcile.drop_benchmark_snapshot(spark, snapshot)

# COMMAND ----------

summary_rows = [
    {
        "pass": row["pass"],
        "variant": row["variant"],
        "wall_seconds": row["wall_seconds"],
        "reported_seconds": row["reported_seconds"],
        "input_rows": row["rows"]["LookThroughAllocationInput"],
        "output_rows": row["rows"]["LookThroughAllocationOutput"],
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

checkpoint_rows = [
    {"pass": row["pass"], **item}
    for row in records if row["variant"] == "updated"
    for item in row["checkpoints"]
]
if checkpoint_rows:
    display(spark.createDataFrame(checkpoint_rows).orderBy("pass", "sequence"))

pool_rows = [
    {"pass": row["pass"], **item}
    for row in records if row["variant"] == "updated"
    for item in row["pools"]
]
if pool_rows:
    display(spark.createDataFrame(pool_rows).orderBy("pass", "pool", "task"))

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

# COMMAND ----------

consumer_counts = {
    "cost_underlyings": 3,
    "entity_hier_final": 2,
    "all_underlyings": 3,
    "asset_class_rel": 2,
    "alloc_output": 2,
}
recommendations = []
for row in records:
    if row["variant"] != "updated":
        continue
    profiles = row["profiles"]
    builder_by_name = {
        item.get("func"): item for item in profiles.get("builder", [])
    }
    action_by_name = {
        item.get("func"): item for item in profiles.get("action", [])
    }
    checkpoint_seconds = {
        item.get("name"): item.get("elapsed_seconds")
        for item in row["checkpoints"]
    }
    for item in profiles.get("checkpoint", []):
        recorded_name = str(item.get("func", ""))
        name = recorded_name.split("#", 1)[0]
        consumers = consumer_counts.get(name, 1)
        related_action_seconds = sum(
            float(action.get("elapsed_seconds") or 0)
            for action_name, action in action_by_name.items()
            if name in str(action_name)
            or (
                name == "alloc_output"
                and str(action_name).startswith("write.")
            )
        )
        recommendations.append(
            {
                "pass": row["pass"],
                "name": name,
                "builder_delta": (
                    builder_by_name.get(name, {}).get("delta")
                ),
                "checkpoint_nodes": item.get("nodes"),
                "depth": item.get("depth"),
                "operator_mix": str(item.get("ops") or {}),
                "consumer_count": consumers,
                "fan_out": consumers > 1,
                "measured_action_seconds": round(
                    related_action_seconds, 3
                ),
                "measured_checkpoint_seconds": checkpoint_seconds.get(name),
                "recommendation": "keep",
                "rationale": (
                    "Production seam retained; fan-out/dependency ordering "
                    "requires measured A/B evidence before removal."
                ),
            }
        )
print("===== CHECKPOINT RECOMMENDATIONS =====")
if recommendations:
    display(
        spark.createDataFrame(recommendations).orderBy(
            "pass", "checkpoint_nodes", ascending=[True, False]
        )
    )
else:
    print("Enable ProfilePlan to populate recommendations")
