# Databricks notebook source
# ruff: noqa: E402, F821
# MAGIC %md
# MAGIC # Add Allocation Summary: full-table production vs outputV2
# MAGIC
# MAGIC Snapshots, purges, fingerprints, and restores all 17 summary tables.
# MAGIC Do not run another process for this RunID during the benchmark.

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
dbutils.widgets.text("TaxPeriodID", "", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "", "7. RunID")
dbutils.widgets.text("CatalogName", "", "8. Catalog")
dbutils.widgets.text("SchemaName", "", "9. Schema")
dbutils.widgets.dropdown(
    "ResultType", "deltalake", ["deltalake"], "10. Result type"
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
number_of_runs = int(
    dbutils.widgets.get("number_of_runs").strip() or "2"
)
execution_order = dbutils.widgets.get("ExecutionOrder").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
result_type = dbutils.widgets.get("ResultType").strip()
volume_path = dbutils.widgets.get("VolumePath").strip()
max_threads = int(dbutils.widgets.get("MaxThreads").strip() or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").lower() == "on"
plan_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold").strip() or "30"
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
if checkpoint_mode is not None and checkpoint_mode not in {1, 2, 3, 4}:
    raise ValueError("CheckpointMode must be 1..4")
if checkpoint_mode == 3 and not volume_path:
    raise ValueError("VolumePath is required for CheckpointMode=3")
if shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

# COMMAND ----------

import importlib
import sys
import time
import uuid

sys.path[:] = [item for item in sys.path if item != source_path]
sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_add_allocation_summary"
PRODUCTION_ROOT = f"{PACKAGE}.output"
OUTPUT_V2_ROOT = f"{PACKAGE}.outputV2"
PRODUCTION_MODULE = f"{PRODUCTION_ROOT}.add_allocation_summary"
OUTPUT_V2_MODULE = f"{OUTPUT_V2_ROOT}.add_allocation_summary"


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
    checkpoint_module = importlib.import_module(
        "Common_V2.core.checkpoint_V2"
    )
    print(f"[import] checkpoint_V2={checkpoint_module.__file__}")
    module = importlib.import_module(module_name)
    print(f"[import] runner={module.__file__}")
    return module


_clear_modules()
reconcile = importlib.import_module(
    f"{OUTPUT_V2_ROOT}.output_reconcile"
)
OUTPUT_TABLES = reconcile.OUTPUT_TABLES

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
        PRODUCTION_MODULE
        if variant == "original"
        else OUTPUT_V2_MODULE
    )
    runner_module = _import_fresh(module_name)
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
            f"benchmark_{pass_number}_{variant}_{uuid.uuid4().hex[:8]}"
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
    result = runner_module.run_add_allocation_summary(spark, **kwargs)
    wall = round(time.time() - started, 3)
    metrics = reconcile.capture_metrics(
        spark, catalog, schema, run_id
    )
    total_rows = sum(
        int(values.get("row_count") or 0)
        for values in metrics.values()
    )
    reported = (
        result.get("elapsed_seconds")
        if isinstance(result, dict)
        else None
    )
    candidate_profile = (
        runner_module.get_last_run_profile()
        if variant == "updated"
        and hasattr(runner_module, "get_last_run_profile")
        else {}
    )
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s "
        f"reported={reported} rows={total_rows}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "total_rows": total_rows,
        "metrics": metrics,
        "profiles": {
            "builder": candidate_profile.get("plan_profile", []),
            "checkpoint": candidate_profile.get(
                "checkpoint_plan_profile", []
            ),
            "action": candidate_profile.get("action_profile", []),
        },
        "checkpoint_activity": candidate_profile.get(
            "checkpoint_activity", []
        ),
        "status": "PASS",
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
        parity_rows.extend(
            reconcile.metric_rows(
                pass_number,
                by_variant["original"]["metrics"],
                by_variant["updated"]["metrics"],
            )
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
        print(
            "[reconcile] RESTORE FAILED; snapshot tables retained"
        )
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
        "rows_across_17_tables": row["total_rows"],
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
    {"pass": row["pass"], **item}
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
                ranked = sorted(
                    values,
                    key=lambda item: (
                        item.get("delta", 0),
                        item.get("nodes", 0),
                    ),
                    reverse=True,
                )
                display(spark.createDataFrame(ranked))

    recommendation_rows = []
    for row in records:
        if row["variant"] != "updated":
            continue
        checkpoint_profile = next(
            (
                item
                for item in row["profiles"].get("checkpoint", [])
                if item.get("func") == "pfic_alloc_text"
            ),
            {},
        )
        checkpoint_activity = next(
            (
                item
                for item in row["checkpoint_activity"]
                if item.get("name") == "pfic_alloc_text"
            ),
            {},
        )
        recommendation_rows.append(
            {
                "pass": row["pass"],
                "name": "pfic_alloc_text",
                "builder_delta": None,
                "checkpoint_nodes": checkpoint_profile.get("nodes"),
                "depth": checkpoint_profile.get("depth"),
                "operator_mix": str(
                    checkpoint_profile.get("ops", {})
                ),
                "consumer_count": 1,
                "fan_out": 1,
                "measured_action_seconds": None,
                "measured_checkpoint_seconds": checkpoint_activity.get(
                    "elapsed_seconds"
                ),
                "recommendation": "keep/measure",
                "rationale": (
                    "Preserves PFIC read-own-writes boundary; retain until "
                    "A/B timing proves materialization cost exceeds savings."
                ),
            }
        )
    display(
        spark.createDataFrame(recommendation_rows).orderBy("pass")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acceptance
# MAGIC
# MAGIC Accept outputV2 only when all 17 tables pass in both orders, updated
# MAGIC wall time improves beyond run variance, and profiler evidence supports
# MAGIC retaining the `pfic_alloc_text` checkpoint.
