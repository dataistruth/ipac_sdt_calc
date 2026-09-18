# Databricks notebook source
"""A/B benchmark for production and outputV2 allocation orchestrators."""

# COMMAND ----------

import importlib
import json
import sys
import time

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
dbutils.widgets.text(
    "VolumePath",
    "/Volumes/qa7/datavolume/databrickdata",
    "10. Volume path",
)
dbutils.widgets.text("MaxThreads", "4", "11. Updated parallel workers")
dbutils.widgets.dropdown(
    "ProfilePlan",
    "off",
    ["off", "on"],
    "12. Plan profiler",
)
dbutils.widgets.text(
    "PlanCheckpointThreshold",
    "30",
    "13. Plan checkpoint threshold",
)
dbutils.widgets.dropdown(
    "CheckpointMode",
    "default",
    ["default", "1", "2", "3", "4"],
    "14. Checkpoint mode",
)
dbutils.widgets.text(
    "SqlShufflePartitions",
    "16",
    "15. spark.sql.shuffle.partitions",
)
dbutils.widgets.dropdown(
    "ResultType",
    "deltalake",
    ["deltalake", "parquet"],
    "16. Result type",
)

# COMMAND ----------

source_root = dbutils.widgets.get("source_path").rstrip("/")
# Import root is Source/, not Source/AllocationV2. AllocationV2 and Common_V2
# are sibling packages beneath this directory.
while source_root in sys.path:
    sys.path.remove(source_root)
sys.path.insert(0, source_root)
print(f"[benchmark] Python import root={sys.path[0]}", flush=True)
for loaded in list(sys.modules):
    if loaded == "Common_V2" or loaded.startswith("Common_V2."):
        del sys.modules[loaded]
importlib.invalidate_caches()
try:
    import Common_V2.core.checkpoint_V2 as _checkpoint_v2  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "Could not import Common_V2.core.checkpoint_V2 after adding "
        f"the Source folder to sys.path: {source_root}"
    ) from exc

shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()
if shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", int(shuffle_partitions))

common_args = {
    "EntityID": int(dbutils.widgets.get("EntityID")),
    "ClientID": int(dbutils.widgets.get("ClientID")),
    "TaxPeriodID": int(dbutils.widgets.get("TaxPeriodID")),
    "RunID": int(dbutils.widgets.get("RunID")),
    "CatalogName": dbutils.widgets.get("CatalogName"),
    "SchemaName": dbutils.widgets.get("SchemaName"),
    "ResultType": dbutils.widgets.get("ResultType"),
    "VolumePath": dbutils.widgets.get("VolumePath"),
}
updated_args = {
    **common_args,
    "MaxThreads": int(dbutils.widgets.get("MaxThreads") or "4"),
    "ProfilePlan": dbutils.widgets.get("ProfilePlan"),
    "PlanCheckpointThreshold": int(
        dbutils.widgets.get("PlanCheckpointThreshold")
    ),
}
checkpoint_mode_raw = dbutils.widgets.get("CheckpointMode").strip().lower()
if checkpoint_mode_raw not in {"", "default"}:
    updated_args["CheckpointMode"] = int(checkpoint_mode_raw)

# COMMAND ----------

PRODUCTION_MODULE = (
    "AllocationV2.usp_load_allocation_input.output.load_allocation_input"
)
OUTPUT_V2_MODULE = (
    "AllocationV2.usp_load_allocation_input.outputV2.load_allocation_input"
)
RECONCILE_MODULE = (
    "AllocationV2.usp_load_allocation_input.outputV2.output_reconcile"
)
MODULE_ROOTS = (
    "AllocationV2.usp_load_allocation_input.output",
    "AllocationV2.usp_load_allocation_input.outputV2",
    "AllocationV2.plan_profiler",
    "Common_V2",
)
OUTPUT_TABLES = (
    "AllocationInput",
    "PFICFootnoteFlowup",
    "PFICFootnoteFlowupWithTrackingKey",
    "Form926Flowup",
    "Form199AFlowup",
    "Form8865Flowup",
    "Form8886Flowup",
    "AtRiskFlowup",
    "CustomFootnoteFlowup",
    "Form200616Flowup",
)


def _clear_modules():
    for loaded in list(sys.modules):
        if any(
            loaded == root or loaded.startswith(root + ".")
            for root in MODULE_ROOTS
        ):
            del sys.modules[loaded]
    importlib.invalidate_caches()


def _fresh_import(module_name):
    _clear_modules()
    return importlib.import_module(module_name)


def _quoted_fqn(table_name):
    return ".".join(
        f"`{part.replace('`', '``')}`"
        for part in (
            common_args["CatalogName"],
            common_args["SchemaName"],
            table_name,
        )
    )


def _purge_run_outputs(variant):
    """Delete only the benchmark RunID where the table format supports it."""
    run_id = int(common_args["RunID"])
    for table_name in OUTPUT_TABLES:
        fqn = _quoted_fqn(table_name)
        try:
            columns = spark.table(fqn).columns
            if "RunID" not in columns:
                print(f"[purge] {variant}: skip {table_name} (no RunID)")
                continue
            spark.sql(f"DELETE FROM {fqn} WHERE RunID = {run_id}")
            print(f"[purge] {variant}: {table_name} RunID={run_id}")
        except Exception as exc:
            print(
                f"[purge] {variant}: skip {table_name}; "
                f"{type(exc).__name__}: {exc}"
            )


def _reported_seconds(result):
    parsed = result
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except (TypeError, ValueError):
            return None
    if not isinstance(parsed, dict):
        return None
    for key in ("elapsed_seconds", "ElapsedSeconds", "total_time_seconds"):
        if parsed.get(key) is not None:
            try:
                return float(parsed[key])
            except (TypeError, ValueError):
                return None
    return None


reconcile_tools = _fresh_import(RECONCILE_MODULE)
create_run_snapshots = reconcile_tools.create_run_snapshots
drop_run_snapshots = reconcile_tools.drop_run_snapshots
restore_run_snapshots = reconcile_tools.restore_run_snapshots


def _run_variant(name):
    if name == "production":
        module_name = PRODUCTION_MODULE
        args = common_args
    else:
        module_name = OUTPUT_V2_MODULE
        args = updated_args
    module = _fresh_import(module_name)
    _purge_run_outputs(name)
    started = time.perf_counter()
    result = module.run_load_allocation_input(spark, **args)
    elapsed = time.perf_counter() - started
    reconcile = importlib.import_module(RECONCILE_MODULE)
    fingerprints = reconcile.capture_outputs(
        spark,
        common_args["CatalogName"],
        common_args["SchemaName"],
        common_args["RunID"],
    )
    parsed_result = result
    if isinstance(parsed_result, str):
        try:
            parsed_result = json.loads(parsed_result)
        except (TypeError, ValueError):
            parsed_result = {}
    if not isinstance(parsed_result, dict):
        parsed_result = {}
    return {
        "variant": name,
        "elapsed_seconds": round(elapsed, 3),
        "reported_seconds": _reported_seconds(result),
        "checkpoint_mode": (
            str(updated_args.get("CheckpointMode", "common-default"))
            if name == "updated"
            else "production"
        ),
        "profile_plan": (
            updated_args["ProfilePlan"] if name == "updated" else "off"
        ),
        "fingerprints": json.dumps(
            fingerprints, default=str, sort_keys=True
        ),
        "checkpoint_activity": parsed_result.get(
            "checkpoint_timings", []
        ),
        "plan_profile": parsed_result.get("plan_profile") or [],
        "result": json.dumps(result, default=str, sort_keys=True),
    }


number_of_runs = max(1, int(dbutils.widgets.get("number_of_runs")))
execution_order = dbutils.widgets.get("ExecutionOrder")

rows = []
snapshots = create_run_snapshots(
    spark,
    common_args["CatalogName"],
    common_args["SchemaName"],
    common_args["RunID"],
)
try:
    for iteration in range(1, number_of_runs + 1):
        if execution_order == "alternate":
            order = (
                ["production", "updated"]
                if iteration % 2
                else ["updated", "production"]
            )
        elif execution_order == "original_first":
            order = ["production", "updated"]
        else:
            order = ["updated", "production"]
        print(
            f"[benchmark] pass {iteration} execution order: "
            f"{' -> '.join(order)}"
        )
        pass_rows = {}
        for variant in order:
            row = _run_variant(variant)
            row["iteration"] = iteration
            row["order"] = " -> ".join(order)
            rows.append(row)
            pass_rows[variant] = row
            print(
                f"[benchmark] {variant}: wall={row['elapsed_seconds']:.3f}s "
                f"reported={row['reported_seconds']}"
            )
        production = json.loads(pass_rows["production"]["fingerprints"])
        updated = json.loads(pass_rows["updated"]["fingerprints"])
        reconcile = importlib.import_module(RECONCILE_MODULE)
        mismatches = reconcile.compare_outputs(production, updated)
        if mismatches:
            print(
                f"[reconcile] FAIL run={iteration}: "
                f"{[row['table'] for row in mismatches]}"
            )
            raise AssertionError(
                f"Output parity failed: {json.dumps(mismatches, default=str)}"
            )
        print(f"[reconcile] PASS {iteration}: all output fingerprints match")
finally:
    try:
        restore_run_snapshots(
            spark,
            common_args["CatalogName"],
            common_args["SchemaName"],
            common_args["RunID"],
            snapshots,
        )
    except Exception:
        print(f"[reconcile] RESTORE FAILED; snapshots retained: {snapshots}")
        raise
    else:
        drop_run_snapshots(
            spark,
            common_args["CatalogName"],
            common_args["SchemaName"],
            snapshots,
        )

benchmark_rows = [
    {
        key: row[key]
        for key in (
            "iteration",
            "order",
            "variant",
            "elapsed_seconds",
            "reported_seconds",
            "checkpoint_mode",
            "profile_plan",
        )
    }
    for row in rows
]
display(
    spark.createDataFrame(benchmark_rows).orderBy("iteration", "variant")
)

# COMMAND ----------

summary = (
    spark.createDataFrame(benchmark_rows)
    .groupBy("variant", "checkpoint_mode", "profile_plan")
    .avg("elapsed_seconds")
    .withColumnRenamed("avg(elapsed_seconds)", "average_elapsed_seconds")
)
display(summary)

# COMMAND ----------

checkpoint_rows = [
    {"iteration": row["iteration"], **item}
    for row in rows
    if row["variant"] == "updated"
    for item in row["checkpoint_activity"]
]
plan_rows = [
    {"iteration": row["iteration"], **item}
    for row in rows
    if row["variant"] == "updated"
    for item in row["plan_profile"]
]
print("===== CHECKPOINT TELEMETRY =====")
if checkpoint_rows:
    display(spark.createDataFrame(checkpoint_rows).orderBy("iteration", "sequence"))
else:
    print("No checkpoint telemetry returned")
print("===== PLAN TELEMETRY =====")
if plan_rows:
    display(spark.createDataFrame(plan_rows).orderBy("iteration"))
else:
    print("Enable ProfilePlan to populate plan telemetry")
