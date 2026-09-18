# Databricks notebook source
# MAGIC %md
# MAGIC # Look-through allocation input A/B benchmark
# MAGIC Use an isolated RunID; all affected rows are restored on exit.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_path", "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source")
dbutils.widgets.text("number_of_runs", "2")
dbutils.widgets.dropdown("ExecutionOrder", "alternate", ["alternate", "original_first", "updated_first"])
for name in ("EntityID", "ClientID", "TaxPeriodID", "RunID", "CatalogName", "SchemaName"):
    dbutils.widgets.text(name, "")
dbutils.widgets.dropdown("ResultType", "deltalake", ["deltalake"])
dbutils.widgets.text("VolumePath", "")
dbutils.widgets.text("MaxThreads", "4")
dbutils.widgets.dropdown("ProfilePlan", "off", ["off", "on"])
dbutils.widgets.text("PlanCheckpointThreshold", "30")
dbutils.widgets.dropdown("CheckpointMode", "default", ["default", "1", "2", "3", "4"])
dbutils.widgets.text("SqlShufflePartitions", "")

# COMMAND ----------

import importlib
import sys
import time

source_path = dbutils.widgets.get("source_path").strip().rstrip("/")
runs = int(dbutils.widgets.get("number_of_runs") or "2")
order_setting = dbutils.widgets.get("ExecutionOrder")
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
volume_path = dbutils.widgets.get("VolumePath").strip()
workers = int(dbutils.widgets.get("MaxThreads") or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").lower() == "on"
threshold = int(dbutils.widgets.get("PlanCheckpointThreshold") or "30")
mode_raw = dbutils.widgets.get("CheckpointMode").strip().lower()
mode = None if mode_raw in {"", "default"} else int(mode_raw)
shuffle = dbutils.widgets.get("SqlShufflePartitions").strip()
if runs < 1 or not 1 <= workers <= 4 or (mode is not None and mode not in (1, 2, 3, 4)):
    raise ValueError("Invalid runs, MaxThreads, or CheckpointMode")
if mode == 3 and not volume_path:
    raise ValueError("VolumePath is required for CheckpointMode=3")
if shuffle:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle)
sys.path[:] = [item for item in sys.path if item != source_path]
sys.path.insert(0, source_path)

package = "AllocationV2.usp_load_lookthrough_allocation_input"
production = f"{package}.output.load_lookthrough_allocation_input"
updated = f"{package}.outputV2.load_lookthrough_allocation_input"


def fresh_import(name):
    roots = (f"{package}.output", f"{package}.outputV2", "AllocationV2.plan_profiler", "Common_V2")
    for loaded in list(sys.modules):
        if any(loaded == root or loaded.startswith(root + ".") for root in roots):
            del sys.modules[loaded]
    importlib.invalidate_caches()
    checkpoint = importlib.import_module("Common_V2.core.checkpoint_V2")
    print(f"[import] checkpoint_V2={checkpoint.__file__}")
    module = importlib.import_module(name)
    print(f"[import] runner={module.__file__}")
    return module


reconcile = fresh_import(f"{package}.outputV2.output_reconcile")


def order_for(number):
    if order_setting == "original_first":
        return ("original", "updated")
    if order_setting == "updated_first":
        return ("updated", "original")
    return ("original", "updated") if number % 2 else ("updated", "original")


def run_variant(variant, number, snapshot):
    reconcile.reset_before_variant(spark, snapshot)
    module = fresh_import(production if variant == "original" else updated)
    kwargs = dict(
        EntityID=entity_id, ClientID=client_id, TaxPeriodID=tax_period_id,
        RunID=run_id, CatalogName=catalog, SchemaName=schema,
        ResultType="deltalake", VolumePath=volume_path or None,
        ExecutionID=f"lt-input-ab-{number}-{variant}",
    )
    if variant == "updated":
        kwargs.update(MaxThreads=workers, ProfilePlan=profile_plan, PlanCheckpointThreshold=threshold)
        if mode is not None:
            kwargs["CheckpointMode"] = mode
    started = time.perf_counter()
    result = module.run_load_lookthrough_allocation_input(spark, **kwargs)
    wall = round(time.perf_counter() - started, 3)
    metrics = reconcile.capture_metrics(spark, catalog, schema, run_id)
    profile = module.get_last_run_profile() if variant == "updated" else {}
    rows = reconcile.summarize_metrics(metrics)["total_rows"]
    print(f"[benchmark] {variant}: wall={wall:.3f}s reported={result.get('elapsed_seconds')} rows={rows}")
    return dict(
        pass_number=number, variant=variant, wall_seconds=wall,
        reported_seconds=result.get("elapsed_seconds"), rows=rows,
        status=result.get("status"), metrics=metrics, profile=profile,
    )


records, parity = [], []
snapshot = reconcile.create_benchmark_snapshot(spark, catalog, schema, run_id)
try:
    for number in range(1, runs + 1):
        order = order_for(number)
        print(f"[benchmark] pass {number} execution order: {' -> '.join(order)}")
        current = {variant: run_variant(variant, number, snapshot) for variant in order}
        records.extend(current.values())
        mismatches = reconcile.compare_metrics(current["original"]["metrics"], current["updated"]["metrics"])
        mismatch_tables = {item["table"] for item in mismatches}
        parity.extend(dict(pass_number=number, table=table, matches=table not in mismatch_tables) for table, _ in reconcile.TABLE_SPECS)
        if mismatches:
            raise AssertionError(f"First mismatch: {mismatches[0]}")
        print(f"[reconcile] PASS {number}: all output fingerprints match")
finally:
    try:
        reconcile.restore_original_state(spark, snapshot)
    except Exception:
        print("[reconcile] RESTORE FAILED; benchmark backups retained")
        raise
    else:
        reconcile.drop_benchmark_snapshot(spark, snapshot)

# COMMAND ----------

summary = [{key: row[key] for key in ("pass_number", "variant", "wall_seconds", "reported_seconds", "rows", "status")} for row in records]
display(spark.createDataFrame(summary).orderBy("pass_number", "variant"))
display(spark.createDataFrame(parity).orderBy("pass_number", "table"))

delta = []
for number in range(1, runs + 1):
    current = {row["variant"]: row for row in records if row["pass_number"] == number}
    old, new = current["original"]["wall_seconds"], current["updated"]["wall_seconds"]
    delta.append(dict(pass_number=number, original_seconds=old, updated_seconds=new, improvement_percent=round(100 * (old - new) / old, 2) if old else None))
display(spark.createDataFrame(delta))


def profile_rows(key):
    return [dict(pass_number=row["pass_number"], **item) for row in records if row["variant"] == "updated" for item in row["profile"].get(key, [])]


for key in ("timings", "parallel_activity", "checkpoint_activity", "plan_profile", "checkpoint_plan_profile", "action_profile"):
    values = profile_rows(key)
    print(f"===== {key.upper()} =====")
    display(spark.createDataFrame(values)) if values else print("No records")

consumers = {"alloc_input_post_unions": 9, "alloc_input_post_pfic": 7}
checkpoint_times = {
    (row["pass_number"], row.get("name")): row.get("elapsed_seconds")
    for row in profile_rows("checkpoint_activity")
}
recommendations = []
for row in profile_rows("checkpoint_plan_profile"):
    name = str(row.get("func", "")).split("#", 1)[0]
    recommendations.append(dict(
        pass_number=row["pass_number"], name=name, builder_delta=row.get("delta"),
        checkpoint_nodes=row.get("nodes"), depth=row.get("depth"),
        operator_mix=str(row.get("ops") or {}), consumer_count=consumers.get(name),
        fan_out=(consumers.get(name) or 0) > 1,
        measured_action_seconds=None,
        measured_checkpoint_seconds=checkpoint_times.get(
            (row["pass_number"], name)
        ),
        recommendation=row.get("recommendation"),
        rationale="Retain until measured A/B parity and timing justify removal.",
    ))
print("===== CHECKPOINT RECOMMENDATIONS =====")
display(spark.createDataFrame(recommendations)) if recommendations else print("Enable ProfilePlan")
