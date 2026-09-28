# Databricks notebook source
# MAGIC %md
# MAGIC # Look-through cost allocation A/B
# MAGIC Defaults match the FEP Development notebook. Restore snapshots on exit.
# MAGIC Output is appended; Input is overwritten — restore, do not purge Output.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("number_of_runs", "1", "2. A/B passes")
dbutils.widgets.dropdown(
    "ExecutionOrder",
    "alternate",
    ["alternate", "original_first", "updated_first"],
    "3. Execution order",
)
dbutils.widgets.text("EntityID", "4137", "4. EntityID")
dbutils.widgets.text("ClientID", "15348", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "17376", "7. RunID")
dbutils.widgets.text("CatalogName", "QA7", "8. Catalog")
dbutils.widgets.text("SchemaName", "iPC_2025_QA7_15348", "9. Schema")
dbutils.widgets.text(
    "VolumePath",
    "/Volumes/qa7/datavolume/databrickdata",
    "10. Volume path",
)
dbutils.widgets.text("LineType", "K1 with Cost", "11. LineType")
dbutils.widgets.text("RankForRule", "0", "12. RankForRule")
dbutils.widgets.dropdown(
    "ExecutionProfile",
    "low",
    ["low", "medium", "big"],
    "13. Execution profile",
)
dbutils.widgets.text("MaxThreads", "", "14. Max threads (blank=profile)")
dbutils.widgets.dropdown(
    "ProfilePlan",
    "off",
    ["off", "on"],
    "15. Plan profiler",
)
dbutils.widgets.text("PlanCheckpointThreshold", "30", "16. Plan threshold")
dbutils.widgets.dropdown(
    "CheckpointMode",
    "default",
    ["default", "1", "2", "3", "4"],
    "17. Checkpoint mode",
)
dbutils.widgets.text(
    "SqlShufflePartitions",
    "",
    "18. spark.sql.shuffle.partitions (blank = profile)",
)
dbutils.widgets.dropdown(
    "ResultType",
    "deltalake",
    ["deltalake", "parquet"],
    "19. Result type",
)

# COMMAND ----------

import importlib
import sys
import time

source_path = dbutils.widgets.get("source_path").strip().rstrip("/")
runs = int(dbutils.widgets.get("number_of_runs") or "1")
order_setting = dbutils.widgets.get("ExecutionOrder")
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
volume_path = dbutils.widgets.get("VolumePath").strip()
line_type = dbutils.widgets.get("LineType").strip()
rank_for_rule = int(dbutils.widgets.get("RankForRule") or "0")
execution_profile = (
    dbutils.widgets.get("ExecutionProfile").strip() or "low"
)
max_threads_raw = dbutils.widgets.get("MaxThreads").strip()
workers = int(max_threads_raw) if max_threads_raw else None
profile_plan = dbutils.widgets.get("ProfilePlan").lower() == "on"
threshold = int(dbutils.widgets.get("PlanCheckpointThreshold") or "30")
mode_raw = dbutils.widgets.get("CheckpointMode").strip().lower()
mode = None if mode_raw in {"", "default"} else int(mode_raw)
shuffle = dbutils.widgets.get("SqlShufflePartitions").strip()
result_type = dbutils.widgets.get("ResultType").strip() or "deltalake"
if runs < 1 or (workers is not None and not 1 <= workers <= 4) or (
    mode is not None and mode not in (1, 2, 3, 4)
):
    raise ValueError("Invalid runs, MaxThreads, or CheckpointMode")
if mode == 3 and not volume_path:
    raise ValueError("VolumePath is required for CheckpointMode=3")
if shuffle:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle)

while source_path in sys.path:
    sys.path.remove(source_path)
sys.path.insert(0, source_path)
print(f"[benchmark] Python import root={sys.path[0]}", flush=True)

package = "AllocationV2.usp_load_lookthrough_cost_alloc_to_output"
production = f"{package}.output.load_lookthrough_cost_alloc_to_output"
updated = f"{package}.outputV2.load_lookthrough_cost_alloc_to_output"


def fresh_import(name):
    roots = (
        f"{package}.output",
        f"{package}.outputV2",
        "AllocationV2.plan_profiler",
        "Common_V2",
    )
    for loaded in list(sys.modules):
        if any(
            loaded == root or loaded.startswith(root + ".")
            for root in roots
        ):
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


def _reported_seconds(result):
    if not isinstance(result, dict):
        return None
    for key in ("elapsed_seconds", "elapsed"):
        if result.get(key) is not None:
            try:
                return float(result[key])
            except (TypeError, ValueError):
                return None
    return None


def run_variant(variant, number, snapshot):
    reconcile.reset_before_variant(spark, snapshot)
    module = fresh_import(production if variant == "original" else updated)
    kwargs = dict(
        EntityID=entity_id,
        ClientID=client_id,
        TaxPeriodID=tax_period_id,
        RunID=run_id,
        CatalogName=catalog,
        SchemaName=schema,
        VolumePath=volume_path or None,
        ExecutionID=f"lt-cost-ab-{number}-{variant}",
        line_type=line_type,
        rank_for_rule=rank_for_rule,
        result_type=result_type,
    )
    if variant == "updated":
        kwargs.update(
            ExecutionProfile=execution_profile,
            ProfilePlan=profile_plan,
            PlanCheckpointThreshold=threshold,
        )
        if workers is not None:
            kwargs["MaxThreads"] = workers
        if mode is not None:
            kwargs["CheckpointMode"] = mode
        if shuffle:
            kwargs["SqlShufflePartitions"] = int(shuffle)
    started = time.perf_counter()
    result = module.run_load_lookthrough_cost_alloc(spark, **kwargs)
    wall = round(time.perf_counter() - started, 3)
    metrics = reconcile.capture_metrics(spark, catalog, schema, run_id)
    profile = (
        module.get_last_run_profile()
        if variant == "updated" and hasattr(module, "get_last_run_profile")
        else {}
    )
    rows = reconcile.summarize_metrics(metrics)["total_rows"]
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s "
        f"reported={_reported_seconds(result)} rows={rows} "
        f"status={result.get('status') if isinstance(result, dict) else None} "
        f"skip_reason={result.get('skip_reason') if isinstance(result, dict) else None}"
    )
    return dict(
        pass_number=number,
        variant=variant,
        wall_seconds=wall,
        reported_seconds=_reported_seconds(result),
        rows=rows,
        status=result.get("status") if isinstance(result, dict) else None,
        skip_reason=(
            result.get("skip_reason") if isinstance(result, dict) else None
        ),
        metrics=metrics,
        profile=profile,
    )


records, parity = [], []
snapshot = reconcile.create_benchmark_snapshot(
    spark, catalog, schema, run_id
)
try:
    for number in range(1, runs + 1):
        order = order_for(number)
        print(
            f"[benchmark] pass {number} execution order: "
            f"{' -> '.join(order)}"
        )
        current = {
            variant: run_variant(variant, number, snapshot)
            for variant in order
        }
        records.extend(current.values())
        mismatches = reconcile.compare_metrics(
            current["original"]["metrics"],
            current["updated"]["metrics"],
        )
        mismatch_tables = {item["table"] for item in mismatches}
        parity.extend(
            dict(
                pass_number=number,
                table=table,
                matches=table not in mismatch_tables,
            )
            for table, _ in reconcile.TABLE_SPECS
        )
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

summary = [
    {
        key: row[key]
        for key in (
            "pass_number",
            "variant",
            "wall_seconds",
            "reported_seconds",
            "rows",
            "status",
            "skip_reason",
        )
    }
    for row in records
]
display(spark.createDataFrame(summary).orderBy("pass_number", "variant"))
display(spark.createDataFrame(parity).orderBy("pass_number", "table"))

delta = []
for number in range(1, runs + 1):
    current = {
        row["variant"]: row
        for row in records
        if row["pass_number"] == number
    }
    old = current["original"]["wall_seconds"]
    new = current["updated"]["wall_seconds"]
    delta.append(
        dict(
            pass_number=number,
            original_seconds=old,
            updated_seconds=new,
            improvement_percent=(
                round(100 * (old - new) / old, 2) if old else None
            ),
        )
    )
display(spark.createDataFrame(delta))


def profile_rows(key):
    return [
        dict(pass_number=row["pass_number"], **item)
        for row in records
        if row["variant"] == "updated"
        for item in row["profile"].get(key, [])
    ]


for key in (
    "timings",
    "checkpoint_activity",
    "plan_profile",
    "checkpoint_plan_profile",
    "action_profile",
):
    values = profile_rows(key)
    print(f"===== {key.upper()} =====")
    if values:
        display(spark.createDataFrame(values))
    else:
        print("No records")
