# Databricks notebook source
# MAGIC %md
# MAGIC # Look-through footnote effective allocation % A/B
# MAGIC Defaults match the FEP Development notebook. Restore snapshots on exit.
# MAGIC `LookThroughAllocationOutput` is input and output — restore, do not purge.

# COMMAND ----------

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("EntityID", "4137", "2. EntityID")
dbutils.widgets.text("ClientID", "15348", "3. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "4. TaxPeriodID")
dbutils.widgets.text("RunID", "17376", "5. RunID")
dbutils.widgets.text("CatalogName", "QA7", "6. Catalog")
dbutils.widgets.text("SchemaName", "iPC_2025_QA7_15348", "7. Schema")
dbutils.widgets.dropdown(
    "ExecutionProfile",
    "low",
    ["low", "medium", "big"],
    "8. Execution profile",
)
dbutils.widgets.text("number_of_runs", "1", "9. A/B passes")
dbutils.widgets.dropdown(
    "ProfilePlan",
    "off",
    ["off", "on"],
    "10. Plan profile",
)

# COMMAND ----------

import importlib
import sys
import time

source_path = dbutils.widgets.get("source_path").strip().rstrip("/")
runs = int(dbutils.widgets.get("number_of_runs") or "1")
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
volume_path = "/Volumes/qa7/datavolume/databrickdata"
execution_profile = (
    dbutils.widgets.get("ExecutionProfile").strip() or "low"
)
result_type = "deltalake"
if runs < 1:
    raise ValueError("A/B passes must be >= 1")

while source_path in sys.path:
    sys.path.remove(source_path)
sys.path.insert(0, source_path)
print(f"[benchmark] Python import root={sys.path[0]}", flush=True)

package = (
    "AllocationV2.usp_load_lookthrough_footnote_effective_allocation_pct"
)
production = (
    f"{package}.output.load_lt_footnote_effective_allocation_pct"
)
updated = (
    f"{package}.outputV2.load_lt_footnote_effective_allocation_pct"
)


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
    del number
    return ("original", "updated")


def _reported_seconds(result):
    if isinstance(result, dict) and result.get("elapsed_seconds") is not None:
        try:
            return float(result["elapsed_seconds"])
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
        ResultType=result_type,
        VolumePath=volume_path or None,
        ExecutionID=f"lt-fn-eff-ab-{number}-{variant}",
    )
    if variant == "updated":
        kwargs["ExecutionProfile"] = execution_profile
        kwargs["ProfilePlan"] = profile_plan
    started = time.perf_counter()
    result = module.run_load_lt_footnote_effective_allocation_pct(
        spark, **kwargs
    )
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
    "parallel_activity",
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
