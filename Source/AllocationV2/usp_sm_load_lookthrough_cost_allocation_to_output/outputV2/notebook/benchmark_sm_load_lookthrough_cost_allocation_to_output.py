# Databricks notebook source
# MAGIC %md
# MAGIC # SM look-through cost allocation A/B
# MAGIC LT QA identity. RankForRulePickup=0 hardcoded. Restore SM Output and Input.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("EntityID", "4755", "2. EntityID")
dbutils.widgets.text("ClientID", "15348", "3. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "4. TaxPeriodID")
dbutils.widgets.text("RunID", "18266", "5. RunID")
dbutils.widgets.text("CatalogName", "qa7", "6. Catalog")
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
execution_profile = (
    dbutils.widgets.get("ExecutionProfile").strip() or "low"
)
volume_path = "/Volumes/qa7/datavolume/databrickdata"
result_type = "deltalake"
rank_for_rule_pickup = 0
if runs < 1:
    raise ValueError("A/B passes must be >= 1")

while source_path in sys.path:
    sys.path.remove(source_path)
sys.path.insert(0, source_path)
print(f"[benchmark] Python import root={sys.path[0]}", flush=True)

package = (
    "AllocationV2.usp_sm_load_lookthrough_cost_allocation_to_output"
)
production = f"{package}.output.orchestrator"
updated = f"{package}.outputV2.orchestrator"


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
        RankForRulePickup=rank_for_rule_pickup,
        ResultType=result_type,
        VolumePath=volume_path or None,
        ExecutionID=f"sm-lt-cost-ab-{number}-{variant}",
    )
    if variant == "updated":
        kwargs["ExecutionProfile"] = execution_profile
        kwargs["ProfilePlan"] = profile_plan
    started = time.perf_counter()
    result = module.run_sm_load_lookthrough_cost_allocation_to_output(
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

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

summary_schema = StructType(
    [
        StructField("pass_number", IntegerType(), False),
        StructField("variant", StringType(), False),
        StructField("wall_seconds", DoubleType(), True),
        StructField("reported_seconds", DoubleType(), True),
        StructField("rows", LongType(), True),
        StructField("status", StringType(), True),
        StructField("skip_reason", StringType(), True),
    ]
)
summary = [
    (
        int(row["pass_number"]),
        str(row["variant"]),
        None if row["wall_seconds"] is None else float(row["wall_seconds"]),
        None
        if row["reported_seconds"] is None
        else float(row["reported_seconds"]),
        int(row["rows"] or 0),
        "" if row["status"] is None else str(row["status"]),
        "" if row["skip_reason"] is None else str(row["skip_reason"]),
    )
    for row in records
]
display(spark.createDataFrame(summary, summary_schema).orderBy("pass_number", "variant"))

parity_schema = StructType(
    [
        StructField("pass_number", IntegerType(), False),
        StructField("table", StringType(), False),
        StructField("matches", BooleanType(), False),
    ]
)
display(
    spark.createDataFrame(
        [
            (int(row["pass_number"]), str(row["table"]), bool(row["matches"]))
            for row in parity
        ],
        parity_schema,
    ).orderBy("pass_number", "table")
)

delta_schema = StructType(
    [
        StructField("pass_number", IntegerType(), False),
        StructField("original_seconds", DoubleType(), True),
        StructField("updated_seconds", DoubleType(), True),
        StructField("improvement_percent", DoubleType(), True),
    ]
)
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
        (
            int(number),
            None if old is None else float(old),
            None if new is None else float(new),
            None if not old else float(round(100 * (old - new) / old, 2)),
        )
    )
display(spark.createDataFrame(delta, delta_schema))


def profile_rows(key):
    return [
        dict(pass_number=row["pass_number"], **item)
        for row in records
        if row["variant"] == "updated"
        for item in row["profile"].get(key, [])
    ]


def show_profile(values):
    if not values:
        print("No records")
        return
    cleaned = [
        {
            key: "" if value is None else str(value)
            for key, value in item.items()
        }
        for item in values
    ]
    display(spark.createDataFrame(cleaned))


for key in (
    "timings",
    "parallel_activity",
    "checkpoint_activity",
    "plan_profile",
    "checkpoint_plan_profile",
    "action_profile",
):
    print(f"===== {key.upper()} =====")
    show_profile(profile_rows(key))
