# Databricks notebook source
# MAGIC %md
# MAGIC # Footnote Allocation: production vs outputV2
# MAGIC
# MAGIC Snapshots and restores both AllocationInput and generated footnote
# MAGIC AllocationOutput rows. Do not run another process for this RunID.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("EntityID", "4032", "2. EntityID")
dbutils.widgets.text("ClientID", "15348", "3. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "4. TaxPeriodID")
dbutils.widgets.text("RunID", "18263", "5. RunID")
dbutils.widgets.text("CatalogName", "QA7", "6. Catalog")
dbutils.widgets.text(
    "SchemaName", "IPC_2025_QA7_15348", "7. Schema"
)
dbutils.widgets.dropdown(
    "ExecutionProfile",
    "low",
    ["low", "medium", "big"],
    "8. Execution profile",
)
dbutils.widgets.text("number_of_runs", "1", "9. A/B passes")
dbutils.widgets.dropdown(
    "ProfilePlan", "off", ["off", "on"], "10. Plan profile"
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(
    dbutils.widgets.get("number_of_runs").strip() or "1"
)
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
rank = 1
execution_profile = (
    dbutils.widgets.get("ExecutionProfile").strip() or "low"
)
if number_of_runs < 1:
    raise ValueError("A/B passes must be >= 1")

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
    del pass_number
    return ("original", "updated")


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
                "ExecutionProfile": execution_profile,
                "ProfilePlan": profile_plan,
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
        f"sp_status={result.get('status') if isinstance(result, dict) else None} "
        f"skip_reason={result.get('skip_reason') if isinstance(result, dict) else None} "
        f"inserted={result.get('write_inserted_rows') if isinstance(result, dict) else None} "
        f"live_footnote={result.get('live_footnote_rows') if isinstance(result, dict) else None} "
        f"output_rows={summary['allocation_output_rows']} "
        f"input_rows={summary['allocation_input_rows']}"
    )
    if isinstance(result, dict):
        print(
            f"[benchmark] {variant} types combined="
            f"{result.get('combined_allocation_types')} "
            f"live={result.get('live_allocation_types')}"
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
        "status": (
            result.get("status")
            if isinstance(result, dict)
            else "UNKNOWN"
        ),
        "skip_reason": (
            result.get("skip_reason")
            if isinstance(result, dict)
            else None
        ),
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
        "skip_reason": row.get("skip_reason"),
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
