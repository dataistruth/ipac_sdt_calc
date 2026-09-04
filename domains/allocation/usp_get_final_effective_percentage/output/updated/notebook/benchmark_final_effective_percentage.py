# Databricks notebook source
# MAGIC %md
# MAGIC # A/B benchmark — usp_get_final_effective_percentage
# MAGIC
# MAGIC Runs the unchanged production module and `output.updated` against the
# MAGIC same RunID. Each variant starts with clean output partitions. Parity
# MAGIC compares row counts, amount sums, schemas, and order-independent row
# MAGIC fingerprints for all three output tables.
# MAGIC
# MAGIC This notebook lives in `output/updated/notebook/`. Python modules live
# MAGIC one folder up in `output/updated/`. Do not import-dir modules into
# MAGIC this `notebook/` folder.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Monolith Source/",
)
dbutils.widgets.text("number_of_runs", "1", "2. A/B passes")
dbutils.widgets.dropdown("Mode", "0", ["0", "4"], "3. Mode")
dbutils.widgets.text("EntityID", "4137", "4. EntityID")
dbutils.widgets.text("ClientID", "15348", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "17376", "7. RunID")
dbutils.widgets.text("CatalogName", "QA7", "8. Catalog")
dbutils.widgets.text(
    "SchemaName",
    "iPC_2025_QA7_15348",
    "9. Schema",
)
dbutils.widgets.dropdown(
    "ResultType",
    "deltalake",
    ["deltalake"],
    "10. Result type",
)
dbutils.widgets.text("VolumePath", "", "11. Volume path (optional)")
dbutils.widgets.dropdown(
    "CheckpointProfile",
    "conservative",
    ["full", "conservative", "balanced"],
    "12. Checkpoint profile",
)
dbutils.widgets.dropdown(
    "ProfilePlan",
    "off",
    ["off", "on"],
    "13. Plan profiler",
)
dbutils.widgets.text(
    "PlanCheckpointThreshold",
    "30",
    "14. Plan checkpoint threshold",
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs").strip() or "1")
mode = int(dbutils.widgets.get("Mode").strip())
entity_id = int(dbutils.widgets.get("EntityID").strip())
client_id = int(dbutils.widgets.get("ClientID").strip())
tax_period_id = int(dbutils.widgets.get("TaxPeriodID").strip())
run_id = int(dbutils.widgets.get("RunID").strip())
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
result_type = dbutils.widgets.get("ResultType").strip()
volume_path = dbutils.widgets.get("VolumePath").strip()
checkpoint_profile = dbutils.widgets.get("CheckpointProfile").strip().lower()
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
plan_checkpoint_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold").strip() or "30"
)

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if result_type.lower() != "deltalake":
    raise ValueError(
        "The A/B parity benchmark requires ResultType=deltalake so output "
        "partitions can be reconciled after each variant."
    )

# COMMAND ----------

import importlib
import json
import os
import sys
import time
from datetime import datetime

if not os.path.isdir(source_path):
    raise RuntimeError(f"Source path does not exist: {source_path}")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_get_final_effective_percentage"
ORIGINAL_MODULE = f"{PACKAGE}.output.orchestrator"
UPDATED_MODULE = f"{PACKAGE}.output.updated.orchestrator"


def _import_fresh(module_name: str):
    for loaded in list(sys.modules):
        if loaded == PACKAGE or loaded.startswith(f"{PACKAGE}."):
            del sys.modules[loaded]
    importlib.invalidate_caches()
    module = importlib.import_module(module_name)
    print(f"imported {module_name}: {module.__file__}")
    return module


def _assert_updated_package_synced() -> None:
    """Fail early if updated modules are missing from the Python package path.

    import-dir for .py modules must target output/updated, not
    output/updated/notebook or output/updated/notebooks.
    """
    updated_dir = os.path.join(
        source_path,
        *PACKAGE.split("."),
        "output",
        "updated",
    )
    sibling_dirs = [
        os.path.join(updated_dir, "notebook"),
        os.path.join(updated_dir, "notebooks"),
    ]
    required = [
        "parent.py",
        "checkpoint.py",
        "read_optimizations.py",
        "cost_pct_loader.py",
        "output_reconcile.py",
        "orchestrator.py",
        "__init__.py",
    ]
    print(f"[sync check] package dir: {updated_dir}")
    print(f"[sync check] exists: {os.path.isdir(updated_dir)}")
    if os.path.isdir(updated_dir):
        print(f"[sync check] files: {sorted(os.listdir(updated_dir))}")
    misplaced = []
    for sibling in sibling_dirs:
        for name in required:
            if os.path.isfile(os.path.join(sibling, name)) and not os.path.isfile(
                os.path.join(updated_dir, name)
            ):
                misplaced.append(f"{os.path.basename(sibling)}/{name}")
    missing = [
        name
        for name in required
        if not os.path.isfile(os.path.join(updated_dir, name))
    ]
    if misplaced:
        raise ModuleNotFoundError(
            "Updated modules were imported into output/updated/notebook(s)/ "
            "instead of output/updated/. Python cannot import them from there. "
            "Rerun import-dir with destination "
            ".../usp_get_final_effective_percentage/output/updated "
            f"(found: {', '.join(misplaced)})"
        )
    if missing:
        raise ModuleNotFoundError(
            "output/updated is missing files on the Python path "
            f"{updated_dir}: {', '.join(missing)}. "
            "Resync output/updated/ (not notebook/) and restart Python."
        )


_assert_updated_package_synced()

CPBT_MODULE = f"{PACKAGE}.output.updated.cost_pct_loader"
try:
    cpbt_module = importlib.import_module(CPBT_MODULE)
except Exception as exc:
    raise ImportError(
        f"Optimized CPBT module exists on disk but cannot be imported: "
        f"{CPBT_MODULE}. Root cause: {type(exc).__name__}: {exc}"
    ) from exc
print(
    f"[sync check] CPBT import OK: {cpbt_module.__file__} | "
    f"profile={cpbt_module.OPTIMIZATION_PROFILE_MARKER}"
)

reconcile = importlib.import_module(f"{PACKAGE}.output.updated.output_reconcile")

# COMMAND ----------

def _run_variant(variant: str, pass_number: int) -> dict:
    module_name = ORIGINAL_MODULE if variant == "original" else UPDATED_MODULE
    runner = _import_fresh(module_name)

    # Fresh import removed the first reconcile module object; use this module's
    # functions already held by the notebook.
    reconcile.purge_output_partitions_for_run(
        spark, catalog, schema, run_id
    )

    print(
        f"\n{'=' * 72}\n"
        f"PASS {pass_number} | {variant.upper()} | "
        f"{datetime.now().isoformat()}\n"
        f"{'=' * 72}"
    )
    started = time.time()
    run_kwargs = dict(
        Mode=mode,
        EntityID=entity_id,
        ClientID=client_id,
        TaxPeriodID=tax_period_id,
        RunID=run_id,
        CatalogName=catalog,
        SchemaName=schema,
        ResultType=result_type,
        VolumePath=volume_path or None,
        ExecutionID=f"benchmark-{pass_number}-{variant}",
    )
    if variant == "updated":
        run_kwargs["CheckpointProfile"] = checkpoint_profile
        # Plan-size profiler is driven by the "13. Plan profiler" widget, not
        # by cfg. When on, the updated runner measures per-builder logical-plan
        # (DAG) growth and returns a ranked report in result["plan_profile"].
        if profile_plan:
            run_kwargs["profile_plan"] = True
            run_kwargs["plan_checkpoint_threshold"] = plan_checkpoint_threshold

    result = runner.run_final_effective_percentages(
        spark,
        **run_kwargs,
    )
    wall = round(time.time() - started, 3)

    metrics = reconcile.capture_output_metrics(
        spark, catalog, schema, run_id
    )
    summary = reconcile.summarize_metrics(metrics)
    profile_data = (
        runner.get_last_run_profile()
        if variant == "updated" and hasattr(runner, "get_last_run_profile")
        else {}
    )
    reported = (
        result.get("elapsed_seconds")
        if isinstance(result, dict)
        else profile_data.get("updated_wall_seconds")
    )
    timings = (
        result.get("timings", [])
        if isinstance(result, dict)
        else profile_data.get("timings", [])
    )
    checkpoint_summary = profile_data.get(
        "checkpoint_summary",
        result.get("checkpoint_summary", {}) if isinstance(result, dict) else {},
    )
    plan_profile = profile_data.get(
        "plan_profile",
        result.get("plan_profile", []) if isinstance(result, dict) else [],
    )
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s "
        f"reported={reported} rows={summary['total_rows']}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "summary": summary,
        "metrics": metrics,
        "timings": timings,
        "cpbt_profile": profile_data.get("cpbt_profile"),
        "checkpoint_profile": checkpoint_summary.get("profile"),
        "checkpoints_written": checkpoint_summary.get("written_count"),
        "checkpoints_bypassed": checkpoint_summary.get("bypassed_count"),
        "plan_profile": plan_profile,
    }


records = []
for pass_number in range(1, number_of_runs + 1):
    # Alternate execution order to reduce systematic warm-cache bias.
    execution_order = (
        ("original", "updated")
        if pass_number % 2 == 1
        else ("updated", "original")
    )
    print(
        f"[benchmark] pass {pass_number} execution order: "
        f"{' -> '.join(execution_order)}"
    )
    pass_records = {
        variant: _run_variant(variant, pass_number)
        for variant in execution_order
    }
    records.extend(pass_records.values())

    mismatches = reconcile.compare_variants(
        pass_records["original"]["metrics"],
        pass_records["updated"]["metrics"],
    )
    if mismatches:
        print(f"[reconcile] FAIL pass {pass_number}")
        for mismatch in mismatches:
            print(f"  {mismatch}")
        raise AssertionError(
            f"Output parity failed with {len(mismatches)} mismatch(es)"
        )
    print(f"[reconcile] PASS {pass_number}: all output fingerprints match")

# COMMAND ----------

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Explicit schemas avoid [CANNOT_DETERMINE_TYPE]: reported_seconds is None for
# the original variant, and timing/summary columns can be entirely null.
_summary_schema = StructType([
    StructField("pass", LongType(), True),
    StructField("variant", StringType(), True),
    StructField("wall_seconds", DoubleType(), True),
    StructField("reported_seconds", DoubleType(), True),
    StructField("total_rows", LongType(), True),
    StructField("tables_with_rows", LongType(), True),
    StructField("cpbt_profile", StringType(), True),
    StructField("checkpoint_profile", StringType(), True),
    StructField("checkpoints_written", LongType(), True),
    StructField("checkpoints_bypassed", LongType(), True),
])


def _as_float(value):
    return float(value) if value is not None else None


def _as_int(value):
    return int(value) if value is not None else None


summary_rows = [
    (
        _as_int(row["pass"]),
        row["variant"],
        _as_float(row["wall_seconds"]),
        _as_float(row["reported_seconds"]),
        _as_int(row["summary"]["total_rows"]),
        _as_int(row["summary"]["tables_with_rows"]),
        row["cpbt_profile"],
        row["checkpoint_profile"],
        _as_int(row["checkpoints_written"]),
        _as_int(row["checkpoints_bypassed"]),
    )
    for row in records
]
if summary_rows:
    display(
        spark.createDataFrame(summary_rows, schema=_summary_schema)
        .orderBy("pass", "variant")
    )
else:
    print("No benchmark records to display")

_timings_schema = StructType([
    StructField("step", StringType(), True),
    StructField("calls", LongType(), True),
    StructField("elapsed_seconds", DoubleType(), True),
])

for row in records:
    if row["variant"] == "updated" and row["timings"]:
        print(f"\nUpdated timings — pass {row['pass']}")
        timing_rows = [
            (
                str(item.get("step")),
                _as_int(item.get("calls")),
                _as_float(item.get("elapsed_seconds")),
            )
            for item in row["timings"]
        ]
        display(
            spark.createDataFrame(timing_rows, schema=_timings_schema)
            .orderBy("elapsed_seconds", ascending=False)
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan-size profile (only when "13. Plan profiler" = on)
# MAGIC Ranks builders by how much they grow the Spark logical plan (DAG).
# MAGIC The largest `delta` is the seam where adding a `checkpoint()` helps most.

_plan_schema = StructType([
    StructField("func", StringType(), True),
    StructField("nodes", LongType(), True),
    StructField("depth", LongType(), True),
    StructField("delta", LongType(), True),
    StructField("checkpoint_candidate", StringType(), True),
])

for row in records:
    if row["variant"] == "updated" and row.get("plan_profile"):
        print(f"\nPlan profile — pass {row['pass']}")
        plan_rows = [
            (
                str(item.get("func")),
                _as_int(item.get("nodes")),
                _as_int(item.get("depth")),
                _as_int(item.get("delta")),
                "yes"
                if (item.get("delta") or 0) >= plan_checkpoint_threshold
                else "",
            )
            for item in row["plan_profile"]
        ]
        display(
            spark.createDataFrame(plan_rows, schema=_plan_schema)
            .orderBy("delta", ascending=False)
        )
