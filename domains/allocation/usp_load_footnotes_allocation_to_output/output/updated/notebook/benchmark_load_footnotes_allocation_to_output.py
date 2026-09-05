# Databricks notebook source
# MAGIC %md
# MAGIC # A/B benchmark — usp_load_footnotes_allocation_to_output
# MAGIC
# MAGIC Compares the unchanged production orchestrator with `output.updated`.
# MAGIC The SP appends `AllocationOutput` and mutates `AllocationInput`; this
# MAGIC notebook snapshots both tables and restores their original state in a
# MAGIC `finally` block.
# MAGIC
# MAGIC **Do not run another process for this RunID during the benchmark.**

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Monolith Source/",
)
dbutils.widgets.text("number_of_runs", "1", "2. A/B passes")
dbutils.widgets.text("EntityID", "115", "3. EntityID")
dbutils.widgets.text("ClientID", "15348", "4. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "5. TaxPeriodID")
dbutils.widgets.text("RunID", "16560", "6. RunID")
dbutils.widgets.text("CatalogName", "QA7", "7. Catalog")
dbutils.widgets.text("SchemaName", "IPC_2025_QA7_15348", "8. Schema")
dbutils.widgets.dropdown(
    "RankForRulePickup", "1", ["1", "2"], "9. Rank for rule pickup"
)
dbutils.widgets.text("ParallelWorkers", "4", "10. Updated parallel workers")
dbutils.widgets.dropdown(
    "ProfilePlan", "on", ["off", "on"], "11. Plan profiler"
)
dbutils.widgets.text(
    "PlanCheckpointThreshold", "30", "12. Plan checkpoint threshold"
)
dbutils.widgets.dropdown(
    "CheckpointBackend", "local", ["delta", "local"], "13. Checkpoint backend"
)
dbutils.widgets.text(
    "SqlShufflePartitions", "4", "14. spark.sql.shuffle.partitions (blank=default)"
)
dbutils.widgets.text(
    "LocalDeltaDenylist", "", "15. Local backend delta-denylist (comma-sep)"
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs").strip() or "1")
entity_id = int(dbutils.widgets.get("EntityID").strip())
client_id = int(dbutils.widgets.get("ClientID").strip())
tax_period_id = int(dbutils.widgets.get("TaxPeriodID").strip())
run_id = int(dbutils.widgets.get("RunID").strip())
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
rank_for_rule_pickup = int(dbutils.widgets.get("RankForRulePickup").strip())
parallel_workers = int(dbutils.widgets.get("ParallelWorkers").strip() or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
plan_checkpoint_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold").strip() or "30"
)
# Checkpoint backend: "local" (df.localCheckpoint, no metastore commit -- the
# fast default) or "delta" (durable UC Delta temp table). If a local run crashes
# with UNRESOLVED_COLUMN at a checkpoint (a self-join seam), add that checkpoint
# name to widget 15 to force it back to delta.
checkpoint_backend = dbutils.widgets.get("CheckpointBackend").strip().lower()
local_delta_denylist = dbutils.widgets.get("LocalDeltaDenylist").strip()
# Session-wide shuffle-partition cap (blank = leave cluster/AQE default). The
# 200 default fans small joins into many tiny tasks/files; 4 suits this dataset.
sql_shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if not 1 <= parallel_workers <= 8:
    raise ValueError("ParallelWorkers must be between 1 and 8")
if checkpoint_backend not in ("delta", "local"):
    raise ValueError("CheckpointBackend must be 'delta' or 'local'")
if sql_shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", sql_shuffle_partitions)
    print(
        "[config] spark.sql.shuffle.partitions = "
        f"{spark.conf.get('spark.sql.shuffle.partitions')}"
    )

# COMMAND ----------

import importlib
import os
import sys
import time
import uuid
from datetime import datetime

if not os.path.isdir(source_path):
    raise RuntimeError(f"Source path does not exist: {source_path}")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_load_footnotes_allocation_to_output"
ORIGINAL_MODULE = f"{PACKAGE}.output.orchestrator"
UPDATED_MODULE = f"{PACKAGE}.output.updated.orchestrator"
RECONCILE_MODULE = f"{PACKAGE}.output.updated.output_reconcile"


def _assert_updated_package_synced() -> None:
    updated_dir = os.path.join(
        source_path, *PACKAGE.split("."), "output", "updated"
    )
    required = [
        "__init__.py",
        "checkpoint.py",
        "join_optimizations.py",
        "orchestrator.py",
        "output_reconcile.py",
        "plan_break_optimizations.py",
    ]
    missing = [
        name for name in required
        if not os.path.isfile(os.path.join(updated_dir, name))
    ]
    print(f"[sync check] package dir: {updated_dir}")
    if missing:
        raise ModuleNotFoundError(
            f"output/updated is missing: {', '.join(missing)}. "
            "Sync the entire output/updated folder to this exact destination "
            "and restart Python."
        )


def _clear_package_modules() -> None:
    for loaded in list(sys.modules):
        if loaded == PACKAGE or loaded.startswith(f"{PACKAGE}."):
            del sys.modules[loaded]


def _import_fresh(module_name: str):
    _clear_package_modules()
    importlib.invalidate_caches()
    module = importlib.import_module(module_name)
    print(f"imported {module_name}: {module.__file__}")
    return module


_assert_updated_package_synced()
_clear_package_modules()
importlib.invalidate_caches()
reconcile = importlib.import_module(RECONCILE_MODULE)

# Bind helpers before fresh imports clear the package cache.
create_snapshot = reconcile.create_benchmark_snapshot
reset_before_variant = reconcile.reset_before_variant
capture_metrics = reconcile.capture_output_metrics
compare_variants = reconcile.compare_variants
summarize_metrics = reconcile.summarize_metrics
restore_original_state = reconcile.restore_original_state
drop_snapshot = reconcile.drop_benchmark_snapshot

# COMMAND ----------


def _run_variant(variant: str, pass_number: int, snapshot: dict) -> dict:
    reset_before_variant(spark, snapshot)
    module_name = ORIGINAL_MODULE if variant == "original" else UPDATED_MODULE
    runner = _import_fresh(module_name)

    print(
        f"\n{'=' * 72}\n"
        f"PASS {pass_number} | {variant.upper()} | {datetime.now().isoformat()}\n"
        f"{'=' * 72}"
    )
    updated_kwargs = {}
    if variant == "updated":
        updated_kwargs["parallel_workers"] = parallel_workers
        # Plan-size profiler is driven by the "11. Plan profiler" widget.
        if profile_plan:
            updated_kwargs["profile_plan"] = True
            updated_kwargs["plan_checkpoint_threshold"] = plan_checkpoint_threshold
        # Checkpoint backend + optional local-mode delta-denylist (widgets 13/15).
        updated_kwargs["checkpoint_backend"] = checkpoint_backend
        if checkpoint_backend == "local" and local_delta_denylist:
            updated_kwargs["local_delta_denylist"] = local_delta_denylist

    started = time.time()
    result = runner.run_load_footnotes_allocation_to_output(
        spark,
        EntityID=entity_id,
        ClientID=client_id,
        TaxPeriodID=tax_period_id,
        RunID=run_id,
        CatalogName=catalog,
        SchemaName=schema,
        RankForRulePickup=rank_for_rule_pickup,
        **updated_kwargs,
    )
    wall = round(time.time() - started, 3)
    metrics = capture_metrics(spark, catalog, schema, run_id)
    summary = summarize_metrics(metrics)
    reported = result.get("elapsed_seconds") if isinstance(result, dict) else None
    timings = result.get("timings", []) if isinstance(result, dict) else []
    plan_profile = (
        result.get("plan_profile", []) if isinstance(result, dict) else []
    )
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s reported={reported} "
        f"output_rows={summary['allocation_output_rows']} "
        f"input_rows={summary['allocation_input_rows']}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "output_rows": summary["allocation_output_rows"],
        "input_rows": summary["allocation_input_rows"],
        "metrics": metrics,
        "timings": timings,
        "plan_profile": plan_profile,
    }


records = []
snapshot = create_snapshot(
    spark,
    catalog,
    schema,
    run_id,
    execution_id=f"{entity_id}_{uuid.uuid4().hex[:12]}",
)
print(
    "[benchmark safety] AllocationInput and footnote AllocationOutput are "
    "snapshotted. Do not run this RunID concurrently."
)

try:
    for pass_number in range(1, number_of_runs + 1):
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
            variant: _run_variant(variant, pass_number, snapshot)
            for variant in execution_order
        }
        records.extend(pass_records.values())

        mismatches = compare_variants(
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
        print(f"[reconcile] PASS {pass_number}: outputs and deductions match")
finally:
    # Drop backups only after restoration succeeds. If restoration fails, the
    # backup tables intentionally remain available for manual recovery.
    try:
        restore_original_state(spark, snapshot)
    except Exception:
        print(
            "[reconcile] RESTORE FAILED. Backup tables were retained; "
            f"input={snapshot['input_backup']} "
            f"output={snapshot['output_backup']}"
        )
        raise
    else:
        drop_snapshot(spark, snapshot)

# COMMAND ----------

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

summary_schema = StructType([
    StructField("pass", LongType(), True),
    StructField("variant", StringType(), True),
    StructField("wall_seconds", DoubleType(), True),
    StructField("reported_seconds", DoubleType(), True),
    StructField("allocation_output_rows", LongType(), True),
    StructField("allocation_input_rows", LongType(), True),
])
summary_rows = [
    (
        int(row["pass"]),
        row["variant"],
        float(row["wall_seconds"]),
        float(row["reported_seconds"]) if row["reported_seconds"] is not None else None,
        int(row["output_rows"]),
        int(row["input_rows"]),
    )
    for row in records
]
display(
    spark.createDataFrame(summary_rows, schema=summary_schema)
    .orderBy("pass", "variant")
)

timing_schema = StructType([
    StructField("step", StringType(), True),
    StructField("elapsed_seconds", DoubleType(), True),
])
for row in records:
    if row["variant"] == "updated" and row["timings"]:
        print(f"\nUpdated timings — pass {row['pass']}")
        timing_rows = [
            (str(item["step"]), float(item["elapsed_seconds"]))
            for item in row["timings"]
        ]
        display(
            spark.createDataFrame(timing_rows, schema=timing_schema)
            .orderBy("elapsed_seconds", ascending=False)
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan-size profile (only when "11. Plan profiler" = on)
# MAGIC Ranks builders by how much they grow the Spark logical plan (DAG),
# MAGIC including a `depth` column. Rendered via the shared
# MAGIC `AllocationV2.plan_profiler` package so every optimized SP notebook
# MAGIC shows the same table. Largest `delta` = best `checkpoint()` seam.

from AllocationV2.plan_profiler import build_plan_profile_display

for row in records:
    if row["variant"] == "updated" and row.get("plan_profile"):
        print(f"\nPlan profile — pass {row['pass']}")
        display(
            build_plan_profile_display(
                spark,
                row["plan_profile"],
                threshold=plan_checkpoint_threshold,
            )
        )
