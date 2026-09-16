# Databricks notebook source
# MAGIC %md
# MAGIC # A/B benchmark — usp_add_lookthrough_alloc_detail_step01
# MAGIC
# MAGIC Compares the unchanged production module with `output.updated`.
# MAGIC The SP only *appends* rows (keyed by RunID) to the lookthrough detail
# MAGIC tables, so each variant purges this RunID first and we compare per-table
# MAGIC row counts / Amount sums for parity. Both variants write Delta so the
# MAGIC comparison reads catalog tables.
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
dbutils.widgets.text("ParallelWorkers", "4", "9. Updated parallel workers")
dbutils.widgets.dropdown("ProfilePlan", "on", ["off", "on"], "10. Plan profiler")
dbutils.widgets.text(
    "PlanCheckpointThreshold", "30", "11. Plan checkpoint threshold"
)
dbutils.widgets.dropdown(
    "CheckpointBackend", "local", ["delta", "local"], "12. Checkpoint backend"
)
dbutils.widgets.text(
    "SqlShufflePartitions", "4", "13. spark.sql.shuffle.partitions (blank=default)"
)
dbutils.widgets.text(
    "LocalDeltaDenylist", "", "14. Local backend delta-denylist (comma-sep)"
)
dbutils.widgets.text(
    "CheckpointCoalesce", "2", "15. Checkpoint write coalesce (blank=off)"
)

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs").strip() or "1")
entity_id = int(dbutils.widgets.get("EntityID").strip())
client_id = int(dbutils.widgets.get("ClientID").strip())
tax_period_id = int(dbutils.widgets.get("TaxPeriodID").strip())
run_id = int(dbutils.widgets.get("RunID").strip())
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
parallel_workers = int(dbutils.widgets.get("ParallelWorkers").strip() or "4")
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
plan_checkpoint_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold").strip() or "30"
)
# Checkpoint backend: "local" (df.localCheckpoint, no metastore commit -- the
# fast default) or "delta" (durable UC Delta temp table). This SP's only seam
# (base_lt_out) is a plain scan+filter, so "local" is safe. If a local run ever
# crashes with UNRESOLVED_COLUMN at a checkpoint, add that name to widget 14.
checkpoint_backend = dbutils.widgets.get("CheckpointBackend").strip().lower()
local_delta_denylist = dbutils.widgets.get("LocalDeltaDenylist").strip()
# Session-wide shuffle-partition cap (blank = leave cluster/AQE default). 200
# fans small joins into many tiny tasks/files; 4 suits this dataset.
sql_shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()
# Coalesce each Delta checkpoint write to this many files (blank/0 = off). In
# backend="local" it only affects any forced-delta seams (widget 14).
checkpoint_coalesce = dbutils.widgets.get("CheckpointCoalesce").strip()

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

PACKAGE = "AllocationV2.usp_add_lookthrough_alloc_detail_step01"
ORIGINAL_MODULE = f"{PACKAGE}.output.add_lookthrough_allocation_detail_step01"
UPDATED_MODULE = f"{PACKAGE}.output.updated.orchestrator"
RECONCILE_MODULE = f"{PACKAGE}.output.updated.output_reconcile"
ENTRY = "run_add_lookthrough_allocation_detail_step01"


def _assert_updated_package_synced() -> None:
    updated_dir = os.path.join(
        source_path, *PACKAGE.split("."), "output", "updated"
    )
    required = [
        "__init__.py",
        "checkpoint.py",
        "orchestrator.py",
        "output_reconcile.py",
        "plan_profiler.py",
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

# Bind reconcile helpers before fresh imports clear the package cache.
capture_metrics = reconcile.capture_output_metrics
summarize_metrics = reconcile.summarize_metrics
compare_variants = reconcile.compare_variants
purge_output = reconcile.purge_output_partitions_for_run
format_mismatch_lines = reconcile.format_mismatch_lines

# COMMAND ----------


def _run_variant(variant: str, pass_number: int) -> dict:
    # Fair A/B: clear this RunID from every output table before the variant.
    purged = purge_output(spark, catalog, schema, run_id)
    print(f"[reconcile] pre-run purge RunID={run_id}: {len(purged)} table(s)")

    module_name = ORIGINAL_MODULE if variant == "original" else UPDATED_MODULE
    runner = _import_fresh(module_name)
    entry = getattr(runner, ENTRY)

    print(
        f"\n{'=' * 72}\n"
        f"PASS {pass_number} | {variant.upper()} | {datetime.now().isoformat()}\n"
        f"{'=' * 72}"
    )
    # Both variants write Delta so outputs land in catalog tables we can read.
    common_kwargs = {
        "ResultType": "deltalake",
        "ExecutionID": f"{entity_id}_{uuid.uuid4().hex[:12]}",
    }
    updated_kwargs = {}
    if variant == "updated":
        updated_kwargs["parallel_workers"] = parallel_workers
        if profile_plan:
            updated_kwargs["profile_plan"] = True
            updated_kwargs["plan_checkpoint_threshold"] = plan_checkpoint_threshold
        updated_kwargs["checkpoint_backend"] = checkpoint_backend
        if checkpoint_backend == "local" and local_delta_denylist:
            updated_kwargs["local_delta_denylist"] = local_delta_denylist
        if checkpoint_coalesce:
            updated_kwargs["checkpoint_coalesce"] = checkpoint_coalesce

    started = time.time()
    result = entry(
        spark,
        EntityID=entity_id,
        ClientID=client_id,
        TaxPeriodID=tax_period_id,
        RunID=run_id,
        CatalogName=catalog,
        SchemaName=schema,
        **common_kwargs,
        **updated_kwargs,
    )
    wall = round(time.time() - started, 3)
    metrics = capture_metrics(
        spark, catalog, schema, run_id, client_id, tax_period_id
    )
    summary = summarize_metrics(metrics)
    reported = result.get("elapsed_seconds") if isinstance(result, dict) else None
    timings = result.get("timings", []) if isinstance(result, dict) else []
    plan_profile = (
        result.get("plan_profile", []) if isinstance(result, dict) else []
    )
    plan_details = (
        result.get("plan_details", []) if isinstance(result, dict) else []
    )
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s reported={reported} "
        f"tables_with_rows={summary['tables_with_rows']} "
        f"total_rows={summary['total_rows']}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "tables_with_rows": summary["tables_with_rows"],
        "total_rows": summary["total_rows"],
        "metrics": metrics,
        "timings": timings,
        "plan_profile": plan_profile,
        "plan_details": plan_details,
    }


records = []
print(
    "[benchmark safety] lookthrough detail tables are purged by RunID before "
    "each variant. Do not run this RunID concurrently."
)

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
        variant: _run_variant(variant, pass_number)
        for variant in execution_order
    }
    records.extend(pass_records.values())

    compare_rows = compare_variants(
        pass_records["original"]["metrics"],
        pass_records["updated"]["metrics"],
    )
    mismatches = [r for r in compare_rows if not r.get("parity_ok")]
    if mismatches:
        print(f"[reconcile] FAIL pass {pass_number}")
        for line in format_mismatch_lines(compare_rows):
            print(line)
        raise AssertionError(
            f"Output parity failed with {len(mismatches)} mismatch(es)"
        )
    print(f"[reconcile] PASS {pass_number}: all output tables match")

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
    StructField("tables_with_rows", LongType(), True),
    StructField("total_rows", LongType(), True),
])
summary_rows = [
    (
        int(row["pass"]),
        row["variant"],
        float(row["wall_seconds"]),
        float(row["reported_seconds"]) if row["reported_seconds"] is not None else None,
        int(row["tables_with_rows"]),
        int(row["total_rows"]),
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
# MAGIC ## Plan-size profile (only when "10. Plan profiler" = on)
# MAGIC Ranks sections by how much they grow the Spark logical plan (DAG),
# MAGIC including a `depth` column, vs the checkpointed `base_lt_out` baseline.
# MAGIC Rendered via the shared `AllocationV2.plan_profiler` package so every
# MAGIC optimized SP notebook shows the same table. Largest `delta` = best
# MAGIC future `checkpoint()` seam.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan explain + operations — printed (per checkpoint & section)
# MAGIC Text output (copy/paste-able, shows in job logs) with the ranked
# MAGIC node/depth/ops summary **and** the full `.explain()` optimized-plan tree
# MAGIC for the `base_lt_out` checkpoint and every section — mirroring what the
# MAGIC prior SPs printed at each checkpoint.

from AllocationV2.plan_profiler import plan_profile_report

for row in records:
    if row["variant"] != "updated" or not row.get("plan_details"):
        continue
    print("\n" + "=" * 78)
    print(f"PLAN DETAIL — pass {row['pass']} (updated)")
    print("=" * 78)
    # Ranked summary: func | nodes | depth | (+delta) | ops  (+ checkpoint flag).
    plan_profile_report(row["plan_profile"], threshold=plan_checkpoint_threshold)
    # Full optimized-plan tree per checkpoint / section.
    for d in row["plan_details"]:
        ops = " ".join(f"{k}={v}" for k, v in sorted(d.get("ops", {}).items()))
        print(
            f"\n{'-' * 78}\n"
            f"{d['name']}: nodes={d['nodes']} depth={d['depth']} "
            f"(+{d['delta']}){('  ' + ops) if ops else ''}\n"
            f"{'-' * 78}"
        )
        print(d.get("explain") or "(no plan captured)")
