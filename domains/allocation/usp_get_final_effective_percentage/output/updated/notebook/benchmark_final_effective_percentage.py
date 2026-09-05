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
# MAGIC
# MAGIC ## Performance-tuning widgets (updated variant)
# MAGIC
# MAGIC These were added while investigating the updated pipeline's runtime.
# MAGIC Findings from profiling + the driver log4j output:
# MAGIC - **`build_cost_percentage_by_type` (~22s)** and
# MAGIC   **`compute_effective_percentage_dated` (~15s)** are dominated by
# MAGIC   **Delta checkpoint I/O**, not joins/compute (~40-50s of wall is
# MAGIC   `saveAsTable` commits). Joins are already broadcast-optimized.
# MAGIC - The output table `sm_finaleffectivepercentages` had **6661 files for
# MAGIC   ~35 MB** (8724 Delta versions) — a small-file/commit-bloat problem.
# MAGIC   Run `OPTIMIZE` + `VACUUM` on it before benchmarking.
# MAGIC
# MAGIC Knobs:
# MAGIC - **12. Checkpoint profile** — `full` keeps every prod seam (incl. the
# MAGIC   non-dated seam); avoid `conservative` (it bypasses `nde_post_miss_fused`
# MAGIC   and replays a huge plan into the non-dated stage).
# MAGIC - **15. Extra checkpoint builders** — force extra post-builder lineage
# MAGIC   breaks; A/B only. Profiling showed prod already checkpoints every deep
# MAGIC   seam, so adding more usually only adds cost.
# MAGIC - **16. Checkpoint backend** — `delta` or `local`. `local`
# MAGIC   (localCheckpoint) skips the metastore commit / small-file I/O and is the
# MAGIC   highest-upside lever (validated: 166.7s -> 104.1s, ~38% faster,
# MAGIC   fingerprints match). It runs as a HYBRID: safe seams use localCheckpoint,
# MAGIC   but the MINIMAL validated denylist `nde_pre_cpbt` / `de_pre_cpbt` (the
# MAGIC   compute_missing_entities self-join feeders) is forced back to `delta`
# MAGIC   because localCheckpoint's LogicalRDD can't be re-resolved for a self-join
# MAGIC   (-> UNRESOLVED_COLUMN). If a new mode/data shape crashes at some other
# MAGIC   checkpoint, add that seam via widget 19.
# MAGIC - **19. Local backend delta-denylist** — comma-separated checkpoint-name
# MAGIC   prefixes that stay on `delta` even when backend=`local` (their result
# MAGIC   feeds a self-join localCheckpoint can't re-resolve). Managed here and
# MAGIC   passed through; pre-filled with `nde_pre_cpbt,de_pre_cpbt`. If a run
# MAGIC   crashes at the effective_calc + plugging self-join (`DealID`), add
# MAGIC   `final_cost_pct` (then `nde_post_miss,de_post_miss` if still unstable).
# MAGIC   Only consulted when backend=`local`.
# MAGIC - **20. Local denylist mode** — `extend` (add widget-19 to the built-ins,
# MAGIC   default) or `replace` (use ONLY widget 19). The built-in default is now
# MAGIC   the minimal validated pair `nde_pre_cpbt,de_pre_cpbt`; a 2026-09-05 run
# MAGIC   proved `nde_post_miss_fused` / `de_post_miss_fused` / `final_cost_pct_fused`
# MAGIC   run safely on local (reclaimed ~8s), so they are no longer force-delta'd.
# MAGIC   Use `replace` only for further experiments; a blank widget-19 in
# MAGIC   `replace` mode safely falls back to the built-in denylist.
# MAGIC - **21. Checkpoint write coalesce** — coalesce each Delta checkpoint
# MAGIC   write to N files (blank/0 = off). The write still emits several tiny
# MAGIC   files per commit on this dataset; a small N (e.g. `2`) trims file-count
# MAGIC   / commit overhead. In backend=`local` this hits exactly the remaining
# MAGIC   forced seams (`nde_pre_cpbt` / `de_pre_cpbt`), the last durable I/O
# MAGIC   in the fast path. `coalesce` is a narrow op (no shuffle). Applied to
# MAGIC   both the delta and parquet-volume write paths.
# MAGIC - **22. Parquet checkpoint volume path** — when set AND backend=`local`,
# MAGIC   the forced self-join seams (widget 19) round-trip through plain Parquet
# MAGIC   under this volume path instead of Delta: `spark.read.parquet` gives a
# MAGIC   fresh, re-resolvable relation (self-join safe) but skips Delta's
# MAGIC   commit/metastore step, so it's faster than the forced-delta fallback.
# MAGIC   Blank = off (those seams fall back to Delta). Widget 21's coalesce is
# MAGIC   applied to the parquet write; temp dirs are run-scoped + auto-cleaned.
# MAGIC - **17. spark.sql.shuffle.partitions** — the 200 default fans small joins
# MAGIC   into ~32 near-empty tasks/files on this dataset; try `4`. Applied to
# MAGIC   both variants for a fair A/B.
# MAGIC - **18. Delta optimizeWrite + autoCompact** — coalesces small files on
# MAGIC   write (output + checkpoint temp tables); attacks the 6661-file problem
# MAGIC   at the source. Applied to both variants.

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
    "full",
    ["full", "conservative", "balanced"],
    "12. Checkpoint profile",
)
dbutils.widgets.dropdown(
    "ProfilePlan",
    "on",
    ["off", "on"],
    "13. Plan profiler",
)
dbutils.widgets.text(
    "PlanCheckpointThreshold",
    "30",
    "14. Plan checkpoint threshold",
)
dbutils.widgets.text(
    "ExtraCheckpointBuilders",
    "",
    "15. Extra checkpoint builders (comma-sep)",
)
dbutils.widgets.dropdown(
    "CheckpointBackend",
    "local",
    ["delta", "local"],
    "16. Checkpoint backend",
)
dbutils.widgets.text(
    "SqlShufflePartitions",
    "4",
    "17. spark.sql.shuffle.partitions (blank=default)",
)
dbutils.widgets.dropdown(
    "DeltaOptimizeWrite",
    "off",
    ["off", "on"],
    "18. Delta optimizeWrite + autoCompact",
)
dbutils.widgets.text(
    "LocalDeltaDenylist",
    "nde_pre_cpbt,de_pre_cpbt",
    "19. Local backend delta-denylist (comma-sep)",
)
dbutils.widgets.dropdown(
    "LocalDeltaDenylistMode",
    "extend",
    ["extend", "replace"],
    "20. Local denylist mode",
)
dbutils.widgets.text(
    "CheckpointCoalesce",
    "2",
    "21. Checkpoint write coalesce (blank=off)",
)
dbutils.widgets.text(
    "CheckpointVolumePath",
    "/Volumes/qa7/datavolume/databrickdata/checkpoint",
    "22. Parquet checkpoint volume path (blank=off)",
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
# A/B lever: extra post-builder checkpoints beyond prod's built-in seams.
# Comma-separated builder names, e.g.
# "build_cost_percentage_snapshot_modes123,build_input_lines". Empty = none.
extra_checkpoint_builders = dbutils.widgets.get(
    "ExtraCheckpointBuilders"
).strip()
# Checkpoint backend: "delta" (durable, current) or "local" (localCheckpoint,
# no metastore commit). Profiling showed ~40-50s of wall is Delta checkpoint
# I/O; "local" is the A/B lever to cut it.
checkpoint_backend = dbutils.widgets.get("CheckpointBackend").strip().lower()
# The delta-denylist is managed HERE (widget 19), comma-separated: these
# checkpoint-name prefixes stay on Delta even when backend="local", because
# their result feeds a self-join that localCheckpoint can't re-resolve
# (UNRESOLVED_COLUMN). Pre-filled with the confirmed-critical pair
# "nde_pre_cpbt,de_pre_cpbt". If a run crashes at the effective_calc + plugging
# self-join (DealID), add "final_cost_pct" (and, if still unstable,
# "nde_post_miss,de_post_miss") right here -- no redeploy. Only used when
# checkpoint_backend == "local".
local_delta_denylist = dbutils.widgets.get("LocalDeltaDenylist").strip()
# "extend" (add to built-ins) or "replace" (use only widget 19). Use "replace"
# with widget 19 = "nde_pre_cpbt,de_pre_cpbt" to trim the preemptive seams and
# measure whether they can safely run local.
local_delta_denylist_mode = (
    dbutils.widgets.get("LocalDeltaDenylistMode").strip().lower()
)
# Coalesce each Delta checkpoint write to this many files (blank/0 = off). On
# this small dataset the write still emits several tiny files per commit; a
# small value (e.g. 2) trims file-count/commit overhead. In backend="local" it
# hits exactly the forced-delta self-join seams (nde_pre_cpbt / de_pre_cpbt).
checkpoint_coalesce = dbutils.widgets.get("CheckpointCoalesce").strip()
# Parquet-backend base path (widget 22). When set AND backend="local", the
# self-join denylist seams (widget 19) are written as plain Parquet under this
# volume path and read back -- a fresh, re-resolvable relation (self-join safe)
# that skips Delta's commit/metastore step, so it's faster than the forced-delta
# fallback. Blank = disable (denylist seams fall back to Delta). The same
# coalesce (widget 21) is applied to the parquet write. Temp dirs are
# run-scoped and cleaned up best-effort at the end of the run.
checkpoint_volume_path = dbutils.widgets.get("CheckpointVolumePath").strip()
# Session-wide shuffle-partition cap. The 200 default fans small joins into
# many tiny tasks/files, inflating every Delta checkpoint write on this small
# dataset. A small value (e.g. 4) cuts that overhead. Blank = leave the
# cluster/AQE default. Applied to BOTH variants so the A/B stays fair; change
# it across runs to measure the effect of 200 vs 4.
sql_shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()
if sql_shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", sql_shuffle_partitions)
    print(
        "[config] spark.sql.shuffle.partitions = "
        f"{spark.conf.get('spark.sql.shuffle.partitions')}"
    )
# Delta optimize-write: coalesce small files on write (output table AND every
# checkpoint temp table). The driver log showed the output table at 6661 files
# for ~35 MB -- this fights that at the source. Applied to BOTH variants.
delta_optimize_write = (
    dbutils.widgets.get("DeltaOptimizeWrite").strip().lower() == "on"
)
if delta_optimize_write:
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    print("[config] delta optimizeWrite + autoCompact enabled")

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
        run_kwargs["CheckpointBackend"] = checkpoint_backend
        # Hybrid: tune which prefixes stay on Delta when backend="local".
        if checkpoint_backend == "local":
            if local_delta_denylist:
                run_kwargs["LocalDeltaDenylist"] = local_delta_denylist
            run_kwargs["LocalDeltaDenylistMode"] = local_delta_denylist_mode
        # Coalesce the checkpoint writes (applies to both backends; in
        # "local" it hits only the forced self-join seams -- delta or parquet).
        if checkpoint_coalesce:
            run_kwargs["CheckpointCoalesce"] = checkpoint_coalesce
        # Parquet-on-volume backend for the forced self-join seams. When set,
        # those seams round-trip through Parquet on this path instead of Delta.
        if checkpoint_volume_path:
            run_kwargs["CheckpointVolumePath"] = checkpoint_volume_path
        # Plan-size profiler is driven by the "13. Plan profiler" widget, not
        # by cfg. When on, the updated runner measures per-builder logical-plan
        # (DAG) growth and returns a ranked report in result["plan_profile"].
        if profile_plan:
            run_kwargs["profile_plan"] = True
            run_kwargs["plan_checkpoint_threshold"] = plan_checkpoint_threshold
        # A/B: force extra lineage breaks on the named builders' outputs.
        if extra_checkpoint_builders:
            run_kwargs["extra_checkpoint_builders"] = extra_checkpoint_builders

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
# MAGIC Rendered via the shared `AllocationV2.plan_profiler` package so every
# MAGIC optimized SP notebook shows the same columns (incl. `depth`).

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
