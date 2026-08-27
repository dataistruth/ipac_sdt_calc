# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark — `load_allocation_input` vs `load_allocation_input_updated`
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Notebook** | `benchmark_load_allocation_input` |
# MAGIC | **SP** | `AllocationV2/usp_load_allocation_input` |
# MAGIC | **Purpose** | A/B wall-clock benchmark: production vs `_updated` module |
# MAGIC | **Deploy** | `Source/AllocationV2/usp_load_allocation_input/output/updated/` (copy whole folder) |
# MAGIC
# MAGIC ## What each pass does
# MAGIC
# MAGIC For each pass (`1` … `number_of_run`):
# MAGIC 1. **`load_allocation_input`** — production (original)
# MAGIC 2. **`load_allocation_input_updated`** — volume checkpoints, parallel config/views/writes, uncompressed
# MAGIC
# MAGIC Records wall time, reported `elapsed_seconds`, and per-pass delta.
# MAGIC
# MAGIC ## Key widgets
# MAGIC
# MAGIC | Widget | Description |
# MAGIC |--------|-------------|
# MAGIC | `sp_name` | SP folder under `AllocationV2/` |
# MAGIC | `number_of_run` | A/B passes (original → updated each pass) |
# MAGIC | `parallel_workers` | Config + flow-up writes (`_updated` only); default `3` |
# MAGIC | `volume_path` | UC volume for checkpoints (`_updated` only) |
# MAGIC | `source_path` | Monolith `Source/` on `sys.path` |
# MAGIC | Run params | `EntityID`, `ClientID`, `TaxPeriodID`, `RunID`, `CatalogName`, `SchemaName` |
# MAGIC
# MAGIC **Note:** Both implementations write to the same `RunID` — use a test run or accept overwrite.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text(
    "sp_name",
    "usp_load_allocation_input",
    "SP folder under AllocationV2/",
)
dbutils.widgets.text(
    "number_of_run",
    "1",
    "Number of A/B passes (original then updated each pass)",
)
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks_msingh/Source",
    "Monolith Source/ on sys.path",
)
dbutils.widgets.text(
    "volume_path",
    "/Volumes/qa7/datavolume/databrickdata/checkpoint",
    "VolumePath for _updated (checkpoints + storer)",
)
dbutils.widgets.text("EntityID", "115", "EntityID")
dbutils.widgets.text("ClientID", "15348", "ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "TaxPeriodID")
dbutils.widgets.text("RunID", "16560", "RunID")
dbutils.widgets.text("CatalogName", "QA7", "CatalogName")
dbutils.widgets.text("SchemaName", "IPC_2025_QA7_15348", "SchemaName")
dbutils.widgets.text(
    "parallel_workers",
    "3",
    "Parallel workers (updated: config + flow-up writes)",
)

sp_name = dbutils.widgets.get("sp_name").strip()
number_of_run = int(dbutils.widgets.get("number_of_run").strip() or "1")
source_path = dbutils.widgets.get("source_path").strip()
volume_path = dbutils.widgets.get("volume_path").strip()
entity_id = int(dbutils.widgets.get("EntityID").strip())
client_id = int(dbutils.widgets.get("ClientID").strip())
tax_period_id = int(dbutils.widgets.get("TaxPeriodID").strip())
run_id = int(dbutils.widgets.get("RunID").strip())
catalog_name = dbutils.widgets.get("CatalogName").strip()
schema_name = dbutils.widgets.get("SchemaName").strip()
parallel_workers = int(dbutils.widgets.get("parallel_workers").strip() or "3")

if parallel_workers < 1:
    raise ValueError("parallel_workers must be >= 1")

if number_of_run < 1:
    raise ValueError("number_of_run must be >= 1")
if not sp_name:
    raise ValueError("sp_name is required")

print(f"sp_name         : {sp_name}")
print(f"number_of_run   : {number_of_run}")
print(f"source_path     : {source_path}")
print(f"volume_path     : {volume_path}")
print(f"EntityID        : {entity_id}")
print(f"ClientID        : {client_id}")
print(f"TaxPeriodID     : {tax_period_id}")
print(f"RunID           : {run_id}")
print(f"CatalogName     : {catalog_name}")
print(f"SchemaName      : {schema_name}")
print(f"parallel_workers: {parallel_workers} (updated only)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark tuning

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

print("Spark adaptive + Delta optimizeWrite enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

import importlib
import json
import sys
import time
from datetime import datetime
from typing import Any

if source_path and source_path not in sys.path:
    sys.path.insert(0, source_path)

MODULE_ORIGINAL = "load_allocation_input"
MODULE_UPDATED = "load_allocation_input_updated"
OUTPUT_PREFIX = f"AllocationV2.{sp_name}.output"


def _clear_output_modules() -> None:
    for name in list(sys.modules):
        if name == OUTPUT_PREFIX or name.startswith(OUTPUT_PREFIX + "."):
            del sys.modules[name]


def _import_module(module_stem: str):
    _clear_output_modules()
    module_name = f"{OUTPUT_PREFIX}.{module_stem}"
    print(f"importing: {module_name}")
    return importlib.import_module(module_name)


def _extract_elapsed(result: Any) -> float | None:
    if isinstance(result, dict):
        val = result.get("elapsed_seconds")
        if val is not None:
            return float(val)
    if isinstance(result, str) and result.strip().startswith("{"):
        try:
            parsed = json.loads(result)
            val = parsed.get("elapsed_seconds")
            if val is not None:
                return float(val)
        except json.JSONDecodeError:
            pass
    return None


def _run_pipeline(runner, variant: str, pass_num: int) -> dict:
    run_kwargs = {
        "EntityID": entity_id,
        "ClientID": client_id,
        "TaxPeriodID": tax_period_id,
        "RunID": run_id,
        "CatalogName": catalog_name,
        "SchemaName": schema_name,
    }
    if variant == "updated":
        run_kwargs["VolumePath"] = volume_path
        run_kwargs["parallel_config_workers"] = parallel_workers
        run_kwargs["parallel_write_workers"] = parallel_workers

    started_at = datetime.now()
    t0 = time.time()
    worker_note = (
        f" | workers={parallel_workers}" if variant == "updated" else ""
    )
    print(f"\n=== pass {pass_num} | {variant}{worker_note} | start {started_at} ===")

    try:
        result = runner.run_load_allocation_input(spark, **run_kwargs)
        status = "SUCCESS"
        error = None
    except Exception as exc:
        result = None
        status = "FAIL"
        error = str(exc)
        print(f"ERROR ({variant}): {exc}")

    wall_seconds = round(time.time() - t0, 3)
    reported = _extract_elapsed(result)
    ended_at = datetime.now()

    row = {
        "pass": pass_num,
        "variant": variant,
        "module": MODULE_ORIGINAL if variant == "original" else MODULE_UPDATED,
        "parallel_workers": parallel_workers if variant == "updated" else None,
        "status": status,
        "wall_seconds": wall_seconds,
        "reported_elapsed_seconds": reported,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "error": error,
    }
    print(
        f"=== pass {pass_num} | {variant} | wall={wall_seconds}s "
        f"| reported={reported} | end {ended_at} ==="
    )
    return row

# COMMAND ----------

# MAGIC %md
# MAGIC ## A/B benchmark loop
# MAGIC
# MAGIC Each pass: **original** → **updated**

# COMMAND ----------

benchmark_rows: list[dict] = []

for pass_num in range(1, number_of_run + 1):
    print(f"\n{'#' * 60}")
    print(f"# BENCHMARK PASS {pass_num} / {number_of_run}")
    print(f"{'#' * 60}")

    original_runner = _import_module(MODULE_ORIGINAL)
    benchmark_rows.append(_run_pipeline(original_runner, "original", pass_num))

    updated_runner = _import_module(MODULE_UPDATED)
    benchmark_rows.append(_run_pipeline(updated_runner, "updated", pass_num))

print(f"\nCompleted {number_of_run} pass(es) — {len(benchmark_rows)} runs recorded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results table

# COMMAND ----------

import pandas as pd

results_df = pd.DataFrame(benchmark_rows)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary (wall time)

# COMMAND ----------

summary = (
    results_df.groupby("variant", as_index=False)
    .agg(
        runs=("wall_seconds", "count"),
        wall_seconds_min=("wall_seconds", "min"),
        wall_seconds_mean=("wall_seconds", "mean"),
        wall_seconds_max=("wall_seconds", "max"),
        wall_seconds_total=("wall_seconds", "sum"),
    )
    .sort_values("variant")
)
summary["wall_seconds_mean"] = summary["wall_seconds_mean"].round(3)
summary["wall_seconds_total"] = summary["wall_seconds_total"].round(3)

display(summary)

if len(summary) == 2:
    orig_mean = summary.loc[summary["variant"] == "original", "wall_seconds_mean"].iloc[0]
    upd_mean = summary.loc[summary["variant"] == "updated", "wall_seconds_mean"].iloc[0]
    delta = round(upd_mean - orig_mean, 3)
    pct = round((delta / orig_mean) * 100, 1) if orig_mean else None
    print(
        f"Mean wall time — original: {orig_mean}s | updated: {upd_mean}s | "
        f"delta: {delta}s ({pct}% vs original)"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-pass comparison

# COMMAND ----------

pass_compare = results_df.pivot_table(
    index="pass",
    columns="variant",
    values="wall_seconds",
    aggfunc="first",
)
if "original" in pass_compare.columns and "updated" in pass_compare.columns:
    pass_compare["delta_seconds"] = pass_compare["updated"] - pass_compare["original"]
    pass_compare["delta_pct"] = (
        (pass_compare["delta_seconds"] / pass_compare["original"]) * 100
    ).round(1)

display(pass_compare.round(3))
