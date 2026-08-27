# Databricks notebook source
# MAGIC %md
# MAGIC # Load Allocation Input (single notebook)
# MAGIC
# MAGIC Runs **`dbo.uspLoadAllocationInput`** logic in **one Spark session** — no multi-task job context switching.
# MAGIC
# MAGIC **Deploy:** place this notebook beside the SP `source/` folder (monolith or `ipac-sdt-calc`).
# MAGIC **Run:** set widgets → Run All.
# MAGIC
# MAGIC Writes `AllocationInput` (+ PFIC / form flow-ups) for the given `RunID`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("entity_id", "", "EntityID")
dbutils.widgets.text("client_id", "", "ClientID")
dbutils.widgets.text("tax_period_id", "", "TaxPeriodID")
dbutils.widgets.text("run_id", "", "RunID")
dbutils.widgets.text("catalog", "qa7", "Catalog (UC)")
dbutils.widgets.text("schema", "IPC_2025_DEV8_MayBuild", "Schema / database name")
dbutils.widgets.text("volume_path", "", "Volume path (optional)")
dbutils.widgets.text("execution_id", "1", "ExecutionID")
dbutils.widgets.dropdown("result_type", "deltalake", ["deltalake", "parquet"], "Result type")
dbutils.widgets.text("call_from", "", "CallFrom (optional)")

entity_id = int(dbutils.widgets.get("entity_id").strip() or "0")
client_id = int(dbutils.widgets.get("client_id").strip() or "0")
tax_period_id = int(dbutils.widgets.get("tax_period_id").strip() or "0")
run_id = int(dbutils.widgets.get("run_id").strip() or "0")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
volume_path = dbutils.widgets.get("volume_path").strip()
execution_id = dbutils.widgets.get("execution_id").strip() or "1"
result_type = dbutils.widgets.get("result_type").strip() or "deltalake"
call_from = dbutils.widgets.get("call_from").strip() or None

if not all([entity_id, client_id, tax_period_id, run_id, catalog, schema]):
    raise ValueError(
        "entity_id, client_id, tax_period_id, run_id, catalog, and schema are required"
    )

print(f"entity_id      : {entity_id}")
print(f"client_id      : {client_id}")
print(f"tax_period_id  : {tax_period_id}")
print(f"run_id         : {run_id}")
print(f"catalog        : {catalog}")
print(f"schema         : {schema}")
print(f"volume_path    : {volume_path or '(none)'}")
print(f"execution_id   : {execution_id}")
print(f"result_type    : {result_type}")
print(f"call_from      : {call_from or '(none)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark tuning (optional — safe defaults for long DAG)

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

print("Spark adaptive + Delta optimizeWrite enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap imports (monolith `Source/` or `ipac-sdt-calc`)

# COMMAND ----------

import os
import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(f"notebook_path  : {nb_path}")

workspace_nb = "/Workspace" + nb_path if not nb_path.startswith("/Workspace") else nb_path
nb_dir = os.path.dirname(workspace_nb)

# SP source directory: ../source relative to notebooks/
sp_source = os.path.normpath(os.path.join(nb_dir, "..", "source"))
if os.path.isdir(sp_source) and sp_source not in sys.path:
    sys.path.insert(0, sp_source)
    print(f"sys.path + SP source: {sp_source}")

# Monolith: .../Source/AllocationV2/.../notebooks → repo/Source
if "/Source/" in workspace_nb:
    source_root = workspace_nb.split("/Source/", 1)[0] + "/Source"
    if os.path.isdir(source_root) and source_root not in sys.path:
        sys.path.insert(0, source_root)
        print(f"sys.path + Source: {source_root}")

# ipac-sdt-calc: .../domains/.../notebooks → repo root + platform/common as Common_V2
if "/domains/" in workspace_nb:
    repo_root = workspace_nb.split("/domains/", 1)[0]
    platform_common = os.path.join(repo_root, "platform", "common")
    if os.path.isdir(platform_common):
        # Common_V2 imports resolve when Source is on path; map platform/common for dev
        if platform_common not in sys.path:
            sys.path.insert(0, platform_common)
            print(f"sys.path + platform/common: {platform_common}")

from load_allocation_input import run_load_allocation_input

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run pipeline (single context)

# COMMAND ----------

result = run_load_allocation_input(
    spark,
    entity_id=entity_id,
    client_id=client_id,
    tax_period_id=tax_period_id,
    run_id=run_id,
    catalog=catalog,
    schema=schema,
    volume_path=volume_path,
    execution_id=execution_id,
    result_type=result_type,
    call_from=call_from,
)

print("--- result ---")
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step timings

# COMMAND ----------

import json

if isinstance(result, str):
    try:
        parsed = json.loads(result)
    except json.JSONDecodeError:
        parsed = {"raw": result}
else:
    parsed = result

timings = parsed.get("timings") if isinstance(parsed, dict) else None
if timings:
    import pandas as pd

    df = pd.DataFrame(timings)
    if "elapsed_seconds" in df.columns:
        df = df.sort_values("elapsed_seconds", ascending=False)
    display(df)
    total = sum(t.get("elapsed_seconds", 0) for t in timings)
    print(f"Tracked step total: {total:.3f}s")
    if isinstance(parsed, dict) and parsed.get("elapsed_seconds"):
        print(f"Wall clock: {parsed['elapsed_seconds']}s")
else:
    print("No per-step timings in result (add StepTimer to load_allocation_input.py)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify AllocationInput row count (this RunID)

# COMMAND ----------

fqn = f"`{catalog}`.`{schema}`.`AllocationInput`"
count_sql = f"SELECT COUNT(*) AS cnt FROM {fqn} WHERE RunID = {run_id}"
spark.sql(count_sql).show()
