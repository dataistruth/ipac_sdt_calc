# Databricks notebook source
# MAGIC %md
# MAGIC # Runner — `usp_load_allocation_input`
# MAGIC
# MAGIC Toggle **module_stem** only. Requires monolith `Source/` on `sys.path` (widget or auto-detect).

# COMMAND ----------

sp_name = "usp_load_allocation_input"

# COMMAND ----------

dbutils.widgets.dropdown(
    "module_stem",
    "updated.load_allocation_input",
    [
        "load_allocation_input",
        "updated.load_allocation_input",
        "updated.load_allocation_input_updated",
    ],
    "Module under output/ (updated package or shim)",
)
dbutils.widgets.text(
    "volume_path",
    "/Volumes/qa7/datavolume/databrickdata/checkpoint",
    "Checkpoint volume (uncompressed parquet)",
)
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks_msingh/Source",
    "Monolith Source/ (parent of AllocationV2/)",
)
dbutils.widgets.dropdown(
    "checkpoint_level",
    "default",
    ["minimal", "default", "full"],
    "Lineage-break checkpoints (updated module only)",
)

module_stem = dbutils.widgets.get("module_stem").strip()
volume_path = dbutils.widgets.get("volume_path").strip()
source_path = dbutils.widgets.get("source_path").strip()
checkpoint_level = dbutils.widgets.get("checkpoint_level").strip()

import os
import sys
import importlib
from datetime import datetime


def _source_from_notebook_path() -> str:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb_path = ctx.notebookPath().get()
        if "/Source/" in nb_path:
            return nb_path.split("/Source/")[0] + "/Source"
        marker = "/AllocationV2/"
        if marker in nb_path:
            return nb_path.split(marker)[0]
    except Exception:
        pass
    return ""


def ensure_source_on_path(path: str) -> str:
    path = (path or "").strip()
    if not path:
        path = _source_from_notebook_path()
    if not path:
        raise ValueError(
            "Set source_path to monolith Source/ (parent of AllocationV2/)."
        )
    alloc = os.path.join(path, "AllocationV2")
    if not os.path.isdir(alloc):
        raise FileNotFoundError(f"AllocationV2 not found at {alloc}")
    if path not in sys.path:
        sys.path.insert(0, path)
    print(f"[path] sys.path ← {path}")
    return path


source_path = ensure_source_on_path(source_path)

prefix = f"AllocationV2.{sp_name}.output"
for name in list(sys.modules):
    if name == prefix or name.startswith(prefix + "."):
        del sys.modules[name]

module_name = f"{prefix}.{module_stem}"
print(f"importing: {module_name}")
lt_runner = importlib.import_module(module_name)

# COMMAND ----------

beginning_time = datetime.now()
print(f"Beginning time: {beginning_time}")

result = lt_runner.run_load_allocation_input(
    spark,
    EntityID=115,
    ClientID=15348,
    TaxPeriodID=1,
    RunID=16560,
    CatalogName="QA7",
    SchemaName="IPC_2025_QA7_15348",
    VolumePath=volume_path,
    CheckpointLevel=checkpoint_level,
)

print(f"Elapsed: {datetime.now() - beginning_time}")
print(result)

# COMMAND ----------

if isinstance(result, dict) and result.get("timings"):
    import pandas as pd
    display(pd.DataFrame(result["timings"]).sort_values("elapsed_seconds", ascending=False))
