# Databricks notebook source
# MAGIC %md
# MAGIC # Runner — `usp_load_allocation_input`
# MAGIC
# MAGIC Toggle **module_stem** only. Same `output/` folder → all `ai_*` imports unchanged.

# COMMAND ----------

sp_name = "usp_load_allocation_input"

# COMMAND ----------

dbutils.widgets.dropdown(
    "module_stem",
    "load_allocation_input_updated",
    ["load_allocation_input", "load_allocation_input_updated"],
    "File in output/ (Change 2)",
)
dbutils.widgets.text(
    "volume_path",
    "/Volumes/qa7/datavolume/databrickdata/checkpoint",
    "Checkpoint volume (uncompressed parquet)",
)

module_stem = dbutils.widgets.get("module_stem").strip()
volume_path = dbutils.widgets.get("volume_path").strip()
source_path = "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks_msingh/Source"

import sys
import importlib
from datetime import datetime

if source_path not in sys.path:
    sys.path.insert(0, source_path)

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
)

print(f"Elapsed: {datetime.now() - beginning_time}")
print(result)

# COMMAND ----------

if isinstance(result, dict) and result.get("timings"):
    import pandas as pd
    display(pd.DataFrame(result["timings"]).sort_values("elapsed_seconds", ascending=False))
