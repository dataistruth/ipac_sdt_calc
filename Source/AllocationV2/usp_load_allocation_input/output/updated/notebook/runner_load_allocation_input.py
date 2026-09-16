# Databricks notebook source
# MAGIC %md
# MAGIC # Updated runner — `usp_load_allocation_input`

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Monolith Source/",
)
dbutils.widgets.text("EntityID", "115", "2. EntityID")
dbutils.widgets.text("ClientID", "15348", "3. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "4. TaxPeriodID")
dbutils.widgets.text("RunID", "16560", "5. RunID")
dbutils.widgets.text("CatalogName", "QA7", "6. Catalog")
dbutils.widgets.text("SchemaName", "IPC_2025_QA7_15348", "7. Schema")
dbutils.widgets.text(
    "VolumePath",
    "/Volumes/qa7/datavolume/databrickdata/checkpoint",
    "8. Volume path",
)
dbutils.widgets.text("MaxThreads", "4", "9. Max threads")
dbutils.widgets.dropdown(
    "ProfilePlan", "on", ["off", "on"], "10. Plan profiler"
)
dbutils.widgets.text("PlanCheckpointThreshold", "30", "11. Plan threshold")
dbutils.widgets.dropdown(
    "CheckpointBackend",
    "delta",
    ["delta", "local"],
    "12. Checkpoint backend",
)

# COMMAND ----------

import importlib
import os
import sys
import time

source_path = dbutils.widgets.get("source_path").strip()
if not os.path.isdir(source_path):
    raise RuntimeError(f"Source path does not exist: {source_path}")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

package = "AllocationV2.usp_load_allocation_input"
for loaded in list(sys.modules):
    if (
        loaded == package
        or loaded.startswith(package + ".")
        or loaded == "AllocationV2.plan_profiler"
        or loaded.startswith("AllocationV2.plan_profiler.")
    ):
        del sys.modules[loaded]
importlib.invalidate_caches()

runner = importlib.import_module(
    f"{package}.output.updated.load_allocation_input"
)
started = time.time()
result = runner.run_load_allocation_input(
    spark,
    EntityID=int(dbutils.widgets.get("EntityID")),
    ClientID=int(dbutils.widgets.get("ClientID")),
    TaxPeriodID=int(dbutils.widgets.get("TaxPeriodID")),
    RunID=int(dbutils.widgets.get("RunID")),
    CatalogName=dbutils.widgets.get("CatalogName").strip(),
    SchemaName=dbutils.widgets.get("SchemaName").strip(),
    VolumePath=dbutils.widgets.get("VolumePath").strip(),
    MaxThreads=int(dbutils.widgets.get("MaxThreads")),
    ProfilePlan=dbutils.widgets.get("ProfilePlan").strip().lower() == "on",
    PlanCheckpointThreshold=int(
        dbutils.widgets.get("PlanCheckpointThreshold")
    ),
    CheckpointBackend=dbutils.widgets.get("CheckpointBackend").strip(),
)
print(f"[runner] wall={time.time() - started:.3f}s")
print(result)
