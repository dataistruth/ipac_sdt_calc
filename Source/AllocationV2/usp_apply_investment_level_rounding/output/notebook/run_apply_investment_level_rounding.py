# Databricks notebook source
# MAGIC %md
# MAGIC # Run uspApplyInvestmentLevelRounding (production)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("EntityID", "4137", "2. EntityID")
dbutils.widgets.text("ClientID", "15348", "3. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "4. TaxPeriodID")
dbutils.widgets.text("RunID", "17376", "5. RunID")
dbutils.widgets.text("CatalogName", "QA7", "6. Catalog")
dbutils.widgets.text("SchemaName", "iPC_2025_QA7_15348", "7. Schema")
dbutils.widgets.dropdown(
    "ExecutionProfile",
    "low",
    ["low", "medium", "big"],
    "8. Execution profile",
)

# COMMAND ----------

import importlib
import sys
import time

source_path = dbutils.widgets.get("source_path").strip().rstrip("/")
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
execution_profile = dbutils.widgets.get("ExecutionProfile").strip() or "low"
volume_path = "/Volumes/qa7/datavolume/databrickdata"
result_type = "deltalake"
call_from = None

while source_path in sys.path:
    sys.path.remove(source_path)
sys.path.insert(0, source_path)

for loaded in list(sys.modules):
    if loaded.startswith(
        "AllocationV2.usp_apply_investment_level_rounding"
    ) or loaded.startswith("Common_V2"):
        del sys.modules[loaded]
importlib.invalidate_caches()

runner = importlib.import_module(
    "AllocationV2.usp_apply_investment_level_rounding.output."
    "apply_investment_level_rounding"
)
print(f"[run] module={runner.__file__}", flush=True)

t0 = time.perf_counter()
result = runner.apply_investment_level_rounding(
    spark,
    EntityID=entity_id,
    ClientID=client_id,
    TaxPeriodID=tax_period_id,
    RunID=run_id,
    CallFrom=call_from,
    CatalogName=catalog,
    SchemaName=schema,
    ResultType=result_type,
    VolumePath=volume_path,
    ExecutionProfile=execution_profile,
)
elapsed = round(time.perf_counter() - t0, 3)
print(f"[run] wall_seconds={elapsed}", flush=True)
display({"result": result, "wall_seconds": elapsed})
