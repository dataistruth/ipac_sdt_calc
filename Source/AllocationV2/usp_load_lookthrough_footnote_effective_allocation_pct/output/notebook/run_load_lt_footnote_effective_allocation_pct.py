# Databricks notebook source
# MAGIC %md
# MAGIC # LT footnote effective allocation % — production run
# MAGIC Production-inline run (Mode 1). Widgets 1–8 only.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("EntityID", "4755", "2. EntityID")
dbutils.widgets.text("ClientID", "15348", "3. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "4. TaxPeriodID")
dbutils.widgets.text("RunID", "18266", "5. RunID")
dbutils.widgets.text("CatalogName", "qa7", "6. Catalog")
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

import pyspark.sql.functions as F

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


while source_path in sys.path:
    sys.path.remove(source_path)
sys.path.insert(0, source_path)
print(f"[run] Python import root={sys.path[0]}", flush=True)

package = "AllocationV2.usp_load_lookthrough_footnote_effective_allocation_pct"


def fresh_import():
    roots = (f"{package}.output", "Common_V2")
    for loaded in list(sys.modules):
        if any(
            loaded == root or loaded.startswith(root + ".")
            for root in roots
        ) or loaded in globals().get("FLAT_SIBLINGS", ()):
            del sys.modules[loaded]
    importlib.invalidate_caches()
    checkpoint = importlib.import_module("Common_V2.core.checkpoint_V2")
    print(f"[import] checkpoint_V2={checkpoint.__file__}")

    module = importlib.import_module("AllocationV2.usp_load_lookthrough_footnote_effective_allocation_pct.output.load_lt_footnote_effective_allocation_pct")
    print(f"[import] runner={module.__file__}")
    return module


module = fresh_import()
kwargs = dict(
    EntityID=entity_id,
    ClientID=client_id,
    TaxPeriodID=tax_period_id,
    RunID=run_id,
    CatalogName=catalog,
    SchemaName=schema,
    VolumePath=volume_path,
    ResultType=result_type,
    ExecutionProfile=execution_profile,
    ExecutionID="lt-mode1-prod",
)


started = time.perf_counter()
result = module.run_load_lt_footnote_effective_allocation_pct(spark, **kwargs)
wall = round(time.perf_counter() - started, 3)
reported = None
if isinstance(result, dict):
    reported = result.get("elapsed_seconds", result.get("elapsed"))

TABLES = [
    'LookThroughAllocationOutput',
    'LookThroughAllocationInput',
]
rows = {}
for table in TABLES:
    fqn = f"{catalog}.{schema}.{table}"
    try:
        rows[table] = spark.table(fqn).filter(F.col("RunID") == run_id).count()
    except Exception as exc:
        rows[table] = f"ERR:{exc}"

print(
    f"[run] production-inline wall={wall} reported={reported} rows={rows} "
    f"status={result.get('status') if isinstance(result, dict) else result}"
)
display(spark.createDataFrame(
    [(k, str(v)) for k, v in rows.items()],
    "table string, rows string",
))
