# Databricks notebook source
# MAGIC %md
# MAGIC # Final Effective Percentage production run
# MAGIC
# MAGIC Runs the live `output` orchestrator once. Wall time and row counts only.

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

source_path = dbutils.widgets.get("source_path").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
execution_profile = dbutils.widgets.get("ExecutionProfile").strip() or "low"

RESULT_TYPE = "deltalake"
VOLUME_PATH = "/Volumes/qa7/datavolume/databrickdata"
OUTPUT_TABLES = (
    "FinalEffectivePercentages",
    "FNFinalEffectivePercentages",
    "SM_FinalEffectivePercentages",
)

# COMMAND ----------

import importlib
import sys
import time

PACKAGE = "AllocationV2.usp_get_final_effective_percentage"
PRODUCTION = f"{PACKAGE}.output.orchestrator"


def _evict():
    roots = (f"{PACKAGE}.output", "Common_V2")
    for name in list(sys.modules):
        if any(name == root or name.startswith(root + ".") for root in roots):
            del sys.modules[name]
    importlib.invalidate_caches()


sys.path[:] = [entry for entry in sys.path if entry != source_path]
sys.path.insert(0, source_path)
_evict()
importlib.import_module("Common_V2.core.checkpoint_V2")
runner = importlib.import_module(PRODUCTION)


def _fqn(table):
    return f"`{catalog}`.`{schema}`.`{table}`"


def _purge_run():
    for table in OUTPUT_TABLES:
        name = _fqn(table)
        if not spark.catalog.tableExists(name):
            continue
        columns = list(spark.table(name).columns)
        if "RunID" not in columns:
            continue
        spark.sql(f"DELETE FROM {name} WHERE RunID = {int(run_id)}")
        try:
            spark.catalog.refreshTable(name)
        except Exception:
            spark.sql(f"REFRESH TABLE {name}")


def _row_counts():
    counts = {}
    total = 0
    for table in OUTPUT_TABLES:
        name = _fqn(table)
        if not spark.catalog.tableExists(name):
            counts[table] = None
            continue
        columns = list(spark.table(name).columns)
        df = spark.table(name)
        if "RunID" in columns:
            df = df.filter(df.RunID == int(run_id))
        n = df.count()
        counts[table] = n
        total += n
    return counts, total


_purge_run()
started = time.time()
result = runner.run_final_effective_percentages(
    spark,
    Mode=0,
    EntityID=entity_id,
    ClientID=client_id,
    TaxPeriodID=tax_period_id,
    RunID=run_id,
    CatalogName=catalog,
    SchemaName=schema,
    ResultType=RESULT_TYPE,
    VolumePath=VOLUME_PATH,
    ExecutionProfile=execution_profile,
)
wall = round(time.time() - started, 3)
reported = (
    float(result["elapsed_seconds"])
    if isinstance(result, dict) and result.get("elapsed_seconds") is not None
    else None
)
counts, total_rows = _row_counts()
print(
    f"[run] production-inline wall={wall:.3f}s reported={reported} "
    f"rows={total_rows}"
)
for table in OUTPUT_TABLES:
    print(f"[run] {table} rows={counts[table]}")
