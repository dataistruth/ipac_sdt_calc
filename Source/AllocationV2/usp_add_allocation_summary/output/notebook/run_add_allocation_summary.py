# Databricks notebook source
# MAGIC %md
# MAGIC # Run uspAddAllocationSummary (production)
# MAGIC Frozen widgets 1–8. ExecutionProfile resolved in the orchestrator.

# COMMAND ----------

# Uncomment to reset widgets to notebook defaults.
# dbutils.widgets.removeAll()

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
import json
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
RESULT_TYPE = "deltalake"
VOLUME_PATH = "/Volumes/qa7/datavolume/databrickdata"
PACKAGE = "AllocationV2.usp_add_allocation_summary"
MODULE = "AllocationV2.usp_add_allocation_summary.output.add_allocation_summary"
ENTRY = "run_add_allocation_summary"
OUTPUT_TABLES = ('K1AllocationSummary', 'M1AdjAllocationSummary', 'BoxJKLAllocationSummary', 'Form926AllocationSummary', 'Form8865AllocationSummary', 'Form199AAllocationSummary', 'PFICFootnoteAllocationSummary', 'PFICFootnoteAllocationText', 'CustomFootnoteAllocationSummary', 'Line18AAllocationSummary', 'UBTIAllocationSummary', 'PassiveIncomeAllocationSummary', 'Form200616AllocationSummary', 'Form8886AllocationSummary', 'AtRiskAllocationSummary', 'GAAPToTaxAllocation', 'AdjustmentAllocationSummary')

while source_path in sys.path:
    sys.path.remove(source_path)
sys.path.insert(0, source_path)


def _evict():
    roots = (f"{PACKAGE}.output", "Common_V2")
    for name in list(sys.modules):
        if any(name == root or name.startswith(root + ".") for root in roots):
            del sys.modules[name]
    importlib.invalidate_caches()


_evict()
importlib.import_module("Common_V2.core.checkpoint_V2")
runner = importlib.import_module(MODULE)
print(f"[run] module={runner.__file__}", flush=True)


def _fqn(table):
    return f"`{catalog}`.`{schema}`.`{table}`"


def _row_counts():
    counts = {}
    total = 0
    for table in OUTPUT_TABLES:
        name = _fqn(table)
        try:
            exists = spark.catalog.tableExists(name)
        except Exception:
            exists = False
        if not exists:
            counts[table] = None
            continue
        df = spark.table(name)
        columns = list(df.columns)
        if "RunID" in columns:
            df = df.filter(df.RunID == int(run_id))
        n = df.count()
        counts[table] = n
        total += n
    return counts, total


def _reported_seconds(result):
    parsed = result
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except (TypeError, ValueError):
            return None
    if not isinstance(parsed, dict):
        return None
    value = parsed.get("elapsed_seconds")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


kwargs = dict(
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


started = time.perf_counter()
result = getattr(runner, ENTRY)(spark, **kwargs)
wall = round(time.perf_counter() - started, 3)
reported = _reported_seconds(result)
counts, total_rows = _row_counts()
print(
    f"[run] production-inline wall={wall:.3f}s reported={reported} rows={total_rows}",
    flush=True,
)
for table in OUTPUT_TABLES:
    print(f"[run] {table} rows={counts[table]}", flush=True)
display({"result": result, "wall_seconds": wall, "reported": reported, "rows": counts})
