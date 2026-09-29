# Databricks notebook source
"""Production-inline run for uspLoadLookThroughAllocationInput."""

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
volume_path = "/Volumes/qa7/datavolume/databrickdata"

while source_path in sys.path:
    sys.path.remove(source_path)
sys.path.insert(0, source_path)
print(f"[run] Python import root={sys.path[0]}", flush=True)

PACKAGE = "AllocationV2.usp_load_lookthrough_allocation_input"
MODULE = f"{PACKAGE}.output.load_lookthrough_allocation_input"
MODULE_ROOTS = (f"{PACKAGE}.output", "Common_V2")
TABLES = (
    ("LookThroughAllocationInput", "RunID"),
    ("SchKTaxableIncome", "UpperTierRunID"),
    ("PFICtoK1IncomeAttributePercentages", "RunID"),
    ("AllocationRunErrors", "RunID"),
    ("AllocationRun", "RunID"),
)


def _clear_modules():
    for loaded in list(sys.modules):
        if any(
            loaded == root or loaded.startswith(root + ".")
            for root in MODULE_ROOTS
        ):
            del sys.modules[loaded]
    importlib.invalidate_caches()


_clear_modules()
try:
    import Common_V2.core.checkpoint_V2 as _checkpoint_v2  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "Could not import Common_V2.core.checkpoint_V2 after adding "
        f"the Source folder to sys.path: {source_path}"
    ) from exc
_clear_modules()
module = importlib.import_module(MODULE)


def _quoted_fqn(table_name):
    return ".".join(
        f"`{part.replace('`', '``')}`"
        for part in (catalog, schema, table_name)
    )


def _row_counts():
    counts = {}
    for table_name, key in TABLES:
        fqn = _quoted_fqn(table_name)
        try:
            df = spark.table(fqn)
            if key in df.columns:
                counts[table_name] = df.filter(df[key] == run_id).count()
            else:
                counts[table_name] = None
        except Exception as exc:
            counts[table_name] = f"{type(exc).__name__}: {exc}"
    return counts


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


started = time.perf_counter()
result = module.run_load_lookthrough_allocation_input(
    spark,
    EntityID=entity_id,
    ClientID=client_id,
    TaxPeriodID=tax_period_id,
    RunID=run_id,
    CatalogName=catalog,
    SchemaName=schema,
    ResultType="deltalake",
    VolumePath=volume_path,
    ExecutionProfile=execution_profile,
)
elapsed = time.perf_counter() - started
reported = _reported_seconds(result)
rows = _row_counts()
print(
    f"[run] production-inline wall={elapsed:.3f}s reported={reported} rows={rows}",
    flush=True,
)
