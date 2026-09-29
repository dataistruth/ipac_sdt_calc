# Databricks notebook source
"""Production-inline run for uspLoadAllocationInput."""

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("EntityID", "115", "2. EntityID")
dbutils.widgets.text("ClientID", "15348", "3. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "4. TaxPeriodID")
dbutils.widgets.text("RunID", "16560", "5. RunID")
dbutils.widgets.text("CatalogName", "QA7", "6. Catalog")
dbutils.widgets.text("SchemaName", "IPC_2025_QA7_15348", "7. Schema")
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

source_root = dbutils.widgets.get("source_path").rstrip("/")
while source_root in sys.path:
    sys.path.remove(source_root)
sys.path.insert(0, source_root)
print(f"[run] Python import root={sys.path[0]}", flush=True)

PACKAGE = "AllocationV2.usp_load_allocation_input"
MODULE = f"{PACKAGE}.output.load_allocation_input"
MODULE_ROOTS = (f"{PACKAGE}.output", "Common_V2")
OUTPUT_TABLES = (
    "AllocationInput",
    "PFICFootnoteFlowup",
    "PFICFootnoteFlowupWithTrackingKey",
    "Form926Flowup",
    "Form199AFlowup",
    "Form8865Flowup",
    "Form8886Flowup",
    "AtRiskFlowup",
    "CustomFootnoteFlowup",
    "Form200616Flowup",
)
common_args = {
    "EntityID": int(dbutils.widgets.get("EntityID")),
    "ClientID": int(dbutils.widgets.get("ClientID")),
    "TaxPeriodID": int(dbutils.widgets.get("TaxPeriodID")),
    "RunID": int(dbutils.widgets.get("RunID")),
    "CatalogName": dbutils.widgets.get("CatalogName"),
    "SchemaName": dbutils.widgets.get("SchemaName"),
    "ResultType": "deltalake",
    "VolumePath": "/Volumes/qa7/datavolume/databrickdata",
    "ExecutionProfile": dbutils.widgets.get("ExecutionProfile").strip() or "low",
}


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
        f"the Source folder to sys.path: {source_root}"
    ) from exc
_clear_modules()
module = importlib.import_module(MODULE)


def _quoted_fqn(table_name):
    return ".".join(
        f"`{part.replace('`', '``')}`"
        for part in (
            common_args["CatalogName"],
            common_args["SchemaName"],
            table_name,
        )
    )


def _purge_run_outputs():
    run_id = int(common_args["RunID"])
    for table_name in OUTPUT_TABLES:
        fqn = _quoted_fqn(table_name)
        try:
            columns = spark.table(fqn).columns
            if "RunID" not in columns:
                print(f"[purge] skip {table_name} (no RunID)")
                continue
            spark.sql(f"DELETE FROM {fqn} WHERE RunID = {run_id}")
            print(f"[purge] {table_name} RunID={run_id}")
        except Exception as exc:
            print(f"[purge] skip {table_name}; {type(exc).__name__}: {exc}")


def _row_counts():
    run_id = int(common_args["RunID"])
    counts = {}
    for table_name in OUTPUT_TABLES:
        fqn = _quoted_fqn(table_name)
        try:
            df = spark.table(fqn)
            if "RunID" in df.columns:
                counts[table_name] = df.filter(df.RunID == run_id).count()
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
    for key in ("elapsed_seconds", "ElapsedSeconds", "total_time_seconds"):
        if parsed.get(key) is not None:
            try:
                return float(parsed[key])
            except (TypeError, ValueError):
                return None
    return None


_purge_run_outputs()
started = time.perf_counter()
result = module.run_load_allocation_input(spark, **common_args)
elapsed = time.perf_counter() - started
reported = _reported_seconds(result)
rows = _row_counts()
print(
    f"[run] production-inline wall={elapsed:.3f}s reported={reported} rows={rows}",
    flush=True,
)
