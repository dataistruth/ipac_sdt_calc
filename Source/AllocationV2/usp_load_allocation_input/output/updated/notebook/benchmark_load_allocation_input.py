# Databricks notebook source
"""A/B benchmark for production and output.updated allocation orchestrators."""

# COMMAND ----------

import importlib
import json
import os
import sys
import time

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks_msingh/Source",
    "1. Monolith Source/",
)
dbutils.widgets.text("number_of_runs", "1", "2. A/B passes")
dbutils.widgets.dropdown(
    "ExecutionOrder",
    "original_first",
    ["original_first", "updated_first"],
    "3. Execution order",
)
dbutils.widgets.text("EntityID", "115", "4. EntityID")
dbutils.widgets.text("ClientID", "15348", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "16560", "7. RunID")
dbutils.widgets.text("CatalogName", "QA7", "8. Catalog")
dbutils.widgets.text("SchemaName", "IPC_2025_QA7_15348", "9. Schema")
dbutils.widgets.text(
    "VolumePath",
    "/Volumes/qa7/datavolume/databrickdata",
    "10. Volume path",
)
dbutils.widgets.text("MaxThreads", "4", "11. Updated parallel workers")
dbutils.widgets.dropdown(
    "ProfilePlan",
    "on",
    ["off", "on"],
    "12. Plan profiler",
)
dbutils.widgets.text(
    "PlanCheckpointThreshold",
    "30",
    "13. Plan checkpoint threshold",
)
dbutils.widgets.dropdown(
    "CheckpointBackend",
    "delta",
    ["local", "delta"],
    "14. Checkpoint backend",
)
dbutils.widgets.text(
    "SqlShufflePartitions",
    "16",
    "15. spark.sql.shuffle.partitions",
)
dbutils.widgets.text(
    "LocalDeltaDenylist",
    "reclass_data,pfic_snapshot,base_flowup",
    "16. Local deny list",
)
dbutils.widgets.dropdown(
    "ResultType",
    "deltalake",
    ["deltalake", "parquet"],
    "17. Result type",
)

# COMMAND ----------

source_path = dbutils.widgets.get("source_path").rstrip("/")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()
if shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", int(shuffle_partitions))

common_args = {
    "EntityID": int(dbutils.widgets.get("EntityID")),
    "ClientID": int(dbutils.widgets.get("ClientID")),
    "TaxPeriodID": int(dbutils.widgets.get("TaxPeriodID")),
    "RunID": int(dbutils.widgets.get("RunID")),
    "CatalogName": dbutils.widgets.get("CatalogName"),
    "SchemaName": dbutils.widgets.get("SchemaName"),
    "ResultType": dbutils.widgets.get("ResultType"),
    "VolumePath": dbutils.widgets.get("VolumePath"),
}
updated_args = {
    **common_args,
    "MaxThreads": int(dbutils.widgets.get("MaxThreads") or "4"),
    "CheckpointBackend": dbutils.widgets.get("CheckpointBackend"),
    "LocalDeltaDenylist": dbutils.widgets.get("LocalDeltaDenylist"),
    "ProfilePlan": dbutils.widgets.get("ProfilePlan"),
    "PlanCheckpointThreshold": int(
        dbutils.widgets.get("PlanCheckpointThreshold")
    ),
}

# COMMAND ----------

def _fresh_import(module_name):
    for loaded in list(sys.modules):
        if loaded == module_name or loaded.startswith(module_name + "."):
            del sys.modules[loaded]
    importlib.invalidate_caches()
    return importlib.import_module(module_name)


def _run_variant(name):
    if name == "production":
        module_name = (
            "AllocationV2.usp_load_allocation_input.output.load_allocation_input"
        )
        args = common_args
    else:
        module_name = (
            "AllocationV2.usp_load_allocation_input.output.updated."
            "load_allocation_input"
        )
        args = updated_args
    module = _fresh_import(module_name)
    started = time.perf_counter()
    result = module.run_load_allocation_input(spark, **args)
    elapsed = time.perf_counter() - started
    return {
        "variant": name,
        "elapsed_seconds": round(elapsed, 3),
        "checkpoint_backend": (
            updated_args["CheckpointBackend"] if name == "updated" else "production"
        ),
        "profile_plan": (
            updated_args["ProfilePlan"] if name == "updated" else "off"
        ),
        "result": json.dumps(result, default=str, sort_keys=True),
    }


number_of_runs = max(1, int(dbutils.widgets.get("number_of_runs")))
order = (
    ["production", "updated"]
    if dbutils.widgets.get("ExecutionOrder") == "original_first"
    else ["updated", "production"]
)

rows = []
for iteration in range(1, number_of_runs + 1):
    for variant in order:
        row = _run_variant(variant)
        row["iteration"] = iteration
        rows.append(row)
        print(
            f"[benchmark] run={iteration} variant={variant} "
            f"elapsed={row['elapsed_seconds']:.3f}s"
        )

display(spark.createDataFrame(rows).orderBy("iteration", "variant"))

# COMMAND ----------

summary = (
    spark.createDataFrame(rows)
    .groupBy("variant", "checkpoint_backend", "profile_plan")
    .avg("elapsed_seconds")
    .withColumnRenamed("avg(elapsed_seconds)", "average_elapsed_seconds")
)
display(summary)
