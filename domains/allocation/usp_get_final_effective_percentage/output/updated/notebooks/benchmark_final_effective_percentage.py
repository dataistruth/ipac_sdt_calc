# Databricks notebook source
# MAGIC %md
# MAGIC # A/B benchmark — usp_get_final_effective_percentage
# MAGIC
# MAGIC Runs the unchanged production module and `output.updated` against the
# MAGIC same RunID. Each variant starts with clean output partitions. Parity
# MAGIC compares row counts, amount sums, schemas, and order-independent row
# MAGIC fingerprints for all three output tables.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Monolith Source/",
)
dbutils.widgets.text("number_of_runs", "1", "2. A/B passes")
dbutils.widgets.dropdown("Mode", "0", ["0", "4"], "3. Mode")
dbutils.widgets.text("EntityID", "5051", "4. EntityID")
dbutils.widgets.text("ClientID", "15347", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "3517", "7. RunID")
dbutils.widgets.text("CatalogName", "QA7", "8. Catalog")
dbutils.widgets.text(
    "SchemaName",
    "iPC_2025_QA7_15347",
    "9. Schema",
)
dbutils.widgets.dropdown(
    "ResultType",
    "deltalake",
    ["deltalake"],
    "10. Result type",
)
dbutils.widgets.text("VolumePath", "", "11. Volume path (optional)")

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs").strip() or "1")
mode = int(dbutils.widgets.get("Mode").strip())
entity_id = int(dbutils.widgets.get("EntityID").strip())
client_id = int(dbutils.widgets.get("ClientID").strip())
tax_period_id = int(dbutils.widgets.get("TaxPeriodID").strip())
run_id = int(dbutils.widgets.get("RunID").strip())
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
result_type = dbutils.widgets.get("ResultType").strip()
volume_path = dbutils.widgets.get("VolumePath").strip()

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if result_type.lower() != "deltalake":
    raise ValueError(
        "The A/B parity benchmark requires ResultType=deltalake so output "
        "partitions can be reconciled after each variant."
    )

# COMMAND ----------

import importlib
import json
import os
import sys
import time
from datetime import datetime

if not os.path.isdir(source_path):
    raise RuntimeError(f"Source path does not exist: {source_path}")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_get_final_effective_percentage"
ORIGINAL_MODULE = f"{PACKAGE}.output.orchestrator"
UPDATED_MODULE = f"{PACKAGE}.output.updated.orchestrator"


def _import_fresh(module_name: str):
    for loaded in list(sys.modules):
        if loaded == PACKAGE or loaded.startswith(f"{PACKAGE}."):
            del sys.modules[loaded]
    importlib.invalidate_caches()
    module = importlib.import_module(module_name)
    print(f"imported {module_name}: {module.__file__}")
    return module


reconcile = importlib.import_module(f"{PACKAGE}.output.updated.output_reconcile")

# COMMAND ----------

def _run_variant(variant: str, pass_number: int) -> dict:
    module_name = ORIGINAL_MODULE if variant == "original" else UPDATED_MODULE
    runner = _import_fresh(module_name)

    # Fresh import removed the first reconcile module object; use this module's
    # functions already held by the notebook.
    reconcile.purge_output_partitions_for_run(
        spark, catalog, schema, run_id
    )

    print(
        f"\n{'=' * 72}\n"
        f"PASS {pass_number} | {variant.upper()} | "
        f"{datetime.now().isoformat()}\n"
        f"{'=' * 72}"
    )
    started = time.time()
    result = runner.run_final_effective_percentages(
        spark,
        Mode=mode,
        EntityID=entity_id,
        ClientID=client_id,
        TaxPeriodID=tax_period_id,
        RunID=run_id,
        CatalogName=catalog,
        SchemaName=schema,
        ResultType=result_type,
        VolumePath=volume_path or None,
        ExecutionID=f"benchmark-{pass_number}-{variant}",
    )
    wall = round(time.time() - started, 3)

    metrics = reconcile.capture_output_metrics(
        spark, catalog, schema, run_id
    )
    summary = reconcile.summarize_metrics(metrics)
    reported = result.get("elapsed_seconds") if isinstance(result, dict) else None
    timings = result.get("timings", []) if isinstance(result, dict) else []
    print(
        f"[benchmark] {variant}: wall={wall:.3f}s "
        f"reported={reported} rows={summary['total_rows']}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "summary": summary,
        "metrics": metrics,
        "timings": timings,
    }


records = []
for pass_number in range(1, number_of_runs + 1):
    original = _run_variant("original", pass_number)
    updated = _run_variant("updated", pass_number)
    records.extend([original, updated])

    mismatches = reconcile.compare_variants(
        original["metrics"],
        updated["metrics"],
    )
    if mismatches:
        print(f"[reconcile] FAIL pass {pass_number}")
        for mismatch in mismatches:
            print(f"  {mismatch}")
        raise AssertionError(
            f"Output parity failed with {len(mismatches)} mismatch(es)"
        )
    print(f"[reconcile] PASS {pass_number}: all output fingerprints match")

# COMMAND ----------

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Explicit schemas avoid [CANNOT_DETERMINE_TYPE]: reported_seconds is None for
# the original variant, and timing/summary columns can be entirely null.
_summary_schema = StructType([
    StructField("pass", LongType(), True),
    StructField("variant", StringType(), True),
    StructField("wall_seconds", DoubleType(), True),
    StructField("reported_seconds", DoubleType(), True),
    StructField("total_rows", LongType(), True),
    StructField("tables_with_rows", LongType(), True),
])


def _as_float(value):
    return float(value) if value is not None else None


def _as_int(value):
    return int(value) if value is not None else None


summary_rows = [
    (
        _as_int(row["pass"]),
        row["variant"],
        _as_float(row["wall_seconds"]),
        _as_float(row["reported_seconds"]),
        _as_int(row["summary"]["total_rows"]),
        _as_int(row["summary"]["tables_with_rows"]),
    )
    for row in records
]
if summary_rows:
    display(
        spark.createDataFrame(summary_rows, schema=_summary_schema)
        .orderBy("pass", "variant")
    )
else:
    print("No benchmark records to display")

_timings_schema = StructType([
    StructField("step", StringType(), True),
    StructField("calls", LongType(), True),
    StructField("elapsed_seconds", DoubleType(), True),
])

for row in records:
    if row["variant"] == "updated" and row["timings"]:
        print(f"\nUpdated timings — pass {row['pass']}")
        timing_rows = [
            (
                str(item.get("step")),
                _as_int(item.get("calls")),
                _as_float(item.get("elapsed_seconds")),
            )
            for item in row["timings"]
        ]
        display(
            spark.createDataFrame(timing_rows, schema=_timings_schema)
            .orderBy("elapsed_seconds", ascending=False)
        )
