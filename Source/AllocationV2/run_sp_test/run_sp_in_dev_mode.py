# Databricks notebook source
# MAGIC %md
# MAGIC # Run 12 SPs in Development mode
# MAGIC
# MAGIC First cell is the locked list. Each SP is run by its own
# MAGIC `outputV2/notebook/benchmark_*.py` (original `output` then `outputV2`).
# MAGIC Child notebooks keep their own Entity / Client / Run / catalog widgets.
# MAGIC
# MAGIC Suite widgets: `source_path`, `ExecutionProfile`, `run_in_parallel`.
# MAGIC `run_in_parallel=3` runs 12 SPs three at a time.

# COMMAND ----------

SP_LIST = [
    "usp_load_allocation_input",
    "usp_load_footnotes_allocation_to_output",
    "usp_get_final_effective_percentage",
    "usp_apply_investment_level_rounding",
    "usp_add_allocation_summary",
    "usp_load_k3_allocation_summary",
    "usp_load_lookthrough_cost_alloc_to_output",
    "usp_load_lookthrough_allocation_input",
    "usp_load_lookthrough_footnote_effective_allocation_pct",
    "usp_add_lookthrough_alloc_detail_step01",
    "usp_sm_load_lookthrough_cost_allocation_to_output",
    "usp_sm_load_lookthrough_effective_allocation_pct",
]
print(f"[suite] {len(SP_LIST)} optimized SPs")
for index, name in enumerate(SP_LIST, start=1):
    print(f"  {index:2d}. {name}")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.dropdown(
    "ExecutionProfile",
    "default",
    ["default", "small", "large"],
    "2. Execution profile",
)
dbutils.widgets.text("run_in_parallel", "1", "3. SPs at a time")

# COMMAND ----------

import importlib
import sys
import time
import traceback

source_path = dbutils.widgets.get("source_path").strip()
execution_profile = dbutils.widgets.get("ExecutionProfile").strip() or "default"
run_in_parallel = max(1, int(dbutils.widgets.get("run_in_parallel") or 1))

sys.path[:] = [entry for entry in sys.path if entry != source_path]
sys.path.insert(0, source_path)
importlib.invalidate_caches()

from AllocationV2.run_sp_test.sp_catalog import SP_SPECS
from AllocationV2.run_sp_test import suite_runtime as suite

assert [spec["name"] for spec in SP_SPECS] == SP_LIST, "SP_LIST drifted from catalog"
specs = suite.all_specs()
child_args = {
    "source_path": source_path,
    "ExecutionProfile": execution_profile,
}
print(
    f"[suite] dev notebooks parallel={run_in_parallel} profile={execution_profile}",
    flush=True,
)


def run_one(spec):
    row = {"sp": spec["name"], "wall": None, "status": "FAIL", "error": ""}
    try:
        wall = suite.run_via_notebook(
            dbutils, source_path, spec["dev_notebook"], child_args
        )
        row["wall"] = wall
        row["status"] = "PASS"
        print(f"[suite] {spec['name']} A/B notebook wall={wall}s", flush=True)
        return row
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
        print(f"[suite] {spec['name']} FAIL\n{traceback.format_exc()}", flush=True)
        return row


started = time.time()
runtime_rows = suite.run_specs(specs, run_one, run_in_parallel)
suite_wall = round(time.time() - started, 3)
print(f"[suite] finished wall={suite_wall}s", flush=True)

# COMMAND ----------

display(
    spark.createDataFrame(
        runtime_rows,
        schema="sp string, wall double, status string, error string",
    )
)
