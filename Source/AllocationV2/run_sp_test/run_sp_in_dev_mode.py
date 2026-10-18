# Databricks notebook source
# MAGIC %md
# MAGIC # Run 12 SPs in Development mode
# MAGIC
# MAGIC First cell is the locked list. For each selected SP this notebook runs
# MAGIC **output** (old / unoptimized when that folder is an `sdt_d` checkout)
# MAGIC then **outputV2** (optimized) and records wall times only.
# MAGIC
# MAGIC `run_in_parallel=3` runs 12 SPs three at a time. Within one SP, original
# MAGIC then updated stay sequential because they write the same tables.
# MAGIC
# MAGIC `invoke=entry` calls the same public orchestrator names as the
# MAGIC per-SP A/B notebooks. `invoke=notebook` runs those
# MAGIC `outputV2/notebook/benchmark_*.py` notebooks (full A/B + hash).

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

# Uncomment to reset widgets to notebook defaults.
# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("ClientID", "15348", "2. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "3. TaxPeriodID")
dbutils.widgets.text("CatalogName", "QA7", "4. Catalog override")
dbutils.widgets.text("SchemaName", "iPC_2025_QA7_15348", "5. Schema override")
dbutils.widgets.text("EntityID", "4137", "6. Entity override")
dbutils.widgets.text("RunID", "17376", "7. RunID override")
dbutils.widgets.dropdown(
    "use_sp_identity",
    "on",
    ["on", "off"],
    "8. Use each SP locked identity",
)
dbutils.widgets.dropdown(
    "ExecutionProfile",
    "low",
    ["low", "medium", "big"],
    "9. Execution profile",
)
dbutils.widgets.text("run_in_parallel", "1", "10. SPs at a time")
dbutils.widgets.text("sp_filter", "", "11. Comma SP names (blank=all)")
dbutils.widgets.dropdown(
    "invoke",
    "entry",
    ["entry", "notebook"],
    "12. Call entry or A/B notebook",
)
dbutils.widgets.dropdown(
    "ProfilePlan",
    "off",
    ["off", "on"],
    "13. Plan profile (updated only)",
)

# COMMAND ----------

import importlib
import sys
import time
import traceback

source_path = dbutils.widgets.get("source_path").strip()
sys.path[:] = [entry for entry in sys.path if entry != source_path]
sys.path.insert(0, source_path)
importlib.invalidate_caches()

from AllocationV2.run_sp_test.sp_catalog import SP_SPECS
from AllocationV2.run_sp_test import suite_runtime as suite

assert [spec["name"] for spec in SP_SPECS] == SP_LIST, "SP_LIST drifted from catalog"

overrides = {
    "use_sp_identity": dbutils.widgets.get("use_sp_identity").strip().lower() == "on",
    "client_id": dbutils.widgets.get("ClientID"),
    "tax_period_id": dbutils.widgets.get("TaxPeriodID"),
    "catalog": dbutils.widgets.get("CatalogName").strip(),
    "schema": dbutils.widgets.get("SchemaName").strip(),
    "entity_id": dbutils.widgets.get("EntityID"),
    "run_id": dbutils.widgets.get("RunID"),
}
execution_profile = dbutils.widgets.get("ExecutionProfile").strip() or "low"
run_in_parallel = max(1, int(dbutils.widgets.get("run_in_parallel") or 1))
invoke = dbutils.widgets.get("invoke").strip().lower() or "entry"
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
specs = suite.selected_specs(dbutils.widgets.get("sp_filter"))
suite.prepare_source(source_path)
print(
    f"[suite] dev invoke={invoke} parallel={run_in_parallel} "
    f"profile={execution_profile} sps={[spec['name'] for spec in specs]}",
    flush=True,
)


def run_one(spec):
    identity = suite.identity_for(spec, overrides)
    row = {
        "sp": spec["name"],
        "entity_id": identity["EntityID"],
        "run_id": identity["RunID"],
        "catalog": identity["CatalogName"],
        "schema": identity["SchemaName"],
        "original_wall": None,
        "updated_wall": None,
        "original_reported": None,
        "updated_reported": None,
        "status": "FAIL",
        "error": "",
    }
    try:
        if invoke == "notebook":
            args = suite.notebook_args(
                spec,
                identity,
                execution_profile,
                number_of_runs=1,
                profile_plan=profile_plan,
                source_path=source_path,
            )
            wall = suite.run_via_notebook(
                dbutils, source_path, spec["dev_notebook"], args, 7200
            )
            row["updated_wall"] = wall
            row["status"] = "PASS"
            print(
                f"[suite] {spec['name']} notebook A/B wall={wall}s "
                f"(see child notebook for original vs updated)",
                flush=True,
            )
            return row
        orig_wall, orig_reported = suite.call_entry(
            spark,
            spec,
            spec["original_module"],
            identity,
            spec["extra"],
            updated=False,
            execution_profile=execution_profile,
            profile_plan=False,
        )
        row["original_wall"] = orig_wall
        row["original_reported"] = orig_reported
        print(
            f"[suite] {spec['name']} original wall={orig_wall} reported={orig_reported}",
            flush=True,
        )
        upd_wall, upd_reported = suite.call_entry(
            spark,
            spec,
            spec["updated_module"],
            identity,
            spec["extra"],
            updated=True,
            execution_profile=execution_profile,
            profile_plan=profile_plan,
        )
        row["updated_wall"] = upd_wall
        row["updated_reported"] = upd_reported
        row["status"] = "PASS"
        print(
            f"[suite] {spec['name']} updated wall={upd_wall} reported={upd_reported}",
            flush=True,
        )
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

runtime_schema = (
    "sp string, entity_id int, run_id int, catalog string, schema string, "
    "original_wall double, updated_wall double, "
    "original_reported double, updated_reported double, "
    "status string, error string"
)
display(spark.createDataFrame(runtime_rows, schema=runtime_schema))
