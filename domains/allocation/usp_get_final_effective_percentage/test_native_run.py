# Databricks notebook source
# MAGIC %md
# MAGIC # Native Databricks Test — uspGetFinalEffectivePercentage (v2 fusion)
# MAGIC
# MAGIC Tests the fully fused `mode=0` pipeline on branch `main_Raja_Pyspark_fineff_v2`.
# MAGIC
# MAGIC **API change vs `main_Raja_Pyspark_fineff`:**
# MAGIC - `run_mode(mode=0)` now executes modes 1+2+3 in a single fused pass.
# MAGIC - Returns `out["results"] = {1: df, 2: df, 3: df}` instead of a single `result`.
# MAGIC - Single-mode calls (`mode=1/2/3`) raise `ValueError`.
# MAGIC - Results are saved to catalog tables automatically by the orchestrator.
# MAGIC   Set `_skip_save=True` in cfg to disable saving during testing.
# MAGIC
# MAGIC **Golden baseline** (entity 152, client 15349, period 1, run 2093):
# MAGIC - Mode 1: 47,210 rows
# MAGIC
# MAGIC - Mode 2: 9,030 rows- Mode 3: 0 rows (no SM_LookThroughAllocationInput data)

# COMMAND ----------

# ====== TEST PARAMETERS ======
# Switch between the two test entities by toggling the block below.

# Golden-baseline test (validates regression against captured row counts)
# ENTITY_ID = 152
# CLIENT_ID = 15349
# TAX_PERIOD_ID = 1
# RUN_ID = 2093
# IS_PE_MODEL = False
# CATALOG = "Dev7"
# SCHEMA = "iPC_2025_Dev7_15349"

# ENTITY_ID = 994
# RUN_ID = 3418
# ENTITY_ID = 5054
# RUN_ID = 3511
ENTITY_ID = 5051
RUN_ID = 3517

CLIENT_ID = 15347
TAX_PERIOD_ID = 1
IS_PE_MODEL = False
CATALOG = "QA7"
SCHEMA = "iPC_2025_QA7_15347"

# Original benchmark entity (no golden baseline; just for perf comparison)
# ENTITY_ID = 144
# RUN_ID = 2074

MODE = 0  # 0 = fused 1+2+3, 4 = 704c. mode=1/2/3 will raise ValueError.

# Expected per-mode row counts for the golden entity (used by the regression check below).
# Edit this dict if you switch entity to one with different known counts.
EXPECTED_ROW_COUNTS = {1: 47_210, 2: 9_030, 3: 0}

# COMMAND ----------

import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("test_native_v2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import the v2 orchestrator
# MAGIC
# MAGIC Adjust `V2_REPO_ROOT` below to point at where you uploaded the **`main_Raja_Pyspark_fineff_v2`** worktree (or where Databricks Repos checked out the v2 branch). It MUST be different from the original `main_Raja_Pyspark_fineff` checkout.

# COMMAND ----------

import sys, os

# ====== POINT AT THE v2 WORKTREE ======
# Update this to wherever main_Raja_Pyspark_fineff_v2 is mounted in your workspace.
# Examples:
#   - Databricks Repos:   /Workspace/Repos/<you>/iPACSCore_SDT_Databricks_v2/Source
#   - Workspace Files:    /Workspace/Users/<you>/iPACSCore_SDT_Databricks_v2/Source
V2_REPO_ROOT = "/Workspace/Users/usa-rajmanikanta@deloitte.com/iPACSCore_SDT_Databricks/Source"

if not os.path.isdir(V2_REPO_ROOT):
    raise RuntimeError(
        f"V2_REPO_ROOT does not exist: {V2_REPO_ROOT}\n"
        "Update the path above to where the main_Raja_Pyspark_fineff_v2 branch is checked out."
    )

# Drop any stale v1 imports first so we don't accidentally call the old code.
for mod_name in list(sys.modules):
    if mod_name.startswith(("AllocationV2.usp_get_final_effective_percentage",
                             "Common_V2")):
        del sys.modules[mod_name]

if V2_REPO_ROOT not in sys.path:
    sys.path.insert(0, V2_REPO_ROOT)

import AllocationV2.usp_get_final_effective_percentage.output.orchestrator as _orch
from AllocationV2.usp_get_final_effective_percentage.output.orchestrator import run_mode, run_modes

print("Imported run_mode from:", _orch.__file__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run `run_mode(mode=0)` — fused 1+2+3

# COMMAND ----------

t0 = time.time()

cfg = {
    "entity_id": ENTITY_ID,
    "client_id": CLIENT_ID,
    "tax_period_id": TAX_PERIOD_ID,
    "run_id": RUN_ID,
    "is_pe_model": IS_PE_MODEL,
    "catalog": CATALOG,
    "schema": SCHEMA,
    "_skip_save": False,   # enable writing to Delta/Parquet
    "_skip_cleanup": False, # keep checkpoint tables alive for result counting
}

out = run_mode(
    spark,
    mode=MODE,
    cfg=cfg,
    verbose=True,
    ResultType="Parquet",
    #VolumePath="/Volumes/dev_poc/devtest1006client009/finaleffectivepercentages",
    VolumePath="/Volumes/qa7/datavolume/dev_poc/devtest1006client009/finaleffectivepercentages/",
    ExecutionID="10",
)


pipeline_time = round(time.time() - t0, 2)
print(f"{'='*60}")

print(f"\n{'='*60}")
print(f"Wall clock (this cell): {pipeline_time}s")

print(f"Pipeline status:        {out['status']}")
print(f"Orchestrator elapsed:   {out['elapsed_seconds']}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-mode row counts + regression check
# MAGIC
# MAGIC Each `count()` triggers full materialization of that mode's result. Compare against the golden baseline.

# COMMAND ----------

# MAGIC %skip
# MAGIC results = out.get("results", {})
# MAGIC
# MAGIC actual_counts = {}
# MAGIC total_count_time = 0.0
# MAGIC
# MAGIC for sub_mode in sorted(results.keys()):
# MAGIC     df = results[sub_mode]
# MAGIC     if df is None:
# MAGIC         actual_counts[sub_mode] = 0
# MAGIC         print(f"Mode {sub_mode}: result=None  (fast-exit / empty input)")
# MAGIC         continue
# MAGIC     t1 = time.time()
# MAGIC     n = df.count()
# MAGIC     ct = round(time.time() - t1, 2)
# MAGIC     total_count_time += ct
# MAGIC     actual_counts[sub_mode] = n
# MAGIC     print(f"Mode {sub_mode}: {n:,} rows   (count time: {ct}s)")
# MAGIC
# MAGIC print(f"\nTotal count time: {round(total_count_time, 2)}s")
# MAGIC print(f"Total (pipeline + count): {round(pipeline_time + total_count_time, 2)}s")
# MAGIC
# MAGIC # Regression check against golden baseline.
# MAGIC print(f"\n{'='*60}")
# MAGIC print("Regression check vs golden baseline:")
# MAGIC print(f"{'='*60}")
# MAGIC any_fail = False
# MAGIC for sub_mode, expected in EXPECTED_ROW_COUNTS.items():
# MAGIC     actual = actual_counts.get(sub_mode, 0)
# MAGIC     delta = actual - expected
# MAGIC     if delta == 0:
# MAGIC         print(f"  Mode {sub_mode}: PASS  (expected={expected:,}, actual={actual:,})")
# MAGIC     else:
# MAGIC         any_fail = True
# MAGIC         sign = "+" if delta > 0 else ""
# MAGIC         print(f"  Mode {sub_mode}: FAIL  (expected={expected:,}, actual={actual:,}, delta={sign}{delta:,})")
# MAGIC
# MAGIC if any_fail:
# MAGIC     print("\n  -> See bisection cell below to localize the bug.")
# MAGIC else:
# MAGIC     print("\n  All modes match the golden baseline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample rows per mode

# COMMAND ----------

# MAGIC %skip
# MAGIC for sub_mode in sorted(results.keys()):
# MAGIC     df = results[sub_mode]
# MAGIC     if df is not None:
# MAGIC         print(f"\n--- Mode {sub_mode} (5 sample rows) ---")
# MAGIC         df.show(5, truncate=False)
# MAGIC     else:
# MAGIC         print(f"\n--- Mode {sub_mode}: None (skipped) ---")