# Databricks notebook source
# MAGIC %md
# MAGIC # usp_get_final_effective_percentage Development A/B
# MAGIC
# MAGIC Original = production `output`. Updated = `outputV2`.
# MAGIC Order is always original then updated.

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
dbutils.widgets.text("number_of_runs", "1", "9. A/B passes")
dbutils.widgets.dropdown(
    "ProfilePlan",
    "off",
    ["off", "on"],
    "10. Plan profile",
)

# COMMAND ----------

import importlib
import json
import sys
import time

from pyspark.sql import functions as F

source_path = dbutils.widgets.get("source_path").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
execution_profile = dbutils.widgets.get("ExecutionProfile").strip() or "low"
number_of_runs = max(1, int(dbutils.widgets.get("number_of_runs") or 1))
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"

PACKAGE = "AllocationV2.usp_get_final_effective_percentage"
ORIGINAL = "AllocationV2.usp_get_final_effective_percentage.output.orchestrator"
UPDATED = "AllocationV2.usp_get_final_effective_percentage.outputV2.orchestrator"
ENTRY = "run_final_effective_percentages"
TABLES = (
    ("FinalEffectivePercentages", "RunID"),
    ("FNFinalEffectivePercentages", "RunID"),
    ("SM_FinalEffectivePercentages", "RunID"),
)
INPUT_TABLES = []
EXTRA = {
    "Mode": 0,
    "ResultType": 'deltalake',
    "VolumePath": '/Volumes/qa7/datavolume/databrickdata',
}


def clear_modules(package):
    shared = "AllocationV2.plan_profiler"
    common = "Common_V2"
    for name in list(sys.modules):
        if (
            name == package
            or name.startswith(package + ".")
            or name == shared
            or name.startswith(shared + ".")
            or name == common
            or name.startswith(common + ".")
        ):
            del sys.modules[name]
    importlib.invalidate_caches()


sys.path[:] = [entry for entry in sys.path if entry != source_path]
sys.path.insert(0, source_path)
clear_modules(PACKAGE)
importlib.import_module("Common_V2.core.checkpoint_V2")
reconcile = importlib.import_module(f"{PACKAGE}.outputV2.output_reconcile")

required = (
    f"{PACKAGE}.outputV2.orchestrator",
    f"{PACKAGE}.outputV2.plan_profiler",
    f"{PACKAGE}.outputV2.output_reconcile",
)
for name in required:
    importlib.import_module(name)


def load_runner(module_name):
    clear_modules(PACKAGE)
    importlib.import_module("Common_V2.core.checkpoint_V2")
    try:
        importlib.import_module("AllocationV2.plan_profiler")
    except Exception as exc:
        print(f"[plan-profile] AllocationV2.plan_profiler not loaded: {exc}")
    return importlib.import_module(module_name)


def reported_seconds(result):
    parsed = result
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except (TypeError, ValueError):
            return None
    if not isinstance(parsed, dict):
        return None
    value = parsed.get("elapsed_seconds", parsed.get("elapsed"))
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def call_variant(module_name, updated):
    runner = load_runner(module_name)
    fn = getattr(runner, ENTRY)
    kwargs = dict(
        EntityID=entity_id,
        ClientID=client_id,
        TaxPeriodID=tax_period_id,
        RunID=run_id,
        CatalogName=catalog,
        SchemaName=schema,
        **EXTRA,
    )
    if updated:
        kwargs["ExecutionProfile"] = execution_profile
        if profile_plan:
            kwargs["profile_plan"] = True
            kwargs["ProfilePlan"] = True
    started = time.time()
    result = fn(spark, **kwargs)
    wall = round(time.time() - started, 3)
    hashes = reconcile.hash_tables(spark, catalog, schema, TABLES, run_id)
    rows = sum((row["rows"] or 0) for row in hashes.values() if row["present"])
    return result, wall, reported_seconds(result), hashes, rows


runtime_rows = []
parity_rows = []
for pass_no in range(1, number_of_runs + 1):
    print(f"[benchmark] pass {pass_no} execution order: original -> updated")
    snaps = reconcile.snapshot_tables(spark, catalog, schema, TABLES, run_id)
    try:
        reconcile.purge_run(spark, catalog, schema, TABLES, run_id)
        orig_result, orig_wall, orig_reported, orig_hash, orig_rows = call_variant(
            ORIGINAL, False
        )
        print(
            f"[benchmark] original: wall={orig_wall} reported={orig_reported} "
            f"rows={orig_rows}"
        )
        reconcile.purge_run(spark, catalog, schema, TABLES, run_id)
        upd_result, upd_wall, upd_reported, upd_hash, upd_rows = call_variant(
            UPDATED, True
        )
        print(
            f"[benchmark] updated: wall={upd_wall} reported={upd_reported} "
            f"rows={upd_rows}"
        )
        ok, report = reconcile.compare_hashes(orig_hash, upd_hash)
        for row in report:
            print(
                f"[reconcile] table={row['table']} rows={row['upd_rows']} "
                f"hash_sum={row['upd_hash_sum']} match={row['exact_match']}"
            )
            parity_rows.append({"pass": pass_no, **row})
        if not ok:
            failed = next(r["table"] for r in report if r["exact_match"] == "FAIL")
            raise RuntimeError(f"fingerprint mismatch on {failed}")
        print(f"[reconcile] PASS {pass_no}: all output fingerprints match")
        runtime_rows.append(
            {
                "pass": pass_no,
                "original_wall": orig_wall,
                "original_reported": orig_reported,
                "updated_wall": upd_wall,
                "updated_reported": upd_reported,
                "original_rows": orig_rows,
                "updated_rows": upd_rows,
            }
        )
    finally:
        reconcile.restore_tables(
            spark, catalog, schema, TABLES, run_id, snaps, INPUT_TABLES
        )
        reconcile.unpersist_snaps(snaps)

runtime_schema = (
    "pass int, original_wall double, original_reported double, "
    "updated_wall double, updated_reported double, "
    "original_rows long, updated_rows long"
)
parity_schema = (
    "pass int, table string, exact_match string, "
    "orig_rows long, upd_rows long, "
    "orig_hash_sum decimal(38,0), upd_hash_sum decimal(38,0)"
)
display(spark.createDataFrame(runtime_rows, schema=runtime_schema))
display(spark.createDataFrame(parity_rows, schema=parity_schema))

# COMMAND ----------

# MAGIC %md
# MAGIC Plan profile (BUILDER / CHECKPOINT / ACTION). Runs only when ProfilePlan is on.
# MAGIC Recommendations: add / keep / measure / collapse / remove.

# COMMAND ----------

if profile_plan:
    shim = importlib.import_module(f"{PACKAGE}.outputV2.plan_profiler")
    last = getattr(shim, "LAST_PLAN_PROFILE", {}) or {}
    try:
        shared = importlib.import_module("AllocationV2.plan_profiler")
        build = shared.build_plan_profile_display
        print("[plan-profile] using AllocationV2.plan_profiler")
    except Exception as exc:
        build = shim.build_plan_profile_display
        print(f"[plan-profile] shared profiler missing ({exc}); using SP shim")
    print("[plan-profile] BUILDER (add / measure / collapse)")
    display(build(spark, last.get("builder"), threshold=30, kind="builder"))
    print("[plan-profile] CHECKPOINT (keep / measure / remove)")
    display(build(spark, last.get("checkpoint"), threshold=30, kind="checkpoint"))
    print("[plan-profile] ACTION (add / measure)")
    display(build(spark, last.get("action"), threshold=30, kind="action"))
else:
    print("[plan-profile] skipped (ProfilePlan=off); no df.explain / measure_plan")
