# Databricks notebook source
# MAGIC %md
# MAGIC # Final Effective Percentage: production vs outputV3
# MAGIC
# MAGIC Alternating two-pass correctness benchmark. The recorded production
# MAGIC contract is 181.893s wall, 172.0s reported, 79 rows, and three tables.
# MAGIC Runtime acceptance must be based on this notebook, not local tests.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("number_of_runs", "2", "2. Number of passes")
dbutils.widgets.dropdown(
    "ExecutionOrder",
    "alternate",
    ["alternate", "production_first", "outputV3_first"],
    "3. Execution order",
)
dbutils.widgets.text("EntityID", "4137", "4. EntityID")
dbutils.widgets.text("ClientID", "15348", "5. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "6. TaxPeriodID")
dbutils.widgets.text("RunID", "17376", "7. RunID")
dbutils.widgets.text("CatalogName", "QA7", "8. Catalog")
dbutils.widgets.text("SchemaName", "iPC_2025_QA7_15348", "9. Schema")
dbutils.widgets.text("MaxThreads", "4", "10. Max threads")
dbutils.widgets.text("SqlShufflePartitions", "4", "11. Shuffle partitions")
dbutils.widgets.text(
    "ParallelGroups",
    "all",
    "12. Parallel groups (all, none, or comma-separated)",
)
dbutils.widgets.dropdown(
    "CheckpointMode", "1", ["1", "2", "3", "4"], "13. Checkpoint mode"
)
dbutils.widgets.dropdown(
    "ProfilePlan", "off", ["off", "on"], "14. Profile Spark plans"
)
dbutils.widgets.text(
    "PlanCheckpointThreshold", "30", "15. Plan checkpoint threshold"
)
dbutils.widgets.text("VolumePath", "", "16. Volume path (required for mode 3)")

source_path = dbutils.widgets.get("source_path").strip()
number_of_runs = int(dbutils.widgets.get("number_of_runs"))
execution_order = dbutils.widgets.get("ExecutionOrder").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
max_threads = int(dbutils.widgets.get("MaxThreads"))
shuffle_partitions = dbutils.widgets.get("SqlShufflePartitions").strip()
parallel_groups = dbutils.widgets.get("ParallelGroups").strip() or "all"
checkpoint_mode = int(dbutils.widgets.get("CheckpointMode"))
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
plan_checkpoint_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold")
)
volume_path = dbutils.widgets.get("VolumePath").strip()

if number_of_runs < 1:
    raise ValueError("number_of_runs must be >= 1")
if not 1 <= max_threads <= 4:
    raise ValueError("MaxThreads must be between 1 and 4")
if checkpoint_mode not in {1, 2, 3, 4}:
    raise ValueError("CheckpointMode must be one of 1, 2, 3, 4")
if checkpoint_mode == 3 and not volume_path:
    raise ValueError("VolumePath is required when CheckpointMode=3")
if shuffle_partitions:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

BASELINE = {
    "run_id": 17376,
    "entity_id": 4137,
    "modes": [1, 2, 3],
    "wall_seconds": 181.893,
    "reported_seconds": 172.0,
    "rows": 79,
    "tables": 3,
}

# COMMAND ----------

import importlib
import sys
import time

sys.path[:] = [entry for entry in sys.path if entry != source_path]
sys.path.insert(0, source_path)

PACKAGE = "AllocationV2.usp_get_final_effective_percentage"
PRODUCTION = f"{PACKAGE}.output.orchestrator"
OUTPUT_V3 = f"{PACKAGE}.outputV3.orchestrator"


def _evict():
    roots = (f"{PACKAGE}.output", f"{PACKAGE}.outputV3", "Common_V2")
    for name in list(sys.modules):
        if any(name == root or name.startswith(root + ".") for root in roots):
            del sys.modules[name]
    importlib.invalidate_caches()


def _fresh(module_name):
    _evict()
    module = importlib.import_module(module_name)
    print(f"[import] {module.__file__}")
    return module


_evict()
reconcile = importlib.import_module(f"{PACKAGE}.outputV3.output_reconcile")
capture_outputs = reconcile.capture_outputs
compare_outputs = reconcile.compare_outputs
create_run_snapshots = reconcile.create_run_snapshots
drop_run_snapshots = reconcile.drop_run_snapshots
purge_run = reconcile.purge_run
restore_run_snapshots = reconcile.restore_run_snapshots
summarize_outputs = reconcile.summarize_outputs
build_plan_profile_display = importlib.import_module(
    "AllocationV2.plan_profiler"
).build_plan_profile_display

# COMMAND ----------


def _run(variant, pass_number):
    runner = _fresh(PRODUCTION if variant == "production" else OUTPUT_V3)
    purge_run(spark, catalog, schema, run_id)
    kwargs = {
        "Mode": 0,
        "EntityID": entity_id,
        "ClientID": client_id,
        "TaxPeriodID": tax_period_id,
        "RunID": run_id,
        "CatalogName": catalog,
        "SchemaName": schema,
        "ResultType": "deltalake",
        "ExecutionID": f"fep-v3-ab-{pass_number}",
    }
    if variant == "outputV3":
        kwargs["MaxThreads"] = max_threads
        kwargs["ParallelGroups"] = parallel_groups
        kwargs["CheckpointMode"] = checkpoint_mode
        kwargs["ProfilePlan"] = profile_plan
        kwargs["PlanCheckpointThreshold"] = plan_checkpoint_threshold
        if volume_path:
            kwargs["VolumePath"] = volume_path
    started = time.time()
    result = runner.run_final_effective_percentages(spark, **kwargs)
    wall = round(time.time() - started, 3)
    fingerprints = capture_outputs(spark, catalog, schema, run_id)
    summary = summarize_outputs(fingerprints)
    profile = (
        runner.get_last_run_profile()
        if variant == "outputV3"
        else {}
    )
    reported = (
        float(result["elapsed_seconds"])
        if isinstance(result, dict) and result.get("elapsed_seconds") is not None
        else None
    )
    if reported is None and profile.get("updated_wall_seconds") is not None:
        reported = float(profile["updated_wall_seconds"])
    print(
        f"[benchmark] pass={pass_number} variant={variant} "
        f"wall={wall:.3f}s reported={reported} "
        f"rows={summary['total_rows']} tables={summary['tables_present']}"
    )
    return {
        "pass": pass_number,
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "rows": summary["total_rows"],
        "tables": summary["tables_present"],
        "fingerprints": fingerprints,
        "profile": profile,
    }


def _order(pass_number):
    if execution_order == "production_first":
        return ("production", "outputV3")
    if execution_order == "outputV3_first":
        return ("outputV3", "production")
    return (
        ("production", "outputV3")
        if pass_number % 2
        else ("outputV3", "production")
    )


records = []
fingerprint_rows = []
snapshots = create_run_snapshots(spark, catalog, schema, run_id)
try:
    for pass_number in range(1, number_of_runs + 1):
        order = _order(pass_number)
        print(f"[benchmark] pass={pass_number} order={' -> '.join(order)}")
        by_variant = {
            variant: _run(variant, pass_number) for variant in order
        }
        records.extend(by_variant.values())
        production = by_variant["production"]
        candidate = by_variant["outputV3"]
        mismatches = compare_outputs(
            production["fingerprints"], candidate["fingerprints"]
        )
        for table in reconcile.OUTPUT_TABLES:
            fingerprint_rows.append(
                {
                    "pass": pass_number,
                    "table": table,
                    "exact_match": not any(
                        item["table"] == table for item in mismatches
                    ),
                    "production_fingerprint": str(
                        production["fingerprints"].get(table)
                    ),
                    "outputV3_fingerprint": str(
                        candidate["fingerprints"].get(table)
                    ),
                }
            )
        if production["rows"] != BASELINE["rows"]:
            raise AssertionError(
                f"Production rows changed: expected 79, got {production['rows']}"
            )
        if production["tables"] != BASELINE["tables"]:
            raise AssertionError(
                "Production must write exactly three output tables"
            )
        if mismatches:
            raise AssertionError(f"Exact fingerprint mismatch: {mismatches[0]}")
        wall_delta = production["wall_seconds"] - candidate["wall_seconds"]
        improvement = (
            100.0 * wall_delta / production["wall_seconds"]
            if production["wall_seconds"]
            else 0.0
        )
        print(
            f"[reconcile] PASS {pass_number}: exact fingerprints match; "
            f"production={production['wall_seconds']:.3f}s "
            f"outputV3={candidate['wall_seconds']:.3f}s "
            f"delta={wall_delta:.3f}s improvement={improvement:.2f}%"
        )
finally:
    try:
        restore_run_snapshots(spark, catalog, schema, run_id, snapshots)
    except Exception:
        print(f"[restore] failed; retained snapshots={snapshots}")
        raise
    else:
        drop_run_snapshots(spark, catalog, schema, snapshots)

# COMMAND ----------

summary_rows = [
    {
        "pass": row["pass"],
        "variant": row["variant"],
        "wall_seconds": row["wall_seconds"],
        "reported_seconds": row["reported_seconds"],
        "rows": row["rows"],
        "tables": row["tables"],
        "baseline_wall_seconds": BASELINE["wall_seconds"],
        "baseline_reported_seconds": BASELINE["reported_seconds"],
        "wall_vs_baseline_seconds": round(
            row["wall_seconds"] - BASELINE["wall_seconds"], 3
        ),
    }
    for row in records
]
SUMMARY_SCHEMA = """
    pass INT,
    variant STRING,
    wall_seconds DOUBLE,
    reported_seconds DOUBLE,
    rows LONG,
    tables LONG,
    baseline_wall_seconds DOUBLE,
    baseline_reported_seconds DOUBLE,
    wall_vs_baseline_seconds DOUBLE
"""
FINGERPRINT_SCHEMA = """
    pass INT,
    table STRING,
    exact_match BOOLEAN,
    production_fingerprint STRING,
    outputV3_fingerprint STRING
"""
display(
    spark.createDataFrame(summary_rows, SUMMARY_SCHEMA).orderBy(
        "pass", "variant"
    )
)
display(
    spark.createDataFrame(fingerprint_rows, FINGERPRINT_SCHEMA).orderBy(
        "pass", "table"
    )
)

stage_rows = [
    {"pass": row["pass"], **item}
    for row in records
    if row["variant"] == "outputV3"
    for item in row["profile"].get("stage_timings", [])
]
checkpoint_rows = [
    {"pass": row["pass"], **item}
    for row in records
    if row["variant"] == "outputV3"
    for item in row["profile"].get("checkpoint_activity", [])
]
parallel_rows = [
    {"pass": row["pass"], **item}
    for row in records
    if row["variant"] == "outputV3"
    for item in row["profile"].get("parallel_activity", [])
]
strategy_rows = [
    {
        "pass": row["pass"],
        "execution_strategy": row["profile"].get("execution_strategy"),
        "pipeline_strategy": row["profile"].get("pipeline_strategy"),
        "branch_strategy": row["profile"].get("branch_strategy"),
        "pass_a_strategy": row["profile"].get("pass_a_strategy"),
        "output_build_strategy": row["profile"].get(
            "output_build_strategy"
        ),
        "effective_max_threads": row["profile"].get(
            "effective_max_threads"
        ),
        "enabled_parallel_groups": ",".join(
            row["profile"].get("enabled_parallel_groups", [])
        ),
    }
    for row in records
    if row["variant"] == "outputV3"
]
artifact_rows = [
    {"pass": row["pass"], **item}
    for row in records
    if row["variant"] == "outputV3"
    for item in row["profile"].get("artifact_merges", [])
]
if stage_rows:
    display(
        spark.createDataFrame(
            stage_rows,
            "pass INT, stage STRING, calls LONG, elapsed_seconds DOUBLE",
        ).orderBy("pass", "stage")
    )
if checkpoint_rows:
    display(
        spark.createDataFrame(
            checkpoint_rows,
            """
            pass INT,
            name STRING,
            stage STRING,
            checkpoint_mode INT,
            policy_backend STRING,
            actual_backend STRING,
            reason STRING,
            elapsed_seconds DOUBLE
            """,
        ).orderBy("pass", "name")
    )
if parallel_rows:
    display(
        spark.createDataFrame(
            parallel_rows,
            """
            pass INT,
            group STRING,
            task STRING,
            status STRING,
            elapsed_seconds DOUBLE,
            thread STRING
            """,
        ).orderBy("pass", "group", "task")
    )
if strategy_rows:
    display(
        spark.createDataFrame(
            strategy_rows,
            """
            pass INT,
            execution_strategy STRING,
            pipeline_strategy STRING,
            branch_strategy STRING,
            pass_a_strategy STRING,
            output_build_strategy STRING,
            effective_max_threads INT,
            enabled_parallel_groups STRING
            """,
        ).orderBy("pass")
    )
if artifact_rows:
    display(
        spark.createDataFrame(
            artifact_rows,
            """
            pass INT,
            artifact STRING,
            producers ARRAY<INT>,
            winner_mode INT,
            conflict_check STRING
            """,
        ).orderBy("pass", "artifact")
    )

for row in records:
    if row["variant"] != "outputV3":
        continue
    for label, key, kind in (
        ("builder", "plan_profile", "builder"),
        ("checkpoint", "checkpoint_plan_profile", "checkpoint"),
        ("action", "action_profile", "action"),
    ):
        plan_rows = row["profile"].get(key, [])
        if plan_rows:
            print(f"[plan profile] pass={row['pass']} kind={label}")
            display(
                build_plan_profile_display(
                    spark,
                    plan_rows,
                    threshold=plan_checkpoint_threshold,
                    kind=kind,
                )
            )
