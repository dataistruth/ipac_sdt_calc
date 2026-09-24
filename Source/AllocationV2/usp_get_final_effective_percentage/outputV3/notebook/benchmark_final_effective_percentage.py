# Databricks notebook source
# MAGIC %md
# MAGIC # Final Effective Percentage: one side-by-side run
# MAGIC
# MAGIC Runs production once and outputV3 once, verifies exact output parity,
# MAGIC restores the RunID snapshots, and displays runtime, parity, and
# MAGIC checkpoint timing tables. Progress is logged with wall-clock times.

# COMMAND ----------

dbutils.widgets.removeAll()
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
dbutils.widgets.text("MaxThreads", "4", "8. Max threads")
dbutils.widgets.text("SqlShufflePartitions", "8", "9. Shuffle partitions")
dbutils.widgets.dropdown(
    "CheckpointMode", "5", ["1", "2", "4", "5"], "10. Checkpoint mode"
)
dbutils.widgets.dropdown(
    "MissingEntityIdentity", "on", ["off", "on"], "11. Missing identity"
)
dbutils.widgets.text("Passes", "1", "12. Passes")

source_path = dbutils.widgets.get("source_path").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
max_threads = int(dbutils.widgets.get("MaxThreads"))
shuffle_partitions = int(dbutils.widgets.get("SqlShufflePartitions"))
checkpoint_mode = int(dbutils.widgets.get("CheckpointMode"))
missing_entity_identity = (
    dbutils.widgets.get("MissingEntityIdentity").strip().lower() == "on"
)
passes = int(dbutils.widgets.get("Passes"))

if not 1 <= max_threads <= 4:
    raise ValueError("MaxThreads must be between 1 and 4")
if checkpoint_mode not in {1, 2, 4, 5}:
    raise ValueError("CheckpointMode must be one of 1, 2, 4, 5")
if shuffle_partitions < 1:
    raise ValueError("SqlShufflePartitions must be >= 1")
if passes < 1:
    raise ValueError("Passes must be >= 1")

BASELINE = {
    "run_id": 17376,
    "entity_id": 4137,
    "modes": [1, 2, 3],
    "wall_seconds": 181.893,
    "reported_seconds": 172.0,
    "rows": 79,
    "tables": 3,
}
ORIGINAL_SPARK_CONFIG = {
    key: spark.conf.get(key)
    for key in (
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    )
}

# COMMAND ----------

import importlib
import sys
import time

PARALLEL_GROUPS = "all"

settings = {
    "source_path": source_path,
    "entity_id": entity_id,
    "client_id": client_id,
    "tax_period_id": tax_period_id,
    "run_id": run_id,
    "catalog": catalog,
    "schema": schema,
    "business_modes": [1, 2, 3],
    "max_threads": max_threads,
    "sql_shuffle_partitions": shuffle_partitions,
    "parallel_groups": PARALLEL_GROUPS,
    "checkpoint_mode": checkpoint_mode,
    "checkpoint_materialization": (
        "local/deferred" if checkpoint_mode == 5 else "configured by mode"
    ),
    "missing_entity_identity": missing_entity_identity,
    "passes": passes,
}
display(
    spark.createDataFrame(
        [
            {"setting": key, "value": str(value)}
            for key, value in settings.items()
        ]
    )
)

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
    return importlib.import_module(module_name)


_evict()
reconcile = importlib.import_module(f"{PACKAGE}.outputV3.output_reconcile")
capture_outputs = reconcile.capture_outputs
compare_outputs = reconcile.compare_outputs
create_run_snapshots = reconcile.create_run_snapshots
drop_run_snapshots = reconcile.drop_run_snapshots
purge_run = reconcile.purge_run
restore_run_snapshots = reconcile.restore_run_snapshots
summarize_outputs = reconcile.summarize_outputs

# COMMAND ----------


def _run(variant):
    is_production = variant == "production"
    variant_shuffle = 4 if is_production else shuffle_partitions
    spark.conf.set(
        "spark.sql.shuffle.partitions", str(variant_shuffle)
    )
    shuffle_before_run = spark.conf.get("spark.sql.shuffle.partitions")
    if is_production:
        spark.conf.set(
            "spark.sql.adaptive.advisoryPartitionSizeInBytes",
            ORIGINAL_SPARK_CONFIG[
                "spark.sql.adaptive.advisoryPartitionSizeInBytes"
            ],
        )
    runner = _fresh(PRODUCTION if is_production else OUTPUT_V3)
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
        "ExecutionID": f"fep-v3-side-by-side-{variant}",
    }
    if not is_production:
        kwargs.update(
            {
                "MaxThreads": max_threads,
                "ParallelGroups": PARALLEL_GROUPS,
                "CheckpointMode": checkpoint_mode,
                "SqlShufflePartitions": shuffle_partitions,
                "MissingEntityIdentity": missing_entity_identity,
            }
        )
    started = time.time()
    result = runner.run_final_effective_percentages(spark, **kwargs)
    wall = round(time.time() - started, 3)
    fingerprints = capture_outputs(spark, catalog, schema, run_id)
    summary = summarize_outputs(fingerprints)
    profile = runner.get_last_run_profile() if not is_production else {}
    shuffle_after_run = spark.conf.get("spark.sql.shuffle.partitions")
    profile_requested_shuffle = (
        profile.get("requested_shuffle_partitions")
        if not is_production
        else variant_shuffle
    )
    profile_effective_shuffle = (
        profile.get("effective_spark_config", {}).get(
            "spark.sql.shuffle.partitions"
        )
        if not is_production
        else shuffle_after_run
    )
    if not is_production and profile_requested_shuffle is None:
        raise RuntimeError(
            "outputV3 profile has no requested_shuffle_partitions; "
            "an old orchestrator is still loaded"
        )
    if not is_production and profile_effective_shuffle is None:
        raise RuntimeError(
            "outputV3 profile has no effective shuffle value; "
            "an old pipeline/orchestrator is still loaded"
        )
    shuffle_matches = (
        int(shuffle_after_run) == int(variant_shuffle)
        and int(profile_requested_shuffle) == int(variant_shuffle)
        and int(profile_effective_shuffle) == int(variant_shuffle)
    )
    verification_required = not is_production
    shuffle_status = (
        "PASS"
        if shuffle_matches
        else (
            "OBSERVED_PRODUCTION_OVERRIDE"
            if is_production
            else "FAIL"
        )
    )
    if verification_required and not shuffle_matches:
        raise AssertionError(
            "outputV3 spark.sql.shuffle.partitions was overwritten: "
            f"requested={variant_shuffle}, session_after={shuffle_after_run}, "
            f"profile_requested={profile_requested_shuffle}, "
            f"profile_effective={profile_effective_shuffle}"
        )
    reported = (
        float(result["elapsed_seconds"])
        if isinstance(result, dict)
        and result.get("elapsed_seconds") is not None
        else None
    )
    if reported is None and profile.get("updated_wall_seconds") is not None:
        reported = float(profile["updated_wall_seconds"])
    record = {
        "variant": variant,
        "wall_seconds": wall,
        "reported_seconds": reported,
        "rows": summary["total_rows"],
        "tables": summary["tables_present"],
        "requested_shuffle_partitions": variant_shuffle,
        "session_shuffle_before": shuffle_before_run,
        "session_shuffle_after": shuffle_after_run,
        "profile_requested_shuffle": profile_requested_shuffle,
        "profile_effective_shuffle": profile_effective_shuffle,
        "shuffle_verified": shuffle_matches,
        "shuffle_verification_required": verification_required,
        "shuffle_status": shuffle_status,
        "fingerprints": fingerprints,
        "profile": profile,
    }
    return record


# COMMAND ----------

# Run production then outputV3 for each pass. Results are shown in the tables
# below; per-pass runtime rows are collected here.
production = None
optimized = None
mismatches = []
final_comparison = None
runtime_rows = []
snapshots = create_run_snapshots(spark, catalog, schema, run_id)
try:
    for pass_index in range(1, passes + 1):
        production = _run("production")
        optimized = _run("outputV3")

        if production["rows"] != BASELINE["rows"]:
            raise AssertionError(
                f"pass {pass_index}: production rows changed: "
                f"expected 79, got {production['rows']}"
            )
        if production["tables"] != BASELINE["tables"]:
            raise AssertionError(
                f"pass {pass_index}: production must write three tables"
            )
        mismatches = compare_outputs(
            production["fingerprints"], optimized["fingerprints"]
        )
        if mismatches:
            raise AssertionError(
                f"pass {pass_index}: exact fingerprint mismatch: "
                f"{mismatches[0]}"
            )

        wall_delta = (
            production["wall_seconds"] - optimized["wall_seconds"]
        )
        improvement = (
            100.0 * wall_delta / production["wall_seconds"]
            if production["wall_seconds"]
            else 0.0
        )
        final_comparison = {
            "parity": "PASS",
            "production_wall_seconds": production["wall_seconds"],
            "production_reported_seconds": production["reported_seconds"],
            "optimized_wall_seconds": optimized["wall_seconds"],
            "optimized_reported_seconds": optimized["reported_seconds"],
            "improvement_seconds": round(wall_delta, 3),
            "improvement_percent": round(improvement, 2),
            "optimized_under_50_seconds": optimized["wall_seconds"] < 50.0,
            "optimized_under_55_seconds": optimized["wall_seconds"] <= 55.0,
            "optimized_under_80_seconds": optimized["wall_seconds"] <= 80.0,
            "requested_shuffle_partitions": optimized[
                "requested_shuffle_partitions"
            ],
            "effective_shuffle_partitions": optimized[
                "profile_effective_shuffle"
            ],
            "shuffle_verified": optimized["shuffle_verified"],
            "rows": optimized["rows"],
            "tables": optimized["tables"],
        }
        for record in (production, optimized):
            runtime_rows.append(
                {
                    "pass": pass_index,
                    "variant": record["variant"],
                    "wall_seconds": record["wall_seconds"],
                    "reported_seconds": record["reported_seconds"],
                    "rows": record["rows"],
                    "tables": record["tables"],
                    "effective_shuffle_partitions": str(
                        record["profile_effective_shuffle"]
                    ),
                    "shuffle_verified": record["shuffle_verified"],
                    "baseline_wall_seconds": BASELINE["wall_seconds"],
                    "wall_vs_baseline_seconds": round(
                        record["wall_seconds"] - BASELINE["wall_seconds"], 3
                    ),
                }
            )
        runtime_rows.append(
            {
                "pass": pass_index,
                "variant": "improvement (production - outputV3)",
                "wall_seconds": final_comparison["improvement_seconds"],
                "reported_seconds": final_comparison["improvement_percent"],
                "rows": final_comparison["rows"],
                "tables": final_comparison["tables"],
                "effective_shuffle_partitions": str(
                    optimized["profile_effective_shuffle"]
                ),
                "shuffle_verified": optimized["shuffle_verified"],
                "baseline_wall_seconds": BASELINE["wall_seconds"],
                "wall_vs_baseline_seconds": round(
                    optimized["wall_seconds"] - BASELINE["wall_seconds"], 3
                ),
            }
        )
finally:
    try:
        restore_run_snapshots(spark, catalog, schema, run_id, snapshots)
    except Exception:
        raise
    else:
        drop_run_snapshots(spark, catalog, schema, snapshots)
    finally:
        for key, value in ORIGINAL_SPARK_CONFIG.items():
            spark.conf.set(key, value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime and data-compare tables
# MAGIC
# MAGIC The tables below show runtime, exact output parity, and checkpoint
# MAGIC registration details without replaying JSON events to stdout.

# COMMAND ----------

RUNTIME_SCHEMA = """
    pass INT,
    variant STRING,
    wall_seconds DOUBLE,
    reported_seconds DOUBLE,
    rows LONG,
    tables LONG,
    effective_shuffle_partitions STRING,
    shuffle_verified BOOLEAN,
    baseline_wall_seconds DOUBLE,
    wall_vs_baseline_seconds DOUBLE
"""
if runtime_rows:
    display(
        spark.createDataFrame(runtime_rows, RUNTIME_SCHEMA).orderBy(
            "pass", "variant"
        )
    )

# COMMAND ----------

compare_rows = []
if production is not None and optimized is not None:
    mismatch_by_table = {}
    for item in mismatches:
        mismatch_by_table.setdefault(item["table"], []).append(item)
    for table in reconcile.OUTPUT_TABLES:
        table_mismatches = mismatch_by_table.get(table, [])
        compare_rows.append(
            {
                "table": table,
                "exact_match": not table_mismatches,
                "production_fingerprint": str(
                    production["fingerprints"].get(table)
                ),
                "outputV3_fingerprint": str(
                    optimized["fingerprints"].get(table)
                ),
                "mismatch_detail": (
                    str(table_mismatches[0]) if table_mismatches else None
                ),
            }
        )

COMPARE_SCHEMA = """
    table STRING,
    exact_match BOOLEAN,
    production_fingerprint STRING,
    outputV3_fingerprint STRING,
    mismatch_detail STRING
"""
if compare_rows:
    display(spark.createDataFrame(compare_rows, COMPARE_SCHEMA))

# COMMAND ----------

checkpoint_timing_rows = []
if optimized is not None:
    for row in sorted(
        optimized["profile"].get("checkpoint_activity", []),
        key=lambda item: str(item.get("started_at") or ""),
    ):
        checkpoint_timing_rows.append(
            {
                "name": row.get("name"),
                "stage": row.get("stage"),
                "thread": row.get("thread"),
                "checkpoint_mode": row.get("checkpoint_mode"),
                "actual_backend": row.get("actual_backend"),
                "materialization": row.get("materialization"),
                "timing_scope": (
                    "registration only"
                    if row.get("materialization") == "deferred"
                    else "materialization"
                ),
                "started_at": row.get("started_at"),
                "ended_at": row.get("ended_at"),
                "elapsed_seconds": row.get("elapsed_seconds"),
            }
        )

CHECKPOINT_TIMING_SCHEMA = """
    name STRING,
    stage STRING,
    thread STRING,
    checkpoint_mode INT,
    actual_backend STRING,
    materialization STRING,
    timing_scope STRING,
    started_at STRING,
    ended_at STRING,
    elapsed_seconds DOUBLE
"""
if checkpoint_timing_rows:
    display(
        spark.createDataFrame(
            checkpoint_timing_rows, CHECKPOINT_TIMING_SCHEMA
        ).orderBy("started_at")
    )
