# Databricks notebook source
# MAGIC %md
# MAGIC # Final Effective Percentage: one side-by-side run
# MAGIC
# MAGIC Runs production once and outputV2 once, verifies exact output parity,
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
dbutils.widgets.dropdown(
    "ExecutionProfile",
    "low",
    ["low", "medium", "big"],
    "8. Execution profile",
)
dbutils.widgets.text("MaxThreads", "", "9. Max threads (blank=profile)")
dbutils.widgets.text(
    "SqlShufflePartitions", "", "10. Shuffle partitions (blank=profile)"
)
dbutils.widgets.dropdown(
    "CheckpointMode",
    "default",
    ["default", "1", "2", "4", "5"],
    "11. Checkpoint mode (blank=profile)",
)
dbutils.widgets.dropdown(
    "MissingEntityIdentity", "on", ["off", "on"], "12. Missing identity"
)
dbutils.widgets.text("Passes", "1", "13. Passes")

source_path = dbutils.widgets.get("source_path").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
execution_profile = (
    dbutils.widgets.get("ExecutionProfile").strip() or "low"
)
max_threads_raw = dbutils.widgets.get("MaxThreads").strip()
max_threads = int(max_threads_raw) if max_threads_raw else None
shuffle_raw = dbutils.widgets.get("SqlShufflePartitions").strip()
shuffle_partitions = int(shuffle_raw) if shuffle_raw else None
checkpoint_raw = dbutils.widgets.get("CheckpointMode").strip().lower()
checkpoint_mode = (
    None
    if checkpoint_raw in {"", "default"}
    else int(checkpoint_raw)
)
missing_entity_identity = (
    dbutils.widgets.get("MissingEntityIdentity").strip().lower() == "on"
)
passes = int(dbutils.widgets.get("Passes"))

if max_threads is not None and not 1 <= max_threads <= 4:
    raise ValueError("MaxThreads must be between 1 and 4")
if checkpoint_mode is not None and checkpoint_mode not in {1, 2, 4, 5}:
    raise ValueError("CheckpointMode must be one of 1, 2, 4, 5")
if shuffle_partitions is not None and shuffle_partitions < 1:
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
import inspect
import sys
import time

PARALLEL_GROUPS = "all"
PROMOTED_OUTPUT_V3_KWARGS = {
    "CpbtInputBreak": "both",
    "ParallelEffective": True,
    "BusinessOptimization": "broadcast_entity_partners",
    "CpbtPostTagEntityBreak": False,
    "ParallelCpbtPostTag": True,
    "CompactAllEntities": True,
    "CpbtNarrowAntiKeys": True,
    "CpbtTransferPrefilter": True,
    "CpbtDropTrackingMatch": True,
    "BatchFootnoteLineIds": True,
    "FootnoteSharedLineage": False,
    "FootnoteCheckpointPartitions": 4,
    "BroadcastCpbtRemaining": True,
    "HierarchyMaterialize": True,
    "ParallelCpbtValidate": True,
    "SkipYearlyEmptyProbe": False,
    "CollapseStatePasses": True,
    "BatchStateWorkflowLookup": True,
    "SinglePickupAntiJoin": True,
    "MaterializeEffectiveInputs": True,
}
CPBT_INPUT_BREAK = PROMOTED_OUTPUT_V3_KWARGS["CpbtInputBreak"]
PARALLEL_EFFECTIVE = PROMOTED_OUTPUT_V3_KWARGS["ParallelEffective"]
BUSINESS_OPTIMIZATION = PROMOTED_OUTPUT_V3_KWARGS["BusinessOptimization"]

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
    "cpbt_input_break": CPBT_INPUT_BREAK,
    "parallel_effective": PARALLEL_EFFECTIVE,
    "business_optimization": BUSINESS_OPTIMIZATION,
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
OUTPUT_V3 = f"{PACKAGE}.outputV2.orchestrator"


def _evict():
    roots = (f"{PACKAGE}.output", f"{PACKAGE}.outputV2", "Common_V2")
    for name in list(sys.modules):
        if any(name == root or name.startswith(root + ".") for root in roots):
            del sys.modules[name]
    importlib.invalidate_caches()


def _fresh(module_name):
    _evict()
    return importlib.import_module(module_name)


def _require_current_optimized_sources():
    """Fail fast when outputV2 was synced without its optimized business copies.

    All optimizations live under ``outputV2/business`` now, so only outputV2
    needs to be synchronized. Production ``output`` stays pristine.
    """
    cost_pct = importlib.import_module(
        f"{PACKAGE}.outputV2.business.cost_pct_loader"
    )
    state = importlib.import_module(
        f"{PACKAGE}.outputV2.business.state_allocation"
    )
    footnotes = importlib.import_module(
        f"{PACKAGE}.outputV2.business.pfic_footnotes"
    )
    requirements = {
        "business.cost_pct_loader.build_cost_percentage_by_type": (
            cost_pct.build_cost_percentage_by_type,
            {"checkpoint_group_fn"},
        ),
        "business.state_allocation.build_state_allocation_input": (
            state.build_state_allocation_input,
            {
                "collapse_state_passes",
                "batch_state_workflow_lookup",
            },
        ),
        "business.pfic_footnotes.build_footnote_input_lines": (
            footnotes.build_footnote_input_lines,
            {"checkpoint_fn"},
        ),
    }
    missing = []
    for helper_name, (helper, expected) in requirements.items():
        actual = set(inspect.signature(helper).parameters)
        absent = sorted(expected - actual)
        if absent:
            missing.append(f"{helper_name}: {', '.join(absent)}")
    hierarchy = importlib.import_module(
        f"{PACKAGE}.outputV2.business.entity_hierarchy"
    )
    if "_output_v3_hierarchy_materialize" not in inspect.getsource(
        hierarchy.build_entity_hierarchy
    ):
        missing.append(
            "business.entity_hierarchy.build_entity_hierarchy: "
            "_output_v3_hierarchy_materialize"
        )
    if "_output_v3_broadcast_cpbt_remaining" not in inspect.getsource(
        cost_pct.build_cost_percentage_by_type
    ):
        missing.append(
            "business.cost_pct_loader.build_cost_percentage_by_type: "
            "_output_v3_broadcast_cpbt_remaining"
        )
    if missing:
        raise RuntimeError(
            "Stale or partially synchronized outputV2. Sync outputV2/ "
            "(including outputV2/business) before benchmarking. Missing "
            "seams: " + "; ".join(missing)
        )


_evict()
reconcile = importlib.import_module(f"{PACKAGE}.outputV2.output_reconcile")
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
    if shuffle_partitions is not None:
        spark.conf.set(
            "spark.sql.shuffle.partitions", str(shuffle_partitions)
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
    if not is_production:
        _require_current_optimized_sources()
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
        "ExecutionID": f"fep-v2-side-by-side-{variant}",
    }
    if not is_production:
        kwargs.update(
            {
                "ExecutionProfile": execution_profile,
                "ParallelGroups": PARALLEL_GROUPS,
                "MissingEntityIdentity": missing_entity_identity,
                **PROMOTED_OUTPUT_V3_KWARGS,
            }
        )
        if max_threads is not None:
            kwargs["MaxThreads"] = max_threads
        if checkpoint_mode is not None:
            kwargs["CheckpointMode"] = checkpoint_mode
        if shuffle_partitions is not None:
            kwargs["SqlShufflePartitions"] = shuffle_partitions
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
        else shuffle_after_run
    )
    profile_effective_shuffle = (
        profile.get("effective_spark_config", {}).get(
            "spark.sql.shuffle.partitions"
        )
        if not is_production
        else shuffle_after_run
    )
    expected_shuffle = (
        shuffle_partitions
        if shuffle_partitions is not None
        else profile_requested_shuffle
    )
    if not is_production and profile_requested_shuffle is None:
        raise RuntimeError(
            "outputV2 profile has no requested_shuffle_partitions; "
            "an old orchestrator is still loaded"
        )
    if not is_production and profile_effective_shuffle is None:
        raise RuntimeError(
            "outputV2 profile has no effective shuffle value; "
            "an old pipeline/orchestrator is still loaded"
        )
    shuffle_matches = (
        expected_shuffle is not None
        and int(shuffle_after_run) == int(expected_shuffle)
        and int(profile_requested_shuffle) == int(expected_shuffle)
        and int(profile_effective_shuffle) == int(expected_shuffle)
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
            "outputV2 spark.sql.shuffle.partitions was overwritten: "
            f"requested={expected_shuffle}, session_after={shuffle_after_run}, "
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
        "requested_shuffle_partitions": expected_shuffle,
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

# Run production then outputV2 for each pass. Results are shown in the tables
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
        optimized = _run("outputV2")

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
                "variant": "improvement (production - outputV2)",
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
                "outputV2_fingerprint": str(
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
    outputV2_fingerprint STRING,
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
