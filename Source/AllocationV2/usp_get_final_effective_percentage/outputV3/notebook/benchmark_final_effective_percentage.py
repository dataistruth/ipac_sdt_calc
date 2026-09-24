# Databricks notebook source
# MAGIC %md
# MAGIC # Final Effective Percentage: one side-by-side run
# MAGIC
# MAGIC Runs production once and outputV3 once, verifies exact output parity,
# MAGIC restores the RunID snapshots, and prints all diagnostics to stdout as
# MAGIC copyable JSON lines. No table output is produced.

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
dbutils.widgets.text("SqlShufflePartitions", "32", "9. Shuffle partitions")
dbutils.widgets.text(
    "ParallelGroups",
    "all",
    "10. Parallel groups (all, none, or comma-separated)",
)
dbutils.widgets.dropdown(
    "CheckpointMode", "4", ["1", "2", "3", "4"], "11. Checkpoint mode"
)
dbutils.widgets.dropdown(
    "ProfilePlan", "off", ["off", "on"], "12. Profile Spark plans"
)
dbutils.widgets.text(
    "PlanCheckpointThreshold", "30", "13. Plan checkpoint threshold"
)
dbutils.widgets.text("VolumePath", "", "14. Volume path (mode 3 only)")
dbutils.widgets.text("ExperimentID", "baseline", "15. Experiment identifier")
dbutils.widgets.dropdown(
    "WarningProbeRemoval",
    "off",
    ["off", "line_items", "quarters", "lookthrough"],
    "16. Remove one warning-only probe",
)
dbutils.widgets.dropdown(
    "MissingEntityIdentity", "off", ["off", "on"], "17. Missing identity"
)
dbutils.widgets.dropdown(
    "CpbtInputBreak",
    "off",
    ["off", "non_dated", "dated", "both"],
    "18. CPBT input break",
)
dbutils.widgets.text("TargetCheckpoint", "", "19. Target checkpoint")
dbutils.widgets.dropdown(
    "TargetPartitionStrategy",
    "off",
    ["off", "coalesce", "repartition"],
    "20. Target partition strategy",
)
dbutils.widgets.text("TargetPartitions", "0", "21. Target partitions")
dbutils.widgets.text(
    "TargetPartitionKeys", "", "22. Repartition keys (comma-separated)"
)
dbutils.widgets.dropdown(
    "BusinessOptimization",
    "off",
    ["off", "broadcast_entity_partners"],
    "23. Isolated business optimization",
)

source_path = dbutils.widgets.get("source_path").strip()
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
catalog = dbutils.widgets.get("CatalogName").strip()
schema = dbutils.widgets.get("SchemaName").strip()
max_threads = int(dbutils.widgets.get("MaxThreads"))
shuffle_partitions = int(dbutils.widgets.get("SqlShufflePartitions"))
parallel_groups = dbutils.widgets.get("ParallelGroups").strip() or "all"
checkpoint_mode = int(dbutils.widgets.get("CheckpointMode"))
profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
plan_checkpoint_threshold = int(
    dbutils.widgets.get("PlanCheckpointThreshold")
)
volume_path = dbutils.widgets.get("VolumePath").strip()
experiment_id = dbutils.widgets.get("ExperimentID").strip() or "baseline"
warning_probe_removal = dbutils.widgets.get("WarningProbeRemoval").strip()
missing_entity_identity = (
    dbutils.widgets.get("MissingEntityIdentity").strip().lower() == "on"
)
cpbt_input_break = dbutils.widgets.get("CpbtInputBreak").strip()
target_checkpoint = dbutils.widgets.get("TargetCheckpoint").strip()
target_partition_strategy = dbutils.widgets.get(
    "TargetPartitionStrategy"
).strip()
target_partitions = int(dbutils.widgets.get("TargetPartitions"))
target_partition_keys = dbutils.widgets.get("TargetPartitionKeys").strip()
business_optimization = dbutils.widgets.get(
    "BusinessOptimization"
).strip()

if not 1 <= max_threads <= 4:
    raise ValueError("MaxThreads must be between 1 and 4")
if checkpoint_mode not in {1, 2, 3, 4}:
    raise ValueError("CheckpointMode must be one of 1, 2, 3, 4")
if checkpoint_mode == 3 and not volume_path:
    raise ValueError("VolumePath is required when CheckpointMode=3")
if shuffle_partitions < 1:
    raise ValueError("SqlShufflePartitions must be >= 1")

active_experiments = [
    name
    for name, enabled in (
        ("shuffle_partitions", shuffle_partitions != 32),
        ("warning_probe", warning_probe_removal != "off"),
        ("missing_entity_identity", missing_entity_identity),
        ("cpbt_input_break", cpbt_input_break != "off"),
        ("target_partition", target_partition_strategy != "off"),
        ("business_optimization", business_optimization != "off"),
    )
    if enabled
]
if len(active_experiments) > 1:
    raise ValueError(
        f"Run one experiment at a time; active={active_experiments}"
    )
if experiment_id == "baseline" and active_experiments:
    raise ValueError(
        "Set a non-baseline ExperimentID for an experimental run"
    )
if experiment_id != "baseline" and len(active_experiments) != 1:
    raise ValueError(
        "A non-baseline ExperimentID must select exactly one experiment"
    )

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
import json
import sys
import time
from datetime import datetime


def _emit(section, **values):
    print(
        f"[FEP_BENCHMARK][{section}] "
        + json.dumps(values, default=str, sort_keys=True),
        flush=True,
    )


def _emit_rows(section, rows, **common):
    for index, row in enumerate(rows, start=1):
        _emit(section, index=index, **common, **dict(row))


_emit(
    "SETTINGS",
    timestamp=datetime.now().isoformat(),
    source_path=source_path,
    entity_id=entity_id,
    client_id=client_id,
    tax_period_id=tax_period_id,
    run_id=run_id,
    catalog=catalog,
    schema=schema,
    mode=0,
    business_modes=[1, 2, 3],
    max_threads=max_threads,
    sql_shuffle_partitions=shuffle_partitions,
    parallel_groups=parallel_groups,
    checkpoint_mode=checkpoint_mode,
    profile_plan=profile_plan,
    plan_checkpoint_threshold=plan_checkpoint_threshold,
    volume_path=volume_path or None,
    experiment_id=experiment_id,
    active_experiments=active_experiments,
    warning_probe_removal=warning_probe_removal,
    missing_entity_identity=missing_entity_identity,
    cpbt_input_break=cpbt_input_break,
    target_checkpoint=target_checkpoint or None,
    target_partition_strategy=target_partition_strategy,
    target_partitions=target_partitions,
    target_partition_keys=target_partition_keys or None,
    business_optimization=business_optimization,
    original_spark_config=ORIGINAL_SPARK_CONFIG,
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
    module = importlib.import_module(module_name)
    _emit("IMPORT", module=module_name, path=module.__file__)
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

# COMMAND ----------


def _print_output_v3_profile(profile):
    effective_config = profile.get("effective_spark_config", {})
    performance = profile.get("performance_summary", {})
    _emit(
        "OUTPUTV3_PROFILE",
        updated_wall_seconds=profile.get("updated_wall_seconds"),
        experiment_id=profile.get("experiment_id"),
        experiment_settings=profile.get("experiment_settings"),
        checkpoint_mode=profile.get("checkpoint_mode"),
        checkpoint_policy=profile.get("checkpoint_policy"),
        profile_plan=profile.get("profile_plan"),
        plan_checkpoint_threshold=profile.get("plan_checkpoint_threshold"),
        requested_shuffle_partitions=profile.get(
            "requested_shuffle_partitions"
        ),
        effective_spark_config=effective_config,
        effective_max_threads=profile.get("effective_max_threads"),
        enabled_parallel_groups=profile.get("enabled_parallel_groups"),
        execution_strategy=profile.get("execution_strategy"),
        pipeline_strategy=profile.get("pipeline_strategy"),
        branch_strategy=profile.get("branch_strategy"),
        pass_a_strategy=profile.get("pass_a_strategy"),
        output_build_strategy=profile.get("output_build_strategy"),
    )
    _emit_rows("STAGE", profile.get("stage_timings", []))
    _emit_rows("OPERATION", profile.get("operation_timings", []))
    # Checkpoints run inside worker threads, so their inline START/DONE prints
    # are not captured by the notebook. Re-emit each one from the main thread,
    # ordered by wall-clock start, so every start/end time is visible here.
    checkpoints = sorted(
        profile.get("checkpoint_activity", []),
        key=lambda row: str(row.get("started_at") or ""),
    )
    checkpoint_events = []
    for row in checkpoints:
        checkpoint_events.extend(
            [
                (
                    str(row.get("started_at") or ""),
                    "START",
                    row,
                ),
                (
                    str(row.get("ended_at") or ""),
                    "DONE",
                    row,
                ),
            ]
        )
    print("\n[outputV3 checkpoint replay] chronological START/DONE events")
    for event_at, event_type, row in sorted(checkpoint_events):
        if event_type == "START":
            print(
                f"[{event_at}] [outputV3 checkpoint] START "
                f"name={row.get('name')} stage={row.get('stage')} "
                f"mode={row.get('checkpoint_mode')} "
                f"thread={row.get('thread')}",
                flush=True,
            )
        else:
            print(
                f"[{event_at}] [outputV3 checkpoint] DONE "
                f"name={row.get('name')} stage={row.get('stage')} "
                f"mode={row.get('checkpoint_mode')} "
                f"backend={row.get('actual_backend')} "
                f"thread={row.get('thread')} "
                f"elapsed={row.get('elapsed_seconds')}s",
                flush=True,
            )
    for row in checkpoints:
        _emit(
            "CHECKPOINT_START",
            name=row.get("name"),
            stage=row.get("stage"),
            checkpoint_mode=row.get("checkpoint_mode"),
            thread=row.get("thread"),
            started_at=row.get("started_at"),
        )
        _emit(
            "CHECKPOINT_DONE",
            name=row.get("name"),
            stage=row.get("stage"),
            checkpoint_mode=row.get("checkpoint_mode"),
            actual_backend=row.get("actual_backend"),
            thread=row.get("thread"),
            ended_at=row.get("ended_at"),
            elapsed_seconds=row.get("elapsed_seconds"),
        )
    _emit_rows("CHECKPOINT", checkpoints)
    _emit_rows("PARALLEL_TASK", profile.get("parallel_activity", []))
    _emit_rows("ARTIFACT_MERGE", profile.get("artifact_merges", []))
    _emit(
        "PERFORMANCE",
        **{
            key: value
            for key, value in performance.items()
            if key not in {
                "critical_actions",
                "parallel_wave_critical_path",
            }
        },
    )
    _emit_rows(
        "CRITICAL_ACTION", performance.get("critical_actions", [])
    )
    _emit_rows(
        "PARALLEL_WAVE",
        performance.get("parallel_wave_critical_path", []),
    )
    _emit_rows("PLAN_BUILDER", profile.get("plan_profile", []))
    _emit_rows(
        "PLAN_CHECKPOINT", profile.get("checkpoint_plan_profile", [])
    )
    _emit_rows("PLAN_ACTION", profile.get("action_profile", []))


def _run(variant):
    is_production = variant == "production"
    print(
        "\n"
        + "=" * 96
        + f"\nFEP BENCHMARK VARIANT START: {variant}\n"
        + "=" * 96,
        flush=True,
    )
    variant_shuffle = 4 if is_production else shuffle_partitions
    spark.conf.set(
        "spark.sql.shuffle.partitions", str(variant_shuffle)
    )
    shuffle_before_run = spark.conf.get("spark.sql.shuffle.partitions")
    print(
        f"[SPARK SHUFFLE BEFORE] variant={variant} "
        f"requested={variant_shuffle} session={shuffle_before_run}",
        flush=True,
    )
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
                "ParallelGroups": parallel_groups,
                "CheckpointMode": checkpoint_mode,
                "ProfilePlan": profile_plan,
                "PlanCheckpointThreshold": plan_checkpoint_threshold,
                "ExperimentID": experiment_id,
                "SqlShufflePartitions": shuffle_partitions,
                "WarningProbeRemoval": warning_probe_removal,
                "MissingEntityIdentity": missing_entity_identity,
                "CpbtInputBreak": cpbt_input_break,
                "TargetCheckpoint": target_checkpoint,
                "TargetPartitionStrategy": target_partition_strategy,
                "TargetPartitions": target_partitions,
                "TargetPartitionKeys": target_partition_keys,
                "BusinessOptimization": business_optimization,
            }
        )
        if volume_path:
            kwargs["VolumePath"] = volume_path
    _emit(
        "RUN_START",
        timestamp=datetime.now().isoformat(),
        variant=variant,
        spark_shuffle_partitions=spark.conf.get(
            "spark.sql.shuffle.partitions"
        ),
        spark_aqe_enabled=spark.conf.get("spark.sql.adaptive.enabled"),
        spark_advisory_partition_bytes=spark.conf.get(
            "spark.sql.adaptive.advisoryPartitionSizeInBytes"
        ),
        arguments=kwargs,
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
    print(
        f"[SPARK SHUFFLE AFTER] variant={variant} "
        f"requested={variant_shuffle} session={shuffle_after_run} "
        f"profile_requested={profile_requested_shuffle} "
        f"profile_effective={profile_effective_shuffle} "
        f"verification_required={verification_required} "
        f"status={shuffle_status}",
        flush=True,
    )
    _emit(
        "SPARK_SHUFFLE_VERIFY",
        variant=variant,
        requested=variant_shuffle,
        session_before=shuffle_before_run,
        session_after=shuffle_after_run,
        profile_requested=profile_requested_shuffle,
        profile_effective=profile_effective_shuffle,
        verification_required=verification_required,
        status=shuffle_status,
        exact_match=shuffle_matches,
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
    _emit(
        "RUN_DONE",
        timestamp=datetime.now().isoformat(),
        variant=variant,
        wall_seconds=wall,
        reported_seconds=reported,
        rows=record["rows"],
        tables=record["tables"],
        requested_shuffle_partitions=variant_shuffle,
        effective_shuffle_partitions=profile_effective_shuffle,
        shuffle_verified=shuffle_matches,
    )
    for table, fingerprint in sorted(fingerprints.items()):
        _emit(
            "FINGERPRINT",
            variant=variant,
            table=table,
            fingerprint=fingerprint,
        )
    if not is_production:
        _print_output_v3_profile(profile)
    print(
        "=" * 96
        + f"\nFEP BENCHMARK VARIANT DONE: {variant} "
        f"wall={wall:.3f}s reported={reported}\n"
        + "=" * 96,
        flush=True,
    )
    return record


_emit(
    "BENCHMARK_START",
    timestamp=datetime.now().isoformat(),
    order=["production", "outputV3"],
)
production = None
optimized = None
mismatches = []
final_comparison = None
snapshots = create_run_snapshots(spark, catalog, schema, run_id)
_emit("SNAPSHOT_CREATED", snapshots=snapshots)
try:
    production = _run("production")
    optimized = _run("outputV3")

    if production["rows"] != BASELINE["rows"]:
        raise AssertionError(
            f"Production rows changed: expected 79, got {production['rows']}"
        )
    if production["tables"] != BASELINE["tables"]:
        raise AssertionError(
            "Production must write exactly three output tables"
        )
    mismatches = compare_outputs(
        production["fingerprints"], optimized["fingerprints"]
    )
    for table in reconcile.OUTPUT_TABLES:
        table_mismatches = [
            item for item in mismatches if item["table"] == table
        ]
        _emit(
            "RECONCILE_TABLE",
            table=table,
            exact_match=not table_mismatches,
            mismatches=table_mismatches,
        )
    if mismatches:
        raise AssertionError(f"Exact fingerprint mismatch: {mismatches[0]}")

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
    _emit("FINAL_COMPARISON", **final_comparison)
finally:
    try:
        restore_run_snapshots(spark, catalog, schema, run_id, snapshots)
        _emit("SNAPSHOT_RESTORED", snapshots=snapshots)
    except Exception as exc:
        _emit(
            "SNAPSHOT_RESTORE_FAILED",
            snapshots=snapshots,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise
    else:
        drop_run_snapshots(spark, catalog, schema, snapshots)
        _emit("SNAPSHOT_DROPPED", snapshots=snapshots)
    finally:
        for key, value in ORIGINAL_SPARK_CONFIG.items():
            spark.conf.set(key, value)
        _emit(
            "BENCHMARK_DONE",
            timestamp=datetime.now().isoformat(),
            restored_spark_config=ORIGINAL_SPARK_CONFIG,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime and data-compare tables
# MAGIC
# MAGIC The cells above stream copyable `[FEP_BENCHMARK][...]` JSON lines to
# MAGIC stdout. The tables below render the same runtime and parity results in
# MAGIC the notebook result tabs.

# COMMAND ----------

runtime_rows = []
for record in (production, optimized):
    if record is None:
        continue
    runtime_rows.append(
        {
            "variant": record["variant"],
            "wall_seconds": record["wall_seconds"],
            "reported_seconds": record["reported_seconds"],
            "rows": record["rows"],
            "tables": record["tables"],
            "requested_shuffle_partitions": record[
                "requested_shuffle_partitions"
            ],
            "session_shuffle_after": record["session_shuffle_after"],
            "profile_requested_shuffle": str(
                record["profile_requested_shuffle"]
            ),
            "profile_effective_shuffle": str(
                record["profile_effective_shuffle"]
            ),
            "shuffle_verified": record["shuffle_verified"],
            "shuffle_verification_required": record[
                "shuffle_verification_required"
            ],
            "shuffle_status": record["shuffle_status"],
            "baseline_wall_seconds": BASELINE["wall_seconds"],
            "wall_vs_baseline_seconds": round(
                record["wall_seconds"] - BASELINE["wall_seconds"], 3
            ),
        }
    )
if final_comparison is not None:
    runtime_rows.append(
        {
            "variant": "improvement (production - outputV3)",
            "wall_seconds": final_comparison["improvement_seconds"],
            "reported_seconds": final_comparison["improvement_percent"],
            "rows": final_comparison["rows"],
            "tables": final_comparison["tables"],
            "requested_shuffle_partitions": optimized[
                "requested_shuffle_partitions"
            ],
            "session_shuffle_after": optimized["session_shuffle_after"],
            "profile_requested_shuffle": str(
                optimized["profile_requested_shuffle"]
            ),
            "profile_effective_shuffle": str(
                optimized["profile_effective_shuffle"]
            ),
            "shuffle_verified": optimized["shuffle_verified"],
            "shuffle_verification_required": optimized[
                "shuffle_verification_required"
            ],
            "shuffle_status": optimized["shuffle_status"],
            "baseline_wall_seconds": BASELINE["wall_seconds"],
            "wall_vs_baseline_seconds": round(
                optimized["wall_seconds"] - BASELINE["wall_seconds"], 3
            )
            if optimized is not None
            else None,
        }
    )

RUNTIME_SCHEMA = """
    variant STRING,
    wall_seconds DOUBLE,
    reported_seconds DOUBLE,
    rows LONG,
    tables LONG,
    requested_shuffle_partitions INT,
    session_shuffle_after STRING,
    profile_requested_shuffle STRING,
    profile_effective_shuffle STRING,
    shuffle_verified BOOLEAN,
    shuffle_verification_required BOOLEAN,
    shuffle_status STRING,
    baseline_wall_seconds DOUBLE,
    wall_vs_baseline_seconds DOUBLE
"""
if runtime_rows:
    display(spark.createDataFrame(runtime_rows, RUNTIME_SCHEMA))

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
                "actual_backend": row.get("actual_backend"),
                "started_at": row.get("started_at"),
                "ended_at": row.get("ended_at"),
                "elapsed_seconds": row.get("elapsed_seconds"),
            }
        )

CHECKPOINT_TIMING_SCHEMA = """
    name STRING,
    stage STRING,
    thread STRING,
    actual_backend STRING,
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
