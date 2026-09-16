"""
load_allocation_input.py

Converted from: dbo.uspLoadAllocationInput
Database: IPC_2025_DEV8_MayBuild (SQL Server: usazutaxw00073.us.deloitte.com)
Conversion date: 2026-05-28

Loads input data to AllocationInput table. Handles K1, Form 926/8886/199A/8865,
PFIC footnote flowup, adjustments, at-risk, M1, GAAP-to-tax,
tag percentages, and rounding.

Supports three execution modes:
  Mode 1 (Job)         — cfg passed via taskValues JSON (built by Job wrapper)
  Mode 2 (Orchestrator) — cfg passed as dict (built by orchestrator)
  Mode 3 (Standalone)  — cfg built from parameters via load_common_config

Usage:1
    from load_allocation_input import run_load_allocation_input

    # Mode 3 (standalone):
    result = run_load_allocation_input(
        spark,
        entity_id=123, client_id=456, tax_period_id=789, run_id=1001,
        catalog="qa7", schema="IPC_2025_DEV8_MayBuild",
    )

    # Mode 1/2 (pre-built cfg):
    result = run_load_allocation_input(spark, cfg=cfg)
"""

from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib
import logging
import time

# Common_V2: shared framework for all converted SPs
from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import table_prefix, log_section, log_timing
from .checkpoint import (
    drop_checkpoints,
    log_checkpoint_plan,
    normalize_checkpoint_backend,
    normalize_local_denylist,
    pipeline_checkpoint,
)
from .ai_shared_views import register_shared_views
from .parent import output_module
from .plan_profiler import plan_profile_report, profile_action, track_plan

# Unchanged services come from the production parent package.
_config = output_module("ai_config_service")
_validation = output_module("ai_validation_service")
_hierarchy = output_module("ai_hierarchy_service")
_k1 = output_module("ai_k1_service")
_form = output_module("ai_form_service")
_pfic = output_module("ai_pfic_service")
_final = output_module("ai_finalization_service")
_flowup = importlib.import_module(f"{__package__}.ai_pfic_flowup_service")

load_config = _config.load_config
run_validations = _validation.run_validations
build_entity_hierarchy = track_plan(_hierarchy.build_entity_hierarchy)
build_lower_tier_funds = track_plan(_hierarchy.build_lower_tier_funds)
build_workflows = track_plan(_hierarchy.build_workflows)
build_k1_and_related_inputs = track_plan(_k1.build_k1_and_related_inputs)
build_all_form_inputs = track_plan(_form.build_all_form_inputs)
build_pfic_snapshot = track_plan(_pfic.build_pfic_snapshot)
build_pfic_elections = track_plan(_pfic.build_pfic_elections)
build_pfic_allocation_input = track_plan(_pfic.build_pfic_allocation_input)
apply_pfic_election_deletes = track_plan(_pfic.apply_pfic_election_deletes)
apply_part_v_vii_flags = track_plan(_pfic.apply_part_v_vii_flags)
build_pfic_flowup_pipeline = track_plan(_flowup.build_pfic_flowup_pipeline)
build_custom_footnote_input = track_plan(_flowup.build_custom_footnote_input)
check_pfic_xml_override_alert = _flowup.check_pfic_xml_override_alert
apply_tag_percentages = track_plan(_final.apply_tag_percentages)
apply_master_feed_override = track_plan(_final.apply_master_feed_override)
apply_blocker_entity_cleanup = track_plan(_final.apply_blocker_entity_cleanup)
apply_distribution_line_suppression = track_plan(
    _final.apply_distribution_line_suppression
)
purge_output_tables = _final.purge_output_tables
write_allocation_input = _final.write_allocation_input
write_pfic_flowup = _final.write_pfic_flowup
write_form_flowups = _final.write_form_flowups

logger = logging.getLogger(__name__)


def run_load_allocation_input(
    spark: SparkSession,
    cfg: dict = None,
    entity_id: int = None,
    client_id: int = None,
    tax_period_id: int = None,
    run_id: int = None,
    catalog: str = None,
    schema: str = None,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "deltalake",
    VolumePath: str = "",
    ExecutionID: str = "1",
    result_type: str = None,
    volume_path: str = None,
    execution_id: str = None,
    call_from: str = None,
    CallFrom: str = None,
    max_threads: int = 4,
    MaxThreads: int = None,
    profile_plan: bool = False,
    ProfilePlan: object = None,
    plan_checkpoint_threshold: int = 30,
    PlanCheckpointThreshold: int = None,
    checkpoint_backend: str = "delta",
    CheckpointBackend: str = None,
    LocalDeltaDenylist: object = "",
    LocalDeltaDenylistMode: str = "extend",
    **kwargs,
) -> dict:
    """Main entry point — orchestrates the full allocation input pipeline.

    Supports three execution modes:
      Mode 1 (Job):         cfg passed via taskValues JSON
      Mode 2 (Orchestrator): cfg passed as dict
      Mode 3 (Standalone):  cfg built from parameters via load_common_config

    Returns dict with status and timing.
    """
    # Normalize params: Orchestrator sends PascalCase, standalone uses snake_case
    entity_id = entity_id or EntityID
    client_id = client_id or ClientID
    tax_period_id = tax_period_id or TaxPeriodID
    run_id = run_id or RunID
    catalog = catalog or CatalogName
    schema = schema or SchemaName
    result_type = result_type or ResultType or "deltalake"
    volume_path = volume_path or VolumePath or ""
    execution_id = execution_id or ExecutionID or "1"
    call_from = call_from or CallFrom

    t0 = time.time()
    log_section("run_load_allocation_input")

    # Mode 3 (standalone): build cfg from scratch; Modes 1/2 already have it
    if cfg is None:
        cfg = load_common_config(
            spark,
            entity_id=entity_id,
            client_id=client_id,
            tax_period_id=tax_period_id,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
            call_from=call_from,
        )
    elif call_from is not None:
        cfg["call_from"] = call_from
    cfg.setdefault("_checkpoint_tables", [])   # tracks temp tables for cleanup
    cfg.setdefault("_parquet_results", {})     # accumulates DFs to persist at end
    try:
        workers = int(MaxThreads if MaxThreads is not None else max_threads)
    except (TypeError, ValueError):
        workers = 4
    workers = max(1, min(workers, 8))
    profile_value = ProfilePlan if ProfilePlan is not None else profile_plan
    if isinstance(profile_value, str):
        profile_value = profile_value.strip().lower() in {
            "1", "true", "on", "yes"
        }
    cfg["profile_plan"] = bool(profile_value)
    cfg["plan_checkpoint_threshold"] = int(
        PlanCheckpointThreshold
        if PlanCheckpointThreshold is not None
        else plan_checkpoint_threshold
    )
    cfg["max_threads"] = workers
    cfg["_checkpoint_backend"] = normalize_checkpoint_backend(
        CheckpointBackend or checkpoint_backend
    )
    cfg["_local_delta_denylist"] = normalize_local_denylist(
        LocalDeltaDenylist, LocalDeltaDenylistMode
    )
    cfg.setdefault("_plan_profile", [])
    cfg.setdefault("_checkpoint_plan_profile", [])
    cfg.setdefault("_action_plan_profile", [])
    # Persist output-routing settings so the final flush works in every mode
    # (Mode 1/2 pass cfg directly; Mode 3 built it above).
    if volume_path:
        cfg["volume_path"] = volume_path
    cfg.setdefault("result_type", result_type)
    cfg.setdefault("execution_id", execution_id)
    log_checkpoint_plan(cfg)
    print(f"[updated] max_threads={workers} profile_plan={cfg['profile_plan']}")

    # RunStatus=FAIL early-abort (mirrors run_sm_apply_investment_level_rounding):
    # load_common_config sets run_status="FAIL" when no AllocationRun row resolves.
    # Abort before any heavy work rather than producing partial output.
    if cfg.get("run_status") == "FAIL":
        logger.error(
            f"RunStatus=FAIL - aborting. RunID={cfg.get('run_id')}, "
            f"EntityID={cfg.get('entity_id')}"
        )
        return {"status": "FAIL", "reason": "run_status_fail"}

    # Phase 1: Load SP-specific scalars + register reusable temp views
    t_phase = time.time()
    cfg = load_config(spark, cfg)
    # Temp-view registration mutates the shared session catalog; retain the
    # production order instead of creating views concurrently.
    register_shared_views(spark, cfg)
    print(f"[phase 1] Config + shared views: {time.time() - t_phase:.1f}s")

    # Phase 2: Build entity hierarchy tree + lower-tier fund lookups + workflow IDs
    t_phase = time.time()
    hierarchy_df = build_entity_hierarchy(spark, cfg)
    lower_tier_df = build_lower_tier_funds(spark, cfg)
    workflows = build_workflows(spark, cfg)
    print(f"[phase 2] Hierarchy + workflows: {time.time() - t_phase:.1f}s")

    # Phase 3: Run pre-conditions (data integrity checks); abort early if invalid
    t_phase = time.time()
    should_continue = run_validations(spark, cfg, lower_tier_df)
    print(f"[phase 3] Validations: {time.time() - t_phase:.1f}s")
    if not should_continue:
        drop_checkpoints(spark, cfg)
        return {"status": "FAIL", "reason": "validation_failed"}

    # Purge existing output rows for this RunID before rebuilding
    t_phase = time.time()
    purge_output_tables(spark, cfg)
    print(f"[purge] Output tables: {time.time() - t_phase:.1f}s")

    # Phase 4: Build form inputs (926, 8886, 199A, 8865, at-risk, M1, GAAP-to-tax)
    t_phase = time.time()
    allocation_input_df = build_all_form_inputs(spark, cfg)
    print(f"[phase 4] Form inputs: {time.time() - t_phase:.1f}s")

    # Phase 5: Build K1 line items + adjustments; union into main allocation DF
    t_phase = time.time()
    k1_df = build_k1_and_related_inputs(spark, cfg, workflows)
    allocation_input_df = allocation_input_df.unionByName(k1_df, allowMissingColumns=True)
    print(f"[phase 5] K1 + related inputs: {time.time() - t_phase:.1f}s")

    # Phase 6: PFIC snapshot → elections → allocation input rows + custom footnotes
    t_phase = time.time()
    pfic_snapshot_df = build_pfic_snapshot(spark, cfg)
    # Perf (playbook §3b): pfic_snapshot_df is a deep frame (large snapshot ⋈ workflows +
    # blocked-filter anti-join chain) consumed lazily by 3 stages — build_pfic_elections,
    # build_pfic_allocation_input, and build_pfic_flowup_pipeline — so it recomputes ~3×.
    # Checkpoint once to break the re-evaluation. Logic-neutral (same rows).
    pfic_snapshot_df = pipeline_checkpoint(
        spark, pfic_snapshot_df, "pfic_snapshot", cfg
    )
    pfic_snapshot_df.createOrReplaceTempView(f"_pfic_snapshot_{cfg['run_id']}")
    pfic_elections = build_pfic_elections(spark, cfg, pfic_snapshot_df)
    pfic_alloc_df = build_pfic_allocation_input(spark, cfg, pfic_snapshot_df, pfic_elections)
    allocation_input_df = allocation_input_df.unionByName(pfic_alloc_df, allowMissingColumns=True)

    custom_fn_df = build_custom_footnote_input(spark, cfg)
    allocation_input_df = allocation_input_df.unionByName(custom_fn_df, allowMissingColumns=True)

    print(f"[phase 6] PFIC + custom footnote: {time.time() - t_phase:.1f}s")

    # Checkpoint: materialize accumulated allocation DF to break lineage
    t_phase = time.time()
    allocation_input_df = pipeline_checkpoint(
        spark, allocation_input_df, "alloc_input", cfg
    )
    print(f"[checkpoint] alloc_input: {time.time() - t_phase:.1f}s")

    # Phase 7a: PFIC flowup — tier-up foreign corp footnotes through entity hierarchy
    t_phase = time.time()
    pfic_flowup_df = build_pfic_flowup_pipeline(
        spark, cfg, pfic_snapshot_df, pfic_elections, lower_tier_df
    )
    pfic_flowup_df = pipeline_checkpoint(
        spark, pfic_flowup_df, "pfic_raw", cfg
    )
    print(f"[phase 7a] PFIC flowup build + checkpoint: {time.time() - t_phase:.1f}s")

    # Phase 7b: Apply election-based deletes + set Part V/VII indicator flags
    t_phase = time.time()
    check_pfic_xml_override_alert(spark, cfg, pfic_flowup_df)

    allocation_input_df, pfic_flowup_df = apply_pfic_election_deletes(
        spark, cfg, allocation_input_df, pfic_flowup_df, pfic_elections, lower_tier_df
    )

    pfic_flowup_df = apply_part_v_vii_flags(spark, cfg, pfic_flowup_df)

    print(f"[phase 7b] Election deletes + Part V/VII: {time.time() - t_phase:.1f}s")

    t_phase = time.time()
    pfic_flowup_df = pipeline_checkpoint(
        spark, pfic_flowup_df, "pfic_flowup", cfg
    )
    print(f"[checkpoint] pfic_flowup: {time.time() - t_phase:.1f}s")

    # Post-processing filters: master feed override, blocker-entity cleanup, suppression
    allocation_input_df = apply_master_feed_override(spark, cfg, allocation_input_df)
    allocation_input_df = apply_blocker_entity_cleanup(spark, cfg, allocation_input_df)
    allocation_input_df = apply_distribution_line_suppression(spark, cfg, allocation_input_df)

    t_phase = time.time()
    allocation_input_df = pipeline_checkpoint(
        spark, allocation_input_df, "alloc_filtered", cfg
    )
    print(f"[checkpoint] alloc_filtered: {time.time() - t_phase:.1f}s")

    # Phase 8: Apply investment-level tag percentages (PE Book Allocation only)
    t_phase = time.time()
    allocation_input_df = apply_tag_percentages(spark, cfg, allocation_input_df)
    print(f"[phase 8] Tag percentages: {time.time() - t_phase:.1f}s")

    if cfg.get("investment_tag_workflow_id", 0) != 0:
        t_phase = time.time()
        allocation_input_df = pipeline_checkpoint(
            spark, allocation_input_df, "alloc_tagged", cfg
        )
        print(f"[checkpoint] alloc_tagged: {time.time() - t_phase:.1f}s")

    # Phase 9: Final aggregation + Delta writes (AllocationInput, PFICFlowup, FormFlowups)
    t_phase = time.time()
    # These builders register temp views and mutate the collector map. Preserve
    # production order; only the final writes to distinct tables are parallel.
    write_allocation_input(spark, cfg, allocation_input_df)
    write_pfic_flowup(spark, cfg, pfic_flowup_df)
    write_form_flowups(spark, cfg)
    print(f"[phase 9] Collect results (groupBy/agg): {time.time() - t_phase:.1f}s")

    # Flush accumulated DataFrames as TWO separate writes:
    #   1. AllocationInput  → Delta   (replaceWhere on RunID — idempotent re-runs)
    #   2. all other tables → Parquet (GenericResultStorer.save_parquet_files)
    parquet_results = cfg.get("_parquet_results", {})
    run_id = cfg["run_id"]
    save_return_value = None

    if parquet_results:
        from datetime import datetime
        from Common_V2.core.helpers import table_prefix
        from Common_V2.core.generic_result_storer import GenericResultStorer
        from pyspark.sql import functions as _F
        prefix = table_prefix(cfg)
        _schema_info = cfg.get("_schema_cache", {})
        _client_id = cfg.get("client_id", client_id)
        _entity_id = cfg.get("entity_id", entity_id)
        _execution_id = cfg.get("execution_id", execution_id) or "1"
        _volume_path = cfg.get("volume_path") or ""
        # Tables that use coalesce(1) to avoid excessive small files
        small_tables = {"Form926Flowup", "Form199AFlowup", "Form8865Flowup",
                        "Form8886Flowup", "AtRiskFlowup", "CustomFootnoteFlowup",
                        "Form200616Flowup", "PFICFootnoteFlowup",
                        "PFICFootnoteFlowupWithTrackingKey"}

        def _align(df, tbl_name):
            """Add missing target columns and project to the table's column order."""
            fqn = f"{prefix}.{tbl_name}"
            if tbl_name in _schema_info:
                target_types = _schema_info[tbl_name]
                target_cols = list(target_types.keys())
            else:
                fields = spark.table(fqn).schema.fields
                target_types = {f.name: f.dataType for f in fields}
                target_cols = [f.name for f in fields]
            out = df
            for col_name in target_cols:
                if col_name not in out.columns:
                    # Cast the null literal to the target column type; an
                    # untyped lit(None) is VOID and cannot be written to Parquet.
                    col_type = target_types.get(col_name)
                    if col_type is not None:
                        out = out.withColumn(col_name, _F.lit(None).cast(col_type))
                    else:
                        out = out.withColumn(col_name, _F.lit(None))
            return out.select(target_cols)

        # ── Write 1: AllocationInput → Delta ──────────────────────────────
        alloc_df = parquet_results.get("AllocationInput")
        if alloc_df is not None:
            profile_action(
                "AllocationInput.saveAsTable",
                alloc_df,
                lambda: (
                    alloc_df.write.format("delta")
                    .mode("overwrite")
                    .option("replaceWhere", f"RunID = {run_id}")
                    .saveAsTable(f"{prefix}.AllocationInput")
                ),
                cfg,
            )
            print("   [ok] AllocationInput (delta)")

        # ── Write 2: all other tables → Parquet (via GenericResultStorer) ──
        parquet_tables = {}
        for tbl_name, df in parquet_results.items():
            if tbl_name == "AllocationInput":
                continue
            write_df = _align(df, tbl_name)
            if tbl_name in small_tables:
                write_df = write_df.coalesce(1)
            #if write_df.isEmpty():
            #    print(f"   [skip] {tbl_name} (empty, skipped)")
            #    continue
            parquet_tables[tbl_name] = write_df

        if parquet_tables:
            try:
                result_type = cfg.get("result_type", "deltalake")
                storer_kwargs = dict(
                    result_type=result_type,
                    catalog_name=cfg.get("catalog", ""),
                    database_name=cfg.get("schema", ""),
                    run_id=run_id,
                    client_id=_client_id,
                    entity_id=_entity_id,
                    execution_id=_execution_id,
                    volume_path=_volume_path,
                    sql_url_path=cfg.get("sql_url_path", ""),
                    sql_username=cfg.get("sql_username", ""),
                    sql_password=cfg.get("sql_password", ""),
                )
                print(
                    f"[store] Writing {len(parquet_tables)} tables "
                    f"(workers={workers}): {datetime.now()}"
                )

                def _store_one(table_name, table_df):
                    storer = GenericResultStorer(spark, None)
                    return profile_action(
                        f"{table_name}.save_results",
                        table_df,
                        lambda: storer.save_results(
                            result={table_name: table_df},
                            **storer_kwargs,
                        ),
                        cfg,
                    )

                if workers > 1 and len(parquet_tables) > 1:
                    errors = []
                    with ThreadPoolExecutor(
                        max_workers=min(workers, len(parquet_tables))
                    ) as pool:
                        futures = {
                            pool.submit(_store_one, name, df): name
                            for name, df in parquet_tables.items()
                        }
                        for future in as_completed(futures):
                            table_name = futures[future]
                            try:
                                value = future.result()
                                if value and not save_return_value:
                                    save_return_value = value
                                print(f"   [ok] {table_name}")
                            except Exception as exc:
                                errors.append(f"{table_name}: {exc}")
                    if errors:
                        raise RuntimeError(
                            "Parallel flow-up writes failed: "
                            + "; ".join(errors)
                        )
                else:
                    for table_name, table_df in parquet_tables.items():
                        value = _store_one(table_name, table_df)
                        if value and not save_return_value:
                            save_return_value = value
                        print(f"   [ok] {table_name}")
                print(
                    f"[done] Stored {len(parquet_tables)} flow-up tables: "
                    f"{datetime.now()}"
                )
            except Exception as e:
                logger.error(
                    f"Parquet flow-up write failed (AllocationInput already committed): "
                    f"{type(e).__name__}: {e}"
                )
                raise

    elapsed = time.time() - t0
    log_timing("run_load_allocation_input", t0)
    plan_profile = None
    if cfg.get("profile_plan"):
        plan_profile = plan_profile_report(cfg)
    drop_checkpoints(spark, cfg)  # clean up temp checkpoint tables

    if save_return_value and isinstance(save_return_value, str) and save_return_value.strip().startswith("{"):
        return save_return_value

    return {
        "status": "SUCCESS",
        "elapsed_seconds": round(elapsed, 1),
        "implementation": "output.updated.load_allocation_input",
        "max_threads": workers,
        "checkpoint_backend": cfg.get("_checkpoint_backend"),
        "checkpoint_timings": cfg.get("_checkpoint_elapsed", []),
        "plan_profile": plan_profile,
        "checkpoint_plan_profile": cfg.get(
            "_checkpoint_plan_profile_report", []
        ),
        "action_plan_profile": cfg.get("_action_plan_profile_report", []),
    }
