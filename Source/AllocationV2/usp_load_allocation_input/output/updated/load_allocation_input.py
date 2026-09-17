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
import contextvars
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyspark.sql.functions as F

# Common_V2: shared framework for all converted SPs
from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import table_prefix, log_section, log_timing
from . import checkpoint as _ckpt

checkpoint = getattr(_ckpt, "checkpoint")
drop_checkpoints = getattr(
    _ckpt,
    "drop_checkpoints",
    lambda spark, cfg: None,
)

try:
    from .plan_profiler import plan_profile_report, profile_dataframe, track_plan
except ImportError:
    def track_plan(fn):
        return fn

    def profile_dataframe(label, df, cfg, *, kind="builder"):
        del label, cfg, kind
        return df

    def plan_profile_report(cfg):
        del cfg
        return []

MAX_THREADS = 4


def normalize_checkpoint_backend(value: object) -> str:
    fn = getattr(_ckpt, "normalize_checkpoint_backend", None)
    if fn:
        return fn(value)
    backend = str(value or "delta").strip().lower()
    if backend not in {"local", "delta"}:
        raise ValueError("CheckpointBackend must be 'local' or 'delta'")
    return backend


def normalize_local_delta_denylist(value: object, mode: object = "extend"):
    fn = getattr(_ckpt, "normalize_local_delta_denylist", None) or getattr(
        _ckpt, "normalize_local_denylist", None
    )
    if fn:
        try:
            return fn(value, mode)
        except TypeError:
            return fn(value)
    if not value:
        return frozenset()
    tokens = re.split(r"[,\s]+", value) if isinstance(value, str) else value
    return frozenset(str(token).strip() for token in tokens if str(token).strip())


def cache_for_run(df, cfg, *, broadcast: bool = False):
    # Serverless / Spark Connect does not fully support persist(); rely on a
    # broadcast hint for small datasets and return the frame unchanged
    # otherwise.
    fn = getattr(_ckpt, "cache_for_run", None)
    if fn:
        return fn(df, cfg, broadcast=broadcast)
    if not hasattr(df, "columns"):
        return df
    if broadcast:
        print(f"[broadcast] columns={len(df.columns)}")
        return F.broadcast(df)
    return df


def unpersist_cached(cfg):
    fn = getattr(_ckpt, "unpersist_cached", None)
    if fn:
        return fn(cfg)
    # No-op: caching is disabled (persist is unsupported on serverless).
    cfg["_cached_dataframes"] = []


def isolated_collector_cfg(cfg: dict) -> dict:
    fn = getattr(_ckpt, "isolated_collector_cfg", None)
    if fn:
        return fn(cfg)
    local = dict(cfg)
    local["_parquet_results"] = {}
    local["_schema_cache"] = {}
    return local


def merge_collector_cfg(target: dict, local: dict) -> None:
    fn = getattr(_ckpt, "merge_collector_cfg", None)
    if fn:
        return fn(target, local)
    target_results = target.setdefault("_parquet_results", {})
    for table_name, df in local.get("_parquet_results", {}).items():
        if table_name in target_results:
            target_results[table_name] = target_results[table_name].unionByName(
                df, allowMissingColumns=True
            )
        else:
            target_results[table_name] = df
    target.setdefault("_schema_cache", {}).update(local.get("_schema_cache", {}))


def run_parallel(tasks, label: str):
    names = [name for name, _ in (tasks or [])]
    print(
        f"[parallel:call] {label}: tasks={len(names)} names={names}",
        flush=True,
    )
    fn = getattr(_ckpt, "run_parallel", None)
    if fn:
        return fn(tasks, label)
    if not tasks:
        print(f"[parallel:skip] {label}: no tasks", flush=True)
        return []
    workers = min(MAX_THREADS, len(tasks))
    started = time.time()
    if workers > 1:
        print(
            f"[PARALLEL] ▶ '{label}' RUNNING IN PARALLEL: {len(tasks)} tasks "
            f"across {workers} threads → {names}",
            flush=True,
        )
    print(
        f"[parallel:start] {label}: workers={workers} tasks={len(tasks)} "
        f"names={names}",
        flush=True,
    )

    def _wrap(name, task):
        def _run():
            thread = threading.current_thread().name
            ident = threading.get_ident()
            t0 = time.time()
            print(
                f"[parallel:run] {label}/{name} thread={thread} ident={ident}",
                flush=True,
            )
            result = task()
            elapsed = time.time() - t0
            return result, thread, ident, elapsed

        return _run

    values = {}
    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix=f"par-{label[:24]}"
    ) as pool:
        futures = {}
        for name, task in tasks:
            print(f"[parallel:submit] {label}/{name}", flush=True)
            context = contextvars.copy_context()
            futures[pool.submit(context.run, _wrap(name, task))] = name
        for future in as_completed(futures):
            name = futures[future]
            result, thread, ident, elapsed = future.result()
            values[name] = result
            print(
                f"[parallel:done] {label}/{name} thread={thread} "
                f"ident={ident} elapsed={elapsed:.2f}s",
                flush=True,
            )
    wall = time.time() - started
    if workers > 1:
        print(
            f"[PARALLEL] ✔ '{label}' finished concurrently: wall={wall:.2f}s "
            f"(threads={workers}, tasks={len(tasks)})",
            flush=True,
        )
    print(
        f"[parallel:end] {label}: wall={wall:.2f}s "
        f"workers={workers} tasks={len(tasks)}",
        flush=True,
    )
    return [(name, values[name]) for name, _ in tasks]

# SP-specific service modules (one per logical section of the original SQL)
from .ai_shared_views import register_shared_views
from .ai_config_service import load_config
from .ai_validation_service import run_validations
from .ai_hierarchy_service import build_entity_hierarchy, build_lower_tier_funds, build_workflows
from .ai_k1_service import build_k1_and_related_inputs
from .ai_form_service import build_all_form_inputs
from .ai_pfic_service import build_pfic_snapshot, build_pfic_elections, build_pfic_allocation_input, apply_pfic_election_deletes, apply_part_v_vii_flags
from .ai_pfic_flowup_service import build_pfic_flowup_pipeline, build_custom_footnote_input, check_pfic_xml_override_alert
from .ai_finalization_service import (
    apply_tag_percentages, write_allocation_input, write_pfic_flowup,
    apply_master_feed_override, apply_blocker_entity_cleanup,
    apply_distribution_line_suppression, write_form_flowups,
    purge_output_tables,
)

logger = logging.getLogger(__name__)

# Builder profiling is inert unless ProfilePlan/profile_plan is enabled.
build_entity_hierarchy = track_plan(build_entity_hierarchy)
build_lower_tier_funds = track_plan(build_lower_tier_funds)
build_workflows = track_plan(build_workflows)
build_k1_and_related_inputs = track_plan(build_k1_and_related_inputs)
build_all_form_inputs = track_plan(build_all_form_inputs)
build_pfic_snapshot = track_plan(build_pfic_snapshot)
build_pfic_elections = track_plan(build_pfic_elections)
build_pfic_allocation_input = track_plan(build_pfic_allocation_input)
apply_pfic_election_deletes = track_plan(apply_pfic_election_deletes)
apply_part_v_vii_flags = track_plan(apply_part_v_vii_flags)
build_pfic_flowup_pipeline = track_plan(build_pfic_flowup_pipeline)
build_custom_footnote_input = track_plan(build_custom_footnote_input)
apply_master_feed_override = track_plan(apply_master_feed_override)
apply_blocker_entity_cleanup = track_plan(apply_blocker_entity_cleanup)
apply_distribution_line_suppression = track_plan(
    apply_distribution_line_suppression
)
apply_tag_percentages = track_plan(apply_tag_percentages)


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
    checkpoint_backend: str = "delta",
    CheckpointBackend: str = None,
    local_delta_denylist: object = "",
    LocalDeltaDenylist: object = None,
    profile_plan: bool = False,
    ProfilePlan: object = None,
    plan_checkpoint_threshold: int = 30,
    PlanCheckpointThreshold: int = None,
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
    cfg.setdefault("_cached_dataframes", [])
    cfg["max_threads"] = 4
    cfg["_checkpoint_backend"] = normalize_checkpoint_backend(
        CheckpointBackend or checkpoint_backend
    )
    denylist_value = (
        LocalDeltaDenylist
        if LocalDeltaDenylist is not None
        else local_delta_denylist
    )
    cfg["_local_delta_denylist"] = normalize_local_delta_denylist(
        denylist_value
    )
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
    cfg.setdefault("_plan_profile", [])
    # Persist output-routing settings so the final flush works in every mode
    # (Mode 1/2 pass cfg directly; Mode 3 built it above).
    if volume_path:
        cfg["volume_path"] = volume_path
    cfg.setdefault("result_type", result_type)
    cfg.setdefault("execution_id", execution_id)

    print(
        "========================================================\n"
        "[updated] output.updated.load_allocation_input starting\n"
        f"[updated] RunID={cfg.get('run_id')} EntityID={cfg.get('entity_id')} "
        f"ClientID={cfg.get('client_id')} TaxPeriodID={cfg.get('tax_period_id')}\n"
        f"[updated] max_threads=4 checkpoint_backend={cfg['_checkpoint_backend']} "
        f"local_deny_list={sorted(cfg['_local_delta_denylist']) or 'none'} "
        f"profile_plan={cfg['profile_plan']}\n"
        "========================================================"
    )

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
    register_shared_views(spark, cfg)
    print(f"[phase 1] Config + shared views: {time.time() - t_phase:.1f}s")

    # Phase 2: Build entity hierarchy tree + lower-tier fund lookups + workflow IDs
    t_phase = time.time()
    hierarchy_df = build_entity_hierarchy(spark, cfg)
    lower_tier_df = cache_for_run(
        build_lower_tier_funds(spark, cfg), cfg, broadcast=True
    )
    workflows = build_workflows(spark, cfg)
    print(f"[phase 2] Hierarchy + workflows: {time.time() - t_phase:.1f}s")

    # Phase 3: Run pre-conditions (data integrity checks); abort early if invalid
    t_phase = time.time()
    should_continue = run_validations(spark, cfg, lower_tier_df)
    print(f"[phase 3] Validations: {time.time() - t_phase:.1f}s")
    if not should_continue:
        unpersist_cached(cfg)
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
    pfic_snapshot_df = checkpoint(spark, pfic_snapshot_df, "pfic_snapshot", cfg)
    pfic_snapshot_df = cache_for_run(pfic_snapshot_df, cfg)
    pfic_snapshot_df.createOrReplaceTempView(f"_pfic_snapshot_{cfg['run_id']}")
    # This builder returns a dictionary of election DataFrames, not one frame.
    pfic_elections = build_pfic_elections(spark, cfg, pfic_snapshot_df)
    pfic_alloc_df = build_pfic_allocation_input(spark, cfg, pfic_snapshot_df, pfic_elections)
    allocation_input_df = allocation_input_df.unionByName(pfic_alloc_df, allowMissingColumns=True)

    custom_fn_df = build_custom_footnote_input(spark, cfg)
    allocation_input_df = allocation_input_df.unionByName(custom_fn_df, allowMissingColumns=True)

    print(f"[phase 6] PFIC + custom footnote: {time.time() - t_phase:.1f}s")

    # Checkpoint: materialize accumulated allocation DF to break lineage
    t_phase = time.time()
    allocation_input_df = checkpoint(spark, allocation_input_df, "alloc_input", cfg)
    print(f"[checkpoint] alloc_input: {time.time() - t_phase:.1f}s")

    # Phase 7a: PFIC flowup — tier-up foreign corp footnotes through entity hierarchy
    t_phase = time.time()
    pfic_flowup_df = build_pfic_flowup_pipeline(
        spark, cfg, pfic_snapshot_df, pfic_elections, lower_tier_df
    )
    pfic_flowup_df = checkpoint(spark, pfic_flowup_df, "pfic_raw", cfg)
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
    pfic_flowup_df = checkpoint(spark, pfic_flowup_df, "pfic_flowup", cfg)
    print(f"[checkpoint] pfic_flowup: {time.time() - t_phase:.1f}s")

    # Post-processing filters: master feed override, blocker-entity cleanup, suppression
    allocation_input_df = apply_master_feed_override(spark, cfg, allocation_input_df)
    allocation_input_df = apply_blocker_entity_cleanup(spark, cfg, allocation_input_df)
    allocation_input_df = apply_distribution_line_suppression(spark, cfg, allocation_input_df)

    t_phase = time.time()
    allocation_input_df = checkpoint(spark, allocation_input_df, "alloc_filtered", cfg)
    print(f"[checkpoint] alloc_filtered: {time.time() - t_phase:.1f}s")

    # Phase 8: Apply investment-level tag percentages (PE Book Allocation only)
    t_phase = time.time()
    allocation_input_df = apply_tag_percentages(spark, cfg, allocation_input_df)
    print(f"[phase 8] Tag percentages: {time.time() - t_phase:.1f}s")

    if cfg.get("investment_tag_workflow_id", 0) != 0:
        t_phase = time.time()
        allocation_input_df = checkpoint(spark, allocation_input_df, "alloc_tagged", cfg)
        print(f"[checkpoint] alloc_tagged: {time.time() - t_phase:.1f}s")

    # Phase 9: Final aggregation + Delta writes (AllocationInput, PFICFlowup, FormFlowups)
    t_phase = time.time()
    # Result builders mostly construct plans and can perform scalar Connect
    # actions. Run them sequentially to avoid RPC contention; only the final,
    # independent physical table writes use the four-thread pool.
    for writer, args in (
        (write_allocation_input, (allocation_input_df,)),
        (write_pfic_flowup, (pfic_flowup_df,)),
        (write_form_flowups, ()),
    ):
        local_cfg = isolated_collector_cfg(cfg)
        writer(spark, local_cfg, *args)
        merge_collector_cfg(cfg, local_cfg)
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
            profile_dataframe(
                "AllocationInput.write", alloc_df, cfg, kind="action"
            )
            alloc_df.write.format("delta").mode("overwrite") \
                .option("replaceWhere", f"RunID = {run_id}") \
                .saveAsTable(f"{prefix}.AllocationInput")
            print("   [ok] AllocationInput (delta)")

        # ── Write 2: all other tables → Parquet (via GenericResultStorer) ──
        parquet_tables = {}
        for tbl_name, df in parquet_results.items():
            if tbl_name == "AllocationInput":
                continue
            write_df = _align(df, tbl_name)
            if tbl_name in small_tables:
                write_df = write_df.coalesce(1)
            profile_dataframe(
                f"{tbl_name}.write", write_df, cfg, kind="action"
            )
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
                    f"[store] Writing {len(parquet_tables)} flow-up tables "
                    f"with 4 threads: {datetime.now()}",
                    flush=True,
                )
                print(
                    "[parallel:note] flow-up-writes: "
                    f"{list(parquet_tables)}",
                    flush=True,
                )

                def _store_one(table_name, table_df):
                    storer = GenericResultStorer(spark, None)
                    return storer.save_results(
                        result={table_name: table_df},
                        **storer_kwargs,
                    )

                write_tasks = [
                    (
                        table_name,
                        lambda table_name=table_name, table_df=table_df: _store_one(
                            table_name, table_df
                        ),
                    )
                    for table_name, table_df in parquet_tables.items()
                ]
                for table_name, value in run_parallel(
                    write_tasks, "flow-up-writes"
                ):
                    if value and not save_return_value:
                        save_return_value = value
                    print(f"   [ok] {table_name}")
                print(f"[done] Stored {len(parquet_tables)} Parquet tables: {datetime.now()}")
            except Exception as e:
                logger.error(
                    f"Parquet flow-up write failed (AllocationInput already committed): "
                    f"{type(e).__name__}: {e}"
                )
                raise

    elapsed = time.time() - t0
    log_timing("run_load_allocation_input", t0)
    plan_profile = (
        plan_profile_report(cfg) if cfg.get("profile_plan") else None
    )
    unpersist_cached(cfg)
    drop_checkpoints(spark, cfg)  # clean up temp checkpoint tables

    if (
        save_return_value
        and isinstance(save_return_value, str)
        and save_return_value.strip().startswith("{")
    ):
        try:
            stored_result = json.loads(save_return_value)
            if isinstance(stored_result, dict):
                stored_result.update(
                    {
                        "elapsed_seconds": round(elapsed, 1),
                        "max_threads": 4,
                        "checkpoint_backend": cfg.get("_checkpoint_backend"),
                        "checkpoint_timings": cfg.get(
                            "_checkpoint_elapsed", []
                        ),
                        "plan_profile": plan_profile,
                    }
                )
                return stored_result
        except ValueError:
            return save_return_value

    return {
        "status": "SUCCESS",
        "elapsed_seconds": round(elapsed, 1),
        "max_threads": 4,
        "checkpoint_backend": cfg.get("_checkpoint_backend"),
        "checkpoint_timings": cfg.get("_checkpoint_elapsed", []),
        "plan_profile": plan_profile,
    }