"""
load_allocation_input.py — optimized pipeline (updated package).

Monolith path:
  Source/AllocationV2/usp_load_allocation_input/output/updated/

Import:
  AllocationV2.usp_load_allocation_input.output.updated.load_allocation_input
  AllocationV2.usp_load_allocation_input.output.updated.load_allocation_input_updated  # shim
"""

from pyspark.sql import SparkSession
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import table_prefix, log_section, log_timing

from .checkpoint import (
    drop_checkpoints,
    log_checkpoint_plan,
    pipeline_checkpoint,
    should_checkpoint,
)
from .step_timer import StepTimer
from .shared_views import register_shared_views_parallel
from . import ai_pfic_flowup_service as _pfic_flowup_svc
from .parent import output_module

_log_pfic = _pfic_flowup_svc._log
_log_pfic("wired into updated.load_allocation_input")

_ai_config = output_module("ai_config_service")
_ai_validation = output_module("ai_validation_service")
_ai_hierarchy = output_module("ai_hierarchy_service")
_ai_k1 = output_module("ai_k1_service")
_ai_form = output_module("ai_form_service")
_ai_pfic = output_module("ai_pfic_service")
_ai_finalization = output_module("ai_finalization_service")

load_config = _ai_config.load_config
run_validations = _ai_validation.run_validations
build_entity_hierarchy = _ai_hierarchy.build_entity_hierarchy
build_lower_tier_funds = _ai_hierarchy.build_lower_tier_funds
build_workflows = _ai_hierarchy.build_workflows
build_k1_and_related_inputs = _ai_k1.build_k1_and_related_inputs
build_all_form_inputs = _ai_form.build_all_form_inputs
build_pfic_snapshot = _ai_pfic.build_pfic_snapshot
build_pfic_elections = _ai_pfic.build_pfic_elections
build_pfic_allocation_input = _ai_pfic.build_pfic_allocation_input
apply_pfic_election_deletes = _ai_pfic.apply_pfic_election_deletes
apply_part_v_vii_flags = _ai_pfic.apply_part_v_vii_flags
build_pfic_flowup_pipeline = _pfic_flowup_svc.build_pfic_flowup_pipeline
build_custom_footnote_input = _pfic_flowup_svc.build_custom_footnote_input
check_pfic_xml_override_alert = _pfic_flowup_svc.check_pfic_xml_override_alert
apply_tag_percentages = _ai_finalization.apply_tag_percentages
write_allocation_input = _ai_finalization.write_allocation_input
write_pfic_flowup = _ai_finalization.write_pfic_flowup
apply_master_feed_override = _ai_finalization.apply_master_feed_override
apply_blocker_entity_cleanup = _ai_finalization.apply_blocker_entity_cleanup
apply_distribution_line_suppression = _ai_finalization.apply_distribution_line_suppression
write_form_flowups = _ai_finalization.write_form_flowups
purge_output_tables = _ai_finalization.purge_output_tables

logger = logging.getLogger(__name__)

_SPARK_PARQUET_CODEC_KEY = "spark.sql.parquet.compression.codec"


def _begin_uncompressed_writes(spark: SparkSession) -> dict[str, str]:
    """Session-level Parquet codec for Delta/Parquet writes (restored after)."""
    previous: dict[str, str] = {}
    try:
        previous[_SPARK_PARQUET_CODEC_KEY] = spark.conf.get(_SPARK_PARQUET_CODEC_KEY)
    except Exception:
        pass
    spark.conf.set(_SPARK_PARQUET_CODEC_KEY, "uncompressed")
    return previous


def _restore_write_compression(spark: SparkSession, previous: dict[str, str]) -> None:
    for key, value in previous.items():
        try:
            spark.conf.set(key, value)
        except Exception:
            pass


def _timed_fail(timer: StepTimer, reason: str, **extra: object) -> dict:
    timer.print_summary()
    return {"status": "FAIL", "reason": reason, "timings": timer.as_dict_list(), **extra}


def _maybe_checkpoint(
    spark: SparkSession,
    timer: StepTimer,
    df: Any,
    name: str,
    cfg: dict,
) -> Any:
    if not should_checkpoint(cfg, name):
        return df
    ckpt_fn = pipeline_checkpoint
    with timer.step(f"checkpoint_{name}"):
        return ckpt_fn(spark, df, name, cfg)


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
    parallel_config_workers: int = None,
    ParallelConfigWorkers: int = None,
    parallel_write_workers: int = None,
    ParallelWriteWorkers: int = None,
    checkpoint_backend: str = None,
    CheckpointBackend: str = None,
    **kwargs,
) -> dict:
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
    log_section("load_allocation_input (updated)")
    timer = StepTimer(logger=logger)

    def _worker_count(
        *values: object,
        default: int = 4,
    ) -> int:
        for raw in values:
            if raw is None:
                continue
            try:
                n = int(raw)
            except (TypeError, ValueError):
                continue
            if n >= 1:
                return n
        return max(1, default)

    parallel_config_workers = _worker_count(
        parallel_config_workers,
        ParallelConfigWorkers,
        kwargs.get("parallel_config_workers"),
        kwargs.get("ParallelConfigWorkers"),
        cfg.get("parallel_config_workers") if cfg else None,
        default=4,
    )
    parallel_write_workers = _worker_count(
        parallel_write_workers,
        ParallelWriteWorkers,
        kwargs.get("parallel_write_workers"),
        kwargs.get("ParallelWriteWorkers"),
        cfg.get("parallel_write_workers") if cfg else None,
        default=parallel_config_workers,
    )

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

    cfg.setdefault("_checkpoint_tables", [])
    cfg.setdefault("_parquet_results", {})
    cfg.setdefault("_checkpoint_paths", [])
    _ckpt_backend = (
        checkpoint_backend
        or CheckpointBackend
        or kwargs.get("checkpoint_backend")
        or kwargs.get("CheckpointBackend")
        or cfg.get("checkpoint_backend")
    )
    if _ckpt_backend:
        cfg["checkpoint_backend"] = str(_ckpt_backend).strip().lower()
    if volume_path:
        cfg["volume_path"] = volume_path.strip()
        cfg.setdefault("checkpoint_backend", "volume")
        cfg.setdefault("checkpoint_use_production", False)
    from .checkpoint import _resolve_backend

    _backend = _resolve_backend(cfg)
    _comp = cfg.get("checkpoint_compression", "uncompressed")
    if volume_path:
        print(
            f"[checkpoint] backend={_backend} compression={_comp} | "
            f"checkpoints: {cfg['volume_path']}/_checkpoints/ | "
            f"flow-up outputs: {cfg['volume_path']}"
        )
    else:
        print(f"[checkpoint] backend={_backend} compression={_comp}")
    cfg.setdefault("result_type", result_type)
    cfg.setdefault("execution_id", execution_id)
    cfg["parallel_config_workers"] = parallel_config_workers
    cfg["parallel_write_workers"] = parallel_write_workers
    cfg.setdefault("write_compression", "uncompressed")
    cfg.setdefault("checkpoint_compression", cfg.get("write_compression", "uncompressed"))
    if _backend == "volume":
        print(
            "[checkpoint] pipeline breaks: UC Volume Parquet "
            "(serverless-safe; skips UC Delta table commits)"
        )
    log_checkpoint_plan(cfg)
    print(
        f"[updated] parallel_config_workers={cfg['parallel_config_workers']} "
        f"parallel_write_workers={cfg['parallel_write_workers']}"
    )
    print("[updated] PFIC flowup: output.updated.ai_pfic_flowup_service")
    write_workers = cfg["parallel_write_workers"]
    if write_workers > 1:
        print(f"[write] parallel flow-up table writes: max_workers={write_workers}")
    if str(cfg.get("write_compression", "")).lower() in ("uncompressed", "none"):
        print("[write] parquet compression: uncompressed (Delta + flow-up outputs)")

    if cfg.get("run_status") == "FAIL":
        drop_checkpoints(spark, cfg)
        return _timed_fail(timer, "run_status_fail")

    with timer.step("phase_1_config_and_shared_views"):
        cfg = load_config(spark, cfg)
        register_shared_views_parallel(spark, cfg)

    with timer.step("phase_2_hierarchy_and_workflows"):
        hierarchy_df = build_entity_hierarchy(spark, cfg)
        lower_tier_df = build_lower_tier_funds(spark, cfg)
        workflows = build_workflows(spark, cfg)

    with timer.step("phase_3_validations"):
        should_continue = run_validations(spark, cfg, lower_tier_df)
        if not should_continue:
            drop_checkpoints(spark, cfg)
            return _timed_fail(timer, "validation_failed")

    with timer.step("purge_output_tables"):
        purge_output_tables(spark, cfg)

    with timer.step("phase_4_form_inputs"):
        allocation_input_df = build_all_form_inputs(spark, cfg)

    with timer.step("phase_5_k1_and_related_inputs"):
        k1_df = build_k1_and_related_inputs(spark, cfg, workflows)
        allocation_input_df = allocation_input_df.unionByName(k1_df, allowMissingColumns=True)

    with timer.step("phase_6a_pfic_snapshot"):
        pfic_snapshot_df = build_pfic_snapshot(spark, cfg)

    pfic_snapshot_df = _maybe_checkpoint(spark, timer, pfic_snapshot_df, "pfic_snapshot", cfg)
    pfic_snapshot_df.createOrReplaceTempView(f"_pfic_snapshot_{cfg['run_id']}")

    with timer.step("phase_6b_pfic_elections_and_alloc"):
        pfic_elections = build_pfic_elections(spark, cfg, pfic_snapshot_df)
        pfic_alloc_df = build_pfic_allocation_input(spark, cfg, pfic_snapshot_df, pfic_elections)
        allocation_input_df = allocation_input_df.unionByName(pfic_alloc_df, allowMissingColumns=True)

    with timer.step("phase_6c_custom_footnote_input"):
        custom_fn_df = build_custom_footnote_input(spark, cfg)
        allocation_input_df = allocation_input_df.unionByName(custom_fn_df, allowMissingColumns=True)

    allocation_input_df = _maybe_checkpoint(spark, timer, allocation_input_df, "alloc_input", cfg)

    with timer.step("phase_7a_pfic_flowup_build"):
        pfic_flowup_df = build_pfic_flowup_pipeline(
            spark, cfg, pfic_snapshot_df, pfic_elections, lower_tier_df
        )

    pfic_flowup_df = _maybe_checkpoint(spark, timer, pfic_flowup_df, "pfic_raw", cfg)

    with timer.step("phase_7b_pfic_election_deletes_and_flags"):
        check_pfic_xml_override_alert(spark, cfg, pfic_flowup_df)
        allocation_input_df, pfic_flowup_df = apply_pfic_election_deletes(
            spark, cfg, allocation_input_df, pfic_flowup_df, pfic_elections, lower_tier_df
        )
        pfic_flowup_df = apply_part_v_vii_flags(spark, cfg, pfic_flowup_df)

    pfic_flowup_df = _maybe_checkpoint(spark, timer, pfic_flowup_df, "pfic_flowup", cfg)

    with timer.step("post_filters"):
        allocation_input_df = apply_master_feed_override(spark, cfg, allocation_input_df)
        allocation_input_df = apply_blocker_entity_cleanup(spark, cfg, allocation_input_df)
        allocation_input_df = apply_distribution_line_suppression(spark, cfg, allocation_input_df)

    allocation_input_df = _maybe_checkpoint(
        spark, timer, allocation_input_df, "alloc_filtered", cfg
    )

    with timer.step("phase_8_tag_percentages"):
        allocation_input_df = apply_tag_percentages(spark, cfg, allocation_input_df)

    if cfg.get("investment_tag_workflow_id", 0) != 0:
        allocation_input_df = _maybe_checkpoint(
            spark, timer, allocation_input_df, "alloc_tagged", cfg
        )

    with timer.step("phase_9_write_outputs"):
        write_allocation_input(spark, cfg, allocation_input_df)
        write_pfic_flowup(spark, cfg, pfic_flowup_df)
        write_form_flowups(spark, cfg)

    parquet_results = cfg.get("_parquet_results", {})
    run_id = cfg["run_id"]
    save_return_value = None

    if parquet_results:
        from datetime import datetime
        from Common_V2.core.generic_result_storer import GenericResultStorer
        from pyspark.sql import functions as _F

        prefix = table_prefix(cfg)
        _schema_info = cfg.get("_schema_cache", {})
        _client_id = cfg.get("client_id", client_id)
        _entity_id = cfg.get("entity_id", entity_id)
        _execution_id = cfg.get("execution_id", execution_id) or "1"
        _volume_path = cfg.get("volume_path") or ""
        small_tables = {
            "Form926Flowup", "Form199AFlowup", "Form8865Flowup", "Form8886Flowup",
            "AtRiskFlowup", "CustomFootnoteFlowup", "Form200616Flowup",
            "PFICFootnoteFlowup", "PFICFootnoteFlowupWithTrackingKey",
        }

        def _align(df, tbl_name):
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
                    col_type = target_types.get(col_name)
                    out = out.withColumn(
                        col_name,
                        _F.lit(None).cast(col_type) if col_type is not None else _F.lit(None),
                    )
            return out.select(target_cols)

        use_uncompressed = str(cfg.get("write_compression", "")).lower() in (
            "uncompressed",
            "none",
        )
        write_conf_prev: dict[str, str] = {}
        if use_uncompressed:
            write_conf_prev = _begin_uncompressed_writes(spark)

        try:
            alloc_df = parquet_results.get("AllocationInput")
            if alloc_df is not None:
                with timer.step("write_allocation_input_delta"):
                    writer = (
                        alloc_df.write.format("delta")
                        .mode("overwrite")
                        .option("replaceWhere", f"RunID = {run_id}")
                    )
                    if use_uncompressed:
                        writer = writer.option("compression", "uncompressed")
                    writer.saveAsTable(f"{prefix}.AllocationInput")
                    print("   [ok] AllocationInput (delta)")

            def _prepare_flowup_table(tbl_name: str, df) -> tuple[str, Any]:
                write_df = _align(df, tbl_name)
                if tbl_name in small_tables:
                    write_df = write_df.coalesce(1)
                return tbl_name, write_df

            parquet_tables: dict[str, Any] = {}
            parallel_workers = int(cfg.get("parallel_write_workers", 1) or 1)
            flowup_sources = {
                n: d for n, d in parquet_results.items() if n != "AllocationInput"
            }

            with timer.step("prepare_parquet_flowup_tables"):
                if parallel_workers > 1 and len(flowup_sources) > 1:
                    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                        futures = [
                            executor.submit(_prepare_flowup_table, name, df)
                            for name, df in flowup_sources.items()
                        ]
                        for fut in as_completed(futures):
                            tbl_name, write_df = fut.result()
                            parquet_tables[tbl_name] = write_df
                else:
                    for tbl_name, df in flowup_sources.items():
                        name, write_df = _prepare_flowup_table(tbl_name, df)
                        parquet_tables[name] = write_df

            if parquet_tables:
                storer_kwargs = {
                    "result_type": cfg.get("result_type", "deltalake"),
                    "catalog_name": cfg.get("catalog", ""),
                    "database_name": cfg.get("schema", ""),
                    "run_id": run_id,
                    "client_id": _client_id,
                    "entity_id": _entity_id,
                    "execution_id": _execution_id,
                    "volume_path": _volume_path,
                    "sql_url_path": cfg.get("sql_url_path", ""),
                    "sql_username": cfg.get("sql_username", ""),
                    "sql_password": cfg.get("sql_password", ""),
                }

                def _save_one_flowup_table(tbl_name: str, write_df) -> str | None:
                    storer = GenericResultStorer(spark, None)
                    return storer.save_results(
                        result={tbl_name: write_df},
                        **storer_kwargs,
                    )

                with timer.step("write_parquet_flowup_tables"):
                    if parallel_workers > 1 and len(parquet_tables) > 1:
                        print(
                            f"[store] Writing {len(parquet_tables)} tables "
                            f"(parallel_workers={parallel_workers}, "
                            f"compression={cfg.get('write_compression')}): "
                            f"{datetime.now()}"
                        )
                        errors: list[str] = []
                        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                            futures = {
                                executor.submit(_save_one_flowup_table, name, df): name
                                for name, df in parquet_tables.items()
                            }
                            for fut in as_completed(futures):
                                tbl = futures[fut]
                                try:
                                    ret = fut.result()
                                    if ret and not save_return_value:
                                        save_return_value = ret
                                    print(f"   [ok] {tbl}")
                                except Exception as exc:
                                    errors.append(f"{tbl}: {exc}")
                        if errors:
                            raise RuntimeError(
                                "Parallel flow-up writes failed: " + "; ".join(errors)
                            )
                    else:
                        storer = GenericResultStorer(spark, None)
                        save_return_value = storer.save_results(
                            result=parquet_tables,
                            **storer_kwargs,
                        )
                    print(
                        f"[done] Stored {len(parquet_tables)} flow-up tables: "
                        f"{datetime.now()}"
                    )
        finally:
            if use_uncompressed:
                _restore_write_compression(spark, write_conf_prev)

    elapsed = time.time() - t0
    cfg["step_timings"] = timer.as_dict_list()
    timer.print_summary("load_allocation_input (updated)")
    log_timing("load_allocation_input (updated)", t0)

    drop_checkpoints(spark, cfg)

    if save_return_value and isinstance(save_return_value, str) and save_return_value.strip().startswith("{"):
        return save_return_value

    return {
        "status": "SUCCESS",
        "elapsed_seconds": round(elapsed, 1),
        "tracked_step_seconds": timer.total_elapsed_seconds(),
        "timings": timer.as_dict_list(),
        "implementation": "updated.load_allocation_input",
        "checkpoint_backend": cfg.get("checkpoint_backend"),
        "volume_path": cfg.get("volume_path") or "",
        "parallel_write_workers": int(cfg.get("parallel_write_workers", 1) or 1),
        "parallel_config_workers": int(cfg.get("parallel_config_workers", 1) or 1),
        "write_compression": cfg.get("write_compression"),
    }
