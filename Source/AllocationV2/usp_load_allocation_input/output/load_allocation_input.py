"""Production orchestrator for uspLoadAllocationInput."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config
from Common_V2.core.execution_profiles import resolve_execution_profile
from Common_V2.core.helpers import log_section, log_timing

from .ai_config_service import load_config
from .ai_finalization_service import (
    apply_blocker_entity_cleanup,
    apply_distribution_line_suppression,
    apply_master_feed_override,
    apply_tag_percentages,
    purge_output_tables,
)
from .ai_form_service import build_all_form_inputs
from .ai_hierarchy_service import (
    build_entity_hierarchy,
    build_lower_tier_funds,
    build_workflows,
)
from .ai_k1_service import build_k1_and_related_inputs
from .ai_pfic_flowup_service import (
    build_custom_footnote_input,
    build_pfic_flowup_pipeline,
    check_pfic_xml_override_alert,
)
from .ai_pfic_service import (
    apply_part_v_vii_flags,
    apply_pfic_election_deletes,
    build_pfic_allocation_input,
    build_pfic_snapshot,
)
from .ai_shared_views import register_shared_views
from .ai_validation_service import run_validations
from .parallel_helpers import (
    normalize_workers,
    parse_enabled_groups,
    run_parallel,
)
from .write_helpers import (
    collect_output_frames_parallel,
    flush_collected_results,
)

logger = logging.getLogger(__name__)

_LAST_RUN_PROFILE = {}


def _blank(value):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _as_int(value):
    if _blank(value) is None:
        return None
    return int(value)


@contextmanager
def _timed(timings, step):
    started = time.perf_counter()
    try:
        yield
    finally:
        timings.append(
            {
                "step": step,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
            }
        )


def _checkpoint(spark, df, name, cfg):
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = checkpoint(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    return result


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def run_load_allocation_input(
    spark,
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
    max_threads: int = None,
    MaxThreads: int = None,
    parallel_groups: str = "all",
    ParallelGroups: str = None,
    execution_profile: str = "low",
    ExecutionProfile: str = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    SqlShufflePartitions=None,
    **kwargs,
) -> dict:
    del kwargs
    global _LAST_RUN_PROFILE
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
    timings = []
    parallel_activity = []
    enabled_groups = parse_enabled_groups(parallel_groups, ParallelGroups)
    profile_name = (
        _blank(ExecutionProfile) or _blank(execution_profile) or "low"
    )
    profile = resolve_execution_profile(profile_name)
    workers = normalize_workers(
        max_threads=(
            MaxThreads
            if MaxThreads is not None
            else max_threads
            if max_threads is not None
            else profile["max_threads"]
        ),
        MaxThreads=MaxThreads,
    )
    mode = None
    save_return_value = None
    log_section("run_load_allocation_input")

    try:
        with _timed(timings, "S1 config and views"):
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
            cfg = {**cfg}
            cfg.setdefault("_checkpoint_tables", [])
            cfg.setdefault("_parquet_results", {})
            if volume_path:
                cfg["volume_path"] = volume_path
            cfg.setdefault("result_type", result_type)
            cfg.setdefault("execution_id", execution_id)

            shuffle_override = _as_int(SqlShufflePartitions)
            if shuffle_override is None:
                spark.conf.set(
                    "spark.sql.shuffle.partitions",
                    str(profile["shuffle_partitions"]),
                )
            else:
                spark.conf.set(
                    "spark.sql.shuffle.partitions",
                    str(shuffle_override),
                )
            explicit_checkpoint = (
                CheckpointMode
                if CheckpointMode is not None
                else checkpoint_mode
            )
            mode = resolve_checkpoint_mode(
                cfg,
                checkpoint_mode=(
                    explicit_checkpoint
                    if _blank(explicit_checkpoint) is not None
                    else profile["checkpoint_mode"]
                ),
                CheckpointMode=CheckpointMode,
            )
            cfg.update(
                {
                    "_checkpoint_paths": [],
                    "_checkpoint_v2_activity": [],
                    "checkpoint_mode": mode,
                    "max_threads": workers,
                    "execution_profile": profile_name,
                }
            )
            initialize_checkpoint_V2(cfg, mode)
            print(
                f"[CHECKPOINT_V2] ExecutionProfile={profile_name} "
                f"CheckpointMode={mode} shuffle="
                f"{shuffle_override or profile['shuffle_partitions']} "
                f"MaxThreads={workers}"
            )
            if cfg.get("run_status") == "FAIL":
                logger.error(
                    f"RunStatus=FAIL - aborting. RunID={cfg.get('run_id')}, "
                    f"EntityID={cfg.get('entity_id')}"
                )
                return {"status": "FAIL", "reason": "run_status_fail"}
            cfg = load_config(spark, cfg)
            register_shared_views(spark, cfg)

        with _timed(timings, "S2 hierarchy and workflows"):
            build_entity_hierarchy(spark, cfg)
            lower_tier_df = build_lower_tier_funds(spark, cfg)
            k1_workflow_df, at_risk_workflow_df = build_workflows(spark, cfg)
            del at_risk_workflow_df

        with _timed(timings, "S3 validations"):
            should_continue = run_validations(spark, cfg, lower_tier_df)
            if not should_continue:
                return {"status": "FAIL", "reason": "validation_failed"}
            purge_output_tables(spark, cfg)

        with _timed(timings, "S4-S6 independent input builders"):
            form_df, k1_df, pfic_snapshot_df = run_parallel(
                [
                    (
                        "build_all_form_inputs",
                        lambda: build_all_form_inputs(
                            spark, {**cfg}, k1_workflow_df
                        ),
                    ),
                    (
                        "build_k1_and_related_inputs",
                        lambda: build_k1_and_related_inputs(
                            spark, {**cfg}
                        ),
                    ),
                    (
                        "build_pfic_snapshot",
                        lambda: build_pfic_snapshot(
                            spark, {**cfg}, k1_workflow_df
                        ),
                    ),
                ],
                workers,
                parallel_activity,
                "independent_input_builders",
                enabled_groups,
            )

        with _timed(timings, "S6 unions and PFIC snapshot checkpoint"):
            pfic_snapshot_df = _checkpoint(
                spark, pfic_snapshot_df, "pfic_snapshot", cfg
            )
            allocation_input_df = form_df.unionByName(
                k1_df, allowMissingColumns=True
            )
            pfic_alloc_df = build_pfic_allocation_input(
                spark, cfg, pfic_snapshot_df
            )
            allocation_input_df = allocation_input_df.unionByName(
                pfic_alloc_df, allowMissingColumns=True
            )
            custom_fn_df = build_custom_footnote_input(spark, cfg)
            allocation_input_df = allocation_input_df.unionByName(
                custom_fn_df, allowMissingColumns=True
            )
            allocation_input_df = _checkpoint(
                spark, allocation_input_df, "alloc_input", cfg
            )

        with _timed(timings, "S7 PFIC flowup"):
            pfic_flowup_df = build_pfic_flowup_pipeline(
                spark, cfg, pfic_snapshot_df, lower_tier_df
            )
            pfic_flowup_df = _checkpoint(
                spark, pfic_flowup_df, "pfic_raw", cfg
            )
            check_pfic_xml_override_alert(spark, cfg, pfic_flowup_df)
            allocation_input_df, pfic_flowup_df = apply_pfic_election_deletes(
                spark, cfg, allocation_input_df, pfic_flowup_df, lower_tier_df
            )
            pfic_flowup_df = apply_part_v_vii_flags(
                spark, cfg, pfic_flowup_df
            )
            pfic_flowup_df = _checkpoint(
                spark, pfic_flowup_df, "pfic_flowup", cfg
            )

        with _timed(timings, "S8 filters and tags"):
            allocation_input_df = apply_master_feed_override(
                spark, cfg, allocation_input_df
            )
            allocation_input_df = apply_blocker_entity_cleanup(
                spark, cfg, allocation_input_df
            )
            allocation_input_df = apply_distribution_line_suppression(
                spark, cfg, allocation_input_df
            )
            allocation_input_df = _checkpoint(
                spark, allocation_input_df, "alloc_filtered", cfg
            )
            allocation_input_df = apply_tag_percentages(
                spark, cfg, allocation_input_df
            )
            if cfg.get("investment_tag_workflow_id", 0) != 0:
                allocation_input_df = _checkpoint(
                    spark, allocation_input_df, "alloc_tagged", cfg
                )

        with _timed(timings, "S9 collect and store"):
            collect_output_frames_parallel(
                spark,
                cfg,
                allocation_input_df,
                pfic_flowup_df,
                k1_workflow_df,
                workers,
                parallel_activity,
                enabled_groups,
            )
            save_return_value = flush_collected_results(
                spark,
                cfg,
                cfg.get("client_id", client_id),
                cfg.get("entity_id", entity_id),
                cfg.get("execution_id", execution_id) or "1",
                workers,
                parallel_activity,
                enabled_groups,
            )
    except Exception:
        raise
    finally:
        elapsed = time.time() - t0
        _LAST_RUN_PROFILE = {
            "timings": list(timings),
            "parallel_activity": list(parallel_activity),
            "checkpoint_activity": (
                list(cfg.get("_checkpoint_v2_activity", []))
                if isinstance(cfg, dict)
                else []
            ),
            "elapsed_seconds": round(elapsed, 1),
            "checkpoint_mode": mode,
            "max_threads": workers,
            "execution_profile": profile_name,
        }

    if isinstance(cfg, dict):
        drop_checkpoints_V2(spark, cfg)
    log_timing("run_load_allocation_input", t0)
    print(f"Overall time in Seconds: {elapsed:.1f}s")

    if (
        save_return_value
        and isinstance(save_return_value, str)
        and save_return_value.strip().startswith("{")
    ):
        return save_return_value

    return {
        "status": "SUCCESS",
        "elapsed_seconds": round(elapsed, 1),
    }


__all__ = [
    "checkpoint",
    "drop_checkpoints_V2",
    "get_last_run_profile",
    "run_load_allocation_input",
]
