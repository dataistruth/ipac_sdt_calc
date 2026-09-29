"""Production orchestrator for look-through allocation input."""

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

from .lt_config_service import (
    build_fx_rates,
    load_config,
    load_lower_tier_funds,
    load_reclass_k1_data,
    load_workflows,
)
from .lt_finalization_service import (
    apply_blocker_entity,
    apply_line_exclusions,
    apply_master_feed_exclusion,
    apply_tag_percentages,
    build_box_jkl_input,
    validate_k3_rules,
)
from .lt_flowup_service import (
    build_lt_flowup_adjustment,
    build_lt_flowup_m1,
)
from .lt_helpers import logger
from .lt_k1_service import (
    build_adjustments_input,
    build_k1_input,
    build_lt_flowup_k1,
    build_rounding_diff,
    recompute_lt_input_from_lower_tier,
)
from .lt_pfic_conversion_service import build_pfic_conversion
from .lt_pfic_service import build_pfic_elections, build_pfic_mapped_lines
from .parallel_helpers import (
    normalize_workers,
    parse_enabled_groups,
    run_parallel,
)
from .write_helpers import (
    build_pfic_income_attributes,
    write_final_output_parallel,
)

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


def _checkpoint(spark, df, name, cfg):
    """Extra plan-break seams plus local qualifier reset."""
    if df is None:
        print(f"[CHECKPOINT_V2] checkpoint {name}: skipped (df=None)", flush=True)
        return None
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = checkpoint(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    print(f"[CHECKPOINT_V2] checkpoint {name}: done", flush=True)
    return result


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


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def run_load_lookthrough_allocation_input(
    spark,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = "dev7",
    SchemaName: str = "iPC_2025_dev7_15349",
    cfg: dict = None,
    verbose: bool = False,
    ResultType: str = "deltalake",
    VolumePath: str = None,
    ExecutionID: str = None,
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
):
    """Run production semantics with V2 checkpoints and bounded plan pools."""
    del kwargs
    global _LAST_RUN_PROFILE
    started = time.perf_counter()
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
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    status = {
        "sp_name": "uspLoadLookThroughAllocationInput",
        "run_id": RunID,
        "entity_id": EntityID,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    try:
        with _timed(timings, "S1 config"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    entity_id=EntityID,
                    client_id=ClientID,
                    tax_period_id=TaxPeriodID,
                    run_id=RunID,
                    catalog=CatalogName,
                    schema=SchemaName,
                )
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
            cfg = {**cfg}
            cfg.setdefault("_checkpoint_tables", [])
            load_config(spark, cfg)
            cfg.setdefault("result_type", ResultType)
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID
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
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            if cfg["run_status"] == "FAIL":
                status["status"] = "SKIPPED"
                status["error"] = "RunStatus=FAIL"
                return status
            status["sections_completed"] = 1

        # Each task only builds and returns a DataFrame; none mutates cfg or writes.
        with _timed(timings, "S2-S4 independent loads"):
            (
                workflow_data,
                lower_tier_funds_df,
                reclass_k1_df,
            ) = run_parallel(
                [
                    ("load_workflows", lambda: load_workflows(spark, {**cfg})),
                    (
                        "load_lower_tier_funds",
                        lambda: load_lower_tier_funds(spark, {**cfg}),
                    ),
                    (
                        "load_reclass_k1_data",
                        lambda: load_reclass_k1_data(spark, {**cfg}),
                    ),
                ],
                workers,
                parallel_activity,
                "independent_early_loads",
                enabled_groups,
            )
            k1_workflow_df, adjustment_workflow_df = workflow_data
            lower_tier_funds_df = _checkpoint(
                spark, lower_tier_funds_df, "lower_tier_funds", cfg
            )
            reclass_k1_df = _checkpoint(spark, reclass_k1_df, "reclass_k1", cfg)
            fx_rates_df = build_fx_rates(spark, cfg, k1_workflow_df)
            fx_rates_df = _checkpoint(spark, fx_rates_df, "fx_rates", cfg)
            status["sections_completed"] = 4

        # These builders consume immutable inputs and return disjoint plans.
        with _timed(timings, "S5-S7 independent input builders"):
            k1_input_df, adj_input_df, lower_tier_amount_df = run_parallel(
                [
                    (
                        "build_k1_input",
                        lambda: build_k1_input(
                            spark, {**cfg}, k1_workflow_df, fx_rates_df
                        ),
                    ),
                    (
                        "build_adjustments_input",
                        lambda: build_adjustments_input(
                            spark, {**cfg}, adjustment_workflow_df, fx_rates_df
                        ),
                    ),
                    (
                        "build_lt_flowup_k1",
                        lambda: build_lt_flowup_k1(
                            spark, {**cfg}, reclass_k1_df
                        ),
                    ),
                ],
                workers,
                parallel_activity,
                "independent_input_builders",
                enabled_groups,
            )
            status["sections_completed"] = 7

        # Shared alloc_input, PFIC, validation, and all RunID mutations stay ordered.
        with _timed(timings, "S8-S10 sequential union pipeline"):
            lower_tier_amount_df = build_rounding_diff(
                spark, cfg, lower_tier_amount_df
            )
            lower_tier_amount_df = _checkpoint(
                spark, lower_tier_amount_df, "lower_tier_amount", cfg
            )
            lt_k1_input_df = recompute_lt_input_from_lower_tier(
                cfg, lower_tier_amount_df
            )
            alloc_input_df = k1_input_df
            if adj_input_df is not None:
                alloc_input_df = alloc_input_df.unionByName(
                    adj_input_df, allowMissingColumns=True
                )
            alloc_input_df = alloc_input_df.unionByName(
                lt_k1_input_df, allowMissingColumns=True
            )
            lt_adj_input_df = build_lt_flowup_adjustment(
                spark, cfg, lower_tier_funds_df
            )
            if lt_adj_input_df is not None:
                alloc_input_df = alloc_input_df.unionByName(
                    lt_adj_input_df, allowMissingColumns=True
                )
            lt_m1_input_df = build_lt_flowup_m1(
                spark, cfg, lower_tier_funds_df
            )
            if lt_m1_input_df is not None:
                alloc_input_df = alloc_input_df.unionByName(
                    lt_m1_input_df, allowMissingColumns=True
                )
            alloc_input_df = _checkpoint(
                spark, alloc_input_df, "alloc_input_post_unions", cfg
            )
            status["sections_completed"] = 10

        with _timed(timings, "S11-S14 sequential PFIC pipeline"):
            pfic_data = build_pfic_elections(spark, cfg)
            (
                alloc_input_df,
                pfic_mapped_df,
                _fcc_blocked_df,
                reclass_unblocked_df,
            ) = build_pfic_mapped_lines(spark, cfg, alloc_input_df)
            pfic_mapped_df = _checkpoint(
                spark, pfic_mapped_df, "pfic_mapped", cfg
            )
            pfic_alloc_input_df, converted_pfic_amounts_df = (
                build_pfic_conversion(
                    spark,
                    cfg,
                    reclass_unblocked_df,
                    pfic_mapped_df,
                    pfic_data,
                    lower_tier_funds_df,
                    pfic_data.get("pfic_footnote_entity_details"),
                    lower_tier_amount_df,
                    pfic_types_df=pfic_data.get("pfic_types"),
                )
            )
            if pfic_alloc_input_df is not None:
                alloc_input_df = alloc_input_df.unionByName(
                    pfic_alloc_input_df, allowMissingColumns=True
                )
            pfic_income_attr_df = build_pfic_income_attributes(
                spark,
                cfg,
                converted_pfic_amounts_df,
                lower_tier_amount_df,
                pfic_mapped_df,
                lower_tier_funds_df,
            )
            alloc_input_df = _checkpoint(
                spark, alloc_input_df, "alloc_input_post_pfic", cfg
            )
            status["sections_completed"] = 14

        with _timed(timings, "S15-S19 sequential finalization"):
            alloc_input_df = build_box_jkl_input(
                spark, cfg, alloc_input_df, fx_rates_df
            )
            alloc_input_df = _checkpoint(
                spark, alloc_input_df, "alloc_input_box_jkl", cfg
            )
            alloc_input_df = apply_master_feed_exclusion(
                spark, cfg, alloc_input_df
            )
            alloc_input_df = apply_blocker_entity(spark, cfg, alloc_input_df)
            alloc_input_df = apply_tag_percentages(
                spark, cfg, alloc_input_df, reclass_k1_df
            )
            alloc_input_df = apply_line_exclusions(
                spark, cfg, alloc_input_df
            )
            alloc_input_df = _checkpoint(
                spark, alloc_input_df, "alloc_input_pre_write", cfg
            )
            status["sections_completed"] = 19

        with _timed(timings, "S20 validation"):
            k3_status = validate_k3_rules(spark, cfg, alloc_input_df)
            status["sections_completed"] = 20
            if k3_status == "FAIL":
                status["status"] = "FAIL"
                status["error"] = "K3 Validations failed"
                return status

        with _timed(timings, "S21 final writes"):
            write_final_output_parallel(
                spark,
                cfg,
                alloc_input_df,
                lower_tier_funds_df,
                pfic_income_attr_df,
                workers,
                parallel_activity,
                enabled_groups,
                run_parallel,
            )
            status["sections_completed"] = 21
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error("[ERROR] %s", exc, exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(time.perf_counter() - started, 1)
        _LAST_RUN_PROFILE = {
            "timings": list(timings),
            "parallel_activity": list(parallel_activity),
            "checkpoint_activity": (
                list(cfg.get("_checkpoint_v2_activity", []))
                if isinstance(cfg, dict)
                else []
            ),
            "elapsed_seconds": status["elapsed_seconds"],
            "checkpoint_mode": mode,
            "max_threads": workers,
            "execution_profile": profile_name,
        }
    return status


__all__ = [
    "checkpoint",
    "drop_checkpoints_V2",
    "get_last_run_profile",
    "run_load_lookthrough_allocation_input",
]
