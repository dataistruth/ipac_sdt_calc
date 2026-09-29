"""Parity-first outputV2 orchestrator for look-through allocation detail step 01."""

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
from Common_V2.core.generic_result_storer import GenericResultStorer
import pyspark.sql.functions as F

from .parallel_helpers import (
    normalize_workers,
    parse_enabled_groups,
    run_parallel,
)
from .parent import output_module
from .plan_profiler import (
    plan_profile_report,
    profile_action,
    track_checkpoint_plan,
    track_plan,
)


def _as_bool(value):
    if isinstance(value, str):
        return value.strip().lower() in {"1", "on", "true", "yes"}
    return bool(value)


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


def _v2(msg):
    print(f"[outputV2] {msg}", flush=True)


def _checkpoint(spark, df, name, cfg):
    if df is None:
        _v2(f"checkpoint {name}: skipped (df=None)")
        return None
    if not hasattr(df, "columns"):
        return df
    track_checkpoint_plan(name, df, cfg)
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = checkpoint(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    _v2(f"checkpoint {name}: done")
    return result


def _emit_reports(cfg):
    reports = {"builder": [], "checkpoint": [], "action": []}
    if not cfg.get("profile_plan"):
        return reports
    threshold = cfg["plan_checkpoint_threshold"]
    for heading, label, key, sink in (
        (
            "BUILDER-LEVEL PLAN PROFILE (where the plan grows)",
            "BUILDER",
            "builder",
            "_plan_profile",
        ),
        (
            "CHECKPOINT-LEVEL PLAN PROFILE (plan truncated at each checkpoint)",
            "CHECKPOINT",
            "checkpoint",
            "_checkpoint_plan_profile",
        ),
        (
            "ACTION-LEVEL PLAN PROFILE (materialization sites)",
            "ACTION",
            "action",
            "_action_plan_profile",
        ),
    ):
        print(f"\n===== {heading} =====")
        reports[key] = plan_profile_report(
            cfg.get(sink, []), threshold, label=label
        )
    return reports


def _apply_profile(spark, cfg, profile_name, profile, workers, profile_enabled, threshold, checkpoint_mode, CheckpointMode, SqlShufflePartitions):
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
        CheckpointMode if CheckpointMode is not None else checkpoint_mode
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
            "profile_plan": profile_enabled,
            "plan_checkpoint_threshold": threshold,
            "checkpoint_mode": mode,
            "max_threads": workers,
            "execution_profile": profile_name,
        }
    )
    initialize_checkpoint_V2(cfg, mode)
    print(
        f"[outputV2] ExecutionProfile={profile_name} "
        f"CheckpointMode={mode} shuffle="
        f"{shuffle_override or profile['shuffle_partitions']} "
        f"MaxThreads={workers} "
        f"ProfilePlan={'on' if profile_enabled else 'off'}"
    )
    return mode

_prod = output_module("add_lookthrough_allocation_detail_step01")
logger = _prod.logger

_load_config = track_plan(_prod._load_config)
_write_m1_sidepocket = track_plan(_prod._write_m1_sidepocket)
_write_book = track_plan(_prod._write_book)
_write_book_k1_adjustment = track_plan(_prod._write_book_k1_adjustment)
_write_offset = track_plan(_prod._write_offset)
_write_dated_transfer = track_plan(_prod._write_dated_transfer)
_write_dated_transfer_without_adj = track_plan(
    _prod._write_dated_transfer_without_adj
)
_write_special_allocation = track_plan(_prod._write_special_allocation)
_write_m1_residual = track_plan(_prod._write_m1_residual)
_write_box_jkl = track_plan(_prod._write_box_jkl)
_write_k1_complete = track_plan(_prod._write_k1_complete)
_write_cy_adjustment = track_plan(_prod._write_cy_adjustment)
_write_k1_text_allocation_detail = track_plan(
    _prod._write_k1_text_allocation_detail
)

_LAST_RUN_PROFILE = {}

def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def _isolated_cfg(cfg):
    local = {**cfg}
    local["_parquet_results"] = {}
    return local


def _merge_parquet_results(target, branch):
    for key, df in (branch or {}).items():
        if key in target:
            target[key] = target[key].unionByName(df)
        else:
            target[key] = df


def run_add_lookthrough_allocation_detail_step01(
    spark,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = "dev7",
    SchemaName: str = "iPC_2025_dev7_15349",
    CallFrom: str = None,
    cfg: dict = None,
    verbose: bool = False,
    ResultType: str = "Parquet",
    VolumePath: str = None,
    ExecutionID: str = None,
    max_threads: int = None,
    MaxThreads: int = None,
    parallel_groups: str = "all",
    ParallelGroups: str = None,
    execution_profile: str = "low",
    ExecutionProfile: str = None,
    profile_plan: bool = False,
    ProfilePlan=None,
    plan_checkpoint_threshold: int = 30,
    PlanCheckpointThreshold: int = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    SqlShufflePartitions=None,
    **kwargs,
):
    del kwargs
    global _LAST_RUN_PROFILE
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName
    call_from = CallFrom
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
    profile_enabled = _as_bool(
        ProfilePlan if ProfilePlan is not None else profile_plan
    )
    threshold = int(
        PlanCheckpointThreshold
        if PlanCheckpointThreshold is not None
        else plan_checkpoint_threshold
    )
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    status = {
        "sp_name": "uspAddLookThroughAllocationDetail_Step_01",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
        "skip_reason": None,
    }
    save_return_value = ""

    try:
        with _timed(timings, "S1 config and profile"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    run_id=run_id,
                    entity_id=entity_id,
                    client_id=client_id,
                    tax_period_id=tax_period_id,
                    catalog=catalog,
                    schema=schema,
                    call_from=call_from,
                )
            elif call_from is not None:
                cfg["call_from"] = call_from
            cfg = {**cfg, "_checkpoint_tables": []}
            if ResultType is not None:
                cfg.setdefault("result_type", ResultType)
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID
            cfg.setdefault("_parquet_results", {})
            cfg.setdefault("_checkpoint_paths", [])
            cfg.setdefault("_checkpoint_v2_activity", [])
            cfg.setdefault("_plan_profile", [])
            cfg.setdefault("_checkpoint_plan_profile", [])
            cfg.setdefault("_action_plan_profile", [])
            mode = _apply_profile(
                spark,
                cfg,
                profile_name,
                profile,
                workers,
                profile_enabled,
                threshold,
                checkpoint_mode,
                CheckpointMode,
                SqlShufflePartitions,
            )
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            _load_config(spark, cfg)
            if cfg.get("run_status") == "FAIL":
                logger.error(
                    "RunStatus=FAIL — aborting. RunID=%s EntityID=%s",
                    cfg["run_id"],
                    cfg["entity_id"],
                )
                status["status"] = "FAIL"
                status["error"] = "RunStatus=FAIL at entry"
                status["skip_reason"] = "run_status_fail"
                return status
            status["sections_completed"] = 1

        with _timed(timings, "S2 checkpoint base_lt_out"):
            base_lt_out = (
                _prod._tbl(spark, "LookThroughAllocationOutput", cfg)
                .filter(
                    (F.col("RunID") == cfg["run_id"])
                    & (F.col("ClientID") == cfg["client_id"])
                )
            )
            cfg["_base_lt_out"] = _checkpoint(
                spark, base_lt_out, "base_lt_out", cfg
            )

        with _timed(timings, "S3 output_writes"):
            def _wrap(name, fn):
                def task():
                    local = _isolated_cfg(cfg)
                    fn(spark, local)
                    return dict(local.get("_parquet_results") or {})
                return name, task

            def _dated_pair(spark_session, local):
                _write_dated_transfer(spark_session, local)
                _write_dated_transfer_without_adj(spark_session, local)

            write_tasks = [
                _wrap("m1_sidepocket", _write_m1_sidepocket),
                _wrap("book", _write_book),
                _wrap("book_k1_adjustment", _write_book_k1_adjustment),
                _wrap("offset", _write_offset),
                _wrap("dated_transfer_pair", _dated_pair),
                _wrap("special_allocation", _write_special_allocation),
                _wrap("m1_residual", _write_m1_residual),
                _wrap("box_jkl", _write_box_jkl),
                _wrap("k1_complete", _write_k1_complete),
                _wrap("cy_adjustment", _write_cy_adjustment),
                _wrap("k1_text", _write_k1_text_allocation_detail),
            ]
            branches = profile_action(
                "output_writes",
                cfg.get("_base_lt_out"),
                lambda: run_parallel(
                    write_tasks,
                    workers,
                    parallel_activity,
                    "output_writes",
                    enabled_groups,
                ),
                cfg,
            )
            merged = {}
            for branch in branches:
                _merge_parquet_results(merged, branch)
            cfg["_parquet_results"] = merged
            status["sections_completed"] = 13

        with _timed(timings, "S4 GenericResultStorer"):
            parquet_results = {
                key: value
                for key, value in cfg.get("_parquet_results", {}).items()
                if value.limit(1).first() is not None
            }
            if parquet_results:
                result_storer = GenericResultStorer(spark, None)
                save_return_value = result_storer.save_results(
                    result=parquet_results,
                    result_type=cfg.get("result_type", "deltalake"),
                    catalog_name=cfg["catalog"],
                    database_name=cfg["schema"],
                    run_id=cfg["run_id"],
                    client_id=cfg["client_id"],
                    entity_id=cfg["entity_id"],
                    execution_id=cfg.get("execution_id", "1"),
                    volume_path=cfg.get("volume_path", ""),
                    sql_url_path=None,
                    sql_username=None,
                    sql_password=None,
                )
            status["elapsed_seconds"] = round(time.time() - t0, 1)
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error("[FAIL] %s", exc, exc_info=True)
        raise
    finally:
        reports = _emit_reports(cfg) if isinstance(cfg, dict) else {}
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        _LAST_RUN_PROFILE = {
            "timings": list(timings),
            "parallel_activity": list(parallel_activity),
            "checkpoint_activity": (
                list(cfg.get("_checkpoint_v2_activity", []))
                if isinstance(cfg, dict)
                else []
            ),
            "plan_profile": reports.get("builder", []),
            "checkpoint_plan_profile": reports.get("checkpoint", []),
            "action_profile": reports.get("action", []),
            "elapsed_seconds": round(time.time() - t0, 1),
            "checkpoint_mode": mode,
            "max_threads": workers,
            "execution_profile": profile_name,
        }
        if isinstance(cfg, dict):
            drop_checkpoints_V2(spark, cfg)
            _prod._drop_checkpoints(spark, cfg)
        _v2(
            f"DONE status={status.get('status')} "
            f"skip_reason={status.get('skip_reason')} "
            f"elapsed={status.get('elapsed_seconds')}s "
            f"profile={profile_name} checkpoint_mode={mode}"
        )
    logger.info(
        "[DONE] add_lookthrough_allocation_detail_step01 | %ss | RunID=%s EntityID=%s",
        status["elapsed_seconds"],
        cfg["run_id"],
        cfg["entity_id"],
    )
    return save_return_value if save_return_value else status


__all__ = [
    "get_last_run_profile",
    "run_add_lookthrough_allocation_detail_step01",
]
