"""Isolated outputV2 orchestration for look-through allocation detail step 01."""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyspark.sql.functions as F
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config
from Common_V2.core.generic_result_storer import GenericResultStorer
from Common_V2.core.helpers import tbl as _tbl

from .parent import output_module
from .plan_profiler import (
    finish_action_profile,
    finish_checkpoint_plan_profile,
    finish_plan_profile,
    measure_plan,
    plan_profile_report,
    profile_action,
    start_action_profile,
    start_checkpoint_plan_profile,
    start_plan_profile,
)

logger = logging.getLogger(__name__)
_production = output_module("add_lookthrough_allocation_detail_step01")

_load_config = _production._load_config

_WRITER_TASKS = (
    (("m1_sidepocket", _production._write_m1_sidepocket),),
    (("book", _production._write_book),),
    (("book_k1_adjustment", _production._write_book_k1_adjustment),),
    (("offset", _production._write_offset),),
    (
        ("dated_transfer", _production._write_dated_transfer),
        (
            "dated_transfer_without_adj",
            _production._write_dated_transfer_without_adj,
        ),
    ),
    (("special_allocation", _production._write_special_allocation),),
    (("m1_residual", _production._write_m1_residual),),
    (("box_jkl", _production._write_box_jkl),),
    (("k1_complete", _production._write_k1_complete),),
    (("cy_adjustment", _production._write_cy_adjustment),),
    (
        (
            "k1_text_allocation_detail",
            _production._write_k1_text_allocation_detail,
        ),
    ),
)

_LAST_RUN_PROFILE = {}


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "on", "true", "yes"}
    return bool(value)


def _normalize_workers(max_threads=4, MaxThreads=None):
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 4))


def _run_writer_task(spark, cfg, writers):
    """Run one writer task; multi-writer tasks remain ordered."""
    results = []
    for name, writer in writers:
        started = time.perf_counter()
        local_cfg = {**cfg, "_parquet_results": {}}
        writer(spark, local_cfg)
        results.append(
            {
                "name": name,
                "outputs": local_cfg.get("_parquet_results", {}),
                "elapsed_seconds": round(
                    time.perf_counter() - started, 3
                ),
                "thread": threading.current_thread().name,
            }
        )
    return results


def _run_writer_pool(spark, cfg, workers):
    pool_workers = max(1, min(workers, len(_WRITER_TASKS), 4))
    started = time.perf_counter()
    if pool_workers == 1:
        grouped = [
            _run_writer_task(spark, cfg, writers)
            for writers in _WRITER_TASKS
        ]
    else:
        grouped_by_index = {}
        with ThreadPoolExecutor(
            max_workers=pool_workers,
            thread_name_prefix="lt-detail-step01",
        ) as pool:
            futures = {
                pool.submit(_run_writer_task, spark, cfg, writers): index
                for index, writers in enumerate(_WRITER_TASKS)
            }
            try:
                for future in as_completed(futures):
                    grouped_by_index[futures[future]] = future.result()
            except Exception:
                for pending in futures:
                    pending.cancel()
                raise
        grouped = [
            grouped_by_index[index] for index in range(len(_WRITER_TASKS))
        ]
    wall = round(time.perf_counter() - started, 3)
    print(
        f"[parallel] section_writers: tasks={len(_WRITER_TASKS)} "
        f"workers={pool_workers} wall={wall:.3f}s"
    )
    return [item for group in grouped for item in group], wall, pool_workers


def _record_builder_plan(records, name, df, base_nodes):
    metrics = measure_plan(df)
    if not metrics:
        return
    records.append(
        {
            "func": name,
            "nodes": metrics["nodes"],
            "depth": metrics["depth"],
            "delta": metrics["nodes"] - base_nodes,
            "ops": metrics["ops"],
        }
    )


def _emit_reports(enabled, threshold, sinks):
    reports = {"builder": [], "checkpoint": [], "action": []}
    if not enabled:
        return reports
    for label, records, key in (
        ("BUILDER", sinks[0], "builder"),
        ("CHECKPOINT", sinks[1], "checkpoint"),
        ("ACTION", sinks[2], "action"),
    ):
        print(f"\n===== {label}-LEVEL PLAN PROFILE =====")
        reports[key] = plan_profile_report(
            records, threshold, label=label
        )
    return reports


def get_last_run_profile():
    return {
        **_LAST_RUN_PROFILE,
        "timings": list(_LAST_RUN_PROFILE.get("timings", ())),
        "parallel_activity": list(
            _LAST_RUN_PROFILE.get("parallel_activity", ())
        ),
        "checkpoint_activity": list(
            _LAST_RUN_PROFILE.get("checkpoint_activity", ())
        ),
        "plan_profile": list(
            _LAST_RUN_PROFILE.get("plan_profile", ())
        ),
        "checkpoint_plan_profile": list(
            _LAST_RUN_PROFILE.get("checkpoint_plan_profile", ())
        ),
        "action_profile": list(
            _LAST_RUN_PROFILE.get("action_profile", ())
        ),
    }


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
    max_threads: int = 4,
    MaxThreads: int = None,
    profile_plan: bool = False,
    ProfilePlan=None,
    plan_checkpoint_threshold: int = 30,
    PlanCheckpointThreshold: int = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    **kwargs,
):
    """Run production business logic with bounded independent planning."""
    del kwargs
    global _LAST_RUN_PROFILE
    started = time.perf_counter()
    timings = []
    parallel_activity = []
    workers = _normalize_workers(max_threads, MaxThreads)
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
        "run_id": RunID,
        "entity_id": EntityID,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }
    plan_token = checkpoint_token = action_token = None
    builder_records = []
    checkpoint_records = []
    action_records = []
    return_value = ""
    mode = None

    try:
        config_started = time.perf_counter()
        if cfg is None:
            cfg = load_common_config(
                spark,
                run_id=RunID,
                entity_id=EntityID,
                client_id=ClientID,
                tax_period_id=TaxPeriodID,
                catalog=CatalogName,
                schema=SchemaName,
                call_from=CallFrom,
            )
        elif CallFrom is not None:
            cfg["call_from"] = CallFrom
        cfg = {
            **cfg,
            "_checkpoint_tables": [],
            "_checkpoint_paths": [],
            "_checkpoint_v2_activity": [],
            "_parquet_results": {},
            "profile_plan": profile_enabled,
            "plan_checkpoint_threshold": threshold,
            "max_threads": workers,
        }
        if ResultType is not None:
            cfg.setdefault("result_type", ResultType)
        if VolumePath is not None:
            cfg["volume_path"] = VolumePath
        if ExecutionID is not None:
            cfg["execution_id"] = ExecutionID
        mode = resolve_checkpoint_mode(
            cfg,
            checkpoint_mode=checkpoint_mode,
            CheckpointMode=CheckpointMode,
        )
        initialize_checkpoint_V2(cfg, mode)
        print(
            f"[CHECKPOINT_V2] mode={mode}; [outputV2] "
            f"MaxThreads={workers} "
            f"ProfilePlan={'on' if profile_enabled else 'off'}"
        )
        if profile_enabled:
            plan_token, builder_records = start_plan_profile()
            checkpoint_token, checkpoint_records = (
                start_checkpoint_plan_profile()
            )
            action_token, action_records = start_action_profile()
        status["run_id"] = cfg.get("run_id")
        status["entity_id"] = cfg.get("entity_id")
        _load_config(spark, cfg)
        timings.append(
            {
                "step": "S1 config",
                "elapsed_seconds": round(
                    time.perf_counter() - config_started, 3
                ),
            }
        )
        if cfg.get("run_status") == "FAIL":
            status["status"] = "FAIL"
            status["error"] = "RunStatus=FAIL at entry"
            return status
        status["sections_completed"] = 1

        checkpoint_started = time.perf_counter()
        base_lt_out = _tbl(
            spark, "LookThroughAllocationOutput", cfg
        ).filter(
            (F.col("RunID") == cfg["run_id"])
            & (F.col("ClientID") == cfg["client_id"])
        )
        if "TaxPeriodID" in base_lt_out.columns:
            base_lt_out = base_lt_out.filter(
                F.col("TaxPeriodID") == cfg["tax_period_id"]
            )
        run_scope = spark.createDataFrame(
            [(int(cfg["run_id"]), int(cfg["client_id"]))],
            "RunID long, ClientID long",
        )
        base_lt_out = base_lt_out.join(
            F.broadcast(run_scope),
            ["RunID", "ClientID"],
            "left_semi",
        )
        activity_start = len(cfg["_checkpoint_v2_activity"])
        base_lt_out = checkpoint_V2(
            spark, base_lt_out, "base_lt_out", cfg
        )
        activity = cfg["_checkpoint_v2_activity"]
        if (
            len(activity) > activity_start
            and activity[-1].get("backend") == "local"
        ):
            base_lt_out = base_lt_out.toDF(*base_lt_out.columns)
        cfg["_base_lt_out"] = base_lt_out
        timings.append(
            {
                "step": "S2 base_lt_out checkpoint",
                "elapsed_seconds": round(
                    time.perf_counter() - checkpoint_started, 3
                ),
            }
        )
        status["sections_completed"] = 2
        base_metrics = measure_plan(base_lt_out) if profile_enabled else None
        base_nodes = base_metrics["nodes"] if base_metrics else 0

        writer_results, pool_wall, pool_workers = _run_writer_pool(
            spark, cfg, workers
        )
        master = cfg["_parquet_results"]
        for result in writer_results:
            parallel_activity.append(
                {
                    "group": "section_writers",
                    "task": result["name"],
                    "status": "SUCCESS",
                    "elapsed_seconds": result["elapsed_seconds"],
                    "thread": result["thread"],
                }
            )
            for table, df in result["outputs"].items():
                if profile_enabled:
                    _record_builder_plan(
                        builder_records, result["name"], df, base_nodes
                    )
                if table in master:
                    master[table] = master[table].unionByName(df)
                else:
                    master[table] = df
        parallel_activity.append(
            {
                "group": "section_writers",
                "task": "__pool__",
                "status": "SUCCESS",
                "elapsed_seconds": pool_wall,
                "thread": threading.current_thread().name,
                "workers": pool_workers,
            }
        )
        timings.append(
            {
                "step": "S3-S13 section writers",
                "elapsed_seconds": pool_wall,
            }
        )
        status["sections_completed"] = 13

        non_empty = {}
        for table, df in master.items():
            probe = df.limit(1)
            first = profile_action(
                f"{table}.first",
                probe,
                probe.first,
                cfg,
            )
            if first is not None:
                non_empty[table] = df
        if non_empty:
            result_storer = GenericResultStorer(spark, None)
            representative = next(iter(non_empty.values()))
            return_value = profile_action(
                "GenericResultStorer.save_results",
                representative,
                lambda: result_storer.save_results(
                    result=non_empty,
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
                ),
                cfg,
            )
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.exception("[FAIL] %s", exc)
        raise
    finally:
        if action_token is not None:
            finish_action_profile(action_token)
        if checkpoint_token is not None:
            finish_checkpoint_plan_profile(checkpoint_token)
        if plan_token is not None:
            finish_plan_profile(plan_token)
        status["elapsed_seconds"] = round(
            time.perf_counter() - started, 1
        )
        reports = _emit_reports(
            profile_enabled,
            threshold,
            (builder_records, checkpoint_records, action_records),
        )
        _LAST_RUN_PROFILE = {
            "timings": timings,
            "parallel_activity": parallel_activity,
            "checkpoint_activity": (
                list(cfg.get("_checkpoint_v2_activity", ()))
                if isinstance(cfg, dict)
                else []
            ),
            "plan_profile": reports["builder"],
            "checkpoint_plan_profile": reports["checkpoint"],
            "action_profile": reports["action"],
            "checkpoint_mode": mode,
            "max_threads": workers,
            "elapsed_seconds": status["elapsed_seconds"],
        }

    logger.info(
        "[DONE] run_add_lookthrough_allocation_detail_step01 | %.1fs | "
        "RunID=%s EntityID=%s",
        status["elapsed_seconds"],
        cfg["run_id"],
        cfg["entity_id"],
    )
    return return_value if return_value else status


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    result = run_add_lookthrough_allocation_detail_step01(
        spark,
        RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
        EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
        ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
        TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
        CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
        SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
    )
    try:
        dbutils.notebook.exit(  # noqa: F821
            json.dumps(result) if not isinstance(result, str) else result
        )
    except Exception:
        print(
            json.dumps(result, indent=2)
            if not isinstance(result, str)
            else result
        )


__all__ = [
    "drop_checkpoints_V2",
    "get_last_run_profile",
    "run_add_lookthrough_allocation_detail_step01",
]
