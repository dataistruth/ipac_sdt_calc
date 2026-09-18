"""Single-SP outputV2 orchestrator for look-through effective allocation."""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

import pyspark.sql.functions as F

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import read_table

from .amount_service import (
    _build_sm_lt_input_and_state_lines,
    build_k1_amounts,
    build_ubti_amounts,
    compute_state_mapped_amounts,
)
from .effective_pct_service import (
    apply_exclude_from_residual,
    apply_pe_book_unmapped_lines,
    compute_effective_percentages,
)
from .mapping_service import build_mapping_data
from .parent import service_module
from .plan_profiler import (
    plan_profile_report,
    profile_action,
    track_plan,
)

logger = logging.getLogger(__name__)

_config = service_module("config_service")
_flowup = service_module("flowup_service")
_writer = service_module("writer_service")
load_sp_config = _config.load_sp_config
validate_allocation_type = _config.validate_allocation_type
build_flowup_k1_amounts = _flowup.build_flowup_k1_amounts
compute_flowup_mapped_amounts = _flowup.compute_flowup_mapped_amounts
compute_flowup_effective_amounts = _flowup.compute_flowup_effective_amounts
write_flowup_allocation_output = _flowup.write_flowup_allocation_output
write_allocation_output = _writer.write_allocation_output
update_allocation_input = _writer.update_allocation_input

for _name in (
    "build_mapping_data",
    "build_flowup_k1_amounts",
    "compute_flowup_mapped_amounts",
    "compute_flowup_effective_amounts",
    "_build_sm_lt_input_and_state_lines",
    "build_k1_amounts",
    "build_ubti_amounts",
    "compute_state_mapped_amounts",
    "compute_effective_percentages",
    "apply_exclude_from_residual",
    "apply_pe_book_unmapped_lines",
):
    globals()[_name] = track_plan(globals()[_name])

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


def _run_parallel(tasks, workers, activity, label):
    """Run independent builders, preserving deterministic result order."""
    count = len(tasks)
    pool_workers = max(1, min(workers, count, 4))
    pool_started = time.perf_counter()
    if pool_workers == 1:
        results = {}
        for name, fn in tasks:
            started = time.perf_counter()
            results[name] = fn()
            activity.append(
                {
                    "group": label,
                    "task": name,
                    "status": "SUCCESS",
                    "elapsed_seconds": round(
                        time.perf_counter() - started, 3
                    ),
                    "thread": threading.current_thread().name,
                }
            )
    else:
        results = {}

        def invoke(name, fn):
            started = time.perf_counter()
            try:
                return fn()
            finally:
                activity.append(
                    {
                        "group": label,
                        "task": name,
                        "status": "SUCCESS",
                        "elapsed_seconds": round(
                            time.perf_counter() - started, 3
                        ),
                        "thread": threading.current_thread().name,
                    }
                )

        with ThreadPoolExecutor(
            max_workers=pool_workers, thread_name_prefix="sm-effective"
        ) as pool:
            futures = {
                pool.submit(invoke, name, fn): name for name, fn in tasks
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception:
                    for pending in futures:
                        pending.cancel()
                    for item in activity:
                        if item["group"] == label and item["task"] == name:
                            item["status"] = "FAIL"
                    raise
    wall = round(time.perf_counter() - pool_started, 3)
    print(
        f"[parallel] {label}: tasks={count} workers={pool_workers} "
        f"wall={wall:.3f}s"
    )
    activity.append(
        {
            "group": label,
            "task": "__pool__",
            "status": "SUCCESS",
            "elapsed_seconds": wall,
            "thread": threading.current_thread().name,
        }
    )
    return [results[name] for name, _ in tasks]


def _emit_plan_reports(cfg):
    reports = {"builder": [], "checkpoint": [], "action": []}
    if not cfg.get("profile_plan"):
        return reports
    threshold = cfg["plan_checkpoint_threshold"]
    for heading, label, key, source_key in (
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
            cfg.get(source_key, []), threshold, label=label
        )
    return reports


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def run_sm_load_lt_effective_alloc_pct(
    spark,
    cfg=None,
    verbose=False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
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
    """Run the production flow with V2 checkpoints and one safe builder pool."""
    del kwargs
    global _LAST_RUN_PROFILE
    run_started = time.perf_counter()
    timings = []
    parallel_activity = []
    rows = None
    workers = _normalize_workers(max_threads, MaxThreads)
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
        "sp_name": "usp_SM_LoadLookThroughEffectiveAllocationPercentage",
        "run_id": RunID,
        "entity_id": EntityID,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    try:
        with _timed(timings, "S1-S3 config and validation"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    run_id=RunID,
                    entity_id=EntityID,
                    client_id=ClientID,
                    tax_period_id=TaxPeriodID,
                    catalog=CatalogName,
                    schema=SchemaName,
                )
            mode = resolve_checkpoint_mode(
                cfg,
                checkpoint_mode=checkpoint_mode,
                CheckpointMode=CheckpointMode,
            )
            cfg = {
                **cfg,
                "_checkpoint_tables": [],
                "_checkpoint_paths": [],
                "_checkpoint_v2_activity": [],
                "_plan_profile": [],
                "_checkpoint_plan_profile": [],
                "_action_plan_profile": [],
                "profile_plan": profile_enabled,
                "plan_checkpoint_threshold": threshold,
                "checkpoint_mode": mode,
                "max_threads": workers,
            }
            if ResultType is not None:
                cfg.setdefault("result_type", ResultType)
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID
            initialize_checkpoint_V2(cfg, mode)
            print(
                f"[CHECKPOINT_V2] mode={mode}; "
                f"[outputV2] MaxThreads={workers} "
                f"ProfilePlan={'on' if profile_enabled else 'off'}"
            )
            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")
            cfg = load_sp_config(spark, cfg)
            if not validate_allocation_type(spark, cfg):
                status["status"] = "FAIL"
                status["error"] = (
                    "Allocation logic not selected for the entity."
                )
                return status

        with _timed(timings, "S4 mappings"):
            mappings = build_mapping_data(spark, cfg)

        with _timed(timings, "S5-S6 flow-up"):
            is_flowup = cfg.get("is_sidepocket_flowup_partner", False)
            flowup_source = read_table(
                spark, "SM_FlowUpPartnerLookThroughAllocationInput", cfg
            )
            current_flowup = flowup_source.filter(
                F.col("RunID") == cfg["run_id"]
            )
            has_flowup_input = bool(
                profile_action(
                    "flowup_input.head",
                    current_flowup,
                    lambda: current_flowup.head(1),
                    cfg,
                )
            )
            fp_data = build_flowup_k1_amounts(spark, cfg)
            if is_flowup or has_flowup_input:
                fp_total = compute_flowup_mapped_amounts(
                    spark, cfg, fp_data, mappings
                )
                fp_effective = compute_flowup_effective_amounts(
                    spark, cfg, fp_total, mappings["state_mapped_lines"]
                )
                profile_action(
                    "write_flowup_allocation_output",
                    fp_effective,
                    lambda: write_flowup_allocation_output(
                        spark, cfg, fp_effective
                    ),
                    cfg,
                )
            else:
                logger.info(
                    "[SKIP] Flow-up partner pipeline: condition not met"
                )

        with _timed(timings, "S7 shared inputs"):
            (
                sm_lt_input,
                state_lines,
                fed_lines,
                non_sp_fp,
                pruned_dm,
                pruned_ubti_dm,
            ) = _build_sm_lt_input_and_state_lines(spark, cfg, mappings)
            del state_lines

        with _timed(timings, "S7-S8 K1 and UBTI"):
            k1_result, ubti_result = _run_parallel(
                [
                    (
                        "build_k1_amounts",
                        lambda: build_k1_amounts(
                            spark,
                            cfg,
                            mappings,
                            fp_data["k1_sidepocket"],
                            fp_data["k1_sidepocket_res"],
                            fed_lines,
                            non_sp_fp,
                        ),
                    ),
                    (
                        "build_ubti_amounts",
                        lambda: build_ubti_amounts(
                            spark, cfg, fed_lines, non_sp_fp
                        ),
                    ),
                ],
                workers,
                parallel_activity,
                "k1_ubti_builders",
            )
            partner_alloc, total_input = k1_result
            partner_alloc_ubti, total_ubti_input = ubti_result

        with _timed(timings, "S9 state mapping"):
            total_amounts = compute_state_mapped_amounts(
                spark,
                cfg,
                pruned_dm,
                pruned_ubti_dm,
                partner_alloc,
                total_input,
                partner_alloc_ubti,
                total_ubti_input,
            )

        with _timed(timings, "S10-S12 effective amounts"):
            effective_amounts, temp_effective = (
                compute_effective_percentages(
                    spark, cfg, total_amounts, sm_lt_input
                )
            )
            effective_amounts = apply_exclude_from_residual(
                spark, cfg, effective_amounts, total_amounts
            )
            effective_amounts = apply_pe_book_unmapped_lines(
                spark,
                cfg,
                effective_amounts,
                temp_effective,
                sm_lt_input,
                mappings,
            )

        # These mutate output and input state and intentionally remain ordered.
        with _timed(timings, "S13 final output write"):
            partner_snapshot = read_table(
                spark, "Partner_Snapshot", cfg
            ).filter(
                F.coalesce(F.col("WorkFlowID"), F.col("Transactionid"))
                == cfg["partner_txn_or_wf_id"]
            )
            rows = profile_action(
                "write_allocation_output",
                effective_amounts,
                lambda: write_allocation_output(
                    spark, cfg, effective_amounts, partner_snapshot
                ),
                cfg,
            )
        with _timed(timings, "S13 allocation input update"):
            profile_action(
                "update_allocation_input",
                effective_amounts,
                lambda: update_allocation_input(
                    spark, cfg, effective_amounts
                ),
                cfg,
            )
            status["sections_completed"] = 13
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error("[FAIL] %s", exc, exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(
            time.perf_counter() - run_started, 1
        )
        reports = _emit_plan_reports(cfg) if isinstance(cfg, dict) else {}
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
            "elapsed_seconds": status["elapsed_seconds"],
            "checkpoint_mode": mode,
            "max_threads": workers,
        }

    logger.info(
        "[DONE] run_sm_load_lt_effective_alloc_pct | %.1fs | "
        "RunID=%s EntityID=%s",
        status["elapsed_seconds"],
        cfg["run_id"],
        cfg["entity_id"],
    )
    if rows and isinstance(rows, str):
        print(f"[PARQUET] Return JSON: {rows}")
        return rows
    return status


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    result = run_sm_load_lt_effective_alloc_pct(
        spark,
        RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
        EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
        ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
        TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
        CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
        SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
    )
    try:
        dbutils.notebook.exit(json.dumps(result))  # noqa: F821
    except Exception:
        print(json.dumps(result, indent=2))


__all__ = [
    "checkpoint",
    "drop_checkpoints_V2",
    "get_last_run_profile",
    "run_sm_load_lt_effective_alloc_pct",
]
