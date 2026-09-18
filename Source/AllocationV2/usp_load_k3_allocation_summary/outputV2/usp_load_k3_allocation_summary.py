"""Isolated outputV2 orchestration for uspLoadK3AllocationSummary."""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import get_logger

from ._country_rounding import apply_country_level_rounding
from .parent import output_module
from .plan_profiler import plan_profile_report, profile_action, track_plan
from .prep_optimized import (
    build_income_attr_rounding_import as _build_income_attr_rounding_import,
)

_production = output_module("usp_load_k3_allocation_summary")
_prep = output_module("_prep")
_summary = output_module("_summary")
_rounding = output_module("_rounding")
_finalize = output_module("_finalize")

load_sp_config = _production.load_sp_config
_build_output = _production._build_output
_save_results = _production._save_results

build_country_sic_lines = track_plan(_prep.build_country_sic_lines)
build_income_attr_rounding_import = track_plan(
    _build_income_attr_rounding_import
)
build_k3_detail = track_plan(_prep.build_k3_detail)
build_rounding_flags = track_plan(_prep.build_rounding_flags)
build_mapped_lines = track_plan(_prep.build_mapped_lines)
build_k1_summary_amounts = track_plan(_prep.build_k1_summary_amounts)
build_k3_summary_rounded = track_plan(_summary.build_k3_summary_rounded)
build_rounding_difference = track_plan(_summary.build_rounding_difference)
apply_country_level_rounding = track_plan(apply_country_level_rounding)
apply_standard_rounding = track_plan(_rounding.apply_standard_rounding)
finalize_summary = track_plan(_finalize.finalize_summary)

logger = get_logger(__name__)
_OUTPUT_TABLE = "K3AllocationSummary"
LAST_RUN_DIAGNOSTICS = {}


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


def _run_pool(tasks, workers, label):
    """Run independent read-only builders and preserve declaration order."""
    pool_size = max(1, min(workers, len(tasks), 4))
    started = time.perf_counter()
    task_activity = []

    def invoke(name, fn):
        task_started = time.perf_counter()
        try:
            result = fn()
        except Exception:
            task_activity.append(
                {
                    "pool": label,
                    "task": name,
                    "status": "FAIL",
                    "elapsed_seconds": round(
                        time.perf_counter() - task_started, 3
                    ),
                }
            )
            raise
        task_activity.append(
            {
                "pool": label,
                "task": name,
                "status": "SUCCESS",
                "elapsed_seconds": round(
                    time.perf_counter() - task_started, 3
                ),
            }
        )
        return result

    results = {}
    if pool_size == 1:
        for name, fn in tasks:
            results[name] = invoke(name, fn)
    else:
        with ThreadPoolExecutor(
            max_workers=pool_size, thread_name_prefix=f"k3-{label}"
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
                    raise

    elapsed = round(time.perf_counter() - started, 3)
    logger.info(
        "[POOL] %s tasks=%d workers=%d wall=%.3fs",
        label,
        len(tasks),
        pool_size,
        elapsed,
    )
    return {name: results[name] for name, _ in tasks}, {
        "pool": label,
        "tasks": [name for name, _ in tasks],
        "workers": pool_size,
        "elapsed_seconds": elapsed,
        "task_activity": task_activity,
    }


def _reports(cfg):
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
    return dict(LAST_RUN_DIAGNOSTICS)


def run_usp_load_k3_allocation_summary(
    spark: SparkSession,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    CallFrom: str = None,
    ResultType: str = "Parquet",
    VolumePath: str = None,
    ExecutionID: str = None,
    cfg: dict = None,
    verbose: bool = False,
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
    """Run production S1-S11 semantics with bounded prep concurrency."""
    del kwargs
    global LAST_RUN_DIAGNOSTICS
    started = time.perf_counter()
    workers = _normalize_workers(max_threads, MaxThreads)
    profiling = _as_bool(
        ProfilePlan if ProfilePlan is not None else profile_plan
    )
    threshold = int(
        PlanCheckpointThreshold
        if PlanCheckpointThreshold is not None
        else plan_checkpoint_threshold
    )
    mode = None
    pool_records = []
    return_value = ""
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    status = {
        "sp_name": "uspLoadK3AllocationSummary",
        "run_id": RunID,
        "entity_id": EntityID,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "FilePathInfo": "",
    }

    try:
        if cfg is None:
            cfg = load_common_config(
                spark,
                entity_id=EntityID,
                client_id=ClientID,
                tax_period_id=TaxPeriodID,
                run_id=RunID,
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
            "_plan_profile": [],
            "_checkpoint_plan_profile": [],
            "_action_plan_profile": [],
            "profile_plan": profiling,
            "plan_checkpoint_threshold": threshold,
            "max_threads": workers,
        }
        cfg.setdefault("result_type", ResultType)
        cfg["verbose"] = verbose
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
        logger.info(
            "[CHECKPOINT_V2] mode=%d MaxThreads=%d ProfilePlan=%s",
            mode,
            workers,
            "on" if profiling else "off",
        )

        status["run_id"] = cfg.get("run_id")
        status["entity_id"] = cfg.get("entity_id")
        if (cfg.get("run_status") or "").upper() == "FAIL":
            logger.warning("[EARLY_EXIT] run_status=FAIL")
            return status

        cfg = load_sp_config(spark, cfg)

        # S2, S3, S4a, and S5a have no cross-dependencies or writes.
        prepared, pool_info = _run_pool(
            [
                (
                    "country_sic",
                    lambda: build_country_sic_lines(spark, cfg),
                ),
                (
                    "income_attr_import",
                    lambda: build_income_attr_rounding_import(spark, cfg),
                ),
                ("k3_detail", lambda: build_k3_detail(spark, cfg)),
                (
                    "mapped_lines",
                    lambda: build_mapped_lines(spark, cfg),
                ),
            ],
            workers,
            "early-prep",
        )
        pool_records.append(pool_info)
        country_sic = prepared["country_sic"]
        income_attr_import = prepared["income_attr_import"]
        k3_detail = prepared["k3_detail"]
        mapped_lines = prepared["mapped_lines"]

        rounding_flags = build_rounding_flags(
            spark, cfg, k3_detail, income_attr_import
        )
        has_mapped = profile_action(
            "mapped_lines.exists",
            mapped_lines,
            lambda: _prep.has_mapped_lines(cfg, mapped_lines),
            cfg,
        )
        k1_amounts = build_k1_summary_amounts(
            spark, cfg, country_sic, mapped_lines, has_mapped
        )

        # S6-S10, including every rank iteration in S8, remain sequential.
        s6 = build_k3_summary_rounded(
            spark,
            cfg,
            k3_detail,
            rounding_flags,
            mapped_lines,
            has_mapped,
            k1_amounts,
        )
        k3_summary = checkpoint(
            spark, s6["summary"], "k3_summary", cfg
        )
        rounding_diff = build_rounding_difference(
            spark, cfg, k3_summary, k1_amounts
        )

        country_level = (
            (cfg.get("flag_country_level_rounding_logic") or "")
            .strip()
            .upper()
            == "C"
        )
        if country_level:
            k3_detail = checkpoint(
                spark, k3_detail, "k3_detail", cfg
            )
            k1_amounts = checkpoint(
                spark, k1_amounts, "k1_amounts", cfg
            )
            rounding_diff = checkpoint(
                spark, rounding_diff, "rounding_diff", cfg
            )
            k3_summary = apply_country_level_rounding(
                spark,
                cfg,
                k3_summary,
                k3_detail,
                rounding_diff,
                k1_amounts,
                mapped_lines,
                has_mapped,
            )
        else:
            k3_summary = apply_standard_rounding(
                spark,
                cfg,
                k3_summary,
                rounding_diff,
                rounding_flags,
                mapped_lines,
                has_mapped,
                s6["temp6a"],
                s6["temp6b"],
            )

        k3_summary = finalize_summary(
            spark,
            cfg,
            k3_summary,
            rounding_flags,
            mapped_lines,
            has_mapped,
        )
        output_df = _build_output(cfg, k3_summary)
        return_value = profile_action(
            "K3AllocationSummary.save",
            output_df,
            lambda: _save_results(
                spark, cfg, {_OUTPUT_TABLE: output_df}
            ),
            cfg,
        )
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error("[FAIL] %s", exc, exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(
            time.perf_counter() - started, 1
        )
        reports = _reports(cfg) if isinstance(cfg, dict) else {}
        LAST_RUN_DIAGNOSTICS = {
            "status": dict(status),
            "pools": pool_records,
            "checkpoint_activity": (
                list(cfg.get("_checkpoint_v2_activity", ()))
                if isinstance(cfg, dict)
                else []
            ),
            "plan_profiles": reports,
            "checkpoint_mode": mode,
            "max_threads": workers,
        }

    logger.info(
        "[DONE] uspLoadK3AllocationSummary | %.1fs | RunID=%s EntityID=%s",
        status["elapsed_seconds"],
        cfg.get("run_id"),
        cfg.get("entity_id"),
    )
    return return_value if return_value else status


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    result = run_usp_load_k3_allocation_summary(
        spark,
        RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
        EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
        ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
        TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
        CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
        SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
    )
    try:
        dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
    except Exception:
        print(f"Result: {result}")


__all__ = [
    "LAST_RUN_DIAGNOSTICS",
    "get_last_run_profile",
    "run_usp_load_k3_allocation_summary",
]
