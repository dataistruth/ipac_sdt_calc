"""Parity-first outputV2 orchestrator for uspLoadK3AllocationSummary."""

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

_prod = output_module("usp_load_k3_allocation_summary")
_prep = output_module("_prep")
_summary = output_module("_summary")
_country = output_module("_country_rounding")
_rounding = output_module("_rounding")
_finalize = output_module("_finalize")
logger = _prod.logger

load_sp_config = track_plan(_prod.load_sp_config)
build_country_sic_lines = track_plan(_prep.build_country_sic_lines)
build_income_attr_rounding_import = track_plan(
    _prep.build_income_attr_rounding_import
)
build_k3_detail = track_plan(_prep.build_k3_detail)
build_rounding_flags = track_plan(_prep.build_rounding_flags)
build_mapped_lines = track_plan(_prep.build_mapped_lines)
has_mapped_lines = _prep.has_mapped_lines
build_k1_summary_amounts = track_plan(_prep.build_k1_summary_amounts)
build_k3_summary_rounded = track_plan(_summary.build_k3_summary_rounded)
build_rounding_difference = track_plan(_summary.build_rounding_difference)
apply_country_level_rounding = track_plan(_country.apply_country_level_rounding)
apply_standard_rounding = track_plan(_rounding.apply_standard_rounding)
finalize_summary = track_plan(_finalize.finalize_summary)
_build_output = _prod._build_output
_save_results = _prod._save_results
_OUTPUT_TABLE = _prod._OUTPUT_TABLE

_LAST_RUN_PROFILE = {}


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def run_usp_load_k3_allocation_summary(
    spark,
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
    catalog_name = CatalogName
    schema_name = SchemaName
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
        "sp_name": "uspLoadK3AllocationSummary",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "FilePathInfo": "",
        "skip_reason": None,
    }
    save_result = ""

    try:
        with _timed(timings, "S1 config and profile"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    entity_id=entity_id,
                    client_id=client_id,
                    tax_period_id=tax_period_id,
                    run_id=run_id,
                    catalog=catalog_name,
                    schema=schema_name,
                    call_from=call_from,
                )
            elif call_from is not None:
                cfg["call_from"] = call_from
            cfg = {**cfg, "_checkpoint_tables": list(cfg.get("_checkpoint_tables") or [])}
            cfg.setdefault("result_type", ResultType)
            cfg["verbose"] = verbose
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID
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
            if (cfg.get("run_status") or "").upper() == "FAIL":
                logger.warning("[EARLY_EXIT] run_status=FAIL — nothing to do.")
                status["skip_reason"] = "run_status_fail"
                return status
            cfg = load_sp_config(spark, cfg)

        holder = {}
        with _timed(timings, "S2 independent_early_loads"):
            run_parallel(
                [
                    (
                        "country_sic",
                        lambda: holder.__setitem__(
                            "country_sic",
                            build_country_sic_lines(spark, cfg),
                        ),
                    ),
                    (
                        "income_attr_import",
                        lambda: holder.__setitem__(
                            "income_attr_import",
                            build_income_attr_rounding_import(spark, cfg),
                        ),
                    ),
                ],
                workers,
                parallel_activity,
                "independent_early_loads",
                enabled_groups,
            )
        with _timed(timings, "S3 independent_inputs"):
            run_parallel(
                [
                    (
                        "k3_detail",
                        lambda: holder.__setitem__(
                            "k3_detail", build_k3_detail(spark, cfg)
                        ),
                    ),
                    (
                        "mapped_lines",
                        lambda: holder.__setitem__(
                            "mapped_lines", build_mapped_lines(spark, cfg)
                        ),
                    ),
                ],
                workers,
                parallel_activity,
                "independent_inputs",
                enabled_groups,
            )
        country_sic = holder["country_sic"]
        income_attr_import = holder["income_attr_import"]
        k3_detail = holder["k3_detail"]
        mapped_lines = holder["mapped_lines"]

        with _timed(timings, "S4 flags k1 and summary"):
            rounding_flags = build_rounding_flags(
                spark, cfg, k3_detail, income_attr_import
            )
            has_mapped = has_mapped_lines(cfg, mapped_lines)
            k1_amounts = build_k1_summary_amounts(
                spark, cfg, country_sic, mapped_lines, has_mapped
            )
            s6 = build_k3_summary_rounded(
                spark,
                cfg,
                k3_detail,
                rounding_flags,
                mapped_lines,
                has_mapped,
                k1_amounts,
            )
            k3_summary = _checkpoint(spark, s6["summary"], "k3_summary", cfg)
            temp6a = s6["temp6a"]
            temp6b = s6["temp6b"]
            rounding_diff = build_rounding_difference(
                spark, cfg, k3_summary, k1_amounts
            )
            is_country_level = (
                cfg.get("flag_country_level_rounding_logic") or ""
            ).strip().upper() == "C"
            if is_country_level:
                k3_detail = _checkpoint(spark, k3_detail, "k3_detail", cfg)
                k1_amounts = _checkpoint(spark, k1_amounts, "k1_amounts", cfg)
                rounding_diff = _checkpoint(
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
                    temp6a,
                    temp6b,
                )
            k3_summary = finalize_summary(
                spark, cfg, k3_summary, rounding_flags, mapped_lines, has_mapped
            )

        with _timed(timings, "S5 save K3AllocationSummary"):
            output_tables = {_OUTPUT_TABLE: _build_output(cfg, k3_summary)}
            save_result = profile_action(
                "save_k3_allocation_summary",
                k3_summary,
                lambda: _save_results(spark, cfg, output_tables),
                cfg,
            )
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
        _v2(
            f"DONE status={status.get('status')} "
            f"skip_reason={status.get('skip_reason')} "
            f"elapsed={status.get('elapsed_seconds')}s "
            f"profile={profile_name} checkpoint_mode={mode}"
        )
    logger.info(
        "[DONE] uspLoadK3AllocationSummary | %ss | RunID=%s EntityID=%s",
        status["elapsed_seconds"],
        cfg.get("run_id"),
        cfg.get("entity_id"),
    )
    return save_result if save_result else status


__all__ = ["get_last_run_profile", "run_usp_load_k3_allocation_summary"]
