"""Parity-first outputV2 orchestrator for uspAddAllocationSummary."""

from __future__ import annotations

import json
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

_prod = output_module("add_allocation_summary")
logger = _prod.logger
read_table = _prod.read_table

load_sp_config = track_plan(_prod.load_sp_config)
build_working_tables = track_plan(_prod.build_working_tables)
build_pfic_reclass_data = track_plan(_prod.build_pfic_reclass_data)
build_custom_footnote_transactions = track_plan(
    _prod.build_custom_footnote_transactions
)
write_k1_allocation_summary = track_plan(_prod.write_k1_allocation_summary)
write_m1_adj_allocation_summary = track_plan(
    _prod.write_m1_adj_allocation_summary
)
write_box_jkl_allocation_summary = track_plan(
    _prod.write_box_jkl_allocation_summary
)
write_form926_allocation_summary = track_plan(
    _prod.write_form926_allocation_summary
)
write_form8865_allocation_summary = track_plan(
    _prod.write_form8865_allocation_summary
)
write_form199a_allocation_summary = track_plan(
    _prod.write_form199a_allocation_summary
)
write_pfic_footnote_allocation_summary = track_plan(
    _prod.write_pfic_footnote_allocation_summary
)
write_pfic_footnote_allocation_text = track_plan(
    _prod.write_pfic_footnote_allocation_text
)
write_custom_footnote_allocation_summary = track_plan(
    _prod.write_custom_footnote_allocation_summary
)
write_line18a_allocation_summary = track_plan(
    _prod.write_line18a_allocation_summary
)
write_ubti_allocation_summary = track_plan(_prod.write_ubti_allocation_summary)
write_passive_income_allocation_summary = track_plan(
    _prod.write_passive_income_allocation_summary
)
write_form200616_allocation_summary = track_plan(
    _prod.write_form200616_allocation_summary
)
write_form8886_allocation_summary = track_plan(
    _prod.write_form8886_allocation_summary
)
write_at_risk_allocation_summary = track_plan(
    _prod.write_at_risk_allocation_summary
)
write_gaap_to_tax_allocation = track_plan(_prod.write_gaap_to_tax_allocation)
write_adjustment_allocation_summary = track_plan(
    _prod.write_adjustment_allocation_summary
)

_LAST_RUN_PROFILE = {}
_MULTI_CONSUMER_FRAMES = (
    "allocation_output_summary",
    "allocation_output",
    "k1_workflow",
    "lower_tier_funds",
    "at_risk_workflow",
    "partner_pfic",
)


def get_last_run_profile():
    return dict(_LAST_RUN_PROFILE)


def _isolated_cfg(cfg):
    local = {**cfg}
    local["_result_file_infos"] = []
    return local


def run_add_allocation_summary(
    spark,
    cfg: dict = None,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "None",
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
    status = {
        "sp_name": "uspAddAllocationSummary",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "skip_reason": None,
    }

    try:
        with _timed(timings, "S1 config and profile"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    entity_id=entity_id,
                    client_id=client_id,
                    tax_period_id=tax_period_id,
                    run_id=run_id,
                    catalog=catalog,
                    schema=schema,
                )
            cfg = {**cfg, "_checkpoint_tables": []}
            if ResultType is not None:
                cfg.setdefault("result_type", ResultType)
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID
            cfg["_result_file_infos"] = []
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
            if cfg.get("run_status") == "FAIL":
                logger.warning("Run status is FAIL — exiting early")
                status["status"] = "SKIPPED"
                status["skip_reason"] = "run_status_fail"
                status["reason"] = "RunStatus=FAIL"
                status["elapsed_seconds"] = round(time.time() - t0, 1)
                return status
            load_sp_config(spark, cfg)
            cfg["allow_pe_book_inserts"] = _prod._should_insert_pe_book(cfg)

        with _timed(timings, "S2 working tables"):
            tables = build_working_tables(spark, cfg)
            partner_wf_id = cfg.get("partner_workflow_id")
            partner_txn_id = cfg.get("partner_transaction_id")
            partner_pfic = read_table(spark, "Partner_Snapshot", cfg).filter(
                (
                    F.coalesce(F.col("WorkFlowID"), F.col("TransactionID"))
                    == F.lit(
                        partner_wf_id
                        if partner_wf_id is not None
                        else partner_txn_id
                    ).cast("int")
                )
                & (F.col("EntityID") == cfg["entity_id"])
                & (F.col("ClientID") == cfg["client_id"])
            ).select(
                _prod.ns(F.col("ShareClass")).alias("ShareClass"),
                F.col("PartnerNumber"),
            ).distinct()
            tables["partner_pfic"] = partner_pfic
            for frame_name in _MULTI_CONSUMER_FRAMES:
                frame = tables.get(frame_name)
                if frame is not None and hasattr(frame, "columns"):
                    tables[frame_name] = _checkpoint(
                        spark, frame, frame_name, cfg
                    )
            partner_pfic = tables["partner_pfic"]

        with _timed(timings, "S3 independent builders"):
            builder_holder = {}

            def _pfic_task():
                builder_holder["pfic_reclass"] = build_pfic_reclass_data(
                    spark, cfg, tables
                )

            def _cf_task():
                builder_holder["cf_data"] = build_custom_footnote_transactions(
                    spark, cfg, tables
                )

            run_parallel(
                [
                    ("pfic_reclass", _pfic_task),
                    ("custom_footnote_txns", _cf_task),
                ],
                workers,
                parallel_activity,
                "independent_builders",
                enabled_groups,
            )
            pfic_reclass_df = builder_holder["pfic_reclass"]
            cf_data = builder_holder["cf_data"]

        with _timed(timings, "S4 output_writes"):
            infos = []

            def _wrap(name, fn):
                def task():
                    local = _isolated_cfg(cfg)
                    fn(local)
                    return list(local.get("_result_file_infos") or [])
                return name, task

            write_tasks = [
                _wrap(
                    "k1",
                    lambda local: write_k1_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "m1",
                    lambda local: write_m1_adj_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "box_jkl",
                    lambda local: write_box_jkl_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "form926",
                    lambda local: write_form926_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "form8865",
                    lambda local: write_form8865_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "form199a",
                    lambda local: write_form199a_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "pfic_summary",
                    lambda local: write_pfic_footnote_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "pfic_text",
                    lambda local: write_pfic_footnote_allocation_text(
                        spark, local, pfic_reclass_df
                    ),
                ),
                _wrap(
                    "custom_footnote",
                    lambda local: write_custom_footnote_allocation_summary(
                        spark, local, tables, cf_data, partner_pfic
                    ),
                ),
                _wrap(
                    "line18a",
                    lambda local: write_line18a_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "ubti",
                    lambda local: write_ubti_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "passive_income",
                    lambda local: write_passive_income_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "form200616",
                    lambda local: write_form200616_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "form8886",
                    lambda local: write_form8886_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "at_risk",
                    lambda local: write_at_risk_allocation_summary(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "gaap_to_tax",
                    lambda local: write_gaap_to_tax_allocation(
                        spark, local, tables
                    ),
                ),
                _wrap(
                    "adjustment",
                    lambda local: write_adjustment_allocation_summary(
                        spark, local, tables
                    ),
                ),
            ]
            branch_infos = profile_action(
                "output_writes",
                tables.get("allocation_output_summary"),
                lambda: run_parallel(
                    write_tasks,
                    workers,
                    parallel_activity,
                    "output_writes",
                    enabled_groups,
                ),
                cfg,
            )
            for blob_list in branch_infos:
                infos.extend(blob_list or [])
            cfg["_result_file_infos"] = infos

        merged = {}
        for blob in cfg.get("_result_file_infos", []):
            try:
                merged.update(json.loads(blob))
            except (json.JSONDecodeError, TypeError):
                pass
        result_json = json.dumps(merged) if merged else ""
        if result_json:
            print(f"[PARQUET] Return JSON: {result_json}")
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        payload = result_json if result_json else status
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = str(exc)
        logger.error("uspAddAllocationSummary FAILED: %s", exc)
        raise
    finally:
        reports = _emit_reports(cfg) if isinstance(cfg, dict) else {}
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
    return payload


__all__ = ["get_last_run_profile", "run_add_allocation_summary"]
