"""
orchestrator.py

Converted from: dbo.usp_SM_LoadLookThroughEffectiveAllocationPercentage.sql
Original procedure: dbo.usp_SM_LoadLookThroughEffectiveAllocationPercentage
Conversion date: 2026-05-04

Calculates and applies effective allocation percentages for state
look-through allocations. Handles flow-up partners, UBTI lines,
side-pocket logic, and exclude-from-residual recalculation.

Usage (standalone):
    from orchestrator import run_sm_load_lt_effective_alloc_pct
    run_sm_load_lt_effective_alloc_pct(
        spark, entity_id=123, client_id=456,
        tax_period_id=789, run_id=1001,
        catalog="dev7", schema="Qa7testschema",
    )

Usage (reuse shared config from a workflow):
    cfg = load_common_config(spark, run_id, entity_id, ...)
    run_sm_load_lt_effective_alloc_pct(spark, cfg=cfg)
"""

import json
import logging
import time
from contextlib import contextmanager

import pyspark.sql.functions as F

from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import read_table
from Common_V2.core.checkpoint import (
    checkpoint as common_checkpoint,
    drop_checkpoints,
)
from Common_V2.core.execution_profiles import resolve_execution_profile
from Common_V2.core.parallel_helpers import (
    normalize_workers,
    parse_enabled_groups,
    run_parallel,
)
from Common_V2.core.observability import get_logger

from AllocationV2.usp_sm_load_lookthrough_effective_allocation_pct.outputV2.services import (
    amount_service,
    effective_pct_service,
    flowup_service,
    mapping_service,
)
from AllocationV2.usp_sm_load_lookthrough_effective_allocation_pct.outputV2.services.config_service import (
    load_sp_config,
    validate_allocation_type,
)
from AllocationV2.usp_sm_load_lookthrough_effective_allocation_pct.outputV2.services.mapping_service import (
    build_mapping_data,
)
from AllocationV2.usp_sm_load_lookthrough_effective_allocation_pct.outputV2.services.flowup_service import (
    build_flowup_k1_amounts,
    compute_flowup_mapped_amounts,
    compute_flowup_effective_amounts,
    write_flowup_allocation_output,
)
from AllocationV2.usp_sm_load_lookthrough_effective_allocation_pct.outputV2.services.amount_service import (
    _build_sm_lt_input_and_state_lines,
    build_k1_amounts,
    build_ubti_amounts,
    compute_state_mapped_amounts,
)
from AllocationV2.usp_sm_load_lookthrough_effective_allocation_pct.outputV2.services.effective_pct_service import (
    compute_effective_percentages,
    apply_exclude_from_residual,
    apply_pe_book_unmapped_lines,
)
from AllocationV2.usp_sm_load_lookthrough_effective_allocation_pct.outputV2.services.writer_service import (
    write_allocation_output,
    update_allocation_input,
)

KNOWN_GROUPS = frozenset({"amount_builders", "independent_loads"})

logger = get_logger(__name__)


def _start_dev_plan_profile(enabled):
    from .plan_profiler import disable_plan_profile

    on = bool(enabled) and str(enabled).strip().lower() not in {
        "0", "false", "off", "no", "",
    }
    print(f"[plan-profile] armed={on}")
    if not on:
        disable_plan_profile()
        return None
    from .plan_profiler import (
        start_action_profile,
        start_checkpoint_plan_profile,
        start_plan_profile,
    )
    return (
        start_plan_profile(),
        start_checkpoint_plan_profile(),
        start_action_profile(),
    )


def _finish_dev_plan_profile(tokens, threshold=30, cfg=None):
    from .plan_profiler import disable_plan_profile, store_last_plan_profile

    if not tokens:
        print("[plan-profile] skipped (not armed)")
        disable_plan_profile()
        return
    from .plan_profiler import (
        finish_action_profile,
        finish_checkpoint_plan_profile,
        finish_plan_profile,
        plan_profile_report,
    )

    def _pair(item):
        if isinstance(item, tuple) and len(item) >= 2:
            return item[0], list(item[1] or [])
        return item, []

    plan_token, cp_token, action_token = tokens
    plan_tok, builder = _pair(plan_token)
    cp_tok, checkpoints = _pair(cp_token)
    action_tok, actions = _pair(action_token)
    finish_action_profile(action_tok)
    finish_checkpoint_plan_profile(cp_tok)
    finish_plan_profile(plan_tok)
    if isinstance(cfg, dict):
        builder = builder + list(cfg.get("_plan_profile") or [])
        checkpoints = checkpoints + list(cfg.get("_checkpoint_plan_profile") or [])
        actions = actions + list(cfg.get("_action_plan_profile") or [])
    try:
        from AllocationV2.plan_profiler import LAST_PLAN_PROFILE as _shared_last

        if not builder:
            builder = list(_shared_last.get("builder") or [])
        if not checkpoints:
            checkpoints = list(_shared_last.get("checkpoint") or [])
        if not actions:
            actions = list(_shared_last.get("action") or [])
    except Exception:
        pass
    builder_rep = plan_profile_report(builder, threshold, label="BUILDER") or []
    ckpt_rep = plan_profile_report(checkpoints, threshold, label="CHECKPOINT") or []
    action_rep = plan_profile_report(actions, threshold, label="ACTION") or []
    store_last_plan_profile(builder_rep, ckpt_rep, action_rep)
    print(
        f"[plan-profile] stored builder={len(builder_rep)} "
        f"checkpoint={len(ckpt_rep)} action={len(action_rep)}"
    )
    disable_plan_profile()


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
    if df is None:
        return None
    from .plan_profiler import track_checkpoint_plan

    track_checkpoint_plan(name, df, cfg)
    return common_checkpoint(spark, df, name, cfg)


# Builders call module-level checkpoint(). Point it at the wrapper after
# _checkpoint is defined. Never have _checkpoint call this name.
checkpoint = _checkpoint
amount_service.checkpoint = _checkpoint
mapping_service.checkpoint = _checkpoint
effective_pct_service.checkpoint = _checkpoint
flowup_service.checkpoint = _checkpoint


@contextmanager
def use_profile_checkpoint():
    """No-op: checkpoint already routes to _checkpoint → common_checkpoint."""
    yield


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
    ResultType: str = "deltalake",
    VolumePath: str = None,
    ExecutionID: str = None,
    execution_profile: str = None,
    ExecutionProfile: str = None,
    max_threads: int = None,
    MaxThreads: int = None,
    parallel_groups: str = "all",
    ParallelGroups: str = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    SqlShufflePartitions=None,
    profile_plan: bool = False,
    plan_checkpoint_threshold: int = 30,
    **kwargs,
):
    """Main entry point for SM Load LookThrough Effective Allocation Percentage.

    Converted from: dbo.usp_SM_LoadLookThroughEffectiveAllocationPercentage

    Sequential write order is required: flow-up Output, then main Output,
    then Input. Independent mapping/amount loads may run in parallel.
    """
    del kwargs
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName
    result_type = ResultType
    volume_path = VolumePath
    execution_id = ExecutionID

    t0 = time.time()
    orig_shuffle = None
    try:
        orig_shuffle = spark.conf.get("spark.sql.shuffle.partitions")
    except Exception:
        orig_shuffle = None
    parallel_activity = []
    enabled_groups = parse_enabled_groups(parallel_groups, ParallelGroups, known_groups=KNOWN_GROUPS)
    profile = resolve_execution_profile(
        _blank(ExecutionProfile) or _blank(execution_profile),
        checkpoint_mode=(
            CheckpointMode if CheckpointMode is not None else checkpoint_mode
        ),
    )
    profile_name = profile["name"]
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

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    status = {
        "sp_name": "usp_SM_LoadLookThroughEffectiveAllocationPercentage",
        "run_id": None,
        "entity_id": None,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }
    rows = None

    try:
        if cfg is None:
            cfg = load_common_config(
                spark,
                run_id=run_id,
                entity_id=entity_id,
                client_id=client_id,
                tax_period_id=tax_period_id,
                catalog=catalog,
                schema=schema,
            )

        cfg = {**cfg, "_checkpoint_tables": []}
        if result_type is not None:
            cfg.setdefault("result_type", result_type)
        if volume_path is not None:
            cfg["volume_path"] = volume_path
        if execution_id is not None:
            cfg["execution_id"] = execution_id
        cfg.setdefault("_parquet_results", {})

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
        mode = profile["checkpoint_mode"]
        cfg.update(
            {
                "checkpoint_mode": mode,
                "max_threads": workers,
                "execution_profile": profile_name,
            }
        )
        cfg.setdefault("_checkpoint_tables", [])
        cfg["profile_plan"] = bool(profile_plan)
        _plan_tokens = _start_dev_plan_profile(profile_plan)
        logger.info(
            f"[profile] ExecutionProfile={profile_name} CheckpointMode={mode} "
            f"MaxThreads={workers}"
        )

        cfg = load_sp_config(spark, cfg)
        status["run_id"] = cfg.get("run_id")
        status["entity_id"] = cfg.get("entity_id")

        if not validate_allocation_type(spark, cfg):
            status["status"] = "FAIL"
            status["error"] = "Allocation logic not selected for the entity."
            return status

        with use_profile_checkpoint():
            mappings, fp_data = run_parallel(
                [
                    ("mappings", lambda: build_mapping_data(spark, cfg)),
                    ("flowup_k1", lambda: build_flowup_k1_amounts(spark, cfg)),
                ],
                workers,
                parallel_activity,
                "independent_loads",
                enabled_groups,
            )

            is_flowup = cfg.get("is_sidepocket_flowup_partner", False)
            sm_fp_lt = read_table(
                spark, "SM_FlowUpPartnerLookThroughAllocationInput", cfg
            )
            has_flowup_input = len(
                sm_fp_lt.filter(F.col("RunID") == cfg["run_id"]).head(1)
            ) > 0

            if is_flowup or has_flowup_input:
                fp_total_amounts = compute_flowup_mapped_amounts(
                    spark, cfg, fp_data, mappings
                )
                fp_effective = compute_flowup_effective_amounts(
                    spark, cfg, fp_total_amounts, mappings["state_mapped_lines"],
                )
                write_flowup_allocation_output(spark, cfg, fp_effective)
            else:
                logger.info("[SKIP] Flow-up partner pipeline: condition not met")

            (
                sm_lt_input, state_lines, fed_lines,
                non_sp_fp, pruned_dm, pruned_ubti_dm,
            ) = _build_sm_lt_input_and_state_lines(spark, cfg, mappings)

            (partner_alloc, total_input), (partner_alloc_ubti, total_ubti_input) = (
                run_parallel(
                    [
                        (
                            "k1_amounts",
                            lambda: build_k1_amounts(
                                spark, cfg, mappings,
                                fp_data["k1_sidepocket"],
                                fp_data["k1_sidepocket_res"],
                                fed_lines, non_sp_fp,
                            ),
                        ),
                        (
                            "ubti_amounts",
                            lambda: build_ubti_amounts(
                                spark, cfg, fed_lines, non_sp_fp,
                            ),
                        ),
                    ],
                    workers,
                    parallel_activity,
                    "amount_builders",
                    enabled_groups,
                )
            )

            total_amounts = compute_state_mapped_amounts(
                spark, cfg, pruned_dm, pruned_ubti_dm,
                partner_alloc, total_input,
                partner_alloc_ubti, total_ubti_input,
            )
            total_amounts = _checkpoint(
                spark, total_amounts, "total_amounts", cfg
            )

            effective_amounts, temp_effective = compute_effective_percentages(
                spark, cfg, total_amounts, sm_lt_input,
            )
            effective_amounts = apply_exclude_from_residual(
                spark, cfg, effective_amounts, total_amounts,
            )
            effective_amounts = apply_pe_book_unmapped_lines(
                spark, cfg, effective_amounts, temp_effective,
                sm_lt_input, mappings,
            )
            effective_amounts = _checkpoint(
                spark, effective_amounts, "alloc_output", cfg
            )

            partner_snap = read_table(spark, "Partner_Snapshot", cfg)
            partner_snapshot = partner_snap.filter(
                F.coalesce(F.col("WorkFlowID"), F.col("Transactionid"))
                == cfg["partner_txn_or_wf_id"]
            )

            rows = write_allocation_output(
                spark, cfg, effective_amounts, partner_snapshot
            )
            update_allocation_input(spark, cfg, effective_amounts)
            status["sections_completed"] = 13

    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error(f"[FAIL] {e}", exc_info=True)
        raise
    finally:

        try:
            _finish_dev_plan_profile(locals().get("_plan_tokens"), cfg=locals().get("cfg"))
        except Exception:
            pass
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        if orig_shuffle is not None:
            spark.conf.set("spark.sql.shuffle.partitions", orig_shuffle)
        if isinstance(cfg, dict):
            drop_checkpoints(spark, cfg)

    logger.info(
        f"[DONE] run_sm_load_lt_effective_alloc_pct | "
        f"{status['elapsed_seconds']}s | "
        f"RunID={cfg['run_id']} EntityID={cfg['entity_id']}"
    )

    if rows and isinstance(rows, str):
        print(f"[PARQUET] Return JSON: {rows}")
        return rows

    return status


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    try:
        result = run_sm_load_lt_effective_alloc_pct(
            spark,
            RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
            EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
            ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
            TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
            CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
            SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
        )
    except Exception as exc:
        print(f"Usage: provide run_id, entity_id, etc. as widget parameters ({exc})")
        import sys
        sys.exit(1)

    try:
        dbutils.notebook.exit(json.dumps(result))  # noqa: F821
    except Exception:
        print(json.dumps(result, indent=2))
