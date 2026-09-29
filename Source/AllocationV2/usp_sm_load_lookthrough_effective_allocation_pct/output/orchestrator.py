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

import pyspark.sql.functions as F

from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import read_table
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2 as drop_checkpoints,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.execution_profiles import resolve_execution_profile
from contextlib import contextmanager
try:
    from parallel_helpers import normalize_workers, parse_enabled_groups, run_parallel
except ImportError:
    from .parallel_helpers import normalize_workers, parse_enabled_groups, run_parallel
from Common_V2.core.observability import log_section, log_timing


from services.config_service import load_sp_config, validate_allocation_type
from services.mapping_service import build_mapping_data, build_parent_k1_mappings
from services.flowup_service import (
    build_flowup_k1_amounts,
    compute_flowup_mapped_amounts,
    compute_flowup_effective_amounts,
    write_flowup_allocation_output,
)
from services.amount_service import (
    build_k1_amounts,
    build_ubti_amounts,
    compute_state_mapped_amounts,
)
from services.effective_pct_service import (
    compute_effective_percentages,
    apply_exclude_from_residual,
    apply_pe_book_unmapped_lines,
)
from services.writer_service import write_allocation_output, update_allocation_input
try:
    import services.amount_service as _amount_service
except ImportError:
    from .services import amount_service as _amount_service

logger = logging.getLogger(__name__)


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
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = checkpoint(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    return result


@contextmanager
def use_v2_amount_checkpoint():
    original = getattr(_amount_service, "checkpoint", None)
    _amount_service.checkpoint = _checkpoint
    try:
        yield
    finally:
        if original is not None:
            _amount_service.checkpoint = original



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
    """Main entry — Checkpoint V2 on multi-consumer frames; sequential writes."""
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
    parallel_activity = []
    enabled_groups = parse_enabled_groups(parallel_groups, ParallelGroups)
    profile_name = _blank(ExecutionProfile) or _blank(execution_profile) or "low"
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

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    rows = None
    status = {
        "sp_name": "usp_SM_LoadLookThroughEffectiveAllocationPercentage",
        "run_id": None,
        "entity_id": None,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
        "skip_reason": None,
    }

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
        cfg.setdefault("_checkpoint_paths", [])
        cfg.setdefault("_checkpoint_v2_activity", [])

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
                "checkpoint_mode": mode,
                "max_threads": workers,
                "execution_profile": profile_name,
            }
        )
        initialize_checkpoint_V2(cfg, mode)
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
            status["skip_reason"] = "allocation_type_invalid"
            status["elapsed_seconds"] = round(time.time() - t0, 1)
            return status

        with use_v2_amount_checkpoint():
            mappings = build_mapping_data(spark, cfg)
            mappings["distinct_mappings"] = _checkpoint(
                spark,
                mappings["distinct_mappings"],
                "distinct_mappings",
                cfg,
            )
            mappings["distinct_ubti_mappings"] = _checkpoint(
                spark,
                mappings["distinct_ubti_mappings"],
                "distinct_ubti_mappings",
                cfg,
            )
            is_flowup = cfg.get("is_sidepocket_flowup_partner", False)
            sm_fp_lt = read_table(
                spark, "SM_FlowUpPartnerLookThroughAllocationInput", cfg
            )
            has_flowup_input = (
                len(
                    sm_fp_lt.filter(F.col("RunID") == cfg["run_id"]).head(1)
                )
                > 0
            )
            fp_data = None
            if is_flowup or has_flowup_input:
                fp_data = build_flowup_k1_amounts(spark, cfg)
                fp_total_amounts = compute_flowup_mapped_amounts(
                    spark, cfg, fp_data, mappings
                )
                fp_effective = compute_flowup_effective_amounts(
                    spark,
                    cfg,
                    fp_total_amounts,
                    mappings["state_mapped_lines"],
                )
                # Flow-up Output write stays before the main write.
                write_flowup_allocation_output(spark, cfg, fp_effective)
            else:
                fp_data = build_flowup_k1_amounts(spark, cfg)
                logger.info(
                    "[SKIP] Flow-up partner pipeline: condition not met"
                )

            from services.amount_service import (
                _build_sm_lt_input_and_state_lines,
            )
            (
                sm_lt_input,
                state_lines,
                fed_lines,
                non_sp_fp,
                pruned_dm,
                pruned_ubti_dm,
            ) = _build_sm_lt_input_and_state_lines(spark, cfg, mappings)
            sm_lt_input = _checkpoint(
                spark, sm_lt_input, "temp_alloc_input", cfg
            )
            k1_pair, ubti_pair = run_parallel(
                [
                    (
                        "k1_amounts",
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
                        "ubti_amounts",
                        lambda: build_ubti_amounts(
                            spark, cfg, fed_lines, non_sp_fp
                        ),
                    ),
                ],
                workers,
                parallel_activity,
                "independent_amounts",
                enabled_groups,
            )
            partner_alloc, total_input = k1_pair
            partner_alloc_ubti, total_ubti_input = ubti_pair
            partner_alloc = _checkpoint(
                spark, partner_alloc, "alloc_pass1", cfg
            )
            partner_alloc_ubti = _checkpoint(
                spark, partner_alloc_ubti, "alloc_pass2", cfg
            )
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
            total_amounts = _checkpoint(
                spark, total_amounts, "alloc_pass3", cfg
            )
            effective_amounts, temp_effective = compute_effective_percentages(
                spark, cfg, total_amounts, sm_lt_input
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
            effective_amounts = _checkpoint(
                spark, effective_amounts, "alloc_output", cfg
            )

            partner_snap = read_table(spark, "Partner_Snapshot", cfg)
            partner_snapshot = partner_snap.filter(
                F.coalesce(F.col("WorkFlowID"), F.col("Transactionid"))
                == cfg["partner_txn_or_wf_id"]
            )
            # Sequential: write_allocation_output then update_allocation_input.
            rows = write_allocation_output(
                spark, cfg, effective_amounts, partner_snapshot
            )
            update_allocation_input(spark, cfg, effective_amounts)
            status["sections_completed"] = 13
            status["status"] = "SUCCESS"
    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error(f"[FAIL] {e}", exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(time.time() - t0, 1)
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



# ════════════════════════════════════════════════════════════════
# __main__: standalone execution (Mode 3).
# Read widget params → call run_sm_load_lt_effective_alloc_pct(...).
# The function's `if cfg is None` branch is the single point that calls
# load_common_config. Job/Orchestrator modes pass cfg in directly and skip
# this block.
# ════════════════════════════════════════════════════════════════

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
