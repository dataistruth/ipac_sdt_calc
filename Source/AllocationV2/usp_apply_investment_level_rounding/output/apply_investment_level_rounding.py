"""
apply_investment_level_rounding — Main orchestrator.

Converted from: uspApplyInvestmentLevelRounding.sql
Original procedure: dbo.uspApplyInvestmentLevelRounding

Usage:
    from output.apply_investment_level_rounding import apply_investment_level_rounding

    apply_investment_level_rounding(
        spark,
        EntityID=100, ClientID=9999, TaxPeriodID=1, RunID=500,
        CatalogName="my_catalog", SchemaName="my_schema",
        ResultType="Parquet",
        VolumePath="/Volumes/my_catalog/my_schema/parquet-output",
        ExecutionID="abc-123",
    )
"""

from pyspark.sql import SparkSession
import logging
import time
import sys
import os

# Ensure Common_V2 parent (Source/) is on sys.path for imports
_source_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _source_dir not in sys.path:
    sys.path.insert(0, _source_dir)

try:
    from Common_V2.core.writers import collect_parquet_result
except ImportError:
    from core.writers import collect_parquet_result

from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import get_logger
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.execution_profiles import resolve_execution_profile

from .parallel_helpers import (
    isolated_cfg,
    normalize_workers,
    parse_enabled_groups,
    run_parallel,
)
from .write_helpers import flush_post_summary_writes

logger = get_logger("apply_investment_level_rounding")


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


def _checkpoint_frame(spark, df, name, cfg):
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


def _load_lookthrough(spark, cfg):
    is_inv_level = cfg.get("is_investment_level_rounding") == "C"
    has_call_from = (cfg.get("call_from") or "") != ""
    if is_inv_level and has_call_from:
        return build_lookthrough_output_inv_level_callfrom(spark, cfg)
    if is_inv_level:
        return build_lookthrough_output_inv_level_no_callfrom(spark, cfg)
    if has_call_from:
        return build_lookthrough_output_entity_callfrom(spark, cfg)
    return build_lookthrough_output_entity_no_callfrom(spark, cfg)

try:
    from .config_service import load_sp_config
    from .lookthrough_service import (
        build_lookthrough_output_inv_level_callfrom,
        build_lookthrough_output_inv_level_no_callfrom,
        build_lookthrough_output_entity_callfrom,
        build_lookthrough_output_entity_no_callfrom,
    )
    from .input_service import build_allocation_input, build_ubti_passive_input
    from .aggregation_service import (
        build_temp_allocation_output,
        compute_rounding_diff,
        compute_max_allocation_type,
    )
    from .partner_service import (
        build_partner_snapshots,
        build_highest_percent_partner,
        build_nocost_partner,
    )
    from .rounding_service import (
        apply_rounding_override,
        apply_rounding_plugged_to_gp,
        apply_rounding_highest_percent,
        apply_rounding_highest_amount,
        apply_rounding_none,
    )
    from .write_service import (
        apply_book_k1_not_rounded_passthrough,
        write_final_summaries,
        write_allocation_summaries,
        update_is_rounded_flag,
    )
except ImportError:
    from config_service import load_sp_config
    from lookthrough_service import (
        build_lookthrough_output_inv_level_callfrom,
        build_lookthrough_output_inv_level_no_callfrom,
        build_lookthrough_output_entity_callfrom,
        build_lookthrough_output_entity_no_callfrom,
    )
    from input_service import build_allocation_input, build_ubti_passive_input
    from aggregation_service import (
        build_temp_allocation_output,
        compute_rounding_diff,
        compute_max_allocation_type,
    )
    from partner_service import (
        build_partner_snapshots,
        build_highest_percent_partner,
        build_nocost_partner,
    )
    from rounding_service import (
        apply_rounding_override,
        apply_rounding_plugged_to_gp,
        apply_rounding_highest_percent,
        apply_rounding_highest_amount,
        apply_rounding_none,
    )
    from write_service import (
        apply_book_k1_not_rounded_passthrough,
        write_final_summaries,
        write_allocation_summaries,
        update_is_rounded_flag,
    )


def apply_investment_level_rounding(
    spark: SparkSession,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CallFrom: str = None,
    CatalogName: str = "dev7",
    SchemaName: str = "iPC_2025_dev7_15349",
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
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    SqlShufflePartitions=None,
    **kwargs,
):
    """Apply investment-level rounding with Checkpoint V2 and parallel phases."""
    del kwargs
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    call_from = CallFrom
    catalog = CatalogName
    schema = SchemaName
    result_type = ResultType
    volume_path = VolumePath
    execution_id = ExecutionID
    t0 = time.time()
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
    return_value_summaries = None
    return_value_alloc = None
    if verbose:
        logger.setLevel(logging.DEBUG)
    status = {
        "sp_name": "uspApplyInvestmentLevelRounding",
        "run_id": run_id,
        "entity_id": entity_id,
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
                call_from=call_from,
            )
        elif call_from is not None:
            cfg["call_from"] = call_from
        cfg = {**cfg, "_checkpoint_tables": []}
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
        print(
            f"[uspApplyInvestmentLevelRounding] ExecutionProfile={profile_name} "
            f"CheckpointMode={mode} shuffle="
            f"{shuffle_override or profile['shuffle_partitions']} "
            f"MaxThreads={workers}"
        )
        cfg = load_sp_config(spark, cfg)
        if cfg.get("run_status") == "FAIL":
            status["status"] = "SKIPPED"
            status["error"] = "RunStatus=FAIL"
            status["skip_reason"] = "run_status_fail"
            return status

        not_rounded_lines_df = cfg.get("not_rounded_lines_df")
        partner_cfg = isolated_cfg(cfg)

        def _lookthrough_task():
            return _load_lookthrough(spark, cfg)

        def _partners_task():
            return build_partner_snapshots(spark, partner_cfg)

        lookthrough_pair, partner_pair = run_parallel(
            [
                ("lookthrough", _lookthrough_task),
                ("partner_snapshots", _partners_task),
            ],
            workers,
            parallel_activity,
            "independent_early",
            enabled_groups,
        )
        lookthrough_output_df, lookthrough_input_df = lookthrough_pair
        partner_snapshot_df, rounding_override_df = partner_pair
        cfg["partner_snapshot_df"] = partner_cfg.get("partner_snapshot_df")
        cfg["has_rounding_override"] = partner_cfg.get(
            "has_rounding_override", False
        )
        lookthrough_output_df = _checkpoint_frame(
            spark, lookthrough_output_df, "lookthrough_output", cfg
        )
        lookthrough_input_df = _checkpoint_frame(
            spark, lookthrough_input_df, "lookthrough_input", cfg
        )
        partner_snapshot_df = _checkpoint_frame(
            spark, partner_snapshot_df, "partners", cfg
        )
        status["sections_completed"] = 12

        def _alloc_input_task():
            return build_allocation_input(
                spark, cfg, lookthrough_input_df, not_rounded_lines_df
            )

        def _temp_out_task():
            return build_temp_allocation_output(
                spark, cfg, lookthrough_output_df, not_rounded_lines_df
            )

        def _max_type_task():
            return compute_max_allocation_type(
                spark, cfg, lookthrough_output_df
            )

        allocation_input_df, temp_alloc_output_df, max_alloc_type_df = (
            run_parallel(
                [
                    ("allocation_input", _alloc_input_task),
                    ("temp_alloc_output", _temp_out_task),
                    ("max_alloc_type", _max_type_task),
                ],
                workers,
                parallel_activity,
                "independent_builders",
                enabled_groups,
            )
        )
        status["sections_completed"] = 11

        allocation_input_df = build_ubti_passive_input(
            spark, cfg, allocation_input_df
        )
        allocation_input_df = _checkpoint_frame(
            spark, allocation_input_df, "temp_alloc_input", cfg
        )
        rounded_diff_df = compute_rounding_diff(
            spark, cfg, temp_alloc_output_df, allocation_input_df
        )
        temp_alloc_output_df = _checkpoint_frame(
            spark, temp_alloc_output_df, "temp_alloc_output", cfg
        )
        rounded_diff_df = _checkpoint_frame(
            spark, rounded_diff_df, "rounded_diff", cfg
        )
        max_alloc_type_df = _checkpoint_frame(
            spark, max_alloc_type_df, "max_alloc_type", cfg
        )
        status["sections_completed"] = 12

        rounding_logic = cfg.get("rounding_logic")
        rounding_override_import = cfg.get("rounding_override_import", False)
        has_rounding_override = cfg.get("has_rounding_override", False)
        k1_summary_df = None
        adjustment_summary_df = None
        alloc_output_detail_df = temp_alloc_output_df.select(
            "EntityID",
            "LineTypeID",
            "LineID",
            "ParentEntityID",
            "TrackingKey",
            "SuperParentEntityID",
            "AdjustmentTypeID",
            "Tag",
            "OriginalParentEntityID",
            "QuickLinkID",
        ).distinct()
        if has_rounding_override and rounding_override_import:
            k1_summary_df, adjustment_summary_df = apply_rounding_override(
                spark,
                cfg,
                temp_alloc_output_df,
                rounded_diff_df,
                max_alloc_type_df,
                rounding_override_df,
                alloc_output_detail_df,
            )
        elif rounding_logic == "Plugged to GP":
            k1_summary_df, adjustment_summary_df = apply_rounding_plugged_to_gp(
                spark,
                cfg,
                temp_alloc_output_df,
                rounded_diff_df,
                max_alloc_type_df,
                partner_snapshot_df,
            )
        elif rounding_logic == "Plugged to Highest Allocation Percent":
            highest_pct_partner_df = build_highest_percent_partner(
                spark, cfg, lookthrough_output_df
            )
            (
                nocost_df,
                rounding_partner_number,
                rounding_share_class,
            ) = build_nocost_partner(
                spark,
                cfg,
                lookthrough_output_df,
                highest_pct_partner_df,
                partner_snapshot_df,
            )
            cfg["rounding_partner_number"] = rounding_partner_number
            cfg["rounding_share_class"] = rounding_share_class
            k1_summary_df, adjustment_summary_df = apply_rounding_highest_percent(
                spark,
                cfg,
                temp_alloc_output_df,
                rounded_diff_df,
                max_alloc_type_df,
                highest_pct_partner_df,
                nocost_df,
                partner_snapshot_df,
            )
        elif rounding_logic == "Plugged to Highest Allocation Amount":
            k1_summary_df, adjustment_summary_df = apply_rounding_highest_amount(
                spark,
                cfg,
                temp_alloc_output_df,
                rounded_diff_df,
                max_alloc_type_df,
                partner_snapshot_df,
            )
        elif rounding_logic == "None":
            k1_summary_df, adjustment_summary_df = apply_rounding_none(
                spark, cfg, temp_alloc_output_df, max_alloc_type_df
            )
        status["sections_completed"] = 19
        if cfg.get("book_k1_adjustment_enabled", False):
            book_k1_passthrough_df = apply_book_k1_not_rounded_passthrough(
                spark, cfg, lookthrough_output_df, not_rounded_lines_df
            )
            if (
                k1_summary_df is not None
                and book_k1_passthrough_df is not None
            ):
                k1_summary_df = k1_summary_df.unionByName(
                    book_k1_passthrough_df, allowMissingColumns=True
                )
        status["sections_completed"] = 20
        k1_summary_df = _checkpoint_frame(spark, k1_summary_df, "k1_summary", cfg)

        k1_write_df, ubti_write_df, adj_write_df, return_value_summaries = (
            write_final_summaries(
                spark, cfg, k1_summary_df, adjustment_summary_df
            )
        )
        status["sections_completed"] = 21
        return_value_alloc = flush_post_summary_writes(
            spark,
            cfg,
            k1_write_df,
            ubti_write_df,
            adj_write_df,
            workers,
            parallel_activity,
            enabled_groups,
        )
        status["sections_completed"] = 23
    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.exception("Failed: %s", e)
        raise
    finally:
        if isinstance(cfg, dict):
            drop_checkpoints_V2(spark, cfg)
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        logger.info(
            "[COMPLETE] %s — %s in %ss, sections=%s/23",
            status["sp_name"],
            status["status"],
            status["elapsed_seconds"],
            status["sections_completed"],
        )

    import json
    combined = {}
    for rv in [return_value_summaries, return_value_alloc]:
        if rv and isinstance(rv, str):
            try:
                parsed = json.loads(rv)
                combined.update(parsed)
            except (json.JSONDecodeError, TypeError):
                pass
    if combined:
        result_json = json.dumps(combined)
        print(f"[PARQUET] Return JSON: {result_json}")
        return result_json
    return status


# ════════════════════════════════════════════════════════════════
# __main__: standalone execution (Mode 3).
# Read widget params → call apply_investment_level_rounding(...).
# The function's `if cfg is None` branch is the single point that calls
# load_common_config. Job/Orchestrator modes pass cfg in directly and skip
# this block.
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import json
    spark = SparkSession.builder.getOrCreate()

    try:
        result = apply_investment_level_rounding(
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
        dbutils.notebook.exit(json.dumps(result) if not isinstance(result, str) else result)  # noqa: F821
    except Exception:
        print(json.dumps(result, indent=2) if not isinstance(result, str) else result)
