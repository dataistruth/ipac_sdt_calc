"""Optimized orchestrator for footnote allocation output.

This module preserves the production S1-S13 flow while using the updated
package's namespaced checkpoints and collecting coarse-grained stage timings.
Business logic remains in the parent ``output`` package.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, Iterator

import pyspark.sql.functions as F

from Common_V2.core.config import load_common_config

from ..allocation_704c import (
    apply_704c_deduction,
    build_704c_allocation_output,
    build_704c_config,
    build_allocation_percentage_temp,
)
from ..allocation_effective import (
    build_effective_pct_allocation,
    resolve_min_quarter,
)
from ..allocation_input import (
    build_temp_allocation_input,
    build_temp_book_effective,
    build_temp_final_effective_pct,
    build_zero_exclude_lines,
)
from ..config import load_sp_config, validate_run_preconditions
from ..quarter_logic import (
    update_form_quarters,
    update_pfic_partv_quarters,
    update_pfic_quarters_by_config,
)
from ..underlyings import (
    build_cost_percentage_data,
    build_underlyings_footnotes_ordered,
    filter_asset_class,
)
from ..writers import apply_deduction, write_allocation_output
from .checkpoint import checkpoint, drop_checkpoints
from .join_optimizations import (
    broadcast_part_v_lines,
    broadcast_zero_exclude_lines,
    build_custom_footnote_line_types,
    derive_cost_underlying_types,
    quarter_join_hints,
)

# Updated-only reimplementations of two plan-heavy builders that add lineage
# breaks at the plan-explosion seams (8-level union tree; 5-pass anti-join
# chain). Semantics are identical to production; parity is verified by the
# benchmark fingerprint checks.
from .plan_break_optimizations import (
    build_allocation_input,
    build_entity_hierarchy,
)

logger = logging.getLogger(__name__)


@contextmanager
def _timed(
    timings: list[dict[str, Any]],
    step: str,
) -> Iterator[None]:
    started = time.time()
    try:
        yield
    finally:
        timings.append(
            {
                "step": step,
                "elapsed_seconds": round(time.time() - started, 3),
            }
        )


def run_load_footnotes_allocation_to_output(
    spark,
    cfg: dict = None,
    verbose: bool = False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    RankForRulePickup: int = None,
    **kwargs,
):
    """Allocate footnote amounts using the production S1-S13 semantics."""
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName
    rank_for_rule_pickup = RankForRulePickup

    t0 = time.time()
    timings: list[dict[str, Any]] = []
    parallel_workers = max(1, min(int(kwargs.pop("parallel_workers", 4)), 8))
    planning_pool = None

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    status = {
        "sp_name": "uspLoadFootnotesAllocationToOutput",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    try:
        with _timed(timings, "S1-S2/config"):
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

            # Keep checkpoint state invocation-local for thread safety.
            cfg = {**cfg, "_checkpoint_tables": []}

            if rank_for_rule_pickup is not None:
                cfg["rank_for_rule_pickup"] = rank_for_rule_pickup
            assert cfg.get("rank_for_rule_pickup") is not None, (
                "rank_for_rule_pickup must be provided"
            )

            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")

            from Common_V2.core.helpers import read_table as _rt_pre

            cfg["_df_pfic_footnote_line_item"] = _rt_pre(
                spark, "PFICFootnoteLineItem", cfg
            )
            cfg["_df_entity"] = _rt_pre(spark, "Entity", cfg)
            load_sp_config(spark, cfg)
            preconditions_met = validate_run_preconditions(spark, cfg)

        if not preconditions_met:
            logger.info(
                f"[SKIP] RunStatus=FAIL or wrong allocation type. "
                f"RunID={cfg['run_id']}, EntityID={cfg['entity_id']}"
            )
            status["status"] = "SKIPPED"
            return status

        planning_pool = ThreadPoolExecutor(
            max_workers=parallel_workers,
            thread_name_prefix="footnote-plan",
        )
        # Cost construction is independent of S3 and S4. Submit it first so
        # any catalog/planning work can overlap the four initial-load plans.
        cost_future = planning_pool.submit(
            build_cost_percentage_data, spark, cfg
        )
        initial_load_futures = {
            "book": planning_pool.submit(
                build_temp_book_effective, spark, cfg
            ),
            "allocation": planning_pool.submit(
                build_temp_allocation_input, spark, cfg
            ),
            "zero_exclude": planning_pool.submit(
                build_zero_exclude_lines, spark, cfg
            ),
            "effective": planning_pool.submit(
                build_temp_final_effective_pct, spark, cfg
            ),
        }

        with _timed(timings, "S3 initial loads"):
            df_temp_book_eff = initial_load_futures["book"].result()
            df_temp_alloc_input = initial_load_futures["allocation"].result()
            df_zero_exclude = broadcast_zero_exclude_lines(
                initial_load_futures["zero_exclude"].result()
            )
            df_temp_final_eff_pct = initial_load_futures["effective"].result()
            status["sections_completed"] = 3

        with _timed(timings, "S4 quarter updates+checkpoint"):
            with quarter_join_hints():
                df_temp_alloc_input, df_part_v_allocable = (
                    update_pfic_partv_quarters(
                        spark,
                        cfg,
                        df_temp_alloc_input,
                        df_temp_final_eff_pct,
                    )
                )
                df_part_v_allocable = broadcast_part_v_lines(
                    df_part_v_allocable
                )
                df_temp_alloc_input = update_pfic_quarters_by_config(
                    spark,
                    cfg,
                    df_temp_alloc_input,
                    df_part_v_allocable,
                    df_temp_final_eff_pct,
                )
                df_temp_alloc_input = update_form_quarters(
                    spark,
                    cfg,
                    df_temp_alloc_input,
                )
            df_temp_alloc_input = checkpoint(
                spark,
                df_temp_alloc_input,
                "temp_alloc_input",
                cfg,
            )
            status["sections_completed"] = 4

        with _timed(timings, "S5 cost"):
            (
                df_cost_pct_snapshot,
                _df_cost_underlying_types_lazy,
            ) = cost_future.result()
            planning_pool.shutdown(wait=True)
            planning_pool = None
            # Materialize the 4-way union + distinct once. Both the snapshot and
            # its underlying-types subset are consumed by several hierarchy
            # branches in S6; without this break the union + distinct is
            # re-evaluated multiple times inside the ~70s all_underlyings
            # checkpoint. Re-derive the subset from the materialized snapshot so
            # the heavy work is computed a single time.
            del _df_cost_underlying_types_lazy
            df_cost_pct_snapshot = checkpoint(
                spark,
                df_cost_pct_snapshot,
                "cost_snapshot",
                cfg,
            )
            df_temp_cost_underlying_types = derive_cost_underlying_types(
                df_cost_pct_snapshot
            )
            status["sections_completed"] = 5

        with _timed(timings, "S6 hierarchy"):
            df_all_underlyings, df_asset_class_rel = build_entity_hierarchy(
                spark,
                cfg,
                df_cost_pct_snapshot,
                df_temp_cost_underlying_types,
            )
            status["sections_completed"] = 6

        with _timed(timings, "S7 filter+checkpoint"):
            df_all_underlyings = filter_asset_class(
                spark,
                cfg,
                df_all_underlyings,
                df_asset_class_rel,
            )
            df_all_underlyings = checkpoint(
                spark,
                df_all_underlyings,
                "all_underlyings",
                cfg,
            )
            status["sections_completed"] = 7

        with _timed(timings, "S8 ordering+checkpoint"):
            df_underlyings_fn = build_underlyings_footnotes_ordered(
                spark,
                cfg,
                df_all_underlyings,
                df_temp_alloc_input,
            )
            df_underlyings_fn = checkpoint(
                spark,
                df_underlyings_fn,
                "underlyings_fn",
                cfg,
            )
            status["sections_completed"] = 8

        with _timed(timings, "S9 allocation input+checkpoint"):
            df_alloc_input = build_allocation_input(
                spark,
                cfg,
                df_temp_alloc_input,
                df_temp_book_eff,
                df_underlyings_fn,
            )
            df_alloc_input = checkpoint(
                spark,
                df_alloc_input,
                "alloc_input",
                cfg,
            )
            status["sections_completed"] = 9

        if df_alloc_input.isEmpty():
            logger.info(
                "[SKIP] #AllocationInput is empty — nothing to allocate"
            )
            status["status"] = "SKIPPED"
            return status

        with _timed(timings, "S10 704c"):
            df_tmp_alloc_output_704c = None
            df_custom_fn_types = build_custom_footnote_line_types(spark, cfg)

            build_704c_config(spark, cfg)

            if cfg.get("is_704c_enabled"):
                df_alloc_pct = build_allocation_percentage_temp(spark, cfg)
                result_704c = build_704c_allocation_output(
                    spark,
                    cfg,
                    df_alloc_input,
                    df_alloc_pct,
                    df_custom_fn_types,
                )
                if result_704c is not None:
                    df_tmp_alloc_output_704c, df_alloc_input = result_704c
            status["sections_completed"] = 10

        with _timed(timings, "S11 deduction"):
            df_alloc_input, df_fn_allocated_lines = apply_704c_deduction(
                spark,
                cfg,
                df_alloc_input,
                df_tmp_alloc_output_704c,
                df_zero_exclude,
            )
            status["sections_completed"] = 11

        with _timed(timings, "S12 effective"):
            from Common_V2.core.helpers import read_table as _rt
            from pyspark.sql import Window as W

            _ps = _rt(spark, "Partner_Snapshot", cfg).filter(
                (F.col("ClientID") == cfg["client_id"])
                & (F.col("TaxPeriodID") == cfg["tax_period_id"])
                & (F.col("EntityID") == cfg["entity_id"])
            )
            _w = W.partitionBy("EntityID")
            _ps_latest = (
                _ps.withColumn(
                    "_wf", F.coalesce(F.col("WorkFlowID"), F.lit(0))
                )
                .withColumn(
                    "_tx", F.coalesce(F.col("TransactionID"), F.lit(0))
                )
                .withColumn("_max_wf", F.max("_wf").over(_w))
                .withColumn("_max_tx", F.max("_tx").over(_w))
                .filter(
                    F.when(
                        F.col("_max_wf") != 0,
                        F.col("_wf") == F.col("_max_wf"),
                    ).otherwise(F.col("_tx") == F.col("_max_tx"))
                )
            )
            df_entity_partners = F.broadcast(
                _ps_latest.select(
                    F.col("PartnerNumber").alias("partnernumber"),
                    F.col("ShareClass"),
                ).distinct()
            )

            resolve_min_quarter(spark, cfg)
            df_tmp_alloc_output_eff = build_effective_pct_allocation(
                spark,
                cfg,
                df_alloc_input,
                df_temp_final_eff_pct,
                df_entity_partners,
                df_custom_fn_types,
            )
            status["sections_completed"] = 12

        with _timed(timings, "S13 writes"):
            if (
                df_tmp_alloc_output_704c is not None
                and df_tmp_alloc_output_eff is not None
            ):
                shared_cols = [
                    "RunID",
                    "ClientID",
                    "EntityID",
                    "ShareClass",
                    "PartnerNumber",
                    "LineTypeID",
                    "QuicklinkID",
                    "LineID",
                    "Amount",
                    "AllocationType",
                    "ParentEntityID",
                    "SuperParentEntityID",
                    "AllocationTypeID",
                    "TrackingKey",
                    "OriginalParentEntityID",
                    "SchID",
                ]
                df_704c_norm = df_tmp_alloc_output_704c
                if "SchID" not in df_704c_norm.columns:
                    df_704c_norm = df_704c_norm.withColumn(
                        "SchID", F.lit(None).cast("int")
                    )
                df_combined = df_704c_norm.select(*shared_cols).unionByName(
                    df_tmp_alloc_output_eff.select(*shared_cols)
                )
            elif df_tmp_alloc_output_eff is not None:
                df_combined = df_tmp_alloc_output_eff
            elif df_tmp_alloc_output_704c is not None:
                df_combined = df_tmp_alloc_output_704c
                if "SchID" not in df_combined.columns:
                    df_combined = df_combined.withColumn(
                        "SchID", F.lit(None).cast("int")
                    )
            else:
                df_combined = None

            if df_combined is not None:
                if parallel_workers > 1:
                    with ThreadPoolExecutor(
                        max_workers=2,
                        thread_name_prefix="footnote-write",
                    ) as write_pool:
                        output_future = write_pool.submit(
                            write_allocation_output,
                            spark,
                            {**cfg},
                            df_combined,
                        )
                        deduction_future = write_pool.submit(
                            apply_deduction,
                            spark,
                            {**cfg},
                            df_combined,
                            df_alloc_input,
                            df_fn_allocated_lines,
                            df_zero_exclude,
                        )
                        output_future.result()
                        deduction_future.result()
                else:
                    write_allocation_output(spark, cfg, df_combined)
                    apply_deduction(
                        spark,
                        cfg,
                        df_combined,
                        df_alloc_input,
                        df_fn_allocated_lines,
                        df_zero_exclude,
                    )
            status["sections_completed"] = 13

    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error(f"[FAIL] {e}", exc_info=True)
        raise
    finally:
        if planning_pool is not None:
            planning_pool.shutdown(wait=True, cancel_futures=True)
        try:
            if isinstance(cfg, dict):
                drop_checkpoints(spark, cfg)
        finally:
            wall = round(time.time() - t0, 3)
            status["elapsed_seconds"] = round(wall, 1)
            checkpoint_timings = (
                list(cfg.get("_updated_checkpoint_timings", []))
                if isinstance(cfg, dict)
                else []
            )
            status["timings"] = timings + checkpoint_timings
            status["updated_wall_seconds"] = wall
            status["optimization_profile"] = {
                "checkpoint_backend": "uc_delta_stats_off",
                "checkpoint_count": len(checkpoint_timings),
                "spark_session_tuning": "none",
                "parallel_workers": parallel_workers,
                "parallel_scopes": [
                    "S3 initial plans + S5 cost plan",
                    "S13 output insert + input deduction",
                ],
                "broadcast_strategy": "bounded_lookup_and_update_sets_only",
            }

    logger.info(
        f"[DONE] run_load_footnotes_allocation_to_output | "
        f"{status['elapsed_seconds']}s | "
        f"RunID={cfg['run_id']} EntityID={cfg['entity_id']}"
    )
    return status


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    status = run_load_footnotes_allocation_to_output(
        spark,
        RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
        EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
        ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
        TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
        CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
        SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
        RankForRulePickup=int(  # noqa: F821
            dbutils.widgets.get("rank_for_rule_pickup")  # noqa: F821
        ),
    )

    try:
        dbutils.notebook.exit(json.dumps(status))  # noqa: F821
    except Exception:
        print(json.dumps(status, indent=2))
