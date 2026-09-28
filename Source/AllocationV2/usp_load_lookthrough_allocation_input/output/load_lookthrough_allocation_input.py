"""
load_lookthrough_allocation_input.py — Orchestrator

Converted from: dbo.uspLoadLookThroughAllocationInput
Original: 1933 lines, Surgical mode (5 chunks)

Usage:
    from load_lookthrough_allocation_input import run_load_lookthrough_allocation_input

    run_load_lookthrough_allocation_input(
        spark,
        entity_id=37, client_id=15349, tax_period_id=15349, run_id=2183,
        catalog="dev7", schema="iPC_2025_dev7_15349",
    )
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time
import logging

from Common_V2.core.config import load_common_config

try:
    from .lt_helpers import logger, log_section, log_timing, checkpoint, drop_checkpoints
    from .lt_config_service import (
        load_config,
        load_workflows,
        load_lower_tier_funds,
        build_fx_rates,
        load_reclass_k1_data,
    )
    from .lt_k1_service import (
        build_k1_input,
        build_adjustments_input,
        build_lt_flowup_k1,
        build_rounding_diff,
        recompute_lt_input_from_lower_tier,
    )
    from .lt_flowup_service import (
        build_lt_flowup_adjustment,
        build_lt_flowup_m1,
    )
    from .lt_pfic_service import (
        build_pfic_elections,
        build_pfic_mapped_lines,
    )
    from .lt_pfic_conversion_service import (
        build_pfic_conversion,
        build_pfic_income_attributes,
    )
    from .lt_finalization_service import (
        build_box_jkl_input,
        apply_master_feed_exclusion,
        apply_blocker_entity,
        apply_tag_percentages,
        apply_line_exclusions,
        validate_k3_rules,
        write_final_output,
    )
except ImportError:
    from lt_helpers import logger, log_section, log_timing, checkpoint, drop_checkpoints
    from lt_config_service import (
        load_config,
        load_workflows,
        load_lower_tier_funds,
        build_fx_rates,
        load_reclass_k1_data,
    )
    from lt_k1_service import (
        build_k1_input,
        build_adjustments_input,
        build_lt_flowup_k1,
        build_rounding_diff,
        recompute_lt_input_from_lower_tier,
    )
    from lt_flowup_service import (
        build_lt_flowup_adjustment,
        build_lt_flowup_m1,
    )
    from lt_pfic_service import (
        build_pfic_elections,
        build_pfic_mapped_lines,
    )
    from lt_pfic_conversion_service import (
        build_pfic_conversion,
        build_pfic_income_attributes,
    )
    from lt_finalization_service import (
        build_box_jkl_input,
        apply_master_feed_exclusion,
        apply_blocker_entity,
        apply_tag_percentages,
        apply_line_exclusions,
        validate_k3_rules,
        write_final_output,
    )


def run_load_lookthrough_allocation_input(
    spark: SparkSession,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = "dev7",
    SchemaName: str = "iPC_2025_dev7_15349",
    cfg: dict = None,
    verbose: bool = False,
    ResultType: str = "deltalake",
    VolumePath: str = None,
    ExecutionID: str = None,
    **kwargs,
):
    """
    Load lookthrough allocation input data.
    Populates LookThroughAllocationInput, SchKTaxableIncome,
    PFICtoK1IncomeAttributePercentages, AllocationRunErrors.
    """
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

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    status = {
        "sp_name": "uspLoadLookThroughAllocationInput",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    try:
        # --- Section 1: Config ---
        # Mode 3 standalone: build cfg from IDs via load_common_config.
        # Modes 1/2 (Job/Orchestrator): cfg is passed in pre-built.
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
        cfg.setdefault("_checkpoint_tables", [])

        # Apply SP-local scalar aliases + TechConfig reads
        load_config(spark, cfg)

        # Pass through output options
        cfg.setdefault("result_type", result_type)
        if volume_path is not None:
            cfg["volume_path"] = volume_path
        if execution_id is not None:
            cfg["execution_id"] = execution_id

        if cfg["run_status"] == "FAIL":
            logger.error(f"RunStatus=FAIL — aborting. RunID={cfg['run_id']}")
            status["status"] = "SKIPPED"
            status["error"] = "RunStatus=FAIL"
            return status

        status["sections_completed"] = 1

        # --- Section 2: Load workflows ---
        k1_workflow_df, adjustment_workflow_df = load_workflows(spark, cfg)
        status["sections_completed"] = 2

        # --- Section 3: Load lower tier funds ---
        lower_tier_funds_df = load_lower_tier_funds(spark, cfg)
        status["sections_completed"] = 3

        # --- Section 4: Build FX rates ---
        fx_rates_df = build_fx_rates(spark, cfg, k1_workflow_df)
        status["sections_completed"] = 4

        # --- Section 4b: Load ReclassK1 data ---
        reclass_k1_df = load_reclass_k1_data(spark, cfg)
        status["sections_completed"] = 4

        # --- Section 5: Build K1 input ---
        k1_input_df = build_k1_input(spark, cfg, k1_workflow_df, fx_rates_df)
        status["sections_completed"] = 5

        # --- Section 6: Build adjustments input ---
        adj_input_df = build_adjustments_input(spark, cfg, adjustment_workflow_df, fx_rates_df)
        status["sections_completed"] = 6

        # --- Section 7: Build LT flowup K1 (raw lower tier amounts) ---
        lower_tier_amount_df = build_lt_flowup_k1(spark, cfg, reclass_k1_df)
        status["sections_completed"] = 7

        # --- Section 8: Build rounding diff (appends correction rows) ---
        lower_tier_amount_df = build_rounding_diff(spark, cfg, lower_tier_amount_df)
        status["sections_completed"] = 8

        # Group lower_tier_amount into alloc input format (AFTER rounding diff)
        lt_k1_input_df = recompute_lt_input_from_lower_tier(cfg, lower_tier_amount_df)

        # Accumulate all input into one DataFrame
        alloc_input_df = k1_input_df
        if adj_input_df is not None:
            alloc_input_df = alloc_input_df.unionByName(adj_input_df, allowMissingColumns=True)
        alloc_input_df = alloc_input_df.unionByName(lt_k1_input_df, allowMissingColumns=True)

        # --- Section 9: Build LT flowup adjustment ---
        lt_adj_input_df = build_lt_flowup_adjustment(spark, cfg, lower_tier_funds_df)
        if lt_adj_input_df is not None:
            alloc_input_df = alloc_input_df.unionByName(lt_adj_input_df, allowMissingColumns=True)
        status["sections_completed"] = 9

        # --- Section 10: Build LT flowup M1 ---
        lt_m1_input_df = build_lt_flowup_m1(spark, cfg, lower_tier_funds_df)
        if lt_m1_input_df is not None:
            alloc_input_df = alloc_input_df.unionByName(lt_m1_input_df, allowMissingColumns=True)
        status["sections_completed"] = 10

        # Break lineage: 5+ unions accumulated, 9 downstream consumers
        alloc_input_df = checkpoint(spark, alloc_input_df, "alloc_input_post_unions", cfg)

        # --- Section 11: Build PFIC elections ---
        pfic_data = build_pfic_elections(spark, cfg)
        status["sections_completed"] = 11

        # --- Section 12: Build PFIC mapped lines ---
        alloc_input_df, pfic_mapped_df, fcc_blocked_df, reclass_unblocked_df = \
            build_pfic_mapped_lines(spark, cfg, alloc_input_df)
        status["sections_completed"] = 12

        # --- Section 13: Build PFIC conversion (PFIC→K1 line items) ---
        pfic_alloc_input_df, converted_pfic_amounts_df = build_pfic_conversion(
            spark, cfg, reclass_unblocked_df, pfic_mapped_df,
            pfic_data, lower_tier_funds_df, pfic_data.get("pfic_footnote_entity_details"),
            lower_tier_amount_df,
            pfic_types_df=pfic_data.get("pfic_types"),
        )
        if pfic_alloc_input_df is not None:
            alloc_input_df = alloc_input_df.unionByName(pfic_alloc_input_df, allowMissingColumns=True)
        status["sections_completed"] = 13

        # --- Section 14: Build PFIC income attributes ---
        pfic_income_attr_df = build_pfic_income_attributes(
            spark, cfg, converted_pfic_amounts_df,
            lower_tier_amount_df, pfic_mapped_df, lower_tier_funds_df,
        )
        status["sections_completed"] = 14

        # Break lineage before finalization: PFIC added more joins, 7 downstream consumers
        alloc_input_df = checkpoint(spark, alloc_input_df, "alloc_input_post_pfic", cfg)

        # --- Section 15: Build Box JKL input ---
        alloc_input_df = build_box_jkl_input(spark, cfg, alloc_input_df, fx_rates_df)
        status["sections_completed"] = 15

        # --- Section 16: Apply master feed exclusion ---
        alloc_input_df = apply_master_feed_exclusion(spark, cfg, alloc_input_df)
        status["sections_completed"] = 16

        # --- Section 17: Apply blocker entity ---
        alloc_input_df = apply_blocker_entity(spark, cfg, alloc_input_df)
        status["sections_completed"] = 17

        # --- Section 18: Apply tag percentages ---
        alloc_input_df = apply_tag_percentages(spark, cfg, alloc_input_df, reclass_k1_df)
        status["sections_completed"] = 18

        # --- Section 19: Apply line exclusions ---
        alloc_input_df = apply_line_exclusions(spark, cfg, alloc_input_df)
        status["sections_completed"] = 19

        # --- Section 20: Validate K3 rules ---
        k3_status = validate_k3_rules(spark, cfg, alloc_input_df)
        status["sections_completed"] = 20
        if k3_status == "FAIL":
            status["status"] = "FAIL"
            status["error"] = "K3 Validations failed"
            return status

        # --- Section 21: Write final output ---
        write_final_output(spark, cfg, alloc_input_df, lower_tier_funds_df, pfic_income_attr_df)
        status["sections_completed"] = 21

        status["status"] = "SUCCESS"
        logger.info(f"[DONE] All sections complete. LookThroughAllocationInput written.")

    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error(f"[ERROR] {e}", exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        drop_checkpoints(spark, cfg)

    return status
