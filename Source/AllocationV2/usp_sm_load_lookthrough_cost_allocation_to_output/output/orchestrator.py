"""
sm_load_lookthrough_cost_allocation_to_output.py

Converted from: dbo.usp_SM_LoadLookThroughCostAllocationToOutput.sql
Original procedure: dbo.usp_SM_LoadLookThroughCostAllocationToOutput
Conversion date: 2026-05-04

Usage (standalone):
    from sm_load_lookthrough_cost_allocation_to_output import run_sm_load_lookthrough_cost_allocation_to_output

    run_sm_load_lookthrough_cost_allocation_to_output(
        spark,
        entity_id=123, client_id=456, tax_period_id=789, run_id=1001,
        rank_for_rule_pickup=0,
        catalog="QA7", schema="iPC_2025_QA7_15347",
    )

Usage (reuse shared config from a workflow):
    cfg = load_common_config(spark, entity_id, client_id, ...)
    run_sm_load_lookthrough_cost_allocation_to_output(spark, cfg=cfg)
"""

from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from contextlib import contextmanager
from datetime import datetime
import logging
import time

from Common_V2.core.helpers import read_table, table_prefix, ns, ns0, sql_round, safe_divide
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2,
    drop_checkpoints_V2 as drop_checkpoints,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.execution_profiles import resolve_execution_profile
try:
    from parallel_helpers import normalize_workers, parse_enabled_groups, run_parallel
except ImportError:
    from .parallel_helpers import normalize_workers, parse_enabled_groups, run_parallel
from Common_V2.core.observability import get_logger, log_section, log_timing
from Common_V2.core.config import load_common_config, validate_run_status
from Common_V2.core.generic_result_storer import GenericResultStorer
from Common_V2.domain.udf_cost_percentage_details import get_cost_percentage_details
from Common_V2.domain.udf_partners_list_for_allocations import get_partners_list_for_allocations

logger = get_logger(__name__)


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
    result = checkpoint_V2(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    return result


# Builders call module-level checkpoint(). Point it at the wrapper after
# _checkpoint is defined. Never have _checkpoint call this name.
checkpoint = _checkpoint


@contextmanager
def use_v2_production_checkpoint():
    """No-op: checkpoint already routes to _checkpoint → checkpoint_V2."""
    yield




# ---------------------------------------------------------------------------
# Private helpers (module-scoped aliases)
# ---------------------------------------------------------------------------
_tbl = read_table
_ns = ns
_ns0 = ns0
_tp = table_prefix


# ---------------------------------------------------------------------------
# Function 1: load_sp_config
# SQL lines: 93–260 (S1-S2)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def load_sp_config(spark: SparkSession, cfg: dict) -> dict:
    """Alias common cfg scalars to the legacy SP-internal names used downstream.

    All scalar config (PE Book allocation type, GlobalMenu flags, ENU_LineType,
    AllocationRun fields, ENU_CustomAllocations IDs) comes from load_common_config.
    """
    log_section(logger, "load_sp_config")
    t0 = time.time()

    # --- ENU_AllocationLogic: PE Book Allocation ID ---
    cfg["allocation_type_id"] = cfg.get("pe_book_allocation_type_id")

    # --- Entity allocation type name ---
    cfg["allocation_type_name"] = cfg.get("entity_allocation_type_name")

    # --- GlobalMenu flags ---
    cfg["is_dated_transfers"] = cfg.get("flag_transfer_by_date")
    cfg["ignore_ac_partnership"] = cfg.get("flag_ignore_asset_class_partnership_level")
    cfg["override_indirect_ac"] = cfg.get("flag_override_indirect_lookthrough_asset_class")

    # --- AllocationRun-derived scalars (legacy SP names) ---
    cfg["custom_alloc_rule_workflow_id"] = cfg.get("state_custom_allocation_workflow_id") or 0
    cfg["dar_txn_id"] = cfg.get("dar_entity_transaction_id")
    cfg["global_dar_txn_id"] = cfg.get("dar_global_transaction_id")

    # --- ENU_CustomAllocations IDs (legacy SP names) ---
    cfg["cost_allocation_type_id"] = cfg.get("custom_allocation_id_cost")
    cfg["book_allocation_type_id"] = cfg.get("custom_allocation_id_book")
    cfg["offset_allocation_type_id"] = cfg.get("custom_allocation_id_offset")

    # k1_line_type_id already in cfg from load_common_config under the same name.

    log_timing(logger, "load_sp_config", t0)
    return cfg


# ---------------------------------------------------------------------------
# Function 2: validate_run_status
# SQL lines: 201-214 (S3)
# ---------------------------------------------------------------------------
def validate_run_status_for_sp(spark: SparkSession, cfg: dict) -> bool:
    """Check RunStatus and entity allocation type match.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 201-214.
    Returns True if SP should proceed, False to abort.
    """
    log_section(logger, "validate_run_status_for_sp")

    if cfg.get("run_status") == "FAIL":
        logger.warning("RunStatus=FAIL — aborting.")
        return False

    # Check entity allocation type matches PE Book Allocation (uses pre-resolved cfg scalars).
    entity_alloc_type = cfg.get("entity_allocation_type_id")
    if entity_alloc_type != cfg.get("allocation_type_id"):
        logger.warning(
            f"Entity AllocationTypeID ({entity_alloc_type}) != "
            f"PE Book Allocation ({cfg.get('allocation_type_id')}) — aborting."
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Function 3: build_book_effective
# SQL lines: 375-400 (S5)
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_book_effective(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load book effective snapshot from SM_StateLineAllocationRule_Snapshot.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 375-400.
    Row count: POSSIBLY-EMPTY (no matching CAR rules for this entity).
    """
    log_section(logger, "build_book_effective")
    t0 = time.time()

    book_alloc_id = cfg["book_allocation_type_id"]
    offset_alloc_id = cfg["offset_allocation_type_id"]

    # L381-386: SELECT INTO #TempBookEffective
    df = (
        _tbl(spark, "SM_StateLineAllocationRule_Snapshot", cfg)
        .filter(
            (F.col("WorkflowID") == cfg["custom_alloc_rule_workflow_id"])
            & (F.col("ClientID") == cfg["client_id"])
            & (F.col("TaxPeriodID") == cfg["tax_period_id"])
        )
        .select(
            "UnderlyingEntityID", "StateLineID", "StateID",
            "AllocationTypeid", "AdjustmentAllocationTypeID",
            "TrackingKey", "Tag",
        )
    )

    # L389-390: UPDATE #TempBookEffective SET AdjustmentAllocationTypeID = AllocationTypeid
    # WHERE AdjustmentAllocationTypeID IN (@BookAllocationTypeID, @OffsetAllocationTypeID)
    df = df.withColumn(
        "AdjustmentAllocationTypeID",
        F.when(
            F.col("AdjustmentAllocationTypeID").isin(book_alloc_id, offset_alloc_id),
            F.col("AllocationTypeid"),
        ).otherwise(F.col("AdjustmentAllocationTypeID")),
    )


    log_timing(logger, "build_book_effective", t0)
    return df


# ---------------------------------------------------------------------------
# Function 4: build_input_data_load
# SQL lines: 397-405 (S6)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_input_data_load(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load raw input data from SM_LookThroughAllocationInput.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 397-405.
    Row count: ALWAYS-NON-EMPTY.
    """
    log_section(logger, "build_input_data_load")
    t0 = time.time()

    # PERF: Project ONLY the columns consumed by downstream stages
    # (all_underlyings_states + 4 passes + pass4 select). Avoids scanning
    # and shuffling unused wide columns through the checkpoint Delta write.
    df = (
        _tbl(spark, "SM_LookThroughAllocationInput", cfg)
        .filter(
            (F.col("RunID") == cfg["run_id"])
            & (_ns0(F.col("Amount")) != 0)
            & (F.col("ClientID") == cfg["client_id"])
        )
        .select(
            "RunID", "ClientID", "EntityID", "LineTypeID",
            "StateLineID", "StateID", "Amount", "QuicklinkID",
            "Amount704b", "CategoryID", "ParentEntityID", "PeriodID",
            "LineCode", "SuperParentEntityID", "AdjustmentTypeID",
            "TrackingKey", "Tag", "OriginalParentEntityID",
        )
    )


    log_timing(logger, "build_input_data_load", t0)
    return df


# ---------------------------------------------------------------------------
# Function 5: build_cost_percentage_snapshot
# SQL lines: 409-425 (S7 first half)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_cost_percentage_snapshot(spark: SparkSession, cfg: dict) -> DataFrame:
    """Full udfGetCostPercentageDetails (4 unions + entity hierarchy) + join Enu_Underlyingtype.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 409-425.
    Row count: ALWAYS-NON-EMPTY.

    Calls Common_V2.domain.udf_cost_percentage_details.get_cost_percentage_details()
    which implements all 4 union components including deal-level hierarchy traversal.
    """
    log_section(logger, "build_cost_percentage_snapshot")
    t0 = time.time()

    workflow_id = cfg["cost_workflow_id"]

    # L415: FROM dbo.udfGetCostPercentageDetails(@CostPercentageWorkflowID)
    cost_pct_base = get_cost_percentage_details(spark, cfg, workflow_id)

    # L420-421: JOIN Enu_Underlyingtype to get EntityUnderlyingtype
    df = (
        cost_pct_base.alias("C")
        .join(
            cfg["_enu_underlying_type"].alias("U"),
            F.col("C.Underlyingtype") == F.col("U.UnderlyingTypeId"),
        )
        .select(
            F.col("C.*"),
            F.col("U.Underlyingtype").alias("EntityUnderlyingtype"),
        )
    )

    log_timing(logger, "build_cost_percentage_snapshot", t0)
    return df


# ---------------------------------------------------------------------------
# Function 6: build_cost_underlying_types
# SQL lines: 427-433 (S7 second half)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_cost_underlying_types(
    spark: SparkSession, cfg: dict, cost_pct_snapshot: DataFrame,
) -> DataFrame:
    """Extract distinct cost underlying types.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 427-433.
    Row count: ALWAYS-NON-EMPTY.
    """
    log_section(logger, "build_cost_underlying_types")
    t0 = time.time()

    # L427-431: SELECT DISTINCT ... FROM #CostPercentage_Snapshot
    # WHERE EntityUnderlyingtype <> 'K-1 ONLY' OR (= 'K-1 ONLY' AND InvestmentID = -1)
    df = (
        cost_pct_snapshot
        .filter(
            (F.upper(F.col("EntityUnderlyingtype")) != "K-1 ONLY")
            | (
                (F.upper(F.col("EntityUnderlyingtype")) == "K-1 ONLY")
                & (F.col("InvestmentID") == -1)
            )
        )
        .select(
            "EntityId", "InvestmentID", "Quarter",
            "AllocationTypeId", "TrackingKey",
            "Underlyingtype", "EntityUnderlyingtype",
        )
        .distinct()
    )


    log_timing(logger, "build_cost_underlying_types", t0)
    return df


# ---------------------------------------------------------------------------
# Function 7: build_entity_asset_class_relationship
# SQL lines: 435-437 (S7 last part)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_entity_asset_class_relationship(
    spark: SparkSession, cfg: dict,
) -> DataFrame:
    """Call udfGetAssetClassRelationship UDF class.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 435-437.
    Row count: POSSIBLY-EMPTY (depends on override configuration).

    The SQL calls udfGetAssetClassRelationship(@LocalClientID, @LocalTaxPeriodID, @LocalEntityID).
    We invoke the existing PySpark UDF class from Common_V2/domain/.
    """
    log_section(logger, "build_entity_asset_class_relationship")
    t0 = time.time()

    from Common_V2.domain.udf_asset_class_relationship import udfGetAssetClassRelationship as _AcrUdf

    entity_id = cfg["entity_id"]
    udf_instance = _AcrUdf(
        spark=spark,
        cfg=cfg,
        entity_ids=str(entity_id),
    )
    df = udf_instance.execute()

    log_timing(logger, "build_entity_asset_class_relationship", t0)
    return df


# ---------------------------------------------------------------------------
# Function 8: build_entity_hierarchy
# SQL lines: 450-500 (S8 - recursive CTE)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_entity_hierarchy(
    spark: SparkSession, cfg: dict,
    cost_underlying_types: DataFrame,
) -> DataFrame:
    """Build entity hierarchy using iterative approach (replaces recursive CTE).

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 450-500.
    Row count: ALWAYS-NON-EMPTY.

    The SQL uses a recursive CTE on EntityRelationship to traverse the entity
    hierarchy from cost underlying types down to lower-tier entities.
    PySpark doesn't support recursive CTEs natively, so we use an iterative
    approach with a bounded loop.
    """
    log_section(logger, "build_entity_hierarchy")
    t0 = time.time()

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]

    er = F.broadcast(
        _tbl(spark, "EntityRelationship", cfg)
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select("LowerTierEntityID", "UpperTierEntityID")
    )

    tc = F.broadcast(cost_underlying_types)

    # L460-473: Base case of recursive CTE
    # Anchor: JOIN #TempCostUnderlyingTypes TC with EntityRelationship ER
    # ON ER.UpperTierEntityID = CASE WHEN EntityUnderlyingtype='Asset Class' THEN TC.EntityId ELSE TC.InvestmentID END
    base = (
        tc.alias("TC")
        .join(
            er.alias("ER"),
            F.col("ER.UpperTierEntityID") == F.when(
                F.upper(F.col("TC.EntityUnderlyingtype")) == "ASSET CLASS",
                F.col("TC.EntityId"),
            ).otherwise(F.col("TC.InvestmentID")),
        )
        .select(
            F.col("ER.LowerTierEntityID"),
            F.col("ER.UpperTierEntityID").alias("ParentEntityID"),
            F.col("ER.UpperTierEntityID").alias("CurrentEntityId"),
            F.lit(2).alias("HLevel"),
            F.col("TC.AllocationTypeId"),
            # TrackingKey logic: Asset Class → '~' + EntityId + '~', else standard
            F.concat(
                F.lit("~"),
                F.when(
                    F.upper(F.col("TC.EntityUnderlyingtype")) == "ASSET CLASS",
                    F.col("ER.LowerTierEntityID").cast("string"),
                ).otherwise(
                    F.when(
                        _ns(F.col("TC.TrackingKey")) == "",
                        F.col("TC.InvestmentID").cast("string"),
                    ).otherwise(F.col("TC.TrackingKey"))
                ),
                F.lit("~"),
            ).alias("TrackingKey"),
            F.col("TC.InvestmentID").alias("AssetClassId"),
            F.col("ER.LowerTierEntityID").alias("ImmediateLowerTierEntityID"),
        )
    )

    # Iterative expansion (replaces UNION ALL recursive part).
    # Per-level Delta checkpoint is REQUIRED: without it, current_level stays
    # lazy and each `.first()` re-derives all prior levels (geometric blowup).
    current_level = base
    all_levels = [base]
    level = 3  # base is HLevel=2, next starts at 3

    while True:
        next_level = (
            er.alias("ER2")
            .join(
                current_level.alias("EH"),
                F.col("ER2.UpperTierEntityID") == F.col("EH.LowerTierEntityID"),
            )
            .select(
                F.col("ER2.LowerTierEntityID"),
                F.col("ER2.UpperTierEntityID").alias("ParentEntityID"),
                F.col("EH.CurrentEntityId"),
                F.lit(level).alias("HLevel"),
                F.col("EH.AllocationTypeId"),
                F.col("EH.TrackingKey"),
                F.col("EH.AssetClassId"),
                F.col("EH.ImmediateLowerTierEntityID"),
            )
        )

        # Materialize this level so `.first()` is O(1) and downstream iters
        # don't recompute the full chain.
        next_level = checkpoint(spark, next_level, f"hier_level_{level}", cfg)

        if next_level.first() is None:
            break

        all_levels.append(next_level)
        current_level = next_level
        level += 1

    # Union all levels (each already materialized)
    hierarchy = all_levels[0]
    for level_df in all_levels[1:]:
        hierarchy = hierarchy.unionByName(level_df)

    # Final checkpoint breaks lineage for downstream consumers
    #hierarchy = checkpoint(spark, hierarchy, "entity_hier_final", cfg)

    log_timing(logger, "build_entity_hierarchy", t0)
    return hierarchy


# ---------------------------------------------------------------------------
# Function 9: build_all_underlyings_combined
# SQL lines: 480-530 (S8 result — 5 UNIONs)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_all_underlyings_combined(
    spark: SparkSession, cfg: dict,
    cost_underlying_types: DataFrame,
    cost_pct_snapshot: DataFrame,
    entity_hierarchy: DataFrame,
) -> DataFrame:
    """Combine entity hierarchy with K-1 Only, Asset Class, Entity Total underlyings.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 480-530.
    Row count: ALWAYS-NON-EMPTY.
    """
    log_section(logger, "build_all_underlyings_combined")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    tc = cost_underlying_types
    cps = cost_pct_snapshot
    # entity_hierarchy is None only when tc was empty — avoid repeat isEmpty()
    tc_has_rows = entity_hierarchy is not None

    parts = []

    # Part 1: From entity hierarchy CTE JOIN cost underlying types (L485-495)
    # Only if entity_hierarchy is not None (i.e., cost_underlying_types had rows)
    if tc_has_rows:
        part1 = (
            tc.alias("TC")
            .join(
                entity_hierarchy.alias("EH"),
                (F.col("EH.CurrentEntityId") == F.when(
                    F.upper(F.col("TC.EntityUnderlyingtype")) == "ASSET CLASS",
                    F.col("TC.EntityId"),
                ).otherwise(F.col("TC.InvestmentID")))
                & (F.col("TC.AllocationTypeId") == F.col("EH.AllocationTypeId"))
                & (F.col("TC.InvestmentID") == F.col("EH.AssetClassId")),
            )
            .select(
                F.col("EH.LowerTierEntityID").alias("UnderlyingEntityId"),
                F.col("EH.CurrentEntityId").alias("EntityId"),
                F.col("EH.HLevel"),
                F.col("TC.Underlyingtype"),
                F.col("TC.AllocationTypeId"),
                F.col("EH.TrackingKey"),
                F.col("EH.AssetClassId"),
                F.col("EH.ImmediateLowerTierEntityID"),
            )
            .distinct()
        )
        parts.append(part1)

    # Part 2: K-1 ONLY with InvestmentID != -1 (L497-500)
    part2 = (
        cps
        .filter(F.upper(F.col("EntityUnderlyingtype")) == "K-1 ONLY")
        .select(
            F.col("InvestmentID").alias("UnderlyingEntityId"),
            F.col("InvestmentID").alias("EntityId"),
            F.lit(1).alias("HLevel"),
            F.col("Underlyingtype"),
            F.col("AllocationTypeId"),
            F.when(
                _ns(F.col("TrackingKey")) == "",
                F.concat(F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")),
            ).otherwise(F.col("TrackingKey")).alias("TrackingKey"),
            F.col("InvestmentID").alias("AssetClassId"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
        )
    )
    parts.append(part2)

    # Part 3: K-1 ONLY with InvestmentID = -1 → use @LocalEntityID (L502-505)
    part3 = (
        cps
        .filter(
            (F.upper(F.col("EntityUnderlyingtype")) == "K-1 ONLY")
            & (F.col("InvestmentID") == -1)
        )
        .select(
            F.lit(entity_id).cast("int").alias("UnderlyingEntityId"),
            F.lit(entity_id).cast("int").alias("EntityId"),
            F.lit(1).alias("HLevel"),
            F.col("Underlyingtype"),
            F.col("AllocationTypeId"),
            F.concat(F.lit("~"), F.lit(entity_id).cast("string"), F.lit("~")).alias("TrackingKey"),
            F.lit(entity_id).cast("int").alias("AssetClassId"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
        )
    )
    parts.append(part3)

    # Part 4: Asset Class (L510-514)
    if tc_has_rows:
        part4 = (
            tc
            .filter(F.upper(F.col("EntityUnderlyingtype")) == "ASSET CLASS")
            .select(
                F.col("EntityId").alias("UnderlyingEntityId"),
                F.col("InvestmentID").alias("EntityId"),
                F.lit(1).alias("HLevel"),
                F.col("Underlyingtype"),
                F.col("AllocationTypeId"),
                F.concat(F.lit("~"), F.col("EntityId").cast("string"), F.lit("~")).alias("TrackingKey"),
                F.col("InvestmentID").alias("AssetClassId"),
                F.col("EntityId").alias("ImmediateLowerTierEntityID"),
            )
        )
        parts.append(part4)

        # Part 5: Entity Total (L516-520)
        part5 = (
            tc
            .filter(F.upper(F.col("EntityUnderlyingtype")) == "ENTITY TOTAL")
            .select(
                F.col("InvestmentID").alias("UnderlyingEntityId"),
                F.col("InvestmentID").alias("EntityId"),
                F.lit(1).alias("HLevel"),
                F.col("Underlyingtype"),
                F.col("AllocationTypeId"),
                F.when(
                    _ns(F.col("TrackingKey")) == "",
                    F.concat(F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")),
                ).otherwise(F.col("TrackingKey")).alias("TrackingKey"),
                F.col("InvestmentID").alias("AssetClassId"),
                F.lit(0).alias("ImmediateLowerTierEntityID"),
            )
        )
        parts.append(part5)

    # SQL uses UNION (not UNION ALL) between the 5 parts, which eliminates duplicates
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    df = df.distinct()


    log_timing(logger, "build_all_underlyings_combined", t0)
    return df


# ---------------------------------------------------------------------------
# Function 10: apply_asset_class_filter
# SQL lines: 530-575 (S9)
# ---------------------------------------------------------------------------
def apply_asset_class_filter(
    spark: SparkSession, cfg: dict,
    all_underlyings: DataFrame,
    entity_asset_class_rel: DataFrame,
) -> DataFrame:
    """Apply asset class filtering based on configuration flags.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 530-575.
    Three conditional DELETE blocks on #TempAllUnderlyingsCombined.
    """
    log_section(logger, "apply_asset_class_filter")
    t0 = time.time()

    override_indirect_ac = cfg.get("override_indirect_ac")
    ignore_ac_partnership = cfg.get("ignore_ac_partnership")
    entity_id = cfg["entity_id"]

    ear = entity_asset_class_rel
    entity = cfg["_entity_lookup"]
    enu_ut = cfg["_enu_underlying_type"].select("UnderlyingTypeId", "Underlyingtype")

    if override_indirect_ac is not None and override_indirect_ac != "C":
        # L536-543: DELETE where Asset Class doesn't match (non-C path)
        # Join on UnderlyingEntityID
        to_delete = (
            all_underlyings.alias("AI")
            .join(
                ear.alias("EAR"),
                (F.col("AI.UnderlyingEntityId") == F.col("EAR.LowerTierEntityID"))
                & (
                    F.when(F.col("EAR.TrackingKey").isNull(), F.lit(""))
                    .otherwise(F.col("EAR.TrackingKey"))
                    ==
                    F.when(F.col("EAR.TrackingKey").isNull(), F.lit(""))
                    .otherwise(F.col("AI.TrackingKey"))
                ),
                "left",
            )
            .join(entity.alias("E"), F.col("E.EntityID") == F.col("AI.UnderlyingEntityId"))
            .join(enu_ut.alias("U"), F.col("AI.Underlyingtype") == F.col("U.UnderlyingTypeId"))
            .filter(
                (F.upper(F.col("U.Underlyingtype")) == "ASSET CLASS")
                & (
                    F.when(
                        _ns0(F.col("EAR.AssetClassID")) == 0,
                        F.col("E.AssetClassID"),
                    ).otherwise(F.col("EAR.AssetClassID"))
                    != F.col("AI.AssetClassId")
                )
            )
            .select(
                F.col("AI.UnderlyingEntityId"),
                F.col("AI.EntityId"),
                F.col("AI.AllocationTypeId"),
                F.col("AI.TrackingKey"),
                F.col("AI.AssetClassId"),
            )
        )
        # Anti-join to remove matched rows
        all_underlyings = (
            all_underlyings.alias("AI2")
            .join(
                to_delete.alias("DEL"),
                (F.col("AI2.UnderlyingEntityId") == F.col("DEL.UnderlyingEntityId"))
                & (F.col("AI2.EntityId") == F.col("DEL.EntityId"))
                & (F.col("AI2.AllocationTypeId") == F.col("DEL.AllocationTypeId"))
                & (F.col("AI2.TrackingKey") == F.col("DEL.TrackingKey"))
                & (F.col("AI2.AssetClassId") == F.col("DEL.AssetClassId")),
                "left_anti",
            )
        )
    else:
        # L548-555: DELETE where Asset Class doesn't match (C path)
        # Join on ImmediateLowerTierEntityID instead
        to_delete = (
            all_underlyings.alias("AI")
            .join(
                ear.alias("EAR"),
                F.col("AI.ImmediateLowerTierEntityID") == F.col("EAR.LowerTierEntityID"),
                "left",
            )
            .join(entity.alias("E"), F.col("E.EntityID") == F.col("AI.ImmediateLowerTierEntityID"))
            .join(enu_ut.alias("U"), F.col("AI.Underlyingtype") == F.col("U.UnderlyingTypeId"))
            .filter(
                (F.upper(F.col("U.Underlyingtype")) == "ASSET CLASS")
                & (
                    F.when(
                        _ns0(F.col("EAR.AssetClassID")) == 0,
                        F.col("E.AssetClassID"),
                    ).otherwise(F.col("EAR.AssetClassID"))
                    != F.col("AI.AssetClassId")
                )
            )
            .select(
                F.col("AI.UnderlyingEntityId"),
                F.col("AI.EntityId"),
                F.col("AI.AllocationTypeId"),
                F.col("AI.TrackingKey"),
                F.col("AI.AssetClassId"),
            )
        )
        all_underlyings = (
            all_underlyings.alias("AI2")
            .join(
                to_delete.alias("DEL"),
                (F.col("AI2.UnderlyingEntityId") == F.col("DEL.UnderlyingEntityId"))
                & (F.col("AI2.EntityId") == F.col("DEL.EntityId"))
                & (F.col("AI2.AllocationTypeId") == F.col("DEL.AllocationTypeId"))
                & (F.col("AI2.TrackingKey") == F.col("DEL.TrackingKey"))
                & (F.col("AI2.AssetClassId") == F.col("DEL.AssetClassId")),
                "left_anti",
            )
        )

    # L558-563: IgnoreAssetclassForPartnershipLevel
    if ignore_ac_partnership == "C":
        all_underlyings = (
            all_underlyings.alias("AI3")
            .join(
                enu_ut.alias("U2"),
                F.col("AI3.Underlyingtype") == F.col("U2.UnderlyingTypeId"),
            )
            .filter(
                ~(
                    (F.col("AI3.UnderlyingEntityId") == entity_id)
                    & (F.upper(F.col("U2.Underlyingtype")) == "ASSET CLASS")
                )
            )
            .select("AI3.*")
        )

    log_timing(logger, "apply_asset_class_filter", t0)
    return all_underlyings


# ---------------------------------------------------------------------------
# Function 11: build_states_dar_rule_mapping
# SQL lines: 575-620 (S10 first half)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_states_dar_rule_mapping(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load state-level default allocation rule mapping.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 575-620.
    Row count: ALWAYS-NON-EMPTY.
    """
    log_section(logger, "build_states_dar_rule_mapping")
    t0 = time.time()

    k1_line_type_id = cfg["k1_line_type_id"]
    dar_txn_id = cfg["dar_txn_id"]
    global_dar_txn_id = cfg["global_dar_txn_id"]

    # L577-582: #StatesMapDefaultAllocRuleToLineItem
    df = (
        _tbl(spark, "MapDefaultAllocRuleToLineItem", cfg).alias("M")
        .join(
            _tbl(spark, "ENU_LineType", cfg).alias("EL"),
            F.col("EL.LineTypeID") == F.col("M.SourceID"),
        )
        .filter(
            F.col("M.TransactionID").isin(dar_txn_id, global_dar_txn_id)
            & (F.col("EL.LineType") == "State Input")
        )
        .select(
            F.lit(k1_line_type_id).alias("SourceID"),
            F.col("M.StateID"),
            F.col("M.SelectedMappingID"),
            F.col("M.RuleID"),
            F.col("M.ExcludeFromTransfers"),
        )
    )


    log_timing(logger, "build_states_dar_rule_mapping", t0)
    return df


# ---------------------------------------------------------------------------
# Function 12: build_all_underlyings_states
# SQL lines: 593-640 (S10 second half)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_all_underlyings_states(
    spark: SparkSession, cfg: dict,
    all_underlyings: DataFrame,
    input_data_load: DataFrame,
    states_dar_mapping: DataFrame,
) -> DataFrame:
    """Build ranked underlyings-states with ROW_NUMBER for rule pickup.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 593-640.
    Row count: ALWAYS-NON-EMPTY.
    """
    log_section(logger, "build_all_underlyings_states")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    override_indirect_ac = cfg.get("override_indirect_ac")
    dar_txn_id = cfg["dar_txn_id"]
    global_dar_txn_id = cfg["global_dar_txn_id"]

    enu_ut = cfg["_enu_underlying_type"].select("UnderlyingTypeID", "UnderlyingType", "DisplayOrder")
    enu_rt = F.broadcast(_tbl(spark, "ENU_RuleType", cfg).select("RuleTypeID", "DisplayOrder"))
    enu_ab = cfg["_enu_allocation_by"].select("AllocationByID", "AllocationBy", "DisplayOrder")
    dar_setup = cfg["_dar_setup"]

    # L593-630: Complex multi-join with ROW_NUMBER ranking
    # The CASE WHEN in the ON clause handles the tracking key matching logic
    ai_with_ut = (
        all_underlyings.alias("AI")
        .join(
            enu_ut.alias("U"),
            F.col("AI.Underlyingtype") == F.col("U.UnderlyingTypeID"),
        )
        .select("AI.*", F.col("U.UnderlyingType").alias("UnderlyingTypeName"), F.col("U.DisplayOrder").alias("U_DisplayOrder"))
    )

    # Build the sentinel condition: when certain conditions are met, use '-1' == '-1' (always true),
    # otherwise use LIKE pattern matching: '~' + L.TrackingKey + '~' LIKE '%' + AI.TrackingKey + '%'
    sentinel_cond = (
        (F.col("AI.UnderlyingEntityId") == entity_id)
        | ((F.col("AI.EntityId") == entity_id) & (F.upper(F.col("AI.UnderlyingTypeName")) != "ASSET CLASS"))
        | ((F.lit(override_indirect_ac != "C")) & (F.upper(F.col("AI.UnderlyingTypeName")) == "ASSET CLASS"))
    )

    tracking_key_match = (
        sentinel_cond
        | F.concat(F.lit("~"), F.col("L.TrackingKey"), F.lit("~"))
        .like(F.concat(F.lit("%"), F.col("AI.TrackingKey"), F.lit("%")))
    )

    ranked = (
        F.broadcast(ai_with_ut).alias("AI")
        .join(
            input_data_load.alias("L"),
            F.col("L.EntityID") == F.col("AI.UnderlyingEntityId"),
        )
        .filter(tracking_key_match)
        .join(
            F.broadcast(states_dar_mapping).alias("M"),
            (
                F.when(F.col("M.StateID") == -1, F.lit(1))
                .otherwise(F.col("M.StateID"))
                ==
                F.when(F.col("M.StateID") == -1, F.lit(1))
                .otherwise(F.col("L.StateID"))
            )
            & (
                F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("L.StateLineID"))
                ==
                F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("M.SelectedMappingID"))
            )
            & (F.col("M.RuleID") == F.col("AI.AllocationTypeId"))
            & (F.col("M.SourceID") == F.col("L.LineTypeID")),
        )
        .join(
            dar_setup.alias("D"),
            (F.col("D.RuleID") == F.col("AI.AllocationTypeId"))
            & (F.col("AI.Underlyingtype") == F.col("D.UnderlyingTypeID")),
        )
        .join(
            enu_rt.alias("R"),
            F.col("D.RuleTypeID") == F.col("R.RuleTypeID"),
        )
        .join(
            enu_ab.alias("EA"),
            F.col("D.AllocationByID") == F.col("EA.AllocationByID"),
        )
    )

    # ROW_NUMBER for ranking (returns INT, not BIT — == 1 is correct)
    w = Window.partitionBy(
        "UnderlyingEntityId",
        "TrackingKey_L",
        "StateLineID",
        "EA_DisplayOrder",
    ).orderBy(
        F.col("HLevel").asc(),
        F.col("R_DisplayOrder").desc(),
        F.col("U_DisplayOrder").asc(),
        F.col("SelectedMappingID").desc(),
        F.col("EA_DisplayOrder").asc(),
    )

    # Flatten column names before windowing to avoid ambiguity
    ranked_flat = ranked.select(
        F.col("AI.Underlyingtype").alias("Underlyingtype"),
        F.col("AI.UnderlyingEntityId").alias("UnderlyingEntityId"),
        F.col("AI.EntityId").alias("EntityId"),
        F.col("L.TrackingKey").alias("TrackingKey_L"),
        F.col("AI.TrackingKey").alias("TrackingMatch"),
        F.col("AI.AllocationTypeId").alias("AllocationTypeId"),
        F.col("L.StateLineID").alias("StateLineID"),
        F.col("M.ExcludeFromTransfers").alias("ExcludeFromTransfers"),
        F.col("EA.AllocationBy").alias("AllocationBy"),
        F.col("L.StateID").alias("StateID"),
        F.col("AI.HLevel").alias("HLevel"),
        F.col("R.DisplayOrder").alias("R_DisplayOrder"),
        F.col("AI.U_DisplayOrder").alias("U_DisplayOrder"),
        F.col("M.SelectedMappingID").alias("SelectedMappingID"),
        F.col("EA.DisplayOrder").alias("EA_DisplayOrder"),
    )

    ranked_df = (
        ranked_flat
        .withColumn("RankForUnderlyingPickup", F.row_number().over(w))
        .filter(F.col("RankForUnderlyingPickup") == 1)  # ROW_NUMBER() returns INT, not BIT
        .select(
            F.col("Underlyingtype"),
            F.col("UnderlyingEntityId"),
            F.col("EntityId"),
            F.col("TrackingKey_L").alias("TrackingKey"),
            F.col("TrackingMatch"),
            F.col("AllocationTypeId"),
            F.col("StateLineID").alias("LineID"),
            F.col("ExcludeFromTransfers"),
            F.col("RankForUnderlyingPickup"),
            F.col("AllocationBy"),
            F.col("StateID"),
        )
    )

    log_timing(logger, "build_all_underlyings_states", t0)
    return ranked_df


# ============================================================================
# CHUNK 2: 4-Pass Build + Allocation (S11-S19)
# ============================================================================

# ---------------------------------------------------------------------------
# Sentinel join helper (used by all 4 passes)
# ---------------------------------------------------------------------------
def _sentinel(col):
    """CASE WHEN ISNULL(col,'') = '' THEN '-1' ELSE col END — sentinel join pattern."""
    return F.when(_ns(col) == "", F.lit("-1")).otherwise(col)


def _alloc_input_columns(l_alias: str, b_alias: str, cfg: dict,
                         ai_alias: str = None) -> list:
    """Standard column selection for #tmpLookThroughAllocationInput.

    Used by all 4 passes with slightly different TypeID / CustomTrackingkey /
    CustomTag / IsExcludefromTransfer logic.
    """
    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    L = l_alias
    B = b_alias

    base_cols = [
        F.col(f"{L}.RunID"),
        F.col(f"{L}.ClientID"),
        F.col(f"{L}.EntityID"),
        F.col(f"{L}.LineTypeID"),
        F.col(f"{L}.StateLineID"),
        F.col(f"{L}.StateID"),
        F.col(f"{L}.Amount"),
        F.col(f"{L}.QuicklinkID"),
        F.col(f"{L}.Amount704b"),
        F.col(f"{L}.CategoryID"),
        F.col(f"{L}.ParentEntityID"),
        F.col(f"{L}.PeriodID"),
        F.col(f"{L}.LineCode"),
        F.col(f"{L}.SuperParentEntityID"),
        F.col(f"{L}.AdjustmentTypeID"),
        F.col(f"{L}.TrackingKey"),
        _ns(F.col(f"{L}.Tag")).alias("Tag"),
    ]

    if ai_alias is None:
        # Passes 1-3: simple defaults
        base_cols.extend([
            _ns(F.col(f"{B}.TrackingKey")).alias("CustomTrackingkey"),
            F.coalesce(F.col(f"{B}.Tag"), _ns(F.col(f"{L}.Tag"))).alias("CustomTag"),
            F.coalesce(
                F.col(f"{B}.AdjustmentAllocationTypeID"),
                F.lit(cost_alloc_type_id),
            ).alias("TypeID"),
            F.col(f"{L}.OriginalParentEntityID"),
            F.lit(False).alias("IsExcludefromTransfer"),
        ])
    else:
        # Pass 4: fallback with LEFT JOINs
        # SQL: ISNULL(B.Trackingkey, ISNULL(L.Trackingkey,''))
        # When B is NULL (LEFT JOIN miss), fall through to L.TrackingKey
        AI = ai_alias
        base_cols.extend([
            F.coalesce(
                F.col(f"{B}.TrackingKey"),
                _ns(F.col(f"{L}.TrackingKey")),
            ).alias("CustomTrackingkey"),
            F.coalesce(F.col(f"{B}.Tag"), _ns(F.col(f"{L}.Tag"))).alias("CustomTag"),
            F.coalesce(
                F.col(f"{B}.AdjustmentAllocationTypeID"),
                F.col(f"{AI}.AllocationTypeId"),
                F.lit(cost_alloc_type_id),
            ).alias("TypeID"),
            F.col(f"{L}.OriginalParentEntityID"),
            F.coalesce(F.col(f"{AI}.ExcludeFromTransfers").cast("boolean"), F.lit(False)).alias("IsExcludefromTransfer"),
        ])

    return base_cols


def _book_effective_join_cond(b_alias: str, l_alias: str) -> F.Column:
    """Shared TrackingKey + Tag wildcard join condition for book effective.

    SQL pattern: CASE WHEN ISNULL(B.TrackingKey,'')='' THEN '-1' ELSE B.TrackingKey END
              = CASE WHEN ISNULL(B.TrackingKey,'')='' THEN '-1' ELSE L.TrackingKey END
    Both sides check B — so when B is NULL/empty, it's a WILDCARD (always matches).
    When B has a value, it must equal L's value.
    """
    B, L = b_alias, l_alias
    tk_match = (
        (_ns(F.col(f"{B}.TrackingKey")) == "")
        | (F.col(f"{B}.TrackingKey") == F.col(f"{L}.TrackingKey"))
    )
    tag_match = (
        (_ns(F.col(f"{B}.Tag")) == "")
        | (F.col(f"{B}.Tag") == F.col(f"{L}.Tag"))
    )
    return tk_match & tag_match


# ---------------------------------------------------------------------------
# Function 13: build_allocation_input_pass1
# SQL lines: 640-740 (S11) — Both StateLineID + StateID match
# Row count: ALWAYS-NON-EMPTY (combined across all passes)
# ---------------------------------------------------------------------------
def build_allocation_input_pass1(
    spark: SparkSession, cfg: dict,
    input_data_load: DataFrame,
    book_effective: DataFrame,
    cost_pct_base: DataFrame,
) -> tuple:
    """Pass 1: Match on both StateLineID AND StateID.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 640-740.
    Returns (alloc_input, remaining_input_data, remaining_book_effective).
    """
    log_section(logger, "build_allocation_input_pass1")
    t0 = time.time()

    sm_state_lines = cfg["_sm_state_lines"]

    # Filter book effective for this pass: StateLineID <> -1 AND StateID <> -1
    be_pass1 = book_effective.filter(
        (F.coalesce(F.col("StateLineID"), F.lit(-1)) != -1)
        & (F.coalesce(F.col("StateID"), F.lit(-1)) != -1)
    )

    # INSERT: Join input with StateLines and BookEffective on StateLineID + StateID
    alloc_input = (
        input_data_load.alias("L")
        .join(
            sm_state_lines.alias("K"),
            (F.col("K.StateFieldID") == F.col("L.StateLineID"))
            & (F.col("L.StateID") == F.col("K.StateID")),
        )
        .join(
            be_pass1.alias("B"),
            (F.col("B.UnderlyingEntityID") == F.col("L.EntityID"))
            & (F.col("B.StateLineID") == F.col("L.StateLineID"))
            & (F.col("B.StateID") == F.col("L.StateID"))
            & _book_effective_join_cond("B", "L"),
        )
        .select(*_alloc_input_columns("L", "B", cfg))
    )

    # DELETE from input_data_load: rows that matched (same join + cost pct check)
    # Only delete if cost percentage has non-zero CommitmentPercent
    matched_for_delete = (
        input_data_load.alias("L")
        .join(
            sm_state_lines.alias("K"),
            (F.col("K.StateFieldID") == F.col("L.StateLineID"))
            & (F.col("L.StateID") == F.col("K.StateID")),
        )
        .join(
            be_pass1.alias("B"),
            (F.col("B.UnderlyingEntityID") == F.col("L.EntityID"))
            & (F.col("B.StateLineID") == F.col("L.StateLineID"))
            & (F.col("B.StateID") == F.col("L.StateID"))
            & _book_effective_join_cond("B", "L"),
        )
        .join(
            cost_pct_base.alias("C"),
            (F.col("C.InvestmentID") == F.col("B.UnderlyingEntityID"))
            & (F.col("C.AllocationTypeId") == F.coalesce(
                F.col("B.AdjustmentAllocationTypeID"), F.col("B.AllocationTypeid")))
            & (
                (_ns(F.col("B.TrackingKey")) == "")
                | (F.col("B.TrackingKey") == F.col("C.TrackingKey"))
            )
            & (
                (_ns(F.col("B.Tag")) == "")
                | (F.col("B.Tag") == F.col("C.Tag"))
            ),
        )
        .filter(_ns0(F.col("C.CommitmentPercent")) != 0)
        .select(F.col("L.RunID"), F.col("L.EntityID"), F.col("L.StateLineID"),
                F.col("L.StateID"), F.col("L.TrackingKey"), F.col("L.Tag"))
    )

    remaining_input = input_data_load.alias("L2").join(
        matched_for_delete.alias("DEL"),
        (F.col("L2.EntityID") == F.col("DEL.EntityID"))
        & (F.col("L2.StateLineID") == F.col("DEL.StateLineID"))
        & (F.col("L2.StateID") == F.col("DEL.StateID"))
        & (F.col("L2.TrackingKey") == F.col("DEL.TrackingKey"))
        & (_ns(F.col("L2.Tag")) == _ns(F.col("DEL.Tag"))),
        "left_anti",
    )

    # DELETE from book_effective: remove pass 1 rows
    remaining_be = book_effective.filter(
        ~(
            (F.coalesce(F.col("StateLineID"), F.lit(-1)) != -1)
            & (F.coalesce(F.col("StateID"), F.lit(-1)) != -1)
        )
    )

    log_timing(logger, "build_allocation_input_pass1", t0)
    return alloc_input, remaining_input, remaining_be


# ---------------------------------------------------------------------------
# Function 14: build_allocation_input_pass2
# SQL lines: 740-820 (S12) — StateID matches, StateLineID = -1 (wildcard line)
# ---------------------------------------------------------------------------
def build_allocation_input_pass2(
    spark: SparkSession, cfg: dict,
    input_data_load: DataFrame,
    book_effective: DataFrame,
    alloc_input_so_far: DataFrame,
) -> tuple:
    """Pass 2: Match on StateID only (StateLineID = -1 in book effective).

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 740-820.
    Returns (updated_alloc_input, remaining_input_data, remaining_book_effective).
    """
    log_section(logger, "build_allocation_input_pass2")
    t0 = time.time()

    sm_state_lines = cfg["_sm_state_lines"]

    # Filter book effective: StateLineID = -1 AND StateID <> -1
    be_pass2 = book_effective.filter(
        (F.coalesce(F.col("StateLineID"), F.lit(-1)) == -1)
        & (F.coalesce(F.col("StateID"), F.lit(-1)) != -1)
    )

    new_rows = (
        input_data_load.alias("L")
        .join(
            sm_state_lines.alias("K"),
            (F.col("K.StateFieldID") == F.col("L.StateLineID"))
            & (F.col("K.StateID") == F.col("L.StateID")),
        )
        .join(
            be_pass2.alias("B"),
            (F.col("B.UnderlyingEntityID") == F.col("L.EntityID"))
            & (F.col("B.StateID") == F.col("L.StateID"))
            & _book_effective_join_cond("B", "L"),
        )
        .select(*_alloc_input_columns("L", "B", cfg))
    )

    alloc_input = alloc_input_so_far.unionByName(new_rows)

    # DELETE matched rows from input_data_load
    matched_keys = new_rows.select("RunID", "EntityID", "StateLineID", "StateID", "TrackingKey", "Tag")
    remaining_input = input_data_load.alias("L2").join(
        matched_keys.alias("DEL"),
        (F.col("L2.EntityID") == F.col("DEL.EntityID"))
        & (F.col("L2.StateLineID") == F.col("DEL.StateLineID"))
        & (F.col("L2.StateID") == F.col("DEL.StateID"))
        & (F.col("L2.TrackingKey") == F.col("DEL.TrackingKey"))
        & (_ns(F.col("L2.Tag")) == _ns(F.col("DEL.Tag"))),
        "left_anti",
    )

    # DELETE book effective rows for this pass
    remaining_be = book_effective.filter(
        ~(
            (F.coalesce(F.col("StateLineID"), F.lit(-1)) == -1)
            & (F.coalesce(F.col("StateID"), F.lit(-1)) != -1)
        )
    )

    log_timing(logger, "build_allocation_input_pass2", t0)
    return alloc_input, remaining_input, remaining_be


# ---------------------------------------------------------------------------
# Function 15: build_allocation_input_pass3
# SQL lines: 820-900 (S13) — StateLineID matches, StateID = -1 (wildcard state)
# ---------------------------------------------------------------------------
def build_allocation_input_pass3(
    spark: SparkSession, cfg: dict,
    input_data_load: DataFrame,
    book_effective: DataFrame,
    alloc_input_so_far: DataFrame,
) -> tuple:
    """Pass 3: Match on StateLineID only (StateID = -1 in book effective).

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 820-900.
    Returns (updated_alloc_input, remaining_input_data, remaining_book_effective).
    """
    log_section(logger, "build_allocation_input_pass3")
    t0 = time.time()

    sm_state_lines = cfg["_sm_state_lines"]

    # Filter book effective: StateLineID <> -1 AND StateID = -1
    be_pass3 = book_effective.filter(
        (F.coalesce(F.col("StateLineID"), F.lit(-1)) != -1)
        & (F.coalesce(F.col("StateID"), F.lit(-1)) == -1)
    )

    new_rows = (
        input_data_load.alias("L")
        .join(
            sm_state_lines.alias("K"),
            (F.col("K.StateFieldID") == F.col("L.StateLineID"))
            & (F.col("K.StateID") == F.col("L.StateID")),
        )
        .join(
            be_pass3.alias("B"),
            (F.col("B.UnderlyingEntityID") == F.col("L.EntityID"))
            & (F.col("B.StateLineID") == F.col("L.StateLineID"))
            & _book_effective_join_cond("B", "L"),
        )
        .select(*_alloc_input_columns("L", "B", cfg))
    )

    alloc_input = alloc_input_so_far.unionByName(new_rows)

    # DELETE matched rows from input_data_load
    matched_keys = new_rows.select("RunID", "EntityID", "StateLineID", "StateID", "TrackingKey", "Tag")
    remaining_input = input_data_load.alias("L2").join(
        matched_keys.alias("DEL"),
        (F.col("L2.EntityID") == F.col("DEL.EntityID"))
        & (F.col("L2.StateLineID") == F.col("DEL.StateLineID"))
        & (F.col("L2.StateID") == F.col("DEL.StateID"))
        & (F.col("L2.TrackingKey") == F.col("DEL.TrackingKey"))
        & (_ns(F.col("L2.Tag")) == _ns(F.col("DEL.Tag"))),
        "left_anti",
    )

    # DELETE book effective rows for this pass
    remaining_be = book_effective.filter(
        ~(
            (F.coalesce(F.col("StateLineID"), F.lit(-1)) != -1)
            & (F.coalesce(F.col("StateID"), F.lit(-1)) == -1)
        )
    )

    log_timing(logger, "build_allocation_input_pass3", t0)
    return alloc_input, remaining_input, remaining_be


# ---------------------------------------------------------------------------
# Function 16: build_allocation_input_pass4
# SQL lines: 900-960 (S14) — Fallback: LEFT JOIN remaining
# ---------------------------------------------------------------------------
def build_allocation_input_pass4(
    spark: SparkSession, cfg: dict,
    input_data_load: DataFrame,
    book_effective: DataFrame,
    all_underlyings_states: DataFrame,
    alloc_input_so_far: DataFrame,
) -> DataFrame:
    """Pass 4: Fallback — LEFT JOIN remaining input with book effective and underlyings states.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 900-960.
    Returns final combined alloc_input.
    """
    log_section(logger, "build_allocation_input_pass4")
    t0 = time.time()

    sm_state_lines = cfg["_sm_state_lines"]

    new_rows = (
        input_data_load.alias("L")
        .join(
            sm_state_lines.alias("K"),
            F.col("K.StateFieldID") == F.col("L.StateLineID"),
        )
        .join(
            book_effective.alias("B"),
            (F.col("B.UnderlyingEntityID") == F.col("L.EntityID"))
            & _book_effective_join_cond("B", "L"),
            "left",
        )
        .join(
            F.broadcast(all_underlyings_states).alias("AI"),
            (F.col("L.EntityID") == F.col("AI.UnderlyingEntityId"))
            & (F.col("L.StateID") == F.col("AI.StateID"))
            & (F.col("L.StateLineID") == F.col("AI.LineID"))
            & (F.col("L.TrackingKey") == F.col("AI.TrackingKey")),
            "left",
        )
        .select(*_alloc_input_columns("L", "B", cfg, ai_alias="AI"))
    )

    alloc_input = alloc_input_so_far.unionByName(new_rows)


    log_timing(logger, "build_allocation_input_pass4", t0)
    return alloc_input


# ---------------------------------------------------------------------------
# Function 17: build_entity_partners
# SQL lines: 980-990 (S15)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_entity_partners(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load entity partners from Partner_Snapshot.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 980-990.
    Row count: ALWAYS-NON-EMPTY.

    TODO: Replace with get_partners_list_for_allocations() once validated.
    """
    log_section(logger, "build_entity_partners")
    t0 = time.time()

    df = _tbl(spark, "Partner_Snapshot", cfg).filter(
        (F.col("ClientID") == cfg["client_id"])
        & (F.col("TaxPeriodID") == cfg["tax_period_id"])
        & (F.col("EntityID") == cfg["entity_id"])
    )
    df = df.select("PartnerNumber", "ShareClass").distinct()

    log_timing(logger, "build_entity_partners", t0)
    return df


# ---------------------------------------------------------------------------
# Function 18: build_final_effective_percentages
# SQL lines: 992-995 (S15)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_final_effective_percentages(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load final effective percentages for this run.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 992-995.
    Row count: ALWAYS-NON-EMPTY.
    """
    log_section(logger, "build_final_effective_percentages")
    t0 = time.time()

    df = (
        _tbl(spark, "SM_FinalEffectivePercentages", cfg)
        .filter(
            (F.col("RunID") == cfg["run_id"])
            & (F.col("RankForRule") == cfg["rank_for_rule_pickup"])
        )
        .select(
            "InvestmentID", "PartnerNumber",
            F.col("EffPercentage"), "AllocationType", "Quarter",
            "TypeID", "TrackingKey", "Tag", "LineID",
            F.col("EffAmount"), "AssetClassID", "IsExcludefromTransfer",
        )
    )


    log_timing(logger, "build_final_effective_percentages", t0)
    return df


# ---------------------------------------------------------------------------
# Function 19: compute_by_amount_allocation
# SQL lines: 1000-1130 (S16)
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def compute_by_amount_allocation(
    spark: SparkSession, cfg: dict,
    alloc_input: DataFrame,
    eff_pct: DataFrame,
    entity_partners: DataFrame,
) -> DataFrame:
    """Compute by-amount allocation and log warnings for overallocations.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 1000-1130.
    Row count: POSSIBLY-EMPTY (not all allocations are by-amount).
    """
    log_section(logger, "compute_by_amount_allocation")
    t0 = time.time()

    dar_txn_id = cfg["dar_txn_id"]
    global_dar_txn_id = cfg["global_dar_txn_id"]
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]

    dar_setup = cfg["_dar_setup"]

    # Common join: alloc_input + eff_pct + DAR + AllocationBy + Partners
    # Filter: EffPercentage=0, EffAmount<>0, AllocationBy='AMOUNT'
    by_amount_base = (
        alloc_input.alias("L")
        .join(
            eff_pct.alias("T"),
            (_ns0(F.col("L.EntityID")) == _ns0(F.col("T.InvestmentID")))
            & (F.col("L.TypeID") == F.col("T.TypeID"))
            & (F.col("L.CustomTrackingkey") == F.col("T.TrackingKey"))
            & (F.col("L.CustomTag") == F.col("T.Tag"))
            & (F.col("L.IsExcludefromTransfer") == F.col("T.IsExcludefromTransfer")),
        )
        .join(
            dar_setup.alias("M"),
            F.col("L.TypeID") == F.col("M.RuleID"),
        )
        .join(
            cfg["_enu_allocation_by"].alias("EA"),
            F.col("M.AllocationByID") == F.col("EA.AllocationByID"),
        )
        .join(
            F.broadcast(_tbl(spark, "ENU_AllocationPercentageType", cfg)).alias("AP"),
            F.col("AP.AllocationPercentageTypeID") == F.col("M.AllocationPercentageTypeID"),
        )
        .join(
            F.broadcast(entity_partners).alias("P"),
            F.col("T.PartnerNumber") == F.col("P.PartnerNumber"),
        )
        .filter(
            (_ns0(F.col("T.EffPercentage")) == 0)
            & (_ns0(F.col("T.EffAmount")) != 0)
            & (F.col("EA.AllocationBy") == "AMOUNT")
        )
    )

    # L1015-1040: Check for overallocation (warning)
    # Single collect replaces isEmpty() + collect() — one Spark action instead of two.
    warning_rules = (
        by_amount_base
        .groupBy(
            F.col("L.EntityID"), F.col("P.ShareClass"),
            F.col("L.LineTypeID"), F.col("L.StateLineID"), F.col("L.StateID"),
            F.col("T.AllocationType"), F.col("L.QuicklinkID"),
            F.col("L.ParentEntityID"), F.col("L.SuperParentEntityID"),
            F.col("L.AdjustmentTypeID"), F.col("L.TrackingKey"),
            F.col("L.TypeID"), F.col("L.Tag"), F.col("L.Amount"),
        )
        .agg(F.sum(F.col("T.EffAmount")).alias("TotalAmount"))
        .filter(_ns0(F.col("TotalAmount")) > F.col("Amount"))
        .join(
            cfg["_enu_custom_allocations"].alias("EC"),
            F.col("TypeID") == F.col("EC.AllocationTypeID"),
        )
        .select(F.col("EC.AllocationType"))
        .distinct()
        .collect()
    )

    # Log warning if overallocated amounts exist
    if warning_rules:
        rules_str = ", ".join([r["AllocationType"] for r in warning_rules])

        # INSERT into AllocationRunErrors
        error_msg = f"Allocated amounts are greater than input amounts for following rules :  {rules_str}"
        error_df = spark.createDataFrame(
            [(run_id, entity_id, error_msg, 0, "Warning")],
            ["RunID", "EntityID", "ErrorMessage", "LogID", "ErrororWarning"],
        )
        error_df.writeTo(f"{_tp(cfg)}.AllocationRunErrors").append()
        logger.warning(f"Overallocation warning logged: {rules_str}")

    # L1080-1120: INSERT into #TempSMLookthroughAllocationOutput (by-amount rows)
    output = (
        by_amount_base
        .select(
            F.col("L.EntityID"),
            F.col("P.ShareClass"),
            F.col("T.PartnerNumber"),
            F.col("L.LineTypeID"),
            F.col("L.StateLineID"),
            F.col("L.StateID"),
            F.col("T.EffAmount").alias("Amount"),
            F.col("T.AllocationType"),
            F.col("L.QuicklinkID"),
            F.col("T.EffAmount").alias("Amount704b"),
            F.col("L.ParentEntityID"),
            F.col("L.SuperParentEntityID"),
            F.col("L.AdjustmentTypeID"),
            F.col("L.TrackingKey"),
            F.col("L.TypeID"),
            F.col("L.Tag"),
            F.col("L.OriginalParentEntityID"),
        )
    )


    log_timing(logger, "compute_by_amount_allocation", t0)
    return output


# ---------------------------------------------------------------------------
# Function 20: apply_amount_deduction
# SQL lines: 1130-1195 (S17)
# ---------------------------------------------------------------------------
def apply_amount_deduction(
    spark: SparkSession, cfg: dict,
    alloc_input: DataFrame,
    by_amount_output: DataFrame,
) -> DataFrame:
    """Aggregate by-amount output and deduct from allocation input amounts.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 1130-1195.
    Returns updated alloc_input with reduced amounts and small amounts removed.
    """
    log_section(logger, "apply_amount_deduction")
    t0 = time.time()

    run_id = cfg["run_id"]
    valid_alloc_types = [
        "Cost", "CostAdjustedDatedTransfer", "ProRata",
        "DEFAULT", "DefaultAdjustedDatedTransfer", "Cost without Transfer Adj %",
    ]

    # L1130-1140: Aggregate by-amount output
    # SQL groups by ISNULL(AdjustmentTypeID,0) — must be in groupBy, not agg
    aggregated = (
        by_amount_output
        .filter(F.col("AllocationType").isin(valid_alloc_types))
        .withColumn("AdjustmentTypeID_g", _ns0(F.col("AdjustmentTypeID")))
        .groupBy(
            "StateLineID", "StateID", "LineTypeID", "EntityID",
            "ParentEntityID", "SuperParentEntityID", "TrackingKey",
            "Tag", "AdjustmentTypeID_g",
        )
        .agg(
            F.sum(_ns0(F.col("Amount"))).alias("AllocatedAmount"),
        )
        .withColumnRenamed("AdjustmentTypeID_g", "AdjustmentTypeID")
    )

    # L1140-1155: UPDATE alloc_input SET Amount = Amount - AllocatedAmount
    # Join with ISNULL null-safe pattern
    # NOTE: SQL omits StateID in this join, but since both sides are keyed by StateID,
    # we include it to prevent fan-out in PySpark (SQL Server UPDATE is non-deterministic on multi-match)
    updated = (
        alloc_input.alias("L")
        .join(
            aggregated.alias("AO"),
            (F.col("L.EntityID") == F.col("AO.EntityID"))
            & (_ns0(F.col("L.ParentEntityID")) == _ns0(F.col("AO.ParentEntityID")))
            & (_ns0(F.col("L.SuperParentEntityID")) == _ns0(F.col("AO.SuperParentEntityID")))
            # NOTE: SQL uses ISNULL(TrackingKey, 0) — VARCHAR with 0 default = string '0'
            & (F.coalesce(F.col("L.TrackingKey"), F.lit("0")) ==
               F.coalesce(F.col("AO.TrackingKey"), F.lit("0")))
            & (_ns0(F.col("L.AdjustmentTypeID")) == _ns0(F.col("AO.AdjustmentTypeID")))
            & (F.col("L.StateLineID") == F.col("AO.StateLineID"))
            & (F.col("L.StateID") == F.col("AO.StateID"))
            & (F.col("L.LineTypeID") == F.col("AO.LineTypeID"))
            & (_ns(F.col("L.Tag")) == F.col("AO.Tag")),
            "left",
        )
        .withColumn(
            "Amount",
            F.when(
                F.col("AO.AllocatedAmount").isNotNull(),
                F.col("L.Amount") - F.col("AO.AllocatedAmount"),
            ).otherwise(F.col("L.Amount")),
        )
        .select(
            F.col("L.RunID"), F.col("L.ClientID"), F.col("L.EntityID"),
            F.col("L.LineTypeID"), F.col("L.StateLineID"), F.col("L.StateID"),
            F.col("Amount"),
            F.col("L.QuicklinkID"), F.col("L.Amount704b"), F.col("L.CategoryID"),
            F.col("L.ParentEntityID"), F.col("L.PeriodID"), F.col("L.LineCode"),
            F.col("L.SuperParentEntityID"), F.col("L.AdjustmentTypeID"),
            F.col("L.TrackingKey"), F.col("L.Tag"),
            F.col("L.CustomTrackingkey"), F.col("L.CustomTag"),
            F.col("L.TypeID"), F.col("L.OriginalParentEntityID"),
            F.col("L.IsExcludefromTransfer"),
        )
    )

    # L1160: DELETE small amounts (between -0.99 and 0.99)
    updated = updated.filter(
        ~(
            (F.col("RunID") == run_id)
            & (_ns0(F.col("Amount")).between(-0.99, 0.99))
        )
    )

    log_timing(logger, "apply_amount_deduction", t0)
    return updated


# ---------------------------------------------------------------------------
# Function 21: compute_by_percentage_allocation
# SQL lines: 1210-1310 (S19)
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def compute_by_percentage_allocation(
    spark: SparkSession, cfg: dict,
    alloc_input: DataFrame,
    eff_pct: DataFrame,
    entity_partners: DataFrame,
) -> DataFrame:
    """Compute by-percentage allocation with dated transfers branching.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 1210-1310.
    Row count: POSSIBLY-EMPTY (not all allocations are by-percentage).

    First removes by-amount rules from eff_pct (S18), then branches on
    IsDatedTransfersConfigured for quarter date resolution.
    """
    log_section(logger, "compute_by_percentage_allocation")
    t0 = time.time()

    dar_txn_id = cfg["dar_txn_id"]
    global_dar_txn_id = cfg["global_dar_txn_id"]
    allocation_type_name = cfg.get("allocation_type_name")
    is_dated_transfers = cfg.get("is_dated_transfers")

    dar_setup = cfg["_dar_setup"]

    valid_alloc_types = [
        "Cost", "CostAdjustedDatedTransfer", "ProRata",
        "DEFAULT", "DefaultAdjustedDatedTransfer", "Cost without Transfer Adj %",
    ]

    # S18: DELETE by-amount rules from eff_pct
    by_amount_rule_ids = (
        dar_setup.alias("M")
        .join(
            cfg["_enu_allocation_by"].alias("EA"),
            (F.col("M.AllocationByID") == F.col("EA.AllocationByID"))
            & (F.col("EA.AllocationBy") == "AMOUNT"),
        )
        .select(F.col("M.RuleID"))
        .distinct()
    )

    filtered_eff_pct = eff_pct.alias("EP").join(
        by_amount_rule_ids.alias("BAR"),
        F.col("EP.TypeID") == F.col("BAR.RuleID"),
        "left_anti",
    )

    sm_state_lines = cfg["_sm_state_lines"]

    if (allocation_type_name == "PE Book Allocation"
            and is_dated_transfers is not None and is_dated_transfers == "C"):
        # Dated transfers path: use QuarterDates
        output = (
            alloc_input.alias("L")
            .join(
                sm_state_lines.alias("K"),
                (F.col("K.StateFieldID") == F.col("L.StateLineID"))
                & (F.col("L.StateID") == F.col("K.StateID")),
            )
            .join(
                _tbl(spark, "QuarterDates", cfg).alias("D"),
                F.coalesce(F.col("K.TransactionDate"), F.lit("1900-01-01").cast("date"))
                == F.col("D.StartDate"),
                "left",
            )
            .join(
                filtered_eff_pct.alias("T"),
                (_ns0(F.col("L.EntityID")) == _ns0(F.col("T.InvestmentID")))
                & (F.col("T.Quarter") == F.col("D.Quarter"))
                & (F.col("T.TypeID") == F.col("L.TypeID"))
                & (F.col("L.CustomTrackingkey") == F.col("T.TrackingKey"))
                & (F.col("L.CustomTag") == F.col("T.Tag"))
                & (F.col("L.IsExcludefromTransfer") == F.col("T.IsExcludefromTransfer"))
                & (F.col("T.AllocationType").isin(valid_alloc_types)),
            )
            .join(
                F.broadcast(entity_partners).alias("P"),
                F.col("T.PartnerNumber") == F.col("P.PartnerNumber"),
            )
            .select(
                F.col("L.EntityID"),
                F.col("P.ShareClass"),
                F.col("T.PartnerNumber"),
                F.col("L.LineTypeID"),
                F.col("L.StateLineID"),
                F.col("L.StateID"),
                (F.col("L.Amount") * F.col("T.EffPercentage")).alias("Amount"),
                F.col("T.AllocationType"),
                F.col("L.QuicklinkID"),
                (F.col("L.Amount") * F.col("T.EffPercentage")).alias("Amount704b"),
                F.col("L.ParentEntityID"),
                F.col("L.SuperParentEntityID"),
                F.col("L.AdjustmentTypeID"),
                F.col("L.TrackingKey"),
                F.col("L.TypeID"),
                F.col("L.Tag"),
                F.col("L.OriginalParentEntityID"),
            )
        )
    else:
        # Non-dated transfers path: use ENU_DF_DataList for quarter resolution
        output = (
            alloc_input.alias("L")
            .join(
                sm_state_lines.alias("K"),
                (F.col("K.StateFieldID") == F.col("L.StateLineID"))
                & (F.col("L.StateID") == F.col("K.StateID")),
            )
            .join(
                _tbl(spark, "ENU_DF_DataList", cfg).alias("D"),
                (F.col("D.LookUpValue") ==
                 F.coalesce(F.month(F.col("K.TransactionDate")), F.lit(0)).cast("string"))
                & (F.col("D.Category") == "QuarterMonth"),
            )
            .join(
                filtered_eff_pct.alias("T"),
                (_ns0(F.col("L.EntityID")) == _ns0(F.col("T.InvestmentID")))
                & (F.col("T.Quarter") == F.col("D.LookUpData"))
                & (F.col("T.TypeID") == F.col("L.TypeID"))
                & (F.col("L.CustomTrackingkey") == F.col("T.TrackingKey"))
                & (F.col("L.CustomTag") == F.col("T.Tag"))
                & (F.col("T.AllocationType").isin(valid_alloc_types)),
            )
            .join(
                F.broadcast(entity_partners).alias("P"),
                F.col("T.PartnerNumber") == F.col("P.PartnerNumber"),
            )
            .select(
                F.col("L.EntityID"),
                F.col("P.ShareClass"),
                F.col("T.PartnerNumber"),
                F.col("L.LineTypeID"),
                F.col("L.StateLineID"),
                F.col("L.StateID"),
                (F.col("L.Amount") * F.col("T.EffPercentage")).alias("Amount"),
                F.col("T.AllocationType"),
                F.col("L.QuicklinkID"),
                (F.col("L.Amount") * F.col("T.EffPercentage")).alias("Amount704b"),
                F.col("L.ParentEntityID"),
                F.col("L.SuperParentEntityID"),
                F.col("L.AdjustmentTypeID"),
                F.col("L.TrackingKey"),
                F.col("L.TypeID"),
                F.col("L.Tag"),
                F.col("L.OriginalParentEntityID"),
            )
        )


    log_timing(logger, "compute_by_percentage_allocation", t0)
    return output


# ============================================================================
# CHUNK 3: Output + Write (S20-S23)
# ============================================================================

# ---------------------------------------------------------------------------
# Function 22: build_final_output
# SQL lines: 1305-1320 (S21)
# Row count: ALWAYS-NON-EMPTY (when alloc_output has rows)
# ---------------------------------------------------------------------------
def build_final_output(
    spark: SparkSession, cfg: dict,
    alloc_output: DataFrame,
) -> DataFrame:
    """Map AllocationType via ENU_CustomAllocations CASE WHEN and build final output.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 1305-1320.
    Row count: ALWAYS-NON-EMPTY (when alloc_output has rows).

    SQL CASE WHEN mapping:
      'Cost'                         → EC.AllocationType
      'CostAdjustedDatedTransfer'    → EC.AllocationType + 'AdjustedDatedTransfer'
      'DEFAULT'                      → 'Cost'
      'DefaultAdjustedDatedTransfer' → 'CostAdjustedDatedTransfer'
      'Cost without Transfer Adj %'  → EC.AllocationType + ' without Transfer Adj %'
      ELSE                           → 'ProRata'
    """
    log_section(logger, "build_final_output")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]

    enu_custom = cfg["_enu_custom_allocations"]

    joined = (
        alloc_output.alias("L")
        .join(
            enu_custom.alias("EC"),
            F.col("L.TypeID") == F.col("EC.AllocationTypeID"),
            "left",
        )
    )

    mapped_alloc_type = (
        F.when(F.col("L.AllocationType") == "Cost", F.col("EC.AllocationType"))
        .when(
            F.col("L.AllocationType") == "CostAdjustedDatedTransfer",
            F.concat(F.col("EC.AllocationType"), F.lit("AdjustedDatedTransfer")),
        )
        .when(F.col("L.AllocationType") == "DEFAULT", F.lit("Cost"))
        .when(
            F.col("L.AllocationType") == "DefaultAdjustedDatedTransfer",
            F.lit("CostAdjustedDatedTransfer"),
        )
        .when(
            F.col("L.AllocationType") == "Cost without Transfer Adj %",
            F.concat(F.col("EC.AllocationType"), F.lit(" without Transfer Adj %")),
        )
        .otherwise(F.lit("ProRata"))
    )

    df = joined.select(
        F.lit(run_id).alias("RunID"),
        F.lit(client_id).alias("ClientID"),
        F.col("L.EntityID"),
        F.col("L.ShareClass"),
        F.col("L.PartnerNumber"),
        F.col("L.LineTypeID"),
        F.col("L.StateLineID"),
        F.col("L.StateID"),
        F.col("L.Amount"),
        mapped_alloc_type.alias("AllocationType"),
        F.col("L.QuicklinkID"),
        F.col("L.Amount").alias("Amount704b"),
        F.col("L.ParentEntityID"),
        F.col("L.SuperParentEntityID"),
        F.col("L.AdjustmentTypeID"),
        F.col("L.TrackingKey"),
        F.col("L.Tag"),
        F.col("EC.AllocationTypeID"),
        F.col("L.OriginalParentEntityID"),
    )


    log_timing(logger, "build_final_output", t0)
    return df


# ---------------------------------------------------------------------------
# Function 23: write_allocation_output
# SQL lines: 1305-1320 (S21)
# ---------------------------------------------------------------------------
def write_allocation_output(
    spark: SparkSession, cfg: dict,
    final_output: DataFrame,
) -> int:
    """INSERT final output into SM_LookThroughAllocationOutput.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 1305-1320.
    """
    log_section(logger, "write_allocation_output")
    t0 = time.time()

    columns = [
        "RunID", "ClientID", "EntityID", "ShareClass", "PartnerNumber",
        "LineTypeID", "StateLineID", "StateID", "Amount", "AllocationType",
        "QuicklinkID", "Amount704b", "ParentEntityID", "SuperParentEntityID",
        "AdjustmentTypeID", "TrackingKey", "Tag", "AllocationTypeID",
        "OriginalParentEntityID",
    ]

    # Write via GenericResultStorer (Delta + Parquet in parallel)
    return_value = None
    rt = cfg.get("result_type", "Parquet")
    if rt:
        # Cast DataFrame to match target Delta table schema (saveAsTable is strict)
        table_name = "SM_LookThroughAllocationOutput"
        fqn = f"{cfg['catalog']}.{cfg['schema']}.{table_name}"
        target_schema = spark.table(fqn).schema
        write_df = final_output.select(columns)
        for field in target_schema:
            if field.name in write_df.columns:
                write_df = write_df.withColumn(field.name, F.col(field.name).cast(field.dataType))

        storer = GenericResultStorer(spark)
        return_value = storer.save_results(
            result={table_name: write_df},
            result_type=rt,
            catalog_name=cfg["catalog"],
            database_name=cfg["schema"],
            run_id=cfg["run_id"],
            client_id=cfg["client_id"],
            entity_id=cfg["entity_id"],
            execution_id=cfg.get("execution_id"),
            volume_path=cfg.get("volume_path"),
            sql_url_path=None,
            sql_username=None,
            sql_password=None,
        )
        if return_value:
            logger.info(f"[WRITE] SM_LookThroughAllocationOutput result_type={rt}, result={return_value}")

    log_timing(logger, "write_allocation_output", t0)
    return return_value


# ---------------------------------------------------------------------------
# Function 24: write_update_allocation_input
# SQL lines: 1325-1355 (S22)
# ---------------------------------------------------------------------------
def write_update_allocation_input(
    spark: SparkSession, cfg: dict,
    alloc_output: DataFrame,
) -> None:
    """Deduct allocated amounts from permanent SM_LookThroughAllocationInput.

    Converted from: usp_SM_LoadLookThroughCostAllocationToOutput, SQL lines 1325-1355.

    Strategy: Read-Modify-Write with broadcast join (single pass, no MERGE).
    1. Aggregate alloc_output (valid types) → deduction amounts
    2. Read RunID rows from target table
    3. Broadcast-join with deductions, compute new Amount + zero residuals in one expression
    4. Atomic overwrite of RunID rows
    """
    log_section(logger, "write_update_allocation_input")
    t0 = time.time()

    run_id = cfg["run_id"]
    fqn = f"{table_prefix(cfg)}.SM_LookThroughAllocationInput"

    valid_alloc_types = [
        "Cost", "CostAdjustedDatedTransfer", "ProRata",
        "DEFAULT", "DefaultAdjustedDatedTransfer", "Cost without Transfer Adj %",
    ]

    # Step 1: Aggregate alloc_output → deduction amounts per key
    deductions = (
        alloc_output
        .filter(F.col("AllocationType").isin(valid_alloc_types))
        .withColumn("AdjustmentTypeID_g", _ns0(F.col("AdjustmentTypeID")))
        .groupBy(
            "EntityID", "StateLineID", "StateID", "LineTypeID",
            "ParentEntityID", "SuperParentEntityID", "TrackingKey",
            "Tag", "AdjustmentTypeID_g",
        )
        .agg(F.sum(_ns0(F.col("Amount"))).alias("DeductAmount"))
        .withColumnRenamed("AdjustmentTypeID_g", "AdjustmentTypeID")
    )

    # Step 2: Read all RunID rows from target table
    current = spark.table(fqn).filter(F.col("RunID") == run_id)

    # Step 3: Broadcast-join + compute new Amount in one pass
    join_cond = (
        (F.col("t.EntityID") == F.col("s.EntityID"))
        & (F.coalesce(F.col("t.ParentEntityID"), F.lit(0)) == F.coalesce(F.col("s.ParentEntityID"), F.lit(0)))
        & (F.coalesce(F.col("t.SuperParentEntityID"), F.lit(0)) == F.coalesce(F.col("s.SuperParentEntityID"), F.lit(0)))
        & (F.coalesce(F.col("t.TrackingKey"), F.lit("0")) == F.coalesce(F.col("s.TrackingKey"), F.lit("0")))
        & (F.coalesce(F.col("t.AdjustmentTypeID"), F.lit(0)) == F.col("s.AdjustmentTypeID"))
        & (F.col("t.StateLineID") == F.col("s.StateLineID"))
        & (F.col("t.LineTypeID") == F.col("s.LineTypeID"))
        & (F.col("t.StateID") == F.col("s.StateID"))
        & (F.coalesce(F.col("t.Tag"), F.lit("")) == F.coalesce(F.col("s.Tag"), F.lit("")))
    )

    # Single expression: deduct if matched, zero if small, pass through otherwise
    raw_amount = F.when(
        F.col("s.DeductAmount").isNotNull(),
        F.col("t.Amount") - F.col("s.DeductAmount")
    ).otherwise(F.col("t.Amount"))

    new_amount = F.when(
        raw_amount.between(-0.99, 0.99), F.lit(0.0)
    ).otherwise(raw_amount).cast("double")

    updated = (
        current.alias("t")
        .join(F.broadcast(deductions).alias("s"), join_cond, "left")
        .select(
            F.col("t.RunID"), F.col("t.ClientID"), F.col("t.EntityID"),
            F.col("t.LineTypeID"), F.col("t.StateID"), F.col("t.StateLineID"),
            new_amount.alias("Amount"),
            F.col("t.QuicklinkID"), F.col("t.Amount704b"), F.col("t.CategoryID"),
            F.col("t.ParentEntityID"), F.col("t.PeriodID"), F.col("t.LineCode"),
            F.col("t.SuperParentEntityID"), F.col("t.AdjustmentTypeID"),
            F.col("t.TrackingKey"), F.col("t.Tag"),
            F.col("t.OriginalParentEntityID"), F.col("t.FlowUpPartner"),
        )
    )

    # Step 4: Atomic overwrite — single Delta commit
    updated.writeTo(fqn).overwrite(F.col("RunID") == run_id)
    logger.info(f"[WRITE] Overwrote deducted amounts in {fqn}")

    log_timing(logger, "write_update_allocation_input", t0)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_sm_load_lookthrough_cost_allocation_to_output(
    spark: SparkSession,
    cfg: dict = None,
    verbose: bool = False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    RankForRulePickup: int = 0,
    CatalogName: str = None,
    SchemaName: str = None,
    CallFrom: str = None,
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
    """Main entry point — Checkpoint V2 seams, sequential SM Output then Input."""
    del kwargs
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    rank_for_rule_pickup = RankForRulePickup
    catalog = CatalogName
    schema = SchemaName
    call_from = CallFrom
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

    status = {
        "sp_name": "usp_SM_LoadLookThroughCostAllocationToOutput",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "skip_reason": None,
    }
    return_value = None

    try:
        if cfg is None:
            cfg = load_common_config(
                spark,
                entity_id=entity_id,
                client_id=client_id,
                tax_period_id=tax_period_id,
                run_id=run_id,
                catalog=catalog,
                schema=schema,
                call_from=call_from,
                rank_for_rule_pickup=rank_for_rule_pickup,
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

        load_sp_config(spark, cfg)
        status["run_id"] = cfg.get("run_id")
        status["entity_id"] = cfg.get("entity_id")
        dar_txn_id = cfg["dar_txn_id"]
        global_dar_txn_id = cfg["global_dar_txn_id"]
        cfg["_sm_state_lines"] = F.broadcast(
            _tbl(spark, "SM_StateLines", cfg).select(
                "StateFieldID", "StateID", "TransactionDate"
            )
        )
        cfg["_dar_setup"] = F.broadcast(
            _tbl(spark, "DefaultAllocationRuleSetup", cfg)
            .filter(F.col("TransactionID").isin(dar_txn_id, global_dar_txn_id))
            .select(
                "RuleID",
                "UnderlyingTypeID",
                "RuleTypeID",
                "AllocationByID",
                "AllocationPercentageTypeID",
            )
        )
        cfg["_enu_allocation_by"] = F.broadcast(_tbl(spark, "ENU_AllocationBy", cfg))
        cfg["_enu_custom_allocations"] = F.broadcast(
            _tbl(spark, "ENU_CustomAllocations", cfg).select(
                "AllocationTypeID", "AllocationType"
            )
        )
        cfg["_enu_underlying_type"] = F.broadcast(
            _tbl(spark, "Enu_Underlyingtype", cfg)
        )
        cfg["_entity_lookup"] = F.broadcast(
            _tbl(spark, "Entity", cfg).select("EntityID", "AssetClassID")
        )
        if not validate_run_status_for_sp(spark, cfg):
            status["status"] = "SKIPPED"
            status["error"] = "RunStatus=FAIL or entity type mismatch"
            status["skip_reason"] = "run_status_or_entity_mismatch"
            return status

        with use_v2_production_checkpoint():
            book_effective, input_data_load, cost_pct_snapshot, entity_ac_rel = (
                run_parallel(
                    [
                        ("book_effective", lambda: build_book_effective(spark, cfg)),
                        ("input_data_load", lambda: build_input_data_load(spark, cfg)),
                        (
                            "cost_pct_snapshot",
                            lambda: build_cost_percentage_snapshot(spark, cfg),
                        ),
                        (
                            "entity_ac_rel",
                            lambda: build_entity_asset_class_relationship(spark, cfg),
                        ),
                    ],
                    workers,
                    parallel_activity,
                    "independent_loads",
                    enabled_groups,
                )
            )
            book_effective = F.broadcast(book_effective)
            input_data_load = _checkpoint(
                spark, input_data_load, "temp_alloc_input", cfg
            )
            cost_pct_snapshot = _checkpoint(
                spark, cost_pct_snapshot, "cost_pct_snapshot", cfg
            )

            cost_underlying_types = build_cost_underlying_types(
                spark, cfg, cost_pct_snapshot
            )
            if cost_underlying_types.isEmpty():
                entity_hier = None
            else:
                entity_hier = build_entity_hierarchy(
                    spark, cfg, cost_underlying_types
                )
                entity_hier = _checkpoint(
                    spark, entity_hier, "entity_hier_final", cfg
                )
            all_underlyings = build_all_underlyings_combined(
                spark,
                cfg,
                cost_underlying_types,
                cost_pct_snapshot,
                entity_hier,
            )
            all_underlyings = apply_asset_class_filter(
                spark, cfg, all_underlyings, entity_ac_rel
            )
            states_dar_mapping = build_states_dar_rule_mapping(spark, cfg)
            all_underlyings_states = build_all_underlyings_states(
                spark,
                cfg,
                all_underlyings,
                input_data_load,
                states_dar_mapping,
            )

            alloc_input, remaining_input, remaining_be = build_allocation_input_pass1(
                spark, cfg, input_data_load, book_effective, cost_pct_snapshot
            )
            alloc_input = _checkpoint(spark, alloc_input, "alloc_pass1", cfg)
            alloc_input, remaining_input, remaining_be = build_allocation_input_pass2(
                spark, cfg, remaining_input, remaining_be, alloc_input
            )
            alloc_input = _checkpoint(spark, alloc_input, "alloc_pass2", cfg)
            alloc_input, remaining_input, remaining_be = build_allocation_input_pass3(
                spark, cfg, remaining_input, remaining_be, alloc_input
            )
            alloc_input = _checkpoint(spark, alloc_input, "alloc_pass3", cfg)
            alloc_input = build_allocation_input_pass4(
                spark,
                cfg,
                remaining_input,
                remaining_be,
                all_underlyings_states,
                alloc_input,
            )
            alloc_input = _checkpoint(
                spark, alloc_input, "alloc_input_final", cfg
            )

            if not alloc_input.isEmpty():
                entity_partners = build_entity_partners(spark, cfg)
                eff_pct = build_final_effective_percentages(spark, cfg)
                eff_pct = _checkpoint(spark, eff_pct, "fep", cfg)
                eff_pct = F.broadcast(eff_pct)
                by_amount_output = compute_by_amount_allocation(
                    spark, cfg, alloc_input, eff_pct, entity_partners
                )
                by_amount_output = _checkpoint(
                    spark, by_amount_output, "alloc_pass1_amount", cfg
                )
                alloc_input = apply_amount_deduction(
                    spark, cfg, alloc_input, by_amount_output
                )
                alloc_input = _checkpoint(
                    spark, alloc_input, "alloc_pass2_deduct", cfg
                )
                by_pct_output = compute_by_percentage_allocation(
                    spark, cfg, alloc_input, eff_pct, entity_partners
                )
                by_pct_output = _checkpoint(
                    spark, by_pct_output, "alloc_pass4", cfg
                )
                alloc_output = by_amount_output.unionByName(by_pct_output)
                alloc_output = _checkpoint(
                    spark, alloc_output, "alloc_output", cfg
                )
                final_output = build_final_output(spark, cfg, alloc_output)
                # Sequential SM Output then Input.
                return_value = write_allocation_output(spark, cfg, final_output)
                write_update_allocation_input(spark, cfg, alloc_output)
            else:
                logger.warning(
                    "alloc_input is empty — skipping allocation logic"
                )
                status["skip_reason"] = "empty_alloc_input"
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
        f"[DONE] run_sm_load_lookthrough_cost_allocation_to_output | "
        f"{status['elapsed_seconds']}s | "
        f"RunID={cfg['run_id']} EntityID={cfg['entity_id']}"
    )
    if (
        return_value
        and isinstance(return_value, str)
        and return_value not in ("SUCCESS", "")
    ):
        logger.info(f"[PARQUET] Return JSON: {return_value}")
        status["parquet_path"] = return_value
    return status



# ════════════════════════════════════════════════════════════════
# __main__: standalone execution (Mode 3).
# Read widget params → call run_sm_load_lookthrough_cost_allocation_to_output(...).
# The function's `if cfg is None` branch is the single point that calls
# load_common_config. Job/Orchestrator modes pass cfg in directly and skip
# this block.
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    try:
        result = run_sm_load_lookthrough_cost_allocation_to_output(
            spark,
            EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
            ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
            TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
            RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
            CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
            SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
        )
    except Exception as exc:
        raise RuntimeError(
            f"Usage: provide run_id, entity_id, client_id, tax_period_id, catalog, schema "
            f"as widget parameters ({exc})"
        )

    logger.info(f"Result: {result}")
