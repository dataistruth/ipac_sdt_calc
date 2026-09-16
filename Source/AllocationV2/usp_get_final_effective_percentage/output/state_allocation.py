"""
state_allocation.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Mode 3 state allocation: state book effective, state underlyings,
state input lines, state amounts, state non-dated/dated entities.
Conversion date: 2026-05-04

SQL lines: 5706-6210
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
import logging
import time

logger = logging.getLogger(__name__)


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _log_timing(name, start):
    logger.info(f"[TIMING] {name}: {time.time() - start:.1f}s")


def _local_cp(df: DataFrame, name: str) -> DataFrame:
    """Eager localCheckpoint that breaks the DAG without Delta I/O.
    Mirrors orchestrator._checkpoint local-mode behavior so this module
    stays import-independent of the orchestrator.
    """
    logger.info(f"[CHECKPOINT] {name} (localCheckpoint)")
    cp = df.localCheckpoint(eager=True)
    # toDF strips alias-qualifier metadata, mimicking spark.table() schema.
    return cp.toDF(*cp.columns)


def _match_key(col):
    return F.when(F.coalesce(col, F.lit("")) == "", F.lit("-1")).otherwise(col)


# ---------------------------------------------------------------------------
# build_state_allocation_input
# SQL lines: 5706-5950
# Row count: POSSIBLY-EMPTY (mode 3 only)
# ---------------------------------------------------------------------------
def build_state_allocation_input(
    spark: SparkSession, cfg: dict,
    underlying_mod: DataFrame,
    sm_input: DataFrame,
    cost_pct_snapshot: DataFrame,
    all_underlyings: DataFrame,
    map_dar: DataFrame,
    dar_setup: DataFrame,
    entity_partners: DataFrame,
) -> tuple:
    """Mode 3 state allocation: build state book effective, state underlyings,
    state entity amounts, and state input lines.

    Only runs when mode 3 data exists (sm_input is not empty).

    Returns: (updated_all_underlyings, state_input_lines, updated_final_amounts)
    """
    t0 = time.time()
    logger.info("[SECTION] build_state_allocation_input")

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    is_car = cfg.get("is_custom_allocation_rule_enabled", "U") == "C"
    override_flag = cfg.get("override_indirect_lookthrough_asset_class", "")
    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    k1_lt_id = cfg["k1_line_type_id"]
    entity_ut = cfg.get("entity_underlying_type_id")
    book_alloc_type_id = cfg.get("book_allocation_type_id")
    offset_alloc_type_id = cfg.get("offset_allocation_type_id")
    dar_tid = cfg.get("default_alloc_rule_transaction_id")
    gdar_tid = cfg.get("global_default_alloc_rule_transaction_id")
    valid_tids = [t for t in [dar_tid, gdar_tid] if t is not None]

    enu_ut = F.broadcast(_tbl(spark, "ENU_UnderlyingType", cfg))

    # --- SM_TempBookEffective: Load from SM_StateLineAllocationRule_Snapshot ---
    # Get state allocation workflow
    sm_event_row = (
        _tbl(spark, "ENU_Event", cfg)
        .filter(F.col("EventName") == "Import_StateAllocationRule")
        .select("EventTypeID")
        .first()
    )
    sm_event_type_id = sm_event_row["EventTypeID"] if sm_event_row else None

    if sm_event_type_id is None:
        logger.warning("Import_StateAllocationRule event not found — skipping state allocation")
        return all_underlyings, None, None

    # Inline udfGetApprovedWorkflow for state event
    # Original SQL joins Workflow → TransactionLog; EventTypeID/EntityID/StatusID
    # are on TransactionLog, NOT on Workflow.
    sm_workflow_id = 0
    phase_id = cfg.get("phase_id")

    # IncludeInCalc step from WorkFlowChain
    wfc_row = (
        _tbl(spark, "WorkFlowChain", cfg)
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
            & (F.col("IncludeInCalc") == True)
        )
        .select("WorkflowStatusID")
        .first()
    )
    include_in_calc_step = wfc_row["WorkflowStatusID"] if wfc_row else None

    if include_in_calc_step is not None:
        # Excluded statuses (non-adjustments path)
        excl_ids = (
            _tbl(spark, "WORKFLOWSTATUS", cfg)
            .filter(F.col("EnumerationName").isin(["Rejected", "Err_Critical", "Err_NonCritical"]))
            .select("StatusID")
        )
        excl_list = [r["StatusID"] for r in excl_ids.collect()]

        wf_tl = (
            _tbl(spark, "WorkFlow", cfg).alias("WF")
            .join(
                _tbl(spark, "TransactionLog", cfg).alias("TL"),
                (F.col("TL.TransactionID") == F.col("WF.TransactionID"))
                & (F.col("TL.EventTypeID") == sm_event_type_id)
                & (F.col("TL.PhaseID") == F.col("WF.PhaseID")),
            )
            .filter(
                (F.col("TL.ClientID") == client_id)
                & (F.col("TL.TaxPeriodID") == tax_period_id)
                & (F.col("TL.EntityID") == entity_id)
                & (F.col("TL.StatusID") >= include_in_calc_step)
                & (F.col("TL.PhaseID") == phase_id)
                & (~F.col("TL.StatusID").isin(excl_list))
            )
        )
        wf_row = wf_tl.agg(F.max("WF.WorkflowID").alias("max_wf")).first()
        if wf_row and wf_row["max_wf"] is not None:
            sm_workflow_id = wf_row["max_wf"]

    sm_book_eff = (
        _tbl(spark, "SM_StateLineAllocationRule_Snapshot", cfg)
        .filter(
            (F.col("WorkflowID") == sm_workflow_id)
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select(
            "UnderlyingEntityID", "StateLineID", "StateID",
            "AllocationTypeID",
            F.col("AdjustmentAllocationTypeID"),
            "TrackingKey", "Tag",
        )
    )

    # Update AdjustmentAllocationTypeID where it equals Book or Offset
    book_offset_ids = [i for i in [book_alloc_type_id, offset_alloc_type_id] if i is not None]
    if book_offset_ids:
        sm_book_eff = sm_book_eff.withColumn(
            "AdjustmentAllocationTypeID",
            F.when(
                F.col("AdjustmentAllocationTypeID").isin(book_offset_ids),
                F.col("AllocationTypeID"),
            ).otherwise(F.col("AdjustmentAllocationTypeID")),
        )

    # --- StatesMapDefaultAllocRuleToLineItem ---
    # From map_dar filtered for 'State Input' line type, replacing SourceID with K1LineTypeID
    enu_lt = F.broadcast(_tbl(spark, "ENU_LineType", cfg))
    state_map_dar = (
        map_dar.alias("M")
        .join(enu_lt.alias("EL"), F.col("EL.LineTypeID") == F.col("M.SourceID"))
        .filter(
            (F.col("M.TransactionID").isin(valid_tids))
            & (F.col("EL.LineType") == "State Input")
        )
        .select(
            F.lit(k1_lt_id).cast("int").alias("SourceID"),
            F.col("M.StateID"),
            F.col("M.SelectedMappingID"),
            F.col("M.RuleID"),
            F.col("M.ExcludeFromTransfers"),
        )
    )

    # --- Tracking key match helper ---
    def _tracking_match(ai_entity_col, ai_underlying_col, ai_tracking_col, l_tracking_col, u_type_col):
        direct = (
            (ai_underlying_col == entity_id)
            | ((ai_entity_col == entity_id) & (u_type_col != "ASSET CLASS"))
            | ((F.lit(override_flag) != "C") & (u_type_col == "ASSET CLASS"))
        )
        ai_side = F.when(direct, F.lit("-1")).otherwise(
            F.concat(F.lit("%"), ai_tracking_col, F.lit("%"))
        )
        l_side = F.when(direct, F.lit("-1")).otherwise(
            F.concat(F.lit("~"), l_tracking_col, F.lit("~"))
        )
        return l_side.like(ai_side)

    # --- State underlyings ordered ---
    if is_car:
        state_ordered = (
            underlying_mod.alias("AI")
            .join(enu_ut.alias("U"), F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"))
            .join(
                sm_input.alias("L"),
                (F.col("L.EntityID") == F.col("AI.UnderlyingEntityID"))
                & (_tracking_match(
                    F.col("AI.EntityID"), F.col("AI.UnderlyingEntityID"),
                    F.col("AI.TrackingKey"), F.col("L.TrackingKey"),
                    F.col("U.UnderlyingType"),
                )),
            )
            .select(
                F.col("AI.UnderlyingType").alias("Underlyingtype"),
                F.col("AI.UnderlyingEntityID").alias("UnderlyingEntityId"),
                F.col("AI.EntityID").alias("EntityId"),
                F.col("L.TrackingKey"),
                F.col("AI.TrackingKey").alias("TrackingMatch"),
                F.col("AI.AllocationTypeID").alias("AllocationTypeId"),
                F.col("L.StateLineID"),
                F.row_number().over(
                    Window.partitionBy(
                        F.col("AI.UnderlyingEntityID"),
                        F.col("L.TrackingKey"),
                        F.col("L.StateLineID"),
                        F.col("L.StateID"),
                        F.col("AI.AllocationTypeID"),
                    ).orderBy(
                        F.col("AI.HLevel"),
                        F.col("U.DisplayOrder"),
                        F.col("AI.TrackingKey"),
                    )
                ).alias("RankForUnderlyingPickup"),
                F.col("L.LineTypeID"),
                F.lit("PERCENT").alias("AllocationBy"),
                F.col("L.StateID"),
                F.lit(False).alias("IsExcludefromTransfer"),
            )
        )
    else:
        # DAR path
        state_ordered = (
            underlying_mod.alias("AI")
            .join(enu_ut.alias("U"), F.col("AI.UnderlyingType") == F.col("U.UnderlyingTypeID"))
            .join(
                sm_input.alias("L"),
                (F.col("L.EntityID") == F.col("AI.UnderlyingEntityID"))
                & (_tracking_match(
                    F.col("AI.EntityID"), F.col("AI.UnderlyingEntityID"),
                    F.col("AI.TrackingKey"), F.col("L.TrackingKey"),
                    F.col("U.UnderlyingType"),
                )),
            )
            .join(
                state_map_dar.alias("M"),
                (
                    F.when(F.col("M.StateID") == -1, F.lit(1))
                    .otherwise(F.col("M.StateID"))
                    == F.when(F.col("M.StateID") == -1, F.lit(1))
                    .otherwise(F.col("L.StateID"))
                )
                & (
                    F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                    .otherwise(F.col("L.StateLineID"))
                    == F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                    .otherwise(F.col("M.SelectedMappingID"))
                )
                & (F.col("M.RuleID") == F.col("AI.AllocationTypeID"))
                & (F.col("M.SourceID") == F.col("L.LineTypeID")),
            )
            .join(
                dar_setup.alias("D"),
                (F.col("D.RuleID") == F.col("AI.AllocationTypeID"))
                & (F.col("AI.UnderlyingType") == F.col("D.UnderlyingTypeID")),
            )
            .join(
                F.broadcast(_tbl(spark, "ENU_RuleType", cfg)).alias("R"),
                F.col("D.RuleTypeID") == F.col("R.RuleTypeID"),
            )
            .join(
                F.broadcast(_tbl(spark, "ENU_AllocationBy", cfg)).alias("EA"),
                F.col("D.AllocationByID") == F.col("EA.AllocationByID"),
            )
            .filter(F.col("D.TransactionID").isin(valid_tids))
            .select(
                F.col("AI.UnderlyingType").alias("Underlyingtype"),
                F.col("AI.UnderlyingEntityID").alias("UnderlyingEntityId"),
                F.col("AI.EntityID").alias("EntityId"),
                F.col("L.TrackingKey"),
                F.col("AI.TrackingKey").alias("TrackingMatch"),
                F.col("AI.AllocationTypeID").alias("AllocationTypeId"),
                F.col("L.StateLineID"),
                F.row_number().over(
                    Window.partitionBy(
                        F.col("AI.UnderlyingEntityID"),
                        F.col("L.TrackingKey"),
                        F.col("L.StateLineID"),
                        F.col("L.StateID"),
                        F.col("EA.DisplayOrder"),
                    ).orderBy(
                        F.col("AI.HLevel"),
                        F.col("R.DisplayOrder").desc(),
                        F.col("U.DisplayOrder"),
                        F.col("M.SelectedMappingID").desc(),
                        F.col("EA.DisplayOrder"),
                        F.col("AI.TrackingKey"),
                    )
                ).alias("RankForUnderlyingPickup"),
                F.col("L.LineTypeID"),
                F.col("EA.AllocationBy"),
                F.col("L.StateID"),
                F.col("M.ExcludeFromTransfers").cast("boolean").alias("IsExcludefromTransfer"),
            )
        )

    # Take rank 1 and append to all_underlyings
    state_rank1 = state_ordered.filter(F.col("RankForUnderlyingPickup") == F.lit(1))

    state_for_union = state_rank1.select(
        "Underlyingtype", "UnderlyingEntityId", "EntityId", "TrackingKey",
        "TrackingMatch", "AllocationTypeId",
        F.col("StateLineID").alias("LineID"),
        "RankForUnderlyingPickup", "LineTypeID", "AllocationBy",
        "StateID", "IsExcludefromTransfer",
    )

    updated_all_underlyings = all_underlyings.unionByName(
        state_for_union, allowMissingColumns=True,
    )

    # OPT: checkpoint updated_all_underlyings BEFORE its 3 downstream consumers
    # (sm_entity_amounts join below, state_input_lines pass4 join, and the
    # returned DF used by the orchestrator + build_cost_percentage_by_type).
    # Without this, the union+upstream DAG (state_ordered with row_number
    # Window over 4 tables) re-evaluates 3x. With this, materialize once,
    # downstream consumers scan the in-memory checkpoint.
    # Replaces the outer all_und_final_m3 checkpoint that the orchestrator
    # used to do after this function returned (which couldn't help pass4 or
    # sm_entity_amounts since both were already wired to the lazy plan).
    updated_all_underlyings = _local_cp(updated_all_underlyings, "state_updated_all_und")

    # --- State entity amounts (by-amount allocation) ---
    # SQL lines 5830-5950
    sm_entity_amounts = (
        enu_ut.alias("U")
        .join(
            updated_all_underlyings.alias("E"),
            F.col("U.UnderlyingTypeID") == F.col("E.Underlyingtype"),
        )
        .join(
            cost_pct_snapshot.alias("C"),
            (F.col("E.EntityId") == F.col("C.InvestmentID"))
            & (F.col("C.UnderlyingType") == F.col("E.Underlyingtype"))
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (
                F.concat(
                    F.lit("~"),
                    F.when(
                        F.coalesce(F.col("C.TrackingKey"), F.lit("")) == "",
                        F.col("C.InvestmentID").cast("string"),
                    ).otherwise(F.col("C.TrackingKey")),
                    F.lit("~"),
                )
                == F.col("E.TrackingMatch")
            ),
        )
        .join(
            sm_input.alias("AI"),
            (F.col("E.UnderlyingEntityId") == F.col("AI.EntityID"))
            & (F.col("E.LineTypeID") == F.col("AI.LineTypeID"))
            & (F.col("AI.StateID") == F.col("E.StateID"))
            & (F.col("AI.StateLineID") == F.col("E.LineID")),
        )
        .join(
            state_map_dar.alias("M2"),
            (F.col("C.AllocationTypeID") == F.col("M2.RuleID"))
            & (
                F.when(F.col("M2.StateID") == -1, F.lit(1))
                .otherwise(F.col("M2.StateID"))
                == F.when(F.col("M2.StateID") == -1, F.lit(1))
                .otherwise(F.col("AI.StateID"))
            )
            & (
                F.when(F.col("M2.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("M2.SelectedMappingID"))
                == F.when(F.col("M2.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("AI.StateLineID"))
            )
            & (F.col("E.IsExcludefromTransfer") == F.col("M2.ExcludeFromTransfers").cast("boolean")),
        )
        .filter(
            (F.col("AI.LineTypeID") == F.col("M2.SourceID"))
            & (F.coalesce(F.col("C.AllocatedAmount"), F.lit(0)) != 0)
            & (F.col("E.AllocationBy") == "AMOUNT")
        )
        .select(
            F.col("E.UnderlyingEntityId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.coalesce(F.col("C.CommitmentPercent"), F.lit(0)).alias("CommitmentPercent"),
            F.coalesce(F.col("C.AllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("AllocationTypeId"),
            F.coalesce(F.col("AI.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("AI.Tag"), F.lit("")).alias("Tag"),
            F.col("AI.StateLineID").alias("LineID"),
            F.coalesce(F.col("AI.Amount"), F.lit(0)).alias("InputAmount"),
            F.coalesce(F.col("C.AllocatedAmount"), F.lit(0)).alias("AllocatedAmount"),
            F.col("C.InvestmentID").alias("CostEntityId"),
            F.coalesce(F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")).alias("UnderlyingTypeId"),
            F.col("AI.LineTypeID"),
            F.col("M2.ExcludeFromTransfers").cast("boolean").alias("ExcludeFromTransfers"),
        )
    )

    # Total amounts per group
    sm_total_amounts = (
        sm_entity_amounts
        .groupBy("LineID", "PartnerNumber", "CostEntityId", "AllocationTypeId",
                 "TrackingKey", "Tag", "LineTypeID")
        .agg(F.sum(F.coalesce(F.col("InputAmount"), F.lit(0))).alias("TotalAmount"))
    )

    # Final effective amounts
    sm_eff_amounts = (
        sm_total_amounts.alias("T")
        .join(
            sm_entity_amounts.alias("C"),
            (F.col("C.CostEntityId") == F.col("T.CostEntityId"))
            & (F.col("C.AllocationTypeId") == F.col("T.AllocationTypeId"))
            & (F.col("C.TrackingKey") == F.col("T.TrackingKey"))
            & (F.col("C.Tag") == F.col("T.Tag"))
            & (F.col("C.LineID") == F.col("T.LineID"))
            & (F.col("C.PartnerNumber") == F.col("T.PartnerNumber"))
            & (F.col("T.LineTypeID") == F.col("C.LineTypeID")),
        )
        .select(
            F.col("C.UnderlyingEntityId").alias("InvestmentID"),
            F.col("C.PartnerNumber"),
            F.when(
                F.col("C.ExcludeFromTransfers") == True,
                F.lit("Cost without Transfer Adj %"),
            ).otherwise(F.lit("Cost")).alias("AllocationType"),
            F.col("C.Quarter"),
            F.col("C.AllocationTypeId").alias("TypeId"),
            F.col("C.TrackingKey"),
            F.col("C.Tag"),
            F.col("C.LineID").alias("LineId"),
            F.when(
                F.col("T.TotalAmount") != 0,
                F.try_divide(F.col("C.InputAmount"), F.col("T.TotalAmount")) * F.col("C.AllocatedAmount"),
            ).otherwise(F.lit(0)).alias("EffectiveAmount"),
            F.col("C.UnderlyingTypeId"),
            F.col("C.LineTypeID").alias("LineTypeId"),
            F.col("C.ExcludeFromTransfers").cast("boolean").alias("IsExcludefromTransfer"),
        )
    )

    # --- State input lines (4-pass insert-delete pattern) ---
    # SQL lines 5960-6140
    sm_state_lines = _tbl(spark, "SM_StateLines", cfg)

    # Pass 1: Both StateLineID != -1 AND StateID != -1
    pass1 = (
        sm_input.alias("I")
        .join(sm_state_lines.alias("K"),
              (F.col("I.StateID") == F.col("K.StateID"))
              & (F.col("I.StateLineID") == F.col("K.StateFieldID")))
        .join(
            sm_book_eff.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("I.StateLineID") == F.col("B.StateLineID"))
            & (F.col("B.StateID") == F.col("I.StateID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
        )
        .filter(
            (F.coalesce(F.col("B.StateLineID"), F.lit(-1)) != -1)
            & (F.coalesce(F.col("B.StateID"), F.lit(-1)) != -1)
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.StateLineID").alias("LineID"),
            F.col("I.StateID").alias("StateId"),
            F.coalesce(F.col("B.AdjustmentAllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("I.Tag"), F.lit("")).alias("Tag"),
            F.lit(None).cast("boolean").alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # Remove pass 1 matched from sm_input
    sm_remaining_1 = sm_input.join(
        pass1,
        (sm_input["EntityID"] == pass1["UnderlyingEntityID"])
        & (sm_input["StateLineID"] == pass1["LineID"])
        & (sm_input["StateID"] == pass1["StateId"]),
        "left_anti",
    )

    # Pass 2: StateLineID = -1, StateID != -1
    pass2 = (
        sm_remaining_1.alias("I")
        .join(sm_state_lines.alias("K"), F.col("I.StateID") == F.col("K.StateID"))
        .join(
            sm_book_eff.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("B.StateID") == F.col("I.StateID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
        )
        .filter(
            (F.coalesce(F.col("B.StateLineID"), F.lit(-1)) == -1)
            & (F.coalesce(F.col("B.StateID"), F.lit(-1)) != -1)
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.StateLineID").alias("LineID"),
            F.col("I.StateID").alias("StateId"),
            F.coalesce(F.col("B.AdjustmentAllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("I.Tag"), F.lit("")).alias("Tag"),
            F.lit(False).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    sm_remaining_2 = sm_remaining_1.join(
        pass2,
        (sm_remaining_1["EntityID"] == pass2["UnderlyingEntityID"])
        & (sm_remaining_1["StateLineID"] == pass2["LineID"])
        & (sm_remaining_1["StateID"] == pass2["StateId"]),
        "left_anti",
    )

    # Pass 3: StateLineID != -1, StateID = -1
    pass3 = (
        sm_remaining_2.alias("I")
        .join(sm_state_lines.alias("K"), F.col("I.StateID") == F.col("K.StateID"))
        .join(
            sm_book_eff.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (F.col("B.StateLineID") == F.col("I.StateLineID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
        )
        .filter(
            (F.coalesce(F.col("B.StateLineID"), F.lit(-1)) != -1)
            & (F.coalesce(F.col("B.StateID"), F.lit(-1)) == -1)
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.StateLineID").alias("LineID"),
            F.col("I.StateID").alias("StateId"),
            F.coalesce(F.col("B.AdjustmentAllocationTypeID"), F.lit(cost_alloc_type_id).cast("int")).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("I.Tag"), F.lit("")).alias("Tag"),
            F.lit(False).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    sm_remaining_3 = sm_remaining_2.join(
        pass3,
        (sm_remaining_2["EntityID"] == pass3["UnderlyingEntityID"])
        & (sm_remaining_2["StateLineID"] == pass3["LineID"])
        & (sm_remaining_2["StateID"] == pass3["StateId"]),
        "left_anti",
    )

    # Pass 4: Remaining — LEFT JOIN fallback
    pass4 = (
        sm_remaining_3.alias("I")
        .join(
            sm_state_lines.alias("K"),
            (F.col("I.StateID") == F.col("K.StateID"))
            & (F.col("I.StateLineID") == F.col("K.StateFieldID")),
        )
        .join(
            sm_book_eff.alias("B"),
            (F.col("I.EntityID") == F.col("B.UnderlyingEntityID"))
            & (_match_key(F.col("B.TrackingKey")) == _match_key(F.col("I.TrackingKey")))
            & (_match_key(F.col("B.Tag")) == _match_key(F.col("I.Tag"))),
            "left",
        )
        .join(
            updated_all_underlyings.alias("AI"),
            (F.col("I.EntityID") == F.col("AI.UnderlyingEntityId"))
            & (F.col("I.StateID") == F.col("AI.StateID"))
            & (F.col("I.StateLineID") == F.col("AI.LineID"))
            & (F.col("AI.AllocationBy") == "PERCENT"),
            "left",
        )
        .select(
            F.col("I.EntityID").alias("UnderlyingEntityID"),
            F.col("I.StateLineID").alias("LineID"),
            F.col("I.StateID").alias("StateId"),
            F.coalesce(
                F.col("B.AdjustmentAllocationTypeID"),
                F.coalesce(F.col("AI.AllocationTypeId"), F.lit(cost_alloc_type_id).cast("int")),
            ).alias("TypeID"),
            F.coalesce(F.col("B.TrackingKey"), F.coalesce(F.col("I.TrackingKey"), F.lit(""))).alias("TrackingKey"),
            F.coalesce(F.col("I.Tag"), F.lit("")).alias("Tag"),
            F.coalesce(F.col("AI.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
        )
        .distinct()
    )

    # Union all passes
    state_input_lines = (
        pass1
        .unionByName(pass2, allowMissingColumns=True)
        .unionByName(pass3, allowMissingColumns=True)
        .unionByName(pass4, allowMissingColumns=True)
    )

    _log_timing("build_state_allocation_input", t0)
    return updated_all_underlyings, state_input_lines, sm_eff_amounts


# ---------------------------------------------------------------------------
# build_state_entities
# SQL lines: 6150-6210
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_state_entities(
    spark: SparkSession, cfg: dict,
    state_input_lines: DataFrame,
    non_dated: DataFrame,
    dated: DataFrame,
) -> tuple:
    """Build state non-dated/dated entities from SM_StateLines.

    Non-dated: SM_StateLines WHERE TransactionDate IS NULL.
    Dated: PE Book → QuarterDates, Standard → QuarterMonth.

    Returns: (updated_non_dated, updated_dated)
    """
    t0 = time.time()
    logger.info("[SECTION] build_state_entities")

    if state_input_lines is None:
        return non_dated, dated

    alloc_type_name = cfg.get("allocation_type_name", "")
    is_dated_transfers = cfg.get("is_dated_transfers_configured", "")
    is_pe_book_dated = (alloc_type_name == "PE Book Allocation" and is_dated_transfers == "C")

    sm_state_lines = _tbl(spark, "SM_StateLines", cfg)

    # State non-dated: TransactionDate IS NULL
    state_non_dated = (
        state_input_lines.alias("L")
        .join(
            sm_state_lines.alias("K"),
            (F.col("K.StateFieldID") == F.col("L.LineID"))
            & (F.col("L.StateId") == F.col("K.StateID"))
            & (F.col("K.TransactionDate").isNull()),
        )
        .select(
            F.col("L.UnderlyingEntityID"),
            F.col("L.TypeID"),
            F.col("L.TrackingKey"),
            F.col("L.Tag"),
            F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
            F.col("K.StateID"),
        )
        .distinct()
    )

    non_dated_updated = non_dated.unionByName(state_non_dated, allowMissingColumns=True)

    # State dated
    if is_pe_book_dated:
        state_dated = (
            state_input_lines.alias("L")
            .join(
                sm_state_lines.alias("K"),
                (F.col("K.StateFieldID") == F.col("L.LineID"))
                & (F.col("L.StateId") == F.col("K.StateID")),
            )
            .join(
                _tbl(spark, "QuarterDates", cfg).alias("D"),
                F.coalesce(F.col("K.TransactionDate"), F.lit("1900-01-01").cast("timestamp"))
                .between(F.col("D.StartDate"), F.col("D.EndDate")),
            )
            .filter(F.col("K.TransactionDate").isNotNull())
            .select(
                F.coalesce(F.col("D.Quarter"), F.lit("Q0")).alias("Quarter"),
                F.col("L.UnderlyingEntityID"),
                F.col("L.TypeID"),
                F.col("L.TrackingKey"),
                F.col("L.Tag"),
                F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
                F.col("L.LineID"),
                F.col("D.Preference"),
            )
            .distinct()
        )
    else:
        state_dated = (
            state_input_lines.alias("L")
            .join(
                sm_state_lines.alias("K"),
                (F.col("K.StateFieldID") == F.col("L.LineID"))
                & (F.col("L.StateId") == F.col("K.StateID")),
            )
            .join(
                F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg)).alias("D"),
                (F.col("D.LookUpValue") == F.coalesce(
                    F.month(F.col("K.TransactionDate")), F.lit(-1),
                ).cast("string"))
                & (F.col("D.Category") == "QuarterMonth"),
            )
            .select(
                F.col("D.LookUpData").alias("Quarter"),
                F.col("L.UnderlyingEntityID"),
                F.col("L.TypeID"),
                F.col("L.TrackingKey"),
                F.col("L.Tag"),
                F.coalesce(F.col("L.IsExcludefromTransfer"), F.lit(False)).alias("IsExcludefromTransfer"),
                F.col("L.LineID"),
                F.lit(None).cast("int").alias("Preference"),
            )
            .distinct()
        )

    dated_updated = dated.unionByName(state_dated, allowMissingColumns=True)

    _log_timing("build_state_entities", t0)
    return non_dated_updated, dated_updated
