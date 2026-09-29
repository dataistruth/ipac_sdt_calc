"""K1 and UBTI amount aggregation for SM LookThrough Effective Allocation Percentage.

Functions:
    build_k1_amounts             — SQL lines 1343-1503: K1 sidepocket amounts
    build_ubti_amounts           — SQL lines 1505-1565: UBTI amounts
    compute_state_mapped_amounts — SQL lines 1565-1780: Map to state lines + totals
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0, table_prefix
from Common_V2.core.checkpoint_V2 import checkpoint_V2 as _checkpoint_v2_impl

def checkpoint(spark, df, name, cfg):
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = _checkpoint_v2_impl(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    return result

from Common_V2.core.observability import log_section, log_timing
from Common_V2.core.assertions import warn_if_empty

logger = logging.getLogger(__name__)


def _build_sm_lt_input_and_state_lines(
    spark: SparkSession, cfg: dict, mappings: dict,
) -> tuple:
    """Build #SM_LookThroughAllocationInput (aggregated, Amount<>0) and
    #StateLines, #FedLines, #NonSidepocketedFP used by K1/UBTI amount functions.

    Converted from: SQL lines 1343-1400.
    """
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    state_mapped_lines = mappings["state_mapped_lines"]
    distinct_mappings = mappings["distinct_mappings"]
    distinct_ubti_mappings = mappings["distinct_ubti_mappings"]
    k1_line_type = cfg["k1_line_type"]
    ubti_line_type = cfg["ubti_line_type"]

    # PERF L3: Read once with column prune + common RunID/ClientID filter.
    # No checkpoint — sm_lt_input (below) already checkpoints the grouped
    # result. non_sp_fp stays lazy; its lineage (re-read source with
    # predicate pushdown) evaluates inside the effective_amounts checkpoint
    # job. Eliminates 1 Spark job (~2-3s fixed overhead for 415 records);
    # for 1M the extra source scan is offset by removing the job.
    sm_lt_filtered = (
        read_table(spark, "SM_LookThroughAllocationInput", cfg)
        .select(
            "RunID", "ClientID", "EntityID", "LineTypeID",
            "StateID", "StateLineID", "ParentEntityID",
            "SuperParentEntityID", "TrackingKey", "OriginalParentEntityID",
            "FlowUpPartner", "Amount",
        )
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
        )
    )
    # PERF L7: checkpoint — 2 consumers (non_sp_fp, sm_lt_input)
    # Cuts lineage at source read, prevents double table scan + reduces Catalyst planning
    sm_lt_filtered = checkpoint(spark, sm_lt_filtered, "sm_lt_filtered", cfg)

    entity_id = cfg["entity_id"]

    # ── S7-line 1357: #NonSidepocketedFP ──
    # PERF L3: pre-compute _join_tk for equi-join in K1/UBTI anti-joins
    non_sp_fp = (
        sm_lt_filtered
        .filter(F.col("FlowUpPartner").isNotNull())
        .select(
            "RunID", "ClientID", "EntityID", "LineTypeID",
            "StateID", "StateLineID", "ParentEntityID",
            "SuperParentEntityID", "TrackingKey",
        )
        .distinct()
        .withColumn(
            "_join_tk",
            F.concat(F.col("TrackingKey"), F.lit("~"), F.lit(str(entity_id)))
        )
    )

    sml_distinct = F.broadcast(state_mapped_lines.select("StateFieldID", "StateID").distinct())

    # ── S7-line 1365: #SM_LookThroughAllocationInput (aggregated, Amount<>0) ──
    # PERF L3: sm_lt_filtered already has RunID/ClientID filter applied
    sm_lt_input = (
        sm_lt_filtered.filter(F.col("Amount") != 0).alias("S")
        .join(
            sml_distinct.alias("SML"),
            (F.col("SML.StateFieldID") == F.col("S.StateLineID"))
            & (F.col("SML.StateID") == F.col("S.StateID")),
        )
        .groupBy(
            F.col("S.EntityID"), F.col("S.StateID"),
            F.col("S.StateLineID"), F.col("S.LineTypeID"),
            F.col("S.ParentEntityID"), F.col("S.SuperParentEntityID"),
            F.col("S.TrackingKey"), F.col("S.OriginalParentEntityID"),
        )
        .agg(F.sum("Amount").alias("StateAmount"))
    )
    # PERF L7: checkpoint — 2 consumers (state_lines, S10 join)
    # Cuts lineage after groupBy aggregation, prevents recomputation + reduces Catalyst planning
    sm_lt_input = checkpoint(spark, sm_lt_input, "sm_lt_input", cfg)

    # ── S7-line 1375: #StateLines ──
    state_lines = sm_lt_input.select("StateID", "StateLineID", "LineTypeID").distinct()

    # ── S7-line 1378: #FedLines ──
    fed_lines_k1 = (
        F.broadcast(distinct_mappings).alias("D")
        .join(
            state_lines.alias("SL1"),
            F.col("D.StateFieldID") == F.col("SL1.StateLineID"),
        )
        .select(F.col("D.RegisterLineID"))
    )
    fed_lines_ubti = (
        F.broadcast(distinct_ubti_mappings).alias("D2")
        .join(
            state_lines.alias("SL2"),
            F.col("D2.StateFieldID") == F.col("SL2.StateLineID"),
        )
        .select(F.col("D2.RegisterLineID"))
    )
    fed_lines = fed_lines_k1.unionByName(fed_lines_ubti).distinct()

    # ── S7-line 1390: Prune DistinctMappings/DistinctUBTIMappings to only active StateLines ──
    pruned_dm = (
        F.broadcast(distinct_mappings).alias("DM")
        .join(
            state_lines.filter(F.col("LineTypeID") == k1_line_type).alias("SLK"),
            F.col("DM.StateFieldID") == F.col("SLK.StateLineID"),
        )
        .select(
            F.col("DM.StateID"), F.col("DM.StateFieldID"),
            F.col("DM.RegisterLineID"), F.col("DM.FieldSourceID"),
            F.col("DM.MapLineSubType"), F.col("DM.OperationType"),
            F.col("DM.SourceTypeID"),
        )
    )
    pruned_ubti_dm = (
        F.broadcast(distinct_ubti_mappings).alias("DU")
        .join(
            state_lines.filter(F.col("LineTypeID") == ubti_line_type).alias("SLU"),
            F.col("DU.StateFieldID") == F.col("SLU.StateLineID"),
        )
        .select(
            F.col("DU.StateID"), F.col("DU.StateFieldID"),
            F.col("DU.RegisterLineID"), F.col("DU.FieldSourceID"),
            F.col("DU.MapLineSubType"), F.col("DU.OperationType"),
            F.col("DU.SourceTypeID"),
        )
    )

    return sm_lt_input, state_lines, fed_lines, non_sp_fp, pruned_dm, pruned_ubti_dm


def build_k1_amounts(
    spark: SparkSession, cfg: dict, mappings: dict,
    k1_sp_detail: DataFrame, k1_sp_residual_detail: DataFrame,
    fed_lines: DataFrame, non_sp_fp: DataFrame,
) -> tuple:
    """Build K1 amounts from sidepocket/residual detail, filtered to mapped federal lines.

    Converted from: SQL lines 1403-1503.
    Row count: ALWAYS-NON-EMPTY — core pipeline.

    Returns:
        (partner_alloc_amount, total_input_amount)
    """
    log_section("build_k1_amounts")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    k1_line_type = cfg["k1_line_type"]

    # PERF L3: K1 detail tables already filtered by RunID/ClientID/TaxPeriodID
    # at source (_get_k1_alloc_data). Column prune to what build_k1_amounts needs.
    k1_res_cols = ["EntityID", "LineID", "TrackingKey", "Amount", "PartnerNumber"]
    k1_sp_cols = k1_res_cols  # same columns needed

    # PERF L3: Ensure _join_tk exists on non_sp_fp (pre-computed in _build_sm_lt_input_and_state_lines)
    if "_join_tk" not in non_sp_fp.columns:
        non_sp_fp = non_sp_fp.withColumn(
            "_join_tk",
            F.concat(F.col("TrackingKey"), F.lit("~"), F.lit(str(entity_id)))
        )

    # ── S7-line 1410: #K1Amount from residual (AllocType=NULL) ──
    k1_res = (
        k1_sp_residual_detail
        .select(*k1_res_cols)
        .alias("KAS")
        .join(F.broadcast(fed_lines).alias("FL"), F.col("KAS.LineID") == F.col("FL.RegisterLineID"))
        .select(
            F.col("KAS.EntityID"), F.col("KAS.LineID"),
            F.col("KAS.TrackingKey"),
            F.coalesce(F.col("KAS.Amount"), F.lit(0.0)).alias("Amount"),
            F.col("KAS.PartnerNumber"),
            F.lit(None).cast("string").alias("AllocType"),
        )
    )

    # ── S7-line 1420: #K1Amount from sidepocket (AllocType='SidePocket') ──
    # LEFT JOIN #NonSidepocketedFP to exclude already-processed flow-up partners
    # PERF L3: use pre-computed _join_tk for equi-join instead of inline F.concat
    k1_sp = (
        k1_sp_detail
        .select(*k1_sp_cols)
        .alias("KAS2")
        .join(F.broadcast(fed_lines).alias("FL2"), F.col("KAS2.LineID") == F.col("FL2.RegisterLineID"))
        .join(
            non_sp_fp.alias("NFP"),
            (F.col("KAS2.EntityID") == F.col("NFP.EntityID"))
            & (F.col("KAS2.TrackingKey") == F.col("NFP._join_tk"))
            & (F.col("NFP.LineTypeID") == k1_line_type),
            "left_anti",
        )
        .select(
            F.col("KAS2.EntityID"), F.col("KAS2.LineID"),
            F.col("KAS2.TrackingKey"),
            F.coalesce(F.col("KAS2.Amount"), F.lit(0.0)).alias("Amount"),
            F.col("KAS2.PartnerNumber"),
            F.lit("SidePocket").alias("AllocType"),
        )
    )

    k1_amount = k1_res.unionByName(k1_sp)
    # PERF L7: checkpoint — 2 consumers (partner_alloc groupBy, total_input groupBy)
    # Cuts lineage after K1 detail table reads, prevents double scan
    k1_amount = checkpoint(spark, k1_amount, "k1_amount", cfg)

    # ── S7-line 1445: #TotalInputAmount ──
    # Sum all partners per LineID FIRST — matches SQL accumulation order
    # to prevent IEEE 754 floating-point precision drift.
    total_input = (
        k1_amount
        .groupBy("EntityID", "LineID", "TrackingKey")
        .agg(F.sum("Amount").alias("INPUTAmount"))
    )

    # ── S7-line 1450: #PartnerAllocAmount ──
    partner_alloc = (
        k1_amount
        .groupBy("EntityID", "PartnerNumber", "LineID", "TrackingKey", "AllocType")
        .agg(F.sum("Amount").alias("AllocAmount"))
    )

    log_timing("build_k1_amounts", t0)
    return partner_alloc, total_input


def build_ubti_amounts(
    spark: SparkSession, cfg: dict,
    fed_lines: DataFrame, non_sp_fp: DataFrame,
) -> tuple:
    """Build UBTI amounts from dedicated UBTI detail tables.

    Converted from: SQL lines 1455-1530.
    Row count: POSSIBLY-EMPTY — UBTI lines may not exist for this entity.

    Returns:
        (partner_alloc_ubti_amount, total_ubti_input_amount)
    """
    log_section("build_ubti_amounts")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    ubti_line_type = cfg["ubti_line_type"]

    # PERF L3: Column prune + filter BEFORE join for both UBTI detail tables
    _ubti_cols = ["RunID", "ClientID", "TaxPeriodID", "EntityID", "PartnerNumber",
                  "LineID", "UBTIType", "TrackingKey", "Amount"]

    # PERF L3: Ensure _join_tk exists on non_sp_fp
    if "_join_tk" not in non_sp_fp.columns:
        non_sp_fp = non_sp_fp.withColumn(
            "_join_tk",
            F.concat(F.col("TrackingKey"), F.lit("~"), F.lit(str(entity_id)))
        )

    # ── S8-line 1455: #UBTIAmount from residual (AllocType=NULL) ──
    ubti_res = (
        read_table(spark, "UBTILookThroughSidePocketResidualAllocationDetail", cfg)
        .select(*_ubti_cols)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .alias("KAS")
        .join(F.broadcast(fed_lines).alias("FL"), F.col("KAS.LineID") == F.col("FL.RegisterLineID"))
        .select(
            F.col("KAS.EntityID"), F.col("KAS.PartnerNumber"),
            F.col("KAS.LineID"), F.col("KAS.UBTIType"),
            F.col("KAS.TrackingKey"), F.col("KAS.Amount"),
            F.lit(None).cast("string").alias("AllocType"),
        )
    )

    # ── S8-line 1470: #UBTIAmount from sidepocket (AllocType='SidePocket') ──
    # PERF L3: filter before join, use pre-computed _join_tk for equi-join
    ubti_sp = (
        read_table(spark, "UBTILookThroughSidePocketAllocationDetail", cfg)
        .select(*_ubti_cols)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .alias("KAS2")
        .join(F.broadcast(fed_lines).alias("FL2"), F.col("KAS2.LineID") == F.col("FL2.RegisterLineID"))
        .join(
            non_sp_fp.alias("NFP"),
            (F.col("KAS2.EntityID") == F.col("NFP.EntityID"))
            & (F.col("KAS2.TrackingKey") == F.col("NFP._join_tk"))
            & (F.col("NFP.LineTypeID") == ubti_line_type),
            "left_anti",
        )
        .select(
            F.col("KAS2.EntityID"), F.col("KAS2.PartnerNumber"),
            F.col("KAS2.LineID"), F.col("KAS2.UBTIType"),
            F.col("KAS2.TrackingKey"), F.col("KAS2.Amount"),
            F.lit("SidePocket").alias("AllocType"),
        )
    )

    ubti_amount = ubti_res.unionByName(ubti_sp)

    # ── S8-line 1500: #TotalUBTIInputAmount ──
    total_ubti_input = (
        ubti_amount
        .groupBy("EntityID", "LineID", "UBTIType", "TrackingKey")
        .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("InputAmount"))
    )

    # ── S8-line 1510: #PartnerAllocUBTIAmount ──
    partner_alloc_ubti = (
        ubti_amount
        .groupBy("EntityID", "PartnerNumber", "LineID", "UBTIType", "TrackingKey", "AllocType")
        .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("AllocAmount"))
    )

    # PERF L3: Removed warn_if_empty(ubti_amount) — .isEmpty() forces costly
    # partial DAG evaluation. Warning deferred to after checkpoint.

    log_timing("build_ubti_amounts", t0)
    return partner_alloc_ubti, total_ubti_input


def compute_state_mapped_amounts(
    spark: SparkSession, cfg: dict,
    pruned_dm: DataFrame, pruned_ubti_dm: DataFrame,
    partner_alloc: DataFrame, total_input: DataFrame,
    partner_alloc_ubti: DataFrame, total_ubti_input: DataFrame,
) -> DataFrame:
    """Map K1/UBTI partner amounts to state lines and compute TotalAmounts.

    Converted from: SQL lines 1565-1780.
    Row count: ALWAYS-NON-EMPTY — core pipeline.

    Returns:
        total_amounts DataFrame with LineTypeID, EntityID, TrackingKey,
        StateID, StateFieldID, PartnerNumber, AllocAmount, InputAmount, AllocType.
    """
    log_section("compute_state_mapped_amounts")
    t0 = time.time()

    k1_line_type = cfg["k1_line_type"]
    ubti_line_type = cfg["ubti_line_type"]
    federal_ids = [
        x for x in [
            cfg["federal_amount_id"], cfg["federal_adj_id"],
            cfg["alloc_only_federal_amount_id"],
        ] if x is not None
    ]
    ubti_ids = [
        x for x in [
            cfg["federal_ubti_id"], cfg["federal_ubti_adj_id"],
            cfg["alloc_only_federal_ubti_id"],
        ] if x is not None
    ]

    # ── S9-line 1575: #Amounts (K1 partner alloc → state lines) ──
    amounts_k1 = (
        partner_alloc.alias("P")
        .join(
            F.broadcast(pruned_dm).alias("M"),
            F.col("M.RegisterLineID") == F.col("P.LineID"),
        )
        .filter(F.col("M.FieldSourceID").isin(federal_ids))
        .groupBy(
            F.col("P.EntityID"), F.col("M.StateID").alias("StateID"),
            F.col("M.StateFieldID").alias("StateFieldID"),
            F.col("P.PartnerNumber"), F.col("P.TrackingKey"),
            F.col("P.AllocType"),
        )
        .agg(
            F.sum(
                F.when(F.col("M.OperationType") == "-", F.lit(-1) * F.col("P.AllocAmount"))
                .otherwise(F.col("P.AllocAmount"))
            ).alias("AllocAmount"),
        )
    )

    # ── S9-line 1600: #TotalMappedInputAmount (K1) ──
    # Uses total_input (sum of ALL partners per LineID) joined with mappings,
    # matching SQL's accumulation order to prevent floating-point drift.
    total_mapped_k1 = (
        total_input.alias("T")
        .join(
            F.broadcast(pruned_dm).alias("M2"),
            F.col("M2.RegisterLineID") == F.col("T.LineID"),
        )
        .filter(F.col("M2.FieldSourceID").isin(federal_ids))
        .groupBy(
            F.col("T.EntityID"),
            F.col("M2.StateID").alias("StateID"),
            F.col("M2.StateFieldID").alias("StateFieldID"),
            F.col("T.TrackingKey"),
        )
        .agg(
            F.sum(
                F.when(F.col("M2.OperationType") == "-", F.lit(-1) * F.col("T.INPUTAmount"))
                .otherwise(F.col("T.INPUTAmount"))
            ).alias("Amount"),
        )
    )

    # ── S9-line 1610: #TotalMappedUBTIInputAmount ──
    total_mapped_ubti = (
        total_ubti_input.alias("T3")
        .join(
            F.broadcast(pruned_ubti_dm).alias("M3"),
            (F.col("M3.RegisterLineID") == F.col("T3.LineID"))
            & (F.col("M3.MapLineSubType") == F.col("T3.UBTIType")),
        )
        .filter(F.col("M3.FieldSourceID").isin(ubti_ids))
        .groupBy(
            F.col("T3.EntityID"),
            F.col("M3.StateID").alias("StateID"),
            F.col("M3.StateFieldID").alias("StateFieldID"),
            F.col("T3.TrackingKey"),
        )
        .agg(
            F.sum(
                F.when(F.col("M3.OperationType") == "-", F.lit(-1) * F.col("T3.InputAmount"))
                .otherwise(F.col("T3.InputAmount"))
            ).alias("Amount"),
        )
    )

    # ── S9-line 1625: #FinalAmounts = Amounts JOIN TotalMappedInput ──
    # PERF L2: broadcast total_mapped_k1 (aggregated without PartnerNumber, much smaller)
    final_amounts = (
        amounts_k1.alias("A")
        .join(
            F.broadcast(total_mapped_k1).alias("TM"),
            (F.col("A.StateFieldID") == F.col("TM.StateFieldID"))
            & (F.col("A.StateID") == F.col("TM.StateID"))
            & (F.col("A.EntityID") == F.col("TM.EntityID"))
            & (F.col("A.TrackingKey") == F.col("TM.TrackingKey")),
        )
        .select(
            F.col("A.EntityID"), F.col("A.StateID"), F.col("A.StateFieldID"),
            F.col("A.PartnerNumber"), F.col("A.TrackingKey"),
            F.col("A.AllocAmount"), F.col("A.AllocType"),
            F.col("TM.Amount").alias("InputAmount"),
        )
    )

    # ── S9-line 1650: #UBTIAmounts (K1 partner → UBTI state lines) ──
    ubti_amounts_from_k1 = (
        partner_alloc.alias("P4")
        .join(
            F.broadcast(pruned_ubti_dm).alias("M4"),
            F.col("M4.RegisterLineID") == F.col("P4.LineID"),
        )
        .filter(F.col("M4.FieldSourceID").isin(ubti_ids))
        .groupBy(
            F.col("P4.EntityID"),
            F.col("M4.StateID").alias("StateID"),
            F.col("M4.StateFieldID").alias("StateFieldID"),
            F.col("P4.PartnerNumber"), F.col("P4.TrackingKey"),
        )
        .agg(
            F.sum(
                F.when(F.col("M4.OperationType") == "-", F.lit(-1) * F.col("P4.AllocAmount"))
                .otherwise(F.col("P4.AllocAmount"))
            ).alias("AllocAmount"),
        )
    )

    # SQL inserts sum of #UBTIAmounts into #TotalMappedUBTIInputAmount
    # then inserts UBTI partner alloc amounts into #UBTIAmounts
    # We need to combine both sources for UBTI

    # ── S9-line 1700: #UBTIAmounts from PartnerAllocUBTIAmount ──
    ubti_amounts_from_ubti = (
        partner_alloc_ubti.alias("P5")
        .join(
            F.broadcast(pruned_ubti_dm).alias("M5"),
            (F.col("M5.RegisterLineID") == F.col("P5.LineID"))
            & (F.col("M5.MapLineSubType") == F.col("P5.UBTIType")),
        )
        .filter(F.col("M5.FieldSourceID").isin(ubti_ids))
        .groupBy(
            F.col("P5.EntityID"),
            F.col("M5.StateID").alias("StateID"),
            F.col("M5.StateFieldID").alias("StateFieldID"),
            F.col("P5.PartnerNumber"), F.col("P5.TrackingKey"),
        )
        .agg(
            F.sum(
                F.when(F.col("M5.OperationType") == "-", F.lit(-1) * F.col("P5.AllocAmount"))
                .otherwise(F.col("P5.AllocAmount"))
            ).alias("AllocAmount"),
        )
    )

    # Union both UBTI sources
    all_ubti_amounts = (
        ubti_amounts_from_k1
        .withColumn("AllocType", F.lit(None).cast("string"))
        .unionByName(
            ubti_amounts_from_ubti
            .withColumn("AllocType", F.lit(None).cast("string"))
        )
    )

    # Add total_mapped_ubti input from ubti_amounts_from_k1 sum
    extra_mapped_ubti = (
        ubti_amounts_from_k1
        .groupBy("EntityID", "StateID", "StateFieldID", "TrackingKey")
        .agg(F.sum("AllocAmount").alias("Amount"))
    )
    combined_mapped_ubti = total_mapped_ubti.unionByName(extra_mapped_ubti)

    # ── S9-line 1750: #FinalUBTIAmounts = UBTIAmounts JOIN TotalMappedUBTIInput ──
    # CONVERTING TO DECIMAL to prevent exponential values
    # PERF L2: broadcast combined_mapped_ubti (aggregated without PartnerNumber, much smaller)
    final_ubti = (
        all_ubti_amounts.alias("UA")
        .join(
            F.broadcast(combined_mapped_ubti).alias("TU"),
            (F.col("UA.StateFieldID") == F.col("TU.StateFieldID"))
            & (F.col("UA.StateID") == F.col("TU.StateID"))
            & (F.col("UA.EntityID") == F.col("TU.EntityID"))
            & (F.col("UA.TrackingKey") == F.col("TU.TrackingKey")),
        )
        .select(
            F.col("UA.EntityID"), F.col("UA.StateID"), F.col("UA.StateFieldID"),
            F.col("UA.PartnerNumber"), F.col("UA.TrackingKey"),
            F.col("UA.AllocAmount"), F.col("UA.AllocType"),
            F.col("TU.Amount").cast("decimal(24,8)").alias("InputAmount"),
        )
    )

    # ── S9-line 1776: #TotalAmounts = K1 + UBTI ──
    # PERF L6: total_k1 groupBy is redundant — final_amounts is already at the
    # correct granularity from amounts_k1's groupBy. Eliminating saves 1 shuffle.
    total_k1 = (
        final_amounts
        .select(
            "EntityID", "TrackingKey", "StateID", "StateFieldID",
            "PartnerNumber", "AllocType", "AllocAmount", "InputAmount",
        )
        .withColumn("LineTypeID", F.lit(k1_line_type))
    )

    total_ubti = (
        final_ubti
        .groupBy("EntityID", "TrackingKey", "StateID", "StateFieldID", "PartnerNumber", "AllocType")
        .agg(
            F.sum("AllocAmount").alias("AllocAmount"),
            F.sum("InputAmount").alias("InputAmount"),
        )
        .withColumn("LineTypeID", F.lit(ubti_line_type))
    )

    total_amounts = total_k1.unionByName(total_ubti)

    # PERF L3: Removed assert_non_empty(total_amounts) — .isEmpty() forces
    # full DAG evaluation (~195s). Assertion moved to after checkpoint.

    log_timing("compute_state_mapped_amounts", t0)
    return total_amounts
