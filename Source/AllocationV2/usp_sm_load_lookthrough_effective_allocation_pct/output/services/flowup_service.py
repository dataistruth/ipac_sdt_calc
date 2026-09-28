"""Flow-up partner pipeline for SM LookThrough Effective Allocation Percentage.

Functions:
    build_flowup_k1_amounts          — SQL lines 810-1039: K1 allocation data + aggregation
    compute_flowup_mapped_amounts    — SQL lines 1039-1163: Map to state lines + totals
    compute_flowup_effective_amounts — SQL lines 1163-1250: Effective % calculation
    write_flowup_allocation_output   — SQL lines 1250-1340: Write output + zero-out input
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0, table_prefix
from Common_V2.core.observability import log_section, log_timing
from Common_V2.core.assertions import warn_if_empty

logger = logging.getLogger(__name__)


def build_flowup_k1_amounts(spark: SparkSession, cfg: dict) -> dict:
    """Build K1 amounts for flow-up partners from sidepocket detail tables.

    Converted from: SQL lines 810-1039.
    Inlines udfGetK1AllocationData for LookthroughSidepocket / LookthroughSidepocketResidual.
    Also reads FlowUpPartnerK1LookThrough* tables for #FPK1Amount.

    Row count: POSSIBLY-EMPTY — only when flow-up partners exist.

    Returns dict with keys:
        fp_k1_amount       — DataFrame (#FPK1Amount)
        fp_total_input     — DataFrame (#FPTotalInputAmount)
        fp_partner_alloc   — DataFrame (#FPPartnerAllocAmount)
        k1_sidepocket      — DataFrame (#K1LookthroughSidePocketAllocationDetail)
        k1_sidepocket_res  — DataFrame (#K1LookthroughSidePocketResidualAllocationDetail)
    """
    log_section("build_flowup_k1_amounts")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]

    # ── S5-line 812/828: Read K1LookThroughCompleteAllocationDetail ONCE,
    #    split into SidePocket vs Residual via filter ──
    k1_base = (
        read_table(spark, "K1LookThroughCompleteAllocationDetail", cfg)
        .select(
            "RunID", "ClientID", "TaxPeriodID", "EntityID", "LineID",
            "Amount", "PartnerNumber", "TrackingKey", "AllocationType",
        )
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
    )

    _alloc_type_lower = F.lower(F.coalesce(F.col("AllocationType"), F.lit("")))

    # #K1LookthroughSidePocketAllocationDetail: WHERE AllocationType = 'SidePocket'
    k1_sp = k1_base.filter(_alloc_type_lower == "sidepocket")

    # #K1LookthroughSidePocketResidualAllocationDetail: WHERE AllocationType <> 'SidePocket'
    k1_sp_res = (
        k1_base
        .filter(_alloc_type_lower != "sidepocket")
        .withColumn("AllocationType", F.coalesce(F.col("AllocationType"), F.lit("ProRata")))
    )

    # ── S6-line 960: #FPK1Amount from FlowUpPartner...ResidualAllocationDetail ──
    fp_res = (
        read_table(spark, "FlowUpPartnerK1LookThroughSidePocketResidualAllocationDetail", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .groupBy("EntityID", "LineID", "SourceFlowUpPartner", "TrackingKey", "PartnerNumber")
        .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("Amount"))
        .withColumn("AllocType", F.lit(None).cast("string"))
    )

    # ── S6-line 972: #FPK1Amount from FlowUpPartner...SidePocketAllocationDetail ──
    fp_sp = (
        read_table(spark, "FlowUpPartnerK1LookThroughSidePocketAllocationDetail", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .groupBy("EntityID", "LineID", "SourceFlowUpPartner", "TrackingKey", "PartnerNumber")
        .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("Amount"))
        .withColumn("AllocType", F.lit("SidePocket"))
    )

    fp_k1_amount = fp_res.unionByName(fp_sp)

    # ── S6-line 980: #FPTotalInputAmount ──
    fp_total_input = (
        fp_k1_amount
        .groupBy("EntityID", "LineID", "SourceFlowUpPartner", "TrackingKey")
        .agg(F.sum(F.col("Amount")).alias("INPUTAmount"))
    )

    # ── S6-line 987: #FPPartnerAllocAmount ──
    fp_partner_alloc = (
        fp_k1_amount
        .groupBy("EntityID", "PartnerNumber", "LineID", "SourceFlowUpPartner", "TrackingKey", "AllocType")
        .agg(F.sum(F.col("Amount")).alias("AllocAmount"))
    )

    result = {
        "fp_k1_amount": fp_k1_amount,
        "fp_total_input": fp_total_input,
        "fp_partner_alloc": fp_partner_alloc,
        "k1_sidepocket": k1_sp,
        "k1_sidepocket_res": k1_sp_res,
    }

    log_timing("build_flowup_k1_amounts", t0)
    return result


def compute_flowup_mapped_amounts(
    spark: SparkSession, cfg: dict, fp_data: dict, mappings: dict,
) -> DataFrame:
    """Map flow-up K1 amounts to state lines and compute totals.

    Converted from: SQL lines 1039-1163.
    Row count: POSSIBLY-EMPTY — conditional on flow-up partner existence.

    Returns:
        fp_total_amounts DataFrame (#FPTotalAmounts)
    """
    log_section("compute_flowup_mapped_amounts")
    t0 = time.time()

    k1_line_type = cfg["k1_line_type"]
    ubti_line_type = cfg["ubti_line_type"]
    federal_ids = [
        x for x in [cfg["federal_amount_id"], cfg["federal_adj_id"], cfg["alloc_only_federal_amount_id"]]
        if x is not None
    ]
    ubti_ids = [
        x for x in [cfg["federal_ubti_id"], cfg["federal_ubti_adj_id"], cfg["alloc_only_federal_ubti_id"]]
        if x is not None
    ]

    fp_partner_alloc = fp_data["fp_partner_alloc"]
    fp_total_input = fp_data["fp_total_input"]
    distinct_mappings = mappings["distinct_mappings"]
    distinct_ubti_mappings = mappings["distinct_ubti_mappings"]

    # ── S6-line 1055: #FPAmounts K1 (Federal) ──
    fp_amounts_k1 = (
        fp_partner_alloc.alias("P")
        .join(
            F.broadcast(distinct_mappings).alias("M"),
            F.col("M.RegisterLineID") == F.col("P.LineID"),
        )
        .filter(F.col("M.FieldSourceID").isin(federal_ids))
        .groupBy(
            F.col("P.EntityID"), F.col("M.StateID"), F.col("M.StateFieldID"),
            F.col("P.PartnerNumber"), F.col("P.TrackingKey"),
            F.col("P.SourceFlowUpPartner"), F.col("P.AllocType"),
        )
        .agg(
            F.sum(
                F.when(F.col("M.OperationType") == "-", F.lit(-1) * F.col("P.AllocAmount"))
                .otherwise(F.col("P.AllocAmount"))
            ).alias("AllocAmount"),
        )
        .withColumn("LineTypeID", F.lit(k1_line_type))
    )

    # ── S6-line 1070: #FPTotalMappedInputAmount K1 (Federal) ──
    # Uses fp_total_input (sum of ALL partners per LineID) joined with mappings,
    # matching SQL's accumulation order to prevent floating-point drift.
    fp_total_mapped_k1 = (
        fp_total_input.alias("T")
        .join(
            F.broadcast(distinct_mappings).alias("M2"),
            F.col("M2.RegisterLineID") == F.col("T.LineID"),
        )
        .filter(F.col("M2.FieldSourceID").isin(federal_ids))
        .groupBy(
            F.col("T.EntityID"), F.col("M2.StateID"), F.col("M2.StateFieldID"),
            F.col("T.TrackingKey"), F.col("T.SourceFlowUpPartner"),
        )
        .agg(
            F.sum(
                F.when(F.col("M2.OperationType") == "-", F.lit(-1) * F.col("T.INPUTAmount"))
                .otherwise(F.col("T.INPUTAmount"))
            ).alias("Amount"),
        )
        .withColumn("LineTypeID", F.lit(k1_line_type))
    )

    # ── S6-line 1082: #FPAmounts UBTI ──
    fp_amounts_ubti = (
        fp_partner_alloc.alias("P3")
        .join(
            F.broadcast(distinct_ubti_mappings).alias("M3"),
            F.col("M3.RegisterLineID") == F.col("P3.LineID"),
        )
        .filter(F.col("M3.FieldSourceID").isin(ubti_ids))
        .groupBy(
            F.col("P3.EntityID"), F.col("M3.StateID"), F.col("M3.StateFieldID"),
            F.col("P3.PartnerNumber"), F.col("P3.TrackingKey"),
            F.col("P3.SourceFlowUpPartner"), F.col("P3.AllocType"),
        )
        .agg(
            F.sum(
                F.when(F.col("M3.OperationType") == "-", F.lit(-1) * F.col("P3.AllocAmount"))
                .otherwise(F.col("P3.AllocAmount"))
            ).alias("AllocAmount"),
        )
        .withColumn("LineTypeID", F.lit(ubti_line_type))
    )

    # ── S6-line 1097: #FPTotalMappedInputAmount UBTI ──
    # Uses fp_total_input (sum of ALL partners per LineID) joined with mappings,
    # matching SQL's accumulation order to prevent floating-point drift.
    fp_total_mapped_ubti = (
        fp_total_input.alias("T4")
        .join(
            F.broadcast(distinct_ubti_mappings).alias("M4"),
            F.col("M4.RegisterLineID") == F.col("T4.LineID"),
        )
        .filter(F.col("M4.FieldSourceID").isin(ubti_ids))
        .groupBy(
            F.col("T4.EntityID"), F.col("M4.StateID"), F.col("M4.StateFieldID"),
            F.col("T4.TrackingKey"), F.col("T4.SourceFlowUpPartner"),
        )
        .agg(
            F.sum(
                F.when(F.col("M4.OperationType") == "-", F.lit(-1) * F.col("T4.INPUTAmount"))
                .otherwise(F.col("T4.INPUTAmount"))
            ).alias("Amount"),
        )
        .withColumn("LineTypeID", F.lit(ubti_line_type))
    )

    # Union K1 + UBTI
    fp_amounts = fp_amounts_k1.unionByName(fp_amounts_ubti)
    fp_total_mapped = fp_total_mapped_k1.unionByName(fp_total_mapped_ubti)

    # ── S6-line 1125: #FPAmountsTot = FPAmounts JOIN FPTotalMappedInputAmount ──
    fp_amounts_tot = (
        fp_amounts.alias("A")
        .join(
            fp_total_mapped.alias("TM"),
            (F.col("A.StateFieldID") == F.col("TM.StateFieldID"))
            & (F.col("A.StateID") == F.col("TM.StateID"))
            & (F.col("A.EntityID") == F.col("TM.EntityID"))
            & (F.col("A.TrackingKey") == F.col("TM.TrackingKey"))
            & (F.col("A.SourceFlowUpPartner") == F.col("TM.SourceFlowUpPartner"))
            & (F.col("A.LineTypeID") == F.col("TM.LineTypeID")),
        )
        .select(
            F.col("A.EntityID"), F.col("A.StateID"), F.col("A.StateFieldID"),
            F.col("A.PartnerNumber"), F.col("A.TrackingKey"),
            F.col("A.SourceFlowUpPartner"), F.col("A.AllocAmount"),
            F.col("A.AllocType"), F.col("A.LineTypeID"),
            F.col("TM.Amount").alias("InputAmount"),
        )
    )

    # ── S6-line 1140: #FPTotalAmounts = GROUP BY ──
    fp_total_amounts = (
        fp_amounts_tot
        .groupBy(
            "LineTypeID", "EntityID", "StateID", "StateFieldID",
            "PartnerNumber", "TrackingKey", "SourceFlowUpPartner", "AllocType",
        )
        .agg(
            F.sum("AllocAmount").alias("AllocAmount"),
            F.sum("InputAmount").alias("InputAmount"),
        )
    )

    # No warn_if_empty — avoids 1 unnecessary Spark action.

    log_timing("compute_flowup_mapped_amounts", t0)
    return fp_total_amounts


def compute_flowup_effective_amounts(
    spark: SparkSession, cfg: dict, fp_total_amounts: DataFrame,
    state_mapped_lines: DataFrame,
) -> DataFrame:
    """Compute effective amounts for flow-up partners.

    Converted from: SQL lines 1163-1250.
    Joins FPTotalAmounts with SM_LookThroughAllocationInput (where FlowUpPartner IS NOT NULL)
    to produce #FPEffectiveAmounts.

    Row count: POSSIBLY-EMPTY — conditional on flow-up partner data.
    """
    log_section("compute_flowup_effective_amounts")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    entity_id = cfg["entity_id"]

    # ── S6-line 1160: #FlowUpLookThroughAllocationInput ──
    sm_lt_input = read_table(spark, "SM_LookThroughAllocationInput", cfg)
    flowup_lt_input = (
        sm_lt_input.alias("S")
        .join(
            state_mapped_lines.select("StateFieldID", "StateID").distinct().alias("SML"),
            (F.col("SML.StateFieldID") == F.col("S.StateLineID"))
            & (F.col("SML.StateID") == F.col("S.StateID")),
        )
        .filter(
            (F.col("S.RunID") == run_id)
            & (F.col("S.ClientID") == client_id)
            & F.col("S.FlowUpPartner").isNotNull()
        )
        .groupBy(
            F.col("S.EntityID"), F.col("S.StateID"),
            F.col("S.StateLineID").alias("StateLineID"),
            F.col("S.LineTypeID"),
            F.col("S.ParentEntityID"), F.col("S.SuperParentEntityID"),
            F.col("S.TrackingKey"), F.col("S.FlowUpPartner"),
            F.col("S.OriginalParentEntityID"),
        )
        .agg(F.sum("Amount").alias("StateAmount"))
    )

    # ── S6-line 1180: #FPEffectiveAmounts ──
    # CASE WHEN InputAmount <> 0 THEN (AllocAmount/InputAmount) * StateAmount ELSE 0 END
    fp_effective = (
        fp_total_amounts.alias("A")
        .join(
            flowup_lt_input.alias("FI"),
            (F.col("A.StateID") == F.col("FI.StateID"))
            & (F.col("A.StateFieldID") == F.col("FI.StateLineID"))
            & (F.col("A.LineTypeID") == F.col("FI.LineTypeID"))
            & (F.col("A.EntityID") == F.col("FI.EntityID"))
            & (F.col("A.SourceFlowUpPartner") == F.col("FI.FlowUpPartner"))
            & (
                F.col("A.TrackingKey")
                == F.concat(F.col("FI.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))
            ),
        )
        .select(
            F.col("FI.EntityID"),
            F.col("A.StateID"),
            F.col("FI.LineTypeID"),
            F.col("A.StateFieldID"),
            F.col("A.PartnerNumber"),
            F.when(
                F.col("A.InputAmount") != 0,
                (F.col("A.AllocAmount") / F.col("A.InputAmount")) * F.col("FI.StateAmount"),
            ).otherwise(F.lit(0.0)).alias("EffectiveAmount"),
            F.col("FI.ParentEntityID"),
            F.col("FI.SuperParentEntityID"),
            F.col("FI.FlowUpPartner"),
            F.col("FI.TrackingKey"),
            F.col("A.AllocType"),
            F.col("FI.OriginalParentEntityID"),
        )
    )

    # No warn_if_empty — avoids 1 unnecessary Spark action.

    log_timing("compute_flowup_effective_amounts", t0)
    return fp_effective


def write_flowup_allocation_output(
    spark: SparkSession, cfg: dict, fp_effective: DataFrame,
) -> int:
    """Write flow-up partner effective amounts to SM_LookThroughAllocationOutput
    and zero-out consumed SM_LookThroughAllocationInput rows.

    Converted from: SQL lines 1250-1340.
    Row count: POSSIBLY-EMPTY — conditional write.

    Returns: number of rows written to output.
    """
    log_section("write_flowup_allocation_output")
    t0 = time.time()

    # No isEmpty() guard — write unconditionally. Empty INSERT is a no-op.
    # Avoids 1 unnecessary Spark action (~2-3s).

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    entity_id = cfg["entity_id"]
    fqn_output = f"{table_prefix(cfg)}.SM_LookThroughAllocationOutput"
    fqn_input = f"{table_prefix(cfg)}.SM_LookThroughAllocationInput"

    # ── Partner_Snapshot join for ShareClass ──
    # Note: Partner_Snapshot uses mixed casing: Clientid, TaxperiodID
    partner_snap = read_table(spark, "Partner_Snapshot", cfg)
    partner_txn_or_wf = cfg["partner_txn_or_wf_id"]

    ps = (
        partner_snap
        .filter(
            F.coalesce(F.col("WorkFlowID"), F.col("Transactionid")) == partner_txn_or_wf
        )
        .select("PartnerNumber", "EntityID", "Clientid", "TaxperiodID", "ShareClass")
    )

    # ── S6-line 1260: INSERT INTO SM_LookThroughAllocationOutput ──
    output_df = (
        fp_effective.alias("EA")
        .join(
            ps.alias("P"),
            (F.col("EA.PartnerNumber") == F.col("P.PartnerNumber"))
            & (F.col("P.EntityID") == entity_id)
            & (F.col("P.Clientid") == client_id)
            & (F.col("P.TaxperiodID") == cfg["tax_period_id"]),
        )
        .groupBy(
            F.col("EA.EntityID"), F.col("EA.PartnerNumber"), F.col("EA.LineTypeID"),
            F.col("EA.StateID"), F.col("EA.StateFieldID"),
            F.col("EA.ParentEntityID"), F.col("EA.SuperParentEntityID"),
            F.col("EA.TrackingKey"), F.col("EA.AllocType"),
            F.col("P.ShareClass"), F.col("EA.OriginalParentEntityID"),
            F.col("EA.FlowUpPartner"),
        )
        .agg(F.sum("EffectiveAmount").alias("Amount"))
    )

    # No isEmpty() guard — INSERT is a no-op on empty data.

    # Write via temp view → spark.sql INSERT
    view_name = f"_fp_alloc_output_{run_id}"
    output_df.createOrReplaceTempView(view_name)

    assert run_id is not None, "run_id must not be None for INSERT"
    assert client_id is not None, "client_id must not be None for INSERT"

    spark.sql(f"""
        INSERT INTO {fqn_output}
        (RunID, ClientID, EntityID, PartnerNumber, LineTypeID, StateID,
         StateLineID, Amount, AllocationType, ParentEntityID,
         SuperParentEntityID, TrackingKey, ShareClass,
         OriginalParentEntityID, FlowUpPartner, AdjustmentTypeID, Tag)
        SELECT
            {run_id}, {client_id}, EntityID, PartnerNumber, LineTypeID,
            StateID, StateFieldID, Amount, AllocType,
            ParentEntityID, SuperParentEntityID, TrackingKey,
            ShareClass, OriginalParentEntityID, FlowUpPartner,
            0, ''
        FROM {view_name}
    """)

    rows_written = -1  # PERF: skip post-INSERT .count()
    logger.info("[WRITE] SM_LookThroughAllocationOutput: flow-up INSERT complete")

    # ── S6-line 1310: UPDATE SM_LookThroughAllocationInput SET Amount = 0 ──
    # WHERE matching #EffectiveAmount rows
    eff_distinct = (
        fp_effective
        .select(
            F.coalesce(F.col("SuperParentEntityID"), F.lit(0)).alias("SuperParentEntityID"),
            "ParentEntityID", "EntityID", "StateID", "LineTypeID",
            "StateFieldID", "TrackingKey", "FlowUpPartner",
        )
        .distinct()
    )

    update_view = f"_fp_eff_amount_{run_id}"
    eff_distinct.createOrReplaceTempView(update_view)

    spark.sql(f"""
        UPDATE {fqn_input} AS AI
        SET Amount = 0
        WHERE AI.RunID = {run_id}
          AND EXISTS (
            SELECT 1 FROM {update_view} SA
            WHERE AI.EntityID = SA.EntityID
              AND AI.StateID = SA.StateID
              AND AI.StateLineID = SA.StateFieldID
              AND AI.LineTypeID = SA.LineTypeID
              AND COALESCE(AI.TrackingKey, '') = COALESCE(SA.TrackingKey, '')
              AND AI.FlowUpPartner = SA.FlowUpPartner
          )
    """)

    logger.info(f"[UPDATE] SM_LookThroughAllocationInput: zeroed flow-up consumed rows")

    # Clean up temp views
    spark.catalog.dropTempView(view_name)
    spark.catalog.dropTempView(update_view)

    log_timing("write_flowup_allocation_output", t0)
    return rows_written
