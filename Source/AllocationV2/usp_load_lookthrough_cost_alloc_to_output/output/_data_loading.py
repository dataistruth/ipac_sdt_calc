"""Data loading functions for lookthrough cost allocation.

Consolidates all data retrieval: partners, line items, input, rules,
cost percentages, book effective, footnote inheritance, and FEP.
"""

import logging
import time

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
)

from Common_V2.core.helpers import read_table, table_prefix, ns
from Common_V2.core.observability import log_section, log_timing
from Common_V2.core.assertions import warn_possibly_empty

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core data loading
# ---------------------------------------------------------------------------

def load_partners(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load partner list for the entity using partner workflow.
    SQL lines: Partner_Snapshot filtered by workflow.
    ALWAYS-NON-EMPTY for valid run.
    """
    log_section("load_partners")
    t0 = time.time()
    prefix = table_prefix(cfg)
    workflow_id = cfg["partner_workflow_id"]

    partners = spark.sql(f"""
        SELECT PartnerNumber, ShareClass
        FROM {prefix}.Partner_Snapshot
        WHERE WorkflowID = {workflow_id}
          AND ClientID = {cfg['client_id']}
          AND TaxPeriodID = {cfg['tax_period_id']}
          AND EntityID = {cfg['entity_id']}
    """)

    log_timing("load_partners", t0)
    return partners


def load_line_items(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load K1 and BoxJKL line items, excluding footnote-source lines.
    SQL lines: #K1lineItem DISTINCT → #LineItem with LEFT JOIN anti-pattern
               UNION ALL BoxjklLineItem.
    ALWAYS-NON-EMPTY for valid entity.
    """
    log_section("load_line_items")
    t0 = time.time()
    prefix = table_prefix(cfg)
    k1_lti = cfg["k1_line_type_id"]
    box_jkl_lti = cfg["box_jkl_line_type_id"]
    yearly_alloc_type_id = cfg["yearly_allocation_type_id"]

    # SQL: SELECT DISTINCT LineID, AllocationTypeRuleId, @K1LineTypeID as LinetypeId,
    #      TransactionDate, IsTransactionDate, IsTransfersAdjusted, Classification,
    #      CapitalGainLoss INTO #K1lineItem FROM K1LineItem K
    # Then: INSERT INTO #LineItem ... FROM #K1lineItem K
    #       LEFT JOIN MAP_SourceAttributeRelation M ON K.LineID = M.AttributeLineID
    #       WHERE M.SourceLineID IS NULL
    k1_lines = spark.sql(f"""
        SELECT DISTINCT K.LineID, K.AllocationTypeRuleId,
               {k1_lti} AS LineTypeID,
               K.TransactionDate, K.IsTransactionDate, K.IsTransfersAdjusted,
               K.Classification, K.CapitalGainLoss
        FROM {prefix}.K1LineItem K
        LEFT JOIN {prefix}.MAP_SourceAttributeRelation M
            ON K.LineID = M.AttributeLineID
        WHERE M.SourceLineID IS NULL
    """)

    # SQL: UNION ALL
    #      SELECT LineID, @YearlyAllocationTypeID, @BoxJKLLineTypeID, NULL,NULL,NULL,NULL,NULL
    #      FROM BoxjklLineItem
    box_jkl_lines = spark.sql(f"""
        SELECT LineID,
               {yearly_alloc_type_id} AS AllocationTypeRuleId,
               {box_jkl_lti} AS LineTypeID,
               CAST(NULL AS TIMESTAMP) AS TransactionDate,
               CAST(NULL AS BOOLEAN) AS IsTransactionDate,
               CAST(NULL AS BOOLEAN) AS IsTransfersAdjusted,
               CAST(NULL AS STRING) AS Classification,
               CAST(NULL AS STRING) AS CapitalGainLoss
        FROM {prefix}.BoxjklLineItem
    """)

    line_items = k1_lines.unionByName(box_jkl_lines)

    log_timing("load_line_items", t0)
    return line_items


def load_lookthrough_input(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load LookThroughAllocationInput filtered by RunID.
    SQL: SELECT * INTO #TempLookThroughAllocationInputDataLoad
         FROM LookThroughAllocationInput
         WHERE RunID = @LocalRunID
           AND (LineTypeID = @BoxJKLLineTypeID
                OR (LineTypeID IN (@K1LineTypeID, @AdjustmentLineTypeID)
                    AND Round(ISNULL(Amount,0),0) <> 0))
           AND ClientID = @LocalClientID
           AND LineTypeID IN (@K1LineTypeID, @AdjustmentLineTypeID, @BoxJKLLineTypeID)
    ALWAYS-NON-EMPTY for valid run.
    """
    log_section("load_lookthrough_input")
    t0 = time.time()
    prefix = table_prefix(cfg)

    k1_lti = cfg.get('k1_line_type_id')
    adj_lti = cfg.get('adjustment_line_type_id')
    jkl_lti = cfg.get('box_jkl_line_type_id')

    # Build line type filter — skip None values from failed lookups
    line_type_ids = [v for v in [k1_lti, adj_lti, jkl_lti] if v is not None]
    line_type_filter = ",".join(str(x) for x in line_type_ids)

    # K1/Adjustment line type IDs for the amount filter
    k1_adj_ids = [v for v in [k1_lti, adj_lti] if v is not None]
    k1_adj_filter = ",".join(str(x) for x in k1_adj_ids)

    # SQL: (LineTypeID = @BoxJKLLineTypeID
    #        OR (LineTypeID IN (@K1LineTypeID, @AdjustmentLineTypeID)
    #            AND Round(ISNULL(Amount,0),0) <> 0))
    jkl_clause = f"LineTypeID = {jkl_lti}" if jkl_lti is not None else "1=0"
    k1_adj_clause = (
        f"(LineTypeID IN ({k1_adj_filter}) AND Round(COALESCE(Amount, 0), 0) <> 0)"
        if k1_adj_ids else "1=0"
    )

    input_df = spark.sql(f"""
        SELECT RunID, ClientID, EntityID, LineTypeID, LineID, Amount,
               QuicklinkID, Amount704b, CategoryID, ParentEntityID,
               PeriodID, LineCode, SuperParentEntityID, AdjustmentTypeID,
               TrackingKey, Tag, OriginalParentEntityID
        FROM {prefix}.LookThroughAllocationInput
        WHERE RunID = {cfg['run_id']}
          AND ClientID = {cfg['client_id']}
          AND LineTypeID IN ({line_type_filter})
          AND ({jkl_clause} OR {k1_adj_clause})
    """)

    log_timing("load_lookthrough_input", t0)
    return input_df


def load_allocation_rules(spark: SparkSession, cfg: dict) -> dict:
    """Load default allocation rules and mapped rules.
    SQL lines: DefaultAllocationRuleSetup + MapDefaultAllocRuleToLineItem.
    Returns dict with 'default_rules', 'map_rules', 'entity_rules' DataFrames.
    POSSIBLY-EMPTY: entity rules may not exist.
    """
    log_section("load_allocation_rules")
    t0 = time.time()
    prefix = table_prefix(cfg)

    global_txn = cfg["dar_global_transaction_id"]
    entity_txn = cfg["dar_entity_transaction_id"]
    entity_rule_wf = cfg["entity_default_rule_override_workflow_id"]

    # Default allocation rule setup
    default_rules = spark.sql(f"""
        SELECT TransactionID, RuleID, AllocationPercentageTypeID,
               AllocationByID, UnderlyingTypeID, RuleTypeID,
               RuleGroupID, ClientID, TaxPeriodID, EntityID
        FROM {prefix}.DefaultAllocationRuleSetup
        WHERE TransactionID IN ({global_txn}, {entity_txn})
    """)

    # Mapped rules (including system-wide -2)
    map_rules = spark.sql(f"""
        SELECT TransactionID, SourceID, StateID, SelectedMappingID,
               RuleID, ExcludeFromTransfers, ClientID, TaxPeriodID, EntityID
        FROM {prefix}.MapDefaultAllocRuleToLineItem
        WHERE TransactionID IN ({global_txn}, {entity_txn}, -2)
    """)

    # Entity allocation rule overrides
    entity_rules = spark.createDataFrame([], "LineID: int, UpdatedAllocationRuleID: int")
    if entity_rule_wf and entity_rule_wf > 0:
        entity_rules = spark.sql(f"""
            SELECT LineID, UpdatedAllocationRuleID
            FROM {prefix}.EntityAllocationRule_Snapshot
            WHERE WorkflowID = {entity_rule_wf}
        """)
        warn_possibly_empty(entity_rules, "load_allocation_rules.entity_rules",
                            f"workflow_id={entity_rule_wf}")

    log_timing("load_allocation_rules", t0)
    return {
        "default_rules": default_rules,
        "map_rules": map_rules,
        "entity_rules": entity_rules,
    }


def _get_cost_percentage_details(spark: SparkSession, cfg: dict, workflow_id) -> DataFrame:
    """Inline replacement for dbo.udfGetCostPercentageDetails(@WorkFlowID).
    Returns the same result set as the SQL Server TVF.
    """
    prefix = table_prefix(cfg)

    # Load base tables
    cost_snap = spark.table(f"{prefix}.CostPercentage_Snapshot").filter(
        F.col("WorkFlowID") == workflow_id
    )
    entity = spark.table(f"{prefix}.Entity")
    enu_ut = spark.table(f"{prefix}.ENU_UnderlyingType")

    # Pre-filtered: InvestmentID = -2 and UnderlyingType <> 'ASSET CLASS'
    enu_ut_pf = enu_ut.select(
        F.col("UnderlyingTypeID").alias("pf_UnderlyingTypeID"),
        F.col("UnderlyingType").alias("pf_UnderlyingTypeName"),
    )
    pre_filtered = (
        cost_snap.filter(F.col("InvestmentID") == -2)
        .join(entity.select("EntityID"), "EntityID")
        .join(enu_ut_pf, cost_snap["UnderlyingType"] == F.col("pf_UnderlyingTypeID"))
        .filter(F.upper(F.col("pf_UnderlyingTypeName")) != "ASSET CLASS")
        .select(
            cost_snap["WorkFlowID"], cost_snap["TransactionID"], cost_snap["ClientID"],
            cost_snap["TaxPeriodID"], cost_snap["EntityID"].alias("EntityId"),
            cost_snap["InvestmentID"], cost_snap["PartnerNumber"], cost_snap["Quarter"],
            cost_snap["CommitmentPercent"], cost_snap["AllocationTypeID"].alias("AllocationTypeId"),
            F.coalesce(cost_snap["Tag"], F.lit("")).alias("Tag"),
            F.coalesce(cost_snap["TrackingKey"], F.lit("")).alias("TrackingKey"),
            cost_snap["UnderlyingType"].alias("Underlyingtype"),
            cost_snap["AllocatedAmount"], cost_snap["CostPercentageID"].alias("CostPercentageId"),
            cost_snap["DealID"],
        ).distinct()
    )

    # All records for the workflow (temp table)
    all_snap = (
        cost_snap
        .join(entity.select("EntityID"), "EntityID")
        .select(
            cost_snap["WorkFlowID"], cost_snap["TransactionID"], cost_snap["ClientID"],
            cost_snap["TaxPeriodID"], cost_snap["EntityID"].alias("EntityId"),
            cost_snap["InvestmentID"], cost_snap["PartnerNumber"], cost_snap["Quarter"],
            cost_snap["CommitmentPercent"], cost_snap["AllocationTypeID"].alias("AllocationTypeId"),
            F.coalesce(cost_snap["Tag"], F.lit("")).alias("Tag"),
            F.coalesce(cost_snap["TrackingKey"], F.lit("")).alias("TrackingKey"),
            cost_snap["UnderlyingType"].alias("Underlyingtype"),
            cost_snap["AllocatedAmount"], cost_snap["CostPercentageID"].alias("CostPercentageId"),
            cost_snap["DealID"],
        ).distinct()
    )

    # Part 1: InvestmentID = -1, not ASSET CLASS
    enu_ut_renamed = enu_ut.select(
        F.col("UnderlyingTypeID"),
        F.col("UnderlyingType").alias("UnderlyingTypeName"),
    )
    part1 = (
        all_snap.filter(F.col("InvestmentID") == -1)
        .join(enu_ut_renamed, F.col("Underlyingtype") == F.col("UnderlyingTypeID"))
        .filter(F.upper(F.col("UnderlyingTypeName")) != "ASSET CLASS")
        .select(
            "WorkFlowID", "TransactionID", "ClientID", "TaxPeriodID",
            "EntityId", "InvestmentID", "PartnerNumber", "Quarter",
            "CommitmentPercent", "AllocationTypeId", "Tag", "TrackingKey",
            "Underlyingtype", "AllocatedAmount", "CostPercentageId"
        ).distinct()
    )

    # Part 2: InvestmentID NOT IN (-1, -2), joined with Entity, not ASSET CLASS
    part2 = (
        all_snap.filter(~F.col("InvestmentID").isin(-1, -2))
        .join(entity.select(F.col("EntityID").alias("InvEntityID")),
              F.col("InvestmentID") == F.col("InvEntityID"))
        .join(enu_ut_renamed, F.col("Underlyingtype") == F.col("UnderlyingTypeID"))
        .filter(F.upper(F.col("UnderlyingTypeName")) != "ASSET CLASS")
        .select(
            "WorkFlowID", "TransactionID", "ClientID", "TaxPeriodID",
            "EntityId", "InvestmentID", "PartnerNumber", "Quarter",
            "CommitmentPercent", "AllocationTypeId", "Tag", "TrackingKey",
            "Underlyingtype", "AllocatedAmount", "CostPercentageId"
        ).distinct()
    )

    # Part 3: ASSET CLASS entries joined with Enu_AssetClass
    enu_ac = spark.table(f"{prefix}.Enu_AssetClass").select("AssetClassID").distinct()
    part3 = (
        all_snap
        .join(enu_ut_renamed, F.col("Underlyingtype") == F.col("UnderlyingTypeID"))
        .filter(F.upper(F.col("UnderlyingTypeName")) == "ASSET CLASS")
        .join(enu_ac, F.col("InvestmentID") == F.col("AssetClassID"))
        .select(
            "WorkFlowID", "TransactionID", "ClientID", "TaxPeriodID",
            "EntityId", "InvestmentID", "PartnerNumber", "Quarter",
            "CommitmentPercent", "AllocationTypeId", "Tag", "TrackingKey",
            "Underlyingtype", "AllocatedAmount", "CostPercentageId"
        ).distinct()
    )

    # Part 4: Deal-level percentages via entity hierarchy
    # Get entities with non-empty DealID
    entities_with_deals = all_snap.filter(
        F.coalesce(F.col("DealID"), F.lit("")) != ""
    ).select("EntityId").distinct()

    entity_rel = spark.table(f"{prefix}.EntityRelationShip")

    # Build hierarchy: recursive CTE equivalent via iterative join
    # Start: direct children of entities with deals
    hierarchy = (
        entity_rel
        .join(entities_with_deals, entity_rel["UpperTierEntityID"] == entities_with_deals["EntityId"])
        .select(
            entities_with_deals["EntityId"].alias("EntityID"),
            entity_rel["UpperTierEntityID"],
            entity_rel["LowerTierEntityID"],
        )
    )

    # Iterate to get full hierarchy (up to 10 levels deep)
    for _ in range(10):
        next_level = (
            entity_rel.alias("R")
            .join(hierarchy.alias("H"),
                  F.col("R.UpperTierEntityID") == F.col("H.LowerTierEntityID"))
            .select(
                F.col("H.EntityID"),
                F.col("R.UpperTierEntityID"),
                F.col("R.LowerTierEntityID"),
            )
        )
        if next_level.isEmpty():
            break
        hierarchy = hierarchy.union(next_level).distinct()

    # Add self-referencing rows
    self_ref = entities_with_deals.select(
        F.col("EntityId").alias("EntityID"),
        F.lit(None).cast("int").alias("UpperTierEntityID"),
        F.col("EntityId").alias("LowerTierEntityID"),
    )
    hierarchy = hierarchy.union(self_ref).distinct()

    # Get deals (Custom10) from Entity for lower tier entities
    entity_deals = (
        hierarchy
        .join(entity.select(F.col("EntityID").alias("LT_EntityID"), "Custom10"),
              hierarchy["LowerTierEntityID"] == F.col("LT_EntityID"))
        .filter(F.coalesce(F.col("Custom10"), F.lit("")) != "")
        .select(
            hierarchy["EntityID"],
            hierarchy["UpperTierEntityID"],
            hierarchy["LowerTierEntityID"],
            F.col("Custom10"),
        )
    )

    # Distinct records where DealID is empty and InvestmentID is NULL
    distinct_no_deal = (
        all_snap
        .filter(
            (F.coalesce(F.col("DealID"), F.lit("")) == "") &
            F.col("InvestmentID").isNull()
        )
        .select("EntityId", "InvestmentID", "Quarter", "AllocationTypeId",
                "Tag", "TrackingKey", "Underlyingtype")
        .distinct()
    )

    # Part 4: join pre-filtered with entity deals on DealID = Custom10
    part4 = (
        pre_filtered
        .join(entity_deals,
              (pre_filtered["DealID"] == entity_deals["Custom10"]) &
              (pre_filtered["EntityId"] == entity_deals["EntityID"]))
        .select(
            pre_filtered["WorkFlowID"],
            pre_filtered["TransactionID"],
            pre_filtered["ClientID"],
            pre_filtered["TaxPeriodID"],
            entity_deals["UpperTierEntityID"].alias("EntityId"),
            entity_deals["LowerTierEntityID"].alias("InvestmentID"),
            pre_filtered["PartnerNumber"],
            pre_filtered["Quarter"],
            pre_filtered["CommitmentPercent"],
            pre_filtered["AllocationTypeId"],
            pre_filtered["Tag"],
            pre_filtered["TrackingKey"],
            pre_filtered["Underlyingtype"],
            pre_filtered["AllocatedAmount"],
            pre_filtered["CostPercentageId"],
        ).distinct()
    )

    # Union all parts
    result = part1.unionByName(part2).unionByName(part3).unionByName(part4)
    return result


def load_cost_percentages(spark: SparkSession, cfg: dict) -> dict:
    """Load cost percentage data from snapshot.
    SQL lines: CostPercentage snapshot load + 704c filtering.
    For line_type='704c' the SQL uses an INNER JOIN to CostPercentage_704c_Snapshot
    (keep only 704c rows). For everything else it uses LEFT JOIN ... IS NULL
    (exclude 704c rows). WARN-2 fix.
    Returns dict with 'cost_percentages' DataFrame and optional temp rules.
    ALWAYS-NON-EMPTY for valid entity with cost workflow.
    """
    log_section("load_cost_percentages")
    t0 = time.time()
    prefix = table_prefix(cfg)
    cost_wf = cfg["cost_workflow_id"]
    line_type = cfg.get("line_type", "")

    # Inline UDF: get cost percentage details
    udf_result = _get_cost_percentage_details(spark, cfg, cost_wf)

    # Join with ENU_UnderlyingType and exclude 704c entries
    enu_ut = spark.table(f"{prefix}.ENU_UnderlyingType")
    cost_704c = spark.table(f"{prefix}.CostPercentage_704c_Snapshot")

    base = (
        udf_result.alias("C")
        .join(enu_ut.alias("U"),
              F.col("C.Underlyingtype") == F.col("U.UnderlyingTypeID"))
    )

    if line_type == "704c":
        # WARN-2 fix: 704c branch — INNER JOIN, keep only mapped 704c rows
        joined = base.join(
            cost_704c.alias("CP"),
            (F.col("C.WorkFlowID") == F.col("CP.WorkFlowID")) &
            (F.col("C.CostPercentageId") == F.col("CP.CostPercentageID")),
        )
    else:
        joined = base.join(
            cost_704c.alias("CP"),
            (F.col("C.WorkFlowID") == F.col("CP.WorkFlowID")) &
            (F.col("C.CostPercentageId") == F.col("CP.CostPercentageID")),
            "left"
        ).filter(F.col("CP.CostPercentageID").isNull())

    cost_df = joined.select(
        F.col("C.WorkFlowID"), F.col("C.TransactionID"), F.col("C.ClientID"),
        F.col("C.TaxPeriodID"), F.col("C.EntityId").alias("EntityID"),
        F.col("C.InvestmentID"), F.col("C.PartnerNumber"), F.col("C.Quarter"),
        F.col("C.CommitmentPercent"),
        F.col("C.AllocationTypeId").alias("AllocationTypeID"),
        F.col("C.Tag"), F.col("C.TrackingKey"),
        F.col("C.Underlyingtype").alias("UnderlyingType"),
        F.col("C.AllocatedAmount"),
        F.col("C.CostPercentageId").alias("CostPercentageID"),
        F.col("U.UnderlyingType").alias("EntityUnderlyingType"),
    )

    result = {
        "cost_percentages": cost_df,
        "temp_map_rules": None,
        "temp_default_rules": None,
    }

    log_timing("load_cost_percentages", t0)
    return result


# ---------------------------------------------------------------------------
# Book effective rules + footnote inheritance
# ---------------------------------------------------------------------------

def load_book_effective_rules(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load book effective snapshot rules from CAR workflow.
    POSSIBLY-EMPTY: no custom allocation rules configured.
    """
    log_section("load_book_effective_rules")
    t0 = time.time()
    prefix = table_prefix(cfg)
    car_wf = cfg.get("car_workflow_id")

    _BOOK_EFFECTIVE_SCHEMA = StructType([
        StructField("UnderlyingEntityID", IntegerType(), True),
        StructField("LineID", IntegerType(), True),
        StructField("FootNoteID", IntegerType(), True),
        StructField("SourceID", IntegerType(), True),
        StructField("AllocationTypeID", IntegerType(), True),
        StructField("AdjustmentAllocationTypeID", IntegerType(), True),
        StructField("TrackingKey", StringType(), True),
        StructField("Tag", StringType(), True),
        StructField("IsExcludefromTransfer", BooleanType(), True),
    ])

    if not car_wf:
        logger.warning("[POSSIBLY-EMPTY] load_book_effective_rules: "
                       "No CAR workflow ID — returning empty.")
        log_timing("load_book_effective_rules", t0)
        return spark.createDataFrame([], _BOOK_EFFECTIVE_SCHEMA)

    book_eff = spark.sql(f"""
        SELECT UnderlyingEntityID, LineID, FootNoteID, SourceID,
               AllocationTypeID, AdjustmentAllocationTypeID,
               TrackingKey, Tag, IsExcludefromTransfer
        FROM {prefix}.BookEffective_Snapshot
        WHERE WorkflowID = {car_wf}
          AND ClientID = {cfg['client_id']}
          AND TaxPeriodID = {cfg['tax_period_id']}
    """)

    warn_possibly_empty(book_eff, "load_book_effective_rules",
                        f"car_workflow_id={car_wf}")
    log_timing("load_book_effective_rules", t0)
    return book_eff


def add_footnote_inheritance(spark: SparkSession, cfg: dict,
                             book_effective: DataFrame,
                             input_data: DataFrame) -> DataFrame:
    """Add footnote lines that inherit federal line custom allocation rules.
    SQL: INSERT INTO #TempBookEffective ... FROM #TempLookThroughAllocationInputDataLoad I
         INNER JOIN #TempFootnoteLines M ON I.LineID = M.FootnoteLine
         INNER JOIN #TempBookEffective B ON B.LineID = M.FedLine AND B.SourceID = @K1LineTypeID
           AND B.UnderlyingEntityID = I.EntityID
           AND TrackingKey/Tag match
         LEFT JOIN #TempBookEffective B2 (same conditions on FootnoteLine)
         WHERE B2.UnderlyingEntityID IS NULL
    POSSIBLY-EMPTY: depends on MAP_DerivedLines content.
    """
    log_section("add_footnote_inheritance")
    t0 = time.time()
    prefix = table_prefix(cfg)
    k1_lti = cfg["k1_line_type_id"]

    # SQL: #TempFootnoteLines (DISTINCT MAP_DerivedLines UNION MAP_SourceAttributeRelation)
    # WHERE EA.AttributeType = 'FN' AND DerivedLineID IS NOT NULL AND BaseLineID IS NOT NULL
    #       AND ISNULL(EA.IsHidden,0)=0
    derived_footnotes = spark.sql(f"""
        SELECT DISTINCT M.BaseLineID AS FedLine, M.DerivedLineID AS FootnoteLine
        FROM {prefix}.MAP_DerivedLines M
        INNER JOIN {prefix}.ENU_AttributeType EA ON M.AttributeID = EA.AttributeID
        WHERE EA.AttributeType = 'FN'
          AND M.DerivedLineID IS NOT NULL AND M.BaseLineID IS NOT NULL
          AND (EA.IsHidden IS NULL OR EA.IsHidden = false)
    """)

    source_footnotes = spark.sql(f"""
        SELECT DISTINCT MD.SourceLineID AS FedLine, MD.AttributeLineID AS FootnoteLine
        FROM {prefix}.MAP_SourceAttributeRelation MD
        INNER JOIN {prefix}.ENU_AttributeType EN ON MD.AttributeID = EN.AttributeID
        WHERE EN.AttributeType = 'FN'
          AND MD.SourceLineID IS NOT NULL AND MD.AttributeLineID IS NOT NULL
          AND (EN.IsHidden IS NULL OR EN.IsHidden = false)
    """)

    footnote_mappings = derived_footnotes.unionByName(source_footnotes).distinct()

    if footnote_mappings.isEmpty():
        log_timing("add_footnote_inheritance", t0)
        return book_effective

    # Build inherited footnote allocation rules:
    # For each footnote line in input, if its federal line has a book effective rule,
    # and the footnote line does NOT already have its own book effective rule,
    # the footnote inherits the federal line's rule.
    inherited = input_data.alias("I").join(
        footnote_mappings.alias("M"),
        F.col("I.LineID") == F.col("M.FootnoteLine")
    ).join(
        book_effective.alias("B"),
        (F.col("B.LineID") == F.col("M.FedLine")) &
        (F.col("B.SourceID") == k1_lti) &
        (F.col("B.UnderlyingEntityID") == F.col("I.EntityID")) &
        (F.when(ns(F.col("B.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B.TrackingKey")) ==
         F.when(ns(F.col("B.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("I.TrackingKey"))) &
        (F.when(ns(F.col("B.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B.Tag")) ==
         F.when(ns(F.col("B.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("I.Tag")))
    ).join(
        # Anti-join: exclude if footnote line already has its own rule
        book_effective.alias("B2"),
        (F.col("B2.LineID") == F.col("M.FootnoteLine")) &
        (F.col("B2.SourceID") == k1_lti) &
        (F.col("B2.UnderlyingEntityID") == F.col("I.EntityID")) &
        (F.when(ns(F.col("B2.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B2.TrackingKey")) ==
         F.when(ns(F.col("B2.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("I.TrackingKey"))) &
        (F.when(ns(F.col("B2.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B2.Tag")) ==
         F.when(ns(F.col("B2.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("I.Tag"))),
        "left"
    ).filter(
        F.col("B2.UnderlyingEntityID").isNull()
    ).select(
        F.col("I.EntityID").alias("UnderlyingEntityID"),
        F.col("I.LineID").alias("LineID"),
        F.col("B.FootNoteID"),
        F.col("B.SourceID"),
        F.col("B.AllocationTypeID"),
        F.col("B.AdjustmentAllocationTypeID"),
        F.col("B.TrackingKey"),
        F.col("B.Tag"),
        F.col("B.IsExcludefromTransfer"),
    ).distinct()

    result = book_effective.unionByName(inherited)
    log_timing("add_footnote_inheritance", t0)
    return result


# ---------------------------------------------------------------------------
# Final effective percentages
# ---------------------------------------------------------------------------

def load_final_effective_percentages(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load final effective percentages based on line type.
    SQL branches: K1 with 704c, K1 with Cost, K1, BoxJKL, 704c.
    ALWAYS-NON-EMPTY for valid run with allocation percentages.
    """
    log_section("load_final_effective_percentages")
    t0 = time.time()
    run_id = cfg["run_id"]
    line_type = cfg.get("line_type", "")
    rank_for_rule = cfg.get("rank_for_rule", 0)

    base = read_table(spark, "FinalEffectivePercentages", cfg).filter(
        F.col("RunID") == run_id
    )

    # Common output columns (all branches except 704c select these)
    out_cols = [
        "T.EntityID", "T.RunID", "T.SourceLEID", "InvestmentID",
        "PartnerNumber", "EffPercentage", "T.AllocationType", "Quarter",
        "TypeID", "TrackingKey", "Tag", "T.LineTypeID", "LineID",
        "IsExcludefromTransfer", "AssetClassID", "EffAmount",
        "CostPercentageId", "RankForRule",
    ]

    if line_type == "K1 with 704c":
        # INNER JOIN ENU_CustomAllocations, LEFT JOIN ENU_LineType
        # WHERE LineType='K1' AND EC.AllocationType<>'Cost' AND T.AllocationType<>'prorata'
        result = base.alias("T").join(
            read_table(spark, "ENU_CustomAllocations", cfg).alias("EC"),
            F.col("EC.AllocationTypeID") == F.col("T.TypeID")
        ).join(
            read_table(spark, "ENU_LineType", cfg).alias("EL"),
            F.col("EL.LineTypeID") == F.col("T.LineTypeID"),
            "left"
        ).filter(
            (F.col("EL.LineType") == "K1") &
            (F.coalesce(F.col("EC.AllocationType"), F.lit("")) != "Cost") &
            (F.coalesce(F.col("T.AllocationType"), F.lit("")) != "prorata")
        ).select(*out_cols)

    elif line_type == "K1 with Cost":
        # INNER JOIN ENU_CustomAllocations, LEFT JOIN ENU_LineType
        # WHERE LineType='K1' AND (EC.AllocationType='Cost' OR T.AllocationType='prorata')
        result = base.alias("T").join(
            read_table(spark, "ENU_CustomAllocations", cfg).alias("EC"),
            F.col("EC.AllocationTypeID") == F.col("T.TypeID")
        ).join(
            read_table(spark, "ENU_LineType", cfg).alias("EL"),
            F.col("EL.LineTypeID") == F.col("T.LineTypeID"),
            "left"
        ).filter(
            (F.col("EL.LineType") == "K1") &
            ((F.coalesce(F.col("EC.AllocationType"), F.lit("")) == "Cost") |
             (F.coalesce(F.col("T.AllocationType"), F.lit("")) == "prorata"))
        ).select(*out_cols)

    elif line_type == "K1":
        # LEFT JOIN ENU_LineType, WHERE LineType='K1'
        result = base.alias("T").join(
            read_table(spark, "ENU_LineType", cfg).alias("EL"),
            F.col("EL.LineTypeID") == F.col("T.LineTypeID"),
            "left"
        ).filter(
            F.col("EL.LineType") == "K1"
        ).select(*out_cols)

    elif line_type == "BoxJKL":
        # LEFT JOIN ENU_LineType, WHERE LineType='BoxJKL' AND RankForRule=@RankForRulePickup
        result = base.alias("T").join(
            read_table(spark, "ENU_LineType", cfg).alias("EL"),
            F.col("EL.LineTypeID") == F.col("T.LineTypeID"),
            "left"
        ).filter(
            (F.col("EL.LineType") == "BoxJKL") &
            (F.col("T.RankForRule") == rank_for_rule)
        ).select(*out_cols)

    elif line_type == "704c":
        # LEFT JOIN ENU_LineType, WHERE LineType='K1' AND 704cPercentageType<>''
        result = base.alias("T").join(
            read_table(spark, "ENU_LineType", cfg).alias("EL"),
            F.col("EL.LineTypeID") == F.col("T.LineTypeID"),
            "left"
        ).filter(
            (F.col("EL.LineType") == "K1") &
            (F.coalesce(F.col("T.704cPercentageType"), F.lit("")) != "")
        ).select(*out_cols, F.col("T.704cPercentageType").alias("704cPercentType"))

    else:
        result = base.alias("T").select(*out_cols)

    log_timing("load_final_effective_percentages", t0)
    return result


# ---------------------------------------------------------------------------
# FAIL-1 fix: 704c-to-K1 line mapping (UNPIVOT block)
# SQL lines ~407-565: when LineType='K1 with 704c' AND @704cAllocationTypeName
# is set. Maps the 10 amount columns of CostPercentage_704c_Snapshot to K1
# lines using MapDataRegister/MappingLineItem/K1LineItem, then unpivots them
# into synthetic cost rows and synthetic allocation rules (TransactionID=-2).
# ---------------------------------------------------------------------------

# The 10 amount columns on CostPercentage_704c_Snapshot. Field 704cGainLoss
# is bracketed in T-SQL because it starts with a digit.
_704C_AMOUNT_COLUMNS = [
    "TotalMgmtFees", "HotIssueGainLoss", "704cGainLoss",
    "GuaranteedPaymentsServices", "GuaranteedPaymentsCapital",
    "UsWithholding", "IncentiveFee", "ForeignTaxes",
    "SpecialAllocation1", "SpecialAllocation2",
]


def apply_704c_to_k1_mappings(spark: SparkSession, cfg: dict,
                              cost_percentages: DataFrame,
                              map_rules: DataFrame,
                              default_rules: DataFrame) -> dict:
    """FAIL-1 fix: PySpark port of the 704c-to-K1 UNPIVOT block.

    Builds #Mappings (entity-specific UNION global) via
    MapDataRegister + MappingLineItem + K1LineItem; unpivots the 10 amount
    columns of CostPercentage_704c_Snapshot using F.stack(); joins back to
    map K1LineID and produce synthetic cost rows; emits synthetic rules
    (TransactionID = -2) into map_rules / default_rules.

    Returns:
        {
            "cost_percentages": updated cost_percentages,
            "map_rules": updated map_rules,
            "default_rules": updated default_rules,
            "distinct_mappings": DataFrame[RegisterLineID, FieldSourceID]
                                 (needed by FAIL-5),
        }
    """
    log_section("apply_704c_to_k1_mappings")
    t0 = time.time()
    prefix = table_prefix(cfg)
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    cost_wf = cfg["cost_workflow_id"]

    # --- Scalar lookups ---
    # SQL: SELECT @RegisterTypeID = MenuID FROM GlobalMenu
    #      WHERE MenuName = '704c To K1 Line Mapping'
    register_type_row = spark.sql(f"""
        SELECT MenuID FROM {prefix}.GlobalMenu
        WHERE MenuName = '704c To K1 Line Mapping'
          AND ClientID = {client_id} AND TaxPeriodID = {tax_period_id}
    """).first()
    if register_type_row is None:
        log_timing("apply_704c_to_k1_mappings", t0)
        return {
            "cost_percentages": cost_percentages,
            "map_rules": map_rules,
            "default_rules": default_rules,
            "distinct_mappings": None,
        }
    register_type_id = register_type_row["MenuID"]

    # @704cSourceID = ENU_MappingSource WHERE SourceName='Tax Allocation Report - 704c'
    src_row = spark.sql(f"""
        SELECT SourceID FROM {prefix}.ENU_MappingSource
        WHERE SourceName = 'Tax Allocation Report - 704c'
    """).first()
    if src_row is None:
        log_timing("apply_704c_to_k1_mappings", t0)
        return {
            "cost_percentages": cost_percentages,
            "map_rules": map_rules,
            "default_rules": default_rules,
            "distinct_mappings": None,
        }
    src_704c_id = src_row["SourceID"]

    # K1 line type id for #Mappings.FieldSourceID
    k1_lt_row = spark.sql(f"""
        SELECT LineTypeID FROM {prefix}.ENU_LineType
        WHERE LineType = 'K1'
    """).first()
    k1_lt_id = k1_lt_row["LineTypeID"] if k1_lt_row else cfg.get(
        "k1_line_type_id", 0)

    # --- Build #Mappings (entity-specific then global UNION) ---
    # Joins: MapDataRegister MR -> MappingLineItem ML on MR.MapLineID
    #        K1LineItem K on ML.RegisterLineID = K.LineID
    #        ENU_MappingSource MS on MR.SourceID = MS.SourceID
    # Cols:  RegisterLineID = ML.RegisterLineID, FieldSourceID = k1_lt_id,
    #        DatabaseName = MR.DatabaseName, MapLineID = MR.MapLineID,
    #        Formula = MR.Formula, EntityID
    mdr = spark.table(f"{prefix}.MapDataRegister").alias("MR")
    mli = spark.table(f"{prefix}.MappingLineItem").alias("ML")
    k1l = spark.table(f"{prefix}.K1LineItem").alias("K")

    base_mappings = (
        mdr.join(mli, F.col("MR.MapLineID") == F.col("ML.MapLineID"))
        .join(k1l, F.col("ML.RegisterLineID") == F.col("K.LineID"))
        .filter(
            (F.col("MR.SourceID") == src_704c_id) &
            (F.col("MR.RegisterTypeID") == register_type_id) &
            (F.col("MR.ClientID") == client_id) &
            (F.col("MR.TaxPeriodID") == tax_period_id)
        )
        .select(
            F.col("ML.RegisterLineID").alias("RegisterLineID"),
            F.lit(k1_lt_id).alias("FieldSourceID"),
            F.col("MR.DatabaseName").alias("DatabaseName"),
            F.col("MR.MapLineID").alias("MapLineID"),
            F.col("MR.Formula").alias("Formula"),
            F.col("MR.EntityID").alias("EntityID"),
        )
    )

    entity_mappings = base_mappings.filter(F.col("EntityID") == entity_id)
    global_mappings = base_mappings.filter(F.col("EntityID") == -1).join(
        entity_mappings.select(F.col("MapLineID").alias("EM_MapLineID")),
        F.col("MapLineID") == F.col("EM_MapLineID"),
        "left_anti"
    )
    mappings = entity_mappings.unionByName(global_mappings)

    # Early exit: SQL `IF EXISTS (SELECT TOP 1 1 FROM #Mappings)`
    # Use take(1) — short-circuits after first row, no aggregation shuffle.
    if not mappings.take(1):
        log_timing("apply_704c_to_k1_mappings", t0)
        return {
            "cost_percentages": cost_percentages,
            "map_rules": map_rules,
            "default_rules": default_rules,
            "distinct_mappings": None,
        }

    # --- #DistinctMappings ---
    distinct_mappings = mappings.groupBy(
        "RegisterLineID", "FieldSourceID"
    ).agg(F.max("DatabaseName").alias("DatabaseName"))

    # --- #CostPercentage704cValues ---
    # SQL: CostPercentage_Function CF (udfGetCostPercentageDetails)
    #      INNER ENU_UnderlyingType U on CF.Underlyingtype=U.UnderlyingTypeID
    #      INNER CostPercentage_704c_Snapshot CP on (CF.WorkFlowID, CF.CostPercentageId)
    #      WHERE WorkFlowID = @CostWorkflowID AND U.UnderlyingType != 'ASSET CLASS'
    udf_result = _get_cost_percentage_details(spark, cfg, cost_wf)
    enu_ut = spark.table(f"{prefix}.ENU_UnderlyingType")
    cost_704c = spark.table(f"{prefix}.CostPercentage_704c_Snapshot")

    amount_select = [F.col("CP." + c).alias(c) for c in _704C_AMOUNT_COLUMNS]
    values_704c = (
        udf_result.alias("CF")
        .join(enu_ut.alias("U"),
              F.col("CF.Underlyingtype") == F.col("U.UnderlyingTypeID"))
        .join(cost_704c.alias("CP"),
              (F.col("CF.WorkFlowID") == F.col("CP.WorkFlowID")) &
              (F.col("CF.CostPercentageId") == F.col("CP.CostPercentageID")))
        .filter(F.upper(F.col("U.UnderlyingType")) != "ASSET CLASS")
        .select(
            F.col("CF.WorkFlowID"), F.col("CF.TransactionID"),
            F.col("CF.ClientID"), F.col("CF.TaxPeriodID"),
            F.col("CF.EntityId").alias("EntityID"),
            F.col("CF.InvestmentID"), F.col("CF.PartnerNumber"),
            F.col("CF.Quarter"), F.col("CF.CommitmentPercent"),
            F.col("CF.AllocationTypeId").alias("AllocationTypeID"),
            F.col("CF.Tag"), F.col("CF.TrackingKey"),
            F.col("CF.Underlyingtype").alias("UnderlyingType"),
            F.col("CF.CostPercentageId").alias("CostPercentageID"),
            F.col("U.UnderlyingType").alias("EntityUnderlyingType"),
            *amount_select,
        )
    )

    # Use take(1) — short-circuits after first row, no aggregation shuffle.
    if not values_704c.take(1):
        log_timing("apply_704c_to_k1_mappings", t0)
        return {
            "cost_percentages": cost_percentages,
            "map_rules": map_rules,
            "default_rules": default_rules,
            "distinct_mappings": distinct_mappings,
        }

    # --- UNPIVOT via F.stack() ---
    # Only unpivot the columns that appear in the mappings (matches the
    # SQL's dynamic UNPIVOT column list).
    mapped_field_names = [r["DatabaseName"] for r in
                          distinct_mappings.select("DatabaseName")
                          .distinct().collect()]
    mapped_cols = [c for c in _704C_AMOUNT_COLUMNS if c in mapped_field_names]
    if not mapped_cols:
        log_timing("apply_704c_to_k1_mappings", t0)
        return {
            "cost_percentages": cost_percentages,
            "map_rules": map_rules,
            "default_rules": default_rules,
            "distinct_mappings": distinct_mappings,
        }

    stack_args = []
    for c in mapped_cols:
        stack_args.append(F.lit(c))
        stack_args.append(F.col("`" + c + "`"))
    stack_expr = F.expr(
        "stack({n}, {pairs}) AS (Mapped704cField, AllocatedAmount)".format(
            n=len(mapped_cols),
            pairs=", ".join(
                f"'{c}', `{c}`" for c in mapped_cols
            ),
        )
    )

    keep_cols = [
        "WorkFlowID", "TransactionID", "ClientID", "TaxPeriodID", "EntityID",
        "InvestmentID", "PartnerNumber", "Quarter", "CommitmentPercent",
        "AllocationTypeID", "Tag", "TrackingKey", "UnderlyingType",
        "CostPercentageID", "EntityUnderlyingType",
    ]
    unpivoted = values_704c.select(
        *[F.col(c) for c in keep_cols], stack_expr
    )

    # --- Join mappings to add K1LineID ---
    # SQL: UPDATE CS SET K1LineID = MS.RegisterLineID
    #      FROM #CostPercentage_Snapshot_UnPivoted CS
    #      INNER JOIN #Mappings MS ON CS.Mapped704cField = MS.DatabaseName
    # Then negate AllocatedAmount where MS.Formula = 'SUBTRACT'.
    mapped_keys = mappings.select(
        F.col("DatabaseName").alias("M_DatabaseName"),
        F.col("RegisterLineID").alias("M_K1LineID"),
        F.col("Formula").alias("M_Formula"),
    )
    unpivoted_mapped = unpivoted.join(
        F.broadcast(mapped_keys),
        F.col("Mapped704cField") == F.col("M_DatabaseName"),
    ).withColumn(
        "K1LineID", F.col("M_K1LineID")
    ).withColumn(
        "AllocatedAmount",
        F.when(F.upper(F.col("M_Formula")) == "SUBTRACT",
               -F.col("AllocatedAmount"))
        .otherwise(F.col("AllocatedAmount"))
    ).drop("M_DatabaseName", "M_K1LineID", "M_Formula")

    # --- Group + sum (SQL #CostPercentage_Snapshot_UnPivotedMerged) ---
    group_cols = [
        "WorkFlowID", "TransactionID", "ClientID", "TaxPeriodID", "EntityID",
        "InvestmentID", "PartnerNumber", "Quarter", "CommitmentPercent",
        "AllocationTypeID", "Tag", "TrackingKey", "UnderlyingType",
        "CostPercentageID", "EntityUnderlyingType", "K1LineID",
    ]
    merged = unpivoted_mapped.groupBy(*group_cols).agg(
        F.sum("AllocatedAmount").alias("AllocatedAmount"),
        F.max("Mapped704cField").alias("Mapped704cField"),
    )

    # --- Override AllocationTypeID via ENU_CustomAllocations ---
    # SQL: UPDATE ... SET AllocationTypeID = EC.AllocationTypeID
    #      WHERE 'Special ' + Mapped704cField = EC.AllocationType
    enu_ca = read_table(spark, "ENU_CustomAllocations", cfg).select(
        F.col("AllocationType").alias("EC_AllocationType"),
        F.col("AllocationTypeID").alias("EC_AllocationTypeID"),
    )
    merged = merged.join(
        F.broadcast(enu_ca),
        F.concat(F.lit("Special "), F.col("Mapped704cField"))
        == F.col("EC_AllocationType"),
        "left"
    ).withColumn(
        "AllocationTypeID",
        F.coalesce(F.col("EC_AllocationTypeID"), F.col("AllocationTypeID"))
    ).drop("EC_AllocationType", "EC_AllocationTypeID")

    # --- Lookup RuleGroupID via ENU_RuleGroup ---
    enu_rg = read_table(spark, "ENU_RuleGroup", cfg).select(
        F.col("RuleGroupName").alias("RG_Name"),
        F.col("RuleGroupID").alias("RG_ID"),
    )
    merged_with_rg = merged.join(
        F.broadcast(enu_rg),
        F.concat(F.lit("Special "), F.col("Mapped704cField"))
        == F.col("RG_Name"),
        "left"
    ).withColumnRenamed("RG_ID", "RuleGroupID").drop("RG_Name")

    # --- Append merged rows to cost_percentages ---
    new_cost_rows = merged_with_rg.select(
        "WorkFlowID", "TransactionID", "ClientID", "TaxPeriodID", "EntityID",
        "InvestmentID", "PartnerNumber", "Quarter", "CommitmentPercent",
        "AllocationTypeID", "Tag", "TrackingKey", "UnderlyingType",
        "AllocatedAmount", "CostPercentageID", "EntityUnderlyingType",
    )
    cost_percentages_new = cost_percentages.unionByName(new_cost_rows)

    # --- Scalar lookups for synthetic rule rows ---
    def _scalar(table: str, name_col: str, name_val: str, id_col: str):
        r = spark.sql(
            f"SELECT {id_col} FROM {prefix}.{table} "
            f"WHERE {name_col} = '{name_val}'"
        ).first()
        return r[id_col] if r else 0

    alloc_pct_type_id = _scalar(
        "ENU_AllocationPercentageType", "AllocationPercentageType",
        "N/A", "AllocationPercentageTypeID")
    alloc_by_amount_id = _scalar(
        "ENU_AllocationBy", "AllocationBy", "AMOUNT", "AllocationByID")
    rule_type_entity_id = _scalar(
        "ENU_RuleType", "RuleType", "ENTITY", "RuleTypeID")

    # --- Build synthetic map_rules and default_rules with TransactionID=-2 ---
    # Distinct rule keys derived from merged: one rule per (RuleGroupID,
    # AllocationTypeID, UnderlyingType).
    rule_keys = merged_with_rg.select(
        "RuleGroupID", "AllocationTypeID", "UnderlyingType"
    ).distinct().filter(F.col("RuleGroupID").isNotNull())

    synthetic_default = rule_keys.select(
        F.lit(-2).cast("long").alias("TransactionID"),
        F.col("AllocationTypeID").alias("RuleID"),
        F.lit(alloc_pct_type_id).cast("int").alias("AllocationPercentageTypeID"),
        F.lit(alloc_by_amount_id).cast("int").alias("AllocationByID"),
        F.col("UnderlyingType").alias("UnderlyingTypeID"),
        F.lit(rule_type_entity_id).cast("int").alias("RuleTypeID"),
        F.col("RuleGroupID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("long").alias("TaxPeriodID"),
        F.lit(entity_id).cast("long").alias("EntityID"),
    )

    # Map synthetic rules: SourceID = k1_lt_id (the K1 line type),
    # SelectedMappingID = RegisterLineID from #DistinctMappings.
    synthetic_map = distinct_mappings.crossJoin(
        rule_keys.select("AllocationTypeID").distinct()
    ).select(
        F.lit(-2).cast("long").alias("TransactionID"),
        F.col("FieldSourceID").alias("SourceID"),
        F.lit(None).cast("int").alias("StateID"),
        F.col("RegisterLineID").alias("SelectedMappingID"),
        F.col("AllocationTypeID").alias("RuleID"),
        F.lit(0).cast("int").alias("ExcludeFromTransfers"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("long").alias("TaxPeriodID"),
        F.lit(entity_id).cast("long").alias("EntityID"),
    )

    default_rules_new = default_rules.unionByName(
        synthetic_default, allowMissingColumns=True)
    map_rules_new = map_rules.unionByName(
        synthetic_map, allowMissingColumns=True)

    log_timing("apply_704c_to_k1_mappings", t0)
    return {
        "cost_percentages": cost_percentages_new,
        "map_rules": map_rules_new,
        "default_rules": default_rules_new,
        "distinct_mappings": distinct_mappings,
    }

