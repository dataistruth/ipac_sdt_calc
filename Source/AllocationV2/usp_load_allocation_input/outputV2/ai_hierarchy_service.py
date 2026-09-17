"""
ai_hierarchy_service.py

Entity hierarchy construction and lower tier fund resolution for uspLoadAllocationInput.

Builds the recursive entity hierarchy (WHILE loop → iterative join),
PE investments, lower tier funds, and workflow resolution.

SQL lines: 845-1050, 1740-1880, 2200-2400
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import table_prefix, read_table, log_section, log_timing

logger = logging.getLogger(__name__)


def build_entity_hierarchy(spark: SparkSession, cfg: dict) -> DataFrame:
    """Build recursive entity hierarchy via iterative join.

    Converts WHILE loop (L2250-2305) to iterative DataFrame join
    until no new rows are discovered. Then adds PE investments.

    SQL lines: 2200-2380
    Returns: DataFrame[UpperTierEntityID, LowerTierEntityID, ImmediateLowerTierID]
    """
    log_section("build_entity_hierarchy")
    t0 = time.time()
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    phase_id = cfg["phase_id"]

    # Collect entity relationships (small table — typically <1000 rows per client)
    all_rels = (
        read_table(spark, "EntityRelationship", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id)
        )
        .select("UpperTierEntityID", "LowerTierEntityID")
        .collect()
    )

    # Build adjacency list for BFS
    children_map = {}
    for r in all_rels:
        parent = r["UpperTierEntityID"]
        child = r["LowerTierEntityID"]
        children_map.setdefault(parent, []).append(child)

    hierarchy_rows = set()
    # Level 1: direct children
    frontier = children_map.get(entity_id, [])
    for child in frontier:
        hierarchy_rows.add((entity_id, child, child))

    # Expand: for each known (Upper, Lower, Immediate) triple, find Lower's children
    # and propagate ImmediateLowerTierID from the path root
    current_triples = [(entity_id, child, child) for child in frontier]
    while True:
        new_triples = []
        for upper, leaf, immediate in current_triples:
            for grandchild in children_map.get(leaf, []):
                triple = (leaf, grandchild, immediate)
                if triple not in hierarchy_rows:
                    hierarchy_rows.add(triple)
                    new_triples.append(triple)
        if not new_triples:
            break
        current_triples = new_triples

    hierarchy_rows_list = list(hierarchy_rows)

    if hierarchy_rows_list:
        _schema = StructType([
            StructField("UpperTierEntityID", IntegerType(), True),
            StructField("LowerTierEntityID", IntegerType(), True),
            StructField("ImmediateLowerTierID", IntegerType(), True),
        ])
        hierarchy_df = spark.createDataFrame(hierarchy_rows_list, schema=_schema)
    else:
        _schema = StructType([
            StructField("UpperTierEntityID", IntegerType(), True),
            StructField("LowerTierEntityID", IntegerType(), True),
            StructField("ImmediateLowerTierID", IntegerType(), True),
        ])
        hierarchy_df = spark.createDataFrame([], schema=_schema)

    # Add PE investments to hierarchy
    pe_base = read_table(spark, "PE_EntityByInvestment", cfg).filter(
        (F.col("ClientID") == client_id) &
        (F.col("TaxPeriodID") == tax_period_id) &
        (F.col("PhaseID") == phase_id)
    )
    pe_max_idx = (
        pe_base
        .groupBy("InvestmentID")
        .agg(F.max("IndexLevel").alias("IndexLevel"))
    )
    pe_investments = (
        pe_base.alias("P")
        .join(
            pe_max_idx.alias("M"),
            (F.col("P.InvestmentID") == F.col("M.InvestmentID")) &
            (F.col("P.IndexLevel") == F.col("M.IndexLevel")),
            "inner"
        )
        .select(F.col("P.InvestmentID"), F.col("P.EntityID"))
    )

    # Add PE rows: LowerTierEntityID = InvestmentID, inheriting from matching hierarchy rows
    pe_hierarchy = (
        pe_investments.alias("pe")
        .join(
            hierarchy_df.alias("h"),
            F.col("pe.EntityID") == F.col("h.LowerTierEntityID"),
            "inner",
        )
        .select(
            F.col("h.LowerTierEntityID").alias("UpperTierEntityID"),
            F.col("pe.InvestmentID").alias("LowerTierEntityID"),
            F.col("h.ImmediateLowerTierID"),
        )
    )

    hierarchy_df = hierarchy_df.distinct().union(pe_hierarchy)

    # Register as a temp view so downstream sections (e.g. Part V/VII domestic
    # traversal, C6) can read the recursive #EntityHierarchy directly.
    hierarchy_df.createOrReplaceTempView(f"_entity_hierarchy_{cfg['run_id']}")

    log_timing("build_entity_hierarchy", t0)
    return hierarchy_df


def build_lower_tier_funds(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load lower tier funds with IsForeign and IsPficCfcQfcEntity flags.

    SQL lines: 1832-1880
    Returns: DataFrame[EntityID, PartnerNumber, RunID, IsForeign, IsPficCfcQfcEntity]

    NOTE: The temp view _lower_tier_funds_{run_id} is pre-registered in
    register_shared_views for use in write_form_flowups. This function
    returns the same data as a DataFrame for use in validations.
    """
    log_section("build_lower_tier_funds")
    t0 = time.time()
    run_id = cfg["run_id"]

    lower_tier_df = spark.table(f"_lower_tier_funds_{run_id}")

    log_timing("build_lower_tier_funds", t0)
    return lower_tier_df


def build_workflows(spark: SparkSession, cfg: dict) -> dict:
    """Load K1, Adjustment, and AtRisk workflows from AllocationInputWorkflow.

    SQL lines: 1740-1810
    Returns dict with k1_workflow_df, adjustment_workflow_df, at_risk_workflow_df
    """
    log_section("build_workflows")
    t0 = time.time()
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    is_international = cfg.get("is_k1_input_international", False)
    disable_adjustments = cfg.get("disable_adjustments_allocations", "U")

    aiw_df = spark.table("_aiw")

    if is_international:
        k1_base = (
            aiw_df.filter(F.coalesce(F.col("K1WorkflowID"), F.lit(0)) != 0)
            .select(F.col("EntityID"), F.col("K1WorkflowID").alias("WorkflowID"))
            .unionByName(
                aiw_df.filter(F.coalesce(F.col("K1InternationalWorkflowID"), F.lit(0)) != 0)
                .select(F.col("EntityID"), F.col("K1InternationalWorkflowID").alias("WorkflowID"))
            )
        )
    else:
        k1_base = (
            aiw_df.filter(F.coalesce(F.col("K1WorkflowID"), F.lit(0)) != 0)
            .select(F.col("EntityID"), F.col("K1WorkflowID").alias("WorkflowID"))
        )

    (
        k1_base.select(F.col("EntityID"), F.col("WorkflowID")).distinct()
        .createOrReplaceTempView(f"_k1_workflow_{run_id}")
    )

    # Set IsForeign flag on K1 workflow entities
    entity_df = read_table(spark, "Entity", cfg)
    tax_class_df = read_table(spark, "ENU_TaxClass", cfg)
    k1_workflow_df = (
        k1_base.alias("W")
        .join(
            entity_df.alias("E"),
            F.col("E.EntityID") == F.col("W.EntityID"),
            "inner"
        )
        .join(tax_class_df.alias("T"), F.col("E.TaxClassID") == F.col("T.TaxClassID"), "left")
        .select(
            F.col("W.EntityID"), F.col("W.WorkflowID"),
            F.when(
                ((F.col("E.ClientID") == client_id) &
                 (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == True)) |
                ((F.coalesce(F.col("E.IsForeign"), F.lit(False)) == False) &
                 (F.lower(F.coalesce(F.col("T.TaxClassName"), F.lit(""))) == "disregarded entity")),
                F.lit(True)
            ).otherwise(F.lit(False)).alias("IsForeign")
        )
    )

    # Adjustment Workflow (only if not disabled)
    if disable_adjustments != "C":
        adjustment_workflow_df = (
            aiw_df.filter(F.coalesce(F.col("AdjustmentsWorkflowID"), F.lit(0)) != 0)
            .select(F.col("EntityID"), F.col("AdjustmentsWorkflowID").alias("WorkflowID"))
        )
    else:
        adjustment_workflow_df = spark.createDataFrame([], "EntityID INT, WorkflowID INT")

    # AtRisk Workflow
    at_risk_workflow_df = (
        aiw_df.filter(F.coalesce(F.col("ImportAtRiskWorkflowID"), F.lit(0)) != 0)
        .select(F.col("EntityID"), F.col("ImportAtRiskWorkflowID").alias("WorkflowID"))
    )

    log_timing("build_workflows", t0)
    return {
        "k1_workflow_df": k1_workflow_df,
        "adjustment_workflow_df": adjustment_workflow_df,
        "at_risk_workflow_df": at_risk_workflow_df,
    }
