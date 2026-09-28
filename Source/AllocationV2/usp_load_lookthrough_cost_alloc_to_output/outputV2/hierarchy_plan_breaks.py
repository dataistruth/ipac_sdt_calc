"""V2 plan breaks for the two recursive hierarchy frames.

Production ``_get_cost_percentage_details`` walks EntityRelationship up to
10 levels with ``union`` + ``isEmpty`` and never materializes ``hierarchy``.
Each loop action rebuilds the growing plan.

Production ``build_entity_hierarchy`` already checkpoints each level, but
through V1 Delta. During Development we swap that helper to Checkpoint V2
(the footnotes ``entity_levels`` pattern).
"""

from __future__ import annotations

from contextlib import contextmanager

import pyspark.sql.functions as F

from Common_V2.core.helpers import table_prefix

from .parent import output_module


@contextmanager
def use_v2_hierarchy_checkpoint(checkpoint_fn):
    """Route ``_hierarchy._checkpoint`` to Checkpoint V2 for this call."""
    hier = output_module("_hierarchy")
    original = hier._checkpoint
    hier._checkpoint = checkpoint_fn
    try:
        yield
    finally:
        hier._checkpoint = original


def _get_cost_percentage_details(spark, cfg, workflow_id, checkpoint_fn):
    """Production TVF body plus deal-hierarchy Checkpoint V2 seams."""
    prefix = table_prefix(cfg)

    cost_snap = spark.table(f"{prefix}.CostPercentage_Snapshot").filter(
        F.col("WorkFlowID") == workflow_id
    )
    entity = spark.table(f"{prefix}.Entity")
    enu_ut = spark.table(f"{prefix}.ENU_UnderlyingType")

    enu_ut_pf = enu_ut.select(
        F.col("UnderlyingTypeID").alias("pf_UnderlyingTypeID"),
        F.col("UnderlyingType").alias("pf_UnderlyingTypeName"),
    )
    pre_filtered = (
        cost_snap.filter(F.col("InvestmentID") == -2)
        .join(entity.select("EntityID"), "EntityID")
        .join(
            enu_ut_pf,
            cost_snap["UnderlyingType"] == F.col("pf_UnderlyingTypeID"),
        )
        .filter(F.upper(F.col("pf_UnderlyingTypeName")) != "ASSET CLASS")
        .select(
            cost_snap["WorkFlowID"],
            cost_snap["TransactionID"],
            cost_snap["ClientID"],
            cost_snap["TaxPeriodID"],
            cost_snap["EntityID"].alias("EntityId"),
            cost_snap["InvestmentID"],
            cost_snap["PartnerNumber"],
            cost_snap["Quarter"],
            cost_snap["CommitmentPercent"],
            cost_snap["AllocationTypeID"].alias("AllocationTypeId"),
            F.coalesce(cost_snap["Tag"], F.lit("")).alias("Tag"),
            F.coalesce(cost_snap["TrackingKey"], F.lit("")).alias(
                "TrackingKey"
            ),
            cost_snap["UnderlyingType"].alias("Underlyingtype"),
            cost_snap["AllocatedAmount"],
            cost_snap["CostPercentageID"].alias("CostPercentageId"),
            cost_snap["DealID"],
        )
        .distinct()
    )

    all_snap = (
        cost_snap.join(entity.select("EntityID"), "EntityID")
        .select(
            cost_snap["WorkFlowID"],
            cost_snap["TransactionID"],
            cost_snap["ClientID"],
            cost_snap["TaxPeriodID"],
            cost_snap["EntityID"].alias("EntityId"),
            cost_snap["InvestmentID"],
            cost_snap["PartnerNumber"],
            cost_snap["Quarter"],
            cost_snap["CommitmentPercent"],
            cost_snap["AllocationTypeID"].alias("AllocationTypeId"),
            F.coalesce(cost_snap["Tag"], F.lit("")).alias("Tag"),
            F.coalesce(cost_snap["TrackingKey"], F.lit("")).alias(
                "TrackingKey"
            ),
            cost_snap["UnderlyingType"].alias("Underlyingtype"),
            cost_snap["AllocatedAmount"],
            cost_snap["CostPercentageID"].alias("CostPercentageId"),
            cost_snap["DealID"],
        )
        .distinct()
    )
    all_snap = checkpoint_fn(spark, all_snap, "cost_all_snap", cfg)

    enu_ut_renamed = enu_ut.select(
        F.col("UnderlyingTypeID"),
        F.col("UnderlyingType").alias("UnderlyingTypeName"),
    )
    part1 = (
        all_snap.filter(F.col("InvestmentID") == -1)
        .join(
            enu_ut_renamed,
            F.col("Underlyingtype") == F.col("UnderlyingTypeID"),
        )
        .filter(F.upper(F.col("UnderlyingTypeName")) != "ASSET CLASS")
        .select(
            "WorkFlowID",
            "TransactionID",
            "ClientID",
            "TaxPeriodID",
            "EntityId",
            "InvestmentID",
            "PartnerNumber",
            "Quarter",
            "CommitmentPercent",
            "AllocationTypeId",
            "Tag",
            "TrackingKey",
            "Underlyingtype",
            "AllocatedAmount",
            "CostPercentageId",
        )
        .distinct()
    )

    part2 = (
        all_snap.filter(~F.col("InvestmentID").isin(-1, -2))
        .join(
            entity.select(F.col("EntityID").alias("InvEntityID")),
            F.col("InvestmentID") == F.col("InvEntityID"),
        )
        .join(
            enu_ut_renamed,
            F.col("Underlyingtype") == F.col("UnderlyingTypeID"),
        )
        .filter(F.upper(F.col("UnderlyingTypeName")) != "ASSET CLASS")
        .select(
            "WorkFlowID",
            "TransactionID",
            "ClientID",
            "TaxPeriodID",
            "EntityId",
            "InvestmentID",
            "PartnerNumber",
            "Quarter",
            "CommitmentPercent",
            "AllocationTypeId",
            "Tag",
            "TrackingKey",
            "Underlyingtype",
            "AllocatedAmount",
            "CostPercentageId",
        )
        .distinct()
    )

    enu_ac = (
        spark.table(f"{prefix}.Enu_AssetClass")
        .select("AssetClassID")
        .distinct()
    )
    part3 = (
        all_snap.join(
            enu_ut_renamed,
            F.col("Underlyingtype") == F.col("UnderlyingTypeID"),
        )
        .filter(F.upper(F.col("UnderlyingTypeName")) == "ASSET CLASS")
        .join(enu_ac, F.col("InvestmentID") == F.col("AssetClassID"))
        .select(
            "WorkFlowID",
            "TransactionID",
            "ClientID",
            "TaxPeriodID",
            "EntityId",
            "InvestmentID",
            "PartnerNumber",
            "Quarter",
            "CommitmentPercent",
            "AllocationTypeId",
            "Tag",
            "TrackingKey",
            "Underlyingtype",
            "AllocatedAmount",
            "CostPercentageId",
        )
        .distinct()
    )

    entities_with_deals = all_snap.filter(
        F.coalesce(F.col("DealID"), F.lit("")) != ""
    ).select("EntityId").distinct()

    entity_rel = spark.table(f"{prefix}.EntityRelationShip")

    hierarchy = (
        entity_rel.join(
            entities_with_deals,
            entity_rel["UpperTierEntityID"] == entities_with_deals["EntityId"],
        )
        .select(
            entities_with_deals["EntityId"].alias("EntityID"),
            entity_rel["UpperTierEntityID"],
            entity_rel["LowerTierEntityID"],
        )
    )
    hierarchy = checkpoint_fn(spark, hierarchy, "deal_hier_lvl_0", cfg)

    for level in range(1, 11):
        next_level = (
            entity_rel.alias("R")
            .join(
                hierarchy.alias("H"),
                F.col("R.UpperTierEntityID") == F.col("H.LowerTierEntityID"),
            )
            .select(
                F.col("H.EntityID"),
                F.col("R.UpperTierEntityID"),
                F.col("R.LowerTierEntityID"),
            )
        )
        next_level = checkpoint_fn(
            spark, next_level, f"deal_hier_lvl_{level}", cfg
        )
        if next_level.first() is None:
            break
        hierarchy = checkpoint_fn(
            spark,
            hierarchy.union(next_level).distinct(),
            f"deal_hier_acc_{level}",
            cfg,
        )

    self_ref = entities_with_deals.select(
        F.col("EntityId").alias("EntityID"),
        F.lit(None).cast("int").alias("UpperTierEntityID"),
        F.col("EntityId").alias("LowerTierEntityID"),
    )
    hierarchy = checkpoint_fn(
        spark,
        hierarchy.union(self_ref).distinct(),
        "deal_hierarchy",
        cfg,
    )

    entity_deals = (
        hierarchy.join(
            entity.select(
                F.col("EntityID").alias("LT_EntityID"), "Custom10"
            ),
            hierarchy["LowerTierEntityID"] == F.col("LT_EntityID"),
        )
        .filter(F.coalesce(F.col("Custom10"), F.lit("")) != "")
        .select(
            hierarchy["EntityID"],
            hierarchy["UpperTierEntityID"],
            hierarchy["LowerTierEntityID"],
            F.col("Custom10"),
        )
    )

    part4 = (
        pre_filtered.join(
            entity_deals,
            (pre_filtered["DealID"] == entity_deals["Custom10"])
            & (pre_filtered["EntityId"] == entity_deals["EntityID"]),
        )
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
        )
        .distinct()
    )

    return part1.unionByName(part2).unionByName(part3).unionByName(part4)


def load_cost_percentages(spark, cfg, checkpoint_fn):
    """Production ``load_cost_percentages`` with the deal-hierarchy V2 loop."""
    data = output_module("_data_loading")
    original = data._get_cost_percentage_details

    def patched(spark_inner, cfg_inner, workflow_id):
        return _get_cost_percentage_details(
            spark_inner, cfg_inner, workflow_id, checkpoint_fn
        )

    data._get_cost_percentage_details = patched
    try:
        return data.load_cost_percentages(spark, cfg)
    finally:
        data._get_cost_percentage_details = original
