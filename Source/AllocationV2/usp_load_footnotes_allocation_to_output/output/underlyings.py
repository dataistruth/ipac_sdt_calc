"""
Entity hierarchy and underlying type resolution for uspLoadFootnotesAllocationToOutput.

Handles cost percentage UDF inlining, recursive entity hierarchy CTE,
asset class filtering, and DAR-based underlyings ranking.
"""
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0
from Common_V2.core.observability import log_section, log_timing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# build_cost_percentage_data
# SQL lines: 849–878 (SP) + inline of udfGetCostPercentageDetails (323 lines)
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_cost_percentage_data(
    spark: SparkSession, cfg: dict,
) -> tuple:
    """Load cost percentage details (UDF inline), join underlying types, build TempCostUnderlyingTypes.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 849-878.
    Inlines: dbo.udfGetCostPercentageDetails(@CostPercentageWorkflowID) — 323-line TVF.
    Row count: POSSIBLY-EMPTY — depends on cost workflow having data.

    The UDF logic reads CostPercentage_Snapshot for the given WorkflowID, then:
    1. Pre-filters InvestmentID=-2 with UnderlyingType<>'ASSET CLASS'
    2. Main load (all rows for the workflow)
    3. Entity hierarchy for deal-level rows (DealID<>'')
    4. Final assembly: 4 UNIONs (InvestmentID=-1, NOT IN(-1,-2), ASSET CLASS, Deal)

    Then the SP:
    - Joins with Enu_Underlyingtype to get EntityUnderlyingtype name
    - Creates #TempCostUnderlyingTypes (DISTINCT subset excluding K-1 ONLY unless InvestmentID=-1)

    Returns:
        tuple: (df_cost_pct_snapshot, df_temp_cost_underlying_types)

    Columns (df_cost_pct_snapshot):
        WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityId, InvestmentID,
        PartnerNumber, Quarter, CommitmentPercent, AllocationTypeId, Tag, TrackingKey,
        Underlyingtype, AllocatedAmount, CostPercentageId, EntityUnderlyingtype

    Columns (df_temp_cost_underlying_types):
        EntityId, InvestmentID, Quarter, AllocationTypeId, TrackingKey, Underlyingtype,
        EntityUnderlyingtype
    """
    log_section("build_cost_percentage_data")
    t0 = time.time()

    cost_wf_id = cfg["cost_workflow_id"]

    # ── Read base table ──
    cost_snap = (
        read_table(spark, "CostPercentage_Snapshot", cfg)
        .filter(F.col("WorkFlowID") == cost_wf_id)
        .select(
            "WorkFlowID", "TransactionID", "ClientID", "TaxPeriodID",
            "EntityId", "InvestmentID", "PartnerNumber", "Quarter",
            "CommitmentPercent", "AllocationTypeId",
            ns(F.col("Tag"), F.lit("")).alias("Tag"),
            ns(F.col("TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.col("Underlyingtype").alias("UnderlyingTypeID_val"),
            "AllocatedAmount", "CostPercentageId", "DealID",
        )
    )

    # ── Read reference tables (cached — read once per SP execution) ──
    enu_ut = F.broadcast(
        read_table(spark, "Enu_Underlyingtype", cfg)
        .select(
            F.col("UnderlyingTypeID"),
            F.col("UnderlyingType").alias("UnderlyingTypeName"),
        )
    )

    entity = (
        cfg["_df_entity"]
        .select(F.col("EntityID").alias("ve_EntityID"))
        .distinct()
    )

    enu_asset_class = F.broadcast(
        read_table(spark, "Enu_AssetClass", cfg)
        .select(F.col("AssetClassID"))
        .distinct()
    ) if _table_exists(spark, "Enu_AssetClass", cfg) else None

    # ── UDF logic: build the 4 union components ──
    # Component 1: InvestmentID = -1, UnderlyingType <> 'ASSET CLASS'
    comp_global = (
        cost_snap
        .filter(F.col("InvestmentID") == -1)
        .join(enu_ut, cost_snap["UnderlyingTypeID_val"] == enu_ut["UnderlyingTypeID"], "inner")
        .filter(F.lower(enu_ut["UnderlyingTypeName"]) != "asset class")
        .select(
            cost_snap["WorkFlowID"], cost_snap["TransactionID"], cost_snap["ClientID"], cost_snap["TaxPeriodID"],
            cost_snap["EntityId"], cost_snap["InvestmentID"], cost_snap["PartnerNumber"], cost_snap["Quarter"],
            cost_snap["CommitmentPercent"], cost_snap["AllocationTypeId"], cost_snap["Tag"], cost_snap["TrackingKey"],
            cost_snap["UnderlyingTypeID_val"], cost_snap["AllocatedAmount"], cost_snap["CostPercentageId"],
        )
    )

    # Component 2: InvestmentID NOT IN (-1, -2), matched against Entity
    comp_entity = (
        cost_snap
        .filter(~F.col("InvestmentID").isin([-1, -2]))
        .join(entity, cost_snap["InvestmentID"] == entity["ve_EntityID"], "inner")
        .join(enu_ut, cost_snap["UnderlyingTypeID_val"] == enu_ut["UnderlyingTypeID"], "inner")
        .filter(F.lower(enu_ut["UnderlyingTypeName"]) != "asset class")
        .select(
            cost_snap["WorkFlowID"], cost_snap["TransactionID"], cost_snap["ClientID"], cost_snap["TaxPeriodID"],
            cost_snap["EntityId"], cost_snap["InvestmentID"], cost_snap["PartnerNumber"], cost_snap["Quarter"],
            cost_snap["CommitmentPercent"], cost_snap["AllocationTypeId"], cost_snap["Tag"], cost_snap["TrackingKey"],
            cost_snap["UnderlyingTypeID_val"], cost_snap["AllocatedAmount"], cost_snap["CostPercentageId"],
        )
    )

    # Component 3: ASSET CLASS (InvestmentID = AssetClassID)
    if enu_asset_class is not None:
        comp_asset = (
            cost_snap
            .join(enu_ut, cost_snap["UnderlyingTypeID_val"] == enu_ut["UnderlyingTypeID"], "inner")
            .filter(F.lower(enu_ut["UnderlyingTypeName"]) == "asset class")
            .join(enu_asset_class, cost_snap["InvestmentID"] == enu_asset_class["AssetClassID"], "inner")
            .select(
                cost_snap["WorkFlowID"], cost_snap["TransactionID"], cost_snap["ClientID"], cost_snap["TaxPeriodID"],
                cost_snap["EntityId"], cost_snap["InvestmentID"], cost_snap["PartnerNumber"], cost_snap["Quarter"],
                cost_snap["CommitmentPercent"], cost_snap["AllocationTypeId"], cost_snap["Tag"], cost_snap["TrackingKey"],
                cost_snap["UnderlyingTypeID_val"], cost_snap["AllocatedAmount"], cost_snap["CostPercentageId"],
            )
        )
    else:
        comp_asset = None

    # Component 4: Deal-level (complex entity hierarchy + Custom10 matching)
    # For deal-level rows, we build the entity hierarchy and match by DealID=Custom10
    deal_rows = cost_snap.filter(ns(F.col("DealID"), F.lit("")) != "")
    # Pre-filtered rows (InvestmentID=-2, non-ASSET CLASS) for deal matching
    pre_filtered = (
        cost_snap
        .filter(F.col("InvestmentID") == -2)
        .join(enu_ut, cost_snap["UnderlyingTypeID_val"] == enu_ut["UnderlyingTypeID"], "inner")
        .filter(F.lower(enu_ut["UnderlyingTypeName"]) != "asset class")
        .select(
            cost_snap["WorkFlowID"], cost_snap["TransactionID"], cost_snap["ClientID"], cost_snap["TaxPeriodID"],
            cost_snap["EntityId"], cost_snap["InvestmentID"], cost_snap["PartnerNumber"], cost_snap["Quarter"],
            cost_snap["CommitmentPercent"], cost_snap["AllocationTypeId"], cost_snap["Tag"], cost_snap["TrackingKey"],
            cost_snap["UnderlyingTypeID_val"], cost_snap["AllocatedAmount"], cost_snap["CostPercentageId"], cost_snap["DealID"],
        )
    )

    # Entity hierarchy for deal entities (iterative recursive CTE)
    # Broadcast: filtered to one client/period = typically < 5K rows
    entity_rel = F.broadcast(
        read_table(spark, "EntityRelationship", cfg)
        .filter(
            (F.col("ClientID") == cfg["client_id"])
            & (F.col("TaxPeriodID") == cfg["tax_period_id"])
        )
        .select("UpperTierEntityID", "LowerTierEntityID")
    )

    deal_entities = deal_rows.select("EntityId").distinct()

    # Build hierarchy: start from deal entities, traverse down
    # Level 0: direct relationships from deal entities
    hierarchy = (
        deal_entities
        .join(
            entity_rel,
            (deal_entities["EntityId"] == entity_rel["UpperTierEntityID"]),
            "inner",
        )
        .select(
            deal_entities["EntityId"].alias("RootEntityID"),
            entity_rel["UpperTierEntityID"],
            entity_rel["LowerTierEntityID"],
        )
    )

    # Iterative expansion (max 5 levels — practical entity depth limit)
    all_hierarchy = hierarchy
    current_level = hierarchy
    for i in range(5):
        er = entity_rel.alias(f"er_{i}")
        next_level = (
            current_level
            .join(
                er,
                current_level["LowerTierEntityID"] == F.col(f"er_{i}.UpperTierEntityID"),
                "inner",
            )
            .select(
                current_level["RootEntityID"],
                F.col(f"er_{i}.UpperTierEntityID").alias("UpperTierEntityID"),
                F.col(f"er_{i}.LowerTierEntityID").alias("LowerTierEntityID"),
            )
        )
        # Break if no more expansion
        all_hierarchy = all_hierarchy.unionByName(next_level)
        current_level = next_level

    # Include self-reference for root entities
    self_ref = (
        deal_entities
        .filter(F.col("EntityId") != -1)
        .select(
            F.col("EntityId").alias("RootEntityID"),
            F.col("EntityId").alias("UpperTierEntityID"),
            F.col("EntityId").alias("LowerTierEntityID"),
        )
    )
    all_hierarchy = all_hierarchy.unionByName(self_ref)

    # Get Custom10 from Entity for lower-tier entities
    entity_c10 = (
        cfg["_df_entity"]
        .filter(ns(F.col("Custom10"), F.lit("")) != "")
        .select(F.col("EntityID").alias("c10_eid"), F.col("Custom10"))
    )

    entity_deals = (
        all_hierarchy
        .join(entity_c10, all_hierarchy["LowerTierEntityID"] == entity_c10["c10_eid"], "inner")
        .select(
            all_hierarchy["RootEntityID"].alias("ed_EntityID"),
            all_hierarchy["UpperTierEntityID"].alias("ed_Upper"),
            all_hierarchy["LowerTierEntityID"].alias("ed_Lower"),
            entity_c10["Custom10"],
        )
    )

    # Match pre-filtered rows by DealID = Custom10 AND EntityId
    # Distinct subset for left-anti-join exclusion
    cost_distinct = (
        cost_snap
        .filter(
            (ns(F.col("DealID"), F.lit("")) == "")
            & F.col("InvestmentID").isNull()
        )
        .select("EntityId", "InvestmentID", "Quarter", "AllocationTypeId",
                "Tag", "TrackingKey", "UnderlyingTypeID_val")
        .distinct()
    )

    comp_deal = (
        pre_filtered.alias("C")
        .join(
            entity_deals,
            (F.col("C.DealID") == entity_deals["Custom10"])
            & (F.col("C.EntityId") == entity_deals["ed_EntityID"]),
            "inner",
        )
        .select(
            F.col("C.WorkFlowID"),
            F.col("C.TransactionID"),
            F.col("C.ClientID"),
            F.col("C.TaxPeriodID"),
            entity_deals["ed_Upper"].alias("EntityId"),
            entity_deals["ed_Lower"].alias("InvestmentID"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.col("C.CommitmentPercent"),
            F.col("C.AllocationTypeId"),
            F.col("C.Tag"),
            F.col("C.TrackingKey"),
            F.col("C.UnderlyingTypeID_val"),
            F.col("C.AllocatedAmount"),
            F.col("C.CostPercentageId"),
        )
    )

    # BUG-26 FIX: SQL excludes rows from comp_deal that already exist in cost_distinct
    # (anti-join on EntityId, InvestmentID, Quarter, AllocationTypeId, Tag, TrackingKey, UnderlyingTypeID_val)
    anti_keys = ["EntityId", "InvestmentID", "Quarter", "AllocationTypeId",
                 "Tag", "TrackingKey", "UnderlyingTypeID_val"]
    comp_deal = comp_deal.join(cost_distinct, anti_keys, "left_anti")

    # ── Assemble final UDF output ──
    parts = [comp_global, comp_entity]
    if comp_asset is not None:
        parts.append(comp_asset)
    parts.append(comp_deal)

    df_cost_func = parts[0]
    for p in parts[1:]:
        df_cost_func = df_cost_func.unionByName(p)
    df_cost_func = df_cost_func.distinct()

    # ── SP logic: JOIN with Enu_Underlyingtype to get EntityUnderlyingtype ──
    df_cost_snapshot = (
        df_cost_func
        .join(enu_ut, df_cost_func["UnderlyingTypeID_val"] == enu_ut["UnderlyingTypeID"], "inner")
        .select(
            df_cost_func["WorkFlowID"],
            df_cost_func["TransactionID"],
            df_cost_func["ClientID"],
            df_cost_func["TaxPeriodID"],
            df_cost_func["EntityId"],
            df_cost_func["InvestmentID"],
            df_cost_func["PartnerNumber"],
            df_cost_func["Quarter"],
            df_cost_func["CommitmentPercent"],
            df_cost_func["AllocationTypeId"],
            df_cost_func["Tag"],
            df_cost_func["TrackingKey"],
            df_cost_func["UnderlyingTypeID_val"].alias("Underlyingtype"),
            df_cost_func["AllocatedAmount"],
            df_cost_func["CostPercentageId"],
            enu_ut["UnderlyingTypeName"].alias("EntityUnderlyingtype"),
        )
    )

    # ── SP logic: Build #TempCostUnderlyingTypes ──
    # SELECT DISTINCT EntityId, InvestmentID, Quarter, AllocationTypeId, TrackingKey,
    #   Underlyingtype, EntityUnderlyingtype
    # FROM #CostPercentage_Snapshot C
    # WHERE (EntityUnderlyingtype <> 'K-1 ONLY'
    #   OR (EntityUnderlyingtype = 'K-1 ONLY' AND InvestmentID = -1))
    df_cost_underlying_types = (
        df_cost_snapshot
        .filter(
            (F.lower(F.col("EntityUnderlyingtype")) != "k-1 only")
            | ((F.lower(F.col("EntityUnderlyingtype")) == "k-1 only")
               & (F.col("InvestmentID") == -1))
        )
        .select(
            "EntityId", "InvestmentID", "Quarter", "AllocationTypeId",
            "TrackingKey", "Underlyingtype", "EntityUnderlyingtype",
        )
        .distinct()
    )

    log_timing("build_cost_percentage_data", t0)
    return df_cost_snapshot, df_cost_underlying_types


def _table_exists(spark: SparkSession, table_name: str, cfg: dict) -> bool:
    """Check if a table exists in the catalog. Returns True if accessible."""
    try:
        catalog = cfg.get("catalog", "")
        schema = cfg.get("schema", "")
        full_name = f"{catalog}.{schema}.{table_name}" if catalog else table_name
        spark.table(full_name)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# build_entity_hierarchy
# SQL lines: 885–970
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_entity_hierarchy(
    spark: SparkSession, cfg: dict,
    df_cost_pct_snapshot: DataFrame,
    df_temp_cost_underlying_types: DataFrame,
) -> tuple:
    """Build entity hierarchy via recursive CTE equivalent + UNION combinations.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 885-970.
    Row count: POSSIBLY-EMPTY — entity may have no underlying investments.
    Returns: (df_all_underlyings_combined, df_asset_class_rel)
        - df_all_underlyings_combined: #TempAllUnderlyingsCombined equivalent
        - df_asset_class_rel: #EntityAssetClassRelationShip for downstream filter_asset_class

    Columns (df_all_underlyings_combined):
        UnderlyingEntityId(INT), EntityId(INT), HLevel(INT), Underlyingtype(INT),
        AllocationTypeId(INT), TrackingKey(VARCHAR), AssetClassId(INT),
        ImmediateLowerTierEntityID(INT)

    The recursive CTE starts from #TempCostUnderlyingTypes joined to EntityRelationship,
    then traverses down the entity hierarchy. Four UNION parts are appended:
    1. CTE result joined back to TempCostUnderlyingTypes
    2. K-1 ONLY (direct investment)
    3. K-1 ONLY with InvestmentID=-1 (entity itself)
    4. Asset Class + Entity Total (direct from TempCostUnderlyingTypes)
    """
    log_section("build_entity_hierarchy")
    t0 = time.time()

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]

    # ── Load EntityRelationship (cached — shared with build_cost_percentage_data) ──
    entity_rel = F.broadcast(
        read_table(spark, "EntityRelationship", cfg)
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select("UpperTierEntityID", "LowerTierEntityID")
    )

    # ── Load #EntityAssetClassRelationShip ──
    # SELECT LowerTierEntityID, AssetClassID, TrackingKey FROM udfGetAssetClassRelationship(...)
    # This UDF returns asset class relationships. We inline it as a direct read.
    df_asset_class_rel = _build_asset_class_relationship(spark, cfg)

    # ── Recursive CTE: EntityHierarchy ──
    # Anchor: TempCostUnderlyingTypes → EntityRelationship
    # The anchor joins on ER.UpperTierEntityID = CASE WHEN EntityUnderlyingtype='Asset Class'
    #   THEN TC.EntityId ELSE TC.InvestmentID END

    tc = df_temp_cost_underlying_types.alias("TC")

    # Build the join entity for the anchor
    anchor_entity = (
        tc.withColumn(
            "_join_entity",
            F.when(
                F.lower(F.col("EntityUnderlyingtype")) == "asset class",
                F.col("EntityId"),
            ).otherwise(F.col("InvestmentID")),
        )
    )

    # Anchor level (HLevel=2)
    anchor = (
        anchor_entity
        .join(entity_rel, anchor_entity["_join_entity"] == entity_rel["UpperTierEntityID"], "inner")
        .select(
            entity_rel["LowerTierEntityID"],
            entity_rel["UpperTierEntityID"].alias("ParentEntityID"),
            entity_rel["UpperTierEntityID"].alias("CurrentEntityId"),
            F.lit(2).alias("HLevel"),
            anchor_entity["AllocationTypeId"],
            # TrackingKey: '~' + CASE logic
            F.concat(
                F.lit("~"),
                F.when(
                    F.lower(anchor_entity["EntityUnderlyingtype"]) == "asset class",
                    F.concat(entity_rel["LowerTierEntityID"].cast("string"), F.lit("~")),
                ).otherwise(
                    F.when(
                        ns(anchor_entity["TrackingKey"], F.lit("")) == "",
                        F.concat(anchor_entity["InvestmentID"].cast("string"), F.lit("~")),
                    ).otherwise(F.concat(anchor_entity["TrackingKey"], F.lit("~")))
                ),
            ).alias("TrackingKey"),
            anchor_entity["InvestmentID"].alias("AssetClassId"),
            entity_rel["LowerTierEntityID"].alias("ImmediateLowerTierEntityID"),
        )
    )

    # Iterative expansion (fixed depth — no take(1) round-trips).
    # Inner join naturally produces 0 rows at depths beyond actual hierarchy,
    # so union adds nothing. Avoids expensive cluster round-trips per level.
    all_levels = anchor
    current_level = anchor
    # BUG-17 FIX: SQL uses recursive CTE (unbounded). Real hierarchies can go 8+ levels.
    MAX_DEPTH = 8
    for depth in range(MAX_DEPTH):
        er = entity_rel.alias(f"er_cte_{depth}")
        next_level = (
            current_level
            .join(
                er,
                current_level["LowerTierEntityID"] == F.col(f"er_cte_{depth}.UpperTierEntityID"),
                "inner",
            )
            .select(
                F.col(f"er_cte_{depth}.LowerTierEntityID").alias("LowerTierEntityID"),
                F.col(f"er_cte_{depth}.UpperTierEntityID").alias("ParentEntityID"),
                current_level["CurrentEntityId"],
                (current_level["HLevel"] + 1).alias("HLevel"),
                current_level["AllocationTypeId"],
                current_level["TrackingKey"],
                current_level["AssetClassId"],
                current_level["ImmediateLowerTierEntityID"],
            )
        )
        all_levels = all_levels.unionByName(next_level)
        current_level = next_level

    # ── Final SELECT from CTE joined back to TempCostUnderlyingTypes ──
    # JOIN EntityHierarchy EH ON EH.CurrentEntityId = CASE WHEN ... END
    #   AND TC.AllocationTypeId=EH.AllocationTypeId AND TC.InvestmentID=EH.AssetClassId
    cte_result = (
        all_levels.alias("EH")
        .join(
            anchor_entity.alias("TC2"),
            (F.col("EH.CurrentEntityId") == F.col("TC2._join_entity"))
            & (F.col("TC2.AllocationTypeId") == F.col("EH.AllocationTypeId"))
            & (F.col("TC2.InvestmentID") == F.col("EH.AssetClassId")),
            "inner",
        )
        .select(
            F.col("EH.LowerTierEntityID").alias("UnderlyingEntityId"),
            F.col("EH.CurrentEntityId").alias("EntityId"),
            F.col("EH.HLevel"),
            F.col("TC2.Underlyingtype"),
            F.col("TC2.AllocationTypeId"),
            F.col("EH.TrackingKey"),
            F.col("EH.AssetClassId"),
            F.col("EH.ImmediateLowerTierEntityID"),
        )
        .distinct()
    )

    # ── UNION 2: K-1 ONLY (direct) ──
    k1_only = (
        df_cost_pct_snapshot
        .filter(F.lower(F.col("EntityUnderlyingtype")) == "k-1 only")
        .select(
            F.col("InvestmentID").alias("UnderlyingEntityId"),
            F.col("InvestmentID").alias("EntityId"),
            F.lit(1).alias("HLevel"),
            F.col("Underlyingtype"),
            F.col("AllocationTypeId"),
            F.when(
                ns(F.col("TrackingKey"), F.lit("")) == "",
                F.concat(F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")),
            ).otherwise(F.col("TrackingKey")).alias("TrackingKey"),
            F.col("InvestmentID").alias("AssetClassId"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
        )
        .distinct()
    )

    # ── UNION 3: K-1 ONLY with InvestmentID=-1 (entity itself) ──
    k1_self = (
        df_cost_pct_snapshot
        .filter(
            (F.lower(F.col("EntityUnderlyingtype")) == "k-1 only")
            & (F.col("InvestmentID") == -1)
        )
        .select(
            F.lit(entity_id).alias("UnderlyingEntityId"),
            F.lit(entity_id).alias("EntityId"),
            F.lit(1).alias("HLevel"),
            F.col("Underlyingtype"),
            F.col("AllocationTypeId"),
            F.concat(F.lit("~"), F.lit(entity_id).cast("string"), F.lit("~")).alias("TrackingKey"),
            F.lit(entity_id).alias("AssetClassId"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
        )
        .distinct()
    )

    # ── UNION 4: Asset Class (from TempCostUnderlyingTypes) ──
    asset_class_union = (
        df_temp_cost_underlying_types
        .filter(F.lower(F.col("EntityUnderlyingtype")) == "asset class")
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

    # ── UNION 5: Entity Total (from TempCostUnderlyingTypes) ──
    entity_total_union = (
        df_temp_cost_underlying_types
        .filter(F.lower(F.col("EntityUnderlyingtype")) == "entity total")
        .select(
            F.col("InvestmentID").alias("UnderlyingEntityId"),
            F.col("InvestmentID").alias("EntityId"),
            F.lit(1).alias("HLevel"),
            F.col("Underlyingtype"),
            F.col("AllocationTypeId"),
            F.when(
                ns(F.col("TrackingKey"), F.lit("")) == "",
                F.concat(F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")),
            ).otherwise(F.col("TrackingKey")).alias("TrackingKey"),
            F.col("InvestmentID").alias("AssetClassId"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
        )
    )

    # ── Final assembly ──
    df_all = (
        cte_result
        .unionByName(k1_only)
        .unionByName(k1_self)
        .unionByName(asset_class_union)
        .unionByName(entity_total_union)
    )

    # NOTE: Do NOT call warn_if_empty here — this DF feeds into
    # build_underlyings_footnotes_ordered which feeds into checkpoint.
    # Calling isEmpty forces plan execution; the plan then re-executes at checkpoint.
    # Matches SQL pattern: #TempAllUnderlyingsCombined is just a temp table,
    # not checked for empty before use.

    log_timing("build_entity_hierarchy", t0)
    return df_all, df_asset_class_rel


def _build_asset_class_relationship(spark: SparkSession, cfg: dict) -> DataFrame:
    """Inline udfGetAssetClassRelationship — returns (LowerTierEntityID, AssetClassID, TrackingKey).

    BUG-02 FIX: Properly inlines the TVF logic instead of reading a non-existent table.
    SQL TVF logic:
      1. BasisOverrideImportData: rows where LineDescription IN ('Asset Class','AssetClass') AND Value<>'0'
      2. If GlobalMenu 'Asset Class Override Import' is enabled (State='C'):
         - Get latest AssetClassOverride TransactionID per entity
         - Read AssetClassOverrideImportData for those transactions
         - Only for entities NOT already in BasisOverride result
    """
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]

    # Step 1: BasisOverrideImportData — Asset Class rows for this entity
    basis_override = (
        read_table(spark, "BasisOverrideImportData", cfg)
        .filter(
            (F.col("UpperTierEntityID") == entity_id)
            & (F.lower(F.col("LineDescription")).isin("asset class", "assetclass"))
            & (F.coalesce(F.col("Value"), F.lit("0")) != "0")
        )
        .select(
            F.col("LowerTierEntityID"),
            F.col("Value").cast("int").alias("AssetClassID"),
        )
    )

    # Track which entities already have BasisOverride entries
    basis_entities = basis_override.select("LowerTierEntityID").distinct()

    # Step 2: Check if Asset Class Override Import is enabled (pre-resolved cfg flag).
    ac_override_enabled = (cfg.get("flag_asset_class_override_import") == "C")

    if ac_override_enabled:
        # Get AssetClassOverride EventTypeID from cfg.
        ac_override_event_id = cfg.get("event_type_id_import_asset_class_override")

        if ac_override_event_id is not None:
            # Get latest TransactionID for AssetClassOverride event for this entity
            # SQL: dbo.udfGetLatestTransactionID(@ClientID, @TaxPeriodID, 0, @Event, EntityID)
            # @UseEntityID = 1 → LEFT JOIN with VW_Entity to validate entity exists
            entity_valid = (
                cfg["_df_entity"]
                .filter(
                    (F.col("ClientID") == client_id)
                    & (F.col("TaxPeriodID") == tax_period_id)
                )
                .select(F.col("EntityID").alias("_ve_EntityID"))
            )
            latest_tx = (
                read_table(spark, "TransactionLog", cfg)
                .filter(
                    (F.col("ClientID") == client_id)
                    & (F.col("TaxPeriodID") == tax_period_id)
                    & (F.col("EventTypeID") == ac_override_event_id)
                    & (F.col("EntityID") == entity_id)
                )
                .join(entity_valid, F.col("EntityID") == F.col("_ve_EntityID"), "left")
                .filter(F.col("_ve_EntityID").isNotNull())
                .agg(F.max("TransactionID").alias("_max_tx"))
                .collect()
            )
            max_tx_id = latest_tx[0]["_max_tx"] if latest_tx and latest_tx[0]["_max_tx"] else None

            if max_tx_id is not None:
                # Read AssetClassOverrideImportData for this transaction
                # Only for entities NOT already in basis_entities
                ac_override = (
                    read_table(spark, "AssetClassOverrideImportData", cfg)
                    .filter(F.col("TransactionID") == max_tx_id)
                    .join(
                        cfg["_df_entity"].select("EntityID", "AssetClassID").alias("E"),
                        F.col("UnderlyingID") == F.col("E.EntityID"),
                        "inner",
                    )
                    .select(
                        F.col("UnderlyingID").alias("LowerTierEntityID"),
                        F.when(
                            F.coalesce(F.col("OverrideAssetClassID").cast("string"), F.lit("")) != "",
                            F.col("OverrideAssetClassID").cast("int"),
                        ).otherwise(
                            F.when(
                                F.coalesce(F.col("E.AssetClassID").cast("string"), F.lit("")) != "",
                                F.col("E.AssetClassID"),
                            ).otherwise(F.lit(-1))
                        ).alias("AssetClassID"),
                        F.col("TrackingKey"),
                    )
                    .join(basis_entities, "LowerTierEntityID", "left_anti")
                )

                # Combine: BasisOverride (no TrackingKey) + AssetClassOverride (with TrackingKey)
                result = (
                    basis_override
                    .withColumn("TrackingKey", F.lit(None).cast("string"))
                    .unionByName(ac_override)
                )
                return result

    # No override or override not enabled — just return BasisOverride rows
    return basis_override.withColumn("TrackingKey", F.lit(None).cast("string"))


# ---------------------------------------------------------------------------
# filter_asset_class
# SQL lines: 980–1170
# Row count: LEGITIMATELY-EMPTY
# ---------------------------------------------------------------------------
def filter_asset_class(
    spark: SparkSession, cfg: dict,
    df_all_underlyings: DataFrame,
    df_asset_class_rel: DataFrame,
) -> DataFrame:
    """Filter underlyings by asset class matching logic.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 980-1170.
    Row count: LEGITIMATELY-EMPTY — asset class override may not apply.

    Two main branches:
    IF @OverrideIndirectLookthroughAssetClass <> 'C':
        Complex matching: build #MatchingAssetClass (TrackingKey NULL vs NOT NULL),
        then DELETE rows from TempAllUnderlyingsCombined that are Asset Class type
        but NOT in #MatchingAssetClass.
    ELSE:
        Simple: DELETE Asset Class rows where AssetClassID != Entity.AssetClassID
        (via EntityAssetClassRelationShip or Entity).

    Then: IF @IgnoreAssetclassForPartnershipLevel = 'C':
        DELETE Asset Class rows where UnderlyingEntityId = @LocalEntityID
    """
    log_section("filter_asset_class")
    t0 = time.time()

    override_flag = cfg.get("override_indirect_lookthrough_asset_class")
    ignore_flag = cfg.get("ignore_assetclass_for_partnership_level")
    entity_id = cfg["entity_id"]

    enu_ut = F.broadcast(
        read_table(spark, "Enu_Underlyingtype", cfg)
        .select(F.col("UnderlyingTypeID"), F.lower(F.col("UnderlyingType")).alias("ut_lower"))
    )

    # Tag each row with underlying type name
    df = (
        df_all_underlyings.alias("AI")
        .join(enu_ut, F.col("AI.Underlyingtype") == enu_ut["UnderlyingTypeID"], "left")
    )

    # Separate Asset Class rows from non-Asset-Class rows
    non_ac = df.filter(F.col("ut_lower") != "asset class").select("AI.*")
    ac_rows = df.filter(F.col("ut_lower") == "asset class")

    if override_flag != "C":
        # ── Complex path: match asset class rows against #EntityAssetClassRelationShip ──
        entity_tbl = cfg["_df_entity"].select(
            F.col("EntityID").alias("e_eid"), F.col("AssetClassID").alias("e_acid")
        )
        vw_entity = cfg["_df_entity"].select(
            F.col("EntityID").alias("vw_eid"), F.col("AssetClassID").alias("vw_acid")
        )

        # Determine effective AssetClassID: CASE WHEN ISNULL(EAR.AssetClassID,0)=0
        #   THEN E.AssetClassID ELSE EAR.AssetClassID END = AI.AssetClassId

        # Part 1: TrackingKey IS NULL in EAR
        ear_null_tk = df_asset_class_rel.filter(F.col("TrackingKey").isNull())
        # Part 2: TrackingKey IS NOT NULL in EAR
        ear_nonnull_tk = df_asset_class_rel.filter(F.col("TrackingKey").isNotNull())

        # Match for NULL TrackingKey
        match_null = (
            ac_rows
            .join(ear_null_tk, F.col("AI.UnderlyingEntityId") == ear_null_tk["LowerTierEntityID"], "inner")
            .join(entity_tbl, F.col("AI.UnderlyingEntityId") == entity_tbl["e_eid"], "inner")
            .filter(
                F.when(
                    F.coalesce(ear_null_tk["AssetClassID"], F.lit(0)) == 0,
                    F.col("e_acid"),
                ).otherwise(ear_null_tk["AssetClassID"]) == F.col("AI.AssetClassId")
            )
            .select(
                F.col("AI.UnderlyingEntityId"), F.col("AI.EntityId"), F.col("AI.HLevel"),
                F.col("AI.Underlyingtype"), F.col("AI.AllocationTypeId"),
                F.col("AI.TrackingKey"), F.col("AI.AssetClassId"),
                F.col("AI.ImmediateLowerTierEntityID"),
            )
        )

        # Match for NOT NULL TrackingKey (LIKE pattern: '~' + EAR.TrackingKey + '~' LIKE '%' + AI.TrackingKey + '%')
        match_nonnull = (
            ac_rows
            .join(
                ear_nonnull_tk,
                (F.col("AI.UnderlyingEntityId") == ear_nonnull_tk["LowerTierEntityID"])
                & (F.concat(F.lit("~"), ear_nonnull_tk["TrackingKey"], F.lit("~"))
                   .contains(F.col("AI.TrackingKey"))),
                "inner",
            )
            .join(entity_tbl, F.col("AI.UnderlyingEntityId") == entity_tbl["e_eid"], "inner")
            .filter(
                F.when(
                    F.coalesce(ear_nonnull_tk["AssetClassID"], F.lit(0)) == 0,
                    F.col("e_acid"),
                ).otherwise(ear_nonnull_tk["AssetClassID"]) == F.col("AI.AssetClassId")
            )
            .select(
                F.col("AI.UnderlyingEntityId"), F.col("AI.EntityId"), F.col("AI.HLevel"),
                F.col("AI.Underlyingtype"), F.col("AI.AllocationTypeId"),
                F.col("AI.TrackingKey"), F.col("AI.AssetClassId"),
                F.col("AI.ImmediateLowerTierEntityID"),
            )
        )

        # Also match via Entity (fallback for unmatched)
        match_vw = (
            ac_rows
            .join(vw_entity, (F.col("AI.UnderlyingEntityId") == vw_entity["vw_eid"])
                  & (F.col("AI.AssetClassId") == vw_entity["vw_acid"]), "inner")
            .select(
                F.col("AI.UnderlyingEntityId"), F.col("AI.EntityId"), F.col("AI.HLevel"),
                F.col("AI.Underlyingtype"), F.col("AI.AllocationTypeId"),
                F.col("AI.TrackingKey"), F.col("AI.AssetClassId"),
                F.col("AI.ImmediateLowerTierEntityID"),
            )
        )

        # Combine all matching asset class rows
        matching_ac = match_null.unionByName(match_nonnull).unionByName(match_vw).distinct()

        # BUG-03 FIX: SQL uses IF EXISTS(SELECT TOP 1 1 FROM #EntityAssetClassRelationShip)
        # When EAR is empty, SQL skips the DELETE entirely — keeps ALL AC rows.
        # Only apply the filter when EAR has rows.
        ear_has_rows = df_asset_class_rel.head(1) is not None and len(df_asset_class_rel.head(1)) > 0
        if not ear_has_rows:
            # EAR empty → keep all AC rows (SQL: IF EXISTS fails → skip DELETE)
            ac_kept = ac_rows.select(
                F.col("AI.UnderlyingEntityId"), F.col("AI.EntityId"), F.col("AI.HLevel"),
                F.col("AI.Underlyingtype"), F.col("AI.AllocationTypeId"),
                F.col("AI.TrackingKey"), F.col("AI.AssetClassId"),
                F.col("AI.ImmediateLowerTierEntityID"),
            )
        else:
            # EAR has rows → apply the matching filter (DELETE those NOT in matching)
            ac_kept = (
                ac_rows.alias("AC2")
                .join(
                    matching_ac.alias("M"),
                    (F.col("AC2.UnderlyingEntityId") == F.col("M.UnderlyingEntityId"))
                    & (F.col("AC2.TrackingKey") == F.col("M.TrackingKey"))
                    & (F.col("AC2.AssetClassId") == F.col("M.AssetClassId"))
                    & (F.col("AC2.ImmediateLowerTierEntityID") == F.col("M.ImmediateLowerTierEntityID")),
                    "left_semi",
                )
                .select(
                    F.col("AC2.UnderlyingEntityId"), F.col("AC2.EntityId"), F.col("AC2.HLevel"),
                    F.col("AC2.Underlyingtype"), F.col("AC2.AllocationTypeId"),
                    F.col("AC2.TrackingKey"), F.col("AC2.AssetClassId"),
                    F.col("AC2.ImmediateLowerTierEntityID"),
                )
            )

    else:
        # ── Simple path: OverrideIndirectLookthroughAssetClass = 'C' ──
        # DELETE WHERE U.UnderlyingType = 'Asset Class'
        #   AND CASE WHEN ISNULL(EAR.AssetClassID,0)=0 THEN E.AssetClassID
        #   ELSE EAR.AssetClassID END != AI.AssetClassId
        vw_entity = cfg["_df_entity"].select(
            F.col("EntityID").alias("vw_eid"), F.col("AssetClassID").alias("vw_acid")
        )

        ac_kept = (
            ac_rows.alias("AC3")
            .join(
                df_asset_class_rel.alias("EAR2"),
                F.col("AC3.ImmediateLowerTierEntityID") == F.col("EAR2.LowerTierEntityID"),
                "left",
            )
            .join(vw_entity, F.col("AC3.ImmediateLowerTierEntityID") == vw_entity["vw_eid"], "inner")
            .filter(
                F.when(
                    F.coalesce(F.col("EAR2.AssetClassID"), F.lit(0)) == 0,
                    F.col("vw_acid"),
                ).otherwise(F.col("EAR2.AssetClassID")) == F.col("AC3.AssetClassId")
            )
            .select(
                F.col("AC3.UnderlyingEntityId"), F.col("AC3.EntityId"), F.col("AC3.HLevel"),
                F.col("AC3.Underlyingtype"), F.col("AC3.AllocationTypeId"),
                F.col("AC3.TrackingKey"), F.col("AC3.AssetClassId"),
                F.col("AC3.ImmediateLowerTierEntityID"),
            )
        )

    # Recombine: non-AC rows + kept AC rows
    df_result = non_ac.unionByName(ac_kept)

    # ── @IgnoreAssetclassForPartnershipLevel = 'C' ──
    # DELETE WHERE UnderlyingEntityId = @LocalEntityID AND UnderlyingType = 'Asset Class'
    if ignore_flag == "C":
        # Join-based filter: remove rows where UnderlyingEntityId = entity AND type is Asset Class
        ac_type_ids = enu_ut.filter(F.col("ut_lower") == "asset class").select("UnderlyingTypeID")
        df_result = df_result.join(
            ac_type_ids,
            (df_result["Underlyingtype"] == ac_type_ids["UnderlyingTypeID"])
            & (df_result["UnderlyingEntityId"] == entity_id),
            "left_anti",
        )

    log_timing("filter_asset_class", t0)
    return df_result


# ---------------------------------------------------------------------------
# build_underlyings_footnotes_ordered
# SQL lines: 1076–1210
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_underlyings_footnotes_ordered(
    spark: SparkSession, cfg: dict,
    df_all_underlyings: DataFrame,
    df_alloc_input: DataFrame,
) -> DataFrame:
    """Build ranked underlyings footnotes using ROW_NUMBER + At Risk fallback.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 1076-1210.
    Row count: POSSIBLY-EMPTY — depends on DAR import having rules.
    Returns: df_underlyings_footnotes (filtered to RankForUnderlyingPickup=1)

    Logic:
    1. Join TempAllUnderlyingsCombined → ENU_UnderlyingType → TempAllocationInput
       → MapDefaultAllocRuleToLineItem → DefaultAllocationRuleSetup → ENU_RuleType
    2. Complex TrackingKey CASE/LIKE matching
    3. ROW_NUMBER OVER (PARTITION BY UnderlyingEntityId, TrackingKey, LineID, LineTypeId
       ORDER BY HLevel, RuleType.DisplayOrder DESC, UnderlyingType.DisplayOrder,
       SelectedMappingID DESC)
    4. INSERT At Risk fallback for lines not already matched
    5. Filter to RankForUnderlyingPickup = 1

    Columns output:
        Underlyingtype, UnderlyingEntityId, EntityId, TrackingKey, TrackingMatch,
        AllocationTypeId, LineID, ExcludeFromTransfers, RankForUnderlyingPickup, LineTypeId
    """
    log_section("build_underlyings_footnotes_ordered")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    override_flag = cfg.get("override_indirect_lookthrough_asset_class")
    dar_tid = cfg.get("default_allocation_rule_transaction_id")
    global_dar_tid = cfg.get("global_default_allocation_rule_transaction_id")
    at_risk_lt = cfg["at_risk_line_type_id"]
    k1_lt = cfg["k1_line_type_id"]

    # Valid TransactionIDs for DAR
    valid_tids = [t for t in [dar_tid, global_dar_tid] if t is not None]
    if not valid_tids:
        logger.warning("[SKIP] No DAR transaction IDs — underlyings footnotes will be empty")
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        schema = StructType([
            StructField("Underlyingtype", IntegerType()),
            StructField("UnderlyingEntityId", IntegerType()),
            StructField("EntityId", IntegerType()),
            StructField("TrackingKey", StringType()),
            StructField("TrackingMatch", StringType()),
            StructField("AllocationTypeId", IntegerType()),
            StructField("LineID", IntegerType()),
            StructField("ExcludeFromTransfers", IntegerType()),
            StructField("RankForUnderlyingPickup", IntegerType()),
            StructField("LineTypeId", IntegerType()),
        ])
        log_timing("build_underlyings_footnotes_ordered", t0)
        return spark.createDataFrame([], schema)

    # ── Load reference tables (all broadcast — small lookup tables, cached) ──
    enu_ut = F.broadcast(
        read_table(spark, "Enu_Underlyingtype", cfg)
        .select(
            F.col("UnderlyingTypeID").alias("ut_id"),
            F.lower(F.col("UnderlyingType")).alias("ut_lower"),
            F.col("DisplayOrder").alias("ut_display_order"),
        )
    )

    map_dar = F.broadcast(
        read_table(spark, "MapDefaultAllocRuleToLineItem", cfg)
        .filter(F.col("TransactionID").isin(valid_tids))
        .select("SelectedMappingID", "RuleID", "SourceID", "ExcludeFromTransfers", "TransactionID")
    )

    dar_setup = F.broadcast(
        read_table(spark, "DefaultAllocationRuleSetup", cfg)
        .filter(F.col("TransactionID").isin(valid_tids))
        .select("RuleID", "UnderlyingTypeID", "RuleTypeID", "TransactionID")
    )

    enu_rule = F.broadcast(
        read_table(spark, "ENU_RuleType", cfg)
        .select(F.col("RuleTypeID"), F.col("DisplayOrder").alias("rule_display_order"))
    )

    # ── Build main ordered set ──
    # Broadcast the hierarchy result — per-entity it's typically < 1000 rows.
    # This converts the non-equi .contains() join from CartesianProduct to
    # BroadcastNestedLoopJoin (efficient for small broadcast side).
    ai = F.broadcast(df_all_underlyings).alias("AI")
    li = df_alloc_input.alias("L")

    # Join AI → ENU_UnderlyingType
    ai_typed = ai.join(enu_ut, F.col("AI.Underlyingtype") == enu_ut["ut_id"], "inner")

    # TrackingKey CASE/LIKE matching logic:
    # CASE WHEN AI.UnderlyingEntityId = @LocalEntityID
    #   OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class')
    #   OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class')
    # THEN '-1' ELSE '~' + L.TrackingKey + '~' END
    # LIKE
    # CASE WHEN ... THEN '-1' ELSE '%' + AI.TrackingKey + '%' END

    # This means: when any of those conditions is true, the TrackingKey match is always true (bypass)
    bypass_cond = (
        (F.col("AI.UnderlyingEntityId") == entity_id)
        | ((F.col("AI.EntityId") == entity_id) & (F.col("ut_lower") != "asset class"))
        | ((F.lit(override_flag != "C")) & (F.col("ut_lower") == "asset class"))
    )

    # Join AI → TempAllocationInput on EntityID = UnderlyingEntityId + TrackingKey logic
    ai_li = (
        ai_typed
        .join(
            li,
            (F.col("L.EntityID") == F.col("AI.UnderlyingEntityId"))
            & (
                bypass_cond
                | F.concat(F.lit("~"), F.col("L.TrackingKey"), F.lit("~"))
                .contains(F.col("AI.TrackingKey"))
            ),
            "inner",
        )
    )

    # Join → MapDefaultAllocRuleToLineItem
    # ON CASE WHEN M.SelectedMappingID=-1 THEN 1 ELSE L.LineID END
    #  = CASE WHEN M.SelectedMappingID=-1 THEN 1 ELSE M.SelectedMappingID END
    # AND M.RuleID=AI.AllocationTypeId AND M.SourceID=L.LineTypeID
    ai_li_m = (
        ai_li
        .join(
            map_dar.alias("M"),
            (F.col("M.RuleID") == F.col("AI.AllocationTypeId"))
            & (F.col("M.SourceID") == F.col("L.LineTypeID"))
            & (
                F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("L.LineID"))
                ==
                F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("M.SelectedMappingID"))
            ),
            "inner",
        )
    )

    # Join → DefaultAllocationRuleSetup
    # ON D.RuleID=AI.AllocationTypeId AND AI.Underlyingtype=D.UnderlyingTypeID
    ai_li_m_d = (
        ai_li_m
        .join(
            dar_setup.alias("D"),
            (F.col("D.RuleID") == F.col("AI.AllocationTypeId"))
            & (F.col("AI.Underlyingtype") == F.col("D.UnderlyingTypeID")),
            "inner",
        )
    )

    # Join → ENU_RuleType
    ai_full = ai_li_m_d.join(enu_rule, F.col("D.RuleTypeID") == enu_rule["RuleTypeID"], "inner")

    # ROW_NUMBER
    w = Window.partitionBy(
        F.col("AI.UnderlyingEntityId"), F.col("L.TrackingKey"),
        F.col("L.LineID"), F.col("L.LineTypeID"),
    ).orderBy(
        F.col("AI.HLevel").asc(),
        F.col("rule_display_order").desc(),
        F.col("ut_display_order").asc(),
        F.col("M.SelectedMappingID").desc(),
    )

    ordered = (
        ai_full
        .withColumn("RankForUnderlyingPickup", F.row_number().over(w))
        .select(
            F.col("AI.Underlyingtype"),
            F.col("AI.UnderlyingEntityId"),
            F.col("AI.EntityId"),
            F.col("L.TrackingKey"),
            F.col("AI.TrackingKey").alias("TrackingMatch"),
            F.col("AI.AllocationTypeId"),
            F.col("L.LineID"),
            F.col("M.ExcludeFromTransfers"),
            F.col("RankForUnderlyingPickup"),
            F.col("L.LineTypeID").alias("LineTypeId"),
        )
    )

    # ── At Risk fallback INSERT ──
    # Insert K-1 rule for At Risk lines not already in the ordered table
    # Same join structure but M.SourceID = @K1LineTypeID (not L.LineTypeID)
    # Additional filter: EL.LineType IN ('At Risk') AND TFO.LineTypeID IS NULL
    at_risk_input = li.filter(F.col("L.LineTypeID") == at_risk_lt)

    at_risk_joined = (
        ai_typed
        .join(
            at_risk_input,
            (F.col("L.EntityID") == F.col("AI.UnderlyingEntityId"))
            & (
                bypass_cond
                | F.concat(F.lit("~"), F.col("L.TrackingKey"), F.lit("~"))
                .contains(F.col("AI.TrackingKey"))
            ),
            "inner",
        )
        .join(
            map_dar.alias("M2"),
            (F.col("M2.RuleID") == F.col("AI.AllocationTypeId"))
            & (F.col("M2.SourceID") == k1_lt)
            & (
                F.when(F.col("M2.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("L.LineID"))
                ==
                F.when(F.col("M2.SelectedMappingID") == -1, F.lit(1))
                .otherwise(F.col("M2.SelectedMappingID"))
            ),
            "inner",
        )
        .join(
            dar_setup.alias("D2"),
            (F.col("D2.RuleID") == F.col("AI.AllocationTypeId"))
            & (F.col("AI.Underlyingtype") == F.col("D2.UnderlyingTypeID")),
            "inner",
        )
        .join(enu_rule.alias("R2"), F.col("D2.RuleTypeID") == F.col("R2.RuleTypeID"), "inner")
    )

    # Anti-join: exclude lines already in ordered set with At Risk line type
    at_risk_new = (
        at_risk_joined
        .join(
            ordered.filter(F.col("LineTypeId") == at_risk_lt).alias("TFO"),
            (F.col("AI.UnderlyingEntityId") == F.col("TFO.UnderlyingEntityId"))
            & (F.col("AI.EntityId") == F.col("TFO.EntityId"))
            & (F.col("L.LineID") == F.col("TFO.LineID"))
            & (F.col("L.LineTypeID") == F.col("TFO.LineTypeId")),
            "left_anti",
        )
    )

    w2 = Window.partitionBy(
        F.col("AI.UnderlyingEntityId"), F.col("L.TrackingKey"),
        F.col("L.LineID"), F.col("L.LineTypeID"),
    ).orderBy(
        F.col("AI.HLevel").asc(),
        F.col("R2.rule_display_order").desc(),
        F.col("ut_display_order").asc(),
        F.col("M2.SelectedMappingID").desc(),
    )

    at_risk_ordered = (
        at_risk_new
        .withColumn("RankForUnderlyingPickup", F.row_number().over(w2))
        .select(
            F.col("AI.Underlyingtype"),
            F.col("AI.UnderlyingEntityId"),
            F.col("AI.EntityId"),
            F.col("L.TrackingKey"),
            F.col("AI.TrackingKey").alias("TrackingMatch"),
            F.col("AI.AllocationTypeId"),
            F.col("L.LineID"),
            F.col("M2.ExcludeFromTransfers"),
            F.col("RankForUnderlyingPickup"),
            F.col("L.LineTypeID").alias("LineTypeId"),
        )
    )

    # ── Combine and filter to Rank=1 ──
    all_ordered = ordered.unionByName(at_risk_ordered)
    df_result = all_ordered.filter(F.col("RankForUnderlyingPickup") == 1)

    # NOTE: Do NOT call warn_if_empty here — it forces full plan execution (~160s).
    # The underlyings DF is used in build_allocation_input LEFT JOINs and then
    # materialized ONCE via checkpoint(). Calling isEmpty here would double the cost.
    # Emptiness is checked on the checkpointed alloc_input DF instead (cheap Delta read).

    log_timing("build_underlyings_footnotes_ordered", t0)
    return df_result
